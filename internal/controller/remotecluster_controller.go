/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	corev1 "k8s.io/api/core/v1"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

//go:embed assets/ml.dcn.ssu.ac.kr_nodeprovisionnetconfigs.yaml
var nodeprovisionnetconfigCRD string

// prepullJobResult carries the outcome of a background image pre-pull goroutine.
type prepullJobResult struct {
	err error
}

// controlPlaneJobResult carries the outcome of a background control-plane init goroutine.
type controlPlaneJobResult struct {
	joinCommand string
	err         error
}

// RemoteClusterReconciler reconciles a RemoteCluster object.
type RemoteClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// prepullJobs holds in-flight image pre-pull goroutines.
	// Key: "namespace/name", Value: <-chan prepullJobResult
	prepullJobs sync.Map

	// controlPlaneJobs holds in-flight control-plane init goroutines.
	// Key: "namespace/name", Value: <-chan controlPlaneJobResult
	controlPlaneJobs sync.Map
}

const (
	remoteClusterFinalizer = "infra.dcn.ssu.ac.kr/remotecluster-finalizer"
	remoteClusterLabelKey  = "infra.dcn.ssu.ac.kr/remotecluster"

	// annotationPkgVariantsCreated marks that PackageVariants have been successfully
	// created for this control-plane cluster, so they are not re-created on every reconcile.
	annotationPkgVariantsCreated = "infra.dcn.ssu.ac.kr/package-variants-created"
	// annotationWorkerJoined marks that this worker has already successfully joined its cluster.
	annotationWorkerJoined = "infra.dcn.ssu.ac.kr/worker-joined"
	// annotationNvidiaInstalled marks that NVIDIA drivers have been installed on this node.
	annotationNvidiaInstalled = "infra.dcn.ssu.ac.kr/nvidia-installed"
	// annotationCDIGenerated marks that the CDI spec has been generated after the post-driver reboot.
	annotationCDIGenerated = "infra.dcn.ssu.ac.kr/nvidia-cdi-generated"
	// annotationImagesPrepulled marks that all images in spec.nodeInfo.softwareConfig.imagePrepulls
	// have been successfully pulled on the worker node.
	annotationImagesPrepulled = "infra.dcn.ssu.ac.kr/images-prepulled"

	phaseProvisioning = "Provisioning"
	phaseReady        = "Ready"
	phaseFailed       = "Failed"

	// repoReadyWait is the time to wait after creating the cluster repo before
	// creating PackageVariants, giving Porch time to sync the new repository.
	repoReadyWait = 2 * time.Minute

	// controlPlaneRetryInterval is how long to wait before re-checking whether
	// the parent control-plane is ready.
	controlPlaneRetryInterval = 30 * time.Second

	// sshOperationTimeout caps total time spent on SSH-heavy provisioning steps.
	sshOperationTimeout = 30 * time.Minute

	// postRebootWait is how long to wait after issuing a reboot before attempting
	// to SSH back into the node.  Kernel + driver initialisation typically takes
	// 60–90 s; 3 minutes gives comfortable headroom.
	postRebootWait = 3 * time.Minute

	// prepullPollInterval is how often the controller checks whether the
	// background image pre-pull goroutine has finished.
	prepullPollInterval = 30 * time.Second

	// controlPlanePollInterval is how often the controller polls the background
	// control-plane init goroutine.  kubeadm init + CNI setup typically takes
	// 5-15 minutes, so 30 s gives reasonable responsiveness without hammering.
	controlPlanePollInterval = 30 * time.Second
)

// packageVariantGVK is the GVK for Porch PackageVariant resources.
var packageVariantGVK = schema.GroupVersionKind{
	Group:   "config.porch.kpt.dev",
	Version: "v1alpha1",
	Kind:    "PackageVariant",
}

// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters/finalizers,verbs=update

func (r *RemoteClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cluster := &infrav1.RemoteCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log = log.WithValues(
		"cluster", cluster.Name,
		"clusterName", cluster.Spec.ClusterName,
		"nodeType", cluster.Spec.NodeInfo.NodeType,
		"phase", cluster.Status.Phase,
	)

	if !cluster.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, cluster)
	}

	if ensureFinalizer(cluster, remoteClusterFinalizer) {
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		// Requeue immediately rather than waiting for a watch event.
		// GenerationChangedPredicate filters the Update event that adding a
		// finalizer produces (metadata-only changes do not increment generation),
		// so without an explicit requeue the controller would never reach
		// reconcileProvisioning after a brand-new resource is created.
		return ctrl.Result{Requeue: true}, nil
	}

	switch cluster.Status.Phase {
	case "", phaseProvisioning:
		return r.reconcileProvisioning(ctx, cluster)
	case phaseReady:
		if cluster.Spec.NodeInfo.NodeType == "control-plane" {
			if cluster.Annotations[annotationPkgVariantsCreated] == "true" {
				log.Info("Cluster fully ready — no action required")
				return ctrl.Result{}, nil
			}
			return r.reconcilePackageVariants(ctx, cluster)
		}
		return ctrl.Result{}, nil
	case phaseFailed:
		return r.reconcileProvisioning(ctx, cluster)
	default:
		return ctrl.Result{}, nil
	}
}

func (r *RemoteClusterReconciler) reconcileProvisioning(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues(
		"cluster", cluster.Name,
		"clusterName", cluster.Spec.ClusterName,
		"nodeType", cluster.Spec.NodeInfo.NodeType,
	)
	log.Info("Starting provisioning node for cluster")

	if err := r.setStatus(ctx, cluster, phaseProvisioning, "Provisioning", "Provisioning in progress", false); err != nil {
		log.Error(err, "Failed to update status to Provisioning — continuing")
	}

	switch cluster.Spec.NodeInfo.NodeType {
	case "control-plane":
		// Control-plane init is long-running SSH work (kubeadm init, CNI, etc.).
		// reconcileControlPlane manages its own SSH connection inside a background
		// goroutine so this reconcile call returns immediately.
		return r.reconcileControlPlane(ctx, cluster)
	case "worker":
		// Worker provisioning also does long-running SSH work; open a connection
		// here with a timeout and pass it down.
		sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
		defer cancel()

		sshClient, err := r.getSSHClient(sshCtx, cluster)
		if err != nil {
			return r.fail(ctx, cluster, "SSHConnectionFailed", fmt.Errorf("connecting via SSH to %s: %w", cluster.Spec.Host, err))
		}
		defer func() { _ = sshClient.Conn.Close() }()

		return r.reconcileWorker(sshCtx, cluster, sshClient)
	default:
		return r.fail(ctx, cluster, "UnknownNodeType", fmt.Errorf("unknown nodeType %q", cluster.Spec.NodeInfo.NodeType))
	}
}

// reconcileControlPlane manages background control-plane initialisation.
//
// kubeadm init + CNI setup is long-running SSH work (5–15 min).  Running it
// synchronously inside Reconcile would hold the single reconcile worker for
// the entire duration, blocking every other RemoteCluster resource.
//
// Instead this follows the same goroutine-per-resource pattern as
// reconcileOnPremProvisioning:
//
//   - If JoinCommand is already persisted the init already finished; skip
//     straight to PackageVariant creation.
//   - If no goroutine is in-flight: open a dedicated SSH connection, spawn
//     the goroutine, return RequeueAfter so the reconcile loop is free.
//   - If a goroutine is in-flight: non-blocking poll; requeue until done.
//
// The caller (reconcileProvisioning) must NOT pass an sshClient — the
// goroutine opens and owns its own connection.
func (r *RemoteClusterReconciler) reconcileControlPlane(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("cluster", cluster.Name, "clusterName", cluster.Spec.ClusterName)

	// Already done — nothing to init, move on.
	if cluster.Status.JoinCommand != "" {
		log.Info("Control plane already initialised; skipping kubeadm init")
		return ctrl.Result{RequeueAfter: repoReadyWait}, nil
	}

	key := cluster.Namespace + "/" + cluster.Name

	v, running := r.controlPlaneJobs.Load(key)
	if !running {
		// Spawn a fresh goroutine.  Open a dedicated SSH connection that the
		// goroutine owns for its entire lifetime.
		sshClient, err := r.getSSHClient(ctx, cluster)
		if err != nil {
			return r.fail(ctx, cluster, "SSHConnectionFailed",
				fmt.Errorf("connecting via SSH to %s: %w", cluster.Spec.Host, err))
		}

		clusterCopy := cluster.DeepCopy()

		ch := make(chan controlPlaneJobResult, 1)
		r.controlPlaneJobs.Store(key, (<-chan controlPlaneJobResult)(ch))

		go func() {
			defer sshClient.Conn.Close()
			joinCommand, err := kubeadm.InitializeControlPlane(sshClient, clusterCopy)
			ch <- controlPlaneJobResult{joinCommand: joinCommand, err: err}
		}()

		log.Info("Control plane init goroutine started")
		return ctrl.Result{RequeueAfter: controlPlanePollInterval}, nil
	}

	// Poll the result channel (non-blocking).
	ch := v.(<-chan controlPlaneJobResult)
	select {
	case res := <-ch:
		r.controlPlaneJobs.Delete(key)
		if res.err != nil {
			return r.fail(ctx, cluster, "ControlPlaneInitFailed",
				fmt.Errorf("initializing control plane: %w", res.err))
		}
		log.Info("Control plane init completed", "joinCommand", res.joinCommand != "")

		// Open a short-lived SSH connection for post-init steps (repo creation,
		// NodeProvisionNetConfig).  These are quick compared to init itself.
		sshClient, err := r.getSSHClient(ctx, cluster)
		if err != nil {
			return r.fail(ctx, cluster, "SSHConnectionFailed",
				fmt.Errorf("post-init SSH connection to %s: %w", cluster.Spec.Host, err))
		}
		defer sshClient.Conn.Close()

		if err := r.createClusterRepo(ctx, cluster); err != nil {
			return r.fail(ctx, cluster, "ClusterRepoFailed",
				fmt.Errorf("creating cluster repo: %w", err))
		}

		// Refresh to avoid resource-version conflicts before the status write.
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("refreshing cluster before status update: %w", err)
		}
		cluster.Status.JoinCommand = res.joinCommand
		if err := r.setStatus(ctx, cluster, phaseReady, "Provisioned", "Cluster provisioned successfully", false); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating status to Ready: %w", err)
		}

		if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cluster, sshClient, cluster.Spec.VPNConfig.IP, "create"); err != nil {
			return r.fail(ctx, cluster, "NodeProvisionNetConfigUpdateFailed",
				fmt.Errorf("updating NodeProvisionNetConfig with used IP: %w", err))
		}

		log.Info("Control plane provisioned; waiting for cluster repo before creating PackageVariants",
			"requeueAfter", repoReadyWait)
		return ctrl.Result{RequeueAfter: repoReadyWait}, nil

	default:
		log.Info("Control plane init in progress, requeueing")
		return ctrl.Result{RequeueAfter: controlPlanePollInterval}, nil
	}
}

func VPNRangeToCIDR(s string) string {

	ip := net.ParseIP(strings.TrimSpace(s))
	if ip == nil {
		return ""
	}

	ip = ip.To4()
	if ip == nil {
		return ""
	}

	mask := net.CIDRMask(24, 32)

	network := ip.Mask(mask)

	return fmt.Sprintf("%s/24", network.String())
}

func (r *RemoteClusterReconciler) reconcilePackageVariants(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("cluster", cluster.Name, "clusterName", cluster.Spec.ClusterName)
	log.Info("Creating PackageVariants")

	if err := r.createCorePackageVariants(ctx, cluster); err != nil {
		return r.fail(ctx, cluster, "CorePackageVariantsFailed", fmt.Errorf("creating core PackageVariants: %w", err))
	}

	if err := r.createOverlaysPlusPostInstallPackageVariants(ctx, cluster); err != nil {
		return r.fail(ctx, cluster, "OverlayPackageVariantsFailed", fmt.Errorf("creating overlay PackageVariants: %w", err))
	}

	ensureAnnotations(cluster)[annotationPkgVariantsCreated] = "true"
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("marking package-variants as created: %w", err)
	}

	log.Info("PackageVariants created; cluster is fully ready")
	return ctrl.Result{}, nil
}

func (r *RemoteClusterReconciler) reconcileWorker(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	sshClient *ssh.Client,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("cluster", cluster.Name, "clusterName", cluster.Spec.ClusterName)
	clusterParent, err := r.findControlPlane(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing RemoteClusters: %w", err)
	}
	if cluster.Annotations[annotationWorkerJoined] != "true" {

		if clusterParent == nil {
			log.Info("Control-plane not found yet; requeueing")
			return ctrl.Result{RequeueAfter: controlPlaneRetryInterval}, nil
		}

		if clusterParent.Status.Phase != phaseReady || clusterParent.Status.JoinCommand == "" {
			log.Info("Control-plane not ready yet; requeueing",
				"cpPhase", clusterParent.Status.Phase)
			return ctrl.Result{RequeueAfter: controlPlaneRetryInterval}, nil
		}

		sshClientCP, err := r.getSSHClient(ctx, clusterParent)
		if err != nil {
			return r.fail(ctx, cluster, "SSHConnectionFailed", fmt.Errorf("connecting to control-plane via SSH: %w", err))
		}
		defer func() { _ = sshClientCP.Conn.Close() }()

		// if err, nodeIP := kubeadm.JoinWorkerNode(sshClient, sshClientCP, cluster, clusterParent.Status.JoinCommand); err != nil {
		// 	return r.fail(ctx, cluster, "WorkerJoinFailed", fmt.Errorf("joining worker node to cluster: %w", err))
		// }

		err, nodeIP := kubeadm.JoinWorkerNode(
			sshClient,
			sshClientCP,
			cluster,
			clusterParent.Status.JoinCommand,
			clusterParent,
		)
		if err != nil {
			return r.fail(
				ctx,
				cluster,
				"WorkerJoinFailed",
				fmt.Errorf(
					"joining worker node to cluster: %w",
					err,
				),
			)
		}

		// Refresh, stamp the joined annotation, then update status — all in one pass.
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("refreshing cluster before status update: %w", err)
		}
		ensureAnnotations(cluster)[annotationWorkerJoined] = "true"
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("marking worker as joined: %w", err)
		}
		if err := r.setStatus(ctx, cluster, phaseReady, "WorkerJoined", "Worker node joined to cluster", false); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating worker status to Ready: %w", err)
		}
		log.Info("Worker node joined to cluster")

		if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cluster, sshClientCP, nodeIP, "update"); err != nil {
			return r.fail(ctx, cluster, "NodeProvisionNetConfigUpdateFailed", fmt.Errorf("updating NodeProvisionNetConfig with used IP: %w", err))
		}

	} else {
		log.Info("Worker already joined; skipping join step")
	}

	// ── GPU: driver installation → reboot → CDI generation ──────────────────
	if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		if cluster.Annotations[annotationNvidiaInstalled] != "true" {
			if err := kubeadm.InstallNvidiaDrivers(sshClient, cluster, clusterParent); err != nil {
				return r.fail(ctx, cluster, "NvidiaInstallFailed", fmt.Errorf("installing NVIDIA drivers on worker node: %w", err))
			}

			if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("refreshing cluster before marking NVIDIA installed: %w", err)
			}
			ensureAnnotations(cluster)[annotationNvidiaInstalled] = "true"
			if err := r.Update(ctx, cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("marking NVIDIA as installed: %w", err)
			}

			log.Info("NVIDIA drivers installed; rebooting node — will reconnect after postRebootWait",
				"wait", postRebootWait)
			// Reboot is best-effort; the SSH connection drops before the response arrives.
			if _, err := ssh.Run(sshClient, "sudo reboot"); err != nil {
				log.Info("Reboot command returned an error (expected — connection closes on reboot)", "err", err)
			}
			// Requeue after the reboot window so the controller reconnects to the
			// freshly booted node and continues with CDI generation + image prepull.
			return ctrl.Result{RequeueAfter: postRebootWait}, nil
		}

		if cluster.Annotations[annotationCDIGenerated] != "true" {
			// Drivers are installed and the node has rebooted — the kernel module
			// is now loaded, so CDI generation via NVML will succeed.
			if err := kubeadm.GenerateCDI(sshClient); err != nil {
				return r.fail(ctx, cluster, "CDIGenerateFailed", fmt.Errorf("generating CDI spec on worker node: %w", err))
			}

			if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("refreshing cluster before marking CDI generated: %w", err)
			}
			ensureAnnotations(cluster)[annotationCDIGenerated] = "true"
			if err := r.Update(ctx, cluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("marking CDI as generated: %w", err)
			}
			log.Info("CDI spec generated successfully — continuing to image pre-pull")
			// Explicit requeue: the annotation update does not change generation so
			// GenerationChangedPredicate would otherwise drop the watch event.
			return ctrl.Result{Requeue: true}, nil
		}

		// ── Image pre-pull (GPU nodes only) ──────────────────────────────────
		// Large GPU images (PyTorch, TensorFlow, etc.) are pulled in a background
		// goroutine so the reconcile loop is not blocked for the download duration.
		if cluster.Annotations[annotationImagesPrepulled] != "true" {
			return r.reconcileImagePrepull(ctx, cluster)
		}
	}

	return ctrl.Result{}, nil
}

// reconcileImagePrepull manages the background goroutine that pre-pulls GPU
// images via crictl on the worker node.
//
// On every call it either:
//   - Starts a new goroutine (first call, or after a controller restart) and
//     returns RequeueAfter so the controller polls back later.
//   - Polls the result channel of an already-running goroutine.  If not yet
//     done it requeues again; if done it stamps the annotation and returns.
//
// The goroutine opens its own SSH connection so the reconcile loop is free to
// return immediately — no blocking on large image downloads.
func (r *RemoteClusterReconciler) reconcileImagePrepull(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	key := cluster.Namespace + "/" + cluster.Name
	images := cluster.Spec.NodeInfo.SoftwareConfig.ImagePrepulls

	// Nothing to pull — mark done immediately.
	if len(images) == 0 {
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, err
		}
		ensureAnnotations(cluster)[annotationImagesPrepulled] = "true"
		return ctrl.Result{}, r.Update(ctx, cluster)
	}

	v, running := r.prepullJobs.Load(key)
	if !running {
		// No goroutine in memory — either first call or controller restarted while
		// a pull was in progress.  Open a dedicated SSH connection for the goroutine
		// (the caller's sshClient will be closed when the current reconcile returns).
		sshClient, err := r.getSSHClient(ctx, cluster)
		if err != nil {
			return r.fail(ctx, cluster, "SSHConnectionFailed",
				fmt.Errorf("opening SSH connection for image pre-pull: %w", err))
		}

		// Capture the logger and image list before returning from this goroutine.
		glog := log.WithValues("cluster", cluster.Name)
		imagesCopy := make([]string, len(images))
		copy(imagesCopy, images)

		ch := make(chan prepullJobResult, 1)
		// Store as receive-only so the poll branch cannot accidentally send.
		r.prepullJobs.Store(key, (<-chan prepullJobResult)(ch))

		go func() {
			defer sshClient.Conn.Close()
			// The map entry is intentionally NOT deleted here.  Deleting inside the
			// goroutine creates a race: the goroutine finishes and removes the entry
			// before the next poll consumes the channel result, causing the poll to
			// see no running job and restart unnecessarily.  Only the consumer
			// (the poll branch below) deletes the entry.
			for _, img := range imagesCopy {
				img = strings.TrimSpace(img)
				if img == "" {
					continue
				}
				output, pullErr := ssh.Run(sshClient, fmt.Sprintf("sudo crictl pull %s", img))
				if pullErr != nil {
					ch <- prepullJobResult{err: fmt.Errorf("pulling %s: %w\nOutput:\n%s", img, pullErr, output)}
					return
				}
				glog.Info("Pre-pulled image", "image", img)
			}
			ch <- prepullJobResult{err: nil}
		}()

		log.Info("Image pre-pull goroutine started", "images", len(imagesCopy))
		return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
	}

	// Goroutine is already running — non-blocking poll of the result channel.
	ch := v.(<-chan prepullJobResult)
	select {
	case res := <-ch:
		// Result available — consume it and remove the map entry.
		r.prepullJobs.Delete(key)
		if res.err != nil {
			return r.fail(ctx, cluster, "ImagePrepullFailed", res.err)
		}
		log.Info("All GPU images pre-pulled successfully")

		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("refreshing cluster after image pre-pull: %w", err)
		}
		ensureAnnotations(cluster)[annotationImagesPrepulled] = "true"
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("marking images as prepulled: %w", err)
		}
		return ctrl.Result{}, nil

	default:
		// Still running — come back later.
		log.Info("Image pre-pull in progress, requeueing", "pollInterval", prepullPollInterval)
		return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
	}
}

func (r *RemoteClusterReconciler) handleCreateUpdateNodeProvisionConfig(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	sshClient *ssh.Client,
	nodeIP,
	action string,
) (ctrl.Result, error) {

	log := logf.FromContext(ctx).WithValues(
		"cluster",
		cluster.Name,
	)

	// ============================================================
	// Resolve wg0 IP from remote node
	// ============================================================

	output, err := sshhelper.Run(
		sshClient,
		"ip -4 addr show wg0 | grep -oP '(?<=inet\\s)\\d+(\\.\\d+){3}'",
	)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf(
			"getting wg0 ip: %w",
			err,
		)
	}

	nodeIP = strings.TrimSpace(output)

	if nodeIP == "" {
		return ctrl.Result{}, fmt.Errorf(
			"empty wg0 ip",
		)
	}

	log.Info(
		"Resolved wg0 IP",
		"nodeIP",
		nodeIP,
	)

	// ============================================================
	// CREATE
	// ============================================================

	if action == "create" {

		vpnCIDR := VPNRangeToCIDR(nodeIP)

		// Ensure the VPN SSH credentials secret exists on the remote cluster so
		// the NodeProvisionNetConfig controller there can read it.
		if cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name != "" {
			vpnSecret := &corev1.Secret{}
			if err := r.Get(ctx, types.NamespacedName{
				Name:      cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name,
				Namespace: cluster.Spec.VPNConfig.VPNSSHCredentialsRef.NameSpace,
			}, vpnSecret); err != nil {
				return ctrl.Result{}, fmt.Errorf(
					"fetching VPN SSH credentials secret %q: %w",
					cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name,
					err,
				)
			}

			secretData := ""
			for k, v := range vpnSecret.Data {
				secretData += fmt.Sprintf("  %s: %s\n", k, base64.StdEncoding.EncodeToString(v))
			}
			secretYAML := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: %s
data:
%s`,
				vpnSecret.Name,
				cluster.Spec.VPNConfig.VPNSSHCredentialsRef.NameSpace,
				string(vpnSecret.Type),
				secretData,
			)

			secretCmd := fmt.Sprintf("cat <<'EOF' | kubectl apply -f -\n%s\nEOF", secretYAML)
			secretOutput, secretErr := sshhelper.Run(sshClient, secretCmd)
			if secretErr != nil {
				return ctrl.Result{}, fmt.Errorf(
					"creating VPN SSH credentials secret on remote cluster: %w\nOutput:\n%s",
					secretErr,
					secretOutput,
				)
			}
			log.Info("Ensured VPN SSH credentials secret on remote cluster", "secret", vpnSecret.Name)
		}

		// Build the imagePrepulls YAML block (indented to match softwareConfig children).
		var imagePrepullsYAML string
		if len(cluster.Spec.NodeInfo.SoftwareConfig.ImagePrepulls) > 0 {
			imagePrepullsYAML = "    imagePrepulls:\n"
			for _, img := range cluster.Spec.NodeInfo.SoftwareConfig.ImagePrepulls {
				imagePrepullsYAML += fmt.Sprintf("    - \"%s\"\n", img)
			}
		}

		netConfigYAML := fmt.Sprintf(`
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvisionNetConfig
metadata:
  name: %s-netconfig
  namespace: %s
spec:
  clusterName: %s
  softwareConfig:
    kubernetesVersion: "%s"
    nvidiaDriverVersion: "%s"
    nvidiaContainerToolkitVersion: "%s"
    k8sDevicePluginVersion: "%s"
%s  vpnRange: %s
  vpnServerPublicConfig:
    publicIP: %s
    vpnSshCredentialsRef:
      name: %s
      namespace: %s
`,
			cluster.Spec.ClusterName,
			cluster.Namespace,
			cluster.Spec.ClusterName,
			cluster.Spec.NodeInfo.SoftwareConfig.KubernetesVersion,
			cluster.Spec.NodeInfo.SoftwareConfig.NvidiaDriverVersion,
			cluster.Spec.NodeInfo.SoftwareConfig.NvidiaContainerToolkitVersion,
			cluster.Spec.NodeInfo.SoftwareConfig.K8sDevicePluginVersion,
			imagePrepullsYAML,
			vpnCIDR,
			cluster.Spec.VPNConfig.VPNServerPublicIP,
			cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name,
			cluster.Spec.VPNConfig.VPNSSHCredentialsRef.NameSpace,
		)

		// Apply the CRD first so the remote API server knows the full schema
		// (including any fields added since the cluster was originally set up,
		// e.g. imagePrepulls).  Applying before the resource ensures strict
		// decoding never rejects a field the schema hasn't seen yet.
		crdCmd := fmt.Sprintf("cat <<'CRDEOF' | kubectl apply -f -\n%s\nCRDEOF", nodeprovisionnetconfigCRD)
		if crdOut, crdErr := sshhelper.Run(sshClient, crdCmd); crdErr != nil {
			// Non-fatal: log and continue.  The worst case is the apply below
			// fails, which will be caught and surfaced as an error.
			log.Error(crdErr, "applying NodeProvisionNetConfig CRD on remote cluster (continuing)",
				"output", crdOut)
		} else {
			log.Info("Applied NodeProvisionNetConfig CRD on remote cluster")
		}

		cmd := fmt.Sprintf(
			"cat <<'EOF' | kubectl apply -f -\n%s\nEOF",
			netConfigYAML,
		)

		output, err := sshhelper.Run(sshClient, cmd)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf(
				"creating remote NodeProvisionNetConfig: %w\nOutput:\n%s",
				err,
				output,
			)
		}

		log.Info("Created NodeProvisionNetConfig remotely")

		// kubectl apply ignores the status subresource — patch it separately.
		joinCmdJSON, _ := json.Marshal(cluster.Status.JoinCommand)
		statusPatchCmd := fmt.Sprintf(
			`kubectl patch nodeprovisionnetconfig %s-netconfig -n %s --type=merge --subresource=status -p '{"status":{"clusterJoinCommand":%s,"usedIPAddresses":["%s"]}}'`,
			cluster.Spec.ClusterName,
			cluster.Namespace,
			string(joinCmdJSON),
			nodeIP,
		)
		statusOutput, statusErr := sshhelper.Run(sshClient, statusPatchCmd)
		if statusErr != nil {
			return ctrl.Result{}, fmt.Errorf(
				"patching NodeProvisionNetConfig status: %w\nOutput:\n%s",
				statusErr,
				statusOutput,
			)
		}

		log.Info("Patched NodeProvisionNetConfig status")
	}

	// ============================================================
	// UPDATE
	// ============================================================

	if action == "update" {

		patchCmd := fmt.Sprintf(`
kubectl patch nodeprovisionnetconfig %s-netconfig \
-n %s \
--type='json' \
-p='[
  {
    "op": "add",
    "path": "/status/usedIPAddresses/-",
    "value": "%s"
  }
]' --subresource=status
`,
			cluster.Spec.ClusterName,
			cluster.Namespace,
			nodeIP,
		)

		output, err := sshhelper.Run(
			sshClient,
			patchCmd,
		)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf(
				"patching remote NodeProvisionNetConfig: %w\nOutput:\n%s",
				err,
				output,
			)
		}

		log.Info(
			"Updated NodeProvisionNetConfig remotely",
			"nodeIP",
			nodeIP,
		)
	}

	return ctrl.Result{}, nil
}

// setStatus appends a new progress condition to the cluster status, preserving
// the full history of all steps (both successes and failures).
func (r *RemoteClusterReconciler) setStatus(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	phase, reason, message string,
	isError bool,
) error {
	cluster.Status.Phase = phase
	cluster.Status.Message = message

	condStatus := metav1.ConditionTrue
	if isError {
		condStatus = metav1.ConditionFalse
	}

	// Append rather than upsert so every step is recorded in order.
	cluster.Status.Conditions = append(cluster.Status.Conditions, metav1.Condition{
		Type:               reason,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	})

	return r.Status().Update(ctx, cluster)
}

func (r *RemoteClusterReconciler) fail(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	reason string,
	err error,
) (ctrl.Result, error) {
	logf.FromContext(ctx).Error(err, "RemoteCluster failed", "cluster", cluster.Name, "reason", reason)
	_ = r.setStatus(ctx, cluster, phaseFailed, reason, err.Error(), true)
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

// ensureFinalizer adds the finalizer if absent; returns true when it was added (caller must Update).
func ensureFinalizer(obj client.Object, finalizer string) bool {
	if controllerutil.ContainsFinalizer(obj, finalizer) {
		return false
	}
	controllerutil.AddFinalizer(obj, finalizer)
	return true
}

// ensureAnnotations initialises the annotation map if nil and returns it.
func ensureAnnotations(obj client.Object) map[string]string {
	if obj.GetAnnotations() == nil {
		obj.SetAnnotations(map[string]string{})
	}
	return obj.GetAnnotations()
}

// findControlPlane returns the control-plane RemoteCluster for the same clusterName,
// or nil if none is found (without error).
func (r *RemoteClusterReconciler) findControlPlane(ctx context.Context, cluster *infrav1.RemoteCluster) (*infrav1.RemoteCluster, error) {
	var list infrav1.RemoteClusterList
	if err := r.List(ctx, &list, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		rc := &list.Items[i]
		if rc.Spec.ClusterName == cluster.Spec.ClusterName && rc.Spec.NodeInfo.NodeType == "control-plane" {
			return rc, nil
		}
	}
	return nil, nil
}

func (r *RemoteClusterReconciler) getSSHClient(ctx context.Context, cluster *infrav1.RemoteCluster) (*ssh.Client, error) {
	var secretRef *infrav1.SecretKeyReference
	if cluster.Spec.Auth.SSHPrivateKeySecretRef != nil {
		secretRef = cluster.Spec.Auth.SSHPrivateKeySecretRef
	} else if cluster.Spec.Auth.PasswordSecretRef != nil {
		secretRef = cluster.Spec.Auth.PasswordSecretRef
	} else {
		return nil, fmt.Errorf("no SSH auth credentials configured in spec.auth")
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretRef.Name,
		Namespace: cluster.Namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("fetching SSH credential secret %q: %w", secretRef.Name, err)
	}

	credentialBytes, ok := secret.Data[secretRef.Key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in secret %q", secretRef.Key, secretRef.Name)
	}

	var host string
	if cluster.Spec.VPNConfig.IP != "" {
		host = cluster.Spec.VPNConfig.IP
	} else {
		host = cluster.Spec.Host
	}

	// VPNConfig is a struct (not a pointer) in the API; compare against its zero value.
	if !reflect.DeepEqual(cluster.Spec.VPNConfig, infrav1.VPNConfig{}) {
		// TODO: implement VPN-aware SSH connectivity (e.g., start tunnel) when needed.
	}

	credential := string(credentialBytes)
	var sshClient *ssh.Client
	var err error
	if strings.HasPrefix(strings.TrimSpace(credential), "-----BEGIN") {
		sshClient, err = ssh.ConnectWithPrivateKey(host, cluster.Spec.Port, cluster.Spec.User, credential)
	} else {
		sshClient, err = ssh.Connect(host, cluster.Spec.Port, cluster.Spec.User, credential)
	}
	if err != nil {
		return nil, fmt.Errorf("SSH connect to %s:%d: %w", host, cluster.Spec.Port, err)
	}
	return sshClient, nil
}

// createClusterRepo creates the Porch Repository, Nephio Repository, and access tokens on the
// management cluster when git integration is enabled.
func (r *RemoteClusterReconciler) createClusterRepo(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	if cluster.Spec.GitConfig.Enable != "true" {
		return nil
	}

	log := logf.FromContext(ctx)
	log.Info("Creating cluster repositories", "remotecluster", cluster.Name)

	labels := map[string]string{
		remoteClusterLabelKey: cluster.Spec.ClusterName,
	}
	secretRefName := cluster.Spec.ClusterName + "-access-token-porch"

	if err := r.ensurePorchRepository(ctx, cluster, labels, secretRefName); err != nil {
		return fmt.Errorf("ensuring porch repository: %w", err)
	}
	if err := r.ensureNephioRepository(ctx, cluster); err != nil {
		return fmt.Errorf("ensuring nephio repository: %w", err)
	}
	if err := r.ensureToken(ctx, cluster, labels, secretRefName); err != nil {
		return fmt.Errorf("ensuring porch access token: %w", err)
	}
	if err := r.ensureNephioToken(ctx, cluster, labels); err != nil {
		return fmt.Errorf("ensuring nephio configsync token: %w", err)
	}

	return nil
}

func (r *RemoteClusterReconciler) ensurePorchRepository(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	labels map[string]string,
	secretRefName string,
) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.porch.kpt.dev",
		Version: "v1alpha1",
		Kind:    "Repository",
	})
	obj.SetName(cluster.Spec.ClusterName)
	obj.SetNamespace(cluster.Namespace)
	obj.SetLabels(labels)

	err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	obj.Object["spec"] = map[string]interface{}{
		"content":    "Package",
		"deployment": true,
		"type":       "git",
		"git": map[string]interface{}{
			"repo":      cluster.Spec.GitConfig.GitServer + "/" + cluster.Spec.GitConfig.GitUsername + "/" + cluster.Spec.ClusterName + ".git",
			"branch":    "main",
			"directory": "/",
			"secretRef": map[string]interface{}{
				"name": secretRefName,
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, obj)
}

func (r *RemoteClusterReconciler) ensureNephioRepository(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "Repository",
	})
	obj.SetName(cluster.Spec.ClusterName)
	obj.SetNamespace(cluster.Namespace)

	err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	obj.Object["spec"] = map[string]interface{}{
		"description":   "Repository for " + cluster.Spec.ClusterName,
		"defaultBranch": "main",
	}
	return r.Create(ctx, obj)
}

func (r *RemoteClusterReconciler) ensureToken(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	labels map[string]string,
	name string,
) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "Token",
	})
	obj.SetName(name)
	obj.SetNamespace(cluster.Namespace)
	obj.SetLabels(labels)

	err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	obj.Object["spec"] = map[string]interface{}{}
	if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, obj)
}

func (r *RemoteClusterReconciler) ensureNephioToken(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	labels map[string]string,
) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "Token",
	})
	obj.SetName(cluster.Spec.ClusterName + "-access-token-configsync")
	obj.SetNamespace(cluster.Namespace)
	obj.SetLabels(labels)
	obj.SetAnnotations(map[string]string{
		"nephio.org/gitops":           "configsync",
		"nephio.org/app":              "tobeinstalledonremotecluster",
		"nephio.org/remote-namespace": "config-management-system",
	})

	err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	obj.Object["spec"] = map[string]interface{}{}
	if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, obj)
}

func (r *RemoteClusterReconciler) handleDelete(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Cleaning up resources for RemoteCluster", "remotecluster", cluster.Name)

	if err := r.deleteClusterResources(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(cluster, remoteClusterFinalizer)
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}

	// TODO: implement SSH-based kubeadm reset using cluster credentials.
	uninstallK8sRemoteCluster(ctx, cluster)

	return ctrl.Result{}, nil
}

// uninstallK8sRemoteCluster is a stub for running `kubeadm reset` on the remote node via SSH.
func uninstallK8sRemoteCluster(ctx context.Context, cluster *infrav1.RemoteCluster) {
	logf.FromContext(ctx).Info("Uninstalling Kubernetes on remote cluster via SSH", "host", cluster.Spec.Host)
}

func (r *RemoteClusterReconciler) deleteClusterResources(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	matchLabels := client.MatchingLabels{remoteClusterLabelKey: cluster.Spec.ClusterName}
	inNamespace := client.InNamespace(cluster.Namespace)

	for _, gvk := range []schema.GroupVersionKind{
		{Group: "config.porch.kpt.dev", Version: "v1alpha1", Kind: "RepositoryList"},
		{Group: "infra.nephio.org", Version: "v1alpha1", Kind: "TokenList"},
		{Group: "config.porch.kpt.dev", Version: "v1alpha1", Kind: "PackageVariantList"},
	} {
		if err := r.deleteUnstructuredList(ctx, gvk, matchLabels, inNamespace); err != nil {
			return err
		}
	}
	return nil
}

func (r *RemoteClusterReconciler) deleteUnstructuredList(ctx context.Context, gvk schema.GroupVersionKind, opts ...client.ListOption) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk)
	if err := r.List(ctx, list, opts...); err != nil {
		return err
	}
	for i := range list.Items {
		if err := r.Delete(ctx, &list.Items[i]); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// packageRef identifies an upstream or downstream package in a PackageVariant.
type packageRef struct {
	pkg      string
	repo     string
	revision string // only meaningful for upstream
}

// packageVariantSpec is a typed description of a PackageVariant to create or update.
type packageVariantSpec struct {
	name        string
	upstream    packageRef
	downstream  packageRef
	annotations map[string]interface{}
}

func (r *RemoteClusterReconciler) createCorePackageVariants(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx)
	log.Info("Creating Platform Core PackageVariants", "remotecluster", cluster.Name)

	variants := []packageVariantSpec{
		// {
		// 	name: "k8s-dra-driver-gpu-variant",
		// 	upstream: packageRef{
		// 		pkg:      "k8s-dra-driver-gpu",
		// 		repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
		// 		revision: cluster.Spec.GitConfig.PackageRevision,
		// 	},
		// 	downstream: packageRef{
		// 		pkg:  "k8s-dra-driver-gpu",
		// 		repo: cluster.Spec.ClusterName,
		// 	},
		// 	annotations: map[string]interface{}{
		// 		"approval.nephio.org/policy": "initial",
		// 	},
		// },
		// {
		// 	name: "gpu-operator-variant",
		// 	upstream: packageRef{
		// 		pkg:      "gpu-operator",
		// 		repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
		// 		revision: cluster.Spec.GitConfig.PackageRevision,
		// 	},
		// 	downstream: packageRef{
		// 		pkg:  "gpu-operator",
		// 		repo: cluster.Spec.ClusterName,
		// 	},
		// 	annotations: map[string]interface{}{
		// 		"approval.nephio.org/policy": "initial",
		// 	},
		// },

		{
			name: "keycloak-variant",
			upstream: packageRef{
				pkg:      "keycloak",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "keycloak",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},
		{
			name: "nfs-provisioner-variant",
			upstream: packageRef{
				pkg:      "nfs-provisioner",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "nfs-provisioner",
				repo: cluster.Spec.ClusterName,
			},
		},

		{
			name: "prometheus-stack-variant",
			upstream: packageRef{
				pkg:      "prometheus-stack",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "prometheus-stack",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},
		{
			name: "hami-variant",
			upstream: packageRef{
				pkg:      "hami",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "hami",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},
		{
			name: "hami-webui-variant",
			upstream: packageRef{
				pkg:      "hami-webui",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "hami-webui",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		{
			name: "enterprise-gateway-variant",
			upstream: packageRef{
				pkg:      "enterprise-gateway",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "enterprise-gateway",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		{
			name: "jupyter-hub-variant",
			upstream: packageRef{
				pkg:      "jupyter-hub",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "jupyter-hub",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		// Commented-out variants (re-enable as needed):
		// minio-variant, enterprise-gateway-variant, gpu-operator-variant,
		// harbor-variant, kai-scheduler-variant, keycloak-variant,
		// kubeflow-variant, kueue-variant, kyverno-variant, prometheus-stack-variant,
		// ml-platform-admin
	}

	return r.upsertPackageVariants(ctx, cluster, variants)
}

func (r *RemoteClusterReconciler) createOverlaysPlusPostInstallPackageVariants(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx)
	log.Info("Creating Platform Overlays and Post Install Config PackageVariants", "remotecluster", cluster.Name)

	// No active variants; pending re-enablement:
	// platform-overlays-variant, post-install-config-variant
	variantsOverlays := []packageVariantSpec{
		// {
		// 	name: "k8s-dra-driver-gpu-variant",
		// 	upstream: packageRef{
		// 		pkg:      "k8s-dra-driver-gpu",
		// 		repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
		// 		revision: cluster.Spec.GitConfig.PackageRevision,
		// 	},
		// 	downstream: packageRef{
		// 		pkg:  "k8s-dra-driver-gpu",
		// 		repo: cluster.Spec.ClusterName,
		// 	},
		// 	annotations: map[string]interface{}{
		// 		"approval.nephio.org/policy": "initial",
		// 	},
		// },
		// {
		// 	name: "gpu-operator-variant",
		// 	upstream: packageRef{
		// 		pkg:      "gpu-operator",
		// 		repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
		// 		revision: cluster.Spec.GitConfig.PackageRevision,
		// 	},
		// 	downstream: packageRef{
		// 		pkg:  "gpu-operator",
		// 		repo: cluster.Spec.ClusterName,
		// 	},
		// 	annotations: map[string]interface{}{
		// 		"approval.nephio.org/policy": "initial",
		// 	},
		// },

		{
			name: "services-overlays-variant",
			upstream: packageRef{
				pkg:      "services-overlays",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "services-overlays",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		// Commented-out variants (re-enable as needed):
		// minio-variant, enterprise-gateway-variant, gpu-operator-variant,
		// harbor-variant, kai-scheduler-variant, keycloak-variant,
		// kubeflow-variant, kueue-variant, kyverno-variant, prometheus-stack-variant,
		// ml-platform-admin
	}

	return r.upsertPackageVariants(ctx, cluster, variantsOverlays)
}

// upsertPackageVariants creates or updates each PackageVariant in the default namespace.
func (r *RemoteClusterReconciler) upsertPackageVariants(ctx context.Context, cluster *infrav1.RemoteCluster, variants []packageVariantSpec) error {
	labels := map[string]string{
		remoteClusterLabelKey: cluster.Spec.ClusterName,
	}

	for _, v := range variants {
		spec := map[string]interface{}{
			"upstream": map[string]interface{}{
				"package":  v.upstream.pkg,
				"repo":     v.upstream.repo,
				"revision": v.upstream.revision,
			},
			"downstream": map[string]interface{}{
				"package": v.downstream.pkg,
				"repo":    v.downstream.repo,
			},
		}
		if len(v.annotations) > 0 {
			spec["annotations"] = v.annotations
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(packageVariantGVK)
		obj.SetName(v.name)
		obj.SetNamespace("default")
		obj.SetLabels(labels)
		obj.Object["spec"] = spec

		if err := r.Client.Create(ctx, obj); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating PackageVariant %q: %w", v.name, err)
			}

			existing := &unstructured.Unstructured{}
			existing.SetGroupVersionKind(packageVariantGVK)
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(obj), existing); err != nil {
				return fmt.Errorf("fetching existing PackageVariant %q: %w", v.name, err)
			}
			existing.Object["spec"] = spec
			if err := r.Client.Update(ctx, existing); err != nil {
				return fmt.Errorf("updating PackageVariant %q: %w", v.name, err)
			}
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RemoteClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1.RemoteCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("remotecluster").
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(r)
}
