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
	"strconv"
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

	// controlPlaneProgress tracks the last phase index completed by an in-flight
	// control-plane goroutine.  Key: "namespace/name", Value: int.
	// The reconcile loop reads this when the goroutine finishes and persists it
	// to the CR annotation so retries can resume from the right phase.
	controlPlaneProgress sync.Map
}

const (
	remoteClusterFinalizer = "infra.dcn.ssu.ac.kr/remotecluster-finalizer"
	remoteClusterLabelKey  = "infra.dcn.ssu.ac.kr/remotecluster"

	// authSecretFinalizer is placed on the SSH credential secret referenced by
	// spec.auth so that it cannot be deleted while the RemoteCluster exists.
	// It is removed at the end of handleDelete, after all SSH cleanup is done.
	authSecretFinalizer = "infra.dcn.ssu.ac.kr/remotecluster-ssh-auth"

	// vpnSecretFinalizer is placed on the VPN SSH credential secret referenced by
	// spec.vpnConfig.vpnSSHCredentialsRef so that it cannot be deleted while the
	// RemoteCluster exists.  It is removed at the end of handleDelete, after the
	// VPN peer removal step has completed.
	vpnSecretFinalizer = "infra.dcn.ssu.ac.kr/remotecluster-vpn-ssh-auth"

	// annotationPkgVariantsCreated marks that PackageVariants have been successfully
	// created for this control-plane cluster, so they are not re-created on every reconcile.
	annotationPkgVariantsCreated = "infra.dcn.ssu.ac.kr/package-variants-created"
	// annotationWorkerJoined marks that this worker has already successfully joined its cluster.
	annotationWorkerJoined = "infra.dcn.ssu.ac.kr/worker-joined"
	// annotationImagesPrepulled marks that all images in spec.nodeInfo.softwareConfig.imagePrepulls
	// have been successfully pulled on the worker node.
	annotationImagesPrepulled = "infra.dcn.ssu.ac.kr/images-prepulled"

	// annotationLastCompletedPhaseCP / Worker persist the last successfully completed
	// provision phase index so that retries can resume from the failed phase rather
	// than restarting from scratch.  Value is a decimal integer; -1 means nothing yet.
	annotationLastCompletedPhaseCP     = "infra.dcn.ssu.ac.kr/last-completed-phase-cp"
	annotationLastCompletedPhaseWorker = "infra.dcn.ssu.ac.kr/last-completed-phase-worker"

	// annotationCPInitComplete is set to "true" once InitializeControlPlane succeeds
	// and the join command is cached in annotationJoinCmdCache.  If the subsequent
	// post-init steps (createClusterRepo, setStatus) fail, the next reconcile detects
	// this annotation and skips re-running kubeadm, retrying only the post-init work.
	annotationCPInitComplete = "infra.dcn.ssu.ac.kr/cp-init-complete"
	// annotationJoinCmdCache holds the kubeadm join command between the goroutine
	// completing and it being written to cluster.Status.JoinCommand.
	annotationJoinCmdCache = "infra.dcn.ssu.ac.kr/join-cmd-cache"
	// annotationNodeProvisionCreated is set to "true" after handleCreateUpdateNodeProvisionConfig
	// succeeds for the control-plane.  The phaseReady reconcile path checks this and
	// re-runs the step if missing, recovering clusters that reached Ready before the
	// NodeProvisionNetConfig was created.
	annotationNodeProvisionCreated = "infra.dcn.ssu.ac.kr/node-provision-created"

	phaseProvisioning = "Provisioning"
	phaseReady        = "Ready"
	phaseFailed       = "Failed"

	// controllerAuthSuffix is appended to the RemoteCluster name to form the
	// controller-owned copy of the SSH credential secret.  An owner reference
	// ties it to the CR so it is GC'd automatically after the finalizer is
	// removed.  During deletion the controller falls back to this copy when
	// the user-managed secret has already been deleted.
	controllerAuthSuffix = "-controller-auth"

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

	// Protect the SSH credential secret with a finalizer so it cannot be
	// deleted while this RemoteCluster exists.  Also keep a controller-owned
	// copy as a second layer of defence (e.g. in case the finalizer was
	// somehow bypassed on an existing cluster).
	if authSecret, err := r.getAuthSecret(ctx, cluster); err == nil {
		if err := r.ensureAuthSecretFinalizer(ctx, cluster, authSecret); err != nil {
			log.Error(err, "adding finalizer to SSH credential secret (non-fatal)")
		}
		if err := r.ensureControllerAuthSecret(ctx, cluster, authSecret); err != nil {
			log.Error(err, "persisting SSH credential copy (non-fatal)")
		}
	}

	// Protect the VPN SSH credential secret the same way.
	if cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name != "" {
		if vpnSecret, err := r.getVPNSecret(ctx, cluster); err == nil {
			if err := r.ensureVPNSecretFinalizer(ctx, cluster, vpnSecret); err != nil {
				log.Error(err, "adding finalizer to VPN SSH credential secret (non-fatal)")
			}
		}
	}

	switch cluster.Status.Phase {
	case "", phaseProvisioning:
		return r.reconcileProvisioning(ctx, cluster)
	case phaseReady:
		if cluster.Spec.NodeInfo.NodeType == "control-plane" {
			// If NodeProvisionNetConfig was not created (e.g. it failed after setStatus
			// already flipped to Ready in a previous run), re-run it now via SSH before
			// proceeding to PackageVariants.
			if cluster.Annotations[annotationNodeProvisionCreated] != "true" {
				log.Info("NodeProvisionNetConfig not yet created; running now")
				sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
				defer cancel()
				sshClient, err := r.getSSHClient(sshCtx, cluster)
				if err != nil {
					return r.fail(ctx, cluster, "SSHConnectionFailed",
						fmt.Errorf("SSH for NodeProvisionNetConfig: %w", err))
				}
				defer func() { _ = sshClient.Conn.Close() }()
				if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cluster, cluster, sshClient, cluster.Spec.VPNConfig.IP, "create"); err != nil {
					return r.fail(ctx, cluster, "NodeProvisionNetConfigUpdateFailed",
						fmt.Errorf("creating NodeProvisionNetConfig: %w", err))
				}
				if patchErr := r.patchAnnotation(ctx, cluster, annotationNodeProvisionCreated, "true"); patchErr != nil {
					log.Error(patchErr, "Failed to stamp node-provision-created annotation")
				}
			}
			if cluster.Annotations[annotationPkgVariantsCreated] == "true" {
				log.Info("Cluster fully ready — no action required")
				return ctrl.Result{}, nil
			}
			return r.reconcilePackageVariants(ctx, cluster)
		}
		// GPU worker: image pre-pull runs after join. Driver and toolkit are
		// managed by GPU Operator; the provisioner only pre-pulls large GPU images.
		if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") &&
			cluster.Annotations[annotationImagesPrepulled] != "true" {
			// If an image pre-pull goroutine is already running, poll its result
			// directly instead of opening a new SSH connection.
			key := cluster.Namespace + "/" + cluster.Name
			if _, running := r.prepullJobs.Load(key); running {
				return r.reconcileImagePrepull(ctx, cluster, cluster)
			}
			sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
			defer cancel()
			sshClient, err := r.getSSHClient(sshCtx, cluster)
			if err != nil {
				log.Info("SSH not yet reachable for GPU image pre-pull — retrying", "err", err)
				return ctrl.Result{RequeueAfter: postRebootWait}, nil
			}
			defer func() { _ = sshClient.Conn.Close() }()
			return r.reconcileWorker(sshCtx, cluster, sshClient)
		}
		if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
			log.Info("GPU worker fully ready — image pre-pull already complete")
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

	// If InitializeControlPlane already succeeded but the post-init steps (createClusterRepo,
	// setStatus) failed, skip re-running kubeadm and only retry those steps.
	// The join command is cached in annotationJoinCmdCache while JoinCommand is not yet
	// in the status (it is only written there after ALL post-init steps succeed).
	if cluster.Annotations[annotationCPInitComplete] == "true" && cluster.Status.JoinCommand == "" {
		log.Info("Control plane kubeadm init already done; retrying post-init steps only")
		return r.completeControlPlane(ctx, cluster, cluster.Annotations[annotationJoinCmdCache])
	}

	key := cluster.Namespace + "/" + cluster.Name

	v, running := r.controlPlaneJobs.Load(key)
	if !running {
		// Determine which phase to start from.  On the first attempt the annotation
		// is absent so startPhase is 0 (full run).  On a retry after failure the
		// annotation holds the last completed phase index and we start from the next one.
		startPhase := 0
		if s, ok := cluster.Annotations[annotationLastCompletedPhaseCP]; ok {
			if n, err := strconv.Atoi(s); err == nil && n >= 0 {
				startPhase = n + 1
				log.Info("Resuming control-plane init from phase", "startPhase", startPhase)
			}
		}

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
			joinCommand, err := kubeadm.InitializeControlPlane(sshClient, clusterCopy, startPhase, func(phaseIdx int) {
				r.controlPlaneProgress.Store(key, phaseIdx)
			})
			ch <- controlPlaneJobResult{joinCommand: joinCommand, err: err}
		}()

		log.Info("Control plane init goroutine started", "startPhase", startPhase)
		return ctrl.Result{RequeueAfter: controlPlanePollInterval}, nil
	}

	// Poll the result channel (non-blocking).
	ch := v.(<-chan controlPlaneJobResult)
	select {
	case res := <-ch:
		r.controlPlaneJobs.Delete(key)

		// Persist the last completed phase to the annotation regardless of
		// success/failure so the next reconcile can resume from there.
		if lastPhase, ok := r.controlPlaneProgress.LoadAndDelete(key); ok {
			phaseStr := strconv.Itoa(lastPhase.(int))
			if patchErr := r.patchAnnotation(ctx, cluster, annotationLastCompletedPhaseCP, phaseStr); patchErr != nil {
				log.Error(patchErr, "Failed to persist control-plane phase progress")
			}
		}

		if res.err != nil {
			return r.fail(ctx, cluster, "ControlPlaneInitFailed",
				fmt.Errorf("initializing control plane: %w", res.err))
		}

		log.Info("Control plane init completed", "joinCommand", res.joinCommand != "")

		// Persist init-complete + join command BEFORE attempting post-init steps.
		// If createClusterRepo or setStatus fails below, the next reconcile detects
		// annotationCPInitComplete and retries only the post-init steps, not kubeadm.
		if patchErr := r.patchAnnotations(ctx, cluster, map[string]string{
			annotationLastCompletedPhaseCP: "-1",
			annotationCPInitComplete:       "true",
			annotationJoinCmdCache:         res.joinCommand,
		}); patchErr != nil {
			log.Error(patchErr, "Failed to persist cp-init-complete annotation")
		}

		return r.completeControlPlane(ctx, cluster, res.joinCommand)

	default:
		// Goroutine still running.  Flush any phase progress it has accumulated
		// to the CR annotation so that a controller restart can resume from the
		// right phase rather than re-running everything from scratch.
		if lastPhase, ok := r.controlPlaneProgress.Load(key); ok {
			phaseStr := strconv.Itoa(lastPhase.(int))
			if current := cluster.Annotations[annotationLastCompletedPhaseCP]; current != phaseStr {
				if patchErr := r.patchAnnotation(ctx, cluster, annotationLastCompletedPhaseCP, phaseStr); patchErr != nil {
					log.Error(patchErr, "Failed to flush control-plane phase progress during poll")
				}
			}
		}
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

	// delay to allow Porch to sync the new cluster repo before creating overlay PackageVariants
	// (otherwise Porch will fail to find the overlay packages in the new repo)
	// sleep will block the reconcile loop, but this is a one-time delay and the cluster is already in Ready phase
	log.Info("Waiting for Porch to sync the new cluster repo before creating overlay PackageVariants",
		"duration", 30*time.Second)
	time.Sleep(30 * time.Second)

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

	// Sync VPN server config from the control-plane onto the worker CR so that
	// handleDelete can call removeVPNPeer using only cluster.Spec.VPNConfig.
	// Worker CRs carry the node's own VPN IP but not the server credentials.
	if clusterParent != nil &&
		cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name == "" &&
		clusterParent.Spec.VPNConfig.VPNSSHCredentialsRef.Name != "" {
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("refreshing worker before VPN config sync: %w", err)
		}
		cluster.Spec.VPNConfig.VPNServerPublicIP = clusterParent.Spec.VPNConfig.VPNServerPublicIP
		cluster.Spec.VPNConfig.VPNServerSSHPort = clusterParent.Spec.VPNConfig.VPNServerSSHPort
		cluster.Spec.VPNConfig.VPNServerSSHUsername = clusterParent.Spec.VPNConfig.VPNServerSSHUsername
		cluster.Spec.VPNConfig.VPNSSHCredentialsRef = clusterParent.Spec.VPNConfig.VPNSSHCredentialsRef
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("syncing VPN server config from control-plane: %w", err)
		}
		log.Info("Synced VPN server config from control-plane", "cp", clusterParent.Name)
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

		// Determine which phase to start from for this worker.
		// On the first attempt the annotation is absent → startPhase=0 (full run).
		// On retry after failure the annotation holds the last completed phase index.
		workerStartPhase := 0
		if s, ok := cluster.Annotations[annotationLastCompletedPhaseWorker]; ok {
			if n, parseErr := strconv.Atoi(s); parseErr == nil && n >= 0 {
				workerStartPhase = n + 1
				log.Info("Resuming worker join from phase", "startPhase", workerStartPhase)
			}
		}

		// Progress callback: persists the last completed phase to the CR annotation
		// after each phase so a retry can skip already-done work.
		onWorkerPhaseComplete := func(phaseIdx int) {
			if patchErr := r.patchAnnotation(ctx, cluster, annotationLastCompletedPhaseWorker, strconv.Itoa(phaseIdx)); patchErr != nil {
				log.Error(patchErr, "Failed to persist worker phase progress", "phase", phaseIdx)
			}
		}

		err, nodeIP := kubeadm.JoinWorkerNode(
			sshClient,
			sshClientCP,
			cluster,
			clusterParent.Status.JoinCommand,
			clusterParent,
			workerStartPhase,
			onWorkerPhaseComplete,
		)
		if err != nil {
			return r.fail(
				ctx,
				cluster,
				"WorkerJoinFailed",
				fmt.Errorf("joining worker node to cluster: %w", err),
			)
		}

		// Refresh, stamp the joined annotation, clear the phase-resume annotation,
		// then update status — all in one pass.
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("refreshing cluster before status update: %w", err)
		}
		anns := ensureAnnotations(cluster)
		anns[annotationWorkerJoined] = "true"
		anns[annotationLastCompletedPhaseWorker] = "-1" // clear so a future reprovision starts fresh
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("marking worker as joined: %w", err)
		}
		if err := r.setStatus(ctx, cluster, phaseReady, "WorkerJoined", "Worker node joined to cluster", false); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating worker status to Ready: %w", err)
		}
		log.Info("Worker node joined to cluster")

		if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cluster, clusterParent, sshClientCP, nodeIP, "update"); err != nil {
			return r.fail(ctx, cluster, "NodeProvisionNetConfigUpdateFailed", fmt.Errorf("updating NodeProvisionNetConfig with used IP: %w", err))
		}

	} else {
		log.Info("Worker already joined; skipping join step")
	}

	// ── GPU: image pre-pull ──────────────────────────────────────────────────
	// The NVIDIA driver and container toolkit are managed entirely by GPU Operator
	// (driver daemonset + toolkit daemonset with CDI mode). The provisioner only
	// needs to pre-pull large GPU images; CRI-O was already configured for CDI
	// in Phase 6 before the node joined.
	if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		if clusterParent == nil {
			log.Error(fmt.Errorf("control-plane not found"), "Cannot proceed with GPU image pre-pull — control-plane RemoteCluster missing; requeueing")
			return ctrl.Result{RequeueAfter: controlPlaneRetryInterval}, nil
		}

		// ── Image pre-pull (GPU nodes only) ──────────────────────────────────
		// Large GPU images (PyTorch, TensorFlow, etc.) are pulled in a background
		// goroutine so the reconcile loop is not blocked for the download duration.
		if cluster.Annotations[annotationImagesPrepulled] != "true" {
			return r.reconcileImagePrepull(ctx, cluster, clusterParent)
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
	parentCluster *infrav1.RemoteCluster,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	key := cluster.Namespace + "/" + cluster.Name

	// Poll branch — check first so we never do credential lookups or SSH on
	// every requeue while the goroutine is busy pulling large images.
	if v, running := r.prepullJobs.Load(key); running {
		ch := v.(<-chan prepullJobResult)
		select {
		case res := <-ch:
			r.prepullJobs.Delete(key)
			if res.err != nil {
				// Do NOT transition to phaseFailed — the node is already joined
				// and functional.  Log the error and let the next reconcile
				// re-spawn the goroutine for a fresh attempt.
				log.Error(res.err, "Image pre-pull attempt failed, will retry on next reconcile")
				return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
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
			log.V(1).Info("Image pre-pull in progress, requeueing")
			return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
		}
	}

	images := parentCluster.Spec.NodeInfo.SoftwareConfig.ImagePrepulls

	// Nothing to pull — mark done immediately.
	if len(images) == 0 {
		log.Info("No images configured for pre-pull — skipping")
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, err
		}
		ensureAnnotations(cluster)[annotationImagesPrepulled] = "true"
		return ctrl.Result{}, r.Update(ctx, cluster)
	}

	// Resolve registry credentials if a pull secret is referenced.
	// Only runs once — when the goroutine is first spawned.
	var pullCreds string
	if ref := parentCluster.Spec.NodeInfo.SoftwareConfig.ImagePullSecretRef; ref != nil {
		credSecret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      ref.Name,
			Namespace: cluster.Namespace,
		}, credSecret); err != nil {
			// Treat as transient — do not fail the cluster.  The secret may not
			// exist yet or the API server may be temporarily unavailable.
			log.Error(err, "Cannot fetch image pull secret, will retry", "secret", ref.Name)
			return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
		}
		username := strings.TrimSpace(string(credSecret.Data["username"]))
		password := strings.TrimSpace(string(credSecret.Data["password"]))
		if username == "" || password == "" {
			log.Error(fmt.Errorf("missing keys"), "Image pull secret must have non-empty \"username\" and \"password\" keys — will retry", "secret", ref.Name)
			return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
		}
		pullCreds = username + ":" + password
		log.Info("Using registry credentials for image pre-pull", "secret", ref.Name, "user", username)
	}

	// No goroutine in memory — spawn one.
	{
		// No goroutine in memory — either first call or controller restarted while
		// a pull was in progress.  Open a dedicated SSH connection for the goroutine
		// (the caller's sshClient will be closed when the current reconcile returns).
		sshClient, err := r.getSSHClient(ctx, cluster)
		if err != nil {
			// SSH failure is transient — do not fail the cluster.  The next
			// reconcile (after prepullPollInterval) will try again.
			log.Error(err, "Cannot open SSH connection for image pre-pull, will retry")
			return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
		}

		// Capture the logger, image list, and credentials before spawning.
		glog := log.WithValues("cluster", cluster.Name)
		imagesCopy := make([]string, len(images))
		copy(imagesCopy, images)
		credsCopy := pullCreds // immutable string — safe to close over

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

			// Configure registry credentials if provided
			if credsCopy != "" {
				// Write Docker auth config for crictl to use
				parts := strings.Split(credsCopy, ":")
				if len(parts) == 2 {
					username, password := parts[0], parts[1]
					// Create auth.json in proper format for crictl
					authJSON := fmt.Sprintf(`{"auths":{"docker.io":{"username":"%s","password":"%s"}}}`,
						strings.ReplaceAll(username, "\"", "\\\""),
						strings.ReplaceAll(password, "\"", "\\\""))
					cmd := fmt.Sprintf("echo '%s' | sudo tee /etc/containers/auth.json > /dev/null && sudo chmod 0600 /etc/containers/auth.json",
						strings.ReplaceAll(authJSON, "'", "'\\''"))
					_, _ = ssh.Run(sshClient, cmd)
				}
			} else {
				// Clear stale registry auth files when no credentials configured
				authFiles := []string{
					"/root/.docker/config.json",
					"/run/containers/0/auth.json",
					"/etc/containers/auth.json",
				}
				for _, f := range authFiles {
					_, _ = ssh.Run(sshClient, fmt.Sprintf("sudo rm -f %s", f))
				}
				glog.Info("Cleared stale registry auth files before anonymous pull")
			}

			// Wait for CRI-O socket to be ready before pulling — the socket may not
			// exist yet if CRI-O just started after a binary swap or storage wipe.
			waitCmd := `for i in $(seq 1 60); do [ -S /var/run/crio/crio.sock ] && break; echo "waiting for crio socket ($i/60)..."; sleep 3; done; [ -S /var/run/crio/crio.sock ] || { sudo systemctl start crio; sleep 5; }`
			if out, err := ssh.Run(sshClient, waitCmd); err != nil {
				ch <- prepullJobResult{err: fmt.Errorf("waiting for CRI-O socket: %w\nOutput:\n%s", err, out)}
				return
			}

			for _, img := range imagesCopy {
				img = strings.TrimSpace(img)
				if img == "" {
					continue
				}
				glog.Info("Pulling image", "image", img)
				// Use full path to crictl since sudo may not have the same PATH.
				// Try /usr/local/bin first (from fallback install), then /usr/bin.
				crictl := `CRICTL=$(command -v crictl 2>/dev/null || echo /usr/local/bin/crictl); [ -x "$CRICTL" ] || CRICTL=/usr/bin/crictl; sudo timeout 7200 "$CRICTL"`
				// Credentials are configured via /etc/containers/auth.json if needed
				// crictl will automatically use it for authentication
				cmd := fmt.Sprintf("%s pull %s", crictl, img)
				output, pullErr := ssh.Run(sshClient, cmd)
				if pullErr != nil {
					ch <- prepullJobResult{err: fmt.Errorf("pulling %s: %w\nOutput:\n%s", img, pullErr, output)}
					return
				}
				glog.Info("Pulled image successfully", "image", img)
			}
			ch <- prepullJobResult{err: nil}
		}()

		log.Info("Image pre-pull goroutine started", "images", len(imagesCopy), "authenticated", pullCreds != "")
		return ctrl.Result{RequeueAfter: prepullPollInterval}, nil
	}
}

func (r *RemoteClusterReconciler) handleCreateUpdateNodeProvisionConfig(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	clusterParent *infrav1.RemoteCluster,
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

		// Ensure the image pull secret exists on the remote cluster so the
		// NodeProvision controller there can authenticate when pre-pulling images.
		if ref := clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePullSecretRef; ref != nil {
			pullSecret := &corev1.Secret{}
			if err := r.Get(ctx, types.NamespacedName{
				Name:      ref.Name,
				Namespace: cluster.Namespace,
			}, pullSecret); err != nil {
				return ctrl.Result{}, fmt.Errorf(
					"fetching image pull secret %q: %w",
					ref.Name, err,
				)
			}

			secretData := ""
			for k, v := range pullSecret.Data {
				secretData += fmt.Sprintf("  %s: %s\n", k, base64.StdEncoding.EncodeToString(v))
			}
			pullSecretYAML := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: %s
data:
%s`,
				pullSecret.Name,
				cluster.Namespace,
				string(pullSecret.Type),
				secretData,
			)

			pullSecretCmd := fmt.Sprintf("cat <<'EOF' | kubectl apply -f -\n%s\nEOF", pullSecretYAML)
			pullSecretOutput, pullSecretErr := sshhelper.Run(sshClient, pullSecretCmd)
			if pullSecretErr != nil {
				return ctrl.Result{}, fmt.Errorf(
					"creating image pull secret on remote cluster: %w\nOutput:\n%s",
					pullSecretErr, pullSecretOutput,
				)
			}
			log.Info("Ensured image pull secret on remote cluster", "secret", pullSecret.Name)
		}

		// Build optional softwareConfig fields (indented to match sibling keys).
		var imagePrepullsYAML string
		if len(clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePrepulls) > 0 {
			imagePrepullsYAML = "    imagePrepulls:\n"
			for _, img := range clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePrepulls {
				imagePrepullsYAML += fmt.Sprintf("    - \"%s\"\n", img)
			}
		}
		if ref := clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePullSecretRef; ref != nil {
			imagePrepullsYAML += fmt.Sprintf("    imagePullSecretRef:\n      name: \"%s\"\n", ref.Name)
		}

		vpnServerSSHPort := cluster.Spec.VPNConfig.VPNServerSSHPort
		if vpnServerSSHPort == 0 {
			vpnServerSSHPort = 22
		}
		vpnServerSSHUsername := cluster.Spec.VPNConfig.VPNServerSSHUsername
		if vpnServerSSHUsername == "" {
			vpnServerSSHUsername = "ubuntu"
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
    sshPort: %d
    sshUsername: %s
    vpnSshCredentialsRef:
      name: %s
      namespace: %s
`,
			cluster.Spec.ClusterName,
			cluster.Namespace,
			cluster.Spec.ClusterName,
			clusterParent.Spec.NodeInfo.SoftwareConfig.KubernetesVersion,
			clusterParent.Spec.NodeInfo.SoftwareConfig.NvidiaDriverVersion,
			clusterParent.Spec.NodeInfo.SoftwareConfig.NvidiaContainerToolkitVersion,
			clusterParent.Spec.NodeInfo.SoftwareConfig.K8sDevicePluginVersion,
			imagePrepullsYAML,
			vpnCIDR,
			cluster.Spec.VPNConfig.VPNServerPublicIP,
			vpnServerSSHPort,
			vpnServerSSHUsername,
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

// completeControlPlane runs the post-kubeadm steps: createClusterRepo, status update,
// and NodeProvisionNetConfig.  It is called both when the goroutine first completes and
// when the controller restarts after those steps previously failed (detected via
// annotationCPInitComplete == "true").
func (r *RemoteClusterReconciler) completeControlPlane(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	joinCommand string,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("cluster", cluster.Name)

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

	// Run NodeProvisionNetConfig BEFORE flipping phase to Ready so that if it
	// fails, r.fail() can still override the phase (no stale-resourceVersion race).
	if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cluster, cluster, sshClient, cluster.Spec.VPNConfig.IP, "create"); err != nil {
		return r.fail(ctx, cluster, "NodeProvisionNetConfigUpdateFailed",
			fmt.Errorf("creating NodeProvisionNetConfig: %w", err))
	}
	if patchErr := r.patchAnnotation(ctx, cluster, annotationNodeProvisionCreated, "true"); patchErr != nil {
		log.Error(patchErr, "Failed to stamp node-provision-created annotation")
	}

	// All side-effects done — now flip to Ready and persist the join command.
	// Refresh to avoid resource-version conflicts before the status write.
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("refreshing cluster before status update: %w", err)
	}
	cluster.Status.JoinCommand = joinCommand
	if err := r.setStatus(ctx, cluster, phaseReady, "Provisioned", "Cluster provisioned successfully", false); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status to Ready: %w", err)
	}

	// All post-init steps succeeded — clear the init-complete flag so that
	// a future full reprovision is not confused into thinking init is done.
	if patchErr := r.patchAnnotations(ctx, cluster, map[string]string{
		annotationCPInitComplete: "",
		annotationJoinCmdCache:   "",
	}); patchErr != nil {
		log.Error(patchErr, "Failed to clear cp-init-complete annotation")
	}

	log.Info("Control plane provisioned; waiting for cluster repo before creating PackageVariants",
		"requeueAfter", repoReadyWait)
	return ctrl.Result{RequeueAfter: repoReadyWait}, nil
}

// patchAnnotation sets a single annotation on the cluster object using a merge patch.
// It re-fetches the latest resource version before patching to avoid conflicts.
func (r *RemoteClusterReconciler) patchAnnotation(ctx context.Context, cluster *infrav1.RemoteCluster, key, value string) error {
	// Re-fetch so we have the current resourceVersion before patching.
	fresh := &infrav1.RemoteCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
		return fmt.Errorf("re-fetching cluster for annotation patch: %w", err)
	}
	patch := client.MergeFrom(fresh.DeepCopy())
	if fresh.Annotations == nil {
		fresh.SetAnnotations(map[string]string{})
	}
	fresh.Annotations[key] = value
	return r.Patch(ctx, fresh, patch)
}

// patchAnnotations sets multiple annotations in a single merge patch, re-fetching
// the latest resource version first.  An empty value removes the annotation key.
func (r *RemoteClusterReconciler) patchAnnotations(ctx context.Context, cluster *infrav1.RemoteCluster, kv map[string]string) error {
	fresh := &infrav1.RemoteCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
		return fmt.Errorf("re-fetching cluster for annotation patch: %w", err)
	}
	patch := client.MergeFrom(fresh.DeepCopy())
	if fresh.Annotations == nil {
		fresh.SetAnnotations(map[string]string{})
	}
	for k, v := range kv {
		if v == "" {
			delete(fresh.Annotations, k)
		} else {
			fresh.Annotations[k] = v
		}
	}
	return r.Patch(ctx, fresh, patch)
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

// ensureAuthSecretFinalizer adds authSecretFinalizer to the referenced SSH
// credential secret.  This prevents the secret from being deleted while the
// RemoteCluster exists, ensuring the controller can always SSH to the node
// during deletion cleanup.
func (r *RemoteClusterReconciler) ensureAuthSecretFinalizer(ctx context.Context, cluster *infrav1.RemoteCluster, secret *corev1.Secret) error {
	if controllerutil.ContainsFinalizer(secret, authSecretFinalizer) {
		return nil
	}
	patch := client.MergeFrom(secret.DeepCopy())
	controllerutil.AddFinalizer(secret, authSecretFinalizer)
	return r.Patch(ctx, secret, patch)
}

// removeAuthSecretFinalizer removes authSecretFinalizer from the SSH credential
// secret.  Called at the end of handleDelete after all SSH work is complete.
func (r *RemoteClusterReconciler) removeAuthSecretFinalizer(ctx context.Context, cluster *infrav1.RemoteCluster) {
	log := logf.FromContext(ctx)

	// Try user-managed secret first; fall back to controller copy.
	secret, err := r.getAuthSecret(ctx, cluster)
	if err != nil {
		secret2, err2 := r.getControllerAuthSecret(ctx, cluster)
		if err2 != nil {
			log.Info("Auth secret already gone — nothing to unfinalise")
			return
		}
		secret = secret2
	}

	if !controllerutil.ContainsFinalizer(secret, authSecretFinalizer) {
		return
	}
	patch := client.MergeFrom(secret.DeepCopy())
	controllerutil.RemoveFinalizer(secret, authSecretFinalizer)
	if err := r.Patch(ctx, secret, patch); err != nil {
		log.Error(err, "removing auth-secret finalizer", "secret", secret.Name)
	}
}

// getVPNSecret fetches the VPN SSH credential secret referenced by
// spec.vpnConfig.vpnSSHCredentialsRef.
func (r *RemoteClusterReconciler) getVPNSecret(ctx context.Context, cluster *infrav1.RemoteCluster) (*corev1.Secret, error) {
	ref := cluster.Spec.VPNConfig.VPNSSHCredentialsRef
	if ref.Name == "" {
		return nil, fmt.Errorf("no VPN SSH credentials configured in spec.vpnConfig.vpnSSHCredentialsRef")
	}
	ns := ref.NameSpace
	if ns == "" {
		ns = cluster.Namespace
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// ensureVPNSecretFinalizer adds vpnSecretFinalizer to the VPN SSH credential
// secret so it cannot be deleted while the RemoteCluster exists.
func (r *RemoteClusterReconciler) ensureVPNSecretFinalizer(ctx context.Context, cluster *infrav1.RemoteCluster, secret *corev1.Secret) error {
	if controllerutil.ContainsFinalizer(secret, vpnSecretFinalizer) {
		return nil
	}
	patch := client.MergeFrom(secret.DeepCopy())
	controllerutil.AddFinalizer(secret, vpnSecretFinalizer)
	return r.Patch(ctx, secret, patch)
}

// removeVPNSecretFinalizer removes vpnSecretFinalizer from the VPN SSH credential
// secret.  Called at the end of handleDelete after the VPN peer removal step.
func (r *RemoteClusterReconciler) removeVPNSecretFinalizer(ctx context.Context, cluster *infrav1.RemoteCluster) {
	log := logf.FromContext(ctx)

	if cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name == "" {
		return
	}

	secret, err := r.getVPNSecret(ctx, cluster)
	if err != nil {
		log.Info("VPN SSH secret already gone — nothing to unfinalise",
			"secret", cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name)
		return
	}

	if !controllerutil.ContainsFinalizer(secret, vpnSecretFinalizer) {
		return
	}
	patch := client.MergeFrom(secret.DeepCopy())
	controllerutil.RemoveFinalizer(secret, vpnSecretFinalizer)
	if err := r.Patch(ctx, secret, patch); err != nil {
		log.Error(err, "removing vpn-secret finalizer", "secret", secret.Name)
	}
}

// getAuthSecret fetches the user-managed SSH credential secret for the cluster.
func (r *RemoteClusterReconciler) getAuthSecret(ctx context.Context, cluster *infrav1.RemoteCluster) (*corev1.Secret, error) {
	var name string
	if cluster.Spec.Auth.SSHPrivateKeySecretRef != nil {
		name = cluster.Spec.Auth.SSHPrivateKeySecretRef.Name
	} else if cluster.Spec.Auth.PasswordSecretRef != nil {
		name = cluster.Spec.Auth.PasswordSecretRef.Name
	} else {
		return nil, fmt.Errorf("no SSH auth credentials configured in spec.auth")
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// getControllerAuthSecret retrieves the controller-owned copy of the SSH
// credential secret (name = <cluster.Name> + controllerAuthSuffix).
func (r *RemoteClusterReconciler) getControllerAuthSecret(ctx context.Context, cluster *infrav1.RemoteCluster) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      cluster.Name + controllerAuthSuffix,
		Namespace: cluster.Namespace,
	}, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// ensureControllerAuthSecret creates or updates a controller-owned copy of the
// user SSH credential secret.  An owner reference on the RemoteCluster CR
// ensures the copy is GC'd automatically once the CR's finalizer is removed,
// so it is never orphaned.
func (r *RemoteClusterReconciler) ensureControllerAuthSecret(ctx context.Context, cluster *infrav1.RemoteCluster, userSecret *corev1.Secret) error {
	copyName := cluster.Name + controllerAuthSuffix

	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: copyName, Namespace: cluster.Namespace}, existing)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting controller auth secret: %w", err)
	}

	desired := &corev1.Secret{}
	desired.Name = copyName
	desired.Namespace = cluster.Namespace
	desired.Type = userSecret.Type
	desired.Data = userSecret.Data

	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference on controller auth secret: %w", err)
	}

	if apierrors.IsNotFound(err) {
		if createErr := r.Create(ctx, desired); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return fmt.Errorf("creating controller auth secret: %w", createErr)
		}
		return nil
	}

	// Update only when the data has changed.
	if !secretDataEqual(existing.Data, userSecret.Data) {
		existing.Data = userSecret.Data
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("updating controller auth secret: %w", err)
		}
	}
	return nil
}

// secretDataEqual returns true when two secret data maps have identical keys and values.
func secretDataEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || string(va) != string(vb) {
			return false
		}
	}
	return true
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
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("fetching SSH credential secret %q: %w", secretRef.Name, err)
		}
		// User-managed secret is gone — fall back to the controller-owned copy.
		ctrlSecret, ctrlErr := r.getControllerAuthSecret(ctx, cluster)
		if ctrlErr != nil {
			return nil, fmt.Errorf("fetching SSH credential secret %q (and controller copy %q): %w",
				secretRef.Name, cluster.Name+controllerAuthSuffix, err)
		}
		logf.FromContext(ctx).Info("User SSH secret not found, falling back to controller copy",
			"userSecret", secretRef.Name)
		secret = ctrlSecret
	}

	credentialBytes, ok := secret.Data[secretRef.Key]
	if !ok {
		// Key may be empty for secrets with a single entry; pick the only value.
		if secretRef.Key == "" && len(secret.Data) == 1 {
			for _, v := range secret.Data {
				credentialBytes = v
			}
		} else {
			return nil, fmt.Errorf("key %q not found in secret %q", secretRef.Key, secretRef.Name)
		}
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
	log.Info("Deprovisioning RemoteCluster", "name", cluster.Name, "nodeType", cluster.Spec.NodeInfo.NodeType)

	if !controllerutil.ContainsFinalizer(cluster, remoteClusterFinalizer) {
		return ctrl.Result{}, nil
	}

	// Step 1: If worker, drain and remove the node from the Kubernetes cluster
	// before wiping it so workloads migrate away gracefully.
	if cluster.Spec.NodeInfo.NodeType == "worker" {
		r.drainWorkerFromCP(ctx, cluster)
	}

	// Step 2: SSH to the node — kubeadm reset, purge packages, remove configs,
	// bring down WireGuard.  Best-effort: if the node is already unreachable
	// we still proceed so the finalizer can be removed.
	if err := r.resetNodeViaSSH(ctx, cluster); err != nil {
		log.Error(err, "node SSH reset incomplete (continuing with cleanup)")
	}

	// Step 3: Remove the WireGuard peer from the VPN server.
	if cluster.Spec.VPNConfig.IP != "" && cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name != "" {
		if err := r.removeVPNPeer(ctx, cluster); err != nil {
			log.Error(err, "VPN peer removal incomplete (continuing)")
		}
	}

	// Step 4: Delete management-cluster resources (Porch repo, Nephio tokens,
	// PackageVariants).  Errors are logged but do not block finalizer removal.
	if err := r.deleteClusterResources(ctx, cluster); err != nil {
		log.Error(err, "deleting management-cluster resources (continuing)")
	}

	// Step 5: Release the auth and VPN SSH secrets (remove our finalizers) so
	// that any pending deletions of user-managed secrets can now proceed.
	// This runs after all SSH work is done so both secrets are available throughout.
	r.removeAuthSecretFinalizer(ctx, cluster)
	r.removeVPNSecretFinalizer(ctx, cluster)

	// Step 6: Remove the RemoteCluster finalizer — lets the API server GC the CR.
	controllerutil.RemoveFinalizer(cluster, remoteClusterFinalizer)
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	log.Info("RemoteCluster cleanup complete", "name", cluster.Name)
	return ctrl.Result{}, nil
}

// drainWorkerFromCP gracefully evicts workloads from the worker node by running
// kubectl drain + kubectl delete node on the control-plane.  Best-effort.
func (r *RemoteClusterReconciler) drainWorkerFromCP(ctx context.Context, cluster *infrav1.RemoteCluster) {
	log := logf.FromContext(ctx)

	// Determine the Kubernetes node name: try the actual OS hostname via SSH,
	// fall back to the cluster name which is typically the provisioned hostname.
	nodeName := cluster.Spec.ClusterName
	if nodeClient, err := r.getSSHClient(ctx, cluster); err == nil {
		if out, err := sshhelper.Run(nodeClient, "hostname"); err == nil {
			if h := strings.TrimSpace(out); h != "" {
				nodeName = h
			}
		}
		nodeClient.Conn.Close()
	}

	cp, err := r.findControlPlane(ctx, cluster)
	if err != nil || cp == nil {
		log.Info("Control-plane not found — skipping kubectl drain", "fallbackNodeName", nodeName)
		return
	}
	cpClient, err := r.getSSHClient(ctx, cp)
	if err != nil {
		log.Error(err, "Cannot SSH to control-plane for drain (continuing without drain)")
		return
	}
	defer cpClient.Conn.Close()

	log.Info("Draining worker from control-plane", "nodeName", nodeName)
	drainCmd := fmt.Sprintf(
		"kubectl drain %s --ignore-daemonsets --delete-emptydir-data --force --timeout=120s 2>/dev/null || true",
		nodeName,
	)
	if out, err := sshhelper.Run(cpClient, drainCmd); err != nil {
		log.Error(err, "kubectl drain encountered errors", "output", out)
	}
	deleteCmd := fmt.Sprintf("kubectl delete node %s --ignore-not-found 2>/dev/null || true", nodeName)
	if out, err := sshhelper.Run(cpClient, deleteCmd); err != nil {
		log.Error(err, "kubectl delete node encountered errors", "output", out)
	}
}

// resetNodeViaSSH SSHes to the node and runs kubeadm reset, purges all
// installed packages (k8s, cri-o, criu, wireguard), removes config files and
// custom binaries, and brings the WireGuard tunnel down.
func (r *RemoteClusterReconciler) resetNodeViaSSH(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx)

	sshClient, err := r.getSSHClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("SSH connect for node reset: %w", err)
	}
	defer sshClient.Conn.Close()

	log.Info("Resetting node via SSH", "host", cluster.Spec.Host)

	const resetScript = `
# kubeadm reset cleans up apiserver/etcd/kubelet state, CNI config, and iptables rules.
if command -v kubeadm >/dev/null 2>&1; then
  sudo kubeadm reset --force 2>/dev/null || true
fi

# Stop and disable services before purging packages.
sudo systemctl stop kubelet crio 2>/dev/null || true
sudo systemctl disable kubelet crio 2>/dev/null || true

# Unmount any lingering container overlay mounts.
sudo umount -l /var/lib/containers/storage/overlay/*/merged 2>/dev/null || true

# Purge Kubernetes packages.
sudo apt-mark unhold kubelet kubeadm kubectl 2>/dev/null || true
sudo apt-get purge -y kubelet kubeadm kubectl 2>/dev/null || true

# Purge CRI-O and related container runtime packages.
sudo apt-get purge -y cri-o criu crun conmon 2>/dev/null || true

# Purge NVIDIA container toolkit packages installed by InstallNvidiaContainerToolkit.
sudo apt-get purge -y nvidia-container-toolkit nvidia-container-toolkit-base \
  libnvidia-container-tools libnvidia-container1 2>/dev/null || true
sudo rm -f /etc/apt/sources.list.d/nvidia-container-toolkit.list 2>/dev/null || true
sudo rm -f /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg 2>/dev/null || true

# Remove Kubernetes state directories.
sudo rm -rf /etc/kubernetes /var/lib/kubelet /var/lib/etcd 2>/dev/null || true

# Remove CRI-O runtime state and the entire config tree (includes
# 999-runc.conf, 10-crun.conf, crio.conf, and any leftover dpkg conffiles).
sudo rm -rf /var/lib/crio /run/crio /var/lib/containers/storage 2>/dev/null || true
sudo rm -rf /etc/crio 2>/dev/null || true

# Remove CRIU config directory (runc.conf and anything else under /etc/criu).
sudo rm -rf /etc/criu 2>/dev/null || true

sudo rm -f  /etc/modules-load.d/k8s.conf /etc/sysctl.d/k8s.conf 2>/dev/null || true
sudo rm -f  /etc/apt/sources.list.d/kubernetes.list /etc/apt/sources.list.d/cri-o.list 2>/dev/null || true
sudo rm -f  /etc/apt/keyrings/kubernetes-apt-keyring.gpg /etc/apt/keyrings/cri-o-apt-keyring.gpg 2>/dev/null || true

# Remove custom binaries installed during provisioning.
sudo rm -f /usr/local/bin/crictl /usr/bin/crictl 2>/dev/null || true
sudo rm -f /usr/local/bin/crun   /usr/bin/crun   2>/dev/null || true
sudo rm -f /usr/sbin/runc /usr/local/sbin/runc   2>/dev/null || true
sudo rm -f /usr/sbin/criu                         2>/dev/null || true
sudo rm -f /usr/bin/crio                          2>/dev/null || true
sudo rm -f /usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || true

# Remove the bootstrap-complete marker so re-provisioning is not skipped.
sudo rm -f /var/lib/node-bootstrap-complete 2>/dev/null || true

sudo apt-get autoremove -y 2>/dev/null || true

# WireGuard teardown runs in the background after a short delay so this SSH
# session (which routes over the VPN IP) can exit cleanly first.
# The controller removes the peer from the VPN server in the next step, which
# permanently severs the tunnel from the server side.
nohup sudo bash -c '
  sleep 3
  systemctl stop    wg-quick@wg0 2>/dev/null || true
  wg-quick down wg0              2>/dev/null || true
  systemctl disable wg-quick@wg0 2>/dev/null || true
  rm -f /etc/wireguard/wg0.conf  2>/dev/null || true
  apt-get purge -y wireguard wireguard-tools 2>/dev/null || true
' >/dev/null 2>&1 &

echo "node reset complete"
`
	out, err := sshhelper.Run(sshClient, resetScript)
	if err != nil {
		log.Error(err, "node reset script reported errors", "output", out)
		return fmt.Errorf("node reset: %w", err)
	}
	log.Info("Node reset complete")
	return nil
}

// removeVPNPeer SSHes to the WireGuard VPN server and removes the peer whose
// AllowedIP matches cluster.Spec.VPNConfig.IP from both the running config and
// the persisted wg0.conf.
func (r *RemoteClusterReconciler) removeVPNPeer(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx)

	vpnIP := cluster.Spec.VPNConfig.IP
	credRef := cluster.Spec.VPNConfig.VPNSSHCredentialsRef

	vpnSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      credRef.Name,
		Namespace: credRef.NameSpace,
	}, vpnSecret); err != nil {
		return fmt.Errorf("fetching VPN server SSH secret %q: %w", credRef.Name, err)
	}

	var credBytes []byte
	if credRef.Key != "" {
		var ok bool
		credBytes, ok = vpnSecret.Data[credRef.Key]
		if !ok {
			return fmt.Errorf("key %q not found in secret %q", credRef.Key, credRef.Name)
		}
	} else {
		if len(vpnSecret.Data) != 1 {
			return fmt.Errorf("secret %q has %d keys; set vpnSshCredentialsRef.key to pick one", credRef.Name, len(vpnSecret.Data))
		}
		for _, v := range vpnSecret.Data {
			credBytes = v
		}
	}

	cred := strings.TrimSpace(string(credBytes))
	vpnHost := cluster.Spec.VPNConfig.VPNServerPublicIP
	vpnPort := cluster.Spec.VPNConfig.VPNServerSSHPort
	if vpnPort == 0 {
		vpnPort = 22
	}
	vpnUser := cluster.Spec.VPNConfig.VPNServerSSHUsername
	if vpnUser == "" {
		vpnUser = "ubuntu"
	}

	var vpnClient *ssh.Client
	var err error
	if strings.HasPrefix(cred, "-----BEGIN") {
		vpnClient, err = ssh.ConnectWithPrivateKey(vpnHost, vpnPort, vpnUser, cred)
	} else {
		vpnClient, err = ssh.Connect(vpnHost, vpnPort, vpnUser, cred)
	}
	if err != nil {
		return fmt.Errorf("SSH to VPN server %s:%d as %s: %w", vpnHost, vpnPort, vpnUser, err)
	}
	defer vpnClient.Conn.Close()

	// Discover the peer's public key from the live WireGuard state.
	dumpOut, err := sshhelper.Run(vpnClient, "sudo wg show wg0 dump 2>/dev/null || true")
	if err != nil {
		return fmt.Errorf("reading WireGuard peer table: %w", err)
	}

	peerKey := ""
	for i, line := range strings.Split(strings.TrimSpace(dumpOut), "\n") {
		if i == 0 || line == "" {
			continue // first line is the interface entry
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		for _, cidr := range strings.Split(fields[3], ",") {
			ip := strings.SplitN(strings.TrimSpace(cidr), "/", 2)[0]
			if ip == vpnIP {
				peerKey = fields[0]
				break
			}
		}
		if peerKey != "" {
			break
		}
	}

	if peerKey == "" {
		log.Info("WireGuard peer not found on VPN server (already removed)", "vpnIP", vpnIP)
		return nil
	}

	// Remove from the running WireGuard interface.
	if out, err := sshhelper.Run(vpnClient, fmt.Sprintf("sudo wg set wg0 peer %s remove", peerKey)); err != nil {
		log.Error(err, "removing peer from running WireGuard config", "output", out)
	}

	// Remove the matching [Peer] block from /etc/wireguard/wg0.conf so the
	// peer is not re-added on VPN server restart.
	removeFromConf := fmt.Sprintf(`
WG_CONF=/etc/wireguard/wg0.conf
if sudo test -f "$WG_CONF"; then
  sudo awk -v our_key="%s" '
    /^\[Peer\]/ { in_peer=1; buf=$0"\n"; has_key=0; next }
    in_peer {
      buf=buf $0 "\n"
      if ($0 ~ "PublicKey" && index($0, our_key)) has_key=1
      if (/^[[:space:]]*$/ || /^\[/) {
        if (!has_key) printf "%%s", buf
        if (/^\[/) { in_peer=0; buf=$0"\n"; has_key=0 } else { in_peer=0; buf="" }
        next
      }
      next
    }
    { print }
    END { if (in_peer && !has_key) printf "%%s", buf }
  ' "$WG_CONF" | sudo tee "${WG_CONF}.tmp" > /dev/null && sudo mv "${WG_CONF}.tmp" "$WG_CONF"
fi`, peerKey)

	if out, err := sshhelper.Run(vpnClient, removeFromConf); err != nil {
		log.Error(err, "removing peer block from wg0.conf", "output", out)
	}

	log.Info("Removed WireGuard peer from VPN server", "vpnIP", vpnIP)
	return nil
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
		// {
		// 	name: "longhorn-storage-provisioner-variant",
		// 	upstream: packageRef{
		// 		pkg:      "longhorn-storage-provisioner",
		// 		repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
		// 		revision: cluster.Spec.GitConfig.PackageRevision,
		// 	},
		// 	downstream: packageRef{
		// 		pkg:  "longhorn-storage-provisioner",
		// 		repo: cluster.Spec.ClusterName,
		// 	},
		// 	annotations: map[string]interface{}{
		// 		"approval.nephio.org/policy": "initial",
		// 	},
		// },

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

		{
			name: "gpu-operator-variant",
			upstream: packageRef{
				pkg:      "gpu-operator",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "gpu-operator",
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
