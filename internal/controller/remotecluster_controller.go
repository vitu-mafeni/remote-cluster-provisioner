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
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
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
	"k8s.io/client-go/util/retry"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	corev1 "k8s.io/api/core/v1"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	"dcn.ssu.ac.kr/infra/provider/onprem"
)

//go:embed assets/ml.dcn.ssu.ac.kr_nodeprovisionnetconfigs.yaml
var nodeprovisionnetconfigCRD string

// controlPlaneJobResult carries the outcome of a background control-plane init goroutine.
type controlPlaneJobResult struct {
	joinCommand string
	err         error
}

// RemoteClusterReconciler reconciles a RemoteCluster object.
type RemoteClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// controlPlaneJobs holds in-flight control-plane init goroutines.
	// Key: "namespace/name", Value: *controlPlaneJob (carries the object UID and
	// a cancel func so a deleted/recreated RemoteCluster never adopts or leaks a
	// stale goroutine).
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
	// annotationJoinTokenRefreshedAt records the RFC3339 timestamp of the last
	// successful kubeadm token refresh so the controller knows when to renew again.
	annotationJoinTokenRefreshedAt = "infra.dcn.ssu.ac.kr/join-token-refreshed-at"
	// annotationCnlabCredentialsHash is the SHA-256 hex digest of the cnlab-runtime
	// registry username+token last synced to the remote cluster.  When this differs
	// from the current secret, the controller re-pushes the secret and patches
	// NodeProvisionNetConfig — including on clusters provisioned before credentials
	// were added — so every cluster stays able to pull runtime updates autonomously.
	annotationCnlabCredentialsHash = "infra.dcn.ssu.ac.kr/cnlab-credentials-hash"
	// annotationNetConfigSyncHash is the SHA-256 hex digest of every
	// NodeProvisionNetConfig spec field this controller keeps in sync from a
	// RemoteCluster (clusterName, vpnRange, vpnServerPublicConfig,
	// softwareConfig minus cnlabRuntime, which has its own credential-aware
	// sync path) — see desiredNodeProvisionNetConfigFields, the single source
	// of truth both the local and remote sync paths patch from. When this
	// differs from the current spec, the controller re-patches the remote
	// cluster's NodeProvisionNetConfig — this is what heals a remote push
	// missed at controller-restart time (e.g. the remote cluster was
	// unreachable then), or left incomplete by an earlier partial sync, on
	// the next successful reconcile — without waiting for another restart or
	// a worker-join event.
	annotationNetConfigSyncHash = "infra.dcn.ssu.ac.kr/netconfig-sync-hash"
	// annotationCoreVariantsCreated marks that core PackageVariants have been
	// applied so the overlay step can proceed without blocking the reconcile
	// worker thread with a sleep.
	annotationCoreVariantsCreated = "infra.dcn.ssu.ac.kr/core-variants-created"
	// annotationDeleteNodeCleanupDone marks (on an object being deleted) that the
	// node drain/reset has been attempted, so a re-entered delete does not repeat
	// minutes of SSH work.
	annotationDeleteNodeCleanupDone = "infra.dcn.ssu.ac.kr/delete-node-cleanup-done"

	// tokenRefreshInterval is how often to rotate the kubeadm bootstrap token.
	// kubeadm tokens expire after 24 h by default; refresh 1 h before expiry.
	tokenRefreshInterval = 23 * time.Hour

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

	// controlPlanePollInterval is how often the controller polls the background
	// control-plane init goroutine.  kubeadm init + CNI setup typically takes
	// 5-15 minutes, so 30 s gives reasonable responsiveness without hammering.
	controlPlanePollInterval = 30 * time.Second

	// maxProvisionRetries is the number of consecutive provisioning failures
	// allowed before the controller stops retrying and leaves the RemoteCluster
	// in a terminal Failed state requiring manual intervention.
	maxProvisionRetries = 5

	// maxCnlabSyncRetries is the number of consecutive SSH failures allowed when
	// pushing cnlab-runtime credentials to the remote cluster before the controller
	// stops retrying and surfaces an error condition on the RemoteCluster resource.
	// The VPN link to the remote cluster may be down.
	maxCnlabSyncRetries = 5

	// cnlabSyncConditionType is the Condition type recorded on the RemoteCluster
	// when the credential sync has reached the retry limit.
	cnlabSyncConditionType = "CnlabCredentialSyncFailed"
)

// packageVariantNamespace is the namespace PackageVariants are created in
// (and therefore listed/deleted in).
const packageVariantNamespace = "default"

// packageVariantGVK is the GVK for Porch PackageVariant resources.
var packageVariantGVK = schema.GroupVersionKind{
	Group:   "config.porch.kpt.dev",
	Version: "v1alpha1",
	Kind:    "PackageVariant",
}

// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisionnetconfigs,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=infra.nephio.org,resources=tokens;repositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.porch.kpt.dev,resources=repositories;packagevariants,verbs=get;list;watch;create;update;patch;delete

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

	// The VPN mode is a property of the cluster, not of a single node: make
	// workers follow their control-plane before anything below reads it.
	if res, done, err := r.reconcileVPNMode(ctx, cluster); done || err != nil {
		return res, err
	}

	// An invalid insecureRegistries entry would end up in a drop-in file name
	// and a shell script on the node: refuse it up front with a clear message
	// instead of failing midway through provisioning.
	if err := pkgruntime.ValidateInsecureRegistries(cluster.Spec.NodeInfo.SoftwareConfig.InsecureRegistries); err != nil {
		cause := fmt.Errorf("spec.nodeInfo.softwareConfig.%w", err)
		if cluster.Status.Phase == phaseReady {
			return r.softFail(ctx, cluster, "InvalidInsecureRegistries", cause)
		}
		return r.failNoCount(ctx, cluster, "InvalidInsecureRegistries", cause)
	}
	r.clearConditions(ctx, cluster, "InvalidInsecureRegistries")

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

	// Protect the VPN SSH credential secret the same way (not used at all when
	// the VPN is disabled).
	if !effectiveVPNDisabled(cluster) && cluster.Spec.VPNConfig.VPNSSHCredentialsRef.Name != "" {
		if vpnSecret, err := r.getVPNSecret(ctx, cluster); err == nil {
			if err := r.ensureVPNSecretFinalizer(ctx, cluster, vpnSecret); err != nil {
				log.Error(err, "adding finalizer to VPN SSH credential secret (non-fatal)")
			}
		}
	}

	isControlPlane := cluster.Spec.NodeInfo.NodeType == "control-plane"

	switch cluster.Status.Phase {
	case "", phaseProvisioning:
		if isControlPlane && cluster.Status.JoinCommand != "" {
			return r.recoverControlPlane(ctx, cluster)
		}
		return r.reconcileProvisioning(ctx, cluster)
	case phaseReady:
		// Record the VPN mode of nodes provisioned before it was tracked, so a
		// later change of spec.disableVPN cannot alter how they are cleaned up.
		if cluster.Annotations[annotationProvisionedVPNMode] == "" {
			if err := r.patchAnnotation(ctx, cluster, annotationProvisionedVPNMode, vpnModeString(cluster.Spec.DisableVPN)); err != nil {
				log.Error(err, "recording provisioned VPN mode (non-fatal)")
			} else {
				ensureAnnotations(cluster)[annotationProvisionedVPNMode] = vpnModeString(cluster.Spec.DisableVPN)
			}
		}
		if !isControlPlane && cluster.Annotations[annotationWorkerFinalizePending] != "" {
			return r.retryWorkerFinalize(ctx, cluster)
		}
		if isControlPlane {
			// If NodeProvisionNetConfig was not created (e.g. it failed after setStatus
			// already flipped to Ready in a previous run), re-run it now via SSH before
			// proceeding to PackageVariants.
			if cluster.Annotations[annotationNodeProvisionCreated] != "true" {
				log.Info("NodeProvisionNetConfig not yet created; running now")
				sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
				defer cancel()
				sshClient, err := r.getSSHClient(sshCtx, cluster)
				if err != nil {
					return r.softFail(ctx, cluster, "SSHConnectionFailed",
						fmt.Errorf("SSH for NodeProvisionNetConfig: %w", err))
				}
				defer func() { _ = sshClient.Conn.Close() }()
				if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cluster, cluster, sshClient, cluster.Spec.VPNConfig.IP, "create"); err != nil {
					return r.softFail(ctx, cluster, "NodeProvisionNetConfigUpdateFailed",
						fmt.Errorf("creating NodeProvisionNetConfig: %w", err))
				}
				r.clearConditions(ctx, cluster, "SSHConnectionFailed", "NodeProvisionNetConfigUpdateFailed")
				if patchErr := r.patchAnnotation(ctx, cluster, annotationNodeProvisionCreated, "true"); patchErr != nil {
					log.Error(patchErr, "Failed to stamp node-provision-created annotation")
				}
			}

			// Keep a local (management-cluster) NodeProvisionNetConfig in sync with
			// this cluster's VPN config on every Ready-phase reconcile — independent
			// of annotationNodeProvisionCreated above, which only gates the one-time
			// SSH-based apply to the *remote* cluster. Without this, the local copy
			// that ml-nodeprovision reads from (e.g. to VPN-connect newly-created AWS
			// EC2 workers) has no source of truth at all and must be hand-authored,
			// which is exactly how it can silently end up with an empty
			// vpnSshCredentialsRef even though RemoteCluster.Spec.VPNConfig is correct.
			if err := r.ensureLocalNodeProvisionNetConfig(ctx, cluster, cluster); err != nil {
				log.Error(err, "syncing local NodeProvisionNetConfig (non-fatal)")
			}

			// Refresh the kubeadm bootstrap token when due — best-effort, never
			// blocks PackageVariant creation.  Always fall through so that a
			// freshly-Ready cluster reaches reconcilePackageVariants on the same
			// reconcile that the token is first refreshed.
			if needsTokenRefresh(cluster) {
				log.Info("Kubeadm bootstrap token due for refresh")
				if _, err := r.refreshJoinToken(ctx, cluster); err != nil {
					log.Error(err, "refreshing join token (non-fatal)")
				}
			} else if ts, ok := cluster.Annotations[annotationJoinTokenRefreshedAt]; ok {
				if t, err := time.Parse(time.RFC3339, ts); err == nil {
					log.Info("Kubeadm bootstrap token still valid",
						"refreshedAt", t.Format(time.RFC3339),
						"nextRefreshIn", time.Until(t.Add(tokenRefreshInterval)).Round(time.Minute).String())
				}
			} else {
				log.Info("Kubeadm bootstrap token has never been explicitly refreshed; will refresh now")
			}

			// Sync cnlab-runtime registry credentials to the remote cluster whenever
			// they are added or rotated — including clusters provisioned before this
			// feature existed (annotationCnlabCredentialsHash will be absent on them).
			if err := r.syncCnlabCredentialsToRemote(ctx, cluster); err != nil {
				log.Error(err, "syncing cnlab-runtime credentials to remote cluster (non-fatal)")
			}

			// Sync NodeProvisionNetConfig (clusterName, vpnRange, vpnServerPublicConfig,
			// softwareConfig minus cnlabRuntime) to the remote cluster whenever it
			// drifts from the last successfully-pushed value — including catching
			// up a push that was missed at controller-restart time because the
			// remote cluster was unreachable then, or left incomplete by an
			// earlier partial sync. Unreachability here is expected/normal (the
			// provisioned cluster is decoupled from this controller), so failures
			// are logged and retried next reconcile, never escalated.
			if err := r.syncNetConfigToRemote(ctx, cluster); err != nil {
				log.Info("syncing NodeProvisionNetConfig to remote cluster — will retry next reconcile",
					"reason", err.Error())
			}

			if cluster.Annotations[annotationPkgVariantsCreated] == "true" {
				// Schedule next wakeup for token renewal.
				requeueAfter := tokenRequeueAfter(cluster)
				log.Info("Cluster fully ready",
					"nextTokenRefreshIn", requeueAfter.Round(time.Minute).String())
				return ctrl.Result{RequeueAfter: requeueAfter}, nil
			}
			return r.reconcilePackageVariants(ctx, cluster)
		}
		return ctrl.Result{}, nil
	case phaseFailed:
		// A control-plane that already finished kubeadm init (JoinCommand set) is a
		// working cluster: a later step failed. Never leave it in the Failed ->
		// Provisioning loop; move it back to Ready so the idempotent Ready steps
		// (PackageVariants, token refresh, sync) run again.
		if isControlPlane && cluster.Status.JoinCommand != "" {
			return r.recoverControlPlane(ctx, cluster)
		}
		// Likewise a worker that already joined: only its finalization is left,
		// which never needs (or consumes the retry budget of) a fresh provisioning.
		if !isControlPlane && cluster.Annotations[annotationWorkerJoined] == "true" {
			return r.retryWorkerFinalize(ctx, cluster)
		}
		if cluster.Status.ProvisionRetryCount >= maxProvisionRetries {
			log.Info("RemoteCluster in terminal Failed state — retry limit reached, manual intervention required",
				"attempts", cluster.Status.ProvisionRetryCount,
				"maxRetries", maxProvisionRetries,
				"message", cluster.Status.Message)
			return ctrl.Result{}, nil
		}
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

	// A worker that already joined must never be re-provisioned nor need a
	// connection to its own host: finish its finalization through the
	// control-plane only (and without spending the provisioning retry budget).
	if cluster.Spec.NodeInfo.NodeType == "worker" && cluster.Annotations[annotationWorkerJoined] == "true" {
		return r.retryWorkerFinalize(ctx, cluster)
	}

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
		return r.recoverControlPlane(ctx, cluster)
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
	if running {
		// Discard a job that belongs to a previous incarnation of this name
		// (deleted and recreated): cancel it and start over.
		if job, ok := v.(*controlPlaneJob); !ok || job.uid != string(cluster.UID) {
			log.Info("Discarding stale control-plane init job from a previous object with the same name")
			r.cancelControlPlaneJob(key)
			running = false
		}
	}
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

		clusterCopy := withEffectiveVPNMode(cluster)

		runtimeCfg, err := r.resolveCnlabRuntimeConfig(ctx, cluster.Spec.NodeInfo.SoftwareConfig, cluster.Namespace)
		if err != nil {
			sshClient.Conn.Close() //nolint:errcheck
			return r.fail(ctx, cluster, "RuntimeConfigError",
				fmt.Errorf("resolving cnlab-runtime config: %w", err))
		}

		// The connection works and the host is about to be modified: record the VPN
		// mode this node is provisioned with, so later cleanup and SSH addressing do
		// not depend on the (mutable) spec. Not earlier — a host that was never
		// reached must stay free to have its spec.disableVPN corrected.
		if err := r.recordProvisionedVPNMode(ctx, cluster); err != nil {
			sshClient.Conn.Close() //nolint:errcheck
			return ctrl.Result{}, err
		}

		ch := make(chan controlPlaneJobResult, 1)
		job := &controlPlaneJob{
			ch:  ch,
			uid: string(cluster.UID),
			// Closing the connection the goroutine owns aborts its running command.
			cancel: func() { _ = sshClient.Conn.Close() },
		}
		r.controlPlaneJobs.Store(key, job)

		go func() {
			defer sshClient.Conn.Close() //nolint:errcheck
			joinCommand, err := kubeadm.InitializeControlPlane(sshClient, clusterCopy, startPhase, func(phaseIdx int) {
				if !job.cancelled.Load() {
					r.controlPlaneProgress.Store(key, phaseIdx)
				}
			}, runtimeCfg)
			ch <- controlPlaneJobResult{joinCommand: joinCommand, err: err}
		}()

		log.Info("Control plane init goroutine started", "startPhase", startPhase)
		return ctrl.Result{RequeueAfter: controlPlanePollInterval}, nil
	}

	// Poll the result channel (non-blocking).
	ch := v.(*controlPlaneJob).ch
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
			// Nothing completed on the host: release the VPN mode record so the
			// user can still correct spec.disableVPN (no-op once a phase completed).
			r.releaseVPNModeIfUntouched(ctx, cluster)
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

	if cluster.Annotations[annotationCoreVariantsCreated] != "true" {
		if err := r.createCorePackageVariants(ctx, cluster); err != nil {
			return r.softFail(ctx, cluster, "CorePackageVariantsFailed", fmt.Errorf("creating core PackageVariants: %w", err))
		}
		r.clearConditions(ctx, cluster, "CorePackageVariantsFailed")
		if patchErr := r.patchAnnotation(ctx, cluster, annotationCoreVariantsCreated, "true"); patchErr != nil {
			log.Error(patchErr, "Failed to stamp core-variants-created annotation (non-fatal)")
		}
		// Give Porch time to sync the new cluster repo before overlay PackageVariants
		// are created. Instead of sleeping (which blocks a worker thread), requeue
		// immediately; the annotation gate above ensures core creation runs only once.
		log.Info("Core PackageVariants created — requeueing to allow Porch sync before overlay step",
			"delay", repoReadyWait.String())
		return ctrl.Result{RequeueAfter: repoReadyWait}, nil
	}

	if err := r.createOverlaysPlusPostInstallPackageVariants(ctx, cluster); err != nil {
		return r.softFail(ctx, cluster, "OverlayPackageVariantsFailed", fmt.Errorf("creating overlay PackageVariants: %w", err))
	}
	r.clearConditions(ctx, cluster, "OverlayPackageVariantsFailed")

	if err := r.patchAnnotation(ctx, cluster, annotationPkgVariantsCreated, "true"); err != nil {
		return ctrl.Result{}, fmt.Errorf("marking package-variants as created: %w", err)
	}
	ensureAnnotations(cluster)[annotationPkgVariantsCreated] = "true"

	// Annotation-only writes never wake the controller (GenerationChangedPredicate),
	// so schedule the next wake-up for the bootstrap-token renewal explicitly.
	requeueAfter := tokenRequeueAfter(cluster)
	log.Info("PackageVariants created; cluster is fully ready", "nextTokenRefreshIn", requeueAfter.Round(time.Minute).String())
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *RemoteClusterReconciler) reconcileWorker(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	sshClient *sshhelper.Client,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("cluster", cluster.Name, "clusterName", cluster.Spec.ClusterName)
	clusterParent, err := r.findControlPlane(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing RemoteClusters: %w", err)
	}

	// Sync VPN server config from the control-plane onto the worker CR so that
	// handleDelete can call removeVPNPeer using only cluster.Spec.VPNConfig.
	// Worker CRs carry the node's own VPN IP but not the server credentials.
	if clusterParent != nil && !effectiveVPNDisabled(cluster) &&
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

	if cluster.Annotations[annotationWorkerJoined] == "true" {
		// Already joined (reconcileProvisioning normally diverts these before it
		// opens the worker connection): only the finalization is left.
		log.Info("Worker already joined; skipping join step")
		return r.retryWorkerFinalize(ctx, cluster)
	}

	if clusterParent == nil {
		log.Info("Control-plane not found yet; requeueing")
		return ctrl.Result{RequeueAfter: controlPlaneRetryInterval}, nil
	}

	if clusterParent.Status.Phase != phaseReady || clusterParent.Status.JoinCommand == "" {
		log.Info("Control-plane not ready yet; requeueing",
			"cpPhase", clusterParent.Status.Phase)
		return ctrl.Result{RequeueAfter: controlPlaneRetryInterval}, nil
	}

	sshClientCP, err := r.getSSHClient(ctx, clusterParent) // ctx is already sshCtx from reconcileProvisioning
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

	workerRuntimeCfg, rErr := r.resolveCnlabRuntimeConfig(ctx, cluster.Spec.NodeInfo.SoftwareConfig, cluster.Namespace)
	if rErr != nil {
		return r.fail(ctx, cluster, "RuntimeConfigError",
			fmt.Errorf("resolving cnlab-runtime config: %w", rErr))
	}
	// If the worker CR has no registry credentials, inherit them from the
	// control-plane CR. This avoids repeating credentialsRef on every worker.
	if workerRuntimeCfg.Token == "" {
		parentCfg, pErr := r.resolveCnlabRuntimeConfig(ctx, clusterParent.Spec.NodeInfo.SoftwareConfig, clusterParent.Namespace)
		if pErr != nil {
			return r.fail(ctx, cluster, "RuntimeConfigError",
				fmt.Errorf("resolving cnlab-runtime config from control-plane: %w", pErr))
		}
		workerRuntimeCfg.Username = parentCfg.Username
		workerRuntimeCfg.Token = parentCfg.Token
		// Inherit registry/repo/version from parent only when the worker
		// has no cnlabRuntime block at all.
		if cluster.Spec.NodeInfo.SoftwareConfig.CnlabRuntime == nil {
			workerRuntimeCfg = parentCfg
		}
	}

	// Both connections are up and the host is about to be modified: record the
	// VPN mode it is provisioned with (once) so a later spec flip cannot change
	// how it is cleaned up. Recorded this late on purpose — a worker that was
	// never reached must stay free to have its spec corrected.
	if err := r.recordProvisionedVPNMode(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	err, nodeIP := kubeadm.JoinWorkerNode(
		sshClient,
		sshClientCP,
		withEffectiveVPNMode(cluster),
		clusterParent.Status.JoinCommand,
		withEffectiveVPNMode(clusterParent),
		workerStartPhase,
		onWorkerPhaseComplete,
		workerRuntimeCfg,
	)
	if err != nil {
		// Failed before any phase completed: the host was not changed as far as we
		// know, so release the VPN mode record and let the user fix the spec.
		r.releaseVPNModeIfUntouched(ctx, cluster)
		return r.fail(
			ctx,
			cluster,
			"WorkerJoinFailed",
			fmt.Errorf("joining worker node to cluster: %w", err),
		)
	}

	// Stamp the joined annotation FIRST (the join itself is not safe to
	// re-run) together with a "finalize pending" marker that carries the
	// node IP. Status=Ready, the control-plane's usedIPAddresses entry and the
	// node labels are recorded afterwards and retried from the marker on any
	// failure, so a failure below can no longer leave a joined worker stuck in
	// Provisioning with its IP never recorded or its node unlabelled.
	pending := nodeIP
	if pending == "" {
		pending = "none"
	}
	if err := r.patchAnnotations(ctx, cluster, map[string]string{
		annotationWorkerJoined:             "true",
		annotationLastCompletedPhaseWorker: "-1", // clear so a future reprovision starts fresh
		annotationWorkerFinalizePending:    pending,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("marking worker as joined: %w", err)
	}
	ensureAnnotations(cluster)[annotationWorkerJoined] = "true"
	ensureAnnotations(cluster)[annotationWorkerFinalizePending] = pending
	log.Info("Worker node joined to cluster")

	if err := r.finalizeWorkerJoin(ctx, cluster, clusterParent, sshClientCP); err != nil {
		return r.softFail(ctx, cluster, "WorkerFinalizeFailed", err)
	}
	return ctrl.Result{}, nil
}

// deployPrepullDaemonSets creates two DaemonSets on the remote cluster via SSH kubectl:
//   - <clusterName>-prepull-all  — runs on every worker (nodeTarget "all" images)
//   - <clusterName>-prepull-gpu  — runs on GPU workers only (nodeTarget "gpu" images)
//
// Both DaemonSets are idempotent (kubectl apply). Worker nodes must be labeled
// infra.dcn.ssu.ac.kr/worker=true and infra.dcn.ssu.ac.kr/hardware-type=gpu|cpu
// (done by reconcileWorker after join) for the pods to schedule.
func deployPrepullDaemonSets(
	ctx context.Context,
	cpSSH *sshhelper.Client,
	clusterName string,
	imagePrepulls []infrav1.ImagePrepull,
	registrySecretName string,
) {
	log := logf.FromContext(ctx)

	type dsSpec struct {
		name            string
		nodeSelectorKey string
		nodeSelectorVal string
		nodeTarget      string
	}
	specs := []dsSpec{
		{clusterName + "-prepull-all", "infra.dcn.ssu.ac.kr/worker", "true", "all"},
		{clusterName + "-prepull-gpu", "infra.dcn.ssu.ac.kr/hardware-type", "gpu", "gpu"},
	}

	for _, s := range specs {
		var filtered []string
		for _, ip := range imagePrepulls {
			if s.nodeTarget == "all" && ip.NodeTarget != "gpu" {
				filtered = append(filtered, ip.Image)
			} else if s.nodeTarget == "gpu" && ip.NodeTarget == "gpu" {
				filtered = append(filtered, ip.Image)
			}
		}
		if len(filtered) == 0 {
			log.Info("No images for DaemonSet — skipping", "daemonset", s.name)
			continue
		}

		manifest := rcBuildPrepullDaemonSetManifest(s.name, s.nodeSelectorKey, s.nodeSelectorVal, filtered, registrySecretName)
		encoded := base64.StdEncoding.EncodeToString([]byte(manifest))
		applyCmd := fmt.Sprintf("echo '%s' | base64 -d | kubectl apply -f -", encoded)
		out, err := sshhelper.Run(cpSSH, applyCmd)
		if err != nil {
			log.Error(err, "Failed to deploy prepull DaemonSet", "daemonset", s.name, "output", strings.TrimSpace(out))
		} else {
			log.Info("Deployed prepull DaemonSet", "daemonset", s.name, "images", filtered, "output", strings.TrimSpace(out))
		}
	}
}

// rcBuildPrepullDaemonSetManifest returns a JSON DaemonSet manifest for image pre-pulling.
// The script pulls all target images then sleeps indefinitely (DaemonSet pods must stay running).
func rcBuildPrepullDaemonSetManifest(dsName, nodeSelectorKey, nodeSelectorVal string, images []string, registrySecretName string) string {
	script := rcBuildDaemonSetPrepullScript(images)
	scriptB64 := base64.StdEncoding.EncodeToString([]byte(script))

	var envJSON string
	if registrySecretName != "" {
		envJSON = fmt.Sprintf(`,
          "env": [
            {"name": "REGISTRY_USER", "valueFrom": {"secretKeyRef": {"name": %s, "key": "username", "optional": true}}},
            {"name": "REGISTRY_PASS", "valueFrom": {"secretKeyRef": {"name": %s, "key": "password", "optional": true}}}
          ]`, jsonString(registrySecretName), jsonString(registrySecretName))
	}

	return fmt.Sprintf(`{
  "apiVersion": "apps/v1",
  "kind": "DaemonSet",
  "metadata": {"name": %s, "namespace": %s, "labels": {"app": %s}},
  "spec": {
    "selector": {"matchLabels": {"app": %s}},
    "template": {
      "metadata": {"labels": {"app": %s}},
      "spec": {
        "nodeSelector": {%s: %s},
        "tolerations": [{"operator": "Exists"}],
        "containers": [{
          "name": "prepull",
          "image": "ubuntu:22.04",
          "command": ["/bin/bash", "-c"],
          "args": ["echo %s | base64 -d | bash"],
          "securityContext": {"privileged": true}%s,
          "volumeMounts": [
            {"name": "crictl",      "mountPath": "/usr/local/bin/crictl"},
            {"name": "cri-socket",  "mountPath": "/var/run/crio/crio.sock"},
            {"name": "crictl-conf", "mountPath": "/etc/crictl.yaml"}
          ]
        }],
        "volumes": [
          {"name": "crictl",      "hostPath": {"path": "/usr/local/bin/crictl",    "type": "File"}},
          {"name": "cri-socket",  "hostPath": {"path": "/var/run/crio/crio.sock", "type": "Socket"}},
          {"name": "crictl-conf", "hostPath": {"path": "/etc/crictl.yaml",        "type": "File"}}
        ]
      }
    }
  }
}`, jsonString(dsName), jsonString(prepullNamespace), jsonString(dsName), jsonString(dsName), jsonString(dsName),
		jsonString(nodeSelectorKey), jsonString(nodeSelectorVal), scriptB64, envJSON)
}

// rcBuildDaemonSetPrepullScript returns a bash script that pulls images then sleeps.
// DaemonSet pods must keep running, so the script ends with exec sleep infinity.
func rcBuildDaemonSetPrepullScript(images []string) string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString("CRICTL=/usr/local/bin/crictl\n")
	b.WriteString("ENDPOINT=unix:///var/run/crio/crio.sock\n")
	b.WriteString("if [ -n \"${REGISTRY_USER:-}\" ] && [ -n \"${REGISTRY_PASS:-}\" ]; then\n")
	b.WriteString("  CREDS=\"--creds ${REGISTRY_USER}:${REGISTRY_PASS}\"\n")
	b.WriteString("else\n")
	b.WriteString("  CREDS=\"\"\n")
	b.WriteString("fi\n")
	for _, img := range images {
		img = strings.TrimSpace(img)
		if img == "" {
			continue
		}
		// shQuote: an image reference must never be interpreted by the shell.
		fmt.Fprintf(&b, "printf '[prepull] Pulling %%s...\\n' %s\n", shQuote(img))
		fmt.Fprintf(&b, "$CRICTL --runtime-endpoint \"$ENDPOINT\" pull $CREDS %s\n", shQuote(img))
	}
	b.WriteString("echo \"[prepull] Done.\"\n")
	b.WriteString("exec sleep infinity\n")
	return b.String()
}

// ensureLocalNodeProvisionNetConfig creates or updates a NodeProvisionNetConfig
// object on THIS (management) cluster — the one the ml-nodeprovision controller
// reads from via requireNetConfig when provisioning new AWS/on-prem nodes that
// need to VPN-join this logical cluster.
//
// This is distinct from handleCreateUpdateNodeProvisionConfig, which applies an
// equivalent object to the REMOTE managed cluster over SSH (for that cluster's
// own on-prem worker onboarding). Before this function existed, nothing ever
// synced RemoteCluster.Spec.VPNConfig into a local NodeProvisionNetConfig: the
// local object had to be hand-authored and kept in sync manually, which is
// exactly how it ended up with an empty vpnSshCredentialsRef even though the
// RemoteCluster itself had the correct value all along. Built from typed Go
// structs (not string-templated YAML) so a field can't be silently dropped.
//
// clusterParent must be the control-plane RemoteCluster whose Spec.VPNConfig
// and Spec.NodeInfo.SoftwareConfig are authoritative for the shared VPN server
// and cluster software config; cluster is only used for its Spec.ClusterName
// and Namespace, which are identical between a control-plane and its workers.
// desiredNodeProvisionNetConfigFields computes every NodeProvisionNetConfig
// spec field that must mirror a control-plane RemoteCluster — clusterName,
// vpnRange, vpnServerPublicConfig, and softwareConfig minus CnlabRuntime
// (which has its own credential-secret-aware sync path,
// syncCnlabCredentialsToRemote, with its own retry/terminal-condition
// tracking, and must not be clobbered by a spec-only patch here).
//
// This is the SINGLE source of truth for both sync paths — the local
// (management-cluster) object via ensureLocalNodeProvisionNetConfig, and the
// remote (SSH-pushed) object via syncNetConfigToRemote — so the two can never
// again drift apart on which fields they cover the way they previously did
// (a remote object was found with clusterName, softwareConfig.kubernetesVersion,
// imagePrepulls and imagePullSecretRef all missing, because the one-time SSH
// "create" push that was supposed to set them either never fully ran or ran
// before those fields existed, and nothing ever re-synced them afterward).
func desiredNodeProvisionNetConfigFields(clusterParent *infrav1.RemoteCluster) mlv1alpha1.NodeProvisionNetConfigSpec {
	vpnDisabled := effectiveVPNDisabled(clusterParent)
	spec := mlv1alpha1.NodeProvisionNetConfigSpec{
		ClusterName: clusterParent.Spec.ClusterName,
		DisableVPN:  vpnDisabled,
	}

	// A control-plane with the VPN disabled has no VPN range or server for
	// nodes to join; leave those fields empty.
	if !vpnDisabled {
		if vpnCIDR := VPNRangeToCIDR(clusterParent.Spec.VPNConfig.IP); vpnCIDR != "" {
			spec.VPNRange = &vpnCIDR
		}

		vpn := clusterParent.Spec.VPNConfig
		spec.VPNServerPublicConfig = mlv1alpha1.VPNServerConfig{
			PublicIP:    vpn.VPNServerPublicIP,
			SSHPort:     defaultString(vpn.VPNServerSSHPort, "22"),
			SSHUsername: defaultString(vpn.VPNServerSSHUsername, "ubuntu"),
			VPNSSHCredentialsRef: mlv1alpha1.VPNSSHCredentialsRef{
				Name:      vpn.VPNSSHCredentialsRef.Name,
				NameSpace: vpnCredRefNamespace(clusterParent, vpn.VPNSSHCredentialsRef),
				Key:       vpn.VPNSSHCredentialsRef.Key,
			},
		}
	}

	sw := clusterParent.Spec.NodeInfo.SoftwareConfig
	spec.SoftwareConfig.KubernetesVersion = sw.KubernetesVersion

	if len(sw.ImagePrepulls) > 0 {
		prepulls := make([]mlv1alpha1.ImagePrepull, len(sw.ImagePrepulls))
		for i, ip := range sw.ImagePrepulls {
			prepulls[i] = mlv1alpha1.ImagePrepull{Image: ip.Image, NodeTarget: ip.NodeTarget}
		}
		spec.SoftwareConfig.ImagePrepulls = prepulls
	}

	if ref := sw.ImagePullSecretRef; ref != nil {
		spec.SoftwareConfig.ImagePullSecretRef = &mlv1alpha1.SecretKeyReference{Name: ref.Name, Key: ref.Key}
	}

	if len(sw.InsecureRegistries) > 0 {
		spec.SoftwareConfig.InsecureRegistries = append([]string(nil), sw.InsecureRegistries...)
	}

	return spec
}

// ensureLocalNodeProvisionNetConfig creates or updates a NodeProvisionNetConfig
// object on THIS (management) cluster — the one the ml-nodeprovision
// controller running here reads from via requireNetConfig when provisioning
// new AWS/on-prem nodes that need to VPN-join this logical cluster.
func (r *RemoteClusterReconciler) ensureLocalNodeProvisionNetConfig(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	clusterParent *infrav1.RemoteCluster,
) error {
	log := logf.FromContext(ctx)
	name := cluster.Spec.ClusterName + "-netconfig"

	nc := &mlv1alpha1.NodeProvisionNetConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}
	res, err := controllerutil.CreateOrUpdate(ctx, r.Client, nc, func() error {
		desired := desiredNodeProvisionNetConfigFields(clusterParent)
		nc.Spec.ClusterName = desired.ClusterName
		nc.Spec.DisableVPN = desired.DisableVPN
		nc.Spec.VPNRange = desired.VPNRange
		nc.Spec.VPNServerPublicConfig = desired.VPNServerPublicConfig
		nc.Spec.SoftwareConfig.KubernetesVersion = desired.SoftwareConfig.KubernetesVersion
		nc.Spec.SoftwareConfig.ImagePrepulls = desired.SoftwareConfig.ImagePrepulls
		nc.Spec.SoftwareConfig.ImagePullSecretRef = desired.SoftwareConfig.ImagePullSecretRef
		nc.Spec.SoftwareConfig.InsecureRegistries = desired.SoftwareConfig.InsecureRegistries
		// CnlabRuntime is deliberately left untouched here — synced separately
		// by syncCnlabCredentialsToRemote, which also handles the credentials
		// Secret it references.
		return nil
	})
	if err != nil {
		return fmt.Errorf("syncing local NodeProvisionNetConfig %q: %w", name, err)
	}
	if res != controllerutil.OperationResultNone {
		log.Info("Synced local NodeProvisionNetConfig from RemoteCluster",
			"netconfig", name, "operation", res)
	}
	return nil
}

// defaultString returns s if non-empty, otherwise fallback.
func defaultString(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func (r *RemoteClusterReconciler) handleCreateUpdateNodeProvisionConfig( //nolint:unparam
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	clusterParent *infrav1.RemoteCluster,
	sshClient *sshhelper.Client,
	nodeIP,
	action string,
) (ctrl.Result, error) {

	log := logf.FromContext(ctx).WithValues(
		"cluster",
		cluster.Name,
	)

	// Everything below is about what is (or is being) configured on the nodes, so
	// it must follow the VPN mode they were provisioned with, not the mutable
	// spec.disableVPN. Work on copies; the originals are never written back.
	cluster = withEffectiveVPNMode(cluster)
	clusterParent = withEffectiveVPNMode(clusterParent)

	// ============================================================
	// Resolve wg0 IP from remote node
	// ============================================================
	// For "create" the caller passes the configured VPN IP; verify via SSH so we
	// get the actual address assigned to wg0 (they should match, but this catches
	// any misconfiguration early).
	// For "update" the caller passes the worker's VPN IP (returned by JoinWorkerNode);
	// do NOT re-resolve via SSH here because sshClient is the control-plane client and
	// would always return the control-plane's IP, not the worker's.

	if action == "create" {
		if cluster.Spec.DisableVPN {
			// No wg0: the control-plane's own address is its node IP.
			var err error
			nodeIP, err = kubeadm.ResolveNodeIP(sshClient, cluster)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("resolving control-plane node ip: %w", err)
			}
		} else {
			output, err := sshhelper.RunStdout(
				sshClient,
				"ip -4 addr show wg0 | grep -oP '(?<=inet\\s)\\d+(\\.\\d+){3}'",
			)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("getting wg0 ip: %w", err)
			}
			nodeIP = strings.TrimSpace(output)
			if nodeIP == "" {
				return ctrl.Result{}, fmt.Errorf("empty wg0 ip")
			}
		}
	}

	if nodeIP == "" {
		return ctrl.Result{}, fmt.Errorf("nodeIP is empty")
	}

	log.Info("Resolved node IP", "nodeIP", nodeIP, "vpnDisabled", cluster.Spec.DisableVPN)

	// ============================================================
	// CREATE
	// ============================================================

	if action == "create" {

		vpnCIDR := VPNRangeToCIDR(nodeIP)

		// Ensure the VPN SSH credentials secret exists on the remote cluster so
		// the NodeProvisionNetConfig controller there can read it.
		vpnRef := cluster.Spec.VPNConfig.VPNSSHCredentialsRef
		vpnNS := vpnCredRefNamespace(cluster, vpnRef)
		if !cluster.Spec.DisableVPN && vpnRef.Name != "" {
			vpnSecret := &corev1.Secret{}
			if err := r.Get(ctx, types.NamespacedName{Name: vpnRef.Name, Namespace: vpnNS}, vpnSecret); err != nil {
				return ctrl.Result{}, fmt.Errorf("fetching VPN SSH credentials secret %q: %w", vpnRef.Name, err)
			}

			secretOutput, secretErr := sshhelper.Run(sshClient,
				secretApplyCmd(vpnSecret.Name, vpnNS, vpnSecret.Type, vpnSecret.Data))
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
		// NodeProvision controller there can authenticate when pre-pulling images,
		// and so the prepull DaemonSets (fixed to prepullNamespace) can read it.
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

			targetNamespaces := []string{cluster.Namespace}
			if cluster.Namespace != prepullNamespace {
				targetNamespaces = append(targetNamespaces, prepullNamespace)
			}
			for _, ns := range targetNamespaces {
				pullSecretOutput, pullSecretErr := sshhelper.Run(sshClient,
					secretApplyCmd(pullSecret.Name, ns, pullSecret.Type, pullSecret.Data))
				if pullSecretErr != nil {
					return ctrl.Result{}, fmt.Errorf(
						"creating image pull secret in namespace %q on remote cluster: %w\nOutput:\n%s",
						ns, pullSecretErr, pullSecretOutput,
					)
				}
				log.Info("Ensured image pull secret on remote cluster", "secret", pullSecret.Name, "namespace", ns)
			}
		}

		// Copy the cnlab-runtime registry credentials secret to the remote cluster
		// so its NodeProvisionReconciler can pull the runtime artifact autonomously —
		// even when the management cluster is unreachable.
		if cr := clusterParent.Spec.NodeInfo.SoftwareConfig.CnlabRuntime; cr != nil && cr.CredentialsRef.Name != "" {
			runtimeCredsSecret := &corev1.Secret{}
			ns := cr.CredentialsRef.NameSpace
			if ns == "" {
				ns = cluster.Namespace
			}
			if err := r.Get(ctx, types.NamespacedName{
				Name:      cr.CredentialsRef.Name,
				Namespace: ns,
			}, runtimeCredsSecret); err != nil {
				return ctrl.Result{}, fmt.Errorf(
					"fetching cnlab-runtime credentials secret %q: %w",
					cr.CredentialsRef.Name, err,
				)
			}

			if runtimeSecretOutput, runtimeSecretErr := sshhelper.Run(sshClient,
				secretApplyCmd(runtimeCredsSecret.Name, ns, runtimeCredsSecret.Type, runtimeCredsSecret.Data)); runtimeSecretErr != nil {
				return ctrl.Result{}, fmt.Errorf(
					"creating cnlab-runtime credentials secret on remote cluster: %w\nOutput:\n%s",
					runtimeSecretErr, runtimeSecretOutput,
				)
			}
			log.Info("Ensured cnlab-runtime credentials secret on remote cluster", "secret", runtimeCredsSecret.Name)
		}

		// Build optional softwareConfig fields (indented to match sibling keys).
		// Every interpolated value goes through yamlQuote so it cannot break out
		// of its scalar.
		var imagePrepullsYAML string
		if len(clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePrepulls) > 0 {
			imagePrepullsYAML = "    imagePrepulls:\n"
			for _, ip := range clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePrepulls {
				imagePrepullsYAML += fmt.Sprintf("    - image: %s\n      nodeTarget: %s\n", yamlQuote(ip.Image), yamlQuote(ip.NodeTarget))
			}
		}
		if ref := clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePullSecretRef; ref != nil {
			imagePrepullsYAML += fmt.Sprintf("    imagePullSecretRef:\n      name: %s\n", yamlQuote(ref.Name))
		}
		if regs := clusterParent.Spec.NodeInfo.SoftwareConfig.InsecureRegistries; len(regs) > 0 {
			imagePrepullsYAML += "    insecureRegistries:\n"
			for _, h := range regs {
				imagePrepullsYAML += fmt.Sprintf("    - %s\n", yamlQuote(h))
			}
		}
		// Propagate cnlabRuntime config so the remote cluster's NodeProvisionReconciler
		// can pull the runtime artifact without connecting back to the management cluster.
		var cnlabRuntimeYAML string
		if cr := clusterParent.Spec.NodeInfo.SoftwareConfig.CnlabRuntime; cr != nil {
			cnlabRuntimeYAML = "    cnlabRuntime:\n"
			if cr.Registry != "" {
				cnlabRuntimeYAML += fmt.Sprintf("      registry: %s\n", yamlQuote(cr.Registry))
			}
			if cr.Repository != "" {
				cnlabRuntimeYAML += fmt.Sprintf("      repository: %s\n", yamlQuote(cr.Repository))
			}
			if cr.Version != "" {
				cnlabRuntimeYAML += fmt.Sprintf("      version: %s\n", yamlQuote(cr.Version))
			}
			if cr.OrasVersion != "" {
				cnlabRuntimeYAML += fmt.Sprintf("      orasVersion: %s\n", yamlQuote(cr.OrasVersion))
			}
			if cr.CredentialsRef.Name != "" {
				cnlabRuntimeYAML += fmt.Sprintf("      credentialsRef:\n        name: %s\n        namespace: %s\n",
					yamlQuote(cr.CredentialsRef.Name),
					yamlQuote(vpnCredRefNamespace(clusterParent, cr.CredentialsRef)),
				)
			}
		}

		vpnServerSSHPort := cluster.Spec.VPNConfig.VPNServerSSHPort
		if vpnServerSSHPort == "" {
			vpnServerSSHPort = "22"
		}
		vpnServerSSHUsername := cluster.Spec.VPNConfig.VPNServerSSHUsername
		if vpnServerSSHUsername == "" {
			vpnServerSSHUsername = "ubuntu"
		}

		// VPN section of the spec — omitted entirely when the VPN is disabled.
		var vpnSpecYAML string
		if !cluster.Spec.DisableVPN {
			vpnSpecYAML = fmt.Sprintf(`  vpnRange: %s
  vpnServerPublicConfig:
    publicIP: %s
    sshPort: %s
    sshUsername: %s
    vpnSshCredentialsRef:
      name: %s
      namespace: %s
`,
				yamlQuote(vpnCIDR),
				yamlQuote(cluster.Spec.VPNConfig.VPNServerPublicIP),
				yamlQuote(vpnServerSSHPort),
				yamlQuote(vpnServerSSHUsername),
				yamlQuote(vpnRef.Name),
				yamlQuote(vpnNS),
			)
		}

		netConfigYAML := fmt.Sprintf(`
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvisionNetConfig
metadata:
  name: %s
  namespace: %s
spec:
  clusterName: %s
  disableVPN: %t
  softwareConfig:
    kubernetesVersion: %s
%s%s%s`,
			yamlQuote(cluster.Spec.ClusterName+"-netconfig"),
			yamlQuote(cluster.Namespace),
			yamlQuote(cluster.Spec.ClusterName),
			cluster.Spec.DisableVPN,
			yamlQuote(clusterParent.Spec.NodeInfo.SoftwareConfig.KubernetesVersion),
			imagePrepullsYAML,
			cnlabRuntimeYAML,
			vpnSpecYAML,
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

		// kubectl apply ignores the status subresource — patch the static fields
		// (join command + initial IP) with a single-quoted JSON body so that no
		// shell variable expansion can corrupt the value.
		joinCmdJSON, _ := json.Marshal(cluster.Status.JoinCommand)
		// usedIPAddresses tracks VPN-range allocations; the node IP of a
		// VPN-less control-plane is not one.
		usedIPsJSON := fmt.Sprintf(`,"usedIPAddresses":[%s]`, jsonString(nodeIP))
		if cluster.Spec.DisableVPN {
			usedIPsJSON = ""
		}
		staticPatchCmd := fmt.Sprintf(
			`kubectl patch nodeprovisionnetconfig %s -n %s --type=merge --subresource=status -p %s`,
			shQuote(cluster.Spec.ClusterName+"-netconfig"),
			shQuote(cluster.Namespace),
			shQuote(fmt.Sprintf(`{"status":{"clusterJoinCommand":%s%s}}`, string(joinCmdJSON), usedIPsJSON)),
		)
		staticOutput, staticErr := sshhelper.Run(sshClient, staticPatchCmd)
		if staticErr != nil {
			return ctrl.Result{}, fmt.Errorf(
				"patching NodeProvisionNetConfig status: %w\nOutput:\n%s",
				staticErr,
				staticOutput,
			)
		}

		// Embed the admin kubeconfig in a second patch so the remote cluster can
		// stay self-sufficient without the management cluster.  Use sudo to read
		// the root-owned admin.conf; failure here is non-fatal — the refresh
		// timer installed below will populate it shortly.
		kcPatchCmd := fmt.Sprintf(
			`KUBECONFIG_B64=$(sudo cat /etc/kubernetes/admin.conf | base64 -w0 2>/dev/null || `+
				`sudo cat /etc/kubernetes/admin.conf | base64 | tr -d '\n') && `+
				`[ -n "$KUBECONFIG_B64" ] && `+
				`kubectl patch nodeprovisionnetconfig %s -n %s --type=merge --subresource=status `+
				`-p "{\"status\":{\"kubeconfig\":\"$KUBECONFIG_B64\"}}" || true`,
			shQuote(cluster.Spec.ClusterName+"-netconfig"),
			shQuote(cluster.Namespace),
		)
		kcOutput, kcErr := sshhelper.Run(sshClient, kcPatchCmd)
		if kcErr != nil {
			log.Error(kcErr, "embedding kubeconfig in NetConfig status (non-fatal)", "output", kcOutput)
		}

		log.Info("Patched NodeProvisionNetConfig status")

		// Deploy the kubeconfig-refresh systemd timer so the remote cluster can
		// renew its own admin kubeconfig and keep this status field fresh without
		// any involvement from the management cluster.
		if err := r.deployKubeconfigRefreshTimer(ctx, cluster, sshClient); err != nil {
			log.Error(err, "deploying kubeconfig-refresh timer (non-fatal)")
		}

		// Deploy image pre-pull DaemonSets on the CP so they automatically schedule
		// on each worker as it joins and gets labeled (see reconcileWorker).
		var registrySecretName string
		if ref := clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePullSecretRef; ref != nil {
			registrySecretName = ref.Name
		}
		deployPrepullDaemonSets(ctx, sshClient, cluster.Spec.ClusterName,
			clusterParent.Spec.NodeInfo.SoftwareConfig.ImagePrepulls, registrySecretName)
	}

	// ============================================================
	// UPDATE
	// ============================================================

	if action == "update" && cluster.Spec.DisableVPN {
		log.Info("Worker has no VPN IP — not recording it in usedIPAddresses", "nodeIP", nodeIP)
	} else if action == "update" {
		// Append the worker's VPN IP to usedIPAddresses only if not already present.
		// Using a shell read-then-write to avoid races and duplicates — the JSON patch
		// "add to array" op has no native dedup.
		appendCmd := fmt.Sprintf(`
NAME=%s; NS=%s; IP=%s
EXISTING=$(kubectl get nodeprovisionnetconfig "$NAME" -n "$NS" \
  -o jsonpath='{.status.usedIPAddresses[*]}' 2>/dev/null || true)
for x in $EXISTING; do
  [ "$x" = "$IP" ] && echo "$IP already in usedIPAddresses, skipping" && exit 0
done
kubectl patch nodeprovisionnetconfig "$NAME" -n "$NS" \
  --type='json' --subresource=status \
  -p="[{\"op\":\"add\",\"path\":\"/status/usedIPAddresses/-\",\"value\":\"$IP\"}]"
`,
			shQuote(cluster.Spec.ClusterName+"-netconfig"),
			shQuote(cluster.Namespace),
			shQuote(nodeIP),
		)

		output, err := sshhelper.Run(sshClient, appendCmd)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf(
				"patching remote NodeProvisionNetConfig: %w\nOutput:\n%s",
				err, output,
			)
		}

		log.Info("Updated NodeProvisionNetConfig remotely", "nodeIP", nodeIP)
	}

	return ctrl.Result{}, nil
}

// needsTokenRefresh returns true when the bootstrap token is absent or due for renewal.
func needsTokenRefresh(cluster *infrav1.RemoteCluster) bool {
	if cluster.Status.JoinCommand == "" {
		return false // no token yet; provisioning hasn't finished
	}
	ts, ok := cluster.Annotations[annotationJoinTokenRefreshedAt]
	if !ok {
		return true // never refreshed
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return true
	}
	return time.Since(t) >= tokenRefreshInterval
}

// refreshJoinToken SSHes into the control-plane, creates a new kubeadm bootstrap
// token, and updates both the RemoteCluster status and the local NodeProvisionNetConfig.
func (r *RemoteClusterReconciler) refreshJoinToken(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
	defer cancel()
	sshClient, err := r.getSSHClient(sshCtx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("SSH connect for token refresh: %w", err)
	}
	defer func() { _ = sshClient.Conn.Close() }()

	// --kubeconfig is explicit rather than relying on kubeadm's default
	// resolution — see the identical comment in pkg/kubeadm/kubeadm.go's
	// getJoinCommand for why: a stray KUBECONFIG env var on the target host
	// can silently redirect kubeadm at an unrelated API server.
	// Stdout only: the output is stored as the join command.
	out, err := sshhelper.RunStdoutCtx(sshCtx, sshClient, "kubeadm token create --print-join-command --kubeconfig=/etc/kubernetes/admin.conf 2>/dev/null")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("kubeadm token create: %w\nOutput: %s", err, out)
	}
	newJoinCmd := strings.TrimSpace(out)
	if newJoinCmd == "" {
		return ctrl.Result{}, fmt.Errorf("kubeadm token create returned empty output")
	}

	log.Info("Refreshed kubeadm bootstrap token", "nextRefreshIn", tokenRefreshInterval.String())

	// Persist the new join command on the RemoteCluster status.
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("refreshing cluster before token status update: %w", err)
	}
	cluster.Status.JoinCommand = newJoinCmd
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating RemoteCluster join command: %w", err)
	}

	// Patch the remote cluster's netconfig so NodeProvision controllers running
	// on the remote cluster see the refreshed token and the latest kubeconfig.
	if err := r.patchRemoteNetConfigJoinCmd(ctx, cluster, sshClient, newJoinCmd); err != nil {
		log.Error(err, "patching remote netconfig join command (non-fatal)")
	}
	if err := r.patchRemoteNetConfigKubeconfig(ctx, cluster, sshClient); err != nil {
		log.Error(err, "patching remote netconfig kubeconfig (non-fatal)")
	}

	refreshedAt := time.Now().UTC().Format(time.RFC3339)
	if patchErr := r.patchAnnotation(ctx, cluster, annotationJoinTokenRefreshedAt, refreshedAt); patchErr != nil {
		log.Error(patchErr, "Failed to stamp join-token-refreshed-at annotation")
	} else {
		// Keep the caller's copy current: it computes the next wake-up from it.
		ensureAnnotations(cluster)[annotationJoinTokenRefreshedAt] = refreshedAt
	}

	return ctrl.Result{RequeueAfter: tokenRefreshInterval}, nil
}

// deployKubeconfigRefreshTimer installs a systemd service + timer on the
// control-plane node that periodically runs `kubeadm certs renew admin.conf`
// and patches the NodeProvisionNetConfig status kubeconfig field.  The timer
// fires every 30 days so certs are renewed well before the 1-year expiry.
// Once installed it operates entirely locally — no management-cluster contact.
func (r *RemoteClusterReconciler) deployKubeconfigRefreshTimer(ctx context.Context, cluster *infrav1.RemoteCluster, sshClient *sshhelper.Client) error {
	log := logf.FromContext(ctx)

	clusterName := cluster.Spec.ClusterName
	namespace := cluster.Namespace

	// Shell script that renews certs and patches the netconfig status.
	script := fmt.Sprintf(`#!/bin/bash
set -euo pipefail

# Renew the admin client certificate (no-op if not yet close to expiry).
kubeadm certs renew admin.conf

# Propagate the renewed kubeconfig to the standard location.
cp -f /etc/kubernetes/admin.conf "$HOME/.kube/config" 2>/dev/null || true

# Discover the netconfig name and namespace.
NETCONFIG_NAME=%s
NAMESPACE=%s

# Base64-encode the fresh kubeconfig (GNU and BSD base64 compatible).
KUBECONFIG_B64=$(base64 -w0 /etc/kubernetes/admin.conf 2>/dev/null || \
                 base64 /etc/kubernetes/admin.conf | tr -d '\n')

# Patch the NodeProvisionNetConfig status.
kubectl patch nodeprovisionnetconfig "$NETCONFIG_NAME" -n "$NAMESPACE" \
  --type=merge --subresource=status \
  -p "{\"status\":{\"kubeconfig\":\"$KUBECONFIG_B64\"}}"

echo "kubeconfig-refresh: done"
`, shQuote(clusterName+"-netconfig"), shQuote(namespace))

	serviceUnit := `[Unit]
Description=Renew kubeadm admin kubeconfig and update NodeProvisionNetConfig
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/kubeconfig-refresh.sh
StandardOutput=journal
StandardError=journal
`

	timerUnit := `[Unit]
Description=Periodic kubeconfig renewal (every 30 days)

[Timer]
OnBootSec=5min
OnUnitActiveSec=30d
Persistent=true

[Install]
WantedBy=timers.target
`

	installCmd := fmt.Sprintf(`
set -e
# Write the refresh script.
sudo tee /usr/local/bin/kubeconfig-refresh.sh > /dev/null <<'SCRIPT'
%s
SCRIPT
sudo chmod 0755 /usr/local/bin/kubeconfig-refresh.sh

# Write the systemd service unit.
sudo tee /etc/systemd/system/kubeconfig-refresh.service > /dev/null <<'SERVICE'
%s
SERVICE

# Write the systemd timer unit.
sudo tee /etc/systemd/system/kubeconfig-refresh.timer > /dev/null <<'TIMER'
%s
TIMER

sudo systemctl daemon-reload
sudo systemctl enable --now kubeconfig-refresh.timer
echo "kubeconfig-refresh timer enabled"
`, script, serviceUnit, timerUnit)

	out, err := sshhelper.Run(sshClient, installCmd)
	if err != nil {
		return fmt.Errorf("installing kubeconfig-refresh timer: %w\nOutput: %s", err, out)
	}
	log.Info("kubeconfig-refresh timer deployed on control-plane", "cluster", clusterName)
	return nil
}

// patchRemoteNetConfigJoinCmd updates the ClusterJoinCommand and
// JoinTokenRefreshedAt fields on the remote cluster's NodeProvisionNetConfig
// via SSH.  The timestamp lets the NodeProvision controller verify that the
// embedded bootstrap token is still within its 24-hour lifetime before
// launching a new EC2 instance.
func (r *RemoteClusterReconciler) patchRemoteNetConfigJoinCmd(ctx context.Context, cluster *infrav1.RemoteCluster, sshClient *sshhelper.Client, joinCmd string) error {
	log := logf.FromContext(ctx)
	joinCmdJSON, _ := json.Marshal(joinCmd)
	nowJSON, _ := json.Marshal(time.Now().UTC().Format(time.RFC3339))
	cmd := fmt.Sprintf(
		`kubectl patch nodeprovisionnetconfig %s -n %s --type=merge --subresource=status -p %s`,
		shQuote(cluster.Spec.ClusterName+"-netconfig"),
		shQuote(cluster.Namespace),
		shQuote(fmt.Sprintf(`{"status":{"clusterJoinCommand":%s,"joinTokenRefreshedAt":%s}}`, string(joinCmdJSON), string(nowJSON))),
	)
	out, err := sshhelper.Run(sshClient, cmd)
	if err != nil {
		return fmt.Errorf("patching remote netconfig join command: %w\nOutput: %s", err, out)
	}
	log.Info("Patched remote NodeProvisionNetConfig join command")
	return nil
}

// patchRemoteNetConfigKubeconfig reads /etc/kubernetes/admin.conf on the
// control-plane node and patches the kubeconfig status field.  Called both
// during initial setup and during token refresh so the stored kubeconfig
// stays current when the management cluster is connected.
func (r *RemoteClusterReconciler) patchRemoteNetConfigKubeconfig(ctx context.Context, cluster *infrav1.RemoteCluster, sshClient *sshhelper.Client) error {
	log := logf.FromContext(ctx)
	// Use sudo cat to read the root-owned admin.conf (mode 600).
	// Guard with [ -n ] so kubectl never runs with an empty variable.
	cmd := fmt.Sprintf(
		`KUBECONFIG_B64=$(sudo cat /etc/kubernetes/admin.conf | base64 -w0 2>/dev/null || `+
			`sudo cat /etc/kubernetes/admin.conf | base64 | tr -d '\n') && `+
			`[ -n "$KUBECONFIG_B64" ] && `+
			`kubectl patch nodeprovisionnetconfig %s -n %s `+
			`--type=merge --subresource=status `+
			`-p "{\"status\":{\"kubeconfig\":\"$KUBECONFIG_B64\"}}"`,
		shQuote(cluster.Spec.ClusterName+"-netconfig"),
		shQuote(cluster.Namespace),
	)
	out, err := sshhelper.Run(sshClient, cmd)
	if err != nil {
		return fmt.Errorf("patching remote netconfig kubeconfig: %w\nOutput: %s", err, out)
	}
	log.Info("Patched remote NodeProvisionNetConfig kubeconfig")
	return nil
}

// setStatus upserts the progress condition for this step (keyed by its Type, so
// the list stays bounded and a repeated step updates rather than duplicates) and
// writes the phase/message.
func (r *RemoteClusterReconciler) setStatus(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	phase, reason, message string,
	isError bool,
) error {
	message = truncateMessage(message)
	cluster.Status.Phase = phase
	cluster.Status.Message = message

	// Reset the retry counter whenever provisioning reaches a success phase.
	if phase == phaseReady && cluster.Status.ProvisionRetryCount > 0 {
		logf.FromContext(ctx).Info("RemoteCluster provisioning succeeded — resetting retry counter",
			"previousAttempts", cluster.Status.ProvisionRetryCount)
		cluster.Status.ProvisionRetryCount = 0
	}

	// Reaching Ready supersedes every earlier failure: fail()/softFail() record a
	// failure as a condition of Status False keyed by its reason, and nothing else
	// would ever clear those, leaving e.g. ControlPlaneInitFailed=False on a
	// working cluster. The cnlab credential-sync condition has its own lifecycle
	// (it also gates retries) and is kept.
	if phase == phaseReady && !isError {
		kept := cluster.Status.Conditions[:0:0]
		for _, c := range cluster.Status.Conditions {
			if c.Status == metav1.ConditionFalse && c.Type != cnlabSyncConditionType {
				continue
			}
			kept = append(kept, c)
		}
		cluster.Status.Conditions = kept
	}

	condStatus := metav1.ConditionTrue
	if isError {
		condStatus = metav1.ConditionFalse
	}

	upsertCondition(cluster, metav1.Condition{
		Type:    reason,
		Status:  condStatus,
		Reason:  reason,
		Message: message,
	})

	return r.Status().Update(ctx, cluster)
}

// fail re-fetches the RemoteCluster, increments ProvisionRetryCount, and
// persists a Failed status via RetryOnConflict so the counter is never silently lost.
func (r *RemoteClusterReconciler) fail(
	ctx context.Context,
	cluster *infrav1.RemoteCluster,
	reason string,
	cause error,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var terminal bool
	var attempts int

	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &infrav1.RemoteCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
			return err
		}
		fresh.Status.ProvisionRetryCount++
		attempts = fresh.Status.ProvisionRetryCount
		var msg string
		if fresh.Status.ProvisionRetryCount >= maxProvisionRetries {
			terminal = true
			msg = fmt.Sprintf("provisioning failed after %d attempts (last error: %v) — manual intervention required",
				fresh.Status.ProvisionRetryCount, cause)
		} else {
			msg = fmt.Sprintf("provisioning failed (attempt %d/%d): %v",
				fresh.Status.ProvisionRetryCount, maxProvisionRetries, cause)
		}
		return r.setStatus(ctx, fresh, phaseFailed, reason, msg, true)
	})
	if updateErr != nil {
		log.Error(updateErr, "failed to persist provisioning failure status",
			"cluster", cluster.Name, "reason", reason)
	}

	if terminal {
		log.Error(cause, "RemoteCluster provisioning reached retry limit — no further retries",
			"cluster", cluster.Name,
			"reason", reason,
			"attempts", attempts,
			"maxRetries", maxProvisionRetries)
		return ctrl.Result{}, nil
	}
	log.Error(cause, "RemoteCluster provisioning failed — will retry",
		"cluster", cluster.Name,
		"reason", reason,
		"attempt", attempts,
		"maxRetries", maxProvisionRetries)
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
	defer sshClient.Conn.Close() //nolint:errcheck

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
	if err := r.ensureLocalNodeProvisionNetConfig(ctx, cluster, cluster); err != nil {
		log.Error(err, "syncing local NodeProvisionNetConfig (non-fatal)")
	}

	// All side-effects done — now flip to Ready and persist the join command.
	// Retry on conflict: long-running SSH work means our copy may be stale.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return err
		}
		cluster.Status.JoinCommand = joinCommand
		return r.setStatus(ctx, cluster, phaseReady, "Provisioned", "Cluster provisioned successfully", false)
	}); err != nil {
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

// reconcileVPNMode keeps a worker's VPN mode consistent with its
// control-plane's, which decides for the whole cluster: flannel is pinned to
// wg0 on VPN clusters and the API server is advertised on the wg0 (or, without
// a VPN, the host) address, so a mixed cluster cannot work.
//
// spec.disableVPN is mutable, but what was actually provisioned is not: once a
// node has been provisioned its mode is recorded in annotationProvisionedVPNMode
// and that record (never the spec) drives every decision about the node. A
// node that is already provisioned is therefore never silently flipped; a
// conflict is surfaced as a condition and re-checked every minute.
//
//   - control-plane without the VPN, unprovisioned worker unset → the worker
//     inherits it (persisted, so deletion cleanup does not depend on the
//     control-plane still existing).
//   - control-plane with the VPN, unprovisioned worker disabled → the worker is
//     failed without consuming the retry budget (the user must fix the spec).
//   - provisioned worker whose mode differs from its control-plane → condition +
//     requeue, nothing is changed.
//
// done is true when the caller must return res immediately.
func (r *RemoteClusterReconciler) reconcileVPNMode(ctx context.Context, cluster *infrav1.RemoteCluster) (res ctrl.Result, done bool, err error) {
	// A worker whose record was stamped but that never got as far as completing a
	// phase (older versions stamped it before even connecting) never touched its
	// host: the record is meaningless and would otherwise wedge it behind a
	// permanent mismatch. Workers are provisioned synchronously, so no attempt
	// can be in flight here.
	if cluster.Spec.NodeInfo.NodeType == "worker" && cluster.Annotations[annotationProvisionedVPNMode] != "" &&
		r.provisioningUntouched(cluster) {
		if err := r.patchAnnotations(ctx, cluster, map[string]string{annotationProvisionedVPNMode: ""}); err != nil {
			return ctrl.Result{}, true, fmt.Errorf("releasing stale VPN mode record of an untouched worker: %w", err)
		}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return ctrl.Result{}, true, err
		}
	}
	recorded := cluster.Annotations[annotationProvisionedVPNMode]
	if recorded != "" && recorded != vpnModeString(cluster.Spec.DisableVPN) {
		// Surface that a spec edit is being ignored for an already provisioned node.
		r.setCondition(ctx, cluster, conditionVPNModeChangeIgnored, metav1.ConditionTrue, "ProvisionedModeRetained",
			fmt.Sprintf("spec.disableVPN=%t differs from the mode this node was provisioned with (%s); the provisioned mode is kept. "+
				"Re-provision the node to change it", cluster.Spec.DisableVPN, recorded))
	} else {
		r.clearConditions(ctx, cluster, conditionVPNModeChangeIgnored)
	}

	if cluster.Spec.NodeInfo.NodeType != "worker" {
		return ctrl.Result{}, false, nil
	}
	cp, err := r.findControlPlane(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, true, fmt.Errorf("listing RemoteClusters: %w", err)
	}
	if cp == nil {
		return ctrl.Result{}, false, nil
	}
	workerDisabled, cpDisabled := effectiveVPNDisabled(cluster), effectiveVPNDisabled(cp)
	if workerDisabled == cpDisabled {
		r.clearConditions(ctx, cluster, conditionVPNModeMismatch)
		return ctrl.Result{}, false, nil
	}

	if recorded != "" {
		msg := fmt.Sprintf("worker was provisioned with the VPN %s but control-plane %q uses it %s; the whole cluster must use the same mode. "+
			"The provisioned worker is left untouched — re-provision it or the control-plane",
			enabledWord(!workerDisabled), cp.Name, enabledWord(!cpDisabled))
		r.setCondition(ctx, cluster, conditionVPNModeMismatch, metav1.ConditionTrue, conditionVPNModeMismatch, msg)
		return ctrl.Result{RequeueAfter: time.Minute}, true, nil
	}

	if !cpDisabled {
		res, err := r.failNoCount(ctx, cluster, conditionVPNModeMismatch, fmt.Errorf(
			"spec.disableVPN is set but control-plane %q runs with the VPN; the whole cluster must use the same mode", cp.Name))
		return res, true, err
	}
	logf.FromContext(ctx).Info("Inheriting disableVPN from control-plane", "cp", cp.Name)
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("refreshing worker before inheriting disableVPN: %w", err)
	}
	cluster.Spec.DisableVPN = true
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("inheriting disableVPN from control-plane: %w", err)
	}
	return ctrl.Result{Requeue: true}, true, nil
}

func enabledWord(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
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
func (r *RemoteClusterReconciler) ensureAuthSecretFinalizer(ctx context.Context, _ *infrav1.RemoteCluster, secret *corev1.Secret) error {
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
	// The user-managed secret may be shared by several RemoteClusters (typically
	// every node of one cluster): only release it when no other live
	// RemoteCluster still references it. The controller-owned copy is private.
	if secret.Name != cluster.Name+controllerAuthSuffix {
		if used, err := r.secretReferenced(ctx, cluster, secret.Namespace, secret.Name,
			func(o *infrav1.RemoteCluster) (string, string) { return o.Namespace, authSecretName(o) }); err != nil {
			log.Error(err, "checking other users of the auth secret — keeping finalizer", "secret", secret.Name)
			return
		} else if used {
			log.Info("Auth secret is still referenced by another RemoteCluster — keeping finalizer", "secret", secret.Name)
			return
		}
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
func (r *RemoteClusterReconciler) ensureVPNSecretFinalizer(ctx context.Context, _ *infrav1.RemoteCluster, secret *corev1.Secret) error {
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
	// Shared by every node of a VPN cluster: release it only when no other live
	// RemoteCluster that uses the VPN still references it.
	used, err := r.secretReferenced(ctx, cluster, secret.Namespace, secret.Name,
		func(o *infrav1.RemoteCluster) (string, string) {
			if effectiveVPNDisabled(o) {
				return "", ""
			}
			ref := o.Spec.VPNConfig.VPNSSHCredentialsRef
			return vpnCredRefNamespace(o, ref), ref.Name
		})
	if err != nil {
		log.Error(err, "checking other users of the VPN secret — keeping finalizer", "secret", secret.Name)
		return
	}
	if used {
		log.Info("VPN SSH secret is still referenced by another RemoteCluster — keeping finalizer", "secret", secret.Name)
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

func (r *RemoteClusterReconciler) getSSHClient(ctx context.Context, cluster *infrav1.RemoteCluster) (*sshhelper.Client, error) {
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
	if cluster.Spec.VPNConfig.IP != "" && !effectiveVPNDisabled(cluster) {
		host = cluster.Spec.VPNConfig.IP
	} else {
		host = cluster.Spec.Host
	}

	// VPN-aware SSH tunnelling is not yet implemented; skip if VPNConfig is set.

	credential := string(credentialBytes)
	var sshClient *sshhelper.Client
	var err error
	port := parsePort(cluster.Spec.Port, 22)
	if strings.HasPrefix(strings.TrimSpace(credential), "-----BEGIN") {
		sshClient, err = sshhelper.ConnectWithPrivateKey(host, port, cluster.Spec.User, credential)
	} else {
		sshClient, err = sshhelper.Connect(host, port, cluster.Spec.User, credential)
	}
	if err != nil {
		return nil, fmt.Errorf("SSH connect to %s:%d: %w", host, port, err)
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
	if err := r.ensureNephioRepository(ctx, cluster, labels); err != nil {
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

func (r *RemoteClusterReconciler) ensureNephioRepository(ctx context.Context, cluster *infrav1.RemoteCluster, labels map[string]string) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "Repository",
	})
	obj.SetName(cluster.Spec.ClusterName)
	obj.SetNamespace(cluster.Namespace)
	// Labelled like the other per-cluster objects so deleteClusterResources
	// (which selects by label) actually finds and removes it.
	obj.SetLabels(labels)

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

func (r *RemoteClusterReconciler) handleDelete(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) { //nolint:unparam
	log := logf.FromContext(ctx)
	log.Info("Deprovisioning RemoteCluster", "name", cluster.Name, "nodeType", cluster.Spec.NodeInfo.NodeType)

	// Abort and forget any in-flight control-plane init for this object: the
	// goroutine must not keep driving a host we are about to wipe, and a stale
	// entry must not be adopted by a later object with the same name.
	r.cancelControlPlaneJob(cluster.Namespace + "/" + cluster.Name)

	if !controllerutil.ContainsFinalizer(cluster, remoteClusterFinalizer) {
		return ctrl.Result{}, nil
	}

	// Everything below that touches the node or the VPN server works on a copy
	// whose spec.disableVPN is the mode the node was actually provisioned with
	// (recorded annotation; for a node that never got that far, its
	// control-plane's mode). It is never written back.
	work := cluster.DeepCopy()
	work.Spec.DisableVPN = r.resolveVPNDisabledForDelete(ctx, cluster)

	// Steps 1-2 take minutes of SSH work and must not be repeated because a later
	// step failed and the reconcile re-entered: once attempted, they are recorded
	// on the object (the rest of the cleanup is cheap and idempotent).
	if cluster.Annotations[annotationDeleteNodeCleanupDone] != "true" {
		// Step 1: If worker, drain and remove the node from the Kubernetes cluster
		// before wiping it so workloads migrate away gracefully.
		if work.Spec.NodeInfo.NodeType == "worker" {
			r.drainWorkerFromCP(ctx, work)
		}

		// Step 2: SSH to the node — kubeadm reset, purge packages, remove configs,
		// bring down WireGuard.  Best-effort: if the node is already unreachable
		// we still proceed so the finalizer can be removed.
		if err := r.resetNodeViaSSH(ctx, work); err != nil {
			log.Error(err, "node SSH reset incomplete (continuing with cleanup)")
		}
		if err := r.patchAnnotation(ctx, cluster, annotationDeleteNodeCleanupDone, "true"); err != nil {
			log.Error(err, "recording node cleanup as done (non-fatal)")
		}
	} else {
		log.Info("Node drain/reset already attempted on a previous pass — skipping")
	}

	// Step 3: Remove the WireGuard peer from the VPN server.
	//
	// Worker CRs only get VPNSSHCredentialsRef populated on their own spec via
	// a one-time sync in reconcileWorker (see the "Synced VPN server config"
	// log line). If that sync never ran or was never persisted — e.g. the CR
	// was deleted before finishing provisioning — the worker's own spec.vpnConfig
	// has an IP but no server credentials. Fall back to reading them fresh from
	// the sibling control-plane CR so peer removal isn't silently skipped.
	if work.Spec.NodeInfo.NodeType == "worker" && !work.Spec.DisableVPN && work.Spec.VPNConfig.VPNSSHCredentialsRef.Name == "" {
		if clusterParent, err := r.findControlPlane(ctx, work); err != nil {
			log.Error(err, "looking up control-plane for VPN server config (continuing)")
		} else if clusterParent != nil && clusterParent.Spec.VPNConfig.VPNSSHCredentialsRef.Name != "" {
			work.Spec.VPNConfig.VPNServerPublicIP = clusterParent.Spec.VPNConfig.VPNServerPublicIP
			work.Spec.VPNConfig.VPNServerSSHPort = clusterParent.Spec.VPNConfig.VPNServerSSHPort
			work.Spec.VPNConfig.VPNServerSSHUsername = clusterParent.Spec.VPNConfig.VPNServerSSHUsername
			work.Spec.VPNConfig.VPNSSHCredentialsRef = clusterParent.Spec.VPNConfig.VPNSSHCredentialsRef
		}
	}

	if work.Spec.DisableVPN {
		log.Info("Skipping VPN peer removal — VPN disabled for this node")
	} else if work.Spec.VPNConfig.IP == "" || work.Spec.VPNConfig.VPNSSHCredentialsRef.Name == "" {
		log.Info("Skipping VPN peer removal — no VPN IP or VPN server credentials configured",
			"vpnIP", work.Spec.VPNConfig.IP, "vpnSSHCredentialsRef", work.Spec.VPNConfig.VPNSSHCredentialsRef.Name)
	} else if err := r.removeVPNPeer(ctx, work); err != nil {
		log.Error(err, "VPN peer removal incomplete (continuing)")
	}

	// Step 4: Delete the cluster's management-cluster resources (Porch repo,
	// Nephio tokens, PackageVariants). They are labelled with the cluster name,
	// which every node of the cluster shares, so only the control-plane may
	// delete them — removing one worker must not wipe the cluster's management
	// resources. Errors are logged but do not block finalizer removal.
	if cluster.Spec.NodeInfo.NodeType == "control-plane" {
		if err := r.deleteClusterResources(ctx, cluster); err != nil {
			log.Error(err, "deleting management-cluster resources (continuing)")
		}
	} else {
		log.Info("Not the control-plane — leaving the cluster's management resources in place")
	}

	// Step 5: Release the auth and VPN SSH secrets (remove our finalizers) so
	// that any pending deletions of user-managed secrets can now proceed —
	// unless another RemoteCluster still uses them.
	// This runs after all SSH work is done so both secrets are available throughout.
	r.removeAuthSecretFinalizer(ctx, work)
	r.removeVPNSecretFinalizer(ctx, work)

	// Step 6: Remove the RemoteCluster finalizer — lets the API server GC the CR.
	// The object read at the start of this reconcile is minutes old by now: patch a
	// fresh copy (optimistic lock, retried) so a concurrent write cannot turn into
	// a conflict that re-runs the whole cleanup.
	if err := r.removeClusterFinalizer(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	log.Info("RemoteCluster cleanup complete", "name", cluster.Name)
	return ctrl.Result{}, nil
}

// removeClusterFinalizer removes remoteClusterFinalizer with a merge patch on a
// freshly read object. A vanished object is success.
func (r *RemoteClusterReconciler) removeClusterFinalizer(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &infrav1.RemoteCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !controllerutil.ContainsFinalizer(fresh, remoteClusterFinalizer) {
			return nil
		}
		patch := client.MergeFromWithOptions(fresh.DeepCopy(), client.MergeFromWithOptimisticLock{})
		controllerutil.RemoveFinalizer(fresh, remoteClusterFinalizer)
		return client.IgnoreNotFound(r.Patch(ctx, fresh, patch))
	})
}

// resolveVPNDisabledForDelete decides whether the VPN was in use on the node
// being deleted. The recorded provisioned mode wins. Without it the node was
// never provisioned by this controller (or predates the record): the spec
// applies, except that a worker never reconciled follows its control-plane —
// on a VPN-less cluster the host was never given WireGuard by this controller
// and must not have it purged.
func (r *RemoteClusterReconciler) resolveVPNDisabledForDelete(ctx context.Context, cluster *infrav1.RemoteCluster) bool {
	if cluster.Annotations[annotationProvisionedVPNMode] != "" {
		return effectiveVPNDisabled(cluster)
	}
	disabled := cluster.Spec.DisableVPN
	if !disabled && cluster.Spec.NodeInfo.NodeType == "worker" {
		cp, err := r.findControlPlane(ctx, cluster)
		if err != nil {
			logf.FromContext(ctx).Error(err, "looking up control-plane VPN mode for delete (using spec)")
		} else if cp != nil && effectiveVPNDisabled(cp) {
			disabled = true
		}
	}
	return disabled
}

// drainWorkerFromCP gracefully evicts workloads from the worker node by running
// kubectl drain + kubectl delete node on the control-plane.  Best-effort.
//
// The node is identified by its OS hostname (over SSH) or, when the worker is
// unreachable, by its IP on the control-plane. It is never guessed from
// spec.clusterName: that name is shared by every node of the cluster, so the
// drain/delete would hit the wrong node.
func (r *RemoteClusterReconciler) drainWorkerFromCP(ctx context.Context, cluster *infrav1.RemoteCluster) {
	log := logf.FromContext(ctx)

	nodeName := ""
	if nodeClient, err := r.getSSHClient(ctx, cluster); err == nil {
		if out, err := sshhelper.RunStdoutCtx(ctx, nodeClient, "hostname"); err == nil {
			if h := strings.ToLower(strings.TrimSpace(out)); validNodeName(h) {
				nodeName = h
			}
		}
		nodeClient.Conn.Close() //nolint:errcheck
	}

	cp, err := r.findControlPlane(ctx, cluster)
	if err != nil || cp == nil {
		log.Info("Control-plane not found — skipping kubectl drain", "nodeName", nodeName)
		return
	}
	cpClient, err := r.getSSHClient(ctx, cp)
	if err != nil {
		log.Error(err, "Cannot SSH to control-plane for drain (continuing without drain)")
		return
	}
	defer cpClient.Conn.Close() //nolint:errcheck

	if nodeName == "" {
		nodeName = r.nodeNameByIP(ctx, cpClient, cluster)
	}
	if nodeName == "" {
		log.Info("Could not resolve the worker's node name (worker unreachable and its IP is not a known node) — skipping drain")
		return
	}

	log.Info("Draining worker from control-plane", "nodeName", nodeName)
	drainCmd := fmt.Sprintf(
		"kubectl drain %s --ignore-daemonsets --delete-emptydir-data --force --timeout=120s 2>/dev/null || true",
		shQuote(nodeName),
	)
	if out, err := sshhelper.Run(cpClient, drainCmd); err != nil {
		log.Error(err, "kubectl drain encountered errors", "output", out)
	}
	deleteCmd := fmt.Sprintf("kubectl delete node %s --ignore-not-found 2>/dev/null || true", shQuote(nodeName))
	if out, err := sshhelper.Run(cpClient, deleteCmd); err != nil {
		log.Error(err, "kubectl delete node encountered errors", "output", out)
	}
}

const wireGuardTeardownScript = `
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
`

// controlPlaneInitKillScript stops a kubeadm init that is still running on the
// host (closing the SSH connection of a cancelled provisioning does not stop the
// remote process). The "[k]" keeps pkill from matching its own command line (or
// its sudo parent's).
const controlPlaneInitKillScript = `
# A cancelled control-plane provisioning may have left kubeadm init running.
sudo pkill -f '[k]ubeadm init' 2>/dev/null || true
sleep 2
sudo pkill -9 -f '[k]ubeadm init' 2>/dev/null || true
`

const nodeResetScriptBase = `
# kubeadm reset cleans up apiserver/etcd/kubelet state, CNI config, and iptables rules.
if command -v kubeadm >/dev/null 2>&1; then
  sudo kubeadm reset --force 2>/dev/null || true
fi

# Stop and disable services before purging packages.
sudo systemctl stop kubelet crio 2>/dev/null || true
sudo systemctl disable kubelet crio 2>/dev/null || true

# Unmount all submounts under container storage paths (deepest first) so that
# subsequent rm -rf does not hit EBUSY on overlay or shm mounts.
awk '$2~/^\/var\/lib\/containers|^\/run\/containers/{print $2}' /proc/mounts \
  | sort -r | xargs -r sudo umount -l 2>/dev/null || true

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
sudo rm -rf /var/lib/crio /run/crio /run/containers 2>/dev/null || true
sudo rm -rf /var/lib/containers 2>/dev/null || true
sudo rm -rf /var/log/crio 2>/dev/null || true
sudo rm -rf /etc/crio /etc/containers 2>/dev/null || true

# Remove CRIU config directory (runc.conf and anything else under /etc/criu).
sudo rm -rf /etc/criu 2>/dev/null || true

sudo rm -f  /etc/modules-load.d/k8s.conf /etc/sysctl.d/k8s.conf 2>/dev/null || true
sudo rm -f  /etc/apt/sources.list.d/kubernetes.list /etc/apt/sources.list.d/cri-o.list 2>/dev/null || true
sudo rm -f  /etc/apt/keyrings/kubernetes-apt-keyring.gpg /etc/apt/keyrings/cri-o-apt-keyring.gpg 2>/dev/null || true

# Remove the kubeconfig-refresh systemd timer deployed on control-plane nodes.
sudo systemctl stop    kubeconfig-refresh.timer   2>/dev/null || true
sudo systemctl disable kubeconfig-refresh.timer   2>/dev/null || true
sudo rm -f /usr/local/bin/kubeconfig-refresh.sh                2>/dev/null || true
sudo rm -f /etc/systemd/system/kubeconfig-refresh.service      2>/dev/null || true
sudo rm -f /etc/systemd/system/kubeconfig-refresh.timer        2>/dev/null || true
sudo systemctl daemon-reload                                    2>/dev/null || true

# Remove custom binaries installed during provisioning.
sudo rm -f /usr/local/bin/crictl /usr/bin/crictl 2>/dev/null || true
sudo rm -f /usr/local/bin/crun   /usr/bin/crun   2>/dev/null || true
sudo rm -f /usr/sbin/runc /usr/local/sbin/runc   2>/dev/null || true
sudo rm -f /usr/sbin/criu                         2>/dev/null || true
sudo rm -f /usr/bin/crio /usr/local/bin/crio       2>/dev/null || true
sudo rm -f /usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || true

# Remove GPU CDI device spec directory.
sudo rm -rf /etc/cdi 2>/dev/null || true

# Remove the bootstrap-complete marker so re-provisioning is not skipped.
sudo rm -f /var/lib/node-bootstrap-complete 2>/dev/null || true

sudo apt-get autoremove -y 2>/dev/null || true

echo "node reset complete"
`

// buildNodeResetScript returns the node teardown script. WireGuard teardown is
// included only when a VPN was in use: nodes provisioned with spec.disableVPN
// never had WireGuard set up by this controller, so any wireguard on the host
// is left alone.
//
// For a control-plane the script first kills any kubeadm init still running
// (kubeadm reset itself still runs afterwards).
func buildNodeResetScript(disableVPN, controlPlane bool) string {
	const tail = "echo \"node reset complete\"\n"
	script := nodeResetScriptBase
	if controlPlane {
		script = controlPlaneInitKillScript + script
	}
	if disableVPN {
		return script
	}
	return strings.Replace(script, tail, wireGuardTeardownScript+"\n"+tail, 1)
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
	defer sshClient.Conn.Close() //nolint:errcheck

	log.Info("Resetting node via SSH", "host", cluster.Spec.Host)

	script := buildNodeResetScript(effectiveVPNDisabled(cluster), cluster.Spec.NodeInfo.NodeType == "control-plane")

	out, err := sshhelper.Run(sshClient, script)
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
		Namespace: vpnCredRefNamespace(cluster, credRef),
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
	vpnPort := parsePort(cluster.Spec.VPNConfig.VPNServerSSHPort, 22)
	vpnUser := cluster.Spec.VPNConfig.VPNServerSSHUsername
	if vpnUser == "" {
		vpnUser = "ubuntu"
	}

	var vpnClient *sshhelper.Client
	var err error
	if strings.HasPrefix(cred, "-----BEGIN") {
		vpnClient, err = sshhelper.ConnectWithPrivateKey(vpnHost, vpnPort, vpnUser, cred)
	} else {
		vpnClient, err = sshhelper.Connect(vpnHost, vpnPort, vpnUser, cred)
	}
	if err != nil {
		return fmt.Errorf("SSH to VPN server %s:%d as %s: %w", vpnHost, vpnPort, vpnUser, err)
	}
	defer vpnClient.Conn.Close() //nolint:errcheck

	// Discover the peer's public key from the live WireGuard state.
	dumpOut, err := sshhelper.RunStdoutCtx(ctx, vpnClient, "sudo wg show wg0 dump 2>/dev/null || true")
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
	if out, err := sshhelper.Run(vpnClient, fmt.Sprintf("sudo wg set wg0 peer %s remove", shQuote(peerKey))); err != nil {
		log.Error(err, "removing peer from running WireGuard config", "output", out)
	}

	// Remove the matching [Peer] block from /etc/wireguard/wg0.conf so the
	// peer is not re-added on VPN server restart.
	if err := onprem.RemoveVPNPeerFromServerConf(vpnClient, peerKey); err != nil {
		log.Error(err, "removing peer block from wg0.conf")
	}

	log.Info("Removed WireGuard peer from VPN server", "vpnIP", vpnIP)
	return nil
}

// deleteClusterResources deletes every management-cluster object labelled with
// the cluster's name. Only the control-plane's deletion may call it (workers
// share the label value but not the ownership).
func (r *RemoteClusterReconciler) deleteClusterResources(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	matchLabels := client.MatchingLabels{remoteClusterLabelKey: cluster.Spec.ClusterName}

	for _, gvk := range []schema.GroupVersionKind{
		{Group: "config.porch.kpt.dev", Version: "v1alpha1", Kind: "RepositoryList"},
		{Group: "infra.nephio.org", Version: "v1alpha1", Kind: "RepositoryList"},
		{Group: "infra.nephio.org", Version: "v1alpha1", Kind: "TokenList"},
	} {
		if err := r.deleteUnstructuredList(ctx, gvk, matchLabels, client.InNamespace(cluster.Namespace)); err != nil {
			return err
		}
	}

	// The Nephio Repository was historically created without the label; remove
	// that legacy object by name too.
	legacy := &unstructured.Unstructured{}
	legacy.SetGroupVersionKind(schema.GroupVersionKind{Group: "infra.nephio.org", Version: "v1alpha1", Kind: "Repository"})
	legacy.SetName(cluster.Spec.ClusterName)
	legacy.SetNamespace(cluster.Namespace)
	if err := r.Delete(ctx, legacy); client.IgnoreNotFound(err) != nil {
		return err
	}

	// PackageVariants live in packageVariantNamespace; also sweep the cluster's
	// own namespace, where earlier versions of the delete path looked.
	pvNamespaces := []string{packageVariantNamespace}
	if cluster.Namespace != packageVariantNamespace {
		pvNamespaces = append(pvNamespaces, cluster.Namespace)
	}
	for _, ns := range pvNamespaces {
		if err := r.deleteUnstructuredList(ctx,
			schema.GroupVersionKind{Group: "config.porch.kpt.dev", Version: "v1alpha1", Kind: "PackageVariantList"},
			matchLabels, client.InNamespace(ns)); err != nil {
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
	// setters is injected as spec.pipeline.mutators[0].configMap so that
	// apply-setters processes every `# kpt-set: ${KEY}` comment in the package.
	// Do NOT use spec.packageContext.data — that only writes into kpt's internal
	// package-context.yaml and never reaches apply-setters.
	setters map[string]string
}

func (r *RemoteClusterReconciler) createCorePackageVariants(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx)
	log.Info("Creating Platform Core PackageVariants", "remotecluster", cluster.Name)

	variants := []packageVariantSpec{
		{
			name: "remote-cluster-provisioner-variant",
			upstream: packageRef{
				pkg:      "ml-platform/remote-cluster-provisioner",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "remote-cluster-provisioner",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		{
			name: "harbor-variant",
			upstream: packageRef{
				pkg:      "ml-platform/harbor",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "harbor",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		{
			name: "prometheus-stack-variant",
			upstream: packageRef{
				pkg:      "ml-platform/prometheus-stack",
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
				pkg:      "ml-platform/keycloak",
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
				pkg:      "ml-platform/hami",
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
			name: "fluidcr-variant",
			upstream: packageRef{
				pkg:      "ml-platform/fluidcr",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "fluidcr",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		{
			name: "enterprise-gateway-variant",
			upstream: packageRef{
				pkg:      "ml-platform/enterprise-gateway",
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
				pkg:      "ml-platform/jupyter-hub",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "jupyter-hub",
				repo: cluster.Spec.ClusterName,
			},
			// annotations: map[string]interface{}{
			// 	"approval.nephio.org/policy": "initial",
			// },

		},

		{
			name: "gpu-operator-variant",
			upstream: packageRef{
				pkg:      "ml-platform/gpu-operator",
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
		{
			name: "services-overlays-variant",
			upstream: packageRef{
				pkg:      "ml-platform/services-overlays",
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

		{
			name: "gpu-monitoring-variant",
			upstream: packageRef{
				pkg:      "ml-platform/gpu-monitoring",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "gpu-monitoring",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
		},

		{
			name: "ml-system-variant",
			upstream: packageRef{
				pkg:      "ml-platform/ml-system",
				repo:     cluster.Spec.GitConfig.UpstreamPlatformRepo,
				revision: cluster.Spec.GitConfig.PackageRevision,
			},
			downstream: packageRef{
				pkg:  "ml-system",
				repo: cluster.Spec.ClusterName,
			},
			annotations: map[string]interface{}{
				"approval.nephio.org/policy": "initial",
			},
			setters: func() map[string]string {
				m := map[string]string{
					// Network locations derived from the cluster host.
					"JUPYTERHUB_PUBLIC_URL":                 "http://" + cluster.Spec.Host + ":30080",
					"NEXT_PUBLIC_KC_URL":                    "http://" + cluster.Spec.Host + ":30090",
					"NEXT_PUBLIC_QUOTA_API":                 "http://" + cluster.Spec.Host + ":30082",
					"NEXT_PUBLIC_ADMIN_EXTERNAL_MONITORING": "http://" + cluster.Spec.Host + ":30802/",

					"EG_KERNELSPECS_NFS_SERVER": cluster.Spec.Host,

					// In-cluster service defaults — override via PlatformVariables
					// when monitoring or an external Postgres is used.
					"GRAFANA_URL":      "http://prometheus-grafana.monitoring.svc.cluster.local:80",
					"ALERTMANAGER_URL": "http://alertmanager-operated.monitoring.svc.cluster.local:9093",
					"PROMETHEUS_URL":   "http://prometheus-operated.monitoring.svc.cluster.local:9090",
					"HARBOR_BASE_URL":  "http://harbor.harbor.svc.cluster.local:80",
					"FLUIDCR_API_URL":  "http://fluidcr-api.fluidcr-system.svc.cluster.local:8080",

					// External Postgres — empty means use the bundled StatefulSet.
					"DB_HOST": "",
					"DB_PORT": "",

					// Storage class for model artifacts / vLLM serving.
					// Empty means the package's committed default applies.
					"MODEL_ARTIFACT_STORAGE_CLASS": "",

					// Credentials — no safe default; must be supplied via PlatformVariables.
					"KEYCLOAK_ADMIN_PASSWORD": "",
					"JUPYTERHUB_API_TOKEN":    "",
					"QUOTA_API_TOKEN":         "",
					"DB_PASSWORD":             "",
					"CREDENTIAL_KEY":          "",
					"GHCR_DOCKERCONFIGJSON":   "",

					// Harbor credentials — supply EITHER the admin pair (auto-creates a
					// scoped robot account once at startup) OR a pre-created robot account.
					// Leave all four empty to skip Harbor integration at startup.
					"HARBOR_ADMIN_USERNAME": "",
					"HARBOR_ADMIN_PASSWORD": "",
					"HARBOR_USERNAME":       "",
					"HARBOR_SECRET":         "",

					// Image tags — must match what was built and pushed.
					"QUOTA_API_IMAGE": "",
					"FRONTEND_IMAGE":  "",

					// Idle GPU reclamation — empty means use the package's committed
					// defaults (RECLAMATION_POLICY_SOURCE=db, i.e. admin-UI managed).
					// Set RECLAMATION_POLICY_SOURCE to "env" via PlatformVariables to
					// activate env-driven policy; the rest only take effect then.
					"RECLAMATION_POLICY_SOURCE":    "",
					"RECLAMATION_ENABLED":          "",
					"RECLAMATION_GPU_UTIL_PCT":     "",
					"RECLAMATION_WINDOW_MINUTES":   "",
					"RECLAMATION_GRACE_MINUTES":    "",
					"RECLAMATION_WARNING_MINUTES":  "",
					"RECLAMATION_REQUIRE_CPU_IDLE": "",
					"RECLAMATION_REQUIRE_GPU_IDLE": "",
				}
				// PlatformVariables override any default above, including credentials
				// and any network location that differs from the computed values.
				for _, pv := range cluster.Spec.NodeInfo.SoftwareConfig.PlatformVariables {
					m[pv.Key] = pv.Value
				}
				return m
			}(),
		},

		// Commented-out variants (re-enable as needed):
		// minio-variant, enterprise-gateway-variant, gpu-operator-variant,
		// harbor-variant, kai-scheduler-variant, keycloak-variant,
		// kubeflow-variant, kueue-variant, kyverno-variant, prometheus-stack-variant,
		// ml-platform-admin
	}

	return r.upsertPackageVariants(ctx, cluster, variantsOverlays)
}

// upsertPackageVariants creates or updates each PackageVariant in
// packageVariantNamespace.
//
// The base names (e.g. harbor-variant) are fixed and packageVariantNamespace is
// shared by every cluster, so a second cluster would collide with the first. A
// variant keeps its base name while that name is free or already owned by this
// cluster (so existing objects are never renamed); when the base name belongs to
// ANOTHER cluster the variant gets the per-cluster name
// perClusterPackageVariantName. Another cluster's variant is never overwritten:
// that would silently repoint it while its label still names the first cluster.
//
// Nothing else in the spec collides between clusters: the downstream repository
// is the cluster's own Porch repository (spec.clusterName) and the upstream
// package is a shared, read-only source package.
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
		// Inject setters as spec.pipeline.mutators[0].configMap so that
		// apply-setters resolves every `# kpt-set: ${KEY}` comment in the package.
		// spec.packageContext.data only writes into kpt's internal package-context.yaml
		// and never reaches apply-setters — do not use it for setter values.
		if len(v.setters) > 0 {
			configMap := make(map[string]interface{}, len(v.setters))
			for k, val := range v.setters {
				if val != "" {
					configMap[k] = val
				}
			}
			spec["pipeline"] = map[string]interface{}{
				"mutators": []interface{}{
					map[string]interface{}{
						"image":     "ghcr.io/kptdev/krm-functions-catalog/apply-setters:v0.2.4",
						"configMap": configMap,
					},
				},
			}
		}

		name, err := r.resolvePackageVariantName(ctx, cluster, v.name)
		if err != nil {
			return err
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(packageVariantGVK)
		obj.SetName(name)
		obj.SetNamespace(packageVariantNamespace)
		obj.SetLabels(labels)
		obj.Object["spec"] = spec

		if err := r.Create(ctx, obj); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating PackageVariant %q: %w", name, err)
			}

			existing := &unstructured.Unstructured{}
			existing.SetGroupVersionKind(packageVariantGVK)
			if err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing); err != nil {
				return fmt.Errorf("fetching existing PackageVariant %q: %w", name, err)
			}
			// Guards the window between resolvePackageVariantName and Create.
			if owner, ok := existing.GetLabels()[remoteClusterLabelKey]; ok && owner != cluster.Spec.ClusterName {
				return packageVariantOwnedError(name, owner, cluster.Spec.ClusterName)
			}
			existingLabels := existing.GetLabels()
			if existingLabels == nil {
				existingLabels = map[string]string{}
			}
			existingLabels[remoteClusterLabelKey] = cluster.Spec.ClusterName
			existing.SetLabels(existingLabels)
			existing.Object["spec"] = spec
			if err := r.Update(ctx, existing); err != nil {
				return fmt.Errorf("updating PackageVariant %q: %w", name, err)
			}
		}
	}
	return nil
}

func packageVariantOwnedError(name, owner, cluster string) error {
	return fmt.Errorf("PackageVariant %s/%s already belongs to cluster %q; refusing to overwrite it for cluster %q",
		packageVariantNamespace, name, owner, cluster)
}

// packageVariantOwner looks up a PackageVariant by name. owner is its cluster
// label ("" when unlabelled, i.e. created before labelling existed).
func (r *RemoteClusterReconciler) packageVariantOwner(ctx context.Context, name string) (owner string, exists bool, err error) {
	pv := &unstructured.Unstructured{}
	pv.SetGroupVersionKind(packageVariantGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: packageVariantNamespace, Name: name}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("looking up PackageVariant %q: %w", name, err)
	}
	return pv.GetLabels()[remoteClusterLabelKey], true, nil
}

// resolvePackageVariantName picks the name under which this cluster owns the
// variant with the given base name: the base name while it is free, unlabelled
// (legacy) or owned by this cluster; otherwise the per-cluster name. An existing
// per-cluster object of this cluster is sticky, so the variant never flips
// between the two names when the other cluster later goes away.
func (r *RemoteClusterReconciler) resolvePackageVariantName(ctx context.Context, cluster *infrav1.RemoteCluster, base string) (string, error) {
	me := cluster.Spec.ClusterName
	baseOwner, baseExists, err := r.packageVariantOwner(ctx, base)
	if err != nil {
		return "", err
	}
	if baseExists && (baseOwner == "" || baseOwner == me) {
		return base, nil
	}
	scoped := perClusterPackageVariantName(base, me)
	scopedOwner, scopedExists, err := r.packageVariantOwner(ctx, scoped)
	if err != nil {
		return "", err
	}
	if scopedExists {
		if scopedOwner != "" && scopedOwner != me {
			return "", packageVariantOwnedError(scoped, scopedOwner, me)
		}
		return scoped, nil
	}
	if !baseExists {
		return base, nil
	}
	logf.FromContext(ctx).Info("PackageVariant base name belongs to another cluster — using a per-cluster name",
		"base", base, "owner", baseOwner, "name", scoped, "clusterName", me)
	return scoped, nil
}

// resolveCnlabRuntimeConfig resolves pkgruntime.Config from the cluster's SoftwareConfig.
// If CnlabRuntime.CredentialsRef is set, the referenced Secret is read to extract
// "username" and "token" keys. Token is kept in memory only and never logged.
func (r *RemoteClusterReconciler) resolveCnlabRuntimeConfig(
	ctx context.Context,
	softwareCfg infrav1.SoftwareConfig,
	namespace string,
) (pkgruntime.Config, error) {
	cfg := pkgruntime.Config{}
	if softwareCfg.CnlabRuntime != nil {
		cr := softwareCfg.CnlabRuntime
		cfg.Registry = cr.Registry
		cfg.Repository = cr.Repository
		cfg.Version = cr.Version
		cfg.OrasVersion = cr.OrasVersion

		ref := cr.CredentialsRef
		if ref.Name != "" {
			ns := ref.NameSpace
			if ns == "" {
				ns = namespace
			}
			var secret corev1.Secret
			if err := r.Get(ctx, types.NamespacedName{
				Name:      ref.Name,
				Namespace: ns,
			}, &secret); err != nil {
				return pkgruntime.Config{}, fmt.Errorf("reading cnlab-runtime credentials secret %s/%s: %w", ns, ref.Name, err)
			}
			cfg.Username = string(secret.Data["username"])
			cfg.Token = string(secret.Data["token"]) // never log
		}
	}
	cfg.ApplyDefaults()
	return cfg, nil
}

// syncCnlabCredentialsToRemote pushes the cnlab-runtime registry secret and
// the cnlabRuntime block of NodeProvisionNetConfig to the remote cluster via SSH
// whenever the credentials change.  It is a no-op when credentials are absent or
// when the annotation hash already matches the current secret.
//
// Calling this from the phaseReady control-plane path means clusters provisioned
// before credentialsRef was added will be backfilled on their next reconcile.
func (r *RemoteClusterReconciler) syncCnlabCredentialsToRemote(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx)

	runtimeCfg, err := r.resolveCnlabRuntimeConfig(ctx, cluster.Spec.NodeInfo.SoftwareConfig, cluster.Namespace)
	if err != nil || runtimeCfg.Token == "" {
		// No credentials configured — nothing to push.
		return nil
	}

	h := sha256.Sum256([]byte(runtimeCfg.Username + ":" + runtimeCfg.Token))
	newHash := fmt.Sprintf("%x", h)
	if cluster.Annotations[annotationCnlabCredentialsHash] == newHash {
		log.Info("cnlab-runtime credentials already in sync on remote cluster",
			"registry", runtimeCfg.Registry, "hash", newHash[:12]+"…")
		return nil
	}
	log.Info("cnlab-runtime credentials changed — syncing to remote cluster",
		"registry", runtimeCfg.Registry)

	// Terminal: retry limit already reached — do not re-attempt until manually reset.
	if cluster.Status.CnlabSyncRetryCount >= maxCnlabSyncRetries {
		log.Info("cnlab-runtime credential sync in terminal error state — retry limit reached",
			"attempts", cluster.Status.CnlabSyncRetryCount,
			"maxRetries", maxCnlabSyncRetries,
			"hint", "patch .status.cnlabSyncRetryCount to 0 to re-enable sync")
		return nil
	}

	sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
	defer cancel()
	sshClient, err := r.getSSHClient(sshCtx, cluster)
	if err != nil {
		return r.recordCnlabSyncFailure(ctx, cluster, fmt.Errorf("SSH connection for credential sync: %w", err))
	}
	defer func() { _ = sshClient.Conn.Close() }()

	cr := cluster.Spec.NodeInfo.SoftwareConfig.CnlabRuntime
	ns := cluster.Namespace
	if cr != nil && cr.CredentialsRef.NameSpace != "" {
		ns = cr.CredentialsRef.NameSpace
	}

	// Push the secret.
	secretName := "cnlab-runtime-registry"
	if cr != nil && cr.CredentialsRef.Name != "" {
		secretName = cr.CredentialsRef.Name
	}
	secretCmd := secretApplyCmd(secretName, ns, corev1.SecretTypeOpaque, map[string][]byte{
		"username": []byte(runtimeCfg.Username),
		"token":    []byte(runtimeCfg.Token),
	})
	if out, sshErr := sshhelper.Run(sshClient, secretCmd); sshErr != nil {
		return r.recordCnlabSyncFailure(ctx, cluster,
			fmt.Errorf("applying cnlab-runtime secret on remote cluster: %w\nOutput: %s", sshErr, out))
	}
	log.Info("Synced cnlab-runtime credentials secret to remote cluster", "secret", secretName)

	// Patch the NodeProvisionNetConfig cnlabRuntime block.
	cnlabRuntimeYAML := fmt.Sprintf(`    cnlabRuntime:
      registry: %s
      repository: %s
      version: %s
      orasVersion: %s
      credentialsRef:
        name: %s
        namespace: %s
`, yamlQuote(runtimeCfg.Registry), yamlQuote(runtimeCfg.Repository), yamlQuote(runtimeCfg.Version), yamlQuote(runtimeCfg.OrasVersion),
		yamlQuote(secretName), yamlQuote(ns))

	netConfigName := cluster.Spec.ClusterName + "-netconfig"
	patchCmd := fmt.Sprintf(`
cat <<'NCEOF' | kubectl apply -f -
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvisionNetConfig
metadata:
  name: %s
  namespace: %s
spec:
  softwareConfig:
%sNCEOF`, yamlQuote(netConfigName), yamlQuote(cluster.Namespace), cnlabRuntimeYAML)

	if out, sshErr := sshhelper.Run(sshClient, patchCmd); sshErr != nil {
		return r.recordCnlabSyncFailure(ctx, cluster,
			fmt.Errorf("patching NodeProvisionNetConfig cnlabRuntime on remote cluster: %w\nOutput: %s", sshErr, out))
	}
	log.Info("Synced cnlabRuntime config to remote NodeProvisionNetConfig", "netconfig", netConfigName)

	// Reset failure counter and clear the error condition on success. The caller's
	// object is likely stale after the SSH work, so this goes through mutateStatus
	// (fresh read + retry on conflict). If it cannot be persisted the credentials
	// hash is NOT stamped: the next reconcile then repeats the (idempotent) sync
	// and gets another chance to reset the counter, instead of leaving it
	// permanently short of the retry limit's reset.
	if cluster.Status.CnlabSyncRetryCount > 0 {
		log.Info("cnlab-runtime credential sync recovered after previous failures",
			"previousAttempts", cluster.Status.CnlabSyncRetryCount)
		if err := r.mutateStatus(ctx, cluster, func(fresh *infrav1.RemoteCluster) bool {
			if fresh.Status.CnlabSyncRetryCount == 0 {
				return false
			}
			fresh.Status.CnlabSyncRetryCount = 0
			// Replace any existing CnlabCredentialSyncFailed condition with a success entry.
			upsertCondition(fresh, metav1.Condition{
				Type:    cnlabSyncConditionType,
				Status:  metav1.ConditionTrue,
				Reason:  "SyncSucceeded",
				Message: "cnlab-runtime credentials successfully synced to remote cluster",
			})
			return true
		}); err != nil {
			return fmt.Errorf("recording cnlab sync recovery: %w", err)
		}
		cluster.Status.CnlabSyncRetryCount = 0
	}

	if patchErr := r.patchAnnotation(ctx, cluster, annotationCnlabCredentialsHash, newHash); patchErr != nil {
		log.Error(patchErr, "failed to stamp cnlab credentials hash annotation (non-fatal)")
	}
	return nil
}

// recordCnlabSyncFailure re-fetches the RemoteCluster, increments
// CnlabSyncRetryCount, and persists the status via RetryOnConflict so the
// counter is never silently lost on a ResourceVersion conflict.
func (r *RemoteClusterReconciler) recordCnlabSyncFailure(ctx context.Context, cluster *infrav1.RemoteCluster, cause error) error {
	log := logf.FromContext(ctx)

	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &infrav1.RemoteCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
			return err
		}
		fresh.Status.CnlabSyncRetryCount++
		if fresh.Status.CnlabSyncRetryCount >= maxCnlabSyncRetries {
			msg := fmt.Sprintf("cnlab-runtime credential sync failed after %d attempts (last error: %v) — VPN to remote cluster may be down; patch .status.cnlabSyncRetryCount to 0 to re-enable",
				fresh.Status.CnlabSyncRetryCount, cause)
			upsertCondition(fresh, metav1.Condition{
				Type:    cnlabSyncConditionType,
				Status:  metav1.ConditionFalse,
				Reason:  "SyncRetryLimitReached",
				Message: msg,
			})
			fresh.Status.Message = truncateMessage(msg)
			log.Error(cause, "cnlab-runtime credential sync reached retry limit — no further retries",
				"attempts", fresh.Status.CnlabSyncRetryCount,
				"maxRetries", maxCnlabSyncRetries)
		} else {
			log.Error(cause, "cnlab-runtime credential sync failed — will retry",
				"attempt", fresh.Status.CnlabSyncRetryCount,
				"maxRetries", maxCnlabSyncRetries)
		}
		return r.Status().Update(ctx, fresh)
	})
	if updateErr != nil {
		log.Error(updateErr, "failed to persist cnlab sync failure status (non-fatal)")
	}
	return cause
}

// SetupWithManager sets up the controller with the Manager.
func (r *RemoteClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.Add(r); err != nil {
		return fmt.Errorf("registering RemoteClusterReconciler as Runnable: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1.RemoteCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("remotecluster").
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(r)
}

// Start implements manager.Runnable. On controller startup it resyncs, for
// every Ready control-plane RemoteCluster:
//  1. The local (management-cluster) NodeProvisionNetConfig — credentials refs
//     and VPN config, via a plain in-cluster API write. Always attempted,
//     regardless of whether the remote cluster is reachable.
//  2. The remote cluster's own credential + VPN config secrets, over SSH.
//     A provisioned cluster is meant to be decoupled from this controller —
//     it keeps running on its own once joined — so an unreachable remote
//     cluster here is only logged and skipped, never retried with a counter
//     or surfaced as a status/condition change.
func (r *RemoteClusterReconciler) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("startup-sync")
	log.Info("Running startup sync for all Ready RemoteClusters")

	clusterList := &infrav1.RemoteClusterList{}
	if err := r.List(ctx, clusterList); err != nil {
		log.Error(err, "startup sync: failed to list RemoteClusters")
		return nil
	}

	for i := range clusterList.Items {
		cluster := &clusterList.Items[i]
		if cluster.Status.Phase != phaseReady || cluster.Spec.NodeInfo.NodeType != "control-plane" {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Local NodeProvisionNetConfig resync — a plain in-cluster API write,
		// no SSH involved. Do this unconditionally, before attempting SSH: a
		// provisioned cluster is meant to keep working via its VPN mesh even
		// when it (or the network path to it) is unreachable from this
		// management cluster, so ml-nodeprovision's local copy of its VPN/
		// credentials config must never depend on remote reachability to stay
		// current. This is also what heals a stale/empty vpnSshCredentialsRef
		// on the local NodeProvisionNetConfig across a controller restart,
		// without needing the remote cluster to be reachable at all.
		if err := r.ensureLocalNodeProvisionNetConfig(ctx, cluster, cluster); err != nil {
			log.Error(err, "startup sync: failed to resync local NodeProvisionNetConfig (non-fatal)",
				"cluster", cluster.Name)
		} else {
			log.Info("startup sync: local NodeProvisionNetConfig resynced", "cluster", cluster.Name)
		}

		// Remote (SSH-based) credential + VPN config push. The remote cluster
		// is intentionally decoupled from this controller once provisioned —
		// it must keep running even if this management cluster is down or the
		// remote cluster is temporarily unreachable — so a failure here is
		// logged and skipped, never retried with a counter or surfaced as a
		// RemoteCluster status/condition change.
		if err := r.startupSyncCluster(ctx, cluster); err != nil {
			log.Info("startup sync: cluster not reachable via SSH — skipping remote resync",
				"cluster", cluster.Name, "reason", err.Error())
		} else {
			log.Info("startup sync: remote resync completed", "cluster", cluster.Name)
		}
	}
	return nil
}

// startupSyncCluster pushes credentials and VPN config to a single remote
// cluster via SSH. Returns an error if the cluster is unreachable; the caller
// logs and skips — no status fields or retry counters are touched.
func (r *RemoteClusterReconciler) startupSyncCluster(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx).WithName("startup-sync").WithValues("cluster", cluster.Name)

	sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
	defer cancel()
	sshClient, err := r.getSSHClient(sshCtx, cluster)
	if err != nil {
		return fmt.Errorf("SSH: %w", err)
	}
	defer func() { _ = sshClient.Conn.Close() }()

	// --- credentials ---
	runtimeCfg, err := r.resolveCnlabRuntimeConfig(ctx, cluster.Spec.NodeInfo.SoftwareConfig, cluster.Namespace)
	if err == nil && runtimeCfg.Token != "" {
		cr := cluster.Spec.NodeInfo.SoftwareConfig.CnlabRuntime
		ns := cluster.Namespace
		if cr != nil && cr.CredentialsRef.NameSpace != "" {
			ns = cr.CredentialsRef.NameSpace
		}
		secretName := "cnlab-runtime-registry"
		if cr != nil && cr.CredentialsRef.Name != "" {
			secretName = cr.CredentialsRef.Name
		}
		secretCmd := secretApplyCmd(secretName, ns, corev1.SecretTypeOpaque, map[string][]byte{
			"username": []byte(runtimeCfg.Username),
			"token":    []byte(runtimeCfg.Token),
		})
		if out, sshErr := sshhelper.Run(sshClient, secretCmd); sshErr != nil {
			return fmt.Errorf("applying credentials secret: %w\nOutput: %s", sshErr, out)
		}
		log.Info("Pushed credentials secret", "secret", secretName)

		// Stamp hash so the normal reconcile path knows credentials are current.
		h := sha256.Sum256([]byte(runtimeCfg.Username + ":" + runtimeCfg.Token))
		if patchErr := r.patchAnnotation(ctx, cluster, annotationCnlabCredentialsHash, fmt.Sprintf("%x", h)); patchErr != nil {
			log.Error(patchErr, "stamping credentials hash annotation (non-fatal)")
		}
	}

	// --- NodeProvisionNetConfig (VPN config + non-credential software config) ---
	if err := pushNetConfigViaSSH(sshClient, cluster); err != nil {
		return err
	}
	log.Info("Pushed NodeProvisionNetConfig to remote cluster")
	return nil
}

// netConfigSyncHash hashes the exact JSON body pushNetConfigViaSSH would send
// for the given RemoteCluster, so drift is detected on every field
// desiredNodeProvisionNetConfigFields computes — not just a hand-picked
// subset. Marshaling the real struct (rather than hand-listing fields into a
// format string, the way this used to work) means a newly-added field can
// never again be silently left out of the hash while the patch below sends
// it, or vice versa: both derive from the same value.
func netConfigSyncHash(clusterParent *infrav1.RemoteCluster) (string, error) {
	b, err := json.Marshal(desiredNodeProvisionNetConfigFields(clusterParent))
	if err != nil {
		return "", fmt.Errorf("marshaling NodeProvisionNetConfig fields for hashing: %w", err)
	}
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h), nil
}

// netConfigPatchBody is the JSON merge-patch body pushNetConfigViaSSH sends:
// desiredNodeProvisionNetConfigFields as JSON. The struct omits vpnRange and
// vpnServerPublicConfig when the VPN is disabled, but a merge patch leaves an
// omitted key alone, so on a cluster that is switched from the VPN to no VPN the
// remote object would keep advertising the old range and VPN server. When the
// VPN is disabled they are therefore sent as explicit nulls (merge-patch
// deletions).
func netConfigPatchBody(cluster *infrav1.RemoteCluster) ([]byte, error) {
	spec := desiredNodeProvisionNetConfigFields(cluster)
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshaling NodeProvisionNetConfig patch body: %w", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("marshaling NodeProvisionNetConfig patch body: %w", err)
	}
	if spec.DisableVPN {
		fields["vpnRange"] = nil
		fields["vpnServerPublicConfig"] = nil
	}
	// Same for every optional softwareConfig field: omitempty drops an
	// empty/unset value from the body, and a merge patch would then leave the
	// previous value on the remote object — keeping registries insecure, or
	// images pre-pulled with a stale pull secret, after the user removed them.
	if sw, ok := fields["softwareConfig"].(map[string]any); ok {
		if len(spec.SoftwareConfig.InsecureRegistries) == 0 {
			sw["insecureRegistries"] = nil
		}
		if len(spec.SoftwareConfig.ImagePrepulls) == 0 {
			sw["imagePrepulls"] = nil
		}
		if spec.SoftwareConfig.ImagePullSecretRef == nil {
			sw["imagePullSecretRef"] = nil
		}
	}
	body, err := json.Marshal(map[string]any{"spec": fields})
	if err != nil {
		return nil, fmt.Errorf("marshaling NodeProvisionNetConfig patch body: %w", err)
	}
	return body, nil
}

// pushNetConfigViaSSH JSON-merge-patches every field
// desiredNodeProvisionNetConfigFields computes onto the remote cluster's own
// NodeProvisionNetConfig, over an already-connected SSH client. A no-op
// (returns nil without doing anything) when the VPN is in use but no VPN server
// is configured yet.
//
// The patch body is produced by encoding/json against the same
// mlv1alpha1.NodeProvisionNetConfigSpec struct used for the local sync,
// instead of a hand-built "{"spec":{...%q...}}" format string — the latter
// is exactly how clusterName, softwareConfig.kubernetesVersion,
// imagePrepulls and imagePullSecretRef ended up silently absent from this
// patch for months (only vpnRange and vpnServerPublicConfig were ever
// listed), even though the initial SSH "create" push
// (handleCreateUpdateNodeProvisionConfig) set them once. Marshaling the
// struct makes that entire bug class structurally impossible here: whatever
// desiredNodeProvisionNetConfigFields returns is what gets sent, in full,
// every time.
func pushNetConfigViaSSH(sshClient *sshhelper.Client, cluster *infrav1.RemoteCluster) error {
	if !effectiveVPNDisabled(cluster) && cluster.Spec.VPNConfig.VPNServerPublicIP == "" {
		return nil
	}
	patchBody, err := netConfigPatchBody(cluster)
	if err != nil {
		return err
	}
	patchCmd := fmt.Sprintf(
		"kubectl patch nodeprovisionnetconfig %s -n %s --type=merge -p %s",
		shQuote(cluster.Spec.ClusterName+"-netconfig"), shQuote(cluster.Namespace), shQuote(string(patchBody)),
	)
	if out, sshErr := sshhelper.Run(sshClient, patchCmd); sshErr != nil {
		return fmt.Errorf("patching NodeProvisionNetConfig: %w\nOutput: %s", sshErr, out)
	}
	return nil
}

// syncNetConfigToRemote re-pushes every field desiredNodeProvisionNetConfigFields
// computes to the remote cluster's NodeProvisionNetConfig whenever it has
// drifted from the last successfully-pushed value (tracked via
// annotationNetConfigSyncHash). This is what heals a remote push missed at
// controller-restart time (the remote cluster was unreachable then), or left
// incomplete by an earlier partial/buggy sync, on the next successful
// periodic reconcile — instead of requiring another restart, a worker-join
// event, or (previously) never healing at all since the one-time SSH
// "create" push is gated by annotationNodeProvisionCreated and never runs
// again once that's set to "true".
//
// Deliberately has no retry counter or terminal/blocking condition, unlike
// syncCnlabCredentialsToRemote: a provisioned cluster is meant to be
// decoupled from this controller, so a persistently unreachable remote
// cluster here is just logged and retried again next reconcile — forever —
// never escalated into a state requiring manual intervention.
func (r *RemoteClusterReconciler) syncNetConfigToRemote(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	log := logf.FromContext(ctx)

	if !effectiveVPNDisabled(cluster) && cluster.Spec.VPNConfig.VPNServerPublicIP == "" {
		// VPN in use but no server configured yet — nothing to push.
		return nil
	}

	newHash, err := netConfigSyncHash(cluster)
	if err != nil {
		return err
	}
	if cluster.Annotations[annotationNetConfigSyncHash] == newHash {
		return nil
	}
	log.Info("NodeProvisionNetConfig fields changed — syncing to remote cluster")

	sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
	defer cancel()
	sshClient, err := r.getSSHClient(sshCtx, cluster)
	if err != nil {
		return fmt.Errorf("SSH connection for NodeProvisionNetConfig sync: %w", err)
	}
	defer func() { _ = sshClient.Conn.Close() }()

	if err := pushNetConfigViaSSH(sshClient, cluster); err != nil {
		return err
	}
	log.Info("Synced NodeProvisionNetConfig to remote cluster")

	if patchErr := r.patchAnnotation(ctx, cluster, annotationNetConfigSyncHash, newHash); patchErr != nil {
		log.Error(patchErr, "failed to stamp NodeProvisionNetConfig sync hash annotation (non-fatal)")
	}
	return nil
}

// parsePort converts a string port value to int, returning defaultPort when
// the string is empty or cannot be parsed. Used to accept port fields that
// the API accepts as strings (e.g. port: "22" in YAML).
func parsePort(s string, defaultPort int) int {
	if s == "" {
		return defaultPort
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return defaultPort
	}
	return n
}
