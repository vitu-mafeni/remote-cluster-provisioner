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

package ml

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
	remotenodeprovision "dcn.ssu.ac.kr/infra/provider/onprem"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// onPremJobResult carries the outcome of a background on-prem provisioning run.
type onPremJobResult struct {
	vpnIP     string
	publicKey string
	err       error
}

// NodeProvisionReconciler reconciles a NodeProvision object
type NodeProvisionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader is a direct (non-cached) client used for reads where an
	// informer-cache lag could cause a false failure — e.g. reading the
	// NodeProvisionNetConfig that node provisioning depends on immediately
	// after this controller (re)starts, before its watch cache is guaranteed
	// to reflect the very latest write. Falls back to the cached Client when
	// nil (e.g. in tests that don't set it).
	APIReader client.Reader
	// CredMgr handles background refresh of AWS STS / MFA-backed sessions.
	CredMgr *awsprovision.CredentialManager
	// onPremJobs holds in-flight on-prem provisioning goroutines.
	// Key: "<namespace>/<name>", Value: *onPremJob (result channel, cancel
	// func and the UID of the NodeProvision it was started for).
	onPremJobs sync.Map
	// onPremProgress tracks the current provisioning step for each in-flight goroutine.
	// Key: "<namespace>/<name>", Value: string
	onPremProgress sync.Map
	// mgrCtx is the manager's root context; used as the base for background
	// goroutines so that they are cancelled on graceful shutdown. Written by
	// Start() concurrently with reconciles, so it is only touched through
	// setManagerContext/baseContext (guarded by mgrMu).
	mgrMu  sync.RWMutex
	mgrCtx context.Context

	// nodeResetUIDs holds the UIDs of terminating NodeProvisions whose node
	// reset already ran (see nodeResetDone).
	nodeResetUIDs sync.Map

	// Test seams; nil means the real implementation (see nodeprovision_seams.go).
	awsOverrides  awsAPI
	gcpOverrides  gcpAPI
	dialVPNServer func(ctx context.Context, nc *mlv1alpha1.NodeProvisionNetConfig) (vpnServer, error)
	// nodeReset runs the node-side reset script and reports whether it ran.
	nodeReset func(ctx context.Context, np *mlv1alpha1.NodeProvision) bool
}

// Start implements manager.Runnable so the reconciler receives the manager's
// root context and can propagate it to background goroutines.
func (r *NodeProvisionReconciler) Start(ctx context.Context) error {
	r.setManagerContext(ctx)
	<-ctx.Done()
	return nil
}

const (
	// nodeProvisionFinalizer is placed on the NodeProvision CR itself.
	nodeProvisionFinalizer = "ml.dcn.ssu.ac.kr/nodeprovision-finalizer"

	// nodeProvisionNodeFinalizer is placed on the Kubernetes Node object so that
	// the node cannot be deleted independently of the NodeProvision lifecycle.
	nodeProvisionNodeFinalizer = "ml.dcn.ssu.ac.kr/nodeprovision-node-finalizer"

	// Ownership labels stamped onto the Kubernetes Node when it joins the cluster.
	// They let any observer (kubectl, dashboard, scripts) find the parent NodeProvision CR.
	nodeProvisionNameLabel     = "ml.dcn.ssu.ac.kr/node-provision"
	nodeProvisionNsLabel       = "ml.dcn.ssu.ac.kr/node-provision-namespace"
	nodeProvisionProviderLabel = "ml.dcn.ssu.ac.kr/provider"
	// nodeProvisionUIDLabel holds the UID of the owning NodeProvision CR.
	// Unlike the name, the UID is globally unique and survives CR rename/recreate,
	// so it is the definitive key for matching a Node back to its exact CR.
	nodeProvisionUIDLabel = "ml.dcn.ssu.ac.kr/node-provision-uid"

	// controllerCredsSuffix is appended to the NodeProvision name to form the
	// name of the controller-owned credential copy.  This copy carries an owner
	// reference so it cannot be deleted while the NodeProvision CR exists; it is
	// GC'd automatically after the CR is fully removed.  During teardown the
	// controller falls back to this copy when the user-managed secret is gone.
	controllerCredsSuffix = "-controller-creds"

	// registryCredsSuffix is appended to the NodeProvision name to form the
	// name of the controller-owned copy of the image-pull registry credentials.
	// Used by the image pre-pull path as a fallback when the user-managed
	// registry secret has been deleted.
	registryCredsSuffix = "-registry-creds"

	// requeueShort is used when waiting for external state (instance running, VPN).
	requeueShort = 30 * time.Second
	// requeueJoining is used while polling for the node to appear in k8s.
	requeueJoining = 15 * time.Second
	// requeueFailed is used to allow manual remediation before retrying.
	requeueFailed = time.Minute
	// npPrepullPollInterval is the requeue interval while images are pre-pulling.
	npPrepullPollInterval = 30 * time.Second
	// maxProvisionRetries is the number of consecutive provisioning failures
	// allowed before the controller stops retrying and leaves the resource in
	// a terminal Failed state requiring manual intervention.
	maxProvisionRetries = 5
	// creatingInstanceStallTimeout bounds how long a NodeProvision may sit in
	// CreatingInstance/ConfiguringVPN without an InstanceID being persisted.
	// Those phases are progress markers only — nothing re-enters provisioning
	// from them — so a failure between persisting the phase and persisting the
	// InstanceID (e.g. the VPN SSH connection failing) must not be allowed to
	// requeue forever. Past this deadline the NodeProvision is failed so it can
	// go through the normal retry/backoff path instead of looping silently.
	creatingInstanceStallTimeout = 10 * time.Minute
	// waitingForInstanceStallTimeout bounds how long a NodeProvision may sit in
	// WaitingForInstance. An EC2 instance normally reaches Running within a
	// couple of minutes; if it hasn't by this deadline (e.g. it silently stuck
	// in Pending, or a transient AWS API error is looping instead of reaching
	// failNodeProvision) fail it so it goes through the retry/backoff path.
	waitingForInstanceStallTimeout = 10 * time.Minute
	// maxPrepullRetries bounds how many times the image pre-pull Job may be
	// recreated after failing before the NodeProvision itself is failed. Without
	// a cap, a permanently bad image reference or expired registry credential
	// would recreate the Job forever with the CR never reaching Ready or Failed.
	maxPrepullRetries = 3
	// prepullJobActiveDeadlineSeconds bounds how long the pre-pull Job's pod may
	// run before Kubernetes marks it Failed. Without a deadline, a hung `crictl
	// pull` against an unreachable registry leaves the Job neither Complete nor
	// Failed, and reconcileImagePrepullJob polls it forever with no way to tell
	// "still pulling" from "will never finish".
	prepullJobActiveDeadlineSeconds = int64(20 * 60)
	// prepullRetryAnnotation tracks pre-pull Job retry attempts across Job
	// recreations. It lives on the NodeProvision's annotations rather than a
	// dedicated status field so it needs no CRD schema change; it is reset
	// whenever a NodeProvision restarts a full provisioning attempt from Failed.
	prepullRetryAnnotation = "ml.dcn.ssu.ac.kr/prepull-retry-count"
	// onPremBootstrapStallTimeout bounds how long the on-prem SSH bootstrap
	// goroutine may run. It has no other deadline (only manager shutdown
	// cancels its context), so a hung remote command (e.g. an apt-get lock
	// wait, a stalled kubeadm join) would otherwise leave the NodeProvision in
	// Bootstrapping, and r.onPremJobs populated, forever.
	onPremBootstrapStallTimeout = 20 * time.Minute
	// registeringNodeStallTimeout bounds how long a NodeProvision may wait for
	// its node to appear in Kubernetes (Joining/RegisteringNode/VerifyingHealth).
	// If cloud-init/kubelet never brings the node up, this fails the CR instead
	// of requeueing forever.
	registeringNodeStallTimeout = 15 * time.Minute
)

// npNodeResetScriptBase is the comprehensive node cleanup script run via SSH
// during deletion.  It mirrors resetNodeViaSSH in the RemoteCluster controller.
// See buildNodeResetScript for the WireGuard teardown that is added on top.
const npNodeResetScriptBase = `
if command -v kubeadm >/dev/null 2>&1; then
  sudo kubeadm reset --force 2>/dev/null || true
fi

sudo systemctl stop kubelet crio 2>/dev/null || true
sudo systemctl disable kubelet crio 2>/dev/null || true

awk '$2~/^\/var\/lib\/containers|^\/run\/containers/{print $2}' /proc/mounts \
  | sort -r | xargs -r sudo umount -l 2>/dev/null || true

sudo apt-mark unhold kubelet kubeadm kubectl 2>/dev/null || true
sudo apt-get purge -y kubelet kubeadm kubectl 2>/dev/null || true

sudo apt-get purge -y cri-o criu crun conmon 2>/dev/null || true

sudo apt-get purge -y nvidia-container-toolkit nvidia-container-toolkit-base \
  libnvidia-container-tools libnvidia-container1 2>/dev/null || true
sudo rm -f /etc/apt/sources.list.d/nvidia-container-toolkit.list 2>/dev/null || true
sudo rm -f /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg 2>/dev/null || true

sudo rm -rf /etc/kubernetes /var/lib/kubelet /var/lib/etcd 2>/dev/null || true

sudo rm -rf /var/lib/crio /run/crio /run/containers 2>/dev/null || true
sudo rm -rf /var/lib/containers 2>/dev/null || true
sudo rm -rf /var/log/crio 2>/dev/null || true
sudo rm -rf /etc/crio /etc/containers 2>/dev/null || true

sudo rm -rf /etc/criu 2>/dev/null || true

sudo rm -f  /etc/modules-load.d/k8s.conf /etc/sysctl.d/k8s.conf 2>/dev/null || true
sudo rm -f  /etc/apt/sources.list.d/kubernetes.list /etc/apt/sources.list.d/cri-o.list 2>/dev/null || true
sudo rm -f  /etc/apt/keyrings/kubernetes-apt-keyring.gpg /etc/apt/keyrings/cri-o-apt-keyring.gpg 2>/dev/null || true

sudo rm -f /usr/local/bin/crictl /usr/bin/crictl 2>/dev/null || true
sudo rm -f /usr/local/bin/crun   /usr/bin/crun   2>/dev/null || true
sudo rm -f /usr/sbin/runc /usr/local/sbin/runc   2>/dev/null || true
sudo rm -f /usr/sbin/criu                         2>/dev/null || true
sudo rm -f /usr/bin/crio /usr/local/bin/crio       2>/dev/null || true
sudo rm -f /usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || true

sudo rm -rf /etc/cdi 2>/dev/null || true

sudo rm -f /var/lib/node-bootstrap-complete 2>/dev/null || true

sudo apt-get autoremove -y 2>/dev/null || true

echo "node reset complete"
`

// npWireGuardTeardownScript brings the node's WireGuard tunnel down and
// removes it. Only used for nodes that were provisioned with a VPN.
const npWireGuardTeardownScript = `
nohup sudo bash -c '
  sleep 3
  systemctl stop    wg-quick@wg0 2>/dev/null || true
  wg-quick down wg0              2>/dev/null || true
  systemctl disable wg-quick@wg0 2>/dev/null || true
  rm -f /etc/wireguard/wg0.conf  2>/dev/null || true
  apt-get purge -y wireguard wireguard-tools 2>/dev/null || true
' >/dev/null 2>&1 &
`

// buildNodeResetScript returns the node cleanup script. WireGuard is only torn
// down when a VPN was in use; nodes provisioned with spec.disableVPN never had
// it set up by this controller, so any wireguard on the host is left alone.
func buildNodeResetScript(disableVPN bool) string {
	if disableVPN {
		return npNodeResetScriptBase
	}
	const tail = "echo \"node reset complete\"\n"
	return strings.Replace(npNodeResetScriptBase, tail, npWireGuardTeardownScript+"\n"+tail, 1)
}

// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions/finalizers,verbs=update
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisionnetconfigs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisionnetconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch;update;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// The namespaced marker below is load-bearing: controller-gen turns it into the
// Role "manager-role" in kube-public (config/rbac/role.yaml) that
// config/rbac/kube_public_role_binding.yaml binds. Do not remove it.
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get,namespace=kube-public
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete

func (r *NodeProvisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	np := &mlv1alpha1.NodeProvision{}
	if err := r.Get(ctx, req.NamespacedName, np); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log = log.WithValues(
		"nodeProvision", np.Name,
		"provider", np.Spec.Provider,
		"role", np.Spec.Role,
		"phase", np.Status.Phase,
	)

	if !np.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, np)
	}

	if ensureFinalizer(np, nodeProvisionFinalizer) {
		if err := r.Update(ctx, np); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}

	// The credentials Secret must live in the NodeProvision's own namespace
	// (see credentialsNamespace). Fail clearly instead of retrying a lookup
	// that can never be allowed. Phases that never read credentials
	// (Failed handling, Ready) are exempt so a terminal state is not rewritten.
	if np.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed && np.Status.Phase != mlv1alpha1.NodeProvisionPhaseReady {
		if _, nsErr := credentialsNamespace(np); nsErr != nil {
			return r.failNodeProvision(ctx, np, nsErr.Error())
		}
	}

	// For AWS nodes, keep a controller-owned copy of the credentials secret up-to-date.
	// The user-supplied secret can be deleted at any time; during teardown the
	// controller falls back to this owned copy to terminate the EC2 instance.
	// On-prem nodes use SSH keys embedded in the same secret so we copy it there too.
	if np.Spec.CredentialsRef.Name != "" {
		if userSecret, err := r.getSecret(ctx, np); err == nil {
			if err := r.ensureControllerCredsSecret(ctx, np, userSecret); err != nil {
				log.Error(err, "Failed to persist credential copy (non-fatal)")
			}
		}
	}

	// For AWS and GCP nodes, keep a controller-owned copy of the image-pull registry
	// credentials so that the pre-pull path can still function if the user
	// deletes the original secret after provisioning.
	if np.Spec.Provider == mlv1alpha1.CloudProviderAWS || np.Spec.Provider == mlv1alpha1.CloudProviderGCP {
		if netConfig, ncErr := r.requireNetConfig(ctx, np); ncErr == nil {
			if ref := netConfig.Spec.SoftwareConfig.ImagePullSecretRef; ref != nil {
				regSecret := &corev1.Secret{}
				if err := r.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: np.Namespace}, regSecret); err == nil {
					if err := r.ensureRegistryCredsSecret(ctx, np, regSecret); err != nil {
						log.Error(err, "Failed to persist registry credential copy (non-fatal)")
					}
				}
			}
		}
	}

	switch np.Status.Phase {
	case "", mlv1alpha1.NodeProvisionPhasePending,
		mlv1alpha1.NodeProvisionPhaseValidating,
		mlv1alpha1.NodeProvisionPhaseProvisioning:

		log.Info("Request received, starting provisioning")
		secret, err := r.getSecret(ctx, np)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
		}
		return r.reconcileProvisioning(ctx, np, secret)

	case mlv1alpha1.NodeProvisionPhaseCreatingInstance,
		mlv1alpha1.NodeProvisionPhaseConfiguringVPN:
		// These phases are written as progress markers while a reconcile is
		// actively creating the instance. A watch event on that write can
		// trigger a new reconcile before the creating reconcile has persisted
		// the InstanceID, causing the informer cache to return stale state.
		//
		// Never re-enter provisioning from these phases. If InstanceID has
		// landed (cache caught up), advance to WaitingForInstance. Otherwise
		// requeue briefly so the in-flight reconcile (or a controller restart
		// recovery) can complete.
		if np.Status.InstanceID != "" {
			log.Info("InstanceID found, advancing to WaitingForInstance", "instanceId", np.Status.InstanceID)
			now := metav1.Now()
			np.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
			np.Status.LastUpdated = &now
			_ = r.Status().Update(ctx, np)
			return ctrl.Result{RequeueAfter: requeueShort}, nil
		}
		// Crash recovery: RunInstances may have succeeded while the InstanceID
		// never reached status (crash, persist failure). The instance carries
		// this NodeProvision's UID tag, so look for it on every pass, before
		// the stall timeout, and adopt it. Only this branch runs from these
		// phases, so without this the instance would only be found after a
		// failure reset, by which time its VPN peer has been released.
		if res, handled, err := r.recoverLaunchedInstance(ctx, np); handled {
			return res, err
		}
		// Safety net: if this phase has been sitting without an InstanceID for
		// longer than creatingInstanceStallTimeout, the in-flight reconcile that
		// was supposed to complete it must have exited early (e.g. via a bare
		// error return) without ever calling failNodeProvision. Fail it here so
		// it goes through the normal retry/backoff path instead of requeueing
		// forever.
		if np.Status.LastUpdated != nil && time.Since(np.Status.LastUpdated.Time) > creatingInstanceStallTimeout {
			log.Info("NodeProvision stalled without an InstanceID, failing for retry",
				"phase", np.Status.Phase, "stalledFor", time.Since(np.Status.LastUpdated.Time))
			return r.failNodeProvision(ctx, np, fmt.Sprintf(
				"stalled in phase %s for over %s without a cloud instance being created", np.Status.Phase, creatingInstanceStallTimeout))
		}
		log.Info("Instance creation in progress, requeueing")
		return ctrl.Result{RequeueAfter: requeueShort}, nil

	case mlv1alpha1.NodeProvisionPhaseWaitingForInstance:
		if np.Status.LastUpdated != nil && time.Since(np.Status.LastUpdated.Time) > waitingForInstanceStallTimeout {
			log.Info("NodeProvision stalled waiting for instance to become running, failing for retry",
				"instanceId", np.Status.InstanceID, "stalledFor", time.Since(np.Status.LastUpdated.Time))
			return r.failNodeProvision(ctx, np, fmt.Sprintf(
				"instance %s did not become running within %s", np.Status.InstanceID, waitingForInstanceStallTimeout))
		}
		secret, err := r.getSecret(ctx, np)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
		}
		return r.reconcileWaitingForInstance(ctx, np, secret)

	case mlv1alpha1.NodeProvisionPhaseBootstrapping:
		// On-prem: poll the background SSH provisioning goroutine.
		// Skip getSecret when the goroutine is already running — it is only
		// needed if we need to restart provisioning (no goroutine in map).
		if np.Spec.Provider == mlv1alpha1.CloudProviderOnPrem {
			if _, running := r.loadOnPremJob(np); running {
				return r.pollOnPremBootstrap(ctx, np, nil)
			}
			secret, err := r.getSecret(ctx, np)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
			}
			return r.pollOnPremBootstrap(ctx, np, secret)
		}
		return r.reconcileJoining(ctx, np)

	case mlv1alpha1.NodeProvisionPhaseJoining,
		mlv1alpha1.NodeProvisionPhaseRegisteringNode,
		mlv1alpha1.NodeProvisionPhaseVerifyingHealth:
		return r.reconcileJoining(ctx, np)

	case mlv1alpha1.NodeProvisionPhasePrePullingImages:
		return r.reconcileNPImagePrepull(ctx, np)

	case mlv1alpha1.NodeProvisionPhaseReady:
		return r.syncRuntimeCredentials(ctx, np)

	case mlv1alpha1.NodeProvisionPhaseFailed:
		return r.reconcileFailed(ctx, np)

	default:
		return ctrl.Result{}, nil
	}
}

// ────────────────────────────────────────────────────────────────────────────
// reconcileProvisioning dispatches to the per-provider provisioning logic.
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) reconcileProvisioning(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// The VPN mode is decided by the cluster (via its NodeProvisionNetConfig),
	// so make this node follow it before anything below reads
	// np.Spec.DisableVPN.
	if res, done, err := r.reconcileVPNMode(ctx, np); done || err != nil {
		return res, err
	}

	switch np.Spec.Provider {
	case mlv1alpha1.CloudProviderAWS:
		return r.reconcileAWSProvisioning(ctx, np, secret)

	case mlv1alpha1.CloudProviderOnPrem:
		return r.reconcileOnPremProvisioning(ctx, np, secret)

	case mlv1alpha1.CloudProviderGCP:
		return r.reconcileGCPProvisioning(ctx, np, secret)

	case mlv1alpha1.CloudProviderAzure:
		log.Info("Azure provisioning not yet implemented")
		return ctrl.Result{}, fmt.Errorf("azure provider not yet implemented")

	default:
		return ctrl.Result{}, fmt.Errorf("unsupported cloud provider: %s", np.Spec.Provider)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// AWS provisioning
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) reconcileAWSProvisioning(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	name := np.Name

	// ── Idempotency guard ────────────────────────────────────────────────────
	// If an EC2 instance was already created (InstanceID persisted in status)
	// skip creation entirely and move straight to WaitingForInstance.
	// This prevents a duplicate launch when a prior reconcile created the
	// instance but failed to persist the status update.
	if np.Status.InstanceID != "" {
		log.Info("EC2 instance already exists, skipping creation", "instanceId", np.Status.InstanceID)
		if np.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
			now := metav1.Now()
			np.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
			np.Status.LastUpdated = &now
			if err := r.Status().Update(ctx, np); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// ── Resolve defaults (instanceType, AMI, network) ────────────────────────
	// resolveAWSDefaults patches the spec when fields are missing.  After a
	// patch the object's ResourceVersion changes; returning here lets the
	// watch event trigger a fresh reconcile with the up-to-date object so that
	// all subsequent status updates use the correct ResourceVersion.
	patched, err := r.resolveAWSDefaults(ctx, np, secret)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving AWS defaults: %v", err))
	}
	if patched {
		// A fresh reconcile will be queued by the spec-change watch event.
		// Return without error so the work queue uses its normal interval
		// instead of exponential backoff.
		return ctrl.Result{}, nil
	}

	// ── Validate ────────────────────────────────────────────────────────────
	if err := awsprovision.ValidateAWSConfig(np.Spec); err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("AWS validation failed: %v", err))
	}
	log.Info("AWS validation successful")

	// ── Set Validating status (non-critical; ignore conflict on the first run) ─
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseValidating, "Validating AWS configuration", 5)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		// Re-fetch so subsequent updates use the current ResourceVersion.
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
	}

	// ── Fetch NodeProvisionNetConfig ────────────────────────────────────────
	netConfig, err := r.requireNetConfig(ctx, np)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// ── Adopt an instance a previous attempt already launched ───────────────
	// RunInstances may have succeeded without the InstanceID reaching status
	// (crash / conflict). The instance is tagged with this NodeProvision's UID,
	// so look it up before allocating a second VPN peer and a second instance.
	creds, err := r.resolveAWSCreds(ctx, np.Spec.Region, secret)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving AWS credentials failed: %v", err))
	}
	if res, handled, aErr := r.adoptExistingInstance(ctx, np, creds, netConfig); handled {
		return res, aErr
	}

	// ── Connect to VPN server ───────────────────────────────────────────────
	if !np.Spec.DisableVPN {
		r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseConfiguringVPN, "Configuring VPN client", 15)
		if sErr := r.Status().Update(ctx, np); sErr != nil {
			if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	var vpnServerClient *ssh.Client
	if !np.Spec.DisableVPN {
		vpnServerClient, err = r.getVPNServerSSHClient(ctx, netConfig)
		if err != nil {
			return r.failNodeProvision(ctx, np, fmt.Sprintf("connecting to VPN server: %v", err))
		}
		defer vpnServerClient.Conn.Close() //nolint:errcheck
	}

	// ── Launch EC2 instance with cloud-init ─────────────────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseCreatingInstance, "Creating EC2 instance", 25)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
	}
	log.Info("Creating EC2 instance")

	runtimeCfg, err := r.resolveCnlabRuntimeConfig(ctx, netConfig.Spec.SoftwareConfig, netConfig.Namespace)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving cnlab-runtime config: %v", err))
	}

	result, err := r.aws().ProvisionEC2Node(ctx, np, creds, vpnServerClient, netConfig, runtimeCfg)
	if errors.Is(err, awsprovision.ErrInstanceAlreadyLaunched) {
		// RunInstances reports that an instance for this NodeProvision exists
		// already and the peer registered for this attempt was released. Not a
		// failure: the next reconcile (phase CreatingInstance/ConfiguringVPN)
		// finds the instance by its UID tag and adopts it.
		log.Info("An EC2 instance was already launched for this NodeProvision; requeueing to adopt it")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	// Always persist VPN allocation immediately — even on EC2 failure — so that
	// cleanupVPNPeer can find and release the peer on the next retry instead of
	// leaving it as an orphan and allocating yet another IP.
	var recordVPN func(*mlv1alpha1.NodeProvisionStatus)
	if result != nil && result.VpnIP != "" {
		if uErr := r.updateNetConfigStatus(ctx, netConfig, result.VpnIP, result.PublicKey, name); uErr != nil {
			log.Error(uErr, "persisting VPN allocation to NetConfig (non-fatal)")
		}
		vpnIP := result.VpnIP
		recordVPN = func(st *mlv1alpha1.NodeProvisionStatus) {
			st.VpnIP = vpnIP
			st.IPAddress = vpnIP
		}
	}
	if err != nil {
		return r.failNodeProvisionWith(ctx, np, fmt.Sprintf("EC2 provisioning failed: %v", err), recordVPN)
	}
	if result.Adopted {
		// ProvisionEC2Node found an instance launched earlier and allocated no
		// peer: recover the peer recorded for it (or terminate if none).
		log.Info("ProvisionEC2Node adopted an existing EC2 instance", "instanceId", result.InstanceID)
		return r.adoptInstance(ctx, np, result.InstanceID, netConfig)
	}
	log.Info("EC2 instance created", "instanceId", result.InstanceID)

	// Persist the InstanceID with a bounded retry against conflicts/transient
	// API errors. The EC2 instance already exists at this point, so we must
	// not fall back to failNodeProvision here: that would leave the phase
	// stuck without an InstanceID. If every retry attempt fails the instance is
	// still recoverable: it is tagged with this NodeProvision's UID, so the
	// CreatingInstance/ConfiguringVPN branch (recoverLaunchedInstance) finds and
	// adopts it on the next reconcile, and deletion terminates it by tag. The
	// creatingInstanceStallTimeout safety net only fires when no instance
	// can be found.
	if persistErr := r.persistInstanceID(ctx, np, result.InstanceID, result.VpnIP); persistErr != nil {
		log.Error(persistErr, "failed to persist InstanceID after retries — will retry on next reconcile",
			"instanceId", result.InstanceID)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	log.Info("EC2 instance created, waiting for it to become running", "instanceId", result.InstanceID)
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// resolveAWSDefaults auto-populates any missing AWS spec fields before validation:
//   - instanceType: derived from nodeLabel when not set (e.g. "cpu" → "t3.xlarge")
//   - awsConfig.ami: latest Ubuntu 22.04 LTS AMI for the region
//   - awsConfig.vpcId / subnetId / securityGroupIds: resolved from the region's
//     default VPC, creating one if none exists
//
// All resolved values are written back via a Patch so they are persisted in the
// CRD and visible to operators.  Fields already set by the user are never overwritten.
// resolveAWSKeyPair ensures an EC2 key pair exists for this node and that the
// private key is stored in a Kubernetes Secret named "<npName>-ssh-key".
//
// If the Secret already exists the stored private key is used to (re-)import
// the public half into EC2, making the function idempotent.
// If the Secret does not exist a fresh RSA key is generated, the private key
// is written to a new Secret, and the public key is imported into EC2.
//
// Returns the EC2 key pair name so the caller can set it in spec.awsConfig.
func (r *NodeProvisionReconciler) resolveAWSKeyPair(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	creds awsprovision.AWSCredentials,
) (keyPairName string, err error) {
	log := logf.FromContext(ctx)

	secretName := np.Name + "-ssh-key"
	secretKey := types.NamespacedName{Name: secretName, Namespace: np.Namespace}

	// Check whether the SSH-key Secret already exists.
	existing := &corev1.Secret{}
	var existingPrivKey string
	if err := r.Get(ctx, secretKey, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("looking up SSH key secret %q: %w", secretName, err)
		}
		// Secret does not exist — will be created below.
	} else {
		existingPrivKey = strings.TrimSpace(string(existing.Data["ssh-privatekey"]))
	}

	result, err := awsprovision.ResolveOrCreateKeyPair(ctx, np.Spec.Region, creds, np.Name, existingPrivKey)
	if err != nil {
		return "", err
	}

	// Persist the private key in a Secret when a new key was generated.
	if result.PrivateKeyPEM != "" {
		trueVal := true
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: np.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         mlv1alpha1.GroupVersion.String(),
						Kind:               "NodeProvision",
						Name:               np.Name,
						UID:                np.UID,
						Controller:         &trueVal,
						BlockOwnerDeletion: &trueVal,
					},
				},
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "node-provision-controller",
					"ml.dcn.ssu.ac.kr/node":        np.Name,
				},
			},
			Type: corev1.SecretTypeSSHAuth,
			Data: map[string][]byte{
				"ssh-privatekey": []byte(result.PrivateKeyPEM),
			},
		}
		if err := r.Create(ctx, secret); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return "", fmt.Errorf("creating SSH key secret %q: %w", secretName, err)
			}
		}
		log.Info("SSH key secret created", "secret", secretName)
	}

	log.Info("EC2 key pair ready", "keyPairName", result.KeyPairName, "secret", result.SecretName)
	return result.KeyPairName, nil
}

// resolveAWSDefaults returns (true, nil) when it patched the spec so the caller
// can return immediately and let the watch event trigger a fresh reconcile.
func (r *NodeProvisionReconciler) resolveAWSDefaults(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (patched bool, err error) {
	log := logf.FromContext(ctx)

	needsNetwork := np.Spec.AWSConfig == nil ||
		np.Spec.AWSConfig.SubnetID == "" ||
		len(np.Spec.AWSConfig.SecurityGroupIDs) == 0
	needsAMI := np.Spec.AWSConfig == nil || np.Spec.AWSConfig.AMI == ""
	needsInstanceType := np.Spec.InstanceType == ""
	needsKeyPair := np.Spec.AWSConfig == nil || np.Spec.AWSConfig.KeyPairName == ""

	if !needsNetwork && !needsAMI && !needsInstanceType && !needsKeyPair {
		return false, nil // nothing to resolve
	}

	creds, err := r.resolveAWSCreds(ctx, np.Spec.Region, secret)
	if err != nil {
		return false, fmt.Errorf("resolving AWS credentials failed: %w", err)
	}
	base := np.DeepCopy()
	if np.Spec.AWSConfig == nil {
		np.Spec.AWSConfig = &mlv1alpha1.AWSConfig{}
	}

	// ── Instance type from node label ────────────────────────────────────────
	if needsInstanceType {
		it := awsprovision.DefaultInstanceTypeForLabel(np.Spec.NodeLabel)
		if it == "" {
			return false, fmt.Errorf("spec.instanceType is required: no default instance type defined for nodeLabel %q", np.Spec.NodeLabel)
		}
		np.Spec.InstanceType = it
		log.Info("Resolved instance type from nodeLabel", "nodeLabel", np.Spec.NodeLabel, "instanceType", it)
	}

	// ── Validate instance type is available in the region ────────────────────
	if err := awsprovision.ValidateInstanceTypeAvailability(ctx, np.Spec.Region, np.Spec.InstanceType, creds); err != nil {
		return false, err
	}

	// ── AMI: latest Ubuntu 22.04 for the region ───────────────────────────────
	if needsAMI {
		log.Info("Resolving latest Ubuntu 22.04 AMI", "region", np.Spec.Region)
		amiID, err := awsprovision.ResolveUbuntu22AMI(ctx, np.Spec.Region, creds)
		if err != nil {
			return false, fmt.Errorf("resolving Ubuntu 22.04 AMI: %w", err)
		}
		np.Spec.AWSConfig.AMI = amiID
		log.Info("Resolved AMI", "ami", amiID)
	}

	// ── Network: default VPC / subnet / security group ───────────────────────
	if needsNetwork {
		log.Info("Resolving default network config", "region", np.Spec.Region)
		netCfg, err := awsprovision.ResolveOrCreateNetworkConfig(ctx, np.Spec.Region, creds, !np.Spec.DisableVPN)
		if err != nil {
			return false, fmt.Errorf("resolving AWS network config: %w", err)
		}
		if np.Spec.AWSConfig.VPCID == "" {
			np.Spec.AWSConfig.VPCID = netCfg.VPCID
		}
		if np.Spec.AWSConfig.SubnetID == "" {
			np.Spec.AWSConfig.SubnetID = netCfg.SubnetID
		}
		if len(np.Spec.AWSConfig.SecurityGroupIDs) == 0 {
			np.Spec.AWSConfig.SecurityGroupIDs = []string{netCfg.SecurityGroupID}
		}
		log.Info("Resolved network config",
			"vpcId", np.Spec.AWSConfig.VPCID,
			"subnetId", np.Spec.AWSConfig.SubnetID,
			"securityGroupId", np.Spec.AWSConfig.SecurityGroupIDs[0],
		)
	}

	// ── Key pair: generate or reuse ─────────────────────────────────────────
	if needsKeyPair {
		kpName, err := r.resolveAWSKeyPair(ctx, np, creds)
		if err != nil {
			return false, fmt.Errorf("resolving EC2 key pair: %w", err)
		}
		np.Spec.AWSConfig.KeyPairName = kpName
		log.Info("Resolved EC2 key pair", "keyPairName", kpName)
	}

	if err := r.Patch(ctx, np, client.MergeFrom(base)); err != nil {
		return false, fmt.Errorf("patching NodeProvision spec with resolved defaults: %w", err)
	}
	log.Info("Patched NodeProvision spec with resolved AWS defaults")
	return true, nil
}

// reconcileWaitingForInstance polls EC2 until the instance is running, then
// transitions to the Joining phase.
func (r *NodeProvisionReconciler) reconcileWaitingForInstance(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if np.Spec.Provider == mlv1alpha1.CloudProviderGCP {
		return r.reconcileGCPWaitingForInstance(ctx, np, secret)
	}

	creds, err := r.resolveAWSCreds(ctx, np.Spec.Region, secret)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving AWS credentials failed: %v", err))
	}

	privateIP, publicIP, err := awsprovision.WaitForInstanceRunning(ctx, np, creds, np.Status.InstanceID)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("polling instance state: %v", err))
	}
	if privateIP == "" {
		log.Info("Instance not yet running, requeueing", "instanceId", np.Status.InstanceID)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	now := metav1.Now()
	np.Status.PrivateIP = privateIP
	np.Status.PublicIP = publicIP
	if np.Spec.DisableVPN {
		// Without a tunnel the kubelet node IP is the instance's private IP
		// (matches the IP cloud-init reads from instance metadata).
		np.Status.IPAddress = privateIP
	}
	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseBootstrapping
	np.Status.Message = "Instance running; cloud-init bootstrap in progress"
	np.Status.Progress = 50
	np.Status.LastUpdated = &now
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
	}
	log.Info("EC2 instance running", "privateIP", privateIP, "publicIP", publicIP)
	log.Info("Executing cloud-init bootstrap")
	return ctrl.Result{RequeueAfter: requeueJoining}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// On-Prem provisioning
// ────────────────────────────────────────────────────────────────────────────

// reconcileOnPremProvisioning validates SSH/VPN connectivity then starts a
// background goroutine for the long-running SSH provisioning work.  It returns
// immediately so the reconcile loop is not blocked.
func (r *NodeProvisionReconciler) reconcileOnPremProvisioning(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	key := onPremKey(np)

	// If a goroutine is already running for this node, just requeue to poll it.
	if _, running := r.loadOnPremJob(np); running {
		log.Info("On-prem bootstrap goroutine already running, requeueing")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// If the node this NodeProvision bootstrapped earlier is already registered
	// in the cluster (e.g. the controller restarted after the join, or a retry
	// after a post-join failure), do not run the SSH bootstrap again on a
	// working node — go straight to the join step.
	if node := r.findRegisteredNode(ctx, np); node != nil {
		log.Info("Node already registered in the cluster — skipping bootstrap", "node", node.Name)
		now := metav1.Now()
		applyAdoptedNode(np, node)
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
		np.Status.Message = "Node already registered; resuming at join"
		np.Status.Progress = 60
		np.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, np); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueJoining}, nil
	}

	// ── Phase: Validating ────────────────────────────────────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseValidating, "Validating SSH connectivity", 5)
	if err := r.Status().Update(ctx, np); err != nil {
		if ferr := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); ferr != nil {
			return r.failNodeProvision(ctx, np, fmt.Sprintf("re-fetching NodeProvision after status conflict: %v", ferr))
		}
	}

	sshClient, err := r.getSSHClient(ctx, np)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("SSH connectivity check failed: %v", err))
	}

	// Verify passwordless sudo before attempting any provisioning.
	// All provisioning commands require sudo in a non-interactive SSH session
	// (no TTY), so the node must have NOPASSWD configured for the SSH user.
	if out, err := ssh.Run(sshClient, "sudo -n true"); err != nil {
		sshClient.Conn.Close()
		return r.failNodeProvision(ctx, np, fmt.Sprintf(
			"passwordless sudo check failed — configure NOPASSWD for the SSH user on this node "+
				"(e.g. echo '<user> ALL=(ALL) NOPASSWD:ALL' | sudo tee /etc/sudoers.d/nopasswd): %v\nOutput: %s",
			err, out,
		))
	}

	netConfig, err := r.requireNetConfig(ctx, np)
	if err != nil {
		sshClient.Conn.Close()
		log.Info("No NodeProvisionNetConfig ready yet; requeueing")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	log.Info("Validation successful")

	// ── Phase: Configuring VPN ───────────────────────────────────────────────
	if !np.Spec.DisableVPN {
		r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseConfiguringVPN, "Configuring WireGuard VPN", 15)
		if err := r.Status().Update(ctx, np); err != nil {
			if ferr := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); ferr != nil {
				sshClient.Conn.Close()
				return r.failNodeProvision(ctx, np, fmt.Sprintf("re-fetching NodeProvision after status conflict: %v", ferr))
			}
		}
	}

	var vpnServerClient *ssh.Client
	if !np.Spec.DisableVPN {
		vpnServerClient, err = r.getVPNServerSSHClient(ctx, netConfig)
		if err != nil {
			sshClient.Conn.Close()
			return r.failNodeProvision(ctx, np, fmt.Sprintf("connecting to VPN server: %v", err))
		}
	}
	closeVPNClient := func() {
		if vpnServerClient != nil {
			vpnServerClient.Conn.Close() //nolint:errcheck
		}
	}

	// ── Phase: Bootstrapping — launch background goroutine ───────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseBootstrapping, "Installing packages and joining cluster (background)", 25)
	if err := r.Status().Update(ctx, np); err != nil {
		if ferr := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); ferr != nil {
			sshClient.Conn.Close() //nolint:errcheck
			closeVPNClient()
			// Unlike the Validating/ConfiguringVPN cases above, ConfiguringVPN was
			// already successfully persisted at this point (the Status().Update
			// call a few lines up succeeded). A bare error return here would leave
			// the CR sitting in the already-persisted ConfiguringVPN phase with no
			// background goroutine ever started — relying solely on the
			// creatingInstanceStallTimeout safety net to notice. Fail explicitly
			// instead so the retry starts immediately rather than after up to 10
			// minutes of silent "in progress" polling.
			return r.failNodeProvision(ctx, np, fmt.Sprintf("re-fetching NodeProvision after status conflict: %v", ferr))
		}
	}

	// Resolve runtime config before the goroutine so credentials are fetched
	// within the reconcile context (which has a proper timeout and client).
	runtimeCfg, err := r.resolveCnlabRuntimeConfig(ctx, netConfig.Spec.SoftwareConfig, netConfig.Namespace)
	if err != nil {
		sshClient.Conn.Close() //nolint:errcheck
		closeVPNClient()
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving cnlab-runtime config: %v", err))
	}

	// Snapshot values needed by the goroutine before returning.
	npCopy := np.DeepCopy()
	secretCopy := secret.DeepCopy()
	netConfigCopy := netConfig.DeepCopy()

	baseCtx := r.baseContext()
	// Bound the goroutine's total runtime. NewInClusterProvisioner runs a
	// series of blocking SSH commands with no deadline of its own; without
	// this, a hung remote command (e.g. an apt-get lock wait, a stalled
	// kubeadm join) would run forever. pollOnPremBootstrap's own no-progress
	// stall check is the backstop in case a blocking step doesn't actually
	// observe context cancellation. The cancel func is also kept on the job so
	// deletion or a stall can stop the bootstrap.
	goroutineCtx, cancel := context.WithTimeout(baseCtx, onPremBootstrapStallTimeout)

	ch := make(chan onPremJobResult, 1)
	r.onPremJobs.Store(key, &onPremJob{uid: np.UID, ch: ch, cancel: cancel})

	reportStep := func(step string) { r.onPremProgress.Store(key, step) }

	go func() {
		defer sshClient.Conn.Close()
		defer closeVPNClient()
		defer cancel()
		// Do NOT delete from onPremJobs here. The map entry must remain
		// until pollOnPremBootstrap has persisted the result. Deleting here
		// creates a window where the goroutine has finished but the result
		// hasn't been consumed yet: the next poll would see no map entry, no
		// VpnIP in status, and falsely restart provisioning.
		vpnNodeIP, publicKey, err := remotenodeprovision.NewInClusterProvisioner(
			goroutineCtx,
			npCopy,
			secretCopy,
			sshClient,
			vpnServerClient,
			netConfigCopy,
			reportStep,
			runtimeCfg,
		)
		ch <- onPremJobResult{vpnIP: vpnNodeIP, publicKey: publicKey, err: err}
	}()

	log.Info("On-prem bootstrap goroutine started")
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// pollOnPremBootstrap is called when phase == Bootstrapping for an on-prem node.
// It checks whether the background goroutine has finished and handles the result.
func (r *NodeProvisionReconciler) pollOnPremBootstrap(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	key := onPremKey(np)

	// If VpnIP is already persisted the goroutine completed in a prior reconcile
	// (or the controller restarted after completion). Advance to Joining.
	if np.Status.VpnIP != "" || np.Status.IPAddress != "" {
		if job, ok := r.loadOnPremJob(np); ok {
			r.dropOnPremJob(np, job)
		}
		return r.reconcileJoining(ctx, np)
	}

	job, running := r.loadOnPremJob(np)
	if !running {
		// No goroutine in memory — controller likely restarted mid-provisioning.
		// Re-enter provisioning to restart the SSH session.
		// secret is guaranteed non-nil here: the Bootstrapping case only passes
		// nil when it has already confirmed the goroutine is running.
		log.Info("No in-flight bootstrap goroutine found (possible restart), restarting provisioning")
		if secret == nil {
			var err error
			if secret, err = r.getSecret(ctx, np); err != nil {
				return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
			}
		}
		return r.reconcileOnPremProvisioning(ctx, np, secret)
	}

	// Collect the result if the goroutine has finished. It is cached on the job
	// so that a step below failing does not lose it (which would re-run the
	// whole bootstrap and allocate a second IP/peer).
	if job.result == nil {
		select {
		case res := <-job.ch:
			job.result = &res
		default:
		}
	}

	if job.result != nil {
		res := *job.result
		if res.err != nil {
			// The provisioner reports the VPN allocation together with the
			// error once the peer is registered. Record it (NetConfig + status,
			// in the same write as the Failed transition) so cleanupVPNPeer can
			// release the peer on retry/delete instead of leaking it.
			var record func(*mlv1alpha1.NodeProvisionStatus)
			if res.vpnIP != "" && !np.Spec.DisableVPN {
				r.recordAllocationInNetConfig(ctx, np, res.vpnIP, res.publicKey)
				vpnIP := res.vpnIP
				record = func(st *mlv1alpha1.NodeProvisionStatus) {
					st.VpnIP = vpnIP
					st.IPAddress = vpnIP
				}
			}
			// Forget the job only once the failure (with any allocated vpnIP)
			// is durably persisted. If the write fails the job and its cached
			// result stay, so the next poll retries the write instead of
			// restarting provisioning and allocating a second peer.
			fres, ferr := r.failNodeProvisionWith(ctx, np, fmt.Sprintf("on-prem provisioning failed: %v", res.err), record)
			if ferr != nil {
				return fres, ferr
			}
			r.dropOnPremJob(np, job)
			return fres, nil
		}
		log.Info("On-prem bootstrap completed", "vpnIP", res.vpnIP)

		netConfig, err := r.requireNetConfig(ctx, np)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting NetConfig after bootstrap: %w", err)
		}
		if !np.Spec.DisableVPN {
			if err := r.updateNetConfigStatus(ctx, netConfig, res.vpnIP, res.publicKey, np.Name); err != nil {
				return ctrl.Result{}, fmt.Errorf("updating NodeProvisionNetConfig status: %w", err)
			}
		}

		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
		now := metav1.Now()
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
		np.Status.Message = "Node bootstrapped; waiting for cluster registration"
		np.Status.IPAddress = res.vpnIP
		if !np.Spec.DisableVPN {
			np.Status.VpnIP = res.vpnIP
		}
		np.Status.Progress = 60
		np.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, np); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
		}
		// Only now is the result persisted; forget the job.
		r.dropOnPremJob(np, job)
		log.Info("On-prem node provisioned, waiting for cluster join", "vpnIP", res.vpnIP)
		return ctrl.Result{RequeueAfter: requeueJoining}, nil
	}

	// Still running — surface the current step in status so the user can see progress.
	step := "bootstrapping node (packages and cluster join in progress)"
	if v, ok := r.onPremProgress.Load(key); ok {
		if s, ok := v.(string); ok {
			step = s
		}
	}
	msg := fmt.Sprintf("Bootstrapping: %s", step)
	if np.Status.Message != msg {
		now := metav1.Now()
		np.Status.Message = msg
		np.Status.LastUpdated = &now
		_ = r.Status().Update(ctx, np)
	} else if np.Status.LastUpdated != nil && time.Since(np.Status.LastUpdated.Time) > onPremBootstrapStallTimeout {
		// No progress has been reported for over onPremBootstrapStallTimeout.
		// The background goroutine's context has its own deadline (set when
		// it was started), but if a blocking SSH call doesn't observe context
		// cancellation the goroutine (and its SSH connection) may still leak —
		// the NodeProvision itself must not be left stuck forever regardless.
		log.Info("On-prem bootstrap stalled with no progress, failing for retry",
			"step", step, "stalledFor", time.Since(np.Status.LastUpdated.Time))
		// Cancel the goroutine so it cannot keep mutating the node or the VPN
		// server, and pick up any VPN allocation it made so the peer is
		// released on retry.
		var record func(*mlv1alpha1.NodeProvisionStatus)
		job.cancel()
		if job.result == nil {
			select {
			case res := <-job.ch:
				job.result = &res
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
			}
		}
		if job.result != nil && job.result.vpnIP != "" && !np.Spec.DisableVPN {
			r.recordAllocationInNetConfig(ctx, np, job.result.vpnIP, job.result.publicKey)
			vpnIP := job.result.vpnIP
			record = func(st *mlv1alpha1.NodeProvisionStatus) {
				st.VpnIP = vpnIP
				st.IPAddress = vpnIP
			}
		}
		fres, ferr := r.failNodeProvisionWith(ctx, np, fmt.Sprintf(
			"on-prem bootstrap made no progress past step %q for over %s", step, onPremBootstrapStallTimeout), record)
		if ferr != nil {
			return fres, ferr // job (and any cached result) kept; retried on the next poll
		}
		r.dropOnPremJob(np, job)
		return fres, nil
	}
	log.Info("On-prem bootstrap in progress", "step", step)
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// reconcileJoining – shared by both AWS and on-prem paths
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) reconcileJoining(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing nodes: %w", err)
	}

	targetIP := np.Status.IPAddress // VPN IP
	var found *corev1.Node
	for i := range nodeList.Items {
		for _, addr := range nodeList.Items[i].Status.Addresses {
			if addr.Address == targetIP {
				found = &nodeList.Items[i]
				break
			}
		}
		if found != nil {
			break
		}
	}

	if found == nil {
		elapsed := ""
		if np.Status.LastUpdated != nil {
			elapsed = fmt.Sprintf(" (%.0fs since last update)", time.Since(np.Status.LastUpdated.Time).Seconds())
		}
		log.Info("Node not yet visible in cluster — waiting for kubelet to register",
			"lookingForIP", targetIP, "elapsed", elapsed)
		addrKind := "VPN IP"
		if np.Status.VpnIP == "" {
			addrKind = "node IP"
		}
		msg := fmt.Sprintf("Waiting for node to register with control plane (%s: %s)", addrKind, targetIP)
		// Only bump LastUpdated when something actually changed (phase just
		// became RegisteringNode, or the target IP changed). Otherwise
		// LastUpdated would be refreshed on every ~15s poll regardless of
		// progress, making it useless as a staleness signal below.
		if np.Status.Phase != mlv1alpha1.NodeProvisionPhaseRegisteringNode || np.Status.Message != msg {
			now := metav1.Now()
			np.Status.Phase = mlv1alpha1.NodeProvisionPhaseRegisteringNode
			np.Status.Message = msg
			np.Status.Progress = 70
			np.Status.LastUpdated = &now
			_ = r.Status().Update(ctx, np)
		} else if np.Status.LastUpdated != nil && time.Since(np.Status.LastUpdated.Time) > registeringNodeStallTimeout {
			// The node has never appeared in Kubernetes with this VPN IP for
			// over registeringNodeStallTimeout — cloud-init/kubelet likely
			// failed to come up. Fail instead of polling forever.
			log.Info("Node registration stalled, failing for retry",
				"lookingForIP", targetIP, "stalledFor", time.Since(np.Status.LastUpdated.Time))
			return r.failNodeProvision(ctx, np, fmt.Sprintf(
				"node with %s %s did not register with the control plane within %s", addrKind, targetIP, registeringNodeStallTimeout))
		}
		return ctrl.Result{RequeueAfter: requeueJoining}, nil
	}

	log.Info("Node registered with control plane", "node", found.Name)

	// Persist the node name BEFORE touching the Node object: once the Node
	// carries our finalizer and labels, deletion must be able to find it. If
	// this write fails we return before stamping anything.
	if np.Status.NodeName != found.Name {
		now := metav1.Now()
		np.Status.NodeName = found.Name
		np.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, np); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting node name before stamping node: %w", err)
		}
	}

	gpu := isGPUNode(np)

	// Stamp ownership labels + hardware-type label + management finalizer onto
	// the Kubernetes Node object in a single patch.  We always do this so that
	// even nodes whose NodeProvision has no NodeLabel still carry the ownership
	// metadata and are protected against accidental kubectl-delete.
	{
		patch := client.MergeFrom(found.DeepCopy())
		if found.Labels == nil {
			found.Labels = map[string]string{}
		}
		// Ownership labels — let anyone find the parent NodeProvision CR.
		found.Labels[nodeProvisionNameLabel] = np.Name
		found.Labels[nodeProvisionNsLabel] = np.Namespace
		found.Labels[nodeProvisionProviderLabel] = string(np.Spec.Provider)
		// UID is the definitive unique identifier: immutable, cluster-scoped,
		// survives a CR delete+recreate with the same name.
		found.Labels[nodeProvisionUIDLabel] = string(np.UID)
		// Optional user-supplied hardware class label.
		if np.Spec.NodeLabel != "" {
			found.Labels["hardware-type"] = np.Spec.NodeLabel
		}
		// DaemonSet targeting labels — used by prepull DaemonSets (deployed on CP
		// during cluster init) to schedule image pre-pull pods on the right nodes.
		// Mirrors the labeling done by RemoteCluster reconcileWorker for SSH-joined nodes.
		found.Labels["infra.dcn.ssu.ac.kr/worker"] = "true"
		hwType := "cpu"
		if gpu {
			hwType = "gpu"
		}
		found.Labels["infra.dcn.ssu.ac.kr/hardware-type"] = hwType
		// GPU-specific labels and taint — mirrors what RemoteCluster does for
		// GPU workers via kubectl label/taint on the control-plane.
		if gpu {
			found.Labels["gpu"] = "on"
			gpuTaint := corev1.Taint{
				Key:    "hardware-type",
				Value:  "gpu",
				Effect: corev1.TaintEffectPreferNoSchedule,
			}
			hasTaint := false
			for _, t := range found.Spec.Taints {
				if t.Key == gpuTaint.Key && t.Effect == gpuTaint.Effect {
					hasTaint = true
					break
				}
			}
			if !hasTaint {
				found.Spec.Taints = append(found.Spec.Taints, gpuTaint)
			}
		}
		// Finalizer on the Node prevents `kubectl delete node` from bypassing
		// controller-managed cleanup (VPN peer removal, EC2 termination, etc.).
		controllerutil.AddFinalizer(found, nodeProvisionNodeFinalizer)
		if err := r.Patch(ctx, found, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patching node ownership labels/finalizer: %w", err)
		}
		log.Info("Stamped ownership metadata on node",
			"node", found.Name,
			nodeProvisionNameLabel, np.Name,
			nodeProvisionNsLabel, np.Namespace,
			nodeProvisionProviderLabel, string(np.Spec.Provider),
			nodeProvisionUIDLabel, string(np.UID),
		)
	}

	now := metav1.Now()
	np.Status.NodeName = found.Name
	np.Status.LastUpdated = &now

	// GPU CDI configuration — mirrors JoinWorkerNode Phase 6 in kubeadm.go.
	// Creates the CDI directories and enables CDI support in CRI-O so that
	// the GPU Operator can inject GPU devices via CDI specs.
	if gpu {
		if sshClient, err := r.getSSHClientByProvider(ctx, np); err != nil {
			log.Error(err, "Cannot SSH to GPU node for CDI configuration (continuing)")
		} else {
			cdiCmds := []string{
				"sudo mkdir -p /etc/cdi /var/run/cdi /etc/crio/crio.conf.d",
				"test -f /etc/crio/crio.conf.d/99-cdi.conf || " +
					`printf '[crio.runtime]\nenable_cdi = true\ncdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]\n' ` +
					"| sudo tee /etc/crio/crio.conf.d/99-cdi.conf > /dev/null",
			}
			for _, cmd := range cdiCmds {
				if out, cmdErr := ssh.Run(sshClient, cmd); cmdErr != nil {
					log.Error(cmdErr, "GPU CDI configuration step failed (continuing)", "output", out)
				}
			}
			sshClient.Conn.Close()
			log.Info("GPU CDI configured on node", "node", found.Name)
		}
	}

	// GPU nodes with images configured get an intermediate phase so the
	// background goroutine can pull without blocking further reconciles.
	// Works for both on-prem (SSH via VPN IP) and AWS (SSH via VPN IP set during provisioning).
	// Image list comes from NodeProvisionNetConfig.Spec.SoftwareConfig.ImagePrepulls.
	if gpu {
		netConfig, err := r.requireNetConfig(ctx, np)
		if err == nil && len(netConfig.Spec.SoftwareConfig.ImagePrepulls) > 0 {
			np.Status.Phase = mlv1alpha1.NodeProvisionPhasePrePullingImages
			np.Status.Message = "Node joined; pre-pulling GPU images in background"
			np.Status.Progress = 90
			if err := r.Status().Update(ctx, np); err != nil {
				return ctrl.Result{}, fmt.Errorf("updating NodeProvision status to PrePullingImages: %w", err)
			}
			log.Info("GPU node joined — starting image pre-pull",
				"node", found.Name, "images", len(netConfig.Spec.SoftwareConfig.ImagePrepulls))
			return ctrl.Result{Requeue: true}, nil
		}
	}

	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseReady
	np.Status.Message = "Node successfully joined cluster"
	np.Status.Progress = 100
	np.Status.CompletionTime = &now
	np.Status.ProvisionRetryCount = 0
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status to Ready: %w", err)
	}
	log.Info("Node reached Ready state", "node", found.Name)
	return ctrl.Result{}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// reconcileNPImagePrepull – GPU image pre-pull for NodeProvision
// ────────────────────────────────────────────────────────────────────────────
// All providers use a Kubernetes Job that mounts crictl and the CRI socket
// from the host node — no SSH required for any path.

func (r *NodeProvisionReconciler) reconcileNPImagePrepull(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
) (ctrl.Result, error) {
	netConfig, err := r.requireNetConfig(ctx, np)
	if err != nil {
		return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
	}
	// Filter images by node target: GPU nodes pull "gpu" and "all"; CPU nodes pull "all" only.
	gpu := isGPUNode(np)
	images := make([]mlv1alpha1.ImagePrepull, 0, len(netConfig.Spec.SoftwareConfig.ImagePrepulls))
	for _, ip := range netConfig.Spec.SoftwareConfig.ImagePrepulls {
		if ip.NodeTarget == "gpu" && !gpu {
			continue
		}
		images = append(images, ip)
	}
	if len(images) == 0 {
		return r.markNodeProvisionReady(ctx, np)
	}
	return r.reconcileImagePrepullJob(ctx, np, netConfig, images)
}

// markNodeProvisionReady re-fetches np and stamps it as Ready.
func (r *NodeProvisionReconciler) markNodeProvisionReady(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("refreshing NodeProvision before marking Ready: %w", err)
	}
	now := metav1.Now()
	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseReady
	np.Status.Message = "Node successfully joined cluster"
	np.Status.Progress = 100
	np.Status.CompletionTime = &now
	np.Status.LastUpdated = &now
	np.Status.ProvisionRetryCount = 0
	return ctrl.Result{}, r.Status().Update(ctx, np)
}

// reconcileImagePrepullJob manages a Kubernetes Job that pre-pulls images by
// mounting crictl and the CRI socket from the host node — works for all providers.
func (r *NodeProvisionReconciler) reconcileImagePrepullJob(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
	images []mlv1alpha1.ImagePrepull,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	jobName := np.Name + "-prepull"

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: np.Namespace}, job)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("checking pre-pull job: %w", err)
	}

	if apierrors.IsNotFound(err) {
		if createErr := r.createImagePrepullJob(ctx, np, netConfig, images); createErr != nil {
			return ctrl.Result{}, fmt.Errorf("creating image pre-pull job: %w", createErr)
		}
		log.Info("Created image pre-pull job", "job", jobName, "images", len(images))
		return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
	}

	// Job exists — inspect conditions.
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			log.Info("Image pre-pull job completed", "job", jobName)
			return r.markNodeProvisionReady(ctx, np)
		}
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			// The Job came from the informer cache, which can still show a Job
			// this reconciler already counted and deleted (the annotation write
			// below wakes the reconciler before the cache catches up). Confirm
			// against the API server so one failure is counted exactly once.
			live := &batchv1.Job{}
			if err := r.apiReader().Get(ctx, types.NamespacedName{Name: jobName, Namespace: np.Namespace}, live); err != nil {
				if apierrors.IsNotFound(err) {
					log.V(1).Info("Cached pre-pull Job is already gone; not counting its failure again", "job", jobName)
					return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
				}
				return ctrl.Result{}, fmt.Errorf("confirming pre-pull job failure: %w", err)
			}
			if live.UID != job.UID || !jobFailed(live) {
				log.V(1).Info("Cached pre-pull Job is stale; waiting for the current one", "job", jobName)
				return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
			}
			attempts := prepullRetryCount(np) + 1
			_ = r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
			if attempts >= maxPrepullRetries {
				log.Error(fmt.Errorf("pre-pull job failed"), "Image pre-pull job failed after max retries, failing NodeProvision",
					"job", jobName, "attempts", attempts, "maxRetries", maxPrepullRetries)
				return r.failNodeProvision(ctx, np, fmt.Sprintf(
					"image pre-pull job failed after %d attempts", attempts))
			}
			log.Error(fmt.Errorf("pre-pull job failed"), "Image pre-pull job failed — deleting for retry",
				"job", jobName, "attempt", attempts, "maxRetries", maxPrepullRetries)
			if uErr := r.setPrepullRetryCount(ctx, np, attempts); uErr != nil {
				log.Error(uErr, "failed to persist pre-pull retry count (continuing)")
			}
			return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
		}
	}

	log.V(1).Info("Image pre-pull job running", "job", jobName)
	return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
}

// jobFailed reports whether the Job carries a true Failed condition.
func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// prepullRetryCount reads the image pre-pull retry count persisted on the
// NodeProvision's annotations. Missing or unparsable values are treated as 0.
func prepullRetryCount(np *mlv1alpha1.NodeProvision) int {
	v, ok := np.Annotations[prepullRetryAnnotation]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// setPrepullRetryCount persists the pre-pull retry count onto a freshly
// fetched copy of the NodeProvision, retrying on update conflicts.
func (r *NodeProvisionReconciler) setPrepullRetryCount(ctx context.Context, np *mlv1alpha1.NodeProvision, count int) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[prepullRetryAnnotation] = strconv.Itoa(count)
		return r.Update(ctx, fresh)
	})
}

// clearPrepullRetryCount removes the pre-pull retry annotation, if present, so
// that a fresh provisioning attempt (after a full Failed/retry cycle) starts
// with a clean pre-pull retry budget instead of inheriting the prior one.
func (r *NodeProvisionReconciler) clearPrepullRetryCount(ctx context.Context, np *mlv1alpha1.NodeProvision) error {
	if _, ok := np.Annotations[prepullRetryAnnotation]; !ok {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return err
		}
		if _, ok := fresh.Annotations[prepullRetryAnnotation]; !ok {
			return nil
		}
		delete(fresh.Annotations, prepullRetryAnnotation)
		return r.Update(ctx, fresh)
	})
}

// createImagePrepullJob creates a Job that runs crictl pull for each image on
// the target node, mounting crictl and the CRI socket from the host.
func (r *NodeProvisionReconciler) createImagePrepullJob(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
	images []mlv1alpha1.ImagePrepull,
) error {
	// Resolve which secret to project into the Job env.
	// For AWS: the controller maintains a copy (<name>-registry-creds) that
	// survives if the user deletes the original — prefer it.
	// For all providers: fall back to the original secret name.
	var registrySecretName string
	if ref := netConfig.Spec.SoftwareConfig.ImagePullSecretRef; ref != nil {
		copyName := np.Name + registryCredsSuffix
		if err := r.Get(ctx, types.NamespacedName{Name: copyName, Namespace: np.Namespace}, &corev1.Secret{}); err == nil {
			registrySecretName = copyName
		} else {
			registrySecretName = ref.Name
		}
	}

	trueVal := true
	privileged := true
	hostPathFile := corev1.HostPathFile
	hostPathSocket := corev1.HostPathSocket
	ttl := int32(600)   // auto-delete 10 min after completion
	backoff := int32(5) // retry pod up to 5 times
	activeDeadline := prepullJobActiveDeadlineSeconds

	env := []corev1.EnvVar{}
	if registrySecretName != "" {
		env = append(env,
			corev1.EnvVar{
				Name: "REGISTRY_USER",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: registrySecretName},
					Key:                  "username",
					Optional:             &trueVal,
				}},
			},
			corev1.EnvVar{
				Name: "REGISTRY_PASS",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: registrySecretName},
					Key:                  "password",
					Optional:             &trueVal,
				}},
			},
		)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      np.Name + "-prepull",
			Namespace: np.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         mlv1alpha1.GroupVersion.String(),
				Kind:               "NodeProvision",
				Name:               np.Name,
				UID:                np.UID,
				Controller:         &trueVal,
				BlockOwnerDeletion: &trueVal,
			}},
			Labels: map[string]string{nodeProvisionNameLabel: np.Name},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   &activeDeadline,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeName:      np.Status.NodeName,
					RestartPolicy: corev1.RestartPolicyOnFailure,
					// Tolerate any taint — new nodes often have not-ready taints.
					Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
					Containers: []corev1.Container{{
						Name:            "prepull",
						Image:           "ubuntu:22.04",
						Command:         []string{"/bin/bash", "-c"},
						Args:            []string{buildPrepullScript(images)},
						Env:             env,
						SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "crictl", MountPath: "/usr/local/bin/crictl"},
							{Name: "cri-socket", MountPath: "/var/run/crio/crio.sock"},
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "crictl",
							VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
								Path: "/usr/local/bin/crictl",
								Type: &hostPathFile,
							}},
						},
						{
							Name: "cri-socket",
							VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
								Path: "/var/run/crio/crio.sock",
								Type: &hostPathSocket,
							}},
						},
					},
				},
			},
		},
	}
	return r.Create(ctx, job)
}

// buildPrepullScript returns a bash script that pulls each image via crictl.
// Image references come from a user-editable CR and the registry credentials
// from a Secret, so nothing is interpolated unquoted: images are single-quoted
// shell words and the credentials are passed as one argv element.
func buildPrepullScript(images []mlv1alpha1.ImagePrepull) string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString("CRICTL=/usr/local/bin/crictl\n")
	b.WriteString("ENDPOINT=unix:///var/run/crio/crio.sock\n")
	b.WriteString("CREDS=()\n")
	b.WriteString("if [ -n \"${REGISTRY_USER:-}\" ] && [ -n \"${REGISTRY_PASS:-}\" ]; then\n")
	b.WriteString("  CREDS=(--creds \"${REGISTRY_USER}:${REGISTRY_PASS}\")\n")
	b.WriteString("fi\n")
	for _, ip := range images {
		img := strings.TrimSpace(ip.Image)
		if img == "" {
			continue
		}
		q := shellQuote(img)
		fmt.Fprintf(&b, "printf '[prepull] Pulling %%s...\\n' %s\n", q)
		fmt.Fprintf(&b, "\"$CRICTL\" --runtime-endpoint \"$ENDPOINT\" pull ${CREDS[@]+\"${CREDS[@]}\"} %s\n", q)
	}
	b.WriteString("echo \"[prepull] Done.\"\n")
	return b.String()
}

// nodeSSHHost returns the address the controller should use to reach the node
// after provisioning: the VPN IP when a tunnel is in use, otherwise the node's
// own recorded address (status IP, then spec IP, then hostname). It follows
// what was provisioned, not the mutable spec.disableVPN: a VPN-less node's
// status IP (its real address, e.g. an AWS private IP that has no spec
// counterpart) stays the right address if the flag is edited afterwards.
func nodeSSHHost(np *mlv1alpha1.NodeProvision) string {
	if np.Status.VpnIP != "" {
		return np.Status.VpnIP
	}
	if np.Status.IPAddress != "" {
		return np.Status.IPAddress
	}
	if np.Spec.IPAddress != "" {
		return np.Spec.IPAddress
	}
	return np.Spec.Hostname
}

// getSSHClientByProvider opens an SSH connection to the node using the correct
// credentials for each provider: on-prem uses the user credential secret;
// AWS and GCP use the dedicated SSH key secret (<name>-ssh-key) created during
// instance provisioning.  All connect via the VPN IP that is reachable
// in-cluster (or the node's own address when the VPN is disabled).
func (r *NodeProvisionReconciler) getSSHClientByProvider(ctx context.Context, np *mlv1alpha1.NodeProvision) (*ssh.Client, error) {
	host := nodeSSHHost(np)
	user := np.Spec.SSHUsernameOverride
	if user == "" {
		user = "ubuntu"
	}
	if np.Spec.Provider == mlv1alpha1.CloudProviderAWS || np.Spec.Provider == mlv1alpha1.CloudProviderGCP {
		sshKeySecret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      np.Name + "-ssh-key",
			Namespace: np.Namespace,
		}, sshKeySecret); err != nil {
			return nil, fmt.Errorf("fetching %s SSH key secret %q: %w", np.Spec.Provider, np.Name+"-ssh-key", err)
		}
		credBytes, err := resolveSecretKey(sshKeySecret, "")
		if err != nil {
			return nil, err
		}
		return dialSSH(host, np.Spec.SSHPort, user, string(credBytes))
	}
	return r.getSSHClientPostJoin(ctx, np)
}

// getSSHClientPostJoin opens an SSH connection to the node using its VPN IP
// (np.Status.VpnIP), which is reachable from the controller once the node has
// joined the cluster.  Falls back to the spec IP/hostname if VPN IP is empty.
func (r *NodeProvisionReconciler) getSSHClientPostJoin(ctx context.Context, np *mlv1alpha1.NodeProvision) (*ssh.Client, error) {
	secret, err := r.getSecret(ctx, np)
	if err != nil {
		return nil, fmt.Errorf("fetching SSH credential secret %q: %w", np.Spec.CredentialsRef.Name, err)
	}

	credBytes, err := resolveSecretKey(secret, np.Spec.CredentialsRef.Key)
	if err != nil {
		return nil, err
	}

	host := nodeSSHHost(np)
	user := np.Spec.SSHUsernameOverride
	if user == "" {
		user = "ubuntu"
	}
	return dialSSH(host, np.Spec.SSHPort, user, string(credBytes))
}

// ────────────────────────────────────────────────────────────────────────────
// handleDelete – deprovisions and removes the finalizer
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) handleDelete(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) { //nolint:unparam
	log := logf.FromContext(ctx)

	// Guard: if our finalizer is already gone a previous reconcile completed
	// cleanup successfully.  A stale watch event can re-deliver the delete
	// notification after the CR is already gone; skip to avoid double-work and
	// a spurious "not found" error on the final Update.
	if !controllerutil.ContainsFinalizer(np, nodeProvisionFinalizer) {
		log.Info("Finalizer already removed — cleanup previously completed, skipping")
		return ctrl.Result{}, nil
	}

	log.Info("Deprovisioning node")

	if np.Status.Phase != mlv1alpha1.NodeProvisionPhaseDeleting {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseDeleting
		now := metav1.Now()
		np.Status.LastUpdated = &now
		_ = r.Status().Update(ctx, np)
	}

	// ── Stop any in-flight on-prem bootstrap ─────────────────────────────────
	// The goroutine keeps SSHing into the node and may register a VPN peer; it
	// must be cancelled, and what it allocated recorded, before cleanup runs.
	// Otherwise it runs on after deletion and a recreated CR of the same name
	// could pick up its result.
	if np.Spec.Provider == mlv1alpha1.CloudProviderOnPrem {
		pending, err := r.stopOnPremJob(ctx, np, 5*time.Second, deletionElapsed(np) < onPremJobStopWait)
		if err != nil {
			log.Error(err, "recording allocation of cancelled bootstrap — will retry")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if pending {
			log.Info("Waiting for cancelled on-prem bootstrap to stop")
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
	}

	// ── Remove Kubernetes node ──────────────────────────────────────────────
	// status.nodeName may be empty if the node registered but the name was never
	// persisted; fall back to the Node stamped with this CR's UID.
	nodeName, err := r.resolveNodeName(ctx, np)
	if err != nil {
		log.Error(err, "resolving node to remove — will retry")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	nodeDeleted := true
	if nodeName != "" {
		nodeDeleted = r.removeK8sNode(ctx, np, nodeName)
	}

	// ── Wait for node deletion confirmation before cleaning up AWS resources ──
	if !nodeDeleted {
		log.Info("Waiting for node to be deleted from cluster before cleaning up cloud resources")
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	// ── Provider-specific cleanup (must happen before secrets are deleted) ──────
	switch np.Spec.Provider {
	case mlv1alpha1.CloudProviderAWS:
		// SSH reset is skipped for AWS: the EC2 key pair (.pem) is not reliably
		// available post-provisioning. Terminating the instance is sufficient cleanup.
		if np.Status.InstanceID != "" {
			// Secrets and the finalizer must not be removed until the instance is
			// confirmed gone — otherwise it is orphaned with no retry path. On
			// failure requeue with a delay (not a tight loop).
			if err := r.terminateAWSInstance(ctx, np, np.Status.InstanceID); err != nil {
				log.Error(err, "EC2 termination failed — requeuing with delay", "instanceId", np.Status.InstanceID)
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			log.Info("EC2 instance terminated", "instanceId", np.Status.InstanceID)
			// Clear InstanceID so any duplicate or requeued reconcile skips
			// termination instead of retrying with potentially stale credentials.
			np.Status.InstanceID = ""
			if statusErr := r.Status().Update(ctx, np); statusErr != nil {
				log.Error(statusErr, "clearing InstanceID from status after termination (non-fatal)")
			}
		} else if r.terminateOrphanedInstance(ctx, np) {
			// No InstanceID was ever persisted, but an instance may have been
			// launched anyway (crash between RunInstances and the status write).
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	case mlv1alpha1.CloudProviderGCP:
		// Deletes the instance and the node's firewall rules; the SSH key lives in
		// the instance metadata and the <name>-ssh-key Secret is removed below.
		if res, wait := r.cleanupGCPOnDelete(ctx, np); wait {
			return res, nil
		}
	case mlv1alpha1.CloudProviderOnPrem:
		r.cleanupOnPremNode(ctx, np)
	}

	// ── Remove VPN peer ─────────────────────────────────────────────────────
	// A failure here would leak the peer and its IP once the finalizer is gone,
	// so keep the finalizer and retry. An unreachable VPN server must not block
	// deletion forever though: give up loudly after deletionGiveUpAfter.
	if err := r.cleanupVPNPeer(ctx, np); err != nil {
		if deletionElapsed(np) < deletionGiveUpAfter {
			log.Error(err, "cleaning up VPN peer failed — keeping finalizer and retrying",
				"giveUpIn", (deletionGiveUpAfter - deletionElapsed(np)).Round(time.Second).String())
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		log.Error(err, "GIVING UP on VPN peer cleanup after the deletion grace period; the WireGuard peer and its IP may be leaked and need manual removal",
			"vpnIP", np.Status.VpnIP, "gracePeriod", deletionGiveUpAfter.String())
	}

	// ── Clean up node-related secrets (after cloud resources are gone) ────────
	sshKeySecret := &corev1.Secret{}
	sshKeyName := np.Name + "-ssh-key"
	if err := r.Get(ctx, types.NamespacedName{Name: sshKeyName, Namespace: np.Namespace}, sshKeySecret); err == nil {
		if err := r.Delete(ctx, sshKeySecret); client.IgnoreNotFound(err) != nil {
			log.Error(err, "deleting SSH key secret", "secret", sshKeyName)
		} else {
			log.Info("Deleted SSH key secret", "secret", sshKeyName)
		}
	}

	controllerutil.RemoveFinalizer(np, nodeProvisionFinalizer)
	if err := r.Update(ctx, np); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	r.nodeResetUIDs.Delete(np.UID)
	log.Info("Cleanup complete")
	return ctrl.Result{}, nil
}

// cleanupVPNPeer removes the peer from the VPN server's running config and
// persisted wg0.conf, then releases the IP from the NodeProvisionNetConfig status.
//
// Public-key resolution order (most specific → most defensive):
//  1. CR status (VPNPeers list) — fast path, always tried first.
//  2. Live VPN server (wg show wg0 dump keyed by VPN IP) — fallback when the
//     CR status was never written or has drifted, e.g. provisioning crashed
//     before updateNetConfigStatus completed.
//
// The VPN server connection is always opened so the fallback can be attempted
// even when the CR has no record of this peer.
func (r *NodeProvisionReconciler) cleanupVPNPeer(ctx context.Context, np *mlv1alpha1.NodeProvision) error {
	log := logf.FromContext(ctx)

	// A node provisioned without the VPN never allocates a VPN IP or peer, and
	// its Status.IPAddress is its real address — not a VPN IP to look up on the
	// server (which a VPN-less cluster does not even have). Gate on what was
	// provisioned, not on the mutable spec flag alone: with no recorded VPN IP,
	// either the flag or a recorded node address means "no VPN", so a VPN-less
	// node whose spec.disableVPN was edited back to false still never contacts
	// a VPN server, while a peer that was actually allocated (VpnIP recorded)
	// is still released whatever the flag says.
	if provisionedWithoutVPN(np) {
		log.Info("VPN disabled for this node — skipping VPN peer cleanup")
		return nil
	}

	netConfig, err := r.netConfigFor(ctx, np)
	if errors.Is(err, errNetConfigNotFound) {
		log.Info("No matching NodeProvisionNetConfig found — skipping VPN peer cleanup", "reason", err.Error())
		return nil
	}
	if err != nil {
		// Ambiguous (or a failed list): never release a peer from a guessed
		// config. Surfaces through the deletion retry until spec.clusterName is set.
		return fmt.Errorf("selecting NodeProvisionNetConfig for VPN cleanup: %w", err)
	}

	// ── 1. Resolve public key and VPN IP from CR status ─────────────────────
	// Do this BEFORE connecting to the VPN server so we still have the peer key
	// on retry even if the server connection previously failed and we have not
	// yet updated (cleared) the NetConfig status.
	var peerPublicKey string
	var resolvedVpnIP string
	for _, p := range netConfig.Status.VPNPeers {
		if p.NodeName == np.Name || p.VPNIP == np.Status.VpnIP {
			peerPublicKey = p.PublicKey
			resolvedVpnIP = p.VPNIP
			break
		}
	}

	vpnIP := resolvedVpnIP
	if vpnIP == "" {
		vpnIP = np.Status.VpnIP
	}
	if vpnIP == "" {
		vpnIP = np.Status.IPAddress
	}

	// Nothing was ever allocated for this node (no recorded peer, no VPN IP):
	// there is nothing to release, so do not depend on the VPN server being
	// reachable — e.g. when the failure being retried is "cannot reach the
	// VPN server" itself.
	if peerPublicKey == "" && vpnIP == "" {
		log.Info("No VPN allocation recorded for this node — nothing to release")
		return nil
	}

	// ── 2. Connect to VPN server ─────────────────────────────────────────────
	vpn, err := r.dialVPN(ctx, netConfig)
	if err != nil {
		return fmt.Errorf("connecting to VPN server for peer removal: %w", err)
	}
	defer vpn.Close() //nolint:errcheck

	// ── 3. Fallback: look up public key from live server when CR has no record ─
	if peerPublicKey == "" && vpnIP != "" {
		serverPeers, lookupErr := vpn.ReadPeers()
		if lookupErr != nil {
			log.Error(lookupErr, "reading live VPN peer list for fallback lookup")
		} else if key, ok := serverPeers[vpnIP]; ok {
			peerPublicKey = key
			log.Info("Resolved peer public key from live VPN server (CR status was missing)",
				"vpnIP", vpnIP, "publicKey", peerPublicKey)
		}
	}

	if key, ok := usablePeerKey(peerPublicKey); !ok {
		log.Error(fmt.Errorf("invalid WireGuard public key format"),
			"refusing to use malformed peer key for removal; releasing the IP only", "vpnIP", vpnIP)
		peerPublicKey = ""
	} else {
		peerPublicKey = key
	}

	if peerPublicKey == "" {
		if vpnIP != "" {
			log.Info("No peer found in CR status or on VPN server for this node — nothing to remove",
				"vpnIP", vpnIP)
		}
		// Still fall through to release the IP from the NetConfig status below.
	} else {
		// ── 4./5. Remove from the running config and from wg0.conf ────────────
		if err := vpn.RemovePeer(peerPublicKey); err != nil {
			log.Error(err, "removing WireGuard peer from the VPN server", "publicKey", peerPublicKey)
		}

		log.Info("Removed VPN peer from server", "publicKey", peerPublicKey, "vpnIP", vpnIP)
	}

	// ── 6. Release IP and peer entry from NetConfig status ───────────────────
	// Done AFTER the server removal so a retry still has the peer key available
	// if the server step fails and the controller restarts.
	// Retry on conflict: a concurrent SSH-based update (RemoteCluster controller
	// adding another worker's IP) must not prevent cleanup from completing.
	ncKey := types.NamespacedName{Name: netConfig.Name, Namespace: netConfig.Namespace}
	for attempt := 0; attempt < 5; attempt++ {
		if err := r.Get(ctx, ncKey, netConfig); err != nil {
			return fmt.Errorf("re-fetching NetConfig for IP release: %w", err)
		}
		base := netConfig.DeepCopy()

		newPeers := make([]mlv1alpha1.VPNPeerStatus, 0, len(netConfig.Status.VPNPeers))
		for _, p := range netConfig.Status.VPNPeers {
			if p.NodeName == np.Name || p.VPNIP == vpnIP {
				continue
			}
			newPeers = append(newPeers, p)
		}
		newIPs := make([]string, 0, len(netConfig.Status.UsedIPAddresses))
		for _, ip := range netConfig.Status.UsedIPAddresses {
			if vpnIP != "" && ip == vpnIP {
				continue
			}
			newIPs = append(newIPs, ip)
		}
		netConfig.Status.VPNPeers = newPeers
		netConfig.Status.UsedIPAddresses = newIPs

		if err := r.Status().Patch(ctx, netConfig, client.MergeFrom(base)); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("patching NodeProvisionNetConfig after VPN peer removal: %w", err)
		}
		return nil
	}
	return fmt.Errorf("releasing NetConfig IP: too many conflicts")
}

// usablePeerKey returns k when it may be used to remove a peer. The key ends
// up in shell commands on the VPN server, so anything that is not a well-formed
// WireGuard public key (from a tampered NetConfig status or a bad server
// line) is rejected and must never be interpolated. An empty key is fine: it
// simply means there is no peer to remove.
func usablePeerKey(k string) (string, bool) {
	if k == "" {
		return "", true
	}
	if !validWireGuardKey(k) {
		return "", false
	}
	return k, true
}

// wireGuardProvisioned reports whether this controller set WireGuard up on the
// node. It follows what was provisioned (see provisionedWithoutVPN), not the
// mutable spec: VPN-less nodes — including ones whose spec.disableVPN was
// edited back to false — leave any host-owned WireGuard alone, while a node
// whose VPN mode was flipped to true after provisioning is still torn down
// correctly.
func wireGuardProvisioned(np *mlv1alpha1.NodeProvision) bool {
	return !provisionedWithoutVPN(np)
}

// provisionedWithoutVPN reports whether np has no VPN allocation to clean up.
// A node with a recorded VPN IP always had one. Otherwise it is VPN-less when
// spec.disableVPN says so, or when a node address was recorded without a VPN
// IP: every VPN path records the two together (status.vpnIP == status.ipAddress)
// while a VPN-less node records only its real address, so that combination
// survives a later edit of spec.disableVPN back to false.
func provisionedWithoutVPN(np *mlv1alpha1.NodeProvision) bool {
	if np.Status.VpnIP != "" {
		return false
	}
	return np.Spec.DisableVPN || np.Status.IPAddress != ""
}

// cleanupOnPremNode resets the physical node once per deletion. Deletion is
// retried while a later step (VPN peer release) keeps failing; without the
// marker the reset script would rerun on every retry. Failures are logged but
// never block the finalizer removal.
func (r *NodeProvisionReconciler) cleanupOnPremNode(ctx context.Context, np *mlv1alpha1.NodeProvision) {
	log := logf.FromContext(ctx)
	if r.nodeResetDone(np) {
		log.Info("Node reset already ran for this deletion — not repeating it")
		return
	}
	reset := r.nodeReset
	if reset == nil {
		reset = r.resetOnPremNodeViaSSH
	}
	if reset(ctx, np) {
		r.markNodeResetDone(ctx, np)
	}
}

// resetOnPremNodeViaSSH SSHes into the physical node (best-effort) and reverses
// the provisioning: resets kubeadm, stops WireGuard, and removes its config.
// It reports whether the script actually ran (false when the node could not be
// reached, so a later retry may still try).
func (r *NodeProvisionReconciler) resetOnPremNodeViaSSH(ctx context.Context, np *mlv1alpha1.NodeProvision) bool {
	log := logf.FromContext(ctx)

	sshClient, err := r.getSSHClient(ctx, np)
	if err != nil {
		// Credential secret may have been deleted before the NodeProvision.
		// VPN peer and Kubernetes node are already cleaned up — this is
		// best-effort kubeadm reset on the physical node.
		log.Info("Cannot SSH to on-prem node for kubeadm reset — credential secret missing or node unreachable; skipping node-side cleanup",
			"err", err)
		return false
	}
	defer sshClient.Conn.Close()

	if out, err := ssh.Run(sshClient, buildNodeResetScript(!wireGuardProvisioned(np))); err != nil {
		log.Error(err, "on-prem node reset script reported errors (continuing)", "output", redactSecrets(out))
	}
	log.Info("On-prem node reset complete")
	return true
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

// joinTokenMaxAge is the window before the 24-hour kubeadm token expiry in
// which the NodeProvision controller proactively issues a fresh bootstrap token.
const joinTokenMaxAge = 20 * time.Hour

// reconcileVPNMode makes np.Spec.DisableVPN follow the cluster's mode, which is
// carried by the NodeProvisionNetConfig (synced from the control-plane): flannel
// is pinned to wg0 on VPN clusters and the API server is advertised on the wg0
// (or, without a VPN, the host) address, so a mixed cluster cannot work.
//   - cluster without the VPN, node unset → the node inherits it (persisted, so
//     deletion cleanup does not depend on the NetConfig still existing).
//   - cluster with the VPN, node disabled → the node is failed.
//
// done is true when the caller must return res immediately. When no
// NetConfig exists yet this is a no-op; requireNetConfig handles that later.
func (r *NodeProvisionReconciler) reconcileVPNMode(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, done bool, err error) {
	nc, err := r.netConfigFor(ctx, np)
	if errors.Is(err, errNetConfigNotFound) {
		return ctrl.Result{}, false, nil
	}
	if errors.Is(err, errNetConfigAmbiguous) {
		// A spec error the user must fix (set spec.clusterName); like the VPN
		// mode mismatch below it must not consume the retry budget.
		res, ferr := r.failNodeProvisionWith(ctx, np, err.Error(), func(st *mlv1alpha1.NodeProvisionStatus) {
			if st.ProvisionRetryCount > 0 {
				st.ProvisionRetryCount--
			}
		})
		return res, true, ferr
	}
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if nc.Spec.DisableVPN == np.Spec.DisableVPN {
		return ctrl.Result{}, false, nil
	}
	if !nc.Spec.DisableVPN {
		// A spec error the user must fix, not a transient failure: it does not
		// consume the retry budget (failNodeProvisionWith counts one, undone
		// here), like the RemoteCluster worker's mismatch, so fixing the spec
		// later is never blocked by a terminal Failed state.
		res, err := r.failNodeProvisionWith(ctx, np, fmt.Sprintf(
			"spec.disableVPN is set but NodeProvisionNetConfig %q describes a cluster that runs with the VPN; the whole cluster must use the same mode (set spec.disableVPN on the control-plane RemoteCluster, or on the NodeProvisionNetConfig, or unset it here)",
			nc.Name), func(st *mlv1alpha1.NodeProvisionStatus) {
			if st.ProvisionRetryCount > 0 {
				st.ProvisionRetryCount--
			}
		})
		return res, true, err
	}
	logf.FromContext(ctx).Info("Inheriting disableVPN from NodeProvisionNetConfig", "netconfig", nc.Name)
	base := np.DeepCopy()
	np.Spec.DisableVPN = true
	if err := r.Patch(ctx, np, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("inheriting disableVPN from NodeProvisionNetConfig: %w", err)
	}
	return ctrl.Result{Requeue: true}, true, nil
}

// requireNetConfig returns the NodeProvisionNetConfig for this cluster.
// If the bootstrap token is absent or older than joinTokenMaxAge, the controller
// creates a new one directly via the Kubernetes API — no SSH or external
// dependency required.
func (r *NodeProvisionReconciler) requireNetConfig(ctx context.Context, np *mlv1alpha1.NodeProvision) (*mlv1alpha1.NodeProvisionNetConfig, error) {
	log := logf.FromContext(ctx)
	// Read directly from the API server (bypassing the informer cache) rather
	// than via r.List/r.Client. Every field of this object — VPN config,
	// credentials refs, Kubernetes version — is load-bearing for provisioning,
	// checked only once per attempt with no re-validation, so even a brief
	// cache lag (e.g. immediately after this controller restarts, or right
	// after RemoteCluster's sync writes a fresh copy) can surface as a
	// confusing "field X is empty" failure despite the object being fully
	// correct in etcd. This read is infrequent enough that the extra API
	// server round-trip is a non-issue.
	lister := r.netConfigReader()
	nc, err := r.netConfigFor(ctx, np)
	switch {
	case errors.Is(err, errNetConfigNotFound):
		log.Info("No matching NodeProvisionNetConfig found yet; requeueing", "reason", err.Error())
		return nil, err
	case errors.Is(err, errNetConfigAmbiguous):
		// Needs a user fix; reconcileVPNMode reports it on the NodeProvision status.
		log.Error(err, "Cannot choose a NodeProvisionNetConfig")
		return nil, err
	case err != nil:
		return nil, err
	}

	needsRefresh := nc.Status.ClusterJoinCommand == "" ||
		nc.Status.JoinTokenRefreshedAt == nil ||
		time.Since(nc.Status.JoinTokenRefreshedAt.Time) > joinTokenMaxAge

	if needsRefresh {
		tokenAge := "never"
		if nc.Status.JoinTokenRefreshedAt != nil {
			tokenAge = time.Since(nc.Status.JoinTokenRefreshedAt.Time).Round(time.Minute).String()
		}
		log.Info("Bootstrap token missing or expired — refreshing via local Kubernetes API",
			"tokenAge", tokenAge,
			"maxAge", joinTokenMaxAge.String(),
			"joinCommandPresent", nc.Status.ClusterJoinCommand != "")
		if err := r.refreshLocalJoinToken(ctx, nc); err != nil {
			log.Error(err, "Failed to refresh bootstrap token")
			return nil, fmt.Errorf("refreshing bootstrap token: %w", err)
		}
		// Re-fetch (uncached, for the same read-after-write reason as above) so
		// callers see the updated join command.
		if err := lister.Get(ctx, client.ObjectKeyFromObject(nc), nc); err != nil {
			return nil, fmt.Errorf("re-fetching NodeProvisionNetConfig after token refresh: %w", err)
		}
	} else if nc.Status.JoinTokenRefreshedAt != nil {
		age := time.Since(nc.Status.JoinTokenRefreshedAt.Time).Round(time.Minute)
		remaining := (joinTokenMaxAge - age).Round(time.Minute)
		log.Info("Bootstrap token still valid",
			"age", age.String(),
			"expiresIn", remaining.String())
	}
	return nc, nil
}

// refreshLocalJoinToken creates a new kubeadm bootstrap token Secret in
// kube-system, derives the CA certificate hash from the cluster-info
// ConfigMap, builds a kubeadm join command, and patches it into the
// NodeProvisionNetConfig status.
//
// This runs entirely against the local Kubernetes API — the controller
// already has in-cluster credentials and does not need SSH access to the
// control-plane node.
//
// RBAC: creating the bootstrap-token Secret in kube-system is covered by the
// cluster-wide secrets rule on Reconcile above, and reading cluster-info in
// kube-public by the namespaced configmaps marker on Reconcile (its Role and
// RoleBinding are config/rbac/role.yaml and config/rbac/kube_public_role_binding.yaml;
// deploy/kube-public-rbac.yaml for plain-manifest installs). No marker is
// repeated here.
func (r *NodeProvisionReconciler) refreshLocalJoinToken(ctx context.Context, nc *mlv1alpha1.NodeProvisionNetConfig) error {
	// ── 1. Generate token ID (6 chars) and secret (16 chars) ─────────────────
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	randStr := func(n int) (string, error) {
		b := make([]byte, n)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}
		for i := range b {
			b[i] = charset[int(b[i])%len(charset)]
		}
		return string(b), nil
	}
	tokenID, err := randStr(6)
	if err != nil {
		return fmt.Errorf("generating token ID: %w", err)
	}
	tokenSecret, err := randStr(16)
	if err != nil {
		return fmt.Errorf("generating token secret: %w", err)
	}
	token := tokenID + "." + tokenSecret

	// ── 2. Create the bootstrap-token Secret in kube-system ──────────────────
	expiry := metav1.NewTime(time.Now().Add(24 * time.Hour))
	bootstrapSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bootstrap-token-" + tokenID,
			Namespace: "kube-system",
		},
		Type: "bootstrap.kubernetes.io/token",
		StringData: map[string]string{
			"token-id":                       tokenID,
			"token-secret":                   tokenSecret,
			"usage-bootstrap-authentication": "true",
			"usage-bootstrap-signing":        "true",
			"auth-extra-groups":              "system:bootstrappers:kubeadm:default-node-token",
			"expiration":                     expiry.UTC().Format(time.RFC3339),
		},
	}
	if err := r.Create(ctx, bootstrapSecret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating bootstrap token secret: %w", err)
	}

	// ── 3. Read cluster-info to get API server URL and CA cert ───────────────
	// Read through the uncached API reader: a cached Get would start a
	// cluster-wide ConfigMap informer (needing list/watch on every namespace)
	// when the RBAC only grants a namespaced `get` in kube-public.
	var cmReader client.Reader = r.Client
	if r.APIReader != nil {
		cmReader = r.APIReader
	}
	clusterInfo := &corev1.ConfigMap{}
	if err := cmReader.Get(ctx, types.NamespacedName{
		Name:      "cluster-info",
		Namespace: "kube-public",
	}, clusterInfo); err != nil {
		return fmt.Errorf("reading cluster-info configmap: %w", err)
	}

	kubeconfigYAML, ok := clusterInfo.Data["kubeconfig"]
	if !ok {
		return fmt.Errorf("cluster-info configmap has no 'kubeconfig' key")
	}

	// Minimal struct to extract server + CA from the kubeconfig YAML.
	// sigs.k8s.io/yaml converts YAML→JSON then uses encoding/json, so json tags
	// are required (yaml tags are silently ignored by the JSON decoder).
	var kc struct {
		Clusters []struct {
			Cluster struct {
				Server                   string `json:"server"`
				CertificateAuthorityData string `json:"certificate-authority-data"`
			} `json:"cluster"`
		} `json:"clusters"`
	}
	if err := yaml.Unmarshal([]byte(kubeconfigYAML), &kc); err != nil {
		return fmt.Errorf("parsing cluster-info kubeconfig: %w", err)
	}
	if len(kc.Clusters) == 0 {
		return fmt.Errorf("cluster-info kubeconfig contains no clusters")
	}
	apiServer := kc.Clusters[0].Cluster.Server
	caData, err := base64.StdEncoding.DecodeString(kc.Clusters[0].Cluster.CertificateAuthorityData)
	if err != nil {
		return fmt.Errorf("decoding CA cert from cluster-info: %w", err)
	}

	// ── 4. Compute SHA256 of the DER-encoded CA certificate ──────────────────
	block, _ := pem.Decode(caData)
	if block == nil {
		return fmt.Errorf("CA data in cluster-info is not PEM-encoded")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parsing CA certificate: %w", err)
	}
	caHash := fmt.Sprintf("sha256:%x", sha256.Sum256(cert.RawSubjectPublicKeyInfo))

	// ── 5. Build join command and update NetConfig status ────────────────────
	joinCmd := fmt.Sprintf("kubeadm join %s --token %s --discovery-token-ca-cert-hash %s",
		strings.TrimPrefix(strings.TrimPrefix(apiServer, "https://"), "http://"),
		token,
		caHash,
	)

	now := metav1.Now()
	patch := nc.DeepCopy()
	patch.Status.ClusterJoinCommand = joinCmd
	patch.Status.JoinTokenRefreshedAt = &now
	if err := r.Status().Patch(ctx, patch, client.MergeFrom(nc)); err != nil {
		return fmt.Errorf("patching NodeProvisionNetConfig with new join command: %w", err)
	}

	logf.FromContext(ctx).Info("Refreshed bootstrap token", "tokenID", tokenID, "apiServer", apiServer)
	return nil
}

func (r *NodeProvisionReconciler) getSecret(ctx context.Context, np *mlv1alpha1.NodeProvision) (*corev1.Secret, error) {
	// Always the NodeProvision's own namespace (see credentialsNamespace); the
	// same rule applies to every credentials lookup and to the controller-owned
	// copy, which is created in np.Namespace.
	ns, err := credentialsNamespace(np)
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      np.Spec.CredentialsRef.Name,
		Namespace: ns,
	}, secret); err != nil {
		return nil, fmt.Errorf("getting credentials secret: %w", err)
	}
	return secret, nil
}

// resolveAWSCreds returns AWS credentials for the given secret via the
// CredentialManager, which maintains an in-memory cache and refreshes
// STS/MFA-backed sessions in the background before they expire.
func (r *NodeProvisionReconciler) resolveAWSCreds(
	ctx context.Context,
	region string,
	secret *corev1.Secret,
) (awsprovision.AWSCredentials, error) {
	if r.CredMgr == nil {
		return awsprovision.AWSCredentials{}, fmt.Errorf("AWS credential manager not initialized")
	}
	return r.CredMgr.Get(ctx, secret.Namespace, secret.Name, region)
}

// getControllerCredsSecret retrieves the controller-owned copy of the credentials
// secret (name = <np.Name> + controllerCredsSuffix).  This copy is created and
// kept up-to-date by ensureControllerCredsSecret so that teardown can proceed
// even when the user-managed secret has been deleted.
func (r *NodeProvisionReconciler) getControllerCredsSecret(ctx context.Context, np *mlv1alpha1.NodeProvision) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      np.Name + controllerCredsSuffix,
		Namespace: np.Namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("getting controller credential copy: %w", err)
	}
	return secret, nil
}

// ensureControllerCredsSecret creates (or updates) a controller-owned copy of
// the user-supplied credentials secret.  The copy carries an owner reference to
// the NodeProvision CR so Kubernetes will GC it once the CR is fully deleted.
// As long as the NodeProvision exists (even in terminating state with our
// finalizer present), the copy is available for EC2 termination.
func (r *NodeProvisionReconciler) ensureControllerCredsSecret(ctx context.Context, np *mlv1alpha1.NodeProvision, userSecret *corev1.Secret) error {
	trueVal := true
	copyName := np.Name + controllerCredsSuffix
	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Name: copyName, Namespace: np.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		desired := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      copyName,
				Namespace: np.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         mlv1alpha1.GroupVersion.String(),
						Kind:               "NodeProvision",
						Name:               np.Name,
						UID:                np.UID,
						Controller:         &trueVal,
						BlockOwnerDeletion: &trueVal,
					},
				},
				Labels: map[string]string{
					nodeProvisionNameLabel: np.Name,
					nodeProvisionUIDLabel:  string(np.UID),
				},
			},
			Type: userSecret.Type,
			Data: userSecret.Data,
		}
		return r.Create(ctx, desired)
	} else if err != nil {
		return fmt.Errorf("checking for controller credential copy: %w", err)
	}
	// Already exists — sync data in case the user rotated the source secret.
	patch := client.MergeFrom(existing.DeepCopy())
	existing.Data = userSecret.Data
	existing.Type = userSecret.Type
	return r.Patch(ctx, existing, patch)
}

// ensureRegistryCredsSecret creates (or updates) a controller-owned copy of
// the image-pull registry credentials referenced by the NodeProvisionNetConfig.
// The copy is named <np.Name>-registry-creds and carries an owner reference to
// the NodeProvision so it is GC'd when the CR is deleted.  The image pre-pull
// path falls back to this copy when the original user-managed secret is gone.
func (r *NodeProvisionReconciler) ensureRegistryCredsSecret(ctx context.Context, np *mlv1alpha1.NodeProvision, registrySecret *corev1.Secret) error {
	trueVal := true
	copyName := np.Name + registryCredsSuffix
	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Name: copyName, Namespace: np.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		desired := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      copyName,
				Namespace: np.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         mlv1alpha1.GroupVersion.String(),
						Kind:               "NodeProvision",
						Name:               np.Name,
						UID:                np.UID,
						Controller:         &trueVal,
						BlockOwnerDeletion: &trueVal,
					},
				},
				Labels: map[string]string{
					nodeProvisionNameLabel: np.Name,
					nodeProvisionUIDLabel:  string(np.UID),
				},
			},
			Type: registrySecret.Type,
			Data: registrySecret.Data,
		}
		return r.Create(ctx, desired)
	} else if err != nil {
		return fmt.Errorf("checking for registry credential copy: %w", err)
	}
	// Already exists — sync data in case credentials were rotated.
	patch := client.MergeFrom(existing.DeepCopy())
	existing.Data = registrySecret.Data
	existing.Type = registrySecret.Type
	return r.Patch(ctx, existing, patch)
}

// getSSHClient creates an SSH client for the node being provisioned.
func (r *NodeProvisionReconciler) getSSHClient(ctx context.Context, np *mlv1alpha1.NodeProvision) (*ssh.Client, error) {
	secret, err := r.getSecret(ctx, np)
	if err != nil {
		return nil, fmt.Errorf("fetching SSH credential secret %q: %w", np.Spec.CredentialsRef.Name, err)
	}

	credBytes, err := resolveSecretKey(secret, np.Spec.CredentialsRef.Key)
	if err != nil {
		return nil, err
	}

	host := np.Spec.IPAddress
	if host == "" {
		host = np.Spec.Hostname
	}
	user := np.Spec.SSHUsernameOverride
	if user == "" {
		user = "ubuntu"
	}
	return dialSSH(host, np.Spec.SSHPort, user, string(credBytes))
}

// getVPNServerSSHClient creates an SSH client to the WireGuard VPN server.
func (r *NodeProvisionReconciler) getVPNServerSSHClient(ctx context.Context, netConfig *mlv1alpha1.NodeProvisionNetConfig) (*ssh.Client, error) {
	ref := netConfig.Spec.VPNServerPublicConfig.VPNSSHCredentialsRef
	if ref.Name == "" {
		return nil, fmt.Errorf("vpnSshCredentialsRef.name is empty in NodeProvisionNetConfig")
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      ref.Name,
		Namespace: ref.NameSpace,
	}, secret); err != nil {
		return nil, fmt.Errorf("fetching VPN server SSH secret %q: %w", ref.Name, err)
	}

	credBytes, err := resolveSecretKey(secret, ref.Key)
	if err != nil {
		return nil, err
	}

	cfg := netConfig.Spec.VPNServerPublicConfig
	username := cfg.SSHUsername
	if username == "" {
		username = "ubuntu"
	}
	port := parsePort(cfg.SSHPort, 22)
	return dialSSH(cfg.PublicIP, port, username, string(credBytes))
}

// updateNetConfigStatus records the newly allocated VPN IP and WireGuard peer.
// Idempotent: duplicate entries are skipped.
// Retries on conflict so a concurrent SSH-based update (from the RemoteCluster
// controller) never causes the IP to be silently dropped.
func (r *NodeProvisionReconciler) updateNetConfigStatus(
	ctx context.Context,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
	vpnIP, publicKey, nodeName string,
) error {
	key := types.NamespacedName{Name: netConfig.Name, Namespace: netConfig.Namespace}
	for attempt := 0; attempt < 5; attempt++ {
		// Re-fetch on every attempt so we always patch against the latest version.
		if err := r.Get(ctx, key, netConfig); err != nil {
			return fmt.Errorf("fetching NetConfig for IP record: %w", err)
		}
		base := netConfig.DeepCopy()

		ipExists := false
		for _, ip := range netConfig.Status.UsedIPAddresses {
			if ip == vpnIP {
				ipExists = true
				break
			}
		}
		if !ipExists {
			netConfig.Status.UsedIPAddresses = append(netConfig.Status.UsedIPAddresses, vpnIP)
		}

		peerExists := false
		for _, p := range netConfig.Status.VPNPeers {
			if p.PublicKey == publicKey || p.VPNIP == vpnIP {
				peerExists = true
				break
			}
		}
		if !peerExists {
			netConfig.Status.VPNPeers = append(netConfig.Status.VPNPeers, mlv1alpha1.VPNPeerStatus{
				NodeName:  nodeName,
				PublicKey: publicKey,
				VPNIP:     vpnIP,
			})
		}

		if ipExists && peerExists {
			return nil // nothing to write
		}

		// Patch only the changed fields — won't clobber ClusterJoinCommand,
		// Kubeconfig, or IPs added by a concurrent SSH-based update.
		if err := r.Status().Patch(ctx, netConfig, client.MergeFrom(base)); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("patching NetConfig IP/peer record: %w", err)
		}
		return nil
	}
	return fmt.Errorf("updating NetConfig IP record: too many conflicts")
}

// setPhaseStatus updates the in-memory phase, message, and progress fields.
// Callers must call r.Status().Update to persist.
func (r *NodeProvisionReconciler) setPhaseStatus(np *mlv1alpha1.NodeProvision, phase mlv1alpha1.NodeProvisionPhase, msg string, progress int) {
	now := metav1.Now()
	np.Status.Phase = phase
	np.Status.Message = msg
	np.Status.Progress = progress
	np.Status.LastUpdated = &now
}

// failNodeProvision re-fetches the NodeProvision, increments ProvisionRetryCount,
// transitions to Failed, and persists the status via RetryOnConflict.
// Once the retry limit is reached the resource is left in a terminal Failed
// state with no RequeueAfter — manual intervention (patch .status.provisionRetryCount
// to 0) is required.
//
// msg is redacted (tokens, private keys, passwords) and truncated before it is
// written to Status.Message or logged. A failed status write is returned as an
// error (the work queue requeues) rather than swallowed.
func (r *NodeProvisionReconciler) failNodeProvision(ctx context.Context, np *mlv1alpha1.NodeProvision, msg string) (ctrl.Result, error) {
	return r.failNodeProvisionWith(ctx, np, msg, nil)
}

// failNodeProvisionWith is failNodeProvision plus an optional mutation applied
// to the status in the same write as the Failed transition — used to persist a
// VPN allocation together with the failure so the peer can be released.
func (r *NodeProvisionReconciler) failNodeProvisionWith(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	msg string,
	mutate func(*mlv1alpha1.NodeProvisionStatus),
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	msg = sanitizeStatusMessage(msg)
	var terminal bool
	var attempts int

	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return err
		}
		fresh.Status.ProvisionRetryCount++
		attempts = fresh.Status.ProvisionRetryCount
		now := metav1.Now()
		fresh.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		fresh.Status.LastUpdated = &now
		if mutate != nil {
			mutate(&fresh.Status)
		}
		if fresh.Status.ProvisionRetryCount >= maxProvisionRetries {
			terminal = true
			fresh.Status.Message = sanitizeStatusMessage(fmt.Sprintf(
				"provisioning failed after %d attempts (last error: %s) — manual intervention required",
				fresh.Status.ProvisionRetryCount, msg))
		} else {
			fresh.Status.Message = sanitizeStatusMessage(fmt.Sprintf("provisioning failed (attempt %d/%d): %s",
				fresh.Status.ProvisionRetryCount, maxProvisionRetries, msg))
		}
		return r.Status().Update(ctx, fresh)
	})
	if updateErr != nil {
		if apierrors.IsNotFound(updateErr) {
			return ctrl.Result{}, nil // the NodeProvision is gone; nothing to record
		}
		// Not durable: surface it so the caller keeps whatever it would drop
		// after a persisted failure (on-prem job, allocation) and the work
		// queue retries with backoff.
		log.Error(updateErr, "failed to persist provisioning failure status", "reason", msg)
		return ctrl.Result{}, fmt.Errorf("persisting provisioning failure: %w", updateErr)
	}

	if terminal {
		log.Error(fmt.Errorf("%s", msg), "provisioning failed — retry limit reached, no further retries",
			"attempts", attempts,
			"maxRetries", maxProvisionRetries)
		return ctrl.Result{}, nil
	}
	log.Error(fmt.Errorf("%s", msg), "provisioning failed — will retry",
		"attempt", attempts,
		"maxRetries", maxProvisionRetries)
	return ctrl.Result{RequeueAfter: requeueFailed}, nil
}

// resolveSecretKey returns the credential bytes from a secret.
func resolveSecretKey(secret *corev1.Secret, key string) ([]byte, error) {
	if key != "" {
		if v, ok := secret.Data[key]; ok {
			return v, nil
		}
		return nil, fmt.Errorf("key %q not found in secret %q", key, secret.Name)
	}
	for _, k := range []string{"privateKey", "id_rsa", "ssh-privatekey", "password", "key"} {
		if v, ok := secret.Data[k]; ok {
			return v, nil
		}
	}
	return nil, fmt.Errorf("no usable credential key found in secret %q (tried: privateKey, id_rsa, ssh-privatekey, password, key)", secret.Name)
}

// dialSSH auto-detects the auth method from the credential value.
func dialSSH(host string, port int, user, credential string) (*ssh.Client, error) {
	if strings.HasPrefix(strings.TrimSpace(credential), "-----BEGIN") {
		return ssh.ConnectWithPrivateKey(host, port, user, credential)
	}
	return ssh.Connect(host, port, user, credential)
}

func ensureFinalizer(np *mlv1alpha1.NodeProvision, finalizer string) bool {
	if !controllerutil.ContainsFinalizer(np, finalizer) {
		controllerutil.AddFinalizer(np, finalizer)
		return true
	}
	return false
}

// resolveCnlabRuntimeConfig builds a pkgruntime.Config from the SoftwareConfig.
// If CnlabRuntime.CredentialsRef is set, it reads the referenced Secret's
// "username" and "token" keys. The token is kept in memory only and never logged.
func (r *NodeProvisionReconciler) resolveCnlabRuntimeConfig(
	ctx context.Context,
	softwareCfg mlv1alpha1.SoftwareConfig,
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

// syncRuntimeCredentials runs oras login on a Ready node whenever the
// registry credentials stored in NodeProvisionNetConfig have changed.
// It is a no-op when no credentials are configured, when the node's stored
// hash already matches, or when SSH access is unavailable (errors are logged
// and the reconcile is requeued so the sync is retried automatically).
func (r *NodeProvisionReconciler) syncRuntimeCredentials(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	netConfig, err := r.requireNetConfig(ctx, np)
	if err != nil {
		// NetConfig absent — nothing to sync yet.
		return ctrl.Result{}, nil
	}

	runtimeCfg, err := r.resolveCnlabRuntimeConfig(ctx, netConfig.Spec.SoftwareConfig, netConfig.Namespace)
	if err != nil {
		log.Error(err, "resolving cnlab-runtime credentials for sync (skipping)")
		return ctrl.Result{}, nil
	}

	if runtimeCfg.Token == "" {
		// Public registry or credentials not configured.
		return ctrl.Result{}, nil
	}

	// Hash username+token — rerun login only when credentials actually change.
	h := sha256.Sum256([]byte(runtimeCfg.Username + ":" + runtimeCfg.Token))
	newHash := fmt.Sprintf("%x", h)
	if np.Status.RuntimeCredentialsHash == newHash {
		log.Info("Runtime registry credentials already in sync on node",
			"registry", runtimeCfg.Registry, "hash", newHash[:12]+"…")
		return ctrl.Result{}, nil
	}
	log.Info("Runtime registry credentials changed — syncing to node via oras login",
		"registry", runtimeCfg.Registry)

	sshClient, err := r.getSSHClientByProvider(ctx, np)
	if err != nil {
		log.Error(err, "opening SSH connection for credential sync (will retry)")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	defer sshClient.Conn.Close() //nolint:errcheck

	// The token goes through a private (0600) temp file and into oras via stdin so
	// it never appears as a command-line argument of oras. Every interpolated
	// value is shell-quoted (registry/username/token come from a Secret and a
	// CR), and a trap removes the file on every exit path, including failure.
	syncCmd := fmt.Sprintf(`set -euo pipefail
umask 077
export HOME="${HOME:-/root}"
tmp="$(mktemp /tmp/.reg-sync.XXXXXX)"
trap 'rm -f "$tmp"' EXIT
printf '%%s' %s > "$tmp"
oras login %s --username %s --password-stdin < "$tmp"`,
		shellQuote(runtimeCfg.Token), shellQuote(runtimeCfg.Registry), shellQuote(runtimeCfg.Username))

	if out, sshErr := ssh.Run(sshClient, syncCmd); sshErr != nil {
		safeOut := redactSecrets(strings.ReplaceAll(out, runtimeCfg.Token, redacted))
		log.Error(fmt.Errorf("%s", redactSecrets(strings.ReplaceAll(sshErr.Error(), runtimeCfg.Token, redacted))),
			"oras login sync failed (will retry)", "output", safeOut)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	np.Status.RuntimeCredentialsHash = newHash
	now := metav1.Now()
	np.Status.LastUpdated = &now
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating RuntimeCredentialsHash: %w", err)
	}

	log.Info("Runtime registry credentials synced to node", "registry", runtimeCfg.Registry)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
// nodeProvisionReconcilePredicate filters the primary NodeProvision watch so
// that this reconciler's OWN status-subresource writes — phase transitions,
// message/progress updates, LastUpdated bumps, which make up the vast
// majority of the writes throughout this file — do not immediately
// re-trigger another reconcile.
//
// Without this, every Status().Update call fires a new watch event and a new
// reconcile is enqueued right away, racing far ahead of the RequeueAfter
// values used everywhere in this file for pacing (e.g. requeueFailed,
// requeueShort). In practice this turned every failure/retry loop into a
// tight burst of several attempts within a few seconds instead of the
// intended ~1-2 minutes apart — hammering the VPN SSH server and the AWS API
// on every single failure instead of backing off.
//
// It still lets through anything that is NOT a pure status-only change, plus
// the two status fields the state machine reacts to (phase and
// provisionRetryCount — see the UpdateFunc):
//   - Generation changes: any .spec edit, including our own
//     resolveAWSDefaults patch.
//   - Finalizer changes: critical — the finalizer-add step early in
//     Reconcile returns ctrl.Result{}, nil with no RequeueAfter and relies
//     entirely on the watch firing again from that same Update event to
//     proceed. A bare predicate.GenerationChangedPredicate{} would leave
//     every new NodeProvision stuck forever right after its finalizer is
//     added, since finalizer changes don't bump generation.
//   - DeletionTimestamp changes: so an external delete request is picked up
//     promptly instead of waiting for the next unrelated status write.
//   - Annotation/label changes: this controller's own annotation writes
//     (the pre-pull retry counter, the node-reset-done marker) therefore wake
//     the reconciler too, right after the write. Handlers must be idempotent
//     under that extra wake-up; reconcileImagePrepullJob, for one, confirms a
//     Job failure against the API server before counting it because the
//     woken reconcile can still see the deleted Job in the informer cache.
func nodeProvisionReconcilePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return true
			}
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}
			if !reflect.DeepEqual(e.ObjectOld.GetFinalizers(), e.ObjectNew.GetFinalizers()) {
				return true
			}
			if !reflect.DeepEqual(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations()) {
				return true
			}
			if !reflect.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) {
				return true
			}
			oldDel := e.ObjectOld.GetDeletionTimestamp()
			newDel := e.ObjectNew.GetDeletionTimestamp()
			if (oldDel == nil) != (newDel == nil) {
				return true
			}
			// Status changes that matter to the state machine: a phase
			// transition, or a change of the retry counter — the latter is how
			// an operator re-arms a terminal Failed resource
			// (patch .status.provisionRetryCount to 0). The Failed handler
			// honours requeueFailed measured from the failure time, so waking
			// on these does not turn into a retry burst.
			if oldNP, ok := e.ObjectOld.(*mlv1alpha1.NodeProvision); ok {
				if newNP, ok := e.ObjectNew.(*mlv1alpha1.NodeProvision); ok {
					if oldNP.Status.Phase != newNP.Status.Phase ||
						oldNP.Status.ProvisionRetryCount != newNP.Status.ProvisionRetryCount {
						return true
					}
				}
			}
			// Other status-only (or no-op) change — skip; RequeueAfter drives pacing instead.
			return false
		},
	}
}

// netConfigChangePredicate limits the NodeProvisionNetConfig watch, which
// re-enqueues every NodeProvision in the namespace, to changes a NodeProvision
// can act on: spec edits (generation) and the join command written to status.
// Peer/IP bookkeeping writes — which the controller itself makes for every
// provisioned node — must not fan out to every NodeProvision.
func netConfigChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNC, ok1 := e.ObjectOld.(*mlv1alpha1.NodeProvisionNetConfig)
			newNC, ok2 := e.ObjectNew.(*mlv1alpha1.NodeProvisionNetConfig)
			if !ok1 || !ok2 {
				return true
			}
			if oldNC.GetGeneration() != newNC.GetGeneration() {
				return true
			}
			return oldNC.Status.ClusterJoinCommand != newNC.Status.ClusterJoinCommand
		},
	}
}

func (r *NodeProvisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Register as a Runnable so Start() receives the manager's root context,
	// which background provisioning goroutines use to stop on graceful shutdown.
	if err := mgr.Add(r); err != nil {
		return fmt.Errorf("registering NodeProvisionReconciler as Runnable: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mlv1alpha1.NodeProvision{}, builder.WithPredicates(nodeProvisionReconcilePredicate())).
		Named("ml-nodeprovision").
		Watches(
			&mlv1alpha1.NodeProvisionNetConfig{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				// Re-reconcile all NodeProvision objects in the same namespace so
				// syncRuntimeCredentials picks up the new credentials.
				npList := &mlv1alpha1.NodeProvisionList{}
				if err := mgr.GetClient().List(ctx, npList, client.InNamespace(obj.GetNamespace())); err != nil {
					return nil
				}
				reqs := make([]reconcile.Request, 0, len(npList.Items))
				ncCluster := ""
				if nc, ok := obj.(*mlv1alpha1.NodeProvisionNetConfig); ok {
					ncCluster = nc.Spec.ClusterName
				}
				for i := range npList.Items {
					// A NodeProvision bound to another cluster is not affected.
					if c := npList.Items[i].Spec.ClusterName; c != "" && ncCluster != "" && c != ncCluster {
						continue
					}
					reqs = append(reqs, reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      npList.Items[i].Name,
							Namespace: npList.Items[i].Namespace,
						},
					})
				}
				return reqs
			}),
			builder.WithPredicates(netConfigChangePredicate()),
		).
		Complete(r)
}

// parsePort converts a string port value to int, returning defaultPort when
// the string is empty or unparseable. Accepts port fields stored as strings in YAML.
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
