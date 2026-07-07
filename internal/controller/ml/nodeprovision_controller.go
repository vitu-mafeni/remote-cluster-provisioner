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
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
	remotenodeprovision "dcn.ssu.ac.kr/infra/provider/onprem"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// onPremJobResult carries the outcome of a background on-prem provisioning run.
type onPremJobResult struct {
	vpnIP     string
	publicKey string
	err       error
}

// npPrepullJobResult carries the outcome of a background image pre-pull run.
type npPrepullJobResult struct {
	err error
}

// NodeProvisionReconciler reconciles a NodeProvision object
type NodeProvisionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// onPremJobs holds in-flight on-prem provisioning goroutines.
	// Key: "<namespace>/<name>", Value: <-chan onPremJobResult
	onPremJobs sync.Map
	// onPremProgress tracks the current provisioning step for each in-flight goroutine.
	// Key: "<namespace>/<name>", Value: string
	onPremProgress sync.Map
	// npPrepullJobs holds in-flight image pre-pull goroutines.
	// Key: "<namespace>/<name>", Value: <-chan npPrepullJobResult
	npPrepullJobs sync.Map
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

	// requeueShort is used when waiting for external state (instance running, VPN).
	requeueShort = 30 * time.Second
	// requeueJoining is used while polling for the node to appear in k8s.
	requeueJoining = 15 * time.Second
	// requeueFailed is used to allow manual remediation before retrying.
	requeueFailed = time.Minute
	// npPrepullPollInterval is the requeue interval while images are pre-pulling.
	npPrepullPollInterval = 30 * time.Second
)

// npNodeResetScript is the comprehensive node cleanup script run via SSH during
// deletion.  It mirrors resetNodeViaSSH in the RemoteCluster controller.
const npNodeResetScript = `
if command -v kubeadm >/dev/null 2>&1; then
  sudo kubeadm reset --force 2>/dev/null || true
fi

sudo systemctl stop kubelet crio 2>/dev/null || true
sudo systemctl disable kubelet crio 2>/dev/null || true

sudo umount -l /var/lib/containers/storage/overlay/*/merged 2>/dev/null || true

sudo apt-mark unhold kubelet kubeadm kubectl 2>/dev/null || true
sudo apt-get purge -y kubelet kubeadm kubectl 2>/dev/null || true

sudo apt-get purge -y cri-o criu crun conmon 2>/dev/null || true

sudo apt-get purge -y nvidia-container-toolkit nvidia-container-toolkit-base \
  libnvidia-container-tools libnvidia-container1 2>/dev/null || true
sudo rm -f /etc/apt/sources.list.d/nvidia-container-toolkit.list 2>/dev/null || true
sudo rm -f /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg 2>/dev/null || true

sudo rm -rf /etc/kubernetes /var/lib/kubelet /var/lib/etcd 2>/dev/null || true

sudo rm -rf /var/lib/crio /run/crio /var/lib/containers/storage 2>/dev/null || true
sudo rm -rf /etc/crio 2>/dev/null || true

sudo rm -rf /etc/criu 2>/dev/null || true

sudo rm -f  /etc/modules-load.d/k8s.conf /etc/sysctl.d/k8s.conf 2>/dev/null || true
sudo rm -f  /etc/apt/sources.list.d/kubernetes.list /etc/apt/sources.list.d/cri-o.list 2>/dev/null || true
sudo rm -f  /etc/apt/keyrings/kubernetes-apt-keyring.gpg /etc/apt/keyrings/cri-o-apt-keyring.gpg 2>/dev/null || true

sudo rm -f /usr/local/bin/crictl /usr/bin/crictl 2>/dev/null || true
sudo rm -f /usr/local/bin/crun   /usr/bin/crun   2>/dev/null || true
sudo rm -f /usr/sbin/runc /usr/local/sbin/runc   2>/dev/null || true
sudo rm -f /usr/sbin/criu                         2>/dev/null || true
sudo rm -f /usr/bin/crio                          2>/dev/null || true
sudo rm -f /usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || true

sudo rm -f /var/lib/node-bootstrap-complete 2>/dev/null || true

sudo apt-get autoremove -y 2>/dev/null || true

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

// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions/finalizers,verbs=update
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisionnetconfigs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisionnetconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch;update;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

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
		log.Info("Instance creation in progress, requeueing")
		return ctrl.Result{RequeueAfter: requeueShort}, nil

	case mlv1alpha1.NodeProvisionPhaseWaitingForInstance:
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
			key := np.Namespace + "/" + np.Name
			if _, running := r.onPremJobs.Load(key); running {
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
		log.V(1).Info("NodeProvision is ready, no action needed")
		return ctrl.Result{}, nil

	case mlv1alpha1.NodeProvisionPhaseFailed:
		if err := r.cleanupVPNPeer(ctx, np); err != nil {
			log.Error(err, "releasing stale VPN peer before retry (continuing)")
		}
		// Re-fetch to get the latest ResourceVersion before writing — a parallel
		// reconcile may have already reset the phase, in which case we do nothing.
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return ctrl.Result{}, err
		}
		if fresh.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
			return ctrl.Result{}, nil
		}
		now := metav1.Now()
		fresh.Status.Phase = ""
		fresh.Status.VpnIP = ""
		fresh.Status.IPAddress = ""
		fresh.Status.Message = "Retrying after failure"
		fresh.Status.LastUpdated = &now
		log.Info("Retrying NodeProvision after failure — releasing stale VPN IP and resetting phase")
		if err := r.Status().Update(ctx, fresh); err != nil {
			// Another reconcile won the race — its reset will trigger re-provisioning.
			log.Info("Phase reset race lost, other reconcile already reset", "err", err)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: requeueFailed}, nil

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

	switch np.Spec.Provider {
	case mlv1alpha1.CloudProviderAWS:
		return r.reconcileAWSProvisioning(ctx, np, secret)

	case mlv1alpha1.CloudProviderOnPrem:
		return r.reconcileOnPremProvisioning(ctx, np, secret)

	case mlv1alpha1.CloudProviderGCP:
		log.Info("GCP provisioning not yet implemented")
		return ctrl.Result{}, fmt.Errorf("GCP provider not yet implemented")

	case mlv1alpha1.CloudProviderAzure:
		log.Info("Azure provisioning not yet implemented")
		return ctrl.Result{}, fmt.Errorf("Azure provider not yet implemented")

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

	// ── Connect to VPN server ───────────────────────────────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseConfiguringVPN, "Configuring VPN client", 15)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
	}

	vpnServerClient, err := r.getVPNServerSSHClient(ctx, netConfig)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("connecting to VPN server: %w", err)
	}
	defer vpnServerClient.Conn.Close()

	// ── Launch EC2 instance with cloud-init ─────────────────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseCreatingInstance, "Creating EC2 instance", 25)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
	}
	log.Info("Creating EC2 instance")

	result, err := awsprovision.ProvisionEC2Node(ctx, np, secret, vpnServerClient, netConfig)
	// Always persist VPN allocation immediately — even on EC2 failure — so that
	// cleanupVPNPeer can find and release the peer on the next retry instead of
	// leaving it as an orphan and allocating yet another IP.
	if result != nil && result.VpnIP != "" {
		if uErr := r.updateNetConfigStatus(ctx, netConfig, result.VpnIP, result.PublicKey, name); uErr != nil {
			log.Error(uErr, "persisting VPN allocation to NetConfig (non-fatal)")
		}
		np.Status.VpnIP = result.VpnIP
		np.Status.IPAddress = result.VpnIP
	}
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("EC2 provisioning failed: %v", err))
	}
	log.Info("EC2 instance created", "instanceId", result.InstanceID)

	// Re-fetch to ensure we have the latest ResourceVersion before writing
	// the critical InstanceID field.  This is necessary because the
	// setPhaseStatus+Update calls above may have been skipped (conflict) and
	// we fell back to r.Get, which updated np in place.
	if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetching NodeProvision before status update: %w", err)
	}

	now := metav1.Now()
	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
	np.Status.Message = "Waiting for instance to become running"
	np.Status.InstanceID = result.InstanceID
	np.Status.VpnIP = result.VpnIP
	np.Status.IPAddress = result.VpnIP
	np.Status.Progress = 30
	np.Status.LastUpdated = &now
	if np.Status.StartTime == nil {
		np.Status.StartTime = &now
	}
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status with InstanceID: %w", err)
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
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: np.Namespace,
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

	creds := awsprovision.ResolveAWSCredentials(secret)
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
		netCfg, err := awsprovision.ResolveOrCreateNetworkConfig(ctx, np.Spec.Region, creds)
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

	privateIP, publicIP, err := awsprovision.WaitForInstanceRunning(ctx, np, secret, np.Status.InstanceID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("polling instance state: %w", err)
	}
	if privateIP == "" {
		log.Info("Instance not yet running, requeueing", "instanceId", np.Status.InstanceID)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	now := metav1.Now()
	np.Status.PrivateIP = privateIP
	np.Status.PublicIP = publicIP
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
	key := np.Namespace + "/" + np.Name

	// If a goroutine is already running for this node, just requeue to poll it.
	if _, running := r.onPremJobs.Load(key); running {
		log.Info("On-prem bootstrap goroutine already running, requeueing")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// ── Phase: Validating ────────────────────────────────────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseValidating, "Validating SSH connectivity", 5)
	if err := r.Status().Update(ctx, np); err != nil {
		if ferr := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); ferr != nil {
			return ctrl.Result{}, ferr
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
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseConfiguringVPN, "Configuring WireGuard VPN", 15)
	if err := r.Status().Update(ctx, np); err != nil {
		if ferr := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); ferr != nil {
			sshClient.Conn.Close()
			return ctrl.Result{}, ferr
		}
	}

	vpnServerClient, err := r.getVPNServerSSHClient(ctx, netConfig)
	if err != nil {
		sshClient.Conn.Close()
		return r.failNodeProvision(ctx, np, fmt.Sprintf("connecting to VPN server: %v", err))
	}

	// ── Phase: Bootstrapping — launch background goroutine ───────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseBootstrapping, "Installing packages and joining cluster (background)", 25)
	if err := r.Status().Update(ctx, np); err != nil {
		if ferr := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); ferr != nil {
			sshClient.Conn.Close()
			vpnServerClient.Conn.Close()
			return ctrl.Result{}, ferr
		}
	}

	// Snapshot values needed by the goroutine before returning.
	npCopy := np.DeepCopy()
	secretCopy := secret.DeepCopy()
	netConfigCopy := netConfig.DeepCopy()

	ch := make(chan onPremJobResult, 1)
	r.onPremJobs.Store(key, (<-chan onPremJobResult)(ch))

	reportStep := func(step string) { r.onPremProgress.Store(key, step) }

	go func() {
		defer sshClient.Conn.Close()
		defer vpnServerClient.Conn.Close()
		defer r.onPremProgress.Delete(key)
		// Do NOT delete from onPremJobs here. The map entry must remain
		// until pollOnPremBootstrap reads the result from the channel.
		// Deleting here creates a window where the goroutine has finished
		// but the result hasn't been consumed yet: the next poll would see
		// no map entry, no VpnIP in status, and falsely restart provisioning.

		vpnNodeIP, publicKey, err := remotenodeprovision.NewInClusterProvisioner(
			context.Background(),
			npCopy,
			secretCopy,
			sshClient,
			vpnServerClient,
			netConfigCopy,
			reportStep,
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
	key := np.Namespace + "/" + np.Name

	// If VpnIP is already persisted the goroutine completed in a prior reconcile
	// (or the controller restarted after completion). Advance to Joining.
	if np.Status.VpnIP != "" {
		return r.reconcileJoining(ctx, np)
	}

	v, running := r.onPremJobs.Load(key)
	if !running {
		// No goroutine in memory — controller likely restarted mid-provisioning.
		// Re-enter provisioning to restart the SSH session.
		// secret is guaranteed non-nil here: the Bootstrapping case only passes
		// nil when it has already confirmed the goroutine is running.
		log.Info("No in-flight bootstrap goroutine found (possible restart), restarting provisioning")
		return r.reconcileOnPremProvisioning(ctx, np, secret)
	}

	ch := v.(<-chan onPremJobResult)
	select {
	case res := <-ch:
		// Goroutine finished — consume the result and remove the map entry.
		// The entry is only removed here (not in the goroutine) so there is
		// no window between "goroutine done" and "result consumed" that would
		// cause the next poll to falsely treat this as a controller restart.
		r.onPremJobs.Delete(key)
		if res.err != nil {
			return r.failNodeProvision(ctx, np, fmt.Sprintf("on-prem provisioning failed: %v", res.err))
		}
		log.Info("On-prem bootstrap completed", "vpnIP", res.vpnIP)

		netConfig, err := r.requireNetConfig(ctx, np)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting NetConfig after bootstrap: %w", err)
		}
		if err := r.updateNetConfigStatus(ctx, netConfig, res.vpnIP, res.publicKey, np.Name); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating NodeProvisionNetConfig status: %w", err)
		}

		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
		now := metav1.Now()
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
		np.Status.Message = "Node bootstrapped; waiting for cluster registration"
		np.Status.IPAddress = res.vpnIP
		np.Status.VpnIP = res.vpnIP
		np.Status.Progress = 60
		np.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, np); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
		}
		log.Info("On-prem node provisioned, waiting for cluster join", "vpnIP", res.vpnIP)
		return ctrl.Result{RequeueAfter: requeueJoining}, nil

	default:
		// Still running — surface the current step in status so the user can see progress.
		step := "bootstrapping node (packages and cluster join in progress)"
		if v, ok := r.onPremProgress.Load(key); ok {
			step = v.(string)
		}
		msg := fmt.Sprintf("Bootstrapping: %s", step)
		if np.Status.Message != msg {
			now := metav1.Now()
			np.Status.Message = msg
			np.Status.LastUpdated = &now
			_ = r.Status().Update(ctx, np)
		}
		log.Info("On-prem bootstrap in progress", "step", step)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
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
		now := metav1.Now()
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseRegisteringNode
		np.Status.Message = fmt.Sprintf("Waiting for node to register with control plane (VPN IP: %s)", targetIP)
		np.Status.Progress = 70
		np.Status.LastUpdated = &now
		_ = r.Status().Update(ctx, np)
		return ctrl.Result{RequeueAfter: requeueJoining}, nil
	}

	log.Info("Node registered with control plane", "node", found.Name)

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
		// GPU-specific labels and taint — mirrors what RemoteCluster does for
		// GPU workers via kubectl label/taint on the control-plane.
		if strings.Contains(np.Spec.NodeLabel, "gpu") {
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
	if strings.Contains(np.Spec.NodeLabel, "gpu") {
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
	if strings.Contains(np.Spec.NodeLabel, "gpu") {
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
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status to Ready: %w", err)
	}
	log.Info("Node reached Ready state", "node", found.Name)
	return ctrl.Result{}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// reconcileNPImagePrepull – background GPU image pre-pull for NodeProvision
// ────────────────────────────────────────────────────────────────────────────

// reconcileNPImagePrepull manages the background goroutine that pre-pulls GPU
// images on the node via crictl.  It follows the same pattern as
// reconcileOnPremProvisioning / pollOnPremBootstrap:
//
//   - First call: opens a dedicated SSH connection, spawns the goroutine,
//     stores a receive-only result channel in npPrepullJobs, returns RequeueAfter.
//   - Subsequent calls: non-blocking poll of the channel; if done, stamp Ready;
//     if still running, requeue again.
//
// Only the poll branch deletes from npPrepullJobs — not the goroutine — to
// avoid the TOCTOU race described in reconcileOnPremProvisioning.
func (r *NodeProvisionReconciler) reconcileNPImagePrepull(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	key := np.Namespace + "/" + np.Name

	// Poll branch — hoisted to the top so credential lookups and SSH only
	// happen once (when spawning), not on every 30s requeue.
	if v, running := r.npPrepullJobs.Load(key); running {
		ch := v.(<-chan npPrepullJobResult)
		select {
		case res := <-ch:
			r.npPrepullJobs.Delete(key)
			if res.err != nil {
				// Do NOT fail the NodeProvision — the node is already joined.
				// Log the error and let the next reconcile re-spawn for a retry.
				log.Error(res.err, "Image pre-pull attempt failed, will retry on next reconcile",
					"node", np.Status.NodeName)
				return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
			}
			log.Info("All GPU images pre-pulled successfully", "node", np.Status.NodeName)

			if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
				return ctrl.Result{}, fmt.Errorf("refreshing NodeProvision after pre-pull: %w", err)
			}
			now := metav1.Now()
			np.Status.Phase = mlv1alpha1.NodeProvisionPhaseReady
			np.Status.Message = "Node successfully joined cluster"
			np.Status.Progress = 100
			np.Status.CompletionTime = &now
			np.Status.LastUpdated = &now
			if err := r.Status().Update(ctx, np); err != nil {
				return ctrl.Result{}, fmt.Errorf("updating NodeProvision status to Ready: %w", err)
			}
			return ctrl.Result{}, nil
		default:
			log.V(1).Info("GPU image pre-pull in progress, requeueing")
			return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
		}
	}

	// No goroutine in memory — fetch config and spawn one.
	{
		// Fetch the image list from NodeProvisionNetConfig.
		netConfig, err := r.requireNetConfig(ctx, np)
		if err != nil {
			return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
		}
		images := netConfig.Spec.SoftwareConfig.ImagePrepulls
		if len(images) == 0 {
			// Nothing to pull — go straight to Ready.
			if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
				return ctrl.Result{}, err
			}
			now := metav1.Now()
			np.Status.Phase = mlv1alpha1.NodeProvisionPhaseReady
			np.Status.Message = "Node successfully joined cluster"
			np.Status.Progress = 100
			np.Status.CompletionTime = &now
			np.Status.LastUpdated = &now
			return ctrl.Result{}, r.Status().Update(ctx, np)
		}

		// Resolve registry credentials before spawning the goroutine.
		// The controller has API access; the goroutine only has SSH.
		var pullCreds string
		if ref := netConfig.Spec.SoftwareConfig.ImagePullSecretRef; ref != nil {
			credSecret := &corev1.Secret{}
			if err := r.Get(ctx, types.NamespacedName{
				Name:      ref.Name,
				Namespace: np.Namespace,
			}, credSecret); err != nil {
				// Transient — do not fail the NodeProvision.
				log.Error(err, "Cannot fetch image pull secret, will retry", "secret", ref.Name)
				return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
			}
			username := strings.TrimSpace(string(credSecret.Data["username"]))
			password := strings.TrimSpace(string(credSecret.Data["password"]))
			if username == "" || password == "" {
				log.Error(fmt.Errorf("missing keys"), "Image pull secret must have non-empty \"username\" and \"password\" keys — will retry", "secret", ref.Name)
				return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
			}
			pullCreds = username + ":" + password
			log.Info("Using registry credentials for image pre-pull",
				"secret", ref.Name, "user", username)
		}

		// Open a dedicated SSH connection for the goroutine.  Use the provider-
		// aware helper so AWS nodes use the EC2 SSH key secret (<name>-ssh-key)
		// rather than the AWS credentials secret.
		sshClient, err := r.getSSHClientByProvider(ctx, np)
		if err != nil {
			// SSH failure is transient — do not fail the NodeProvision.
			log.Error(err, "Cannot open SSH connection for image pre-pull, will retry")
			return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
		}

		imagesCopy := make([]string, len(images))
		copy(imagesCopy, images)
		credsCopy := pullCreds // immutable string — safe to close over
		glog := log.WithValues("node", np.Status.NodeName)

		ch := make(chan npPrepullJobResult, 1)
		r.npPrepullJobs.Store(key, (<-chan npPrepullJobResult)(ch))

		go func() {
			defer sshClient.Conn.Close()
			// Clear stale registry auth files when no explicit credentials are
			// configured.  Stale docker.io entries cause 401 errors even for
			// public images because the runtime always sends stored credentials.
			if credsCopy == "" {
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

			for _, img := range imagesCopy {
				img = strings.TrimSpace(img)
				if img == "" {
					continue
				}
				glog.Info("Pulling image", "image", img)
				var cmd string
				if credsCopy != "" {
					// timeout prevents an indefinite hang if the TCP connection
					// drops silently (no SSH keepalive through firewalls).
					cmd = fmt.Sprintf("sudo timeout 7200 crictl pull --creds %s %s", credsCopy, img)
				} else {
					cmd = fmt.Sprintf("sudo timeout 7200 crictl pull %s", img)
				}
				output, pullErr := ssh.Run(sshClient, cmd)
				if pullErr != nil {
					ch <- npPrepullJobResult{err: fmt.Errorf("pulling %s: %w\nOutput:\n%s", img, pullErr, output)}
					return
				}
				glog.Info("Pulled image successfully", "image", img)
			}
			ch <- npPrepullJobResult{err: nil}
		}()

		log.Info("GPU image pre-pull goroutine started",
			"node", np.Status.NodeName, "images", len(imagesCopy),
			"authenticated", pullCreds != "")
		return ctrl.Result{RequeueAfter: npPrepullPollInterval}, nil
	}
}

// getSSHClientByProvider opens an SSH connection to the node using the correct
// credentials for each provider: on-prem uses the user credential secret;
// AWS uses the dedicated SSH key secret (<name>-ssh-key) created during EC2
// provisioning.  Both connect via the VPN IP that is reachable in-cluster.
func (r *NodeProvisionReconciler) getSSHClientByProvider(ctx context.Context, np *mlv1alpha1.NodeProvision) (*ssh.Client, error) {
	host := np.Status.VpnIP
	if host == "" {
		host = np.Spec.IPAddress
		if host == "" {
			host = np.Spec.Hostname
		}
	}
	user := np.Spec.SSHUsernameOverride
	if user == "" {
		user = "ubuntu"
	}
	if np.Spec.Provider == mlv1alpha1.CloudProviderAWS {
		sshKeySecret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      np.Name + "-ssh-key",
			Namespace: np.Namespace,
		}, sshKeySecret); err != nil {
			return nil, fmt.Errorf("fetching AWS SSH key secret %q: %w", np.Name+"-ssh-key", err)
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
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      np.Spec.CredentialsRef.Name,
		Namespace: np.Namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("fetching SSH credential secret %q: %w", np.Spec.CredentialsRef.Name, err)
	}

	credBytes, err := resolveSecretKey(secret, np.Spec.CredentialsRef.Key)
	if err != nil {
		return nil, err
	}

	host := np.Status.VpnIP
	if host == "" {
		host = np.Spec.IPAddress
	}
	if host == "" {
		host = np.Spec.Hostname
	}
	user := np.Spec.SSHUsernameOverride
	if user == "" {
		user = "ubuntu"
	}
	return dialSSH(host, np.Spec.SSHPort, user, string(credBytes))
}

// ────────────────────────────────────────────────────────────────────────────
// handleDelete – deprovisions and removes the finalizer
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) handleDelete(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
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

	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseDeleting
	now := metav1.Now()
	np.Status.LastUpdated = &now
	_ = r.Status().Update(ctx, np)

	// ── Remove Kubernetes node ──────────────────────────────────────────────
	nodeDeleted := false
	if np.Status.NodeName != "" {
		node := &corev1.Node{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Status.NodeName}, node); err == nil {
			// Strip our management finalizer first.  This is necessary because if
			// the node already has a DeletionTimestamp (e.g. from a previous
			// controller run that called Delete before crashing, or from a manual
			// `kubectl delete node`), removing the last finalizer causes the API
			// server to GC the node immediately — so the subsequent Delete call
			// would hit "not found".  We handle that below with IgnoreNotFound.
			if controllerutil.ContainsFinalizer(node, nodeProvisionNodeFinalizer) {
				patch := client.MergeFrom(node.DeepCopy())
				controllerutil.RemoveFinalizer(node, nodeProvisionNodeFinalizer)
				if err := r.Patch(ctx, node, patch); err != nil {
					log.Error(err, "removing node finalizer (continuing)", "node", np.Status.NodeName)
				} else {
					log.Info("Removed management finalizer from node", "node", np.Status.NodeName)
				}
			}
			// If DeletionTimestamp is already set the API server will delete the
			// node as soon as all finalizers are gone (handled above).  Calling
			// Delete again is harmless but produces a confusing "not found" log
			// line, so skip it in that case.
			if node.DeletionTimestamp.IsZero() {
				if err := r.Delete(ctx, node); client.IgnoreNotFound(err) != nil {
					log.Error(err, "deleting node from cluster", "node", np.Status.NodeName)
				} else if err == nil {
					log.Info("Removed node from cluster", "node", np.Status.NodeName)
				}
			} else {
				log.Info("Node already terminating — finalizer removal will complete deletion", "node", np.Status.NodeName)
			}
		} else if apierrors.IsNotFound(err) {
			nodeDeleted = true
			log.Info("Node confirmed deleted from cluster", "node", np.Status.NodeName)
		}
	} else {
		nodeDeleted = true
	}

	// ── Wait for node deletion confirmation before cleaning up AWS resources ──
	if !nodeDeleted {
		log.Info("Waiting for node to be deleted from cluster before cleaning up cloud resources")
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	// ── Provider-specific cleanup (must happen before secrets are deleted) ──────
	switch np.Spec.Provider {
	case mlv1alpha1.CloudProviderAWS:
		// SSH reset before instance termination — mirrors resetNodeViaSSH in the
		// RemoteCluster controller.  Best-effort: if SSH fails we still terminate.
		if np.Status.VpnIP != "" {
			if sshClient, sshErr := r.getSSHClientByProvider(ctx, np); sshErr != nil {
				log.Error(sshErr, "Cannot SSH to AWS node for reset (continuing with termination)")
			} else {
				if out, resetErr := ssh.Run(sshClient, npNodeResetScript); resetErr != nil {
					log.Error(resetErr, "AWS node SSH reset incomplete (continuing)", "output", out)
				} else {
					log.Info("AWS node SSH reset complete")
				}
				sshClient.Conn.Close()
			}
		}
		if np.Status.InstanceID != "" {
			// Prefer the user-supplied secret; fall back to the controller-owned copy
			// in case the user deleted their secret before deleting the NodeProvision.
			secret, err := r.getSecret(ctx, np)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					log.Error(err, "getting credentials for EC2 termination, trying controller copy")
				} else {
					log.Info("User credentials secret not found, falling back to controller copy",
						"userSecret", np.Spec.CredentialsRef.Name)
				}
				secret, err = r.getControllerCredsSecret(ctx, np)
				if err != nil {
					log.Error(err, "getting controller credential copy for EC2 termination (skipping)",
						"instanceId", np.Status.InstanceID,
						"hint", "manually terminate this EC2 instance")
				}
			}
			if secret != nil {
				if err := awsprovision.TerminateInstance(ctx, np, secret, np.Status.InstanceID); err != nil {
					log.Error(err, "terminating EC2 instance", "instanceId", np.Status.InstanceID)
				} else {
					log.Info("EC2 instance terminated", "instanceId", np.Status.InstanceID)
				}
			}
		}
	case mlv1alpha1.CloudProviderOnPrem:
		r.cleanupOnPremNode(ctx, np)
	}

	// ── Remove VPN peer ─────────────────────────────────────────────────────
	if err := r.cleanupVPNPeer(ctx, np); err != nil {
		log.Error(err, "cleaning up VPN peer (continuing)")
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

	netConfigList := &mlv1alpha1.NodeProvisionNetConfigList{}
	if err := r.List(ctx, netConfigList); err != nil {
		return fmt.Errorf("listing NodeProvisionNetConfigs for VPN cleanup: %w", err)
	}
	if len(netConfigList.Items) == 0 {
		log.Info("No NodeProvisionNetConfig found — skipping VPN peer cleanup")
		return nil
	}
	netConfig := &netConfigList.Items[0]

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

	// ── 2. Connect to VPN server ─────────────────────────────────────────────
	vpnClient, err := r.getVPNServerSSHClient(ctx, netConfig)
	if err != nil {
		return fmt.Errorf("connecting to VPN server for peer removal: %w", err)
	}
	defer vpnClient.Conn.Close()

	// ── 3. Fallback: look up public key from live server when CR has no record ─
	if peerPublicKey == "" && vpnIP != "" {
		serverPeers, lookupErr := remotenodeprovision.ReadVPNServerPeers(vpnClient)
		if lookupErr != nil {
			log.Error(lookupErr, "reading live VPN peer list for fallback lookup")
		} else if key, ok := serverPeers[vpnIP]; ok {
			peerPublicKey = key
			log.Info("Resolved peer public key from live VPN server (CR status was missing)",
				"vpnIP", vpnIP, "publicKey", peerPublicKey)
		}
	}

	if peerPublicKey == "" {
		if vpnIP != "" {
			log.Info("No peer found in CR status or on VPN server for this node — nothing to remove",
				"vpnIP", vpnIP)
		}
		// Still fall through to release the IP from the NetConfig status below.
	} else {
		// ── 4. Remove from running WireGuard config ───────────────────────────
		removeCmd := fmt.Sprintf("sudo wg set wg0 peer %s remove", peerPublicKey)
		if _, err := ssh.Run(vpnClient, removeCmd); err != nil {
			log.Error(err, "removing WireGuard peer from running config", "publicKey", peerPublicKey)
		}

		// ── 5. Remove block from persisted wg0.conf ──────────────────────────
		cleanCmd := fmt.Sprintf(`
sudo awk -v our_key="%s" '
  /^\[Peer\]/ {
    in_peer=1; buf=$0"\n"; has_key=0; next
  }
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
' /etc/wireguard/wg0.conf 2>/dev/null | sudo tee /etc/wireguard/wg0.conf.tmp > /dev/null &&
sudo mv /etc/wireguard/wg0.conf.tmp /etc/wireguard/wg0.conf 2>/dev/null || true`,
			peerPublicKey,
		)
		if _, err := ssh.Run(vpnClient, cleanCmd); err != nil {
			log.Error(err, "removing peer block from wg0.conf on VPN server")
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

// cleanupOnPremNode SSHes into the physical node (best-effort) and reverses
// the provisioning: resets kubeadm, stops WireGuard, and removes its config.
// Failures are logged but never block the finalizer removal.
func (r *NodeProvisionReconciler) cleanupOnPremNode(ctx context.Context, np *mlv1alpha1.NodeProvision) {
	log := logf.FromContext(ctx)

	sshClient, err := r.getSSHClient(ctx, np)
	if err != nil {
		// Credential secret may have been deleted before the NodeProvision.
		// VPN peer and Kubernetes node are already cleaned up — this is
		// best-effort kubeadm reset on the physical node.
		log.Info("Cannot SSH to on-prem node for kubeadm reset — credential secret missing or node unreachable; skipping node-side cleanup",
			"err", err)
		return
	}
	defer sshClient.Conn.Close()

	if out, err := ssh.Run(sshClient, npNodeResetScript); err != nil {
		log.Error(err, "on-prem node reset script reported errors (continuing)", "output", out)
	}
	log.Info("On-prem node reset complete")
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) requireNetConfig(ctx context.Context, np *mlv1alpha1.NodeProvision) (*mlv1alpha1.NodeProvisionNetConfig, error) {
	log := logf.FromContext(ctx)
	netConfigList := &mlv1alpha1.NodeProvisionNetConfigList{}
	if err := r.List(ctx, netConfigList); err != nil {
		return nil, fmt.Errorf("listing NodeProvisionNetConfigs: %w", err)
	}
	if len(netConfigList.Items) == 0 {
		log.Info("No NodeProvisionNetConfig found yet; requeueing")
		return nil, fmt.Errorf("no NodeProvisionNetConfig")
	}
	nc := &netConfigList.Items[0]
	if nc.Status.ClusterJoinCommand == "" {
		log.Info("Cluster join command not ready yet; requeueing")
		return nil, fmt.Errorf("join command not ready")
	}
	return nc, nil
}

func (r *NodeProvisionReconciler) getSecret(ctx context.Context, np *mlv1alpha1.NodeProvision) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      np.Spec.CredentialsRef.Name,
		Namespace: np.Spec.CredentialsRef.Namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("getting credentials secret: %w", err)
	}
	return secret, nil
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

// getSSHClient creates an SSH client for the node being provisioned.
func (r *NodeProvisionReconciler) getSSHClient(ctx context.Context, np *mlv1alpha1.NodeProvision) (*ssh.Client, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      np.Spec.CredentialsRef.Name,
		Namespace: np.Namespace,
	}, secret); err != nil {
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
	port := cfg.SSHPort
	if port == 0 {
		port = 22
	}
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

// failNodeProvision transitions to Failed and persists the status.
func (r *NodeProvisionReconciler) failNodeProvision(ctx context.Context, np *mlv1alpha1.NodeProvision, msg string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Error(fmt.Errorf("%s", msg), "provisioning failed")
	now := metav1.Now()
	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
	np.Status.Message = msg
	np.Status.LastUpdated = &now
	_ = r.Status().Update(ctx, np)
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

// SetupWithManager sets up the controller with the Manager.
func (r *NodeProvisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mlv1alpha1.NodeProvision{}).
		Named("ml-nodeprovision").
		Complete(r)
}
