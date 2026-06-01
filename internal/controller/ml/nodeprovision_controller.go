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
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
	remotenodeprovision "dcn.ssu.ac.kr/infra/provider/onprem"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	// onPremJobs holds in-flight on-prem provisioning goroutines.
	// Key: "<namespace>/<name>", Value: <-chan onPremJobResult
	onPremJobs sync.Map
}

const (
	nodeProvisionFinalizer = "ml.dcn.ssu.ac.kr/nodeprovision-finalizer"

	// requeueShort is used when waiting for external state (instance running, VPN).
	requeueShort = 30 * time.Second
	// requeueJoining is used while polling for the node to appear in k8s.
	requeueJoining = 15 * time.Second
	// requeueFailed is used to allow manual remediation before retrying.
	requeueFailed = time.Minute
)

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
		// AWS: cloud-init is running on the instance; just wait for the node to appear.
		if np.Spec.Provider == mlv1alpha1.CloudProviderOnPrem {
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

	case mlv1alpha1.NodeProvisionPhaseReady:
		log.Info("NodeProvision is ready, no action needed")
		return ctrl.Result{}, nil

	case mlv1alpha1.NodeProvisionPhaseFailed:
		log.Info("NodeProvision failed; waiting before retry")
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
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("EC2 provisioning failed: %v", err))
	}
	log.Info("EC2 instance created", "instanceId", result.InstanceID)

	// Persist VPN peer in NetConfig before writing InstanceID into NodeProvision
	// status.  If the NodeProvision update fails we can still recover the VPN
	// allocation on the next reconcile via the NetConfig peer list.
	if err := r.updateNetConfigStatus(ctx, netConfig, result.VpnIP, result.PublicKey, name); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvisionNetConfig status: %w", err)
	}

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

	go func() {
		defer sshClient.Conn.Close()
		defer vpnServerClient.Conn.Close()
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
		// Still running.
		log.Info("On-prem bootstrap in progress, requeueing")
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
		log.Info("Node not yet visible in cluster, requeueing", "vpnIP", targetIP)
		now := metav1.Now()
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseRegisteringNode
		np.Status.Message = "Waiting for node to register with control plane"
		np.Status.Progress = 70
		np.Status.LastUpdated = &now
		_ = r.Status().Update(ctx, np)
		return ctrl.Result{RequeueAfter: requeueJoining}, nil
	}

	log.Info("Node registered with control plane", "node", found.Name)
	log.Info("Node join progress: verifying health")

	// Apply hardware-type label if requested.
	if np.Spec.NodeLabel != "" {
		patch := client.MergeFrom(found.DeepCopy())
		if found.Labels == nil {
			found.Labels = map[string]string{}
		}
		found.Labels["hardware-type"] = np.Spec.NodeLabel
		if err := r.Patch(ctx, found, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patching node label: %w", err)
		}
		log.Info("Applied node label", "node", found.Name, "hardware-type", np.Spec.NodeLabel)
	}

	now := metav1.Now()
	np.Status.NodeName = found.Name
	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseReady
	np.Status.Message = "Node successfully joined cluster"
	np.Status.Progress = 100
	np.Status.CompletionTime = &now
	np.Status.LastUpdated = &now
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status to Ready: %w", err)
	}
	log.Info("Node reached Ready state", "node", found.Name)
	return ctrl.Result{}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// handleDelete – deprovisions and removes the finalizer
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) handleDelete(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Deprovisioning node")

	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseDeleting
	now := metav1.Now()
	np.Status.LastUpdated = &now
	_ = r.Status().Update(ctx, np)

	// ── Remove Kubernetes node ──────────────────────────────────────────────
	if np.Status.NodeName != "" {
		node := &corev1.Node{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Status.NodeName}, node); err == nil {
			if err := r.Delete(ctx, node); err != nil {
				log.Error(err, "deleting node from cluster", "node", np.Status.NodeName)
			} else {
				log.Info("Removed node from cluster", "node", np.Status.NodeName)
			}
		}
	}

	// ── Remove VPN peer ─────────────────────────────────────────────────────
	if err := r.cleanupVPNPeer(ctx, np); err != nil {
		log.Error(err, "cleaning up VPN peer (continuing)")
	}

	// ── Provider-specific cleanup ────────────────────────────────────────────
	switch np.Spec.Provider {
	case mlv1alpha1.CloudProviderAWS:
		if np.Status.InstanceID != "" {
			secret, err := r.getSecret(ctx, np)
			if err != nil {
				log.Error(err, "getting credentials for EC2 termination (continuing)")
			} else if err := awsprovision.TerminateInstance(ctx, np, secret, np.Status.InstanceID); err != nil {
				log.Error(err, "terminating EC2 instance", "instanceId", np.Status.InstanceID)
			} else {
				log.Info("EC2 instance terminated", "instanceId", np.Status.InstanceID)
			}
		}
	case mlv1alpha1.CloudProviderOnPrem:
		r.cleanupOnPremNode(ctx, np)
	}

	controllerutil.RemoveFinalizer(np, nodeProvisionFinalizer)
	if err := r.Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	log.Info("Cleanup complete")
	return ctrl.Result{}, nil
}

// cleanupVPNPeer removes the peer from the VPN server and releases the IP
// allocation from the NodeProvisionNetConfig status.
func (r *NodeProvisionReconciler) cleanupVPNPeer(ctx context.Context, np *mlv1alpha1.NodeProvision) error {
	log := logf.FromContext(ctx)

	netConfigList := &mlv1alpha1.NodeProvisionNetConfigList{}
	if err := r.List(ctx, netConfigList); err != nil || len(netConfigList.Items) == 0 {
		return nil
	}
	netConfig := &netConfigList.Items[0]

	// Find the peer entry for this node.
	var peerPublicKey string
	newPeers := make([]mlv1alpha1.VPNPeerStatus, 0, len(netConfig.Status.VPNPeers))
	for _, p := range netConfig.Status.VPNPeers {
		if p.NodeName == np.Name || p.VPNIP == np.Status.VpnIP {
			peerPublicKey = p.PublicKey
		} else {
			newPeers = append(newPeers, p)
		}
	}

	// Remove VPN IP from used list.
	newIPs := make([]string, 0, len(netConfig.Status.UsedIPAddresses))
	for _, ip := range netConfig.Status.UsedIPAddresses {
		if ip != np.Status.VpnIP && ip != np.Status.IPAddress {
			newIPs = append(newIPs, ip)
		}
	}

	netConfig.Status.VPNPeers = newPeers
	netConfig.Status.UsedIPAddresses = newIPs
	if err := r.Status().Update(ctx, netConfig); err != nil {
		return fmt.Errorf("updating NodeProvisionNetConfig status: %w", err)
	}

	// Remove peer from VPN server if we have credentials.
	if peerPublicKey != "" {
		vpnClient, err := r.getVPNServerSSHClient(ctx, netConfig)
		if err != nil {
			log.Error(err, "connecting to VPN server for peer removal")
			return nil
		}
		defer vpnClient.Conn.Close()

		removeCmd := fmt.Sprintf("sudo wg set wg0 peer %s remove", peerPublicKey)
		if _, err := ssh.Run(vpnClient, removeCmd); err != nil {
			log.Error(err, "removing WireGuard peer from server")
		}

		// Remove from persisted wg0.conf.
		vpnIP := np.Status.VpnIP
		if vpnIP == "" {
			vpnIP = np.Status.IPAddress
		}
		cleanCmd := fmt.Sprintf(
			`sudo sed -i '/^\\[Peer\\]/{N;/PublicKey = %s/,/^$/d}' /etc/wireguard/wg0.conf 2>/dev/null || true`,
			peerPublicKey,
		)
		if _, err := ssh.Run(vpnClient, cleanCmd); err != nil {
			log.Error(err, "cleaning wg0.conf on VPN server")
		}
		log.Info("Removed VPN peer", "publicKey", peerPublicKey, "vpnIP", vpnIP)
	}
	return nil
}

// cleanupOnPremNode SSHes into the physical node (best-effort) and reverses
// the provisioning: resets kubeadm, stops WireGuard, and removes its config.
// Failures are logged but never block the finalizer removal.
func (r *NodeProvisionReconciler) cleanupOnPremNode(ctx context.Context, np *mlv1alpha1.NodeProvision) {
	log := logf.FromContext(ctx)

	sshClient, err := r.getSSHClient(ctx, np)
	if err != nil {
		log.Error(err, "SSH to on-prem node failed during cleanup (continuing)")
		return
	}
	defer sshClient.Conn.Close()

	cmds := []string{
		"sudo kubeadm reset -f 2>/dev/null || true",
		"sudo systemctl stop wg-quick@wg0 2>/dev/null || true",
		"sudo systemctl disable wg-quick@wg0 2>/dev/null || true",
		"sudo rm -f /etc/wireguard/wg0.conf",
		"sudo apt-mark unhold kubelet kubeadm kubectl 2>/dev/null || true",
	}
	for _, cmd := range cmds {
		if _, err := ssh.Run(sshClient, cmd); err != nil {
			log.Error(err, "cleanup command failed on on-prem node (continuing)", "cmd", cmd)
		}
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
	return dialSSH(host, np.Spec.SSHPort, np.Spec.SSHUsernameOverride, string(credBytes))
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
func (r *NodeProvisionReconciler) updateNetConfigStatus(
	ctx context.Context,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
	vpnIP, publicKey, nodeName string,
) error {
	updated := false

	ipExists := false
	for _, ip := range netConfig.Status.UsedIPAddresses {
		if ip == vpnIP {
			ipExists = true
			break
		}
	}
	if !ipExists {
		netConfig.Status.UsedIPAddresses = append(netConfig.Status.UsedIPAddresses, vpnIP)
		updated = true
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
		updated = true
	}

	if !updated {
		return nil
	}
	return r.Status().Update(ctx, netConfig)
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
