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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	awsprovision "dcn.ssu.ac.kr/infra/helpers/aws-node-provision"
	remotenodeprovision "dcn.ssu.ac.kr/infra/helpers/remote-node-provision"
	"dcn.ssu.ac.kr/infra/helpers/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeProvisionReconciler reconciles a NodeProvision object
type NodeProvisionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

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
		mlv1alpha1.NodeProvisionPhaseCreatingInstance,
		mlv1alpha1.NodeProvisionPhaseConfiguringVPN,
		mlv1alpha1.NodeProvisionPhaseProvisioning:

		log.Info("Request received, starting provisioning")
		secret, err := r.getSecret(ctx, np)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
		}
		return r.reconcileProvisioning(ctx, np, secret)

	case mlv1alpha1.NodeProvisionPhaseWaitingForInstance:
		secret, err := r.getSecret(ctx, np)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
		}
		return r.reconcileWaitingForInstance(ctx, np, secret)

	case mlv1alpha1.NodeProvisionPhaseJoining,
		mlv1alpha1.NodeProvisionPhaseBootstrapping,
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

	// ── Resolve network config when awsConfig is absent or incomplete ───────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseValidating, "Validating AWS configuration", 5)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		log.Error(sErr, "updating status")
	}
	if err := r.resolveAWSDefaults(ctx, np, secret); err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving AWS defaults: %v", err))
	}

	// ── Validate ────────────────────────────────────────────────────────────
	if err := awsprovision.ValidateAWSConfig(np.Spec); err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("AWS validation failed: %v", err))
	}
	log.Info("AWS validation successful")

	// ── Fetch NodeProvisionNetConfig ────────────────────────────────────────
	netConfig, err := r.requireNetConfig(ctx, np)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// ── Connect to VPN server ───────────────────────────────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseConfiguringVPN, "Configuring VPN client", 15)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		log.Error(sErr, "updating status")
	}

	vpnServerClient, err := r.getVPNServerSSHClient(ctx, netConfig)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("connecting to VPN server: %w", err)
	}
	defer vpnServerClient.Conn.Close()

	// ── Launch EC2 instance with cloud-init ─────────────────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseCreatingInstance, "Creating EC2 instance", 25)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		log.Error(sErr, "updating status")
	}
	log.Info("Creating EC2 instance")

	result, err := awsprovision.ProvisionEC2Node(ctx, np, secret, vpnServerClient, netConfig)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("EC2 provisioning failed: %v", err))
	}

	log.Info("EC2 instance created", "instanceId", result.InstanceID)

	// Persist VPN IP and peer in NetConfig status before updating NodeProvision,
	// so a crash between the two updates doesn't lose the peer registration.
	if err := r.updateNetConfigStatus(ctx, netConfig, result.VpnIP, result.PublicKey, name); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvisionNetConfig status: %w", err)
	}

	// Transition to WaitingForInstance.
	now := metav1.Now()
	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
	np.Status.Message = "Waiting for instance to become running"
	np.Status.InstanceID = result.InstanceID
	np.Status.VpnIP = result.VpnIP
	np.Status.IPAddress = result.VpnIP // kept for backward-compat with reconcileJoining
	np.Status.Progress = 30
	np.Status.LastUpdated = &now
	if np.Status.StartTime == nil {
		np.Status.StartTime = &now
	}
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
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
func (r *NodeProvisionReconciler) resolveAWSDefaults(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) error {
	log := logf.FromContext(ctx)

	needsNetwork := np.Spec.AWSConfig == nil ||
		np.Spec.AWSConfig.SubnetID == "" ||
		len(np.Spec.AWSConfig.SecurityGroupIDs) == 0
	needsAMI := np.Spec.AWSConfig == nil || np.Spec.AWSConfig.AMI == ""
	needsInstanceType := np.Spec.InstanceType == ""

	if !needsNetwork && !needsAMI && !needsInstanceType {
		return nil // nothing to resolve
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
			return fmt.Errorf("spec.instanceType is required: no default instance type defined for nodeLabel %q", np.Spec.NodeLabel)
		}
		np.Spec.InstanceType = it
		log.Info("Resolved instance type from nodeLabel", "nodeLabel", np.Spec.NodeLabel, "instanceType", it)
	}

	// ── AMI: latest Ubuntu 22.04 for the region ───────────────────────────────
	if needsAMI {
		log.Info("Resolving latest Ubuntu 22.04 AMI", "region", np.Spec.Region)
		amiID, err := awsprovision.ResolveUbuntu22AMI(ctx, np.Spec.Region, creds)
		if err != nil {
			return fmt.Errorf("resolving Ubuntu 22.04 AMI: %w", err)
		}
		np.Spec.AWSConfig.AMI = amiID
		log.Info("Resolved AMI", "ami", amiID)
	}

	// ── Network: default VPC / subnet / security group ───────────────────────
	if needsNetwork {
		log.Info("Resolving default network config", "region", np.Spec.Region)
		netCfg, err := awsprovision.ResolveOrCreateNetworkConfig(ctx, np.Spec.Region, creds)
		if err != nil {
			return fmt.Errorf("resolving AWS network config: %w", err)
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

	if err := r.Patch(ctx, np, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patching NodeProvision spec with resolved defaults: %w", err)
	}
	log.Info("Patched NodeProvision spec with resolved AWS defaults")
	return nil
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
// On-Prem provisioning (unchanged logic, extracted for clarity)
// ────────────────────────────────────────────────────────────────────────────

func (r *NodeProvisionReconciler) reconcileOnPremProvisioning(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	sshClient, err := r.getSSHClient(ctx, np)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting SSH client for node: %w", err)
	}
	defer sshClient.Conn.Close()

	netConfig, err := r.requireNetConfig(ctx, np)
	if err != nil {
		log.Info("No NodeProvisionNetConfig ready yet; requeueing")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	vpnServerClient, err := r.getVPNServerSSHClient(ctx, netConfig)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("connecting to VPN server: %w", err)
	}
	defer vpnServerClient.Conn.Close()

	vpnNodeIP, publicKey, err := remotenodeprovision.NewInClusterProvisioner(
		ctx,
		np,
		secret,
		sshClient,
		vpnServerClient,
		netConfig,
	)
	if err != nil {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.Message = err.Error()
		_ = r.Status().Update(ctx, np)
		return ctrl.Result{}, fmt.Errorf("provisioning on-prem node: %w", err)
	}

	if err := r.updateNetConfigStatus(ctx, netConfig, vpnNodeIP, publicKey, np.Name); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvisionNetConfig status: %w", err)
	}

	now := metav1.Now()
	np.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
	np.Status.IPAddress = vpnNodeIP
	np.Status.VpnIP = vpnNodeIP
	np.Status.LastUpdated = &now
	if err := r.Status().Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
	}
	log.Info("On-prem node provisioned, waiting for cluster join", "vpnIP", vpnNodeIP)
	return ctrl.Result{}, nil
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

	// ── Terminate EC2 instance ──────────────────────────────────────────────
	if np.Spec.Provider == mlv1alpha1.CloudProviderAWS && np.Status.InstanceID != "" {
		secret, err := r.getSecret(ctx, np)
		if err != nil {
			log.Error(err, "getting credentials for EC2 termination (continuing)")
		} else {
			if err := awsprovision.TerminateInstance(ctx, np, secret, np.Status.InstanceID); err != nil {
				log.Error(err, "terminating EC2 instance", "instanceId", np.Status.InstanceID)
			}
		}
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
