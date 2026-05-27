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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	remotenodeprovision "dcn.ssu.ac.kr/infra/helpers/remote-node-provision"
	"dcn.ssu.ac.kr/infra/helpers/ssh"
	corev1 "k8s.io/api/core/v1"
)

// NodeProvisionReconciler reconciles a NodeProvision object
type NodeProvisionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	nodeProvisionFinalizer = "ml.cnlab.ai/v1alpha1/nodeprovision-finalizer"
	remoteClusterLabelKey  = "ml.cnlab.ai/v1alpha1/nodeprovision"
)

type NodeProvisionPhase string
type CloudProvider string

const (
	NodeProvisionPhasePending      NodeProvisionPhase = "Pending"
	NodeProvisionPhaseProvisioning NodeProvisionPhase = "Provisioning"
	NodeProvisionPhaseJoining      NodeProvisionPhase = "Joining"
	NodeProvisionPhaseReady        NodeProvisionPhase = "Ready"
	NodeProvisionPhaseFailed       NodeProvisionPhase = "Failed"

	CloudProviderAWS    CloudProvider = "AWS"
	CloudProviderGCP    CloudProvider = "GCP"
	CloudProviderAzure  CloudProvider = "Azure"
	CloudProviderOnPrem CloudProvider = "OnPrem"
)

// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ml.dcn.ssu.ac.kr,resources=nodeprovisions/finalizers,verbs=update

func (r *NodeProvisionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling NodeProvision")

	nodeProvision := &mlv1alpha1.NodeProvision{}
	if err := r.Get(ctx, req.NamespacedName, nodeProvision); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log = log.WithValues(
		"nodeProvision", nodeProvision.Name,
		"provider", nodeProvision.Spec.Provider,
		"role", nodeProvision.Spec.Role,
		"phase", nodeProvision.Status.Phase,
	)

	if !nodeProvision.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, nodeProvision)
	}

	if ensureFinalizer(nodeProvision, nodeProvisionFinalizer) {
		if err := r.Update(ctx, nodeProvision); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}

	switch nodeProvision.Status.Phase {
	case "", mlv1alpha1.NodeProvisionPhaseProvisioning:
		secret := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{
			Name:      nodeProvision.Spec.CredentialsRef.Name,
			Namespace: nodeProvision.Spec.CredentialsRef.Namespace,
		}, secret); err != nil {
			return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
		}
		return r.reconcileProvisioning(ctx, nodeProvision, secret)
	case mlv1alpha1.NodeProvisionPhaseReady:
		log.Info("NodeProvision is ready, no action needed")
		return ctrl.Result{}, nil
	case mlv1alpha1.NodeProvisionPhaseFailed:
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	default:
		return ctrl.Result{}, nil
	}
}

// reconcileProvisioning handles the provisioning phase of the NodeProvision lifecycle.
func (r *NodeProvisionReconciler) reconcileProvisioning(ctx context.Context, nodeProvision *mlv1alpha1.NodeProvision, secret *corev1.Secret) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	switch nodeProvision.Spec.Provider {
	case mlv1alpha1.CloudProviderAWS:
		// Handle AWS-specific provisioning logic
	case mlv1alpha1.CloudProviderGCP:
		// Handle GCP-specific provisioning logic
	case mlv1alpha1.CloudProviderAzure:
		// Handle Azure-specific provisioning logic
	case mlv1alpha1.CloudProviderOnPrem:
		sshClient, err := r.getSSHClient(ctx, nodeProvision)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("getting SSH client for node: %w", err)
		}
		defer sshClient.Conn.Close()

		netConfigList := &mlv1alpha1.NodeProvisionNetConfigList{}
		if err := r.List(ctx, netConfigList); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing NodeProvisionNetConfigs: %w", err)
		}
		if len(netConfigList.Items) == 0 {
			log.Info("No NodeProvisionNetConfig found yet; requeueing")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		netNodeConfig := &netConfigList.Items[0]
		if netNodeConfig.Status.ClusterJoinCommand == "" {
			log.Info("Cluster join command not ready yet; requeueing")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		vpnServerClient, err := r.getVPNServerSSHClient(ctx, netNodeConfig)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("connecting to VPN server: %w", err)
		}
		defer vpnServerClient.Conn.Close()

		vpnNodeIP, publicKey, err := remotenodeprovision.NewInClusterProvisioner(
			ctx,
			nodeProvision,
			secret,
			sshClient,
			vpnServerClient,
			netNodeConfig,
		)
		if err != nil {
			nodeProvision.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
			nodeProvision.Status.Message = err.Error()
			_ = r.Status().Update(ctx, nodeProvision)
			return ctrl.Result{}, fmt.Errorf("provisioning on-prem node: %w", err)
		}

		if err := r.updateNetConfigStatus(ctx, netNodeConfig, vpnNodeIP, publicKey, nodeProvision.Name); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating NodeProvisionNetConfig status: %w", err)
		}

		nodeProvision.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
		nodeProvision.Status.IPAddress = vpnNodeIP
		if err := r.Status().Update(ctx, nodeProvision); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
		}

	default:
		return ctrl.Result{}, fmt.Errorf("unsupported cloud provider: %s", nodeProvision.Spec.Provider)
	}

	return ctrl.Result{}, nil
}

// getSSHClient creates an SSH client for the node being provisioned.
// Auth type is auto-detected: PEM private key → key auth; anything else → password auth.
func (r *NodeProvisionReconciler) getSSHClient(ctx context.Context, nodeProvision *mlv1alpha1.NodeProvision) (*ssh.Client, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      nodeProvision.Spec.CredentialsRef.Name,
		Namespace: nodeProvision.Namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("fetching SSH credential secret %q: %w", nodeProvision.Spec.CredentialsRef.Name, err)
	}

	credBytes, err := resolveSecretKey(secret, nodeProvision.Spec.CredentialsRef.Key)
	if err != nil {
		return nil, err
	}

	var host string
	if nodeProvision.Spec.IPAddress != "" {
		host = nodeProvision.Spec.IPAddress
	} else {
		host = nodeProvision.Spec.Hostname
	}

	return dialSSH(host, nodeProvision.Spec.SSHPort, nodeProvision.Spec.SSHUsernameOverride, string(credBytes))
}

// getVPNServerSSHClient creates an SSH client to the WireGuard VPN server using
// credentials from the secret referenced in the NodeProvisionNetConfig.
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

// updateNetConfigStatus records the newly allocated VPN IP and peer in the
// NodeProvisionNetConfig status.  Idempotent: duplicate entries are skipped.
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

// resolveSecretKey returns the credential bytes from a secret.
// If key is specified it is used directly; otherwise well-known field names are tried in order.
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
// A PEM-encoded block (starts with "-----BEGIN") uses private-key auth; everything else uses password auth.
func dialSSH(host string, port int, user, credential string) (*ssh.Client, error) {
	if strings.HasPrefix(strings.TrimSpace(credential), "-----BEGIN") {
		return ssh.ConnectWithPrivateKey(host, port, user, credential)
	}
	return ssh.Connect(host, port, user, credential)
}

func (r *NodeProvisionReconciler) handleDelete(ctx context.Context, nodeProvision *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(nodeProvision, nodeProvisionFinalizer)
	if err := r.Update(ctx, nodeProvision); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

func ensureFinalizer(nodeProvision *mlv1alpha1.NodeProvision, finalizer string) bool {
	if !controllerutil.ContainsFinalizer(nodeProvision, finalizer) {
		controllerutil.AddFinalizer(nodeProvision, finalizer)
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
