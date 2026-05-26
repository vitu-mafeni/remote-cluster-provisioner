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
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NodeProvision object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
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
		// GET THE SECRET REFERENCED BY CREDENTIALSREF
		secret := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeProvision.Spec.CredentialsRef.Name, Namespace: nodeProvision.Spec.CredentialsRef.Namespace}, secret); err != nil {
			return ctrl.Result{}, fmt.Errorf("getting credentials secret: %w", err)
		}

		return r.reconcileProvisioning(ctx, nodeProvision, secret)
	case mlv1alpha1.NodeProvisionPhaseReady:
		log.Info("NodeProvision is ready, no action needed")
		return ctrl.Result{}, nil
	case mlv1alpha1.NodeProvisionPhaseFailed:
		// Terminal state: manual intervention required to reset phase.
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	default:
		return ctrl.Result{}, nil
	}

}

// reconcileProvisioning handles the provisioning phase of the NodeProvision lifecycle.
func (r *NodeProvisionReconciler) reconcileProvisioning(ctx context.Context, nodeProvision *mlv1alpha1.NodeProvision, secret *corev1.Secret) (ctrl.Result, error) {
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
			return ctrl.Result{}, fmt.Errorf("getting SSH client: %w", err)
		}

		// get cluster join command from NodeProvisionNetConfig There will only be one NodeProvisionNetConfig per cluster, so we can list and get one only.
		netConfigList := &mlv1alpha1.NodeProvisionNetConfigList{}
		if err := r.List(ctx, netConfigList); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing NodeProvisionNetConfigs: %w", err)
		}
		if len(netConfigList.Items) == 0 {
			return ctrl.Result{}, fmt.Errorf("no NodeProvisionNetConfig found")
		}
		netNodeConfig := &netConfigList.Items[0]
		joinCommand := netNodeConfig.Status.ClusterJoinCommand
		if joinCommand == "" {
			return ctrl.Result{}, fmt.Errorf("cluster join command not found in NodeProvisionNetConfig")
		}

		// ssh to the on-prem node and run provisioning commands (e.g., install kubeadm, join cluster)
		// This is a placeholder implementation; replace with actual provisioning logic.
		// For example, you might run a script on the remote node that performs the necessary setup and joins it to the cluster.

		// Handle on-premises-specific provisioning logic
		err = remotenodeprovision.NewInClusterProvisioner(ctx, nodeProvision, secret, sshClient, netNodeConfig)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("provisioning on-prem node: %w", err)
		}

		nodeProvision.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
		if err := r.Status().Update(ctx, nodeProvision); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
		}
	default:
		return ctrl.Result{}, fmt.Errorf("unsupported cloud provider: %s", nodeProvision.Spec.Provider)
	}

	return ctrl.Result{}, nil

}

func (r *NodeProvisionReconciler) getSSHClient(ctx context.Context, nodeProvision *mlv1alpha1.NodeProvision) (*ssh.Client, error) {
	secretRef := nodeProvision.Spec.CredentialsRef

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretRef.Name,
		Namespace: nodeProvision.Namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("fetching SSH credential secret %q: %w", secretRef.Name, err)
	}

	// Assuming the credential key is stored under a standard key like "password" or "key"
	// Adjust the key name based on your CredentialsRef structure
	var passwordBytes []byte
	var ok bool

	// Try common credential key names
	if passwordBytes, ok = secret.Data["password"]; !ok {
		if passwordBytes, ok = secret.Data["key"]; !ok {
			return nil, fmt.Errorf("credential key not found in secret %q", secretRef.Name)
		}
	}

	var host string
	if nodeProvision.Spec.IPAddress != "" {
		host = nodeProvision.Spec.IPAddress
	} else {
		host = nodeProvision.Spec.Hostname
	}

	sshClient, err := ssh.Connect(host, nodeProvision.Spec.SSHPort, nodeProvision.Spec.SSHUsernameOverride, string(passwordBytes))
	if err != nil {
		return nil, fmt.Errorf("SSH connect to %s:%d: %w", host, nodeProvision.Spec.SSHPort, err)
	}
	return sshClient, nil
}

func (r *NodeProvisionReconciler) handleDelete(ctx context.Context, nodeProvision *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	panic("unimplemented")
}

func ensureFinalizer(nodeProvision *mlv1alpha1.NodeProvision, nodeProvisionFinalizer string) bool {
	panic("unimplemented")
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeProvisionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mlv1alpha1.NodeProvision{}).
		Named("ml-nodeprovision").
		Complete(r)
}
