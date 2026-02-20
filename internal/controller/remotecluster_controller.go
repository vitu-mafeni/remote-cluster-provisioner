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
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	"dcn.ssu.ac.kr/infra/helpers/provision"
	"dcn.ssu.ac.kr/infra/helpers/ssh"

	corev1 "k8s.io/api/core/v1"
)

// RemoteClusterReconciler reconciles a RemoteCluster object
type RemoteClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=infra.dcn.ssu.ac.kr,resources=remoteclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the RemoteCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *RemoteClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	logger := logf.FromContext(ctx).WithValues("remotecluster", req.NamespacedName)
	logger.Info("Reconciling RemoteCluster")

	cluster := &infrav1.RemoteCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if cluster.Status.Phase == "Ready" {
		return ctrl.Result{}, nil
	}

	// Set Provisioning
	cluster.Status.Phase = "Provisioning"
	_ = r.Status().Update(ctx, cluster)

	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      cluster.Spec.Auth.PasswordSecretRef.Name,
		Namespace: cluster.Namespace,
	}, secret)
	if err != nil {
		return ctrl.Result{}, err
	}

	passwordBytes, ok := secret.Data[cluster.Spec.Auth.PasswordSecretRef.Key]
	if !ok {
		return ctrl.Result{}, fmt.Errorf("key %s not found in secret",
			cluster.Spec.Auth.PasswordSecretRef.Key)
	}
	password := string(passwordBytes)

	client, err := ssh.Connect(cluster.Spec.Host, cluster.Spec.Port, cluster.Spec.User, password)
	if err != nil {
		return r.fail(ctx, cluster, err)
	}
	// defer client.Close()

	err = provision.SingleNode(client, cluster.Spec.Kubernetes.Version)
	if err != nil {
		return r.fail(ctx, cluster, err)
	}

	// Success
	cluster.Status.Phase = "Ready"
	cluster.Status.Message = "Cluster provisioned"
	_ = r.Status().Update(ctx, cluster)

	return ctrl.Result{}, nil
}

func (r *RemoteClusterReconciler) fail(ctx context.Context, c *infrav1.RemoteCluster, err error) (ctrl.Result, error) {
	c.Status.Phase = "Failed"
	c.Status.Message = err.Error()
	_ = r.Status().Update(ctx, c)

	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RemoteClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1.RemoteCluster{}).
		Named("remotecluster").
		Complete(r)
}
