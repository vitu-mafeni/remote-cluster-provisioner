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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

const (
	remoteClusterFinalizer = "infra.dcn.ssu.ac.kr/remotecluster-finalizer"
	remoteClusterLabelKey  = "infra.dcn.ssu.ac.kr/remotecluster"
)

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

	if cluster.ObjectMeta.DeletionTimestamp.IsZero() {

		// ADD finalizer if missing
		if !controllerutil.ContainsFinalizer(cluster, remoteClusterFinalizer) {
			controllerutil.AddFinalizer(cluster, remoteClusterFinalizer)
			if err := r.Update(ctx, cluster); err != nil {
				return ctrl.Result{}, err
			}
		}

	} else {
		// BEING DELETED
		return r.handleDelete(ctx, cluster)
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

	err = provision.SingleNode(client, cluster)
	if err != nil {
		return r.fail(ctx, cluster, err)
	}

	// Success
	cluster.Status.Phase = "Ready"
	cluster.Status.Message = "Cluster provisioned"
	_ = r.Status().Update(ctx, cluster)

	err = r.createClusterRepo(ctx, cluster)
	if err != nil {
		return r.fail(ctx, cluster, err)
	}

	// delay 2mins
	logger.Info("Waiting for cluster repo to be ready before creating PackageVariants", "duration", "3m")
	time.Sleep(3 * time.Minute)
	err = r.createPackageVariants(ctx, cluster)
	if err != nil {
		return r.fail(ctx, cluster, err)
	}

	return ctrl.Result{}, nil
}

// create cluster repository on the management cluster if git integration is enabled
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

	/*
		--------------------------------------------------
		Porch Repository (config.porch.kpt.dev)
		--------------------------------------------------
	*/
	porchRepo := &unstructured.Unstructured{}
	porchRepo.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.porch.kpt.dev",
		Version: "v1alpha1",
		Kind:    "Repository",
	})

	porchRepo.SetName(cluster.Spec.ClusterName)
	porchRepo.SetNamespace(cluster.Namespace)
	porchRepo.SetLabels(labels)

	err := r.Get(ctx, client.ObjectKeyFromObject(porchRepo), porchRepo)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if apierrors.IsNotFound(err) {
		porchRepo.Object["spec"] = map[string]interface{}{
			"content":    "Package",
			"deployment": true,
			"git": map[string]interface{}{
				"repo":      cluster.Spec.GitConfig.GitServer + "/" + cluster.Spec.GitConfig.GitUsername + "/" + cluster.Spec.ClusterName + ".git",
				"branch":    "main",
				"directory": "/",
				"secretRef": map[string]interface{}{
					"name": secretRefName,
				},
			},
			"type": "git",
		}

		if err := controllerutil.SetControllerReference(cluster, porchRepo, r.Scheme); err != nil {
			return err
		}

		if err := r.Create(ctx, porchRepo); err != nil {
			return err
		}
	}

	/*
		--------------------------------------------------
		Nephio Repository (infra.nephio.org)
		--------------------------------------------------
	*/
	nephioRepo := &unstructured.Unstructured{}
	nephioRepo.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "Repository",
	})

	nephioRepo.SetName(cluster.Spec.ClusterName)
	nephioRepo.SetNamespace(cluster.Namespace)
	// nephioRepo.SetLabels(labels)

	err = r.Get(ctx, client.ObjectKeyFromObject(nephioRepo), nephioRepo)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if apierrors.IsNotFound(err) {
		nephioRepo.Object["spec"] = map[string]interface{}{
			"description":   "Repository for " + cluster.Spec.ClusterName,
			"defaultBranch": "main",
		}

		// if err := controllerutil.SetControllerReference(cluster, nephioRepo, r.Scheme); err != nil {
		// 	return err
		// }

		if err := r.Create(ctx, nephioRepo); err != nil {
			return err
		}
	}

	/*
		--------------------------------------------------
		Token (infra.nephio.org)
		--------------------------------------------------
	*/
	token := &unstructured.Unstructured{}
	token.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "Token",
	})

	token.SetName(secretRefName)
	token.SetNamespace(cluster.Namespace)
	token.SetLabels(labels)

	err = r.Get(ctx, client.ObjectKeyFromObject(token), token)
	if err == nil {
		return nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	// token.SetAnnotations(map[string]string{
	// 	"nephio.org/gitops":           "configsync",
	// 	"nephio.org/app":              "tobeinstalledonremotecluster",
	// 	"nephio.org/remote-namespace": "config-management-system",
	// })

	token.Object["spec"] = map[string]interface{}{}

	if err := controllerutil.SetControllerReference(cluster, token, r.Scheme); err != nil {
		return err
	}

	if err := r.Create(ctx, token); err != nil {
		return err
	}

	/*
		--------------------------------------------------
		Nephio Token (infra.nephio.org)
		--------------------------------------------------
	*/
	nephioToken := &unstructured.Unstructured{}
	nephioToken.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "Token",
	})

	nephioToken.SetName(cluster.Spec.ClusterName + "-access-token-configsync")
	nephioToken.SetNamespace(cluster.Namespace)
	nephioToken.SetLabels(labels)

	err = r.Get(ctx, client.ObjectKeyFromObject(nephioToken), nephioToken)
	if err == nil {
		return nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	nephioToken.SetAnnotations(map[string]string{
		"nephio.org/gitops":           "configsync",
		"nephio.org/app":              "tobeinstalledonremotecluster",
		"nephio.org/remote-namespace": "config-management-system",
	})

	nephioToken.Object["spec"] = map[string]interface{}{}

	if err := controllerutil.SetControllerReference(cluster, nephioToken, r.Scheme); err != nil {
		return err
	}

	if err := r.Create(ctx, nephioToken); err != nil {
		return err
	}

	return nil
}

// actually delete cluster resources like repo and token when remotecluster is deleted, and also cleanup k8s cluster via SSH kubeadm reset, and any other cleanup needed on the remote node
func (r *RemoteClusterReconciler) handleDelete(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) {

	log := logf.FromContext(ctx)
	log.Info("Cleaning up resources for RemoteCluster", "remotecluster", cluster.Name)

	if err := r.deleteClusterResources(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(cluster, remoteClusterFinalizer)
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// cleanup ssh kubeadm reset via SSH on deletion, Remote node cleanup
	unInstallK8sRemoteCluster()

	return ctrl.Result{}, nil
}

func unInstallK8sRemoteCluster() {
	log := logf.FromContext(context.Background())
	log.Info("Uninstalling Kubernetes on remote cluster via SSH")
}

func (r *RemoteClusterReconciler) deleteClusterResources(ctx context.Context, cluster *infrav1.RemoteCluster) error {

	labels := client.MatchingLabels{
		remoteClusterFinalizer: cluster.Spec.ClusterName,
	}

	// -------------------------
	// Delete Repository
	// -------------------------
	repoList := &unstructured.UnstructuredList{}
	repoList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.porch.kpt.dev",
		Version: "v1alpha1",
		Kind:    "RepositoryList",
	})

	if err := r.List(ctx, repoList, labels, client.InNamespace(cluster.Namespace)); err != nil {
		return err
	}

	for _, repo := range repoList.Items {
		_ = r.Delete(ctx, &repo) // ignore notfound
	}

	// -------------------------
	// Delete Token
	// -------------------------
	tokenList := &unstructured.UnstructuredList{}
	tokenList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "infra.nephio.org",
		Version: "v1alpha1",
		Kind:    "TokenList",
	})

	if err := r.List(ctx, tokenList, labels, client.InNamespace(cluster.Namespace)); err != nil {
		return err
	}

	for _, token := range tokenList.Items {
		_ = r.Delete(ctx, &token)
	}

	return nil
}

// install packagevariants once cluster is ready, and git repo is created, then create packagevariants that deploy ml-platform on the remote cluster
func (r *RemoteClusterReconciler) createPackageVariants(ctx context.Context, clusterRemote *infrav1.RemoteCluster) error {

	variants := []map[string]interface{}{

		{
			"name": "minio-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package": "minio",
					"repo":    "catalog-nephio-optional",
				},
				"downstream": map[string]interface{}{
					"package": "minio",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "enterprise-gateway-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package": "enterprise-gateway",
					"repo":    "catalog-workloads-mlplatform",
				},
				"downstream": map[string]interface{}{
					"package": "enterprise-gateway",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "gpu-operator-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "gpu-operator",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "gpu-operator",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "harbor-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "harbor",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "harbor",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "k8s-dra-driver-gpu-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "k8s-dra-driver-gpu",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "k8s-dra-driver-gpu",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "kai-scheduler-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "kai-scheduler",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "kai-scheduler",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "keycloak-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "keycloak",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "keycloak",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "kubeflow-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "kubeflow",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "kubeflow",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "kueue-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "kueue",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "kueue",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "kyverno-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "kyverno",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "kyverno",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "prometheus-stack-variant",
			"spec": map[string]interface{}{
				"annotations": map[string]interface{}{
					"approval.nephio.org/policy": "initial",
				},
				"upstream": map[string]interface{}{
					"package":  "prometheus-stack",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "prometheus-stack",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
		{
			"name": "nfs-provisioner-variant",
			"spec": map[string]interface{}{
				// "annotations": map[string]interface{}{
				// 	"approval.nephio.org/policy": "initial",
				// },
				"upstream": map[string]interface{}{
					"package":  "nfs-provisioner",
					"repo":     clusterRemote.Spec.GitConfig.UpstreamPlatformRepo,
					"revision": clusterRemote.Spec.GitConfig.PackageRevision,
				},
				"downstream": map[string]interface{}{
					"package": "nfs-provisioner",
					"repo":    clusterRemote.Spec.ClusterName,
				},
			},
		},
	}

	gvk := schema.GroupVersionKind{
		Group:   "config.porch.kpt.dev",
		Version: "v1alpha1",
		Kind:    "PackageVariant",
	}

	for _, v := range variants {

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		obj.SetName(v["name"].(string))
		obj.SetNamespace("default")

		obj.Object["spec"] = v["spec"]

		err := r.Client.Create(ctx, obj)
		if err != nil {

			if apierrors.IsAlreadyExists(err) {

				existing := &unstructured.Unstructured{}
				existing.SetGroupVersionKind(gvk)

				err = r.Client.Get(ctx,
					client.ObjectKey{
						Name:      obj.GetName(),
						Namespace: obj.GetNamespace(),
					},
					existing)
				if err != nil {
					return err
				}

				existing.Object["spec"] = obj.Object["spec"]

				err = r.Client.Update(ctx, existing)
				if err != nil {
					return err
				}

			} else {
				return fmt.Errorf("failed creating PackageVariant %s: %w",
					obj.GetName(), err)
			}
		}
	}

	return nil
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
