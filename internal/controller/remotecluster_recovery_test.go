package controller

import (
	"context"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{infrav1.AddToScheme, mlv1alpha1.AddToScheme, corev1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func newTestReconciler(t *testing.T, objs ...client.Object) *RemoteClusterReconciler {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&infrav1.RemoteCluster{}).
		Build()
	return &RemoteClusterReconciler{Client: c, Scheme: s}
}

func node(name, clusterName, nodeType string) *infrav1.RemoteCluster {
	return &infrav1.RemoteCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID("uid-" + name),
			Finalizers: []string{remoteClusterFinalizer},
		},
		Spec: infrav1.RemoteClusterSpec{
			ClusterName: clusterName,
			NodeInfo:    infrav1.NodeInfo{NodeType: nodeType},
		},
	}
}

func unstr(apiVersion, kind, ns, name string, labels map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(apiVersion)
	u.SetKind(kind)
	u.SetNamespace(ns)
	u.SetName(name)
	if labels != nil {
		u.SetLabels(labels)
	}
	u.Object["spec"] = map[string]interface{}{}
	return u
}

func getRC(t *testing.T, r *RemoteClusterReconciler, name string) *infrav1.RemoteCluster {
	t.Helper()
	got := &infrav1.RemoteCluster{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, got); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return got
}

// --- finding 1 -------------------------------------------------------------

func TestReconcile_ControlPlaneWithJoinCommandRecoversToReady(t *testing.T) {
	for _, phase := range []string{phaseFailed, phaseProvisioning} {
		cp := node("cp", "c1", "control-plane")
		cp.Status.Phase = phase
		cp.Status.JoinCommand = "kubeadm join x"
		cp.Status.ProvisionRetryCount = maxProvisionRetries // even a terminal Failed must recover
		r := newTestReconciler(t, cp)

		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		if err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		if !res.Requeue {
			t.Errorf("%s: expected an immediate requeue so the Ready branch runs", phase)
		}
		got := getRC(t, r, "cp")
		if got.Status.Phase != phaseReady {
			t.Errorf("%s: phase = %q, want Ready", phase, got.Status.Phase)
		}
		if got.Status.ProvisionRetryCount != 0 {
			t.Errorf("%s: retry counter not reset: %d", phase, got.Status.ProvisionRetryCount)
		}
	}
}

func TestReconcile_PostReadyFailureDoesNotDemoteCluster(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Status.Phase = phaseReady
	cp.Status.JoinCommand = "kubeadm join x"
	cp.Spec.VPNConfig.IP = "10.8.0.2"
	cp.Annotations = map[string]string{
		annotationNodeProvisionCreated: "true",
		annotationProvisionedVPNMode:   vpnModeVPN,
		// Refreshed just now so the (SSH-based) token refresh is skipped.
		annotationJoinTokenRefreshedAt: time.Now().UTC().Format(time.RFC3339),
	}
	// The base name AND the per-cluster fallback name of the first PackageVariant
	// are both owned by other clusters: the Porch step cannot proceed.
	const base = "remote-cluster-provisioner-variant"
	foreign := unstr("config.porch.kpt.dev/v1alpha1", "PackageVariant", packageVariantNamespace,
		base, map[string]string{remoteClusterLabelKey: "other"})
	foreign2 := unstr("config.porch.kpt.dev/v1alpha1", "PackageVariant", packageVariantNamespace,
		perClusterPackageVariantName(base, "c1"), map[string]string{remoteClusterLabelKey: "another"})
	r := newTestReconciler(t, cp, foreign, foreign2)

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 {
		t.Error("expected a delayed retry")
	}
	got := getRC(t, r, "cp")
	if got.Status.Phase != phaseReady {
		t.Errorf("a post-Ready failure demoted the cluster to %q", got.Status.Phase)
	}
	if got.Status.ProvisionRetryCount != 0 {
		t.Errorf("post-Ready failure consumed the retry budget: %d", got.Status.ProvisionRetryCount)
	}
	c := apimeta.FindStatusCondition(got.Status.Conditions, "CorePackageVariantsFailed")
	if c == nil || c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "already belongs to cluster") {
		t.Errorf("expected a CorePackageVariantsFailed condition explaining the collision, got %+v", c)
	}
}

// --- finding 2 -------------------------------------------------------------

func mgmtResources() []client.Object {
	l := map[string]string{remoteClusterLabelKey: "c1"}
	return []client.Object{
		unstr("config.porch.kpt.dev/v1alpha1", "Repository", "default", "c1", l),
		unstr("infra.nephio.org/v1alpha1", "Repository", "default", "c1", nil),
		unstr("infra.nephio.org/v1alpha1", "Token", "default", "c1-access-token-porch", l),
		unstr("config.porch.kpt.dev/v1alpha1", "PackageVariant", packageVariantNamespace, "harbor-variant", l),
	}
}

func countMgmt(t *testing.T, r *RemoteClusterReconciler) int {
	t.Helper()
	n := 0
	for _, k := range []struct{ gv, kind string }{
		{"config.porch.kpt.dev/v1alpha1", "RepositoryList"},
		{"infra.nephio.org/v1alpha1", "RepositoryList"},
		{"infra.nephio.org/v1alpha1", "TokenList"},
		{"config.porch.kpt.dev/v1alpha1", "PackageVariantList"},
	} {
		l := &unstructured.UnstructuredList{}
		l.SetAPIVersion(k.gv)
		l.SetKind(k.kind)
		if err := r.List(context.Background(), l); err != nil {
			t.Fatal(err)
		}
		n += len(l.Items)
	}
	return n
}

func deleting(rc *infrav1.RemoteCluster) *infrav1.RemoteCluster {
	now := metav1.Now()
	rc.DeletionTimestamp = &now
	return rc
}

func TestHandleDelete_WorkerKeepsClusterManagementResources(t *testing.T) {
	objs := append(mgmtResources(), node("cp", "c1", "control-plane"), deleting(node("w", "c1", "worker")))
	r := newTestReconciler(t, objs...)
	w := getRC(t, r, "w")

	if _, err := r.handleDelete(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if n := countMgmt(t, r); n != 4 {
		t.Errorf("deleting a worker removed cluster management resources: %d/4 left", n)
	}
}

func TestHandleDelete_ControlPlaneRemovesClusterManagementResources(t *testing.T) {
	objs := append(mgmtResources(), deleting(node("cp", "c1", "control-plane")))
	r := newTestReconciler(t, objs...)
	cp := getRC(t, r, "cp")

	if _, err := r.handleDelete(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
	if n := countMgmt(t, r); n != 0 {
		t.Errorf("control-plane delete left %d management resources", n)
	}
}

// --- finding 4 -------------------------------------------------------------

func TestSetStatus_UpsertsConditionsByType(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	// History left behind by the old append-only behaviour.
	cp.Status.Conditions = []metav1.Condition{
		{Type: "Provisioning", Status: metav1.ConditionTrue, Reason: "Provisioning", Message: "old1", LastTransitionTime: metav1.Now()},
		{Type: "Provisioning", Status: metav1.ConditionTrue, Reason: "Provisioning", Message: "old2", LastTransitionTime: metav1.Now()},
	}
	r := newTestReconciler(t, cp)
	c := getRC(t, r, "cp")
	for i := 0; i < 5; i++ {
		if err := r.setStatus(context.Background(), c, phaseProvisioning, "Provisioning", "in progress", false); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.setStatus(context.Background(), c, phaseFailed, "SomethingFailed", "boom", true); err != nil {
		t.Fatal(err)
	}
	got := getRC(t, r, "cp")
	if len(got.Status.Conditions) != 2 {
		t.Fatalf("want one condition per type (2), got %d: %+v", len(got.Status.Conditions), got.Status.Conditions)
	}
	if p := apimeta.FindStatusCondition(got.Status.Conditions, "Provisioning"); p == nil || p.Message != "in progress" {
		t.Errorf("Provisioning condition not updated in place: %+v", p)
	}
}

// --- finding 3 -------------------------------------------------------------

func TestReconcile_ReadyWorkerWithPendingFinalizeIsRetried(t *testing.T) {
	// No control-plane yet: must requeue (not drop the pending work).
	w := node("w", "c1", "worker")
	w.Status.Phase = phaseReady
	w.Annotations = map[string]string{
		annotationWorkerJoined: "true", annotationWorkerFinalizePending: "10.8.0.5",
		annotationProvisionedVPNMode: vpnModeVPN,
	}
	r := newTestReconciler(t, w)
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 {
		t.Error("expected a requeue while the pending worker finalization cannot run")
	}
	if getRC(t, r, "w").Annotations[annotationWorkerFinalizePending] != "10.8.0.5" {
		t.Error("pending marker must survive")
	}
}

// --- finding 6/7 -----------------------------------------------------------

func TestResolveVPNDisabledForDelete(t *testing.T) {
	novpnCP := node("cp", "c1", "control-plane")
	novpnCP.Spec.DisableVPN = true
	vpnCP := node("cp", "c1", "control-plane")

	mk := func(spec bool, ann string) *infrav1.RemoteCluster {
		w := node("w", "c1", "worker")
		w.Spec.DisableVPN = spec
		if ann != "" {
			w.Annotations = map[string]string{annotationProvisionedVPNMode: ann}
		}
		return w
	}
	cases := []struct {
		name string
		cp   *infrav1.RemoteCluster
		w    *infrav1.RemoteCluster
		want bool
	}{
		{"recorded vpn beats a flipped spec", novpnCP, mk(true, vpnModeVPN), false},
		{"recorded novpn beats a flipped spec", vpnCP, mk(false, vpnModeNoVPN), true},
		{"never reconciled worker on a VPN-less CP is not purged", novpnCP, mk(false, ""), true},
		{"never reconciled worker on a VPN CP follows the spec", vpnCP, mk(false, ""), false},
		{"never reconciled, spec disabled, VPN CP: host never had wg", vpnCP, mk(true, ""), true},
	}
	for _, tc := range cases {
		r := newTestReconciler(t, tc.cp, tc.w)
		if got := r.resolveVPNDisabledForDelete(context.Background(), tc.w); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
	// Control-plane: recorded mode wins, otherwise spec.
	cp := node("cp", "c1", "control-plane")
	cp.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeNoVPN}
	r := newTestReconciler(t, cp)
	if !r.resolveVPNDisabledForDelete(context.Background(), cp) {
		t.Error("control-plane recorded novpn must be honoured")
	}
}

func TestEffectiveVPNDisabled_RecordedModeWins(t *testing.T) {
	c := node("n", "c1", "worker")
	c.Spec.DisableVPN = true
	if !effectiveVPNDisabled(c) {
		t.Error("no record: spec applies")
	}
	c.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN}
	if effectiveVPNDisabled(c) {
		t.Error("recorded vpn must win over spec.disableVPN=true")
	}
	if withEffectiveVPNMode(c).Spec.DisableVPN || !c.Spec.DisableVPN {
		t.Error("withEffectiveVPNMode must return a modified copy and leave the original untouched")
	}
}

func TestReconcileVPNMode_ProvisionedWorkerIsNotFlipped(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Spec.DisableVPN = true
	w := node("w", "c1", "worker")
	w.Status.Phase = phaseReady // really provisioned
	w.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN}
	r := newTestReconciler(t, cp, w)

	res, done, err := r.reconcileVPNMode(context.Background(), getRC(t, r, "w"))
	if err != nil || !done || res.RequeueAfter != time.Minute {
		t.Fatalf("want done + 1m requeue, got res=%+v done=%v err=%v", res, done, err)
	}
	got := getRC(t, r, "w")
	if got.Spec.DisableVPN {
		t.Error("a provisioned worker must not be silently flipped to no-VPN")
	}
	if c := apimeta.FindStatusCondition(got.Status.Conditions, conditionVPNModeMismatch); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("mismatch must be surfaced as a condition, got %+v", c)
	}
}

func TestReconcileVPNMode_MismatchDoesNotBurnRetryBudget(t *testing.T) {
	cp := node("cp", "c1", "control-plane") // VPN cluster
	w := node("w", "c1", "worker")
	w.Spec.DisableVPN = true // unprovisioned worker asks for no VPN
	r := newTestReconciler(t, cp, w)

	for i := 0; i < maxProvisionRetries+2; i++ {
		res, done, err := r.reconcileVPNMode(context.Background(), getRC(t, r, "w"))
		if err != nil || !done || res.RequeueAfter != time.Minute {
			t.Fatalf("iteration %d: res=%+v done=%v err=%v", i, res, done, err)
		}
	}
	got := getRC(t, r, "w")
	if got.Status.ProvisionRetryCount != 0 {
		t.Errorf("VPNModeMismatch consumed the retry budget: %d", got.Status.ProvisionRetryCount)
	}
	if got.Status.Phase != phaseFailed || !strings.Contains(got.Status.Message, "same mode") {
		t.Errorf("want a clear Failed status, got %q / %q", got.Status.Phase, got.Status.Message)
	}
	// Fixing the spec clears the condition.
	got.Spec.DisableVPN = false
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.reconcileVPNMode(context.Background(), getRC(t, r, "w")); done || err != nil {
		t.Fatalf("consistent worker must proceed, done=%v err=%v", done, err)
	}
	if c := apimeta.FindStatusCondition(getRC(t, r, "w").Status.Conditions, conditionVPNModeMismatch); c != nil {
		t.Errorf("mismatch condition must be cleared once resolved: %+v", c)
	}
}

func TestReconcileVPNMode_SpecFlipOnProvisionedControlPlaneIsSurfaced(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Spec.DisableVPN = true
	cp.Annotations = map[string]string{annotationProvisionedVPNMode: vpnModeVPN}
	r := newTestReconciler(t, cp)
	if _, done, err := r.reconcileVPNMode(context.Background(), getRC(t, r, "cp")); done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if c := apimeta.FindStatusCondition(getRC(t, r, "cp").Status.Conditions, conditionVPNModeChangeIgnored); c == nil {
		t.Error("ignored spec flip must be surfaced")
	}
}

// --- finding 8 -------------------------------------------------------------

func getPV(t *testing.T, r *RemoteClusterReconciler, name string) (*unstructured.Unstructured, bool) {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(packageVariantGVK)
	err := r.Get(context.Background(), types.NamespacedName{Namespace: packageVariantNamespace, Name: name}, got)
	if err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		return nil, false
	}
	return got, true
}

func TestUpsertPackageVariants_NeverOverwritesAnotherClustersVariant(t *testing.T) {
	foreign := unstr("config.porch.kpt.dev/v1alpha1", "PackageVariant", packageVariantNamespace, "pv1",
		map[string]string{remoteClusterLabelKey: "other"})
	foreign.Object["spec"] = map[string]interface{}{"marker": "other-cluster"}
	r := newTestReconciler(t, foreign)
	cp := node("cp", "c1", "control-plane")

	if err := r.upsertPackageVariants(context.Background(), cp, []packageVariantSpec{{name: "pv1"}}); err != nil {
		t.Fatal(err)
	}
	got, _ := getPV(t, r, "pv1")
	if spec, _ := got.Object["spec"].(map[string]interface{}); spec["marker"] != "other-cluster" || got.GetLabels()[remoteClusterLabelKey] != "other" {
		t.Errorf("the other cluster's PackageVariant was overwritten: %v %v", got.Object["spec"], got.GetLabels())
	}
	own, ok := getPV(t, r, perClusterPackageVariantName("pv1", "c1"))
	if !ok || own.GetLabels()[remoteClusterLabelKey] != "c1" {
		t.Fatalf("expected a per-cluster PackageVariant labelled for c1, got %v (exists=%v)", own, ok)
	}
}

func TestUpsertPackageVariants_TwoClustersCoexistAndLegacyNameIsReused(t *testing.T) {
	r := newTestReconciler(t)
	ctx := context.Background()
	c1, c2 := node("cp1", "c1", "control-plane"), node("cp2", "c2", "control-plane")
	v := func(rev string) []packageVariantSpec {
		return []packageVariantSpec{{name: "harbor-variant", upstream: packageRef{pkg: "a", repo: "r", revision: rev}}}
	}

	// c1 created its variant first (legacy, base name); c2 arrives later.
	for _, c := range []*infrav1.RemoteCluster{c1, c2} {
		if err := r.upsertPackageVariants(ctx, c, v("1")); err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
	}
	base, ok := getPV(t, r, "harbor-variant")
	if !ok || base.GetLabels()[remoteClusterLabelKey] != "c1" {
		t.Fatalf("the legacy name must stay with its owner c1: %v", base)
	}
	scoped, ok := getPV(t, r, perClusterPackageVariantName("harbor-variant", "c2"))
	if !ok || scoped.GetLabels()[remoteClusterLabelKey] != "c2" {
		t.Fatalf("c2 must get its own per-cluster variant: %v", scoped)
	}

	// Updates go to the right object for each owner and never create a third one.
	for _, c := range []*infrav1.RemoteCluster{c1, c2} {
		if err := r.upsertPackageVariants(ctx, c, v("2")); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"harbor-variant", perClusterPackageVariantName("harbor-variant", "c2")} {
		got, _ := getPV(t, r, name)
		if rev, _, _ := unstructured.NestedString(got.Object, "spec", "upstream", "revision"); rev != "2" {
			t.Errorf("%s not updated: revision %q", name, rev)
		}
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: packageVariantGVK.Group, Version: packageVariantGVK.Version, Kind: "PackageVariantList"})
	if err := r.List(ctx, list); err != nil || len(list.Items) != 2 {
		t.Fatalf("want exactly 2 PackageVariants, got %d (err %v)", len(list.Items), err)
	}

	// c1 goes away; c2 keeps its per-cluster object instead of flipping to the freed base name.
	if err := r.deleteClusterResources(ctx, c1); err != nil {
		t.Fatal(err)
	}
	if _, ok := getPV(t, r, "harbor-variant"); ok {
		t.Error("c1's variant must be swept by the label sweep")
	}
	if err := r.upsertPackageVariants(ctx, c2, v("3")); err != nil {
		t.Fatal(err)
	}
	if _, ok := getPV(t, r, "harbor-variant"); ok {
		t.Error("c2 must keep using its per-cluster name, not create a duplicate under the freed base name")
	}
	// c2's delete sweeps its per-cluster variant too.
	if err := r.deleteClusterResources(ctx, c2); err != nil {
		t.Fatal(err)
	}
	if _, ok := getPV(t, r, perClusterPackageVariantName("harbor-variant", "c2")); ok {
		t.Error("the per-cluster variant survived its cluster's deletion")
	}
}

func TestUpsertPackageVariants_UnlabelledLegacyVariantIsAdopted(t *testing.T) {
	legacy := unstr("config.porch.kpt.dev/v1alpha1", "PackageVariant", packageVariantNamespace, "pv1", nil)
	r := newTestReconciler(t, legacy)
	if err := r.upsertPackageVariants(context.Background(), node("cp", "c1", "control-plane"), []packageVariantSpec{{name: "pv1"}}); err != nil {
		t.Fatal(err)
	}
	got, _ := getPV(t, r, "pv1")
	if got.GetLabels()[remoteClusterLabelKey] != "c1" {
		t.Errorf("unlabelled legacy variant must be adopted, labels=%v", got.GetLabels())
	}
}

func TestPerClusterPackageVariantName(t *testing.T) {
	valid := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	if got := perClusterPackageVariantName("harbor-variant", "c2"); got != "harbor-variant-c2" {
		t.Errorf("simple name: %q", got)
	}
	long1 := strings.Repeat("x", 80) + "-one"
	long2 := strings.Repeat("x", 80) + "-two"
	seen := map[string]string{}
	for _, cn := range []string{"c2", "My_Cluster.1", long1, long2, "___", "UPPER"} {
		got := perClusterPackageVariantName("enterprise-gateway-variant", cn)
		if len(got) > maxPackageVariantNameLen || !valid.MatchString(got) {
			t.Errorf("%q -> %q is not a valid <=63 char name", cn, got)
		}
		if got != perClusterPackageVariantName("enterprise-gateway-variant", cn) {
			t.Errorf("%q -> %q is not stable", cn, got)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("%q and %q collide on %q", prev, cn, got)
		}
		seen[got] = cn
	}
	// Distinct raw names that sanitise identically must still differ.
	if perClusterPackageVariantName("b", "A_b") == perClusterPackageVariantName("b", "a-b") {
		t.Error("names that sanitise identically must not collide")
	}
}

func TestUpsertPackageVariants_CreatesAndUpdatesOwnVariants(t *testing.T) {
	r := newTestReconciler(t)
	cp := node("cp", "c1", "control-plane")
	cp.Namespace = "team-a" // PackageVariants stay in the fixed namespace
	v := packageVariantSpec{name: "pv1", upstream: packageRef{pkg: "a", repo: "r", revision: "1"}}

	for i := 0; i < 2; i++ { // second pass exercises the update path
		if err := r.upsertPackageVariants(context.Background(), cp, []packageVariantSpec{v}); err != nil {
			t.Fatal(err)
		}
		v.upstream.revision = "2"
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(packageVariantGVK)
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: packageVariantNamespace, Name: "pv1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.GetLabels()[remoteClusterLabelKey] != "c1" {
		t.Errorf("label lost: %v", got.GetLabels())
	}
	if rev, _, _ := unstructured.NestedString(got.Object, "spec", "upstream", "revision"); rev != "2" {
		t.Errorf("own variant not updated, revision=%q", rev)
	}

	// Delete sweeps the namespace the variants were created in even though the
	// RemoteCluster lives elsewhere.
	if err := r.deleteClusterResources(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: packageVariantNamespace, Name: "pv1"}, got); err == nil {
		t.Error("PackageVariant was not deleted from its own namespace")
	}
}

// --- finding 9 -------------------------------------------------------------

func authRef(name string) infrav1.RemoteClusterAuth {
	return infrav1.RemoteClusterAuth{SSHPrivateKeySecretRef: &infrav1.SecretKeyReference{Name: name, Key: "k"}}
}

func TestRemoveAuthSecretFinalizer_SharedSecretKeptWhileOthersUseIt(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "ssh", Namespace: "default", Finalizers: []string{authSecretFinalizer}}}
	a, b := node("a", "c1", "control-plane"), node("b", "c1", "worker")
	a.Spec.Auth, b.Spec.Auth = authRef("ssh"), authRef("ssh")
	r := newTestReconciler(t, secret, a, b)
	ctx := context.Background()

	r.removeAuthSecretFinalizer(ctx, getRC(t, r, "a"))
	got := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(secret), got); err != nil {
		t.Fatal(err)
	}
	if len(got.Finalizers) != 1 {
		t.Fatal("finalizer stripped while another RemoteCluster still uses the secret")
	}

	// b is going away too: it no longer counts as a user.
	if err := r.Delete(ctx, getRC(t, r, "b")); err != nil { // finalizer keeps it as "deleting"
		t.Fatal(err)
	}
	r.removeAuthSecretFinalizer(ctx, getRC(t, r, "a"))
	if err := r.Get(ctx, client.ObjectKeyFromObject(secret), got); err != nil {
		t.Fatal(err)
	}
	if len(got.Finalizers) != 0 {
		t.Errorf("finalizer must be released once no live RemoteCluster references the secret: %v", got.Finalizers)
	}
}

func TestRemoveVPNSecretFinalizer_SharedSecretKeptWhileOthersUseIt(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "vpn", Namespace: "default", Finalizers: []string{vpnSecretFinalizer}}}
	a, b := node("a", "c1", "control-plane"), node("b", "c1", "worker")
	a.Spec.VPNConfig.VPNSSHCredentialsRef = infrav1.VPNSSHCredentialsRef{Name: "vpn"}                       // empty ns defaults
	b.Spec.VPNConfig.VPNSSHCredentialsRef = infrav1.VPNSSHCredentialsRef{Name: "vpn", NameSpace: "default"} // explicit ns
	r := newTestReconciler(t, secret, a, b)
	ctx := context.Background()

	r.removeVPNSecretFinalizer(ctx, getRC(t, r, "a"))
	got := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(secret), got); err != nil {
		t.Fatal(err)
	}
	if len(got.Finalizers) != 1 {
		t.Fatal("VPN secret finalizer stripped while another node still uses it")
	}
	if err := r.Delete(ctx, getRC(t, r, "b")); err != nil { // finalizer keeps it as "deleting"
		t.Fatal(err)
	}
	r.removeVPNSecretFinalizer(ctx, getRC(t, r, "a"))
	if err := r.Get(ctx, client.ObjectKeyFromObject(secret), got); err != nil {
		t.Fatal(err)
	}
	if len(got.Finalizers) != 0 {
		t.Errorf("finalizer must be released once nobody else uses the secret: %v", got.Finalizers)
	}
}

// --- finding 10 ------------------------------------------------------------

func TestVPNSecretNamespaceDefaulting(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Namespace = "ns1"
	cp.Spec.VPNConfig = infrav1.VPNConfig{
		IP: "10.8.0.2", VPNServerPublicIP: "203.0.113.9",
		VPNSSHCredentialsRef: infrav1.VPNSSHCredentialsRef{Name: "vpn"},
	}
	spec := desiredNodeProvisionNetConfigFields(cp)
	if got := spec.VPNServerPublicConfig.VPNSSHCredentialsRef.NameSpace; got != "ns1" {
		t.Errorf("empty credential namespace must default to the RemoteCluster's, got %q", got)
	}
	cp.Spec.VPNConfig.VPNSSHCredentialsRef.NameSpace = "other"
	if got := vpnCredRefNamespace(cp, cp.Spec.VPNConfig.VPNSSHCredentialsRef); got != "other" {
		t.Errorf("explicit namespace must win, got %q", got)
	}
}

// --- finding 11 ------------------------------------------------------------

func TestCancelControlPlaneJobCancelsAndClears(t *testing.T) {
	r := &RemoteClusterReconciler{}
	var cancelled atomic.Int32
	job := &controlPlaneJob{uid: "u1", cancel: func() { cancelled.Add(1) }}
	r.controlPlaneJobs.Store("default/cp", job)
	r.controlPlaneProgress.Store("default/cp", 3)

	r.cancelControlPlaneJob("default/cp")
	if cancelled.Load() != 1 || !job.cancelled.Load() {
		t.Error("in-flight job was not cancelled")
	}
	if _, ok := r.controlPlaneJobs.Load("default/cp"); ok {
		t.Error("job entry not cleared")
	}
	if _, ok := r.controlPlaneProgress.Load("default/cp"); ok {
		t.Error("progress entry not cleared")
	}
	r.cancelControlPlaneJob("default/cp") // idempotent
}

func TestHandleDelete_CancelsInFlightControlPlaneJob(t *testing.T) {
	cp := deleting(node("cp", "c1", "control-plane"))
	r := newTestReconciler(t, cp)
	var cancelled atomic.Int32
	r.controlPlaneJobs.Store("default/cp", &controlPlaneJob{uid: string(cp.UID), cancel: func() { cancelled.Add(1) }})

	if _, err := r.handleDelete(context.Background(), getRC(t, r, "cp")); err != nil {
		t.Fatal(err)
	}
	if cancelled.Load() != 1 {
		t.Error("handleDelete must cancel the init goroutine")
	}
	if _, ok := r.controlPlaneJobs.Load("default/cp"); ok {
		t.Error("stale job left behind after delete")
	}
}

func TestReconcileControlPlane_DiscardsJobOfPreviousIncarnation(t *testing.T) {
	cp := node("cp", "c1", "control-plane") // UID uid-cp; no auth => SSH fails fast
	r := newTestReconciler(t, cp)
	var cancelled atomic.Int32
	ch := make(chan controlPlaneJobResult, 1)
	ch <- controlPlaneJobResult{joinCommand: "kubeadm join STALE"} // must never be adopted
	r.controlPlaneJobs.Store("default/cp", &controlPlaneJob{ch: ch, uid: "some-older-uid", cancel: func() { cancelled.Add(1) }})

	if _, err := r.reconcileControlPlane(context.Background(), getRC(t, r, "cp")); err != nil {
		t.Fatal(err)
	}
	if cancelled.Load() != 1 {
		t.Error("stale job must be cancelled")
	}
	if got := getRC(t, r, "cp"); got.Status.JoinCommand != "" {
		t.Errorf("stale result adopted by a new object: %q", got.Status.JoinCommand)
	}
}

// --- findings 12/13/14 -----------------------------------------------------

func TestParseNodeNameByIP(t *testing.T) {
	out := `cp-1     Ready   control-plane   5d   v1.34.2   10.8.0.2   <none>   Ubuntu 22.04   5.15   cri-o://1.34
worker-a Ready   <none>          4d   v1.34.2   10.8.0.7   <none>   Ubuntu 22.04   5.15   cri-o://1.34
worker-b Ready   <none>          4d   v1.34.2   10.8.0.70  <none>   Ubuntu 22.04   5.15   cri-o://1.34
`
	cases := map[string]string{"10.8.0.7": "worker-a", "10.8.0.70": "worker-b", "10.8.0.2": "cp-1", "10.8.0.9": "", "": ""}
	for ip, want := range cases {
		if got := parseNodeNameByIP(out, ip); got != want {
			t.Errorf("ip %q: got %q, want %q", ip, got, want)
		}
	}
}

func TestShellAndYAMLQuoting(t *testing.T) {
	if got := shQuote("it's $(x)"); got != `'it'\''s $(x)'` {
		t.Errorf("shQuote: %s", got)
	}
	if validNodeName("c1; rm -rf /") || validNodeName("") || !validNodeName("worker-1.example") {
		t.Error("validNodeName")
	}
	evil := "ns\nkind: Secret\n  x: \"y"
	cmd := secretApplyCmd("n", evil, corev1.SecretTypeOpaque, map[string][]byte{"k": []byte("v")})
	if strings.Contains(cmd, "\nkind: Secret\n  x:") {
		t.Errorf("namespace broke out of its YAML scalar:\n%s", cmd)
	}
	if !strings.Contains(cmd, `namespace: "ns\nkind: Secret`) {
		t.Errorf("namespace not emitted as a quoted scalar:\n%s", cmd)
	}
}

func TestPrepullScriptAndManifest_QuoteInterpolatedValues(t *testing.T) {
	evil := `img";touch /tmp/pwn;echo "x`
	script := rcBuildDaemonSetPrepullScript([]string{evil})
	if !strings.Contains(script, shQuote(evil)) || strings.Contains(script, `pull $CREDS "img";`) {
		t.Errorf("image reference not shell-quoted:\n%s", script)
	}
	m := rcBuildPrepullDaemonSetManifest("ds", "k", "v", []string{"a"}, "reg")
	if !strings.Contains(m, `"namespace": "`+prepullNamespace+`"`) {
		t.Errorf("DaemonSet must live in the prepull namespace: %s", m)
	}
}
