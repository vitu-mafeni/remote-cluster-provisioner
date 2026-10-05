package ml

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

const testWGKey = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ=" // 43 chars + '='

func fixReconciler(t *testing.T, objs ...client.Object) *NodeProvisionReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := mlv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mlv1alpha1.NodeProvision{}, &mlv1alpha1.NodeProvisionNetConfig{}).
		WithObjects(objs...).Build()
	return &NodeProvisionReconciler{Client: c}
}

func newNP(name string, mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	np := &mlv1alpha1.NodeProvision{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: "uid-" + "1"},
		Spec:       mlv1alpha1.NodeProvisionSpec{Provider: mlv1alpha1.CloudProviderOnPrem},
	}
	if mut != nil {
		mut(np)
	}
	return np
}

func readyNetConfig() *mlv1alpha1.NodeProvisionNetConfig {
	now := metav1.Now()
	return &mlv1alpha1.NodeProvisionNetConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "c-netconfig", Namespace: "default"},
		Status: mlv1alpha1.NodeProvisionNetConfigStatus{
			ClusterJoinCommand:   "kubeadm join x --token a.b --discovery-token-ca-cert-hash sha256:0",
			JoinTokenRefreshedAt: &now,
		},
	}
}

func reqFor(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Name: name, Namespace: "default"}}
}

func getNP(t *testing.T, r *NodeProvisionReconciler, name string) *mlv1alpha1.NodeProvision {
	t.Helper()
	got := &mlv1alpha1.NodeProvision{}
	if err := r.Get(context.Background(), client.ObjectKey{Name: name, Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	return got
}

// ── redaction ───────────────────────────────────────────────────────────────

func TestRedactSecrets(t *testing.T) {
	pem := "-----BEGIN OPENSSH PRIVATE KEY-----\nAAAAB3Nza\nC1rZXktdjE=\n-----END OPENSSH PRIVATE KEY-----"
	cases := []struct {
		name, in string
		gone     []string
	}{
		{"github pat", "pull failed for ghp_" + strings.Repeat("a1B2", 9), []string{"ghp_a1B2"}},
		{"fine grained", "token github_pat_" + strings.Repeat("Z9_x", 10), []string{"github_pat_Z9"}},
		{"docker pat", "login dckr_pat_AbCdEf0123456789-_xyz denied", []string{"dckr_pat_AbCdEf"}},
		{"pem block", "ssh failed: " + pem + " (end)", []string{"AAAAB3Nza", "C1rZXktdjE="}},
		{"pem unterminated", "out: -----BEGIN RSA PRIVATE KEY-----\nMIIEow", []string{"MIIEow"}},
		{"wireguard", "cfg:\n[Interface]\nPrivateKey = " + testWGKey + "\nAddress=10.0.0.2", []string{testWGKey}},
		{"token flag", "kubeadm join 1.2.3.4:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:1", []string{"abcdef.0123456789abcdef"}},
		{"password kv", "mysql password=hunter2 failed", []string{"hunter2"}},
		{"creds flag", "crictl pull --creds bob:s3cr3t img", []string{"s3cr3t"}},
		{"bearer", "Authorization: Bearer abcDEF123456.tok", []string{"abcDEF123456"}},
		{"aws key id", "key AKIAABCDEFGHIJKLMNOP bad", []string{"AKIAABCDEFGHIJKLMNOP"}},
	}
	for _, c := range cases {
		got := redactSecrets(c.in)
		for _, g := range c.gone {
			if strings.Contains(got, g) {
				t.Errorf("%s: %q still contains %q", c.name, got, g)
			}
		}
		if !strings.Contains(got, "[REDACTED]") {
			t.Errorf("%s: expected redaction marker in %q", c.name, got)
		}
	}
	if got := redactSecrets("plain error: connection refused"); got != "plain error: connection refused" {
		t.Errorf("benign text altered: %q", got)
	}
}

func TestTruncateMessage(t *testing.T) {
	long := strings.Repeat("é", 2000) // multi-byte runes
	got := sanitizeStatusMessage(long)
	if len(got) > statusMessageMaxLen {
		t.Errorf("len %d > %d", len(got), statusMessageMaxLen)
	}
	if !strings.HasSuffix(got, "(truncated)") {
		t.Errorf("missing truncation marker: %q", got[len(got)-20:])
	}
	if strings.ContainsRune(got, '�') {
		t.Error("truncation split a rune")
	}
	if s := sanitizeStatusMessage("short"); s != "short" {
		t.Errorf("short message changed: %q", s)
	}
}

func TestFailNodeProvisionRedactsAndTruncates(t *testing.T) {
	np := newNP("n", nil)
	r := fixReconciler(t, np)
	tok := "ghp_" + strings.Repeat("Q7w3", 9)
	if _, err := r.failNodeProvision(context.Background(), np, "boom "+tok+" "+strings.Repeat("x", 3000)); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if strings.Contains(got.Status.Message, tok) {
		t.Error("token leaked into status message")
	}
	if len(got.Status.Message) > statusMessageMaxLen {
		t.Errorf("message not bounded: %d", len(got.Status.Message))
	}
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 {
		t.Errorf("unexpected status %+v", got.Status)
	}
}

// ── shell handling ──────────────────────────────────────────────────────────

func TestShellQuote(t *testing.T) {
	if got := shellQuote("it's $(x)"); got != `'it'\''s $(x)'` {
		t.Errorf("got %s", got)
	}
}

func TestBuildPrepullScriptQuotesImages(t *testing.T) {
	evil := `reg.io/a'; touch /tmp/pwned; echo '`
	s := buildPrepullScript([]mlv1alpha1.ImagePrepull{{Image: evil}, {Image: "  "}, {Image: "reg.io/ok:1"}})
	if !strings.Contains(s, shellQuote(evil)) {
		t.Errorf("image not shell-quoted:\n%s", s)
	}
	if strings.Contains(s, "CREDS=\"--creds") || strings.Contains(s, "pull $CREDS") {
		t.Errorf("credentials still expanded unquoted:\n%s", s)
	}
	if !strings.Contains(s, `--creds "${REGISTRY_USER}:${REGISTRY_PASS}"`) || !strings.Contains(s, "${CREDS[@]+") {
		t.Errorf("credentials must be a quoted array element:\n%s", s)
	}
	if strings.Count(s, "pull ") != 2 {
		t.Errorf("blank image must be skipped:\n%s", s)
	}
}

func TestValidWireGuardKey(t *testing.T) {
	if !validWireGuardKey(testWGKey) {
		t.Error("valid key rejected")
	}
	for _, bad := range []string{"", "short=", testWGKey + "x", `abc"; rm -rf / #` + strings.Repeat("A", 30) + "=", strings.Repeat("A", 44)} {
		if validWireGuardKey(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}

// ── GPU rule ────────────────────────────────────────────────────────────────

func TestIsGPUNode(t *testing.T) {
	cases := []struct {
		hw, label string
		want      bool
	}{
		{"gpu", "", true}, {"GPU", "cpu", true},
		{"cpu", "gpu-a100", false}, // explicit hardwareType wins
		{"", "gpu-a100", true}, {"", "cpu", false}, {"", "", false},
	}
	for _, c := range cases {
		np := &mlv1alpha1.NodeProvision{Spec: mlv1alpha1.NodeProvisionSpec{HardwareType: c.hw, NodeLabel: c.label}}
		if got := isGPUNode(np); got != c.want {
			t.Errorf("hw=%q label=%q: got %v want %v", c.hw, c.label, got, c.want)
		}
	}
}

// ── credentials namespace ───────────────────────────────────────────────────

func TestGetSecretRestrictedToOwnNamespace(t *testing.T) {
	other := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "kube-system"}}
	own := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "default"}}
	r := fixReconciler(t, other, own)

	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.CredentialsRef.Name = "creds" })
	got, err := r.getSecret(context.Background(), np)
	if err != nil || got.Namespace != "default" {
		t.Fatalf("empty namespace must default to the NodeProvision's: %v %v", got, err)
	}

	np.Spec.CredentialsRef.Namespace = "default"
	if _, err := r.getSecret(context.Background(), np); err != nil {
		t.Fatalf("same namespace must work: %v", err)
	}

	np.Spec.CredentialsRef.Namespace = "kube-system"
	_, err = r.getSecret(context.Background(), np)
	if err == nil || !strings.Contains(err.Error(), "credentialsRef.namespace") {
		t.Fatalf("cross-namespace ref must be rejected with a clear message, got %v", err)
	}
}

func TestReconcileFailsOnForeignCredentialsNamespace(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Finalizers = []string{nodeProvisionFinalizer}
		np.Spec.CredentialsRef = mlv1alpha1.CredentialsRef{Name: "creds", Namespace: "kube-system"}
	})
	r := fixReconciler(t, np)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "credentialsRef.namespace") {
		t.Errorf("expected Failed with namespace message, got %q / %q", got.Status.Phase, got.Status.Message)
	}
}

// ── on-prem job tracking ────────────────────────────────────────────────────

func TestLoadOnPremJobDiscardsStaleUID(t *testing.T) {
	r := &NodeProvisionReconciler{}
	np := newNP("n", nil)
	cancelled := false
	r.onPremJobs.Store(onPremKey(np), &onPremJob{uid: "old-uid", cancel: func() { cancelled = true }})

	if _, ok := r.loadOnPremJob(np); ok {
		t.Fatal("job of a previous incarnation must not be returned")
	}
	if !cancelled {
		t.Error("stale job must be cancelled")
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("stale job must be removed")
	}

	r.onPremJobs.Store(onPremKey(np), &onPremJob{uid: np.UID, cancel: func() {}})
	if _, ok := r.loadOnPremJob(np); !ok {
		t.Error("job for the current UID must be returned")
	}
}

func startedJob(np *mlv1alpha1.NodeProvision, res *onPremJobResult) (*onPremJob, *bool) {
	ch := make(chan onPremJobResult, 1)
	if res != nil {
		ch <- *res
	}
	cancelled := new(bool)
	return &onPremJob{uid: np.UID, ch: ch, cancel: func() { *cancelled = true }}, cancelled
}

func TestPollOnPrem_FailureKeepsAllocationAndClearsJob(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.Phase = mlv1alpha1.NodeProvisionPhaseBootstrapping })
	r := fixReconciler(t, np, readyNetConfig())
	job, _ := startedJob(np, &onPremJobResult{vpnIP: "10.8.0.7", publicKey: testWGKey, err: context.DeadlineExceeded})
	r.onPremJobs.Store(onPremKey(np), job)

	if _, err := r.pollOnPremBootstrap(context.Background(), getNP(t, r, "n"), nil); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("phase = %q", got.Status.Phase)
	}
	if got.Status.VpnIP != "10.8.0.7" || got.Status.IPAddress != "10.8.0.7" {
		t.Errorf("allocation must be persisted with the failure, got vpn=%q ip=%q", got.Status.VpnIP, got.Status.IPAddress)
	}
	nc := &mlv1alpha1.NodeProvisionNetConfig{}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "c-netconfig", Namespace: "default"}, nc); err != nil {
		t.Fatal(err)
	}
	if len(nc.Status.VPNPeers) != 1 || nc.Status.VPNPeers[0].PublicKey != testWGKey {
		t.Errorf("peer must be recorded in the NetConfig: %+v", nc.Status)
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("job must be removed after a failure was recorded")
	}
}

func TestPollOnPrem_SuccessKeepsResultUntilPersisted(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.Phase = mlv1alpha1.NodeProvisionPhaseBootstrapping })
	r := fixReconciler(t, np) // no NetConfig yet: the persist step fails
	job, _ := startedJob(np, &onPremJobResult{vpnIP: "10.8.0.9", publicKey: testWGKey})
	r.onPremJobs.Store(onPremKey(np), job)

	if _, err := r.pollOnPremBootstrap(context.Background(), getNP(t, r, "n"), nil); err == nil {
		t.Fatal("expected an error while the NetConfig is unavailable")
	}
	v, ok := r.onPremJobs.Load(onPremKey(np))
	if !ok {
		t.Fatal("job result must not be lost when persisting fails (would re-run the whole bootstrap)")
	}
	if v.(*onPremJob).result == nil || v.(*onPremJob).result.vpnIP != "10.8.0.9" {
		t.Error("consumed result must be cached on the job")
	}

	// NetConfig appears: the retry succeeds from the cached result.
	if err := r.Create(context.Background(), readyNetConfig()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.pollOnPremBootstrap(context.Background(), getNP(t, r, "n"), nil); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseJoining || got.Status.VpnIP != "10.8.0.9" {
		t.Errorf("unexpected status %+v", got.Status)
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("job must be removed only after the result is persisted")
	}
}

func TestStopOnPremJobCancelsAndRecordsAllocation(t *testing.T) {
	np := newNP("n", nil)
	r := fixReconciler(t, np, readyNetConfig())
	job, cancelled := startedJob(np, &onPremJobResult{vpnIP: "10.8.0.4", publicKey: testWGKey, err: context.Canceled})
	r.onPremJobs.Store(onPremKey(np), job)

	pending, err := r.stopOnPremJob(context.Background(), np, 0, true)
	if err != nil || pending {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if !*cancelled {
		t.Error("goroutine context must be cancelled")
	}
	if got := getNP(t, r, "n"); got.Status.VpnIP != "10.8.0.4" {
		t.Errorf("allocation of the cancelled bootstrap must be recorded, got %q", got.Status.VpnIP)
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("job must be removed")
	}
}

func TestStopOnPremJobWaitsForSlowGoroutine(t *testing.T) {
	np := newNP("n", nil)
	r := fixReconciler(t, np)
	job, cancelled := startedJob(np, nil) // never reports
	r.onPremJobs.Store(onPremKey(np), job)

	pending, err := r.stopOnPremJob(context.Background(), np, 0, true)
	if err != nil || !pending || !*cancelled {
		t.Fatalf("pending=%v cancelled=%v err=%v", pending, *cancelled, err)
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); !still {
		t.Error("job must stay registered while pending")
	}
	// Past the wait budget: forget it.
	if pending, _ = r.stopOnPremJob(context.Background(), np, 0, false); pending {
		t.Error("must not stay pending once waiting is disallowed")
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("job must be dropped")
	}
}

// ── Failed handling ─────────────────────────────────────────────────────────

func TestReconcileFailed_HonoursBackoff(t *testing.T) {
	recent := metav1.NewTime(time.Now().Add(-10 * time.Second))
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = 1
		np.Status.LastUpdated = &recent
	})
	r := fixReconciler(t, np)
	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > requeueFailed {
		t.Errorf("expected the remaining backoff, got %v", res.RequeueAfter)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("must not reset before the backoff elapsed, phase=%q", got.Status.Phase)
	}
}

func TestReconcileFailed_ResetsAfterBackoffAndClearsFields(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-2 * requeueFailed))
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status = mlv1alpha1.NodeProvisionStatus{
			Phase: mlv1alpha1.NodeProvisionPhaseFailed, ProvisionRetryCount: 2, LastUpdated: &old,
			IPAddress: "1.2.3.4", PublicIP: "5.6.7.8", PrivateIP: "10.0.0.1", NodeName: "gone",
		}
	})
	r := fixReconciler(t, np)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	s := getNP(t, r, "n").Status
	if s.Phase != "" || s.IPAddress != "" || s.PublicIP != "" || s.PrivateIP != "" || s.NodeName != "" || s.InstanceID != "" {
		t.Errorf("retry reset must clear stale identity, got %+v", s)
	}
}

func TestReconcileFailed_AWSKeepsInstanceWhenTerminationFails(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-2 * requeueFailed))
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.Provider = mlv1alpha1.CloudProviderAWS
		np.Spec.Region = "us-east-1"
		np.Status = mlv1alpha1.NodeProvisionStatus{
			Phase: mlv1alpha1.NodeProvisionPhaseFailed, ProvisionRetryCount: 1, LastUpdated: &old,
			InstanceID: "i-0abc", VpnIP: "10.8.0.3",
		}
	})
	// No credentials secret, no controller copy: termination cannot happen.
	r := fixReconciler(t, np)
	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 {
		t.Error("must requeue to retry the termination")
	}
	s := getNP(t, r, "n").Status
	if s.Phase != mlv1alpha1.NodeProvisionPhaseFailed || s.InstanceID != "i-0abc" || s.VpnIP != "10.8.0.3" {
		t.Errorf("status must not be reset while a live instance exists, got %+v", s)
	}
}

func TestReconcileFailed_OnPremResumesRegisteredNode(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-2 * requeueFailed))
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status = mlv1alpha1.NodeProvisionStatus{
			Phase: mlv1alpha1.NodeProvisionPhaseFailed, ProvisionRetryCount: 1, LastUpdated: &old,
			VpnIP: "10.8.0.5", IPAddress: "10.8.0.5",
		}
	})
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Labels: map[string]string{nodeProvisionUIDLabel: string(np.UID)}},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.8.0.5"}}},
	}
	r := fixReconciler(t, np, node)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	s := getNP(t, r, "n").Status
	if s.Phase != mlv1alpha1.NodeProvisionPhaseJoining || s.VpnIP != "10.8.0.5" || s.NodeName != "worker-1" {
		t.Errorf("registered node must be resumed at Joining without releasing its peer, got %+v", s)
	}
}

func TestFindRegisteredNodeNeverAdoptsForeignNode(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.IPAddress = "10.8.0.5" })
	foreign := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Labels: map[string]string{nodeProvisionUIDLabel: "someone-else"}},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.8.0.5"}}},
	}
	r := fixReconciler(t, foreign)
	if n := r.findRegisteredNode(context.Background(), np); n != nil {
		t.Errorf("must not adopt a node owned by another NodeProvision, got %s", n.Name)
	}
}

// ── Deletion ────────────────────────────────────────────────────────────────

func terminatingNP(age time.Duration) *mlv1alpha1.NodeProvision {
	ts := metav1.NewTime(time.Now().Add(-age))
	return newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Finalizers = []string{nodeProvisionFinalizer}
		np.DeletionTimestamp = &ts
		np.Status.VpnIP = "10.8.0.6"
		np.Status.IPAddress = "10.8.0.6"
	})
}

// A NetConfig whose VPN server cannot be reached makes cleanupVPNPeer fail.
func brokenVPNNetConfig() *mlv1alpha1.NodeProvisionNetConfig {
	nc := readyNetConfig()
	nc.Status.VPNPeers = []mlv1alpha1.VPNPeerStatus{{NodeName: "n", VPNIP: "10.8.0.6", PublicKey: testWGKey}}
	return nc
}

func TestHandleDelete_KeepsFinalizerWhenVPNCleanupFails(t *testing.T) {
	np := terminatingNP(time.Minute)
	r := fixReconciler(t, np, brokenVPNNetConfig())
	res, err := r.handleDelete(context.Background(), getNP(t, r, "n"))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 {
		t.Error("must requeue to retry VPN cleanup")
	}
	if got := getNP(t, r, "n"); len(got.Finalizers) == 0 {
		t.Error("finalizer must be kept while the peer could not be released")
	}
}

func TestHandleDelete_GivesUpAfterGracePeriod(t *testing.T) {
	np := terminatingNP(deletionGiveUpAfter + time.Minute)
	r := fixReconciler(t, np, brokenVPNNetConfig())
	if _, err := r.handleDelete(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	got := &mlv1alpha1.NodeProvision{}
	err := r.Get(context.Background(), client.ObjectKey{Name: "n", Namespace: "default"}, got)
	if err == nil {
		t.Errorf("finalizer must be removed after the grace period, still present: %v", got.Finalizers)
	}
}

func TestWireGuardProvisionedFollowsStatus(t *testing.T) {
	cases := []struct {
		disable bool
		vpnIP   string
		want    bool
	}{
		{false, "", true}, {false, "10.0.0.2", true}, {true, "10.0.0.2", true}, {true, "", false},
	}
	for _, c := range cases {
		np := &mlv1alpha1.NodeProvision{
			Spec:   mlv1alpha1.NodeProvisionSpec{DisableVPN: c.disable},
			Status: mlv1alpha1.NodeProvisionStatus{VpnIP: c.vpnIP},
		}
		if got := wireGuardProvisioned(np); got != c.want {
			t.Errorf("disable=%v vpnIP=%q: got %v want %v", c.disable, c.vpnIP, got, c.want)
		}
	}
}

func TestCleanupVPNPeer_NothingAllocatedDoesNotNeedVPNServer(t *testing.T) {
	np := newNP("n", nil) // VPN enabled, but no peer / IP recorded
	r := fixReconciler(t, np, readyNetConfig())
	if err := r.cleanupVPNPeer(context.Background(), np); err != nil {
		t.Fatalf("no allocation means nothing to release, got %v", err)
	}
}

// ── Node name persistence / owner lookup ────────────────────────────────────

func TestResolveNodeNameFallsBackToUIDLabel(t *testing.T) {
	np := newNP("n", nil)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "w", Labels: map[string]string{nodeProvisionUIDLabel: string(np.UID)}}}
	r := fixReconciler(t, node)
	got, err := r.resolveNodeName(context.Background(), np)
	if err != nil || got != "w" {
		t.Fatalf("got %q, %v", got, err)
	}
	np.Status.NodeName = "explicit"
	if got, _ := r.resolveNodeName(context.Background(), np); got != "explicit" {
		t.Errorf("status.nodeName must win, got %q", got)
	}
}

func TestReconcileJoiningPersistsNodeNameBeforeStamping(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
		np.Status.IPAddress = "10.8.0.5"
	})
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "w"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.8.0.5"}}},
	}
	r := fixReconciler(t, np, node)
	if _, err := r.reconcileJoining(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if got := getNP(t, r, "n"); got.Status.NodeName != "w" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseReady {
		t.Errorf("unexpected status %+v", got.Status)
	}
	n := &corev1.Node{}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "w"}, n); err != nil {
		t.Fatal(err)
	}
	if n.Labels[nodeProvisionUIDLabel] != string(np.UID) {
		t.Error("node must be stamped with the owner UID")
	}
}

// ── Predicates ──────────────────────────────────────────────────────────────

func TestNodeProvisionPredicateStatusChanges(t *testing.T) {
	p := nodeProvisionReconcilePredicate()
	mk := func(phase mlv1alpha1.NodeProvisionPhase, cnt int, msg string) *mlv1alpha1.NodeProvision {
		np := newNP("n", nil)
		np.Status.Phase, np.Status.ProvisionRetryCount, np.Status.Message = phase, cnt, msg
		return np
	}
	base := mk(mlv1alpha1.NodeProvisionPhaseFailed, 5, "x")
	if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: mk(mlv1alpha1.NodeProvisionPhaseFailed, 0, "x")}) {
		t.Error("resetting provisionRetryCount must re-trigger reconcile")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: mk("", 5, "x")}) {
		t.Error("phase change must re-trigger reconcile")
	}
	if p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: mk(mlv1alpha1.NodeProvisionPhaseFailed, 5, "other message")}) {
		t.Error("message-only status change must stay filtered")
	}
}

func TestNetConfigChangePredicate(t *testing.T) {
	p := netConfigChangePredicate()
	a := readyNetConfig()
	peers := a.DeepCopy()
	peers.Status.VPNPeers = []mlv1alpha1.VPNPeerStatus{{NodeName: "x"}}
	peers.Status.UsedIPAddresses = []string{"10.0.0.2"}
	if p.Update(event.UpdateEvent{ObjectOld: a, ObjectNew: peers}) {
		t.Error("peer/IP bookkeeping must not fan out to every NodeProvision")
	}
	join := a.DeepCopy()
	join.Status.ClusterJoinCommand = "kubeadm join other"
	if !p.Update(event.UpdateEvent{ObjectOld: a, ObjectNew: join}) {
		t.Error("join command change must be delivered")
	}
	spec := a.DeepCopy()
	spec.Generation = 2
	if !p.Update(event.UpdateEvent{ObjectOld: a, ObjectNew: spec}) {
		t.Error("spec change must be delivered")
	}
}
