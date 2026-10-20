package ml

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

// ── shared helpers ──────────────────────────────────────────────────────────

func fixReconcilerFuncs(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *NodeProvisionReconciler {
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
		WithInterceptorFuncs(funcs).
		WithObjects(objs...).Build()
	return &NodeProvisionReconciler{Client: c}
}

func awsNP(name string, mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return newNP(name, func(np *mlv1alpha1.NodeProvision) {
		np.Finalizers = []string{nodeProvisionFinalizer}
		np.Spec.Provider = mlv1alpha1.CloudProviderAWS
		np.Spec.Region = "us-east-1"
		np.Spec.InstanceType = "t3.micro"
		np.Spec.CredentialsRef.Name = "creds"
		np.Spec.AWSConfig = &mlv1alpha1.AWSConfig{
			AMI: "ami-1", SubnetID: "subnet-1", SecurityGroupIDs: []string{"sg-1"}, KeyPairName: "kp",
		}
		if mut != nil {
			mut(np)
		}
	})
}

func awsCredsSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "default"},
		Data: map[string][]byte{
			"awsAccessKeyId":     []byte("AKIDEXAMPLE"),
			"awsSecretAccessKey": []byte("notarealsecretkey"),
		},
	}
}

// awsStub replaces the AWS calls with in-memory behaviour.
type awsStub struct {
	mu          sync.Mutex
	found       string // instance returned by the tag lookup
	findErr     error
	terminated  []string
	termErr     error
	onTerminate func(id string)
	provision   func() (*awsprovision.ProvisionResult, error)
	finds       int
}

func (s *awsStub) install(r *NodeProvisionReconciler) {
	r.awsOverrides = awsAPI{
		FindInstanceID: func(context.Context, *mlv1alpha1.NodeProvision, awsprovision.AWSCredentials) (string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.finds++
			return s.found, s.findErr
		},
		TerminateInstance: func(_ context.Context, _ *mlv1alpha1.NodeProvision, _ awsprovision.AWSCredentials, id string) error {
			s.mu.Lock()
			if s.termErr != nil {
				err := s.termErr
				s.mu.Unlock()
				return err
			}
			s.terminated = append(s.terminated, id)
			hook := s.onTerminate
			if s.found == id {
				s.found = "" // a terminated instance is no longer found
			}
			s.mu.Unlock()
			if hook != nil {
				hook(id)
			}
			return nil
		},
		ProvisionEC2Node: func(context.Context, *mlv1alpha1.NodeProvision, awsprovision.AWSCredentials,
			*ssh.Client, *mlv1alpha1.NodeProvisionNetConfig, pkgruntime.Config) (*awsprovision.ProvisionResult, error) {
			if s.provision == nil {
				return nil, errors.New("unexpected launch")
			}
			return s.provision()
		},
	}
}

func (s *awsStub) terminatedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.terminated...)
}

// fakeVPN is an in-memory WireGuard server.
type fakeVPN struct {
	mu      sync.Mutex
	peers   map[string]string
	removed []string
	dials   int
	dialErr error
}

func (f *fakeVPN) install(r *NodeProvisionReconciler) {
	r.dialVPNServer = func(context.Context, *mlv1alpha1.NodeProvisionNetConfig) (vpnServer, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.dials++
		if f.dialErr != nil {
			return nil, f.dialErr
		}
		return fakeVPNConn{f}, nil
	}
}

type fakeVPNConn struct{ f *fakeVPN }

func (c fakeVPNConn) ReadPeers() (map[string]string, error) { return c.f.peers, nil }
func (c fakeVPNConn) RemovePeer(k string) error {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.removed = append(c.f.removed, k)
	return nil
}
func (c fakeVPNConn) Close() error { return nil }

func netConfigWithPeer(nodeName, vpnIP, key string) *mlv1alpha1.NodeProvisionNetConfig {
	nc := readyNetConfig()
	nc.Status.VPNPeers = []mlv1alpha1.VPNPeerStatus{{NodeName: nodeName, VPNIP: vpnIP, PublicKey: key}}
	nc.Status.UsedIPAddresses = []string{vpnIP}
	return nc
}

func getNetConfig(t *testing.T, r *NodeProvisionReconciler) *mlv1alpha1.NodeProvisionNetConfig {
	t.Helper()
	nc := &mlv1alpha1.NodeProvisionNetConfig{}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "c-netconfig", Namespace: "default"}, nc); err != nil {
		t.Fatal(err)
	}
	return nc
}

func minutesAgo(m int) *metav1.Time {
	t := metav1.NewTime(time.Now().Add(-time.Duration(m) * time.Minute))
	return &t
}

// ── #3 redaction ────────────────────────────────────────────────────────────

func TestRedactSecrets_FormatsThatUsedToLeak(t *testing.T) {
	const sec = "S3cretVal9"
	cases := []struct{ name, in, want string }{
		{"quoted kv", `login failed: password="S3cretVal9" retry`, `login failed: password="[REDACTED]" retry`},
		{"export quoted", `export PASSWORD="S3cretVal9"`, `export PASSWORD="[REDACTED]"`},
		{"json", `{"password":"S3cretVal9"}`, `{"password":"[REDACTED]"}`},
		{"colon", `db password: S3cretVal9 rejected`, `db password: [REDACTED] rejected`},
		{"pgpassword", `PGPASSWORD=S3cretVal9 psql -h db`, `PGPASSWORD=[REDACTED] psql -h db`},
		{"prefixed name", `MY_TOKEN=S3cretVal9`, `MY_TOKEN=[REDACTED]`},
		{"bare kubeadm token", `join with token abcdef.0123456789abcdef failed`, `join with token [REDACTED] failed`},
		{"quoted flag with spaces", `run --password 'my S3cretVal9 pw' now`, `run --password '[REDACTED]' now`},
		{"double quoted flag", `run --token "a b S3cretVal9" now`, `run --token "[REDACTED]" now`},
		{"url userinfo", `pull https://bob:S3cretVal9@registry.io/x failed`, `pull https://bob:[REDACTED]@registry.io/x failed`},
		{"authorization basic", `Authorization: Basic dXNlcjpTM2NyZXRWYWw5`, ""},
		{"authorization bearer", `Authorization: Bearer abcDEF123456.tok`, ""},
		{"short bearer", `retry with Bearer abcDEF123456.tok`, `retry with Bearer [REDACTED]`},
		{"unterminated pem", "out: -----BEGIN RSA PRIVATE KEY-----\nMIIEowS3cretVal9", `out: [REDACTED]`},
	}
	for _, c := range cases {
		got := redactSecrets(c.in)
		if c.want != "" && got != c.want {
			t.Errorf("%s: redactSecrets(%q) = %q, want %q", c.name, c.in, got, c.want)
		}
		for _, leak := range []string{sec, "0123456789abcdef", "dXNlcjpTM2NyZXRWYWw5", "abcDEF123456"} {
			if strings.Contains(got, leak) {
				t.Errorf("%s: %q survived in %q", c.name, leak, got)
			}
		}
		if !strings.Contains(got, "[REDACTED]") {
			t.Errorf("%s: no redaction marker in %q", c.name, got)
		}
	}
}

func TestRedactSecrets_LegitimateMessagesSurvive(t *testing.T) {
	for _, in := range []string{
		"cwd pwd=/home/u ok",
		"the server sent a bearer authentication challenge",
		`fetching VPN server SSH secret "vpn-ssh": not found`,
		"dial tcp 10.8.0.1:22: i/o timeout",
		"instance i-0abc123 did not become running within 10m0s",
		"kubeadm join 1.2.3.4:6443 --discovery-token-ca-cert-hash sha256:0123abcd",
		"oras login reg.io --username u --password-stdin",
		"spec.credentialsRef.name=creds is not allowed",
		"provisioning failed (attempt 2/5): connection refused",
		"node worker-1 did not register within 15m0s",
	} {
		if got := redactSecrets(in); got != in {
			t.Errorf("legitimate message altered:\n in: %q\nout: %q", in, got)
		}
	}
}

func TestRedactSecrets_Idempotent(t *testing.T) {
	in := `--password 'my pw' MY_TOKEN=abc token abcdef.0123456789abcdef {"secret":"zz"}`
	once := redactSecrets(in)
	if twice := redactSecrets(once); twice != once {
		t.Errorf("not idempotent:\n%q\n%q", once, twice)
	}
}

// ── #2 GPU rule ─────────────────────────────────────────────────────────────

func TestIsGPUNodeAgreesWithAWSRule(t *testing.T) {
	for _, hw := range []string{"", "gpu", "GPU", " gpu ", "cpu"} {
		for _, label := range []string{"", "gpu", "cpu", "a100-gpu"} {
			np := &mlv1alpha1.NodeProvision{Spec: mlv1alpha1.NodeProvisionSpec{HardwareType: hw, NodeLabel: label}}
			if isGPUNode(np) != awsprovision.IsGPUNode(np) {
				t.Errorf("hw=%q label=%q: controller and cloud-init disagree", hw, label)
			}
		}
	}
	// The regression that motivated sharing the rule: hardwareType "gpu" with no label.
	if !isGPUNode(&mlv1alpha1.NodeProvision{Spec: mlv1alpha1.NodeProvisionSpec{HardwareType: "gpu"}}) {
		t.Error("hardwareType=gpu must be a GPU node")
	}
}

// ── #1 crash recovery / adoption ────────────────────────────────────────────

func creatingInstanceNP(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return awsNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseCreatingInstance
		np.Status.LastUpdated = minutesAgo(1)
		if mut != nil {
			mut(np)
		}
	})
}

func TestCrashBetweenLaunchAndStatusWrite_AdoptsAndKeepsPeer(t *testing.T) {
	np := creatingInstanceNP(nil) // VPN mode
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, awsCredsSecret())
	stub := &awsStub{found: "i-0crash"}
	stub.install(r)
	vpn := &fakeVPN{}
	vpn.install(r)

	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue to poll the adopted instance, got %+v", res)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "i-0crash" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Fatalf("instance must be adopted, got phase=%q id=%q", got.Status.Phase, got.Status.InstanceID)
	}
	if got.Status.VpnIP != "10.8.0.7" || got.Status.IPAddress != "10.8.0.7" {
		t.Errorf("recorded peer address must be recovered, got vpn=%q ip=%q", got.Status.VpnIP, got.Status.IPAddress)
	}
	if got.Status.ProvisionRetryCount != 0 {
		t.Errorf("adoption must not count a failure, got %d", got.Status.ProvisionRetryCount)
	}
	if ids := stub.terminatedIDs(); len(ids) != 0 {
		t.Errorf("a healthy instance must not be terminated, terminated %v", ids)
	}
	if peers := getNetConfig(t, r).Status.VPNPeers; len(peers) != 1 || peers[0].PublicKey != testWGKey {
		t.Errorf("the recorded peer must be preserved, got %+v", peers)
	}
	if len(vpn.removed) != 0 || vpn.dials != 0 {
		t.Errorf("the VPN server must not be touched, dials=%d removed=%v", vpn.dials, vpn.removed)
	}
}

func TestCrashRecovery_NoVPNJustPersistsInstanceBeforeStallTimeout(t *testing.T) {
	// The phase has been stalled far past the timeout: adoption must still win
	// over failing the attempt.
	np := creatingInstanceNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.LastUpdated = minutesAgo(int(creatingInstanceStallTimeout/time.Minute) + 5)
	})
	r := fixReconciler(t, np, awsCredsSecret())
	(&awsStub{found: "i-0abc"}).install(r)

	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "i-0abc" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Errorf("got phase=%q id=%q", got.Status.Phase, got.Status.InstanceID)
	}
	if got.Status.ProvisionRetryCount != 0 {
		t.Errorf("no failure may be counted, got %d", got.Status.ProvisionRetryCount)
	}
}

func TestCrashRecovery_VPNModeWithoutRecordedPeerTerminatesAndFailsForRetry(t *testing.T) {
	np := creatingInstanceNP(nil)
	r := fixReconciler(t, np, readyNetConfig(), awsCredsSecret()) // no peer recorded
	stub := &awsStub{found: "i-0orphan"}
	stub.install(r)

	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if ids := stub.terminatedIDs(); len(ids) != 1 || ids[0] != "i-0orphan" {
		t.Fatalf("an instance that can never join must be terminated exactly once, got %v", ids)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 {
		t.Errorf("attempt must fail for a clean retry, got phase=%q count=%d", got.Status.Phase, got.Status.ProvisionRetryCount)
	}
	if got.Status.InstanceID != "" {
		t.Errorf("a terminated instance must not be recorded, got %q", got.Status.InstanceID)
	}
}

func TestCreatingInstance_NothingLaunchedStillHonoursStallTimeout(t *testing.T) {
	stalled := creatingInstanceNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.LastUpdated = minutesAgo(int(creatingInstanceStallTimeout/time.Minute) + 5)
	})
	r := fixReconciler(t, stalled, awsCredsSecret())
	stub := &awsStub{}
	stub.install(r)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("stalled phase without an instance must fail, got %q", got.Status.Phase)
	}
	if stub.finds != 1 {
		t.Errorf("the instance lookup must run before the stall decision, ran %d times", stub.finds)
	}

	fresh := creatingInstanceNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	r2 := fixReconciler(t, fresh, awsCredsSecret())
	(&awsStub{}).install(r2)
	res, err := r2.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter != requeueShort {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if got := getNP(t, r2, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseCreatingInstance {
		t.Errorf("phase must be untouched while waiting, got %q", got.Status.Phase)
	}
}

func failedAWSNP(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return awsNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = 1
		np.Status.LastUpdated = minutesAgo(5)
		if mut != nil {
			mut(np)
		}
	})
}

func TestReconcileFailed_UnrecordedInstanceIsAdoptedBeforeItsPeerIsReleased(t *testing.T) {
	np := failedAWSNP(nil) // VPN mode, InstanceID never recorded
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, awsCredsSecret())
	stub := &awsStub{found: "i-0late"}
	stub.install(r)
	vpn := &fakeVPN{}
	vpn.install(r)

	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue, got %+v", res)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "i-0late" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance || got.Status.VpnIP != "10.8.0.7" {
		t.Errorf("instance must be adopted with its peer, got %+v", got.Status)
	}
	if ids := stub.terminatedIDs(); len(ids) != 0 {
		t.Errorf("a healthy instance must not be terminated, got %v", ids)
	}
	if len(vpn.removed) != 0 || len(getNetConfig(t, r).Status.VPNPeers) != 1 {
		t.Errorf("the instance's peer must not be released (removed=%v)", vpn.removed)
	}
}

func TestReconcileFailed_UnrecordedInstanceWithoutPeerIsTerminatedThenReset(t *testing.T) {
	np := failedAWSNP(nil)
	r := fixReconciler(t, np, readyNetConfig(), awsCredsSecret()) // VPN mode, no recorded peer
	stub := &awsStub{found: "i-0orphan"}
	stub.install(r)

	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if ids := stub.terminatedIDs(); len(ids) != 1 || ids[0] != "i-0orphan" {
		t.Fatalf("got terminated %v", ids)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != "" || got.Status.InstanceID != "" {
		t.Errorf("status must be reset after the orphan is gone, got %+v", got.Status)
	}
}

func TestReconcileFailed_UnrecordedInstanceNoVPNIsAdopted(t *testing.T) {
	np := failedAWSNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	r := fixReconciler(t, np, awsCredsSecret())
	stub := &awsStub{found: "i-0late"}
	stub.install(r)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "i-0late" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Errorf("got %+v", got.Status)
	}
	if len(stub.terminatedIDs()) != 0 {
		t.Error("must not terminate")
	}
}

func TestReconcileFailed_LookupErrorBlocksResetSoNoPeerIsReleased(t *testing.T) {
	np := failedAWSNP(nil)
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, awsCredsSecret())
	(&awsStub{findErr: errors.New("throttled")}).install(r)
	vpn := &fakeVPN{}
	vpn.install(r)

	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if len(vpn.removed) != 0 || len(getNetConfig(t, r).Status.VPNPeers) != 1 {
		t.Error("the peer must not be released while the instance state is unknown")
	}
	if got := getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("status must not be reset, got %q", got.Status.Phase)
	}
}

func TestReconcileFailed_AWSSuccessPathTerminatesRemovesNodeReleasesPeerAndResets(t *testing.T) {
	np := failedAWSNP(func(np *mlv1alpha1.NodeProvision) {
		np.Status.InstanceID = "i-0dead"
		np.Status.NodeName = "worker-a"
		np.Status.VpnIP = "10.8.0.7"
		np.Status.IPAddress = "10.8.0.7"
		np.Status.PrivateIP = "172.31.0.5"
		np.Status.PublicIP = "3.3.3.3"
	})
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "worker-a", Finalizers: []string{nodeProvisionNodeFinalizer},
		Labels: map[string]string{nodeProvisionUIDLabel: string(np.UID)},
	}}
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, node, awsCredsSecret())
	stub := &awsStub{}
	stub.install(r)
	vpn := &fakeVPN{}
	vpn.install(r)

	var got *mlv1alpha1.NodeProvision
	for i := 0; i < 4; i++ {
		if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
			t.Fatal(err)
		}
		if got = getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
			break
		}
	}
	if got.Status.Phase != "" {
		t.Fatalf("status must be reset after a successful teardown, got %+v", got.Status)
	}
	if ids := stub.terminatedIDs(); len(ids) != 1 || ids[0] != "i-0dead" {
		t.Errorf("instance must be terminated exactly once, got %v", ids)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "worker-a"}, &corev1.Node{}); !apierrors.IsNotFound(err) {
		t.Errorf("the joined node must be removed, err=%v", err)
	}
	if len(vpn.removed) != 1 || vpn.removed[0] != testWGKey {
		t.Errorf("peer must be removed from the VPN server, got %v", vpn.removed)
	}
	if nc := getNetConfig(t, r); len(nc.Status.VPNPeers) != 0 || len(nc.Status.UsedIPAddresses) != 0 {
		t.Errorf("peer/IP must be released from the NetConfig, got %+v", nc.Status)
	}
	// No stale identity may survive: the next attempt must launch fresh.
	s := got.Status
	if s.InstanceID != "" || s.VpnIP != "" || s.IPAddress != "" || s.NodeName != "" || s.PublicIP != "" || s.PrivateIP != "" {
		t.Errorf("stale identity kept after teardown: %+v", s)
	}
	if s.ProvisionRetryCount != 1 {
		t.Errorf("the retry count must survive the reset, got %d", s.ProvisionRetryCount)
	}
}

func TestReconcileFailed_AWSTerminationFailureLeavesStatusAlone(t *testing.T) {
	np := failedAWSNP(func(np *mlv1alpha1.NodeProvision) { np.Status.InstanceID = "i-0live"; np.Status.VpnIP = "10.8.0.7" })
	r := fixReconciler(t, np, netConfigWithPeer("n", "10.8.0.7", testWGKey), awsCredsSecret())
	(&awsStub{termErr: errors.New("boom")}).install(r)
	vpn := &fakeVPN{}
	vpn.install(r)
	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if got := getNP(t, r, "n"); got.Status.InstanceID != "i-0live" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("got %+v", got.Status)
	}
	if len(vpn.removed) != 0 {
		t.Error("the peer must stay until the instance is gone")
	}
}

// ── #1 provisioning entry: Adopted / ErrInstanceAlreadyLaunched ────────────

func awsProvisioningReconciler(t *testing.T, np *mlv1alpha1.NodeProvision, stub *awsStub, objs ...client.Object) *NodeProvisionReconciler {
	t.Helper()
	nc := readyNetConfig()
	nc.Spec.DisableVPN = np.Spec.DisableVPN // the cluster's mode must match the node's
	all := append([]client.Object{np, nc, awsCredsSecret()}, objs...)
	r := fixReconciler(t, all...)
	r.CredMgr = awsprovision.NewCredentialManager(r.Client, ctrl.Log.WithName("test"))
	stub.install(r)
	return r
}

func TestProvisioning_AlreadyLaunchedRequeuesWithoutCountingFailure(t *testing.T) {
	np := awsNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub := &awsStub{provision: func() (*awsprovision.ProvisionResult, error) {
		// A peer allocation is reported alongside; it was released, so it must not be recorded.
		return &awsprovision.ProvisionResult{VpnIP: "10.8.0.9", PublicKey: testWGKey},
			fmt.Errorf("launching: %w", awsprovision.ErrInstanceAlreadyLaunched)
	}}
	r := awsProvisioningReconciler(t, np, stub)

	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("expected a ~5s requeue, got %+v", res)
	}
	got := getNP(t, r, "n")
	if got.Status.ProvisionRetryCount != 0 || got.Status.Phase == mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("must not count a failure, got phase=%q count=%d", got.Status.Phase, got.Status.ProvisionRetryCount)
	}
	if got.Status.VpnIP != "" || len(getNetConfig(t, r).Status.VPNPeers) != 0 {
		t.Error("the released peer must not be recorded")
	}
}

func TestProvisioning_AdoptedResultPersistsInstance(t *testing.T) {
	np := awsNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub := &awsStub{provision: func() (*awsprovision.ProvisionResult, error) {
		return &awsprovision.ProvisionResult{InstanceID: "i-0adopted", Adopted: true}, nil
	}}
	r := awsProvisioningReconciler(t, np, stub)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "i-0adopted" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Errorf("got %+v", got.Status)
	}
	if len(stub.terminatedIDs()) != 0 {
		t.Error("must not terminate an adopted instance")
	}
}

func TestAdoptInstance_VPNModeRecoversPeerByNodeName(t *testing.T) {
	np := awsNP("n", nil)
	nc := netConfigWithPeer("n", "10.8.0.11", testWGKey)
	nc.Status.VPNPeers = append(nc.Status.VPNPeers, mlv1alpha1.VPNPeerStatus{NodeName: "other", VPNIP: "10.8.0.12", PublicKey: testWGKey})
	r := fixReconciler(t, np, nc, awsCredsSecret())
	stub := &awsStub{}
	stub.install(r)
	if _, err := r.adoptInstance(context.Background(), getNP(t, r, "n"), "i-1", nc); err != nil {
		t.Fatal(err)
	}
	if got := getNP(t, r, "n"); got.Status.VpnIP != "10.8.0.11" || got.Status.InstanceID != "i-1" {
		t.Errorf("must recover this node's own peer, got %+v", got.Status)
	}
}

// ── #4 on-prem failure persistence ──────────────────────────────────────────

func TestPollOnPrem_FailurePersistErrorKeepsJobAndDoesNotRestart(t *testing.T) {
	var failWrites atomic.Bool
	failWrites.Store(true)
	funcs := interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if _, ok := obj.(*mlv1alpha1.NodeProvision); ok && failWrites.Load() {
			return errors.New("apiserver unavailable")
		}
		return c.SubResource(sub).Update(ctx, obj, opts...)
	}}
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Finalizers = []string{nodeProvisionFinalizer}
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseBootstrapping
	})
	r := fixReconcilerFuncs(t, funcs, np, readyNetConfig())
	job, cancelled := startedJob(np, &onPremJobResult{vpnIP: "10.8.0.7", publicKey: testWGKey, err: errors.New("apt failed")})
	r.onPremJobs.Store(onPremKey(np), job)

	_, err := r.Reconcile(context.Background(), reqFor("n"))
	if err == nil {
		t.Fatal("a failed status write must be returned so the request is requeued")
	}
	stored, still := r.onPremJobs.Load(onPremKey(np))
	if !still || stored.(*onPremJob) != job {
		t.Fatal("the job (with its cached result) must be kept until the failure is persisted")
	}
	if got := getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseBootstrapping || got.Status.VpnIP != "" {
		t.Fatalf("nothing may be persisted yet, got %+v", got.Status)
	}
	_ = cancelled

	failWrites.Store(false)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 {
		t.Errorf("failure must be recorded once, got phase=%q count=%d", got.Status.Phase, got.Status.ProvisionRetryCount)
	}
	if got.Status.VpnIP != "10.8.0.7" {
		t.Errorf("the allocated peer must be recorded with the failure (else it leaks), got %q", got.Status.VpnIP)
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("job must be dropped once the failure is durable")
	}
}

func TestFailNodeProvision_ReturnsErrorWhenStatusWriteFails(t *testing.T) {
	funcs := interceptor.Funcs{SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
		return errors.New("boom")
	}}
	np := newNP("n", nil)
	r := fixReconcilerFuncs(t, funcs, np)
	if _, err := r.failNodeProvision(context.Background(), np, "x"); err == nil {
		t.Fatal("must not swallow a failed status write")
	}
	// A CR that is already gone is not an error.
	gone := newNP("gone", nil)
	if _, err := r.failNodeProvision(context.Background(), gone, "x"); err != nil {
		t.Errorf("NotFound must be ignored, got %v", err)
	}
}

// ── #5 terminal teardown ────────────────────────────────────────────────────

func terminalAWSNP(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return awsNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = maxProvisionRetries
		np.Status.LastUpdated = minutesAgo(1)
		np.Status.Message = "provisioning failed after 5 attempts (last error: boom) — manual intervention required"
		np.Status.InstanceID = "i-0term"
		np.Status.PrivateIP = "172.31.0.5"
		if mut != nil {
			mut(np)
		}
	})
}

func TestTerminalFailure_AWSTeardownRunsOnceAndIsRecorded(t *testing.T) {
	np := terminalAWSNP(nil)
	r := fixReconciler(t, np, awsCredsSecret())
	stub := &awsStub{}
	stub.install(r)

	for i := 0; i < 3; i++ { // reruns (restarts, resyncs) must be no-ops after the first
		if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
			t.Fatal(err)
		}
	}
	if ids := stub.terminatedIDs(); len(ids) != 1 || ids[0] != "i-0term" {
		t.Fatalf("instance must be terminated exactly once, got %v", ids)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != maxProvisionRetries {
		t.Errorf("terminal state must be preserved, got %+v", got.Status)
	}
	if !strings.Contains(got.Status.Message, terminalTeardownMarker) || !strings.Contains(got.Status.Message, "i-0term terminated") ||
		!strings.Contains(got.Status.Message, "manual intervention required") {
		t.Errorf("message must record the teardown and keep the failure reason, got %q", got.Status.Message)
	}
	if got.Status.InstanceID != "" || got.Status.PrivateIP != "" {
		t.Errorf("released identity must be cleared, got %+v", got.Status)
	}
}

func TestTerminalFailure_TerminationErrorRetriesWithoutRecording(t *testing.T) {
	np := terminalAWSNP(nil)
	r := fixReconciler(t, np, awsCredsSecret())
	(&awsStub{termErr: errors.New("throttled")}).install(r)
	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	got := getNP(t, r, "n")
	if strings.Contains(got.Status.Message, terminalTeardownMarker) || got.Status.InstanceID != "i-0term" {
		t.Errorf("an incomplete teardown must not be recorded as done: %+v", got.Status)
	}
}

func TestTerminalFailure_ResetDuringTeardownIsNotOverwritten(t *testing.T) {
	np := terminalAWSNP(nil)
	r := fixReconciler(t, np, awsCredsSecret())
	stub := &awsStub{}
	stub.install(r)
	// The operator re-arms the resource while the instance is being terminated.
	stub.onTerminate = func(string) {
		cur := getNP(t, r, "n")
		cur.Status.ProvisionRetryCount = 0
		if err := r.Status().Update(context.Background(), cur); err != nil {
			t.Error(err)
		}
	}
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.ProvisionRetryCount != 0 || strings.Contains(got.Status.Message, terminalTeardownMarker) {
		t.Errorf("the teardown must not fight a reset, got count=%d msg=%q", got.Status.ProvisionRetryCount, got.Status.Message)
	}
	// And the normal retry path then proceeds from the cleaned status without a second terminate.
	cur := getNP(t, r, "n")
	cur.Status.LastUpdated = minutesAgo(5)
	cur.Status.InstanceID = ""
	if err := r.Status().Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != "" {
		t.Errorf("retry must proceed after the reset, got %q", got.Status.Phase)
	}
	if ids := stub.terminatedIDs(); len(ids) != 1 {
		t.Errorf("terminate must not repeat, got %v", ids)
	}
}

func TestTerminalFailure_OnPremReleasesPeerButNeverAWorkingNode(t *testing.T) {
	mk := func() *mlv1alpha1.NodeProvision {
		return newNP("n", func(np *mlv1alpha1.NodeProvision) {
			np.Status = mlv1alpha1.NodeProvisionStatus{
				Phase: mlv1alpha1.NodeProvisionPhaseFailed, ProvisionRetryCount: maxProvisionRetries, LastUpdated: minutesAgo(1),
				VpnIP: "10.8.0.5", IPAddress: "10.8.0.5", Message: "failed",
			}
		})
	}
	// No registered node: the peer is released.
	np := mk()
	r := fixReconciler(t, np, netConfigWithPeer("n", "10.8.0.5", testWGKey))
	vpn := &fakeVPN{}
	vpn.install(r)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if len(vpn.removed) != 1 || len(getNetConfig(t, r).Status.VPNPeers) != 0 {
		t.Errorf("peer must be released, removed=%v", vpn.removed)
	}
	if got := getNP(t, r, "n"); !strings.Contains(got.Status.Message, terminalTeardownMarker) || got.Status.VpnIP != "" {
		t.Errorf("got %+v", got.Status)
	}

	// A registered node is a working machine: nothing may be cut off.
	np2 := mk()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "w", Labels: map[string]string{nodeProvisionUIDLabel: string(np2.UID)}},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.8.0.5"}}},
	}
	r2 := fixReconciler(t, np2, node, netConfigWithPeer("n", "10.8.0.5", testWGKey))
	vpn2 := &fakeVPN{}
	vpn2.install(r2)
	if _, err := r2.reconcileFailed(context.Background(), getNP(t, r2, "n")); err != nil {
		t.Fatal(err)
	}
	if len(vpn2.removed) != 0 || vpn2.dials != 0 {
		t.Errorf("a registered node's peer must be left alone, removed=%v", vpn2.removed)
	}
	if got := getNP(t, r2, "n"); got.Status.VpnIP != "10.8.0.5" {
		t.Errorf("status must be untouched, got %+v", got.Status)
	}
}

// ── #6 reconcileFailed error handling ──────────────────────────────────────

func TestReconcileFailed_StatusUpdateErrors(t *testing.T) {
	mk := func() *mlv1alpha1.NodeProvision {
		return newNP("n", func(np *mlv1alpha1.NodeProvision) {
			np.Spec.DisableVPN = true
			np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
			np.Status.ProvisionRetryCount = 1
			np.Status.LastUpdated = minutesAgo(5)
		})
	}
	failWith := func(err error) interceptor.Funcs {
		return interceptor.Funcs{SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return err
		}}
	}

	r := fixReconcilerFuncs(t, failWith(errors.New("etcd timeout")), mk())
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err == nil {
		t.Error("a non-conflict write failure must be returned, not swallowed")
	}

	conflict := apierrors.NewConflict(schema.GroupResource{Group: "ml.dcn.ssu.ac.kr", Resource: "nodeprovisions"}, "n", errors.New("stale"))
	r2 := fixReconcilerFuncs(t, failWith(conflict), mk())
	res, err := r2.reconcileFailed(context.Background(), getNP(t, r2, "n"))
	if err != nil || !res.Requeue {
		t.Errorf("a conflict must requeue immediately without error, got res=%+v err=%v", res, err)
	}
}

func TestReconcileFailed_VPNCleanupFailureIsSurfacedOnce(t *testing.T) {
	var updates atomic.Int32
	funcs := interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if _, ok := obj.(*mlv1alpha1.NodeProvision); ok {
			updates.Add(1)
		}
		return c.SubResource(sub).Update(ctx, obj, opts...)
	}}
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = 1
		np.Status.LastUpdated = minutesAgo(5)
		np.Status.VpnIP = "10.8.0.6"
		np.Status.Message = "provisioning failed (attempt 1/5): apt failed"
	})
	r := fixReconcilerFuncs(t, funcs, np, netConfigWithPeer("n", "10.8.0.6", testWGKey))
	vpn := &fakeVPN{dialErr: errors.New("ssh: dial tcp 1.2.3.4:22: password=Hunter2Pw rejected")}
	vpn.install(r)
	before := getNP(t, r, "n").Status.LastUpdated.DeepCopy()

	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	got := getNP(t, r, "n")
	if !strings.HasPrefix(got.Status.Message, retryBlockedPrefix) {
		t.Fatalf("the blocked retry must be visible in the message, got %q", got.Status.Message)
	}
	if strings.Contains(got.Status.Message, "Hunter2Pw") || !strings.Contains(got.Status.Message, "[REDACTED]") {
		t.Errorf("the message must be redacted, got %q", got.Status.Message)
	}
	if !strings.Contains(got.Status.Message, "apt failed") {
		t.Errorf("the original failure must stay visible, got %q", got.Status.Message)
	}
	if len(got.Status.Message) > statusMessageMaxLen {
		t.Errorf("message not bounded: %d", len(got.Status.Message))
	}
	if !got.Status.LastUpdated.Equal(before) || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.VpnIP != "10.8.0.6" {
		t.Errorf("backoff anchor and handle must be untouched: %+v", got.Status)
	}

	first := updates.Load()
	for i := 0; i < 3; i++ {
		if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
			t.Fatal(err)
		}
	}
	if n := updates.Load(); n != first {
		t.Errorf("an unchanged cleanup failure must not cause more status writes (%d -> %d)", first, n)
	}
}

func TestRetryBlockedMessageIsIdempotentAndBounded(t *testing.T) {
	err := errors.New(strings.Repeat("x", 5000))
	m1 := retryBlockedMessage("provisioning failed (attempt 1/5): boom", err)
	if m2 := retryBlockedMessage(m1, err); m2 != m1 {
		t.Errorf("not idempotent:\n%q\n%q", m1, m2)
	}
	if len(m1) > statusMessageMaxLen {
		t.Errorf("unbounded: %d", len(m1))
	}
	m3 := retryBlockedMessage(m1, errors.New("other"))
	if !strings.Contains(m3, "other") || strings.Count(m3, lastFailureSep) != 1 || !strings.Contains(m3, "boom") {
		t.Errorf("a new error must replace the old one without nesting: %q", m3)
	}
}

// ── #7 reconcileFailed stops the on-prem job ───────────────────────────────

func TestReconcileFailed_StopsRegisteredOnPremJobBeforeReset(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = 1
		np.Status.LastUpdated = minutesAgo(5)
	})
	r := fixReconciler(t, np)
	job, cancelled := startedJob(np, &onPremJobResult{err: context.Canceled})
	r.onPremJobs.Store(onPremKey(np), job)

	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if !*cancelled {
		t.Error("the bootstrap goroutine must be cancelled")
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("a leftover job would make re-provisioning requeue forever; it must be dropped")
	}
	if got := getNP(t, r, "n"); got.Status.Phase != "" {
		t.Errorf("status must then be reset, got %q", got.Status.Phase)
	}
}

func TestReconcileFailed_StoppedJobAllocationIsReleasedNotLeaked(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = 1
		np.Status.LastUpdated = minutesAgo(5)
	})
	r := fixReconciler(t, np, readyNetConfig())
	job, _ := startedJob(np, &onPremJobResult{vpnIP: "10.8.0.21", publicKey: testWGKey, err: context.Canceled})
	r.onPremJobs.Store(onPremKey(np), job)
	vpn := &fakeVPN{}
	vpn.install(r)

	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if len(vpn.removed) != 1 || vpn.removed[0] != testWGKey {
		t.Errorf("the peer the cancelled bootstrap registered must be released, got %v", vpn.removed)
	}
}

func TestReconcileFailed_UnreportedJobIsForgottenOncePastTheWaitBudget(t *testing.T) {
	old := failedJobStopBlock
	failedJobStopBlock = 0
	defer func() { failedJobStopBlock = old }()

	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = 1
		np.Status.LastUpdated = minutesAgo(5) // beyond onPremJobStopWait
	})
	r := fixReconciler(t, np)
	job, cancelled := startedJob(np, nil) // never reports
	r.onPremJobs.Store(onPremKey(np), job)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if !*cancelled {
		t.Error("must cancel")
	}
	if _, still := r.onPremJobs.Load(onPremKey(np)); still {
		t.Error("a goroutine that never reports must eventually be forgotten")
	}
}

// ── #8 manager context race ────────────────────────────────────────────────

func TestManagerContextIsRaceFree(t *testing.T) {
	r := &NodeProvisionReconciler{}
	if r.baseContext() == nil || r.baseContext().Err() != nil {
		t.Fatal("before Start a usable background context is expected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = r.baseContext().Err()
				}
			}
		}()
	}
	go func() { done <- r.Start(ctx) }() // writes the context while the readers run
	deadline := time.Now().Add(2 * time.Second)
	for r.baseContext() == context.Background() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.baseContext() == context.Background() {
		t.Fatal("Start must publish the manager context")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if r.baseContext().Err() == nil {
		t.Error("background goroutines must observe the manager shutdown")
	}
}

// ── #9 node reset runs once per deletion ────────────────────────────────────

func TestHandleDelete_NodeResetRunsOncePerDeletion(t *testing.T) {
	np := terminatingNP(time.Minute) // on-prem
	r := fixReconciler(t, np, brokenVPNNetConfig())
	var runs int
	r.nodeReset = func(context.Context, *mlv1alpha1.NodeProvision) bool { runs++; return true }

	for i := 0; i < 3; i++ { // the VPN cleanup keeps failing, so deletion is retried
		res, err := r.handleDelete(context.Background(), getNP(t, r, "n"))
		if err != nil || res.RequeueAfter <= 0 {
			t.Fatalf("pass %d: res=%+v err=%v", i, res, err)
		}
	}
	if runs != 1 {
		t.Errorf("the node reset script ran %d times, want exactly 1", runs)
	}
	if got := getNP(t, r, "n"); got.Annotations[nodeResetDoneAnnotation] == "" {
		t.Error("completion must be recorded on the NodeProvision")
	}

	// A fresh reconciler (controller restart) honours the annotation.
	r2 := fixReconciler(t, getNP(t, r, "n"), brokenVPNNetConfig())
	var runs2 int
	r2.nodeReset = func(context.Context, *mlv1alpha1.NodeProvision) bool { runs2++; return true }
	r2.handleDelete(context.Background(), getNP(t, r2, "n")) //nolint:errcheck
	if runs2 != 0 {
		t.Error("the annotation must survive a restart")
	}
}

func TestHandleDelete_UnreachableNodeIsRetried(t *testing.T) {
	np := terminatingNP(time.Minute)
	r := fixReconciler(t, np, brokenVPNNetConfig())
	var runs int
	r.nodeReset = func(context.Context, *mlv1alpha1.NodeProvision) bool { runs++; return false } // could not connect
	for i := 0; i < 2; i++ {
		if _, err := r.handleDelete(context.Background(), getNP(t, r, "n")); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 2 {
		t.Errorf("a reset that never ran must be retried, ran %d times", runs)
	}
}

func TestNodeResetInMemoryMarkerCoversFailedAnnotationWrite(t *testing.T) {
	np := terminatingNP(time.Minute)
	funcs := interceptor.Funcs{Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
		return errors.New("patch denied")
	}}
	r := fixReconcilerFuncs(t, funcs, np, brokenVPNNetConfig())
	var runs int
	r.nodeReset = func(context.Context, *mlv1alpha1.NodeProvision) bool { runs++; return true }
	for i := 0; i < 2; i++ {
		if _, err := r.handleDelete(context.Background(), getNP(t, r, "n")); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 1 {
		t.Errorf("ran %d times, want 1", runs)
	}
}

// ── #10 pre-pull retry counting ─────────────────────────────────────────────

func failedPrepullJob(np *mlv1alpha1.NodeProvision) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: np.Name + "-prepull", Namespace: "default", UID: "job-uid-1"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}},
	}
}

func prepullNetConfig() *mlv1alpha1.NodeProvisionNetConfig {
	nc := readyNetConfig()
	nc.Spec.SoftwareConfig.ImagePrepulls = []mlv1alpha1.ImagePrepull{{Image: "reg.io/a:1"}}
	return nc
}

func TestPrepullFailureIsCountedOncePerJobDespiteStaleCache(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.Phase = mlv1alpha1.NodeProvisionPhasePrePullingImages })
	job := failedPrepullJob(np)

	// The API server (truth) is a plain fake; the reconciler's cached client keeps
	// returning the Job as Failed, like an informer that has not caught up.
	truth := fixReconciler(t, np, prepullNetConfig(), job)
	stale := job.DeepCopy()
	funcs := interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if j, ok := obj.(*batchv1.Job); ok {
			stale.DeepCopyInto(j)
			return nil
		}
		return c.Get(ctx, key, obj, opts...)
	}}
	r := &NodeProvisionReconciler{Client: interceptorOver(truth.Client, funcs), APIReader: truth.Client}

	for i := 0; i < 3; i++ { // each annotation write wakes another reconcile
		if _, err := r.reconcileNPImagePrepull(context.Background(), getNP(t, truth, "n")); err != nil {
			t.Fatal(err)
		}
	}
	got := getNP(t, truth, "n")
	if n := prepullRetryCount(got); n != 1 {
		t.Errorf("one Job failure must be counted once, counted %d", n)
	}
	if got.Status.Phase == mlv1alpha1.NodeProvisionPhaseFailed {
		t.Error("a single failure must not exhaust the retry budget")
	}
}

func TestPrepullFailureConfirmedByAPIServerIsCounted(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.Phase = mlv1alpha1.NodeProvisionPhasePrePullingImages })
	r := fixReconciler(t, np, prepullNetConfig(), failedPrepullJob(np))
	if _, err := r.reconcileNPImagePrepull(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if n := prepullRetryCount(getNP(t, r, "n")); n != 1 {
		t.Errorf("a genuine failure must be counted, got %d", n)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "n-prepull", Namespace: "default"}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Errorf("the failed Job must be deleted for the retry, err=%v", err)
	}
}

// interceptorOver wraps an existing client with interceptor functions.
func interceptorOver(c client.Client, funcs interceptor.Funcs) client.Client {
	wc, ok := c.(client.WithWatch)
	if !ok {
		panic("fake client must implement WithWatch")
	}
	return interceptor.NewClient(wc, funcs)
}

// ── #12 malformed peer key must never reach the VPN server ─────────────────

func TestUsablePeerKey(t *testing.T) {
	if k, ok := usablePeerKey(testWGKey); !ok || k != testWGKey {
		t.Errorf("valid key rejected: %q %v", k, ok)
	}
	if k, ok := usablePeerKey(""); !ok || k != "" {
		t.Errorf("empty key means nothing to remove and is fine: %q %v", k, ok)
	}
	for _, bad := range []string{`x"; rm -rf /; "`, "short=", testWGKey + "x", "$(reboot)" + strings.Repeat("A", 40) + "="} {
		if k, ok := usablePeerKey(bad); ok || k != "" {
			t.Errorf("%q must be rejected, got %q %v", bad, k, ok)
		}
	}
}

func TestCleanupVPNPeer_MalformedRecordedKeyNeverReachesTheServer(t *testing.T) {
	evil := `x"; rm -rf /; "`
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.VpnIP = "10.8.0.6" })
	r := fixReconciler(t, np, netConfigWithPeer("n", "10.8.0.6", evil))
	vpn := &fakeVPN{}
	vpn.install(r)

	if err := r.cleanupVPNPeer(context.Background(), np); err != nil {
		t.Fatal(err)
	}
	if len(vpn.removed) != 0 {
		t.Errorf("a malformed key must never be passed on for removal, got %v", vpn.removed)
	}
	if nc := getNetConfig(t, r); len(nc.Status.VPNPeers) != 0 || len(nc.Status.UsedIPAddresses) != 0 {
		t.Errorf("the IP must still be released, got %+v", nc.Status)
	}
}

func TestCleanupVPNPeer_MalformedLiveServerKeyIsIgnoredValidKeyIsRemoved(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.VpnIP = "10.8.0.6" })
	// No CR record: the key comes from the live server's peer list.
	r := fixReconciler(t, np, readyNetConfig())
	vpn := &fakeVPN{peers: map[string]string{"10.8.0.6": "bad key; reboot"}}
	vpn.install(r)
	if err := r.cleanupVPNPeer(context.Background(), np); err != nil {
		t.Fatal(err)
	}
	if len(vpn.removed) != 0 {
		t.Errorf("malformed live key must not be used, got %v", vpn.removed)
	}

	r2 := fixReconciler(t, newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.VpnIP = "10.8.0.6" }), readyNetConfig())
	vpn2 := &fakeVPN{peers: map[string]string{"10.8.0.6": testWGKey}}
	vpn2.install(r2)
	if err := r2.cleanupVPNPeer(context.Background(), np); err != nil {
		t.Fatal(err)
	}
	if len(vpn2.removed) != 1 || vpn2.removed[0] != testWGKey {
		t.Errorf("a valid key must be removed, got %v", vpn2.removed)
	}
}

func TestCleanupVPNPeer_ConnectionErrorIsReturned(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Status.VpnIP = "10.8.0.6" })
	r := fixReconciler(t, np, netConfigWithPeer("n", "10.8.0.6", testWGKey))
	(&fakeVPN{dialErr: errors.New("unreachable")}).install(r)
	if err := r.cleanupVPNPeer(context.Background(), np); err == nil {
		t.Error("an unreachable VPN server must fail the cleanup so it is retried")
	}
	if len(getNetConfig(t, r).Status.VPNPeers) != 1 {
		t.Error("the peer record must be kept until the server confirmed removal")
	}
}
