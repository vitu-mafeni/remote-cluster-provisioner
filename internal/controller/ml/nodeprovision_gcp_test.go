package ml

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
	gcpprovision "dcn.ssu.ac.kr/infra/provider/gcp"
)

// ── fixtures ────────────────────────────────────────────────────────────────

const gcpSAKey = `{"type":"service_account","project_id":"proj-1","private_key":"-----BEGIN PRIVATE KEY-----\nMIIfake\n-----END PRIVATE KEY-----\n","client_email":"sa@proj-1.iam.gserviceaccount.com","token_uri":"https://oauth2.googleapis.com/token"}`

func gcpNP(name string, mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return newNP(name, func(np *mlv1alpha1.NodeProvision) {
		np.Finalizers = []string{nodeProvisionFinalizer}
		np.Spec.Provider = mlv1alpha1.CloudProviderGCP
		np.Spec.Region = "us-central1"
		np.Spec.InstanceType = "e2-standard-4"
		np.Spec.CredentialsRef.Name = "gcp-creds"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{
			ProjectID: "proj-1", Zone: "us-central1-a", Network: "default",
			SourceImage: "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20260101",
		}
		if mut != nil {
			mut(np)
		}
	})
}

func gcpCredsSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "gcp-creds", Namespace: "default"},
		Data:       map[string][]byte{"credentials.json": []byte(gcpSAKey)},
	}
}

var (
	gcpKeyOnce sync.Once
	gcpKeyPEM  string
	gcpKeyPub  []byte
)

// gcpSSHKeySecret is a pre-existing <name>-ssh-key Secret (generating a 4096-bit
// RSA key per test would be slow); the generation path has its own test.
func gcpSSHKeySecret(t *testing.T, name string) *corev1.Secret {
	t.Helper()
	gcpKeyOnce.Do(func() {
		var err error
		gcpKeyPEM, gcpKeyPub, err = gcpprovision.GenerateSSHKeyPair()
		if err != nil {
			panic(err)
		}
	})
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-ssh-key", Namespace: "default"},
		Type:       corev1.SecretTypeSSHAuth,
		Data:       map[string][]byte{"ssh-privatekey": []byte(gcpKeyPEM)},
	}
}

// gcpStub replaces the GCP calls (and the VPN-server dial) with in-memory behaviour.
type gcpStub struct {
	mu sync.Mutex

	resolve      func() (*gcpprovision.Resolved, error)
	resolveCalls int

	found    string
	findErr  error
	finds    int
	provCall int
	sawVPN   []bool // per ProvisionInstance call: was a VPN client passed
	sawKey   []byte
	provSeen *mlv1alpha1.NodeProvision

	provision func() (*gcpprovision.ProvisionResult, error)

	waitIn, waitEx string
	waitErr        error
	waits          int

	deleted   []string // instance ids passed to DeleteInstance ("" = firewalls only)
	deleteErr error
	onDelete  func(id string)

	vpnDials int
	vpnErr   error
	vpnConn  func() *ssh.Client
}

func (s *gcpStub) install(r *NodeProvisionReconciler) {
	r.gcpOverrides = gcpAPI{
		ResolveDefaults: func(context.Context, *mlv1alpha1.NodeProvision, gcpprovision.Credentials) (*gcpprovision.Resolved, error) {
			s.mu.Lock()
			s.resolveCalls++
			f := s.resolve
			s.mu.Unlock()
			if f == nil {
				return nil, errors.New("unexpected ResolveDefaults call")
			}
			return f()
		},
		FindInstance: func(context.Context, *mlv1alpha1.NodeProvision, gcpprovision.Credentials) (string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.finds++
			return s.found, s.findErr
		},
		ProvisionInstance: func(_ context.Context, np *mlv1alpha1.NodeProvision, _ gcpprovision.Credentials, pub []byte,
			vpn *ssh.Client, _ *mlv1alpha1.NodeProvisionNetConfig, _ pkgruntime.Config) (*gcpprovision.ProvisionResult, error) {
			s.mu.Lock()
			s.provCall++
			s.sawVPN = append(s.sawVPN, vpn != nil)
			s.sawKey = pub
			s.provSeen = np.DeepCopy()
			f := s.provision
			s.mu.Unlock()
			if f == nil {
				return nil, errors.New("unexpected launch")
			}
			return f()
		},
		WaitForInstanceRunning: func(context.Context, *mlv1alpha1.NodeProvision, gcpprovision.Credentials, string) (string, string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.waits++
			return s.waitIn, s.waitEx, s.waitErr
		},
		DeleteInstance: func(_ context.Context, _ *mlv1alpha1.NodeProvision, _ gcpprovision.Credentials, id string) error {
			s.mu.Lock()
			if s.deleteErr != nil {
				err := s.deleteErr
				s.mu.Unlock()
				return err
			}
			s.deleted = append(s.deleted, id)
			if s.found == id {
				s.found = ""
			}
			hook := s.onDelete
			s.mu.Unlock()
			if hook != nil {
				hook(id)
			}
			return nil
		},
		DialVPNServer: func(context.Context, *mlv1alpha1.NodeProvisionNetConfig) (*ssh.Client, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.vpnDials++
			if s.vpnErr != nil {
				return nil, s.vpnErr
			}
			if s.vpnConn == nil {
				return nil, errors.New("unexpected VPN dial")
			}
			return s.vpnConn(), nil
		},
	}
}

func (s *gcpStub) deletedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deleted...)
}

// vpnClient serves real SSH connections from an in-process server so the
// controller can `defer vpn.Conn.Close()` like in production.
func (s *gcpStub) withVPNServer(t *testing.T) {
	t.Helper()
	srv := sshtest.Start(t, sshtest.Options{})
	s.vpnConn = func() *ssh.Client { return &ssh.Client{Conn: srv.Dial(t)} }
}

// gcpProvisioningReconciler builds a reconciler whose cluster mode matches the node's.
func gcpProvisioningReconciler(t *testing.T, np *mlv1alpha1.NodeProvision, stub *gcpStub, objs ...client.Object) *NodeProvisionReconciler {
	t.Helper()
	nc := readyNetConfig()
	nc.Spec.DisableVPN = np.Spec.DisableVPN
	all := append([]client.Object{np, nc, gcpCredsSecret(), gcpSSHKeySecret(t, np.Name)}, objs...)
	r := fixReconciler(t, all...)
	stub.install(r)
	return r
}

func gcpResult(vpnIP string) *gcpprovision.ProvisionResult {
	res := &gcpprovision.ProvisionResult{InstanceID: "n"}
	if vpnIP != "" {
		res.VpnIP, res.PublicKey = vpnIP, testWGKey
	}
	return res
}

func reconcileOnce(t *testing.T, r *NodeProvisionReconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reqFor(name)); err != nil {
		t.Fatal(err)
	}
}

// ── happy paths ─────────────────────────────────────────────────────────────

func TestGCP_HappyPathWithVPN(t *testing.T) {
	np := gcpNP("n", nil)
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) { return gcpResult("10.8.0.9"), nil }}
	stub.withVPNServer(t)
	stub.waitIn, stub.waitEx = "10.128.0.7", "34.1.2.3"
	r := gcpProvisioningReconciler(t, np, stub)
	vpn := &fakeVPN{}
	vpn.install(r)

	// 1. provisioning: validate, adopt-check, connect to the VPN server, launch, persist.
	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "n" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Fatalf("instance must be persisted right after launch, got phase=%q id=%q", got.Status.Phase, got.Status.InstanceID)
	}
	if got.Status.VpnIP != "10.8.0.9" || got.Status.IPAddress != "10.8.0.9" || got.Status.ProvisionRetryCount != 0 {
		t.Errorf("VPN identity must be recorded: %+v", got.Status)
	}
	if stub.provCall != 1 || len(stub.sawVPN) != 1 || !stub.sawVPN[0] {
		t.Errorf("the VPN server connection must be passed to the launch, sawVPN=%v", stub.sawVPN)
	}
	if stub.vpnDials != 1 {
		t.Errorf("VPN server dials = %d", stub.vpnDials)
	}
	if len(stub.sawKey) == 0 || string(stub.sawKey) != string(gcpKeyPub) {
		t.Error("the public key derived from the <name>-ssh-key Secret must reach the launch")
	}
	if peers := getNetConfig(t, r).Status.VPNPeers; len(peers) != 1 || peers[0].NodeName != "n" || peers[0].VPNIP != "10.8.0.9" {
		t.Errorf("the peer must be recorded under the node's name so it can be released later: %+v", peers)
	}

	// 2. waiting: instance running -> Bootstrapping with the cloud addresses.
	reconcileOnce(t, r, "n")
	got = getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseBootstrapping || got.Status.PrivateIP != "10.128.0.7" || got.Status.PublicIP != "34.1.2.3" {
		t.Fatalf("phase=%q private=%q public=%q", got.Status.Phase, got.Status.PrivateIP, got.Status.PublicIP)
	}
	if got.Status.IPAddress != "10.8.0.9" {
		t.Errorf("with a VPN the node address stays the tunnel IP, got %q", got.Status.IPAddress)
	}

	// 3. joining: no node yet -> RegisteringNode; once kubelet registers on the VPN IP -> Ready.
	reconcileOnce(t, r, "n")
	if got = getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseRegisteringNode {
		t.Fatalf("phase=%q", got.Status.Phase)
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "gcp-n"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.8.0.9"}}},
	}
	if err := r.Create(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, "n")
	got = getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseReady || got.Status.NodeName != "gcp-n" {
		t.Fatalf("phase=%q node=%q", got.Status.Phase, got.Status.NodeName)
	}
	stamped := &corev1.Node{}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "gcp-n"}, stamped); err != nil {
		t.Fatal(err)
	}
	if stamped.Labels[nodeProvisionProviderLabel] != "GCP" || stamped.Labels[nodeProvisionUIDLabel] != string(np.UID) {
		t.Errorf("node ownership labels: %v", stamped.Labels)
	}
	if len(vpn.removed) != 0 {
		t.Error("a healthy node's peer must not be released")
	}
	if stub.provCall != 1 {
		t.Errorf("the instance must be launched exactly once, launches=%d", stub.provCall)
	}
}

func TestGCP_HappyPathWithoutVPNNeverContactsTheVPNServer(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) { return gcpResult(""), nil }}
	stub.waitIn, stub.waitEx = "10.128.0.7", "34.1.2.3"
	// vpnConn deliberately unset: any dial is an "unexpected VPN dial" error.
	r := gcpProvisioningReconciler(t, np, stub)
	vpn := &fakeVPN{}
	vpn.install(r)

	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "n" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Fatalf("phase=%q id=%q msg=%q", got.Status.Phase, got.Status.InstanceID, got.Status.Message)
	}
	if got.Status.VpnIP != "" {
		t.Errorf("no VPN IP without a VPN, got %q", got.Status.VpnIP)
	}
	if len(stub.sawVPN) != 1 || stub.sawVPN[0] || stub.vpnDials != 0 {
		t.Errorf("the VPN server must not be dialled: sawVPN=%v dials=%d", stub.sawVPN, stub.vpnDials)
	}
	if len(getNetConfig(t, r).Status.VPNPeers) != 0 {
		t.Error("no peer may be recorded")
	}

	// The kubelet node IP is the instance's internal IP.
	reconcileOnce(t, r, "n")
	got = getNP(t, r, "n")
	if got.Status.IPAddress != "10.128.0.7" || got.Status.PrivateIP != "10.128.0.7" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseBootstrapping {
		t.Fatalf("status = %+v", got.Status)
	}
	if nodeSSHHost(got) != "10.128.0.7" {
		t.Errorf("the controller must reach the node on its internal IP, got %q", nodeSSHHost(got))
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "gcp-n"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.128.0.7"}}},
	}
	if err := r.Create(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, "n") // RegisteringNode picked up by the next pass
	reconcileOnce(t, r, "n")
	if got = getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseReady {
		t.Fatalf("phase=%q", got.Status.Phase)
	}
	if vpn.dials != 0 || stub.vpnDials != 0 {
		t.Errorf("VPN server contacted without a VPN: dials=%d/%d", vpn.dials, stub.vpnDials)
	}
}

func TestGCP_PhasesAreReportedInOrder(t *testing.T) {
	np := gcpNP("n", nil)
	var phases []mlv1alpha1.NodeProvisionPhase
	stub := &gcpStub{}
	stub.withVPNServer(t)
	r := gcpProvisioningReconciler(t, np, stub)
	stub.provision = func() (*gcpprovision.ProvisionResult, error) {
		phases = append(phases, getNP(t, r, "n").Status.Phase) // what the user sees while the launch runs
		return gcpResult("10.8.0.9"), nil
	}
	reconcileOnce(t, r, "n")
	if len(phases) != 1 || phases[0] != mlv1alpha1.NodeProvisionPhaseCreatingInstance {
		t.Errorf("CreatingInstance must be visible during the launch, saw %v", phases)
	}
}

// ── defaults ────────────────────────────────────────────────────────────────

func TestGCP_DefaultsArePatchedOnceAndNotRecomputed(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Spec.InstanceType = ""
		np.Spec.GCPConfig = nil
		np.Spec.NodeLabel = "cpu"
	})
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) { return gcpResult(""), nil }}
	stub.resolve = func() (*gcpprovision.Resolved, error) {
		return &gcpprovision.Resolved{Region: "us-central1", InstanceType: "e2-standard-4", Config: mlv1alpha1.GCPConfig{
			ProjectID: "proj-1", Zone: "us-central1-b", Network: "default",
			SourceImage: "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v1",
		}}, nil
	}
	r := gcpProvisioningReconciler(t, np, stub)

	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("a patch returns so the spec-change event drives the next pass: res=%+v err=%v", res, err)
	}
	got := getNP(t, r, "n")
	if got.Spec.InstanceType != "e2-standard-4" || got.Spec.GCPConfig == nil || got.Spec.GCPConfig.Zone != "us-central1-b" ||
		got.Spec.GCPConfig.SourceImage == "" || got.Spec.GCPConfig.ProjectID != "proj-1" {
		t.Fatalf("resolved defaults must be persisted in the spec: %+v", got.Spec)
	}
	if stub.provCall != 0 {
		t.Error("nothing may be launched on the pass that patched the spec")
	}

	reconcileOnce(t, r, "n") // second pass: complete -> straight to launch
	if stub.resolveCalls != 1 {
		t.Errorf("a complete spec must not trigger more API lookups, ResolveDefaults calls = %d", stub.resolveCalls)
	}
	if got = getNP(t, r, "n"); got.Status.InstanceID != "n" {
		t.Errorf("instance must be launched on the second pass: %+v", got.Status)
	}
}

func TestGCP_DefaultsErrorAndValidationErrorFailTheNodeProvision(t *testing.T) {
	// defaults resolution error (e.g. no zone offers the machine type)
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.GCPConfig.SourceImage = "" })
	stub := &gcpStub{resolve: func() (*gcpprovision.Resolved, error) {
		return nil, errors.New("machine type e2-standard-4 is not available in any zone of region us-central1")
	}}
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 ||
		!strings.Contains(got.Status.Message, "resolving GCP defaults") || !strings.Contains(got.Status.Message, "not available in any zone") {
		t.Errorf("phase=%q count=%d msg=%q", got.Status.Phase, got.Status.ProvisionRetryCount, got.Status.Message)
	}

	// validation error on a complete spec (zone outside spec.region)
	np = gcpNP("v", func(np *mlv1alpha1.NodeProvision) { np.Spec.Region = "europe-west1" })
	stub = &gcpStub{}
	r = gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "v")
	got = getNP(t, r, "v")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "GCP validation failed") {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}
	if stub.provCall != 0 || stub.finds != 0 {
		t.Error("nothing may reach the cloud after a validation failure")
	}
}

// ── credentials ─────────────────────────────────────────────────────────────

func TestGCP_CredentialErrorFailsNodeProvisionWithoutTouchingTheCloud(t *testing.T) {
	for name, secret := range map[string]*corev1.Secret{
		"no key in the secret": {ObjectMeta: metav1.ObjectMeta{Name: "gcp-creds", Namespace: "default"}, Data: map[string][]byte{"other": []byte("x")}},
		"not a service account": {ObjectMeta: metav1.ObjectMeta{Name: "gcp-creds", Namespace: "default"},
			Data: map[string][]byte{"credentials.json": []byte(`{"type":"external_account"}`)}},
		"garbage": {ObjectMeta: metav1.ObjectMeta{Name: "gcp-creds", Namespace: "default"},
			Data: map[string][]byte{"credentials.json": []byte("TOPSECRET not json")}},
	} {
		np := gcpNP("n", nil)
		stub := &gcpStub{}
		nc := readyNetConfig()
		r := fixReconciler(t, np, nc, secret)
		stub.install(r)

		reconcileOnce(t, r, "n")
		got := getNP(t, r, "n")
		if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 {
			t.Errorf("%s: phase=%q count=%d", name, got.Status.Phase, got.Status.ProvisionRetryCount)
		}
		if !strings.Contains(got.Status.Message, "resolving GCP credentials failed") {
			t.Errorf("%s: message %q", name, got.Status.Message)
		}
		if strings.Contains(got.Status.Message, "TOPSECRET") {
			t.Errorf("%s: the key leaked into the status message: %q", name, got.Status.Message)
		}
		if stub.provCall != 0 || stub.finds != 0 || stub.resolveCalls != 0 {
			t.Errorf("%s: no cloud call may be made without usable credentials", name)
		}
	}
}

func TestGCP_CredentialErrorWhileWaitingFailsToo(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
		np.Status.InstanceID = "n"
		np.Status.LastUpdated = minutesAgo(1)
		np.Spec.CredentialsRef.Key = "wrong-key"
	})
	stub := &gcpStub{}
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, `key "wrong-key" not found`) {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}
	if stub.waits != 0 {
		t.Error("the instance must not be polled without credentials")
	}
}

func TestGCP_CredentialSecretAndRegistryCopiesAreKept(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	nc.Spec.SoftwareConfig.ImagePullSecretRef = &mlv1alpha1.SecretKeyReference{Name: "reg"}
	reg := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "reg", Namespace: "default"}, Data: map[string][]byte{"username": []byte("u"), "password": []byte("p")}}
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) { return gcpResult(""), nil }}
	r := fixReconciler(t, np, nc, gcpCredsSecret(), gcpSSHKeySecret(t, "n"), reg)
	stub.install(r)
	reconcileOnce(t, r, "n")

	for _, name := range []string{"n" + controllerCredsSuffix, "n" + registryCredsSuffix} {
		s := &corev1.Secret{}
		if err := r.Get(context.Background(), client.ObjectKey{Name: name, Namespace: "default"}, s); err != nil {
			t.Errorf("controller-owned copy %s missing: %v", name, err)
			continue
		}
		if len(s.OwnerReferences) != 1 || s.OwnerReferences[0].Kind != "NodeProvision" {
			t.Errorf("%s must be owned by the NodeProvision so it is garbage collected with it: %+v", name, s.OwnerReferences)
		}
	}
	cp := &corev1.Secret{}
	_ = r.Get(context.Background(), client.ObjectKey{Name: "n" + controllerCredsSuffix, Namespace: "default"}, cp)
	if string(cp.Data["credentials.json"]) != gcpSAKey {
		t.Error("the credentials copy must carry the service-account key (teardown falls back to it)")
	}
}

func TestGCP_CredentialsNamespaceMustBeTheNodeProvisionsOwn(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.CredentialsRef.Namespace = "kube-system" })
	stub := &gcpStub{}
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "credentialsRef.namespace") {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}
}

// ── launch outcomes ─────────────────────────────────────────────────────────

func TestGCP_AlreadyLaunchedRequeuesWithoutCountingFailure(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) {
		// A peer allocation reported alongside was released: it must not be recorded.
		return &gcpprovision.ProvisionResult{VpnIP: "10.8.0.9", PublicKey: testWGKey},
			fmt.Errorf("insert: %w", gcpprovision.ErrInstanceAlreadyLaunched)
	}}
	r := gcpProvisioningReconciler(t, np, stub)
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

func TestGCP_AdoptedResultPersistsInstanceAndAnInstanceFoundByLookupIsAdopted(t *testing.T) {
	// (a) ProvisionInstance reports Adopted
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) {
		return &gcpprovision.ProvisionResult{InstanceID: "n", Adopted: true}, nil
	}}
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "n" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Errorf("got %+v", got.Status)
	}
	if len(stub.deletedIDs()) != 0 {
		t.Error("must not delete an adopted instance")
	}

	// (b) the lookup before launch finds an instance (retry after a crash): no launch at all
	np = gcpNP("n2", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub = &gcpStub{found: "n2"}
	r = gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n2")
	got = getNP(t, r, "n2")
	if got.Status.InstanceID != "n2" || stub.provCall != 0 {
		t.Errorf("a retry after a crash must adopt, not launch again: id=%q launches=%d", got.Status.InstanceID, stub.provCall)
	}
	if got.Status.ProvisionRetryCount != 0 {
		t.Error("adoption is not a failure")
	}
}

func TestGCP_InstanceAlreadyRecordedIsNeverLaunchedAgain(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.InstanceID = "n"
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseProvisioning
	})
	stub := &gcpStub{}
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance || stub.provCall != 0 || stub.finds != 0 {
		t.Errorf("phase=%q launches=%d finds=%d", got.Status.Phase, stub.provCall, stub.finds)
	}
}

func TestGCP_LaunchFailureIsCountedAndTheVPNPeerIsKeptForRelease(t *testing.T) {
	np := gcpNP("n", nil)
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) {
		return gcpResult("10.8.0.9"), errors.New("quota exceeded")
	}}
	stub.withVPNServer(t)
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 ||
		!strings.Contains(got.Status.Message, "GCE provisioning failed") {
		t.Errorf("phase=%q count=%d msg=%q", got.Status.Phase, got.Status.ProvisionRetryCount, got.Status.Message)
	}
	if got.Status.VpnIP != "10.8.0.9" {
		t.Errorf("the peer identity must be persisted with the failure so the retry can release it, got %q", got.Status.VpnIP)
	}
	if peers := getNetConfig(t, r).Status.VPNPeers; len(peers) != 1 {
		t.Errorf("the peer must be recorded in the NetConfig, got %+v", peers)
	}
}

func TestGCP_VPNServerUnreachableFailsBeforeLaunching(t *testing.T) {
	np := gcpNP("n", nil)
	stub := &gcpStub{vpnErr: errors.New("dial tcp: i/o timeout")}
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "connecting to VPN server") {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}
	if stub.provCall != 0 {
		t.Error("no instance may be launched when the VPN server cannot be reached")
	}
}

func TestGCP_LookupErrorFailsTheAttempt(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub := &gcpStub{findErr: errors.New("backend unavailable")}
	r := gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "looking up existing GCE instance") {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}
	if stub.provCall != 0 {
		t.Error("must not launch when the existing-instance check failed")
	}
}

// ── crash recovery (CreatingInstance / ConfiguringVPN without an InstanceID) ──

func gcpCreatingNP(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return gcpNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseCreatingInstance
		np.Status.LastUpdated = minutesAgo(1)
		if mut != nil {
			mut(np)
		}
	})
}

func TestGCP_CrashBetweenInsertAndStatusWrite_AdoptsAndKeepsPeer(t *testing.T) {
	np := gcpCreatingNP(nil) // VPN mode
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	stub := &gcpStub{found: "n"}
	stub.install(r)
	vpn := &fakeVPN{}
	vpn.install(r)

	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "n" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
		t.Fatalf("instance must be adopted, got phase=%q id=%q", got.Status.Phase, got.Status.InstanceID)
	}
	if got.Status.VpnIP != "10.8.0.7" || got.Status.ProvisionRetryCount != 0 {
		t.Errorf("recorded peer must be recovered without counting a failure: %+v", got.Status)
	}
	if len(stub.deletedIDs()) != 0 || len(vpn.removed) != 0 || vpn.dials != 0 {
		t.Errorf("nothing may be deleted/released: deleted=%v removed=%v", stub.deletedIDs(), vpn.removed)
	}
}

func TestGCP_CrashRecovery_NoVPNAdoptsEvenPastTheStallTimeout(t *testing.T) {
	np := gcpCreatingNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.LastUpdated = minutesAgo(int(creatingInstanceStallTimeout/time.Minute) + 5)
	})
	r := fixReconciler(t, np, gcpCredsSecret())
	(&gcpStub{found: "n"}).install(r)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "n" || got.Status.ProvisionRetryCount != 0 {
		t.Errorf("adoption must win over the stall timeout: %+v", got.Status)
	}
}

func TestGCP_CrashRecovery_VPNModeWithoutRecordedPeerDeletesAndFailsForRetry(t *testing.T) {
	np := gcpCreatingNP(nil)
	r := fixReconciler(t, np, readyNetConfig(), gcpCredsSecret()) // no peer recorded
	stub := &gcpStub{found: "n"}
	stub.install(r)
	reconcileOnce(t, r, "n")
	if ids := stub.deletedIDs(); len(ids) != 1 || ids[0] != "n" {
		t.Fatalf("an instance that can never join must be deleted exactly once, got %v", ids)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 || got.Status.InstanceID != "" {
		t.Errorf("attempt must fail for a clean retry: %+v", got.Status)
	}
}

func TestGCP_CreatingInstanceNothingLaunchedHonoursTheStallTimeout(t *testing.T) {
	stalled := gcpCreatingNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.LastUpdated = minutesAgo(int(creatingInstanceStallTimeout/time.Minute) + 5)
	})
	r := fixReconciler(t, stalled, gcpCredsSecret())
	(&gcpStub{}).install(r)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "cloud instance") {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}

	fresh := gcpCreatingNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	r = fixReconciler(t, fresh, gcpCredsSecret())
	(&gcpStub{}).install(r)
	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("a recent stall just requeues: %+v %v", res, err)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseCreatingInstance {
		t.Errorf("phase=%q", got.Status.Phase)
	}
}

// ── WaitingForInstance ──────────────────────────────────────────────────────

func gcpWaitingNP(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return gcpNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
		np.Status.InstanceID = "n"
		np.Status.VpnIP = "10.8.0.9"
		np.Status.IPAddress = "10.8.0.9"
		np.Status.LastUpdated = minutesAgo(1)
		if mut != nil {
			mut(np)
		}
	})
}

func TestGCP_TerminatedInstanceFailsTheNodeProvision(t *testing.T) {
	for _, state := range []string{"TERMINATED", "STOPPING", "SUSPENDED"} {
		np := gcpWaitingNP(nil)
		stub := &gcpStub{waitErr: fmt.Errorf("instance n entered terminal state %q and will never become running", state)}
		r := gcpProvisioningReconciler(t, np, stub)
		res, err := r.Reconcile(context.Background(), reqFor("n"))
		if err != nil || res.RequeueAfter != requeueFailed {
			t.Fatalf("%s: res=%+v err=%v", state, res, err)
		}
		got := getNP(t, r, "n")
		if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || got.Status.ProvisionRetryCount != 1 ||
			!strings.Contains(got.Status.Message, state) || !strings.Contains(got.Status.Message, "polling instance state") {
			t.Errorf("%s: phase=%q count=%d msg=%q", state, got.Status.Phase, got.Status.ProvisionRetryCount, got.Status.Message)
		}
		if got.Status.InstanceID != "n" {
			t.Errorf("%s: the instance id must stay so the retry teardown can delete it", state)
		}
	}
}

func TestGCP_ProvisioningStateTransientErrorsRequeueButStallTimeoutFails(t *testing.T) {
	// A transient API error is retried; it does not fail the node.
	np := gcpWaitingNP(nil)
	stub := &gcpStub{waitErr: fmt.Errorf("describing instance n: %w", &googleapi.Error{Code: 503, Message: "backend unavailable"})}
	r := gcpProvisioningReconciler(t, np, stub)
	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter != requeueShort {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance || got.Status.ProvisionRetryCount != 0 {
		t.Errorf("phase=%q count=%d", got.Status.Phase, got.Status.ProvisionRetryCount)
	}

	// Not running yet: requeue.
	np = gcpWaitingNP(nil)
	stub = &gcpStub{}
	r = gcpProvisioningReconciler(t, np, stub)
	if res, err := r.Reconcile(context.Background(), reqFor("n")); err != nil || res.RequeueAfter != requeueShort {
		t.Fatalf("res=%+v err=%v", res, err)
	}

	// Stuck for longer than waitingForInstanceStallTimeout: fail for retry without polling.
	np = gcpWaitingNP(func(np *mlv1alpha1.NodeProvision) {
		np.Status.LastUpdated = minutesAgo(int(waitingForInstanceStallTimeout/time.Minute) + 5)
	})
	stub = &gcpStub{}
	r = gcpProvisioningReconciler(t, np, stub)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "did not become running") {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}
}

func TestGCP_WaitingRecordsAddressesConflictSafely(t *testing.T) {
	// The status write goes through a fresh read + retry, so a stale in-memory
	// object (concurrent write) must not make it fail.
	np := gcpWaitingNP(nil)
	stub := &gcpStub{waitIn: "10.128.0.7", waitEx: ""}
	r := gcpProvisioningReconciler(t, np, stub)

	// Bump the stored object behind the reconciler's back (message change).
	fresh := getNP(t, r, "n")
	fresh.Status.Message = "changed by someone else"
	if err := r.Status().Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	stale := np.DeepCopy() // resourceVersion is behind now
	stale.ResourceVersion = "1"
	if _, err := r.reconcileGCPWaitingForInstance(context.Background(), stale, gcpCredsSecret()); err != nil {
		t.Fatalf("a stale object must not break the status write: %v", err)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseBootstrapping || got.Status.PrivateIP != "10.128.0.7" || got.Status.PublicIP != "" {
		t.Errorf("status = %+v", got.Status)
	}
}

// ── SSH key secret ──────────────────────────────────────────────────────────

func TestGCP_EnsureSSHKeyGeneratesOnceAndReusesTheSecret(t *testing.T) {
	np := gcpNP("n", nil)
	r := fixReconciler(t, np)
	pub1, err := r.ensureGCPSSHKey(context.Background(), np)
	if err != nil {
		t.Fatal(err)
	}
	s := &corev1.Secret{}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "n-ssh-key", Namespace: "default"}, s); err != nil {
		t.Fatalf("the key Secret must be created with the AWS layout: %v", err)
	}
	if s.Type != corev1.SecretTypeSSHAuth || !strings.Contains(string(s.Data["ssh-privatekey"]), "BEGIN RSA PRIVATE KEY") {
		t.Errorf("secret layout: type=%q", s.Type)
	}
	if len(s.OwnerReferences) != 1 || s.OwnerReferences[0].Name != "n" || s.Labels["ml.dcn.ssu.ac.kr/node"] != "n" {
		t.Errorf("owner/labels: %+v %v", s.OwnerReferences, s.Labels)
	}
	if len(pub1) == 0 || strings.Contains(string(pub1), "PRIVATE") {
		t.Error("a public key must be returned")
	}
	pub2, err := r.ensureGCPSSHKey(context.Background(), np)
	if err != nil || string(pub1) != string(pub2) {
		t.Errorf("a second call must reuse the stored key (err=%v)", err)
	}
}

func TestGCP_EnsureSSHKeyErrors(t *testing.T) {
	np := gcpNP("n", nil)
	bad := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "n-ssh-key", Namespace: "default"}, Data: map[string][]byte{"ssh-privatekey": []byte("not a pem")}}
	if _, err := fixReconciler(t, np, bad).ensureGCPSSHKey(context.Background(), np); err == nil {
		t.Error("an unparseable stored key must be an error (never silently replaced)")
	}
	empty := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "n-ssh-key", Namespace: "default"}}
	if _, err := fixReconciler(t, np, empty).ensureGCPSSHKey(context.Background(), np); err == nil {
		t.Error("a Secret without ssh-privatekey must be an error")
	}
	// A bad key fails the provisioning attempt (and nothing is launched).
	stub := &gcpStub{}
	np = gcpNP("k", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	r := fixReconciler(t, np, nc, gcpCredsSecret(),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "k-ssh-key", Namespace: "default"}, Data: map[string][]byte{"ssh-privatekey": []byte("junk")}})
	stub.install(r)
	reconcileOnce(t, r, "k")
	if got := getNP(t, r, "k"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "preparing SSH key") || stub.provCall != 0 {
		t.Errorf("phase=%q msg=%q launches=%d", got.Status.Phase, got.Status.Message, stub.provCall)
	}
}

func TestGCP_SSHClientUsesTheNodeKeySecret(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true; np.Status.IPAddress = "127.0.0.1" })
	r := fixReconciler(t, np)
	if _, err := r.getSSHClientByProvider(context.Background(), np); err == nil || !strings.Contains(err.Error(), "GCP SSH key secret") {
		t.Errorf("a GCP node must look for its <name>-ssh-key Secret, got %v", err)
	}
	// With the Secret present the lookup succeeds and the failure is the (expected) dial.
	r = fixReconciler(t, np, gcpSSHKeySecret(t, "n"))
	_, err := r.getSSHClientByProvider(context.Background(), np)
	if err == nil || strings.Contains(err.Error(), "SSH key secret") {
		t.Errorf("secret found, so the error must come from dialling: %v", err)
	}
}

// ── deletion ────────────────────────────────────────────────────────────────

func deletingGCPNP(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return gcpNP("n", func(np *mlv1alpha1.NodeProvision) {
		now := metav1.Now()
		np.DeletionTimestamp = &now
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseReady
		if mut != nil {
			mut(np)
		}
	})
}

func npGone(t *testing.T, r *NodeProvisionReconciler) bool {
	t.Helper()
	err := r.Get(context.Background(), client.ObjectKey{Name: "n", Namespace: "default"}, &mlv1alpha1.NodeProvision{})
	return apierrors.IsNotFound(err)
}

func TestGCP_DeleteRemovesInstanceVPNPeerKeySecretAndFinalizer(t *testing.T) {
	np := deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Status.InstanceID = "n"
		np.Status.VpnIP = "10.8.0.7"
		np.Status.IPAddress = "10.8.0.7"
	})
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, gcpCredsSecret(), gcpSSHKeySecret(t, "n"))
	stub := &gcpStub{}
	stub.install(r)
	vpn := &fakeVPN{}
	vpn.install(r)

	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if ids := stub.deletedIDs(); len(ids) != 1 || ids[0] != "n" {
		t.Errorf("the instance must be deleted exactly once, got %v", ids)
	}
	if !npGone(t, r) {
		t.Error("the finalizer must be removed once everything is cleaned up")
	}
	if len(vpn.removed) != 1 || vpn.removed[0] != testWGKey {
		t.Errorf("the VPN peer must be released, got %v", vpn.removed)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "n-ssh-key", Namespace: "default"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("the node's SSH key Secret must be deleted, err=%v", err)
	}
}

func TestGCP_DeleteIsIdempotentAndRetriesAfterAFailure(t *testing.T) {
	np := deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.InstanceID = "n"
	})
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	stub := &gcpStub{deleteErr: errors.New("backend unavailable")}
	stub.install(r)

	// Deletion fails: the finalizer, status and secrets stay; the retry is delayed.
	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter != 30*time.Second {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if npGone(t, r) {
		t.Fatal("the finalizer must stay while the instance may still exist")
	}
	if got := getNP(t, r, "n"); got.Status.InstanceID != "n" {
		t.Error("the instance id must be kept for the retry")
	}

	// The API recovers: deleted, id cleared, finalizer removed.
	stub.deleteErr = nil
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if !npGone(t, r) {
		t.Fatal("must be gone after a successful retry")
	}
	if ids := stub.deletedIDs(); len(ids) != 1 {
		t.Errorf("exactly one successful delete, got %v", ids)
	}
	// A duplicate delete event for the already-finalized object is a no-op.
	if res, err := r.Reconcile(context.Background(), reqFor("n")); err != nil || res.RequeueAfter != 0 {
		t.Errorf("reconcile of a deleted object: %+v %v", res, err)
	}
}

func TestGCP_DeleteFallsBackToTheControllerCredentialCopy(t *testing.T) {
	np := deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.InstanceID = "n"
	})
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	// The user's Secret is gone; only the controller-owned copy remains.
	cp := gcpCredsSecret()
	cp.Name = "n" + controllerCredsSuffix
	r := fixReconciler(t, np, nc, cp)
	stub := &gcpStub{}
	stub.install(r)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if ids := stub.deletedIDs(); len(ids) != 1 || !npGone(t, r) {
		t.Errorf("teardown must still work from the copy: deleted=%v gone=%v", ids, npGone(t, r))
	}
}

func TestGCP_DeleteWithoutAnyCredentialsKeepsTheFinalizerWhenAnInstanceIsRecorded(t *testing.T) {
	np := deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.InstanceID = "n"
	})
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	r := fixReconciler(t, np, nc) // no user secret, no copy
	stub := &gcpStub{}
	stub.install(r)
	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter != 30*time.Second || npGone(t, r) || len(stub.deletedIDs()) != 0 {
		t.Errorf("an instance must never be orphaned silently: res=%+v err=%v gone=%v", res, err, npGone(t, r))
	}
}

func TestGCP_DeleteWithoutInstanceIDSweepsAnOrphanAndFirewalls(t *testing.T) {
	// crash between the insert and the status write, then the user deletes the CR
	np := deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseCreatingInstance
	})
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	stub := &gcpStub{found: "n"}
	stub.install(r)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if ids := stub.deletedIDs(); len(ids) != 1 || ids[0] != "n" || !npGone(t, r) {
		t.Errorf("the orphaned instance must be found by name+UID and deleted: %v gone=%v", ids, npGone(t, r))
	}

	// nothing was launched, but the failed attempt may have left firewall rules: the
	// cleanup call still goes out (with an empty instance id).
	np = deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
	})
	r = fixReconciler(t, np, nc, gcpCredsSecret())
	stub = &gcpStub{}
	stub.install(r)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if ids := stub.deletedIDs(); len(ids) != 1 || ids[0] != "" || !npGone(t, r) {
		t.Errorf("firewall-only cleanup expected: %q gone=%v", ids, npGone(t, r))
	}
}

func TestGCP_DeleteOrphanSweepGivesUpAfterTheGraceAndSkipsUnresolvedSpecs(t *testing.T) {
	// lookup keeps failing, within the grace period: wait
	np := deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseCreatingInstance
	})
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	(&gcpStub{findErr: errors.New("boom")}).install(r)
	res, err := r.Reconcile(context.Background(), reqFor("n"))
	if err != nil || res.RequeueAfter != 30*time.Second || npGone(t, r) {
		t.Fatalf("res=%+v err=%v gone=%v", res, err, npGone(t, r))
	}

	// ... but not forever
	old := metav1.NewTime(time.Now().Add(-deletionGiveUpAfter - time.Minute))
	np = deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseCreatingInstance
		np.DeletionTimestamp = &old
	})
	r = fixReconciler(t, np, nc, gcpCredsSecret())
	(&gcpStub{findErr: errors.New("boom")}).install(r)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if !npGone(t, r) {
		t.Error("after the grace period the stuck lookup must be abandoned so the CR can go")
	}

	// A spec whose defaults never resolved cannot own anything: no cloud call at all.
	np = deletingGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Spec.GCPConfig = nil
		np.Status.Phase = mlv1alpha1.NodeProvisionPhasePending
	})
	r = fixReconciler(t, np, nc, gcpCredsSecret())
	stub := &gcpStub{}
	stub.install(r)
	if _, err := r.Reconcile(context.Background(), reqFor("n")); err != nil {
		t.Fatal(err)
	}
	if stub.finds != 0 || len(stub.deletedIDs()) != 0 || !npGone(t, r) {
		t.Errorf("finds=%d deleted=%v gone=%v", stub.finds, stub.deletedIDs(), npGone(t, r))
	}
}

// ── retry / terminal teardown ───────────────────────────────────────────────

func failedGCPNP(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	return gcpNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseFailed
		np.Status.ProvisionRetryCount = 1
		np.Status.LastUpdated = minutesAgo(5)
		if mut != nil {
			mut(np)
		}
	})
}

func TestGCP_ReconcileFailedDeletesInstanceRemovesNodeReleasesPeerAndResets(t *testing.T) {
	np := failedGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Status.InstanceID = "n"
		np.Status.NodeName = "worker-a"
		np.Status.VpnIP = "10.8.0.7"
		np.Status.IPAddress = "10.8.0.7"
		np.Status.PrivateIP = "10.128.0.5"
		np.Status.PublicIP = "3.3.3.3"
	})
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "worker-a", Finalizers: []string{nodeProvisionNodeFinalizer},
		Labels: map[string]string{nodeProvisionUIDLabel: string(np.UID)},
	}}
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, node, gcpCredsSecret())
	stub := &gcpStub{}
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
	if ids := stub.deletedIDs(); len(ids) != 1 || ids[0] != "n" {
		t.Errorf("instance must be deleted exactly once, got %v", ids)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Name: "worker-a"}, &corev1.Node{}); !apierrors.IsNotFound(err) {
		t.Errorf("the joined node must be removed, err=%v", err)
	}
	if len(vpn.removed) != 1 || vpn.removed[0] != testWGKey {
		t.Errorf("peer must be removed from the VPN server, got %v", vpn.removed)
	}
	s := got.Status
	if s.InstanceID != "" || s.VpnIP != "" || s.IPAddress != "" || s.NodeName != "" || s.PublicIP != "" || s.PrivateIP != "" {
		t.Errorf("stale identity kept after teardown: %+v", s)
	}
	if s.ProvisionRetryCount != 1 {
		t.Errorf("the retry count must survive the reset, got %d", s.ProvisionRetryCount)
	}
}

func TestGCP_ReconcileFailedDeleteErrorLeavesStatusAndPeerAlone(t *testing.T) {
	np := failedGCPNP(func(np *mlv1alpha1.NodeProvision) { np.Status.InstanceID = "n"; np.Status.VpnIP = "10.8.0.7" })
	r := fixReconciler(t, np, netConfigWithPeer("n", "10.8.0.7", testWGKey), gcpCredsSecret())
	(&gcpStub{deleteErr: errors.New("boom")}).install(r)
	vpn := &fakeVPN{}
	vpn.install(r)
	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if got := getNP(t, r, "n"); got.Status.InstanceID != "n" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("got %+v", got.Status)
	}
	if len(vpn.removed) != 0 {
		t.Error("the peer must stay until the instance is gone")
	}
}

func TestGCP_ReconcileFailedUnrecordedInstanceIsAdoptedBeforeItsPeerIsReleased(t *testing.T) {
	np := failedGCPNP(nil) // VPN mode, InstanceID never recorded
	nc := netConfigWithPeer("n", "10.8.0.7", testWGKey)
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	stub := &gcpStub{found: "n"}
	stub.install(r)
	vpn := &fakeVPN{}
	vpn.install(r)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if got.Status.InstanceID != "n" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance || got.Status.VpnIP != "10.8.0.7" {
		t.Errorf("instance must be adopted with its peer, got %+v", got.Status)
	}
	if len(stub.deletedIDs()) != 0 || len(vpn.removed) != 0 {
		t.Error("nothing may be deleted or released")
	}
}

func TestGCP_ReconcileFailedNoInstanceStillCleansFirewallsAndResets(t *testing.T) {
	np := failedGCPNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	stub := &gcpStub{}
	stub.install(r)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if ids := stub.deletedIDs(); len(ids) != 1 || ids[0] != "" {
		t.Errorf("firewall rules of the failed attempt must be removed, got %q", ids)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != "" {
		t.Errorf("status must be reset, got %+v", got.Status)
	}
	// Without credentials the retry is not blocked on that cleanup.
	np = failedGCPNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	r = fixReconciler(t, np, nc)
	(&gcpStub{}).install(r)
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != "" {
		t.Errorf("a missing credential Secret must not block the retry, got %+v", got.Status)
	}
}

func TestGCP_TerminalFailureTeardownRunsOnceAndIsRecorded(t *testing.T) {
	np := failedGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.ProvisionRetryCount = maxProvisionRetries
		np.Status.InstanceID = "n"
		np.Status.Message = "provisioning failed"
	})
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	stub := &gcpStub{}
	stub.install(r)

	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	got := getNP(t, r, "n")
	if !strings.Contains(got.Status.Message, terminalTeardownMarker) || !strings.Contains(got.Status.Message, "instance n deleted") {
		t.Errorf("the release must be recorded: %q", got.Status.Message)
	}
	if got.Status.InstanceID != "" || got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("terminal Failed stays Failed with a clean identity: %+v", got.Status)
	}
	if _, err := r.reconcileFailed(context.Background(), getNP(t, r, "n")); err != nil {
		t.Fatal(err)
	}
	if ids := stub.deletedIDs(); len(ids) != 1 {
		t.Errorf("teardown must run once, deleted %v", ids)
	}

	// A terminal failure whose delete keeps failing is retried without being recorded.
	np = failedGCPNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Status.ProvisionRetryCount = maxProvisionRetries
		np.Status.InstanceID = "n"
	})
	r = fixReconciler(t, np, nc, gcpCredsSecret())
	(&gcpStub{deleteErr: errors.New("boom")}).install(r)
	res, err := r.reconcileFailed(context.Background(), getNP(t, r, "n"))
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if got := getNP(t, r, "n"); strings.Contains(got.Status.Message, terminalTeardownMarker) || got.Status.InstanceID != "n" {
		t.Errorf("a failed teardown must not be recorded: %+v", got.Status)
	}
}

// ── dispatch ────────────────────────────────────────────────────────────────

func TestGCP_NoLongerReportedAsUnsupported(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	stub := &gcpStub{provision: func() (*gcpprovision.ProvisionResult, error) { return gcpResult(""), nil }}
	r := gcpProvisioningReconciler(t, np, stub)
	if _, err := r.reconcileProvisioning(context.Background(), np, gcpCredsSecret()); err != nil {
		t.Fatalf("GCP must be dispatched to the GCP path: %v", err)
	}
	if stub.provCall != 1 {
		t.Errorf("launches = %d", stub.provCall)
	}
}

func TestGCP_VPNModeMismatchWithTheClusterIsRejectedLikeAWS(t *testing.T) {
	np := gcpNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	nc := readyNetConfig() // the cluster runs with the VPN
	stub := &gcpStub{}
	r := fixReconciler(t, np, nc, gcpCredsSecret())
	stub.install(r)
	reconcileOnce(t, r, "n")
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "disableVPN") {
		t.Errorf("phase=%q msg=%q", got.Status.Phase, got.Status.Message)
	}
	if stub.provCall != 0 || stub.resolveCalls != 0 {
		t.Error("nothing may reach GCP")
	}
}

// ── samples ─────────────────────────────────────────────────────────────────

// The shipped sample must decode strictly into the API types (so a renamed or
// removed field breaks the build of the docs, not a user's apply).
func TestGCP_SampleManifestsDecodeStrictly(t *testing.T) {
	raw, err := os.ReadFile("../../../config/samples/ml_v1alpha1_nodeprovision_gcp.yaml")
	if os.IsNotExist(err) {
		t.Skip("config/samples is not part of this build context")
	}
	if err != nil {
		t.Fatal(err)
	}
	var vpn, novpn *mlv1alpha1.NodeProvision
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		if !strings.Contains(doc, "kind: NodeProvision\n") {
			if strings.Contains(doc, "kind: Secret") {
				var sec corev1.Secret
				if err := yaml.UnmarshalStrict([]byte(doc), &sec); err != nil {
					t.Fatalf("secret: %v", err)
				}
				// the embedded key must be a well-formed service-account document
				if _, err := gcpprovision.ParseServiceAccountKey([]byte(sec.StringData["credentials.json"])); err != nil {
					t.Errorf("sample key: %v", err)
				}
			}
			continue
		}
		np := &mlv1alpha1.NodeProvision{}
		if err := yaml.UnmarshalStrict([]byte(doc), np); err != nil {
			t.Fatalf("NodeProvision sample does not decode strictly: %v\n%s", err, doc)
		}
		if np.Spec.Provider != mlv1alpha1.CloudProviderGCP {
			t.Errorf("%s: provider %q", np.Name, np.Spec.Provider)
		}
		if np.Spec.DisableVPN {
			novpn = np
		} else {
			vpn = np
		}
	}
	if vpn == nil || novpn == nil {
		t.Fatal("the sample must show both a VPN and a VPN-less NodeProvision")
	}
	if novpn.Spec.GCPConfig == nil || novpn.Spec.GCPConfig.Accelerator == nil || len(novpn.Spec.GCPConfig.FirewallSourceRanges) == 0 {
		t.Errorf("VPN-less sample lost its gcpConfig: %+v", novpn.Spec.GCPConfig)
	}
	// Both samples are sensible specs: their structural part passes validation once
	// the fields the controller resolves (project, zone, image) are filled in.
	for _, np := range []*mlv1alpha1.NodeProvision{vpn, novpn} {
		spec := *np.Spec.DeepCopy()
		if spec.InstanceType == "" {
			spec.InstanceType = gcpprovision.DefaultMachineTypeForLabel(spec.NodeLabel)
		}
		if spec.GCPConfig == nil {
			spec.GCPConfig = &mlv1alpha1.GCPConfig{}
		}
		spec.GCPConfig.ProjectID = "p"
		if spec.GCPConfig.Zone == "" {
			spec.GCPConfig.Zone = spec.Region + "-a"
		}
		if err := gcpprovision.ValidateGCPConfig(spec); err != nil {
			t.Errorf("%s: %v", np.Name, err)
		}
	}
}
