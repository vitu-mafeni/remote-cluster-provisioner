package gcp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

const testUID = "11111111-2222-3333-4444-555555555555"

// gerr builds the error the REST client returns for an HTTP status.
func gerr(code int, reason, msg string) error {
	return &googleapi.Error{Code: code, Message: msg, Errors: []googleapi.ErrorItem{{Reason: reason, Message: msg}}}
}

// fakeCompute is an in-memory Compute Engine implementing computeAPI.
type fakeCompute struct {
	mu sync.Mutex

	instances map[string]*computepb.Instance // key zone/name
	firewalls map[string]*computepb.Firewall
	networks  map[string]*computepb.Network
	subnets   map[string]*computepb.Subnetwork // key project/region/name
	zones     []*computepb.Zone
	machines  map[string]bool // zone/type available
	accels    map[string]bool // zone/type available
	images    map[string]*computepb.Image

	// error injection (nil = succeed)
	getInstanceErr    error
	insertInstanceErr error
	deleteInstanceErr error
	getFirewallErr    error
	insertFirewallErr error
	deleteFirewallErr error
	listZonesErr      error

	// call records
	inserted        []*computepb.Instance
	insertRequestID []string
	deletedInst     []string
	insertedFW      []*computepb.Firewall
	deletedFW       []string
	getInstanceN    int
	closed          int
	projects        []string // project of each call, to prove the right one is used
}

func newFakeCompute() *fakeCompute {
	return &fakeCompute{
		instances: map[string]*computepb.Instance{},
		firewalls: map[string]*computepb.Firewall{},
		networks: map[string]*computepb.Network{
			"default": {Name: proto.String("default"), AutoCreateSubnetworks: proto.Bool(true)},
		},
		subnets:  map[string]*computepb.Subnetwork{},
		machines: map[string]bool{},
		accels:   map[string]bool{},
		images: map[string]*computepb.Image{
			DefaultImageProject + "/" + DefaultImageFamily: {
				Name:         proto.String("ubuntu-2204-jammy-v20260101"),
				SelfLink:     proto.String("https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20260101"),
				Architecture: proto.String("X86_64"),
			},
		},
	}
}

// useFake installs f as the package's client factory for the test.
func useFake(t *testing.T, f *fakeCompute) {
	t.Helper()
	old := newClient
	newClient = func(context.Context, Credentials) (computeAPI, error) { return f, nil }
	t.Cleanup(func() { newClient = old })
}

func (f *fakeCompute) rec(project string) { f.projects = append(f.projects, project) }

func (f *fakeCompute) GetInstance(_ context.Context, project, zone, name string) (*computepb.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rec(project)
	f.getInstanceN++
	if f.getInstanceErr != nil {
		return nil, f.getInstanceErr
	}
	if inst, ok := f.instances[zone+"/"+name]; ok {
		return inst, nil
	}
	return nil, gerr(404, "notFound", fmt.Sprintf("The resource 'projects/%s/zones/%s/instances/%s' was not found", project, zone, name))
}

func (f *fakeCompute) InsertInstance(_ context.Context, project, zone, requestID string, inst *computepb.Instance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rec(project)
	f.inserted = append(f.inserted, inst)
	f.insertRequestID = append(f.insertRequestID, requestID)
	if f.insertInstanceErr != nil {
		return f.insertInstanceErr
	}
	key := zone + "/" + inst.GetName()
	if _, ok := f.instances[key]; ok {
		return gerr(409, "alreadyExists", "The resource already exists")
	}
	f.instances[key] = proto.Clone(inst).(*computepb.Instance)
	return nil
}

func (f *fakeCompute) DeleteInstance(_ context.Context, project, zone, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rec(project)
	f.deletedInst = append(f.deletedInst, name)
	if f.deleteInstanceErr != nil {
		return f.deleteInstanceErr
	}
	key := zone + "/" + name
	if _, ok := f.instances[key]; !ok {
		return gerr(404, "notFound", "not found")
	}
	delete(f.instances, key)
	return nil
}

func (f *fakeCompute) GetFirewall(_ context.Context, project, name string) (*computepb.Firewall, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rec(project)
	if f.getFirewallErr != nil {
		return nil, f.getFirewallErr
	}
	if fw, ok := f.firewalls[name]; ok {
		return fw, nil
	}
	return nil, gerr(404, "notFound", "firewall not found")
}

func (f *fakeCompute) InsertFirewall(_ context.Context, project string, fw *computepb.Firewall) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rec(project)
	f.insertedFW = append(f.insertedFW, fw)
	if f.insertFirewallErr != nil {
		return f.insertFirewallErr
	}
	if _, ok := f.firewalls[fw.GetName()]; ok {
		return gerr(409, "alreadyExists", "exists")
	}
	f.firewalls[fw.GetName()] = proto.Clone(fw).(*computepb.Firewall)
	return nil
}

func (f *fakeCompute) DeleteFirewall(_ context.Context, project, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rec(project)
	f.deletedFW = append(f.deletedFW, name)
	if f.deleteFirewallErr != nil {
		return f.deleteFirewallErr
	}
	if _, ok := f.firewalls[name]; !ok {
		return gerr(404, "notFound", "firewall not found")
	}
	delete(f.firewalls, name)
	return nil
}

func (f *fakeCompute) GetNetwork(_ context.Context, _, name string) (*computepb.Network, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n, ok := f.networks[name]; ok {
		return n, nil
	}
	return nil, gerr(404, "notFound", "network not found")
}

func (f *fakeCompute) GetSubnetwork(_ context.Context, project, region, name string) (*computepb.Subnetwork, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sn, ok := f.subnets[project+"/"+region+"/"+name]; ok {
		return sn, nil
	}
	return nil, gerr(404, "notFound", "subnetwork not found")
}

func (f *fakeCompute) ListZones(_ context.Context, _, region string) ([]*computepb.Zone, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listZonesErr != nil {
		return nil, f.listZonesErr
	}
	var out []*computepb.Zone
	for _, z := range f.zones {
		if strings.HasPrefix(z.GetName(), region+"-") {
			out = append(out, z)
		}
	}
	return out, nil
}

func (f *fakeCompute) GetMachineType(_ context.Context, _, zone, name string) (*computepb.MachineType, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.machines[zone+"/"+name] {
		return &computepb.MachineType{Name: proto.String(name)}, nil
	}
	return nil, gerr(404, "notFound", "machine type not found")
}

func (f *fakeCompute) GetAcceleratorType(_ context.Context, _, zone, name string) (*computepb.AcceleratorType, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.accels[zone+"/"+name] {
		return &computepb.AcceleratorType{Name: proto.String(name)}, nil
	}
	return nil, gerr(404, "notFound", "accelerator type not found")
}

func (f *fakeCompute) GetImageFromFamily(_ context.Context, project, family string) (*computepb.Image, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if img, ok := f.images[project+"/"+family]; ok {
		return img, nil
	}
	return nil, gerr(404, "notFound", "image family not found")
}

func (f *fakeCompute) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

// ── fixtures ─────────────────────────────────────────────────────────────────

func zoneUp(name string) *computepb.Zone {
	return &computepb.Zone{Name: proto.String(name), Status: proto.String("UP")}
}

// testNP is a fully resolved GCP NodeProvision (defaults already applied).
func testNP() *mlv1alpha1.NodeProvision {
	return &mlv1alpha1.NodeProvision{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: "ns", UID: k8stypes.UID(testUID)},
		Spec: mlv1alpha1.NodeProvisionSpec{
			Provider: mlv1alpha1.CloudProviderGCP, Region: "us-central1", InstanceType: "e2-standard-4",
			GCPConfig: &mlv1alpha1.GCPConfig{
				ProjectID: "proj-1", Zone: "us-central1-a", Network: "default",
				SourceImage: "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20260101",
			},
		},
	}
}

func testCreds() Credentials {
	return Credentials{JSON: []byte(`{"type":"service_account"}`), ProjectID: "proj-1", ClientEmail: "sa@proj-1.iam.gserviceaccount.com"}
}

// ownedInstance is an instance labelled as launched for np, with the given status.
func ownedInstance(np *mlv1alpha1.NodeProvision, status string) *computepb.Instance {
	return &computepb.Instance{
		Name:   proto.String(InstanceName(np)),
		Status: proto.String(status),
		Labels: ControllerLabels(np),
		NetworkInterfaces: []*computepb.NetworkInterface{{
			NetworkIP:     proto.String("10.128.0.7"),
			AccessConfigs: []*computepb.AccessConfig{{NatIP: proto.String("34.1.2.3")}},
		}},
	}
}

func (f *fakeCompute) put(zone string, inst *computepb.Instance) {
	f.instances[zone+"/"+inst.GetName()] = inst
}

func metaValue(inst *computepb.Instance, key string) (string, bool) {
	for _, it := range inst.GetMetadata().GetItems() {
		if it.GetKey() == key {
			return it.GetValue(), true
		}
	}
	return "", false
}

// ── provisioning environment: fake GCE + fake VPN server over SSH ────────────

const (
	wgKeyServer = "SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS="
	wgConfBase  = "[Interface]\nAddress = 10.8.0.1/24\nListenPort = 51820\nPostUp = wg set %i private-key /etc/wireguard/k\n"
)

type provEnv struct {
	*sshtest.Stubs
	client *sshhelper.Client
	conf   string
	gce    *fakeCompute
	np     *mlv1alpha1.NodeProvision
	nc     *mlv1alpha1.NodeProvisionNetConfig
	pubKey []byte
}

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

	nc := newProvEnvNetConfig()

	_, pub, err := testKey()
	if err != nil {
		t.Fatal(err)
	}
	gce := newFakeCompute()
	useFake(t, gce)
	return &provEnv{Stubs: stubs, client: &sshhelper.Client{Conn: srv.Dial(t)}, conf: conf, gce: gce, np: testNP(), nc: nc, pubKey: pub}
}

func (e *provEnv) provision(t *testing.T) (*ProvisionResult, error) {
	t.Helper()
	return ProvisionInstance(context.Background(), e.np, testCreds(), e.pubKey, e.client, e.nc, pkgruntimeConfig())
}

func (e *provEnv) confText() string {
	b, _ := os.ReadFile(e.conf)
	return string(b)
}

func newProvEnvNetConfig() *mlv1alpha1.NodeProvisionNetConfig {
	rng := "10.8.0.0/24"
	nc := &mlv1alpha1.NodeProvisionNetConfig{}
	nc.Spec.VPNRange = &rng
	nc.Spec.VPNServerPublicConfig.PublicIP = "203.0.113.9"
	nc.Spec.SoftwareConfig.KubernetesVersion = "1.35.0"
	nc.Status.ClusterJoinCommand = "kubeadm join 10.8.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:00"
	return nc
}
