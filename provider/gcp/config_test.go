package gcp

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

func TestInstanceName_DeterministicAndValid(t *testing.T) {
	mk := func(ns, name string) *mlv1alpha1.NodeProvision {
		return &mlv1alpha1.NodeProvision{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	}
	// A valid GCE name is used as is, so the hostname equals the node name.
	if got := InstanceName(mk("ns", "gcp-worker-1")); got != "gcp-worker-1" {
		t.Errorf("valid names must be kept, got %q", got)
	}
	for _, name := range []string{"Worker.Example.COM", "9lives", "a_b", strings.Repeat("x", 80), "-lead", "trail-", "..."} {
		a := InstanceName(mk("ns", name))
		if a != InstanceName(mk("ns", name)) {
			t.Errorf("%q: the name must be deterministic (idempotent relaunch)", name)
		}
		if !gceNameRE.MatchString(a) {
			t.Errorf("%q -> %q is not a valid GCE name", name, a)
		}
	}
	if InstanceName(mk("a", "Worker.1")) == InstanceName(mk("b", "Worker.1")) {
		t.Error("sanitised names of different namespaces must not collide")
	}
	if InstanceName(mk("ns", "a.b")) == InstanceName(mk("ns", "a-b")) {
		t.Error("distinct names must not collapse to the same instance name")
	}
}

func TestPerNodeResourceNames(t *testing.T) {
	long := strings.Repeat("a", 63)
	for _, inst := range []string{"w1", "gcp-worker-1", long} {
		for _, suf := range nodeFirewallSuffixes {
			fw := FirewallName(inst, suf)
			if !gceNameRE.MatchString(fw) {
				t.Errorf("firewall name %q for %q is invalid", fw, inst)
			}
			if fw != FirewallName(inst, suf) {
				t.Error("firewall names must be deterministic: cleanup recomputes them")
			}
		}
		if tag := NodeTag(inst); !gceNameRE.MatchString(tag) {
			t.Errorf("node tag %q for %q is invalid", tag, inst)
		}
	}
	if FirewallName("w1", "wg") == FirewallName("w1", "mgmt") {
		t.Error("rule suffixes must differ")
	}
	if FirewallName(long, "wg") == FirewallName(long[:62]+"b", "wg") {
		t.Error("truncated names of different instances must keep a distinguishing hash")
	}
	if NodeTag("w1") == NodeTag("w2") {
		t.Error("each node needs its own tag so its rules affect only itself")
	}
}

func TestRequestIDForAttempt(t *testing.T) {
	uuidRE := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	a0 := RequestIDForAttempt(testUID, 0)
	if !uuidRE.MatchString(a0) {
		t.Fatalf("not a valid UUID: %q", a0)
	}
	if a0 != RequestIDForAttempt(testUID, 0) {
		t.Error("the same attempt must produce the same request id (idempotent retry after a crash)")
	}
	if a0 == RequestIDForAttempt(testUID, 1) {
		t.Error("the next attempt must use a new request id (the server replays a request id for an hour)")
	}
	if a0 == RequestIDForAttempt("other-uid", 0) {
		t.Error("different NodeProvisions must not share a request id")
	}
	if RequestIDForAttempt(testUID, -3) != a0 {
		t.Error("a negative counter is treated as attempt 0")
	}
}

func TestMachineFamilyAndGPUShapes(t *testing.T) {
	for in, want := range map[string]string{
		"n1-standard-8": "n1", "e2-standard-4": "e2", "g2-standard-8": "g2", "a2-highgpu-1g": "a2",
		"a3-highgpu-8g": "a3", "custom-4-8192": "n1", "n1-custom-4-8192": "n1", "n2-custom-4-8192": "n2",
		"E2-Medium": "e2", "t2a-standard-4": "t2a",
	} {
		if got := MachineFamily(in); got != want {
			t.Errorf("MachineFamily(%q) = %q, want %q", in, got, want)
		}
	}
	for _, mt := range []string{"a2-highgpu-1g", "a3-highgpu-8g", "g2-standard-8"} {
		if !HasImpliedGPU(mt) || !UsesGPU(mt, nil) {
			t.Errorf("%s includes its GPUs", mt)
		}
	}
	if HasImpliedGPU("n1-standard-8") || UsesGPU("n1-standard-8", nil) {
		t.Error("a plain N1 has no GPU")
	}
	cfg := &mlv1alpha1.GCPConfig{Accelerator: &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 1}}
	if !UsesGPU("n1-standard-8", cfg) || UsesGPU("e2-standard-4", &mlv1alpha1.GCPConfig{}) {
		t.Error("UsesGPU must follow the accelerator")
	}
}

func TestRegionOfZone(t *testing.T) {
	for zone, want := range map[string]string{
		"us-central1-a": "us-central1", "europe-west4-b": "europe-west4",
		"northamerica-northeast1-c": "northamerica-northeast1", "asia-southeast1-a": "asia-southeast1",
	} {
		if got, ok := RegionOfZone(zone); !ok || got != want {
			t.Errorf("RegionOfZone(%q) = %q,%v", zone, got, ok)
		}
	}
	for _, bad := range []string{"", "us-central1", "us-central1-aa", "nonsense", "us-central1-"} {
		if _, ok := RegionOfZone(bad); ok {
			t.Errorf("%q must not parse as a zone", bad)
		}
	}
}

func TestParseNetworkRef(t *testing.T) {
	for _, tc := range []struct{ in, wantProj, wantName string }{
		{"", "p", "default"},
		{"vpc-1", "p", "vpc-1"},
		{"projects/host/global/networks/shared", "host", "shared"},
		{"https://www.googleapis.com/compute/v1/projects/host/global/networks/shared", "host", "shared"},
	} {
		proj, name := ParseNetworkRef("p", tc.in)
		if proj != tc.wantProj || name != tc.wantName {
			t.Errorf("ParseNetworkRef(%q) = %s,%s", tc.in, proj, name)
		}
	}
}

func TestSanitizeLabelValueAndControllerLabels(t *testing.T) {
	if got := sanitizeLabelValue("My.Node/1"); got != "my-node-1" {
		t.Errorf("got %q", got)
	}
	if len(sanitizeLabelValue(strings.Repeat("a", 100))) != 63 {
		t.Error("labels are capped at 63 characters")
	}
	np := testNP()
	np.Name = "worker.example"
	l := ControllerLabels(np)
	if l[LabelUID] != testUID || l[LabelName] != "worker-example" || l[LabelManagedBy] != ManagedByValue || l[LabelNamespace] != "ns" {
		t.Errorf("labels = %v", l)
	}
	for k, v := range l {
		if !labelKeyRE.MatchString(k) || !labelValRE.MatchString(v) {
			t.Errorf("label %s=%s is not valid for GCE", k, v)
		}
	}
	// User labels merge in but never override the controller's.
	np.Spec.GCPConfig.Labels = map[string]string{"team": "ml", LabelUID: "evil", LabelManagedBy: "me"}
	merged := instanceLabels(np)
	if merged["team"] != "ml" || merged[LabelUID] != testUID || merged[LabelManagedBy] != ManagedByValue {
		t.Errorf("merged labels = %v", merged)
	}
}

func TestDefaultMachineTypeForLabel(t *testing.T) {
	if DefaultMachineTypeForLabel("cpu") != "e2-standard-4" || DefaultMachineTypeForLabel(" GPU ") != "n1-standard-8" ||
		DefaultMachineTypeForLabel("other") != "" || DefaultMachineTypeForLabel("") != "" {
		t.Error("default machine types per nodeLabel")
	}
}

// ── ValidateGCPConfig ────────────────────────────────────────────────────────

func TestValidateGCPConfig(t *testing.T) {
	ok := testNP().Spec
	if err := ValidateGCPConfig(ok); err != nil {
		t.Fatalf("a resolved spec must validate: %v", err)
	}
	mut := func(f func(s *mlv1alpha1.NodeProvisionSpec)) mlv1alpha1.NodeProvisionSpec {
		s := *testNP().Spec.DeepCopy()
		f(&s)
		return s
	}
	cases := []struct {
		name string
		spec mlv1alpha1.NodeProvisionSpec
		want string // substring of the error
	}{
		{"no machine type", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.InstanceType = "" }), "instanceType"},
		{"no gcpConfig", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig = nil }), "gcpConfig is required"},
		{"no project", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.ProjectID = "" }), "projectId"},
		{"no zone", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.Zone = "" }), "zone"},
		{"bad zone", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.Zone = "central" }), "not a valid zone"},
		{"zone outside region", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.Region = "europe-west1" }), "region"},
		{"arm machine", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.InstanceType = "t2a-standard-4" }), "Arm"},
		{"tiny disk", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.BootDiskSizeGB = 5 }), "bootDiskSizeGB"},
		{"accelerator on e2", mut(func(s *mlv1alpha1.NodeProvisionSpec) {
			s.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 1}
		}), "only supported on N1"},
		{"accelerator on g2", mut(func(s *mlv1alpha1.NodeProvisionSpec) {
			s.InstanceType = "g2-standard-8"
			s.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-l4", Count: 1}
		}), "already includes its GPUs"},
		{"too many accelerators", mut(func(s *mlv1alpha1.NodeProvisionSpec) {
			s.InstanceType = "n1-standard-8"
			s.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 99}
		}), "count"},
		{"bad label key", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.Labels = map[string]string{"Bad Key": "v"} }), "labels key"},
		{"bad label value", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.Labels = map[string]string{"k": "Has.Dots"} }), "value is invalid"},
		{"bad tag", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.NetworkTags = []string{"Bad_Tag"} }), "networkTags"},
		{"bad cidr", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.FirewallSourceRanges = []string{"10.0.0.1"} }), "CIDR"},
		{"bad sa email", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.ServiceAccountEmail = "nope" }), "serviceAccountEmail"},
		{"scopes without sa", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.ServiceAccountScopes = []string{"x"} }), "requires"},
		{"root ssh user", mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.SSHUsernameOverride = "root" }), "sshUsernameOverride"},
	}
	for _, c := range cases {
		err := ValidateGCPConfig(c.spec)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want substring %q", c.name, err, c.want)
		}
	}
	// Valid GPU shapes.
	n1 := mut(func(s *mlv1alpha1.NodeProvisionSpec) {
		s.InstanceType = "n1-standard-8"
		s.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 2}
	})
	g2 := mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.InstanceType = "g2-standard-8" })
	for name, s := range map[string]mlv1alpha1.NodeProvisionSpec{"n1+accelerator": n1, "g2": g2} {
		if err := ValidateGCPConfig(s); err != nil {
			t.Errorf("%s must validate: %v", name, err)
		}
	}
	// A region alone (zone empty) is not enough once defaults ran.
	if err := ValidateGCPConfig(mut(func(s *mlv1alpha1.NodeProvisionSpec) { s.GCPConfig.Zone = "" })); err == nil {
		t.Error("zone must be resolved before validation")
	}
}

// ── ResolveDefaults ──────────────────────────────────────────────────────────

func resolveNP(mut func(np *mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
	np := &mlv1alpha1.NodeProvision{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: "ns", UID: testUID},
		Spec:       mlv1alpha1.NodeProvisionSpec{Provider: mlv1alpha1.CloudProviderGCP, Region: "us-central1", NodeLabel: "cpu"},
	}
	if mut != nil {
		mut(np)
	}
	return np
}

func TestResolveDefaults_MinimalSpecGetsEverything(t *testing.T) {
	f := newFakeCompute()
	f.zones = []*computepb.Zone{zoneUp("us-central1-c"), zoneUp("us-central1-a"), {Name: proto.String("us-central1-b"), Status: proto.String("DOWN")}}
	f.machines["us-central1-a/e2-standard-4"] = true
	f.machines["us-central1-c/e2-standard-4"] = true
	useFake(t, f)

	res, err := ResolveDefaults(context.Background(), testCreds(), resolveNP(nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.InstanceType != "e2-standard-4" || res.Region != "us-central1" {
		t.Errorf("machine type/region: %+v", res)
	}
	c := res.Config
	if c.ProjectID != "proj-1" {
		t.Errorf("project must come from the key, got %q", c.ProjectID)
	}
	if c.Zone != "us-central1-a" {
		t.Errorf("first UP zone (alphabetical) offering the machine type, got %q", c.Zone)
	}
	if c.Network != "default" || c.Subnetwork != "" {
		t.Errorf("default auto-mode network: %q/%q", c.Network, c.Subnetwork)
	}
	if c.SourceImage != "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20260101" {
		t.Errorf("Ubuntu 22.04 family image, compacted: %q", c.SourceImage)
	}
	// The resolved spec must be complete enough to validate.
	spec := resolveNP(nil).Spec
	spec.Region, spec.InstanceType, spec.GCPConfig = res.Region, res.InstanceType, &res.Config
	if err := ValidateGCPConfig(spec); err != nil {
		t.Errorf("resolved defaults must validate: %v", err)
	}
	if f.closed == 0 {
		t.Error("the client must be closed")
	}
}

func TestResolveDefaults_NeverOverwritesUserValues(t *testing.T) {
	f := newFakeCompute()
	f.networks["vpc-x"] = &computepb.Network{Name: proto.String("vpc-x"), AutoCreateSubnetworks: proto.Bool(false)}
	addSubnet(f, "mine", "us-east1", "sub-x", "mine", "vpc-x")
	addImage(f, "p", "custom", 20, nil)
	f.machines["us-east1-b/n2-standard-4"] = true
	useFake(t, f)

	np := resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.Region = "us-east1"
		np.Spec.InstanceType = "n2-standard-4"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{
			ProjectID: "mine", Zone: "us-east1-b", Network: "vpc-x", Subnetwork: "sub-x",
			SourceImage: "projects/p/global/images/custom", BootDiskSizeGB: 80,
		}
	})
	res, err := ResolveDefaults(context.Background(), testCreds(), np)
	if err != nil {
		t.Fatal(err)
	}
	c := res.Config
	if c.ProjectID != "mine" || c.Zone != "us-east1-b" || c.Network != "vpc-x" || c.Subnetwork != "sub-x" ||
		c.SourceImage != "projects/p/global/images/custom" || c.BootDiskSizeGB != 80 || res.InstanceType != "n2-standard-4" {
		t.Errorf("user values were changed: %+v", res)
	}
	if np.Spec.GCPConfig.ProjectID != "mine" {
		t.Error("the input NodeProvision must not be mutated")
	}
}

func TestResolveDefaults_GPULabelGetsN1WithT4(t *testing.T) {
	f := newFakeCompute()
	f.zones = []*computepb.Zone{zoneUp("us-central1-a"), zoneUp("us-central1-b")}
	f.machines["us-central1-a/n1-standard-8"] = true
	f.machines["us-central1-b/n1-standard-8"] = true
	f.accels["us-central1-b/nvidia-tesla-t4"] = true // only zone b has the T4
	useFake(t, f)

	res, err := ResolveDefaults(context.Background(), testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.NodeLabel = "gpu" }))
	if err != nil {
		t.Fatal(err)
	}
	if res.InstanceType != "n1-standard-8" {
		t.Errorf("machine type %q", res.InstanceType)
	}
	if a := res.Config.Accelerator; a == nil || a.Type != "nvidia-tesla-t4" || a.Count != 1 {
		t.Errorf("a defaulted GPU shape needs the default accelerator, got %+v", a)
	}
	if res.Config.Zone != "us-central1-b" {
		t.Errorf("the zone must offer the accelerator too, got %q", res.Config.Zone)
	}
}

func TestResolveDefaults_ExplicitMachineTypeKeepsItsOwnAccelerator(t *testing.T) {
	f := newFakeCompute()
	f.machines["us-central1-a/n1-standard-4"] = true
	useFake(t, f)
	// An explicit machine type with hardwareType gpu but no accelerator is the
	// user's call: no accelerator is invented for it.
	res, err := ResolveDefaults(context.Background(), testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "n1-standard-4"
		np.Spec.HardwareType = "gpu"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Zone: "us-central1-a"}
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Config.Accelerator != nil {
		t.Errorf("no accelerator may be invented for an explicit machine type, got %+v", res.Config.Accelerator)
	}
	// A user accelerator with count 0 defaults to 1.
	f.accels["us-central1-a/nvidia-tesla-t4"] = true
	res, err = ResolveDefaults(context.Background(), testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "n1-standard-4"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Zone: "us-central1-a", Accelerator: &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4"}}
	}))
	if err != nil || res.Config.Accelerator.Count != 1 {
		t.Errorf("count must default to 1: %+v err=%v", res, err)
	}
}

func TestResolveDefaults_ZoneAndRegionRules(t *testing.T) {
	f := newFakeCompute()
	f.machines["us-central1-a/e2-standard-4"] = true
	useFake(t, f)
	run := func(mut func(np *mlv1alpha1.NodeProvision)) (*Resolved, error) {
		return ResolveDefaults(context.Background(), testCreds(), resolveNP(mut))
	}

	// zone alone implies the region.
	res, err := run(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.Region = ""
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Zone: "us-central1-a"}
	})
	if err != nil || res.Region != "us-central1" {
		t.Errorf("zone must imply the region: %+v err=%v", res, err)
	}
	// zone/region mismatch.
	if _, err := run(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.Region = "europe-west1"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Zone: "us-central1-a"}
	}); err == nil || !strings.Contains(err.Error(), "region") {
		t.Errorf("mismatch must be rejected, got %v", err)
	}
	// neither zone nor region.
	if _, err := run(func(np *mlv1alpha1.NodeProvision) { np.Spec.Region = "" }); err == nil {
		t.Error("a location is required")
	}
	// malformed region and zone.
	if _, err := run(func(np *mlv1alpha1.NodeProvision) { np.Spec.Region = "central" }); err == nil {
		t.Error("malformed region")
	}
	if _, err := run(func(np *mlv1alpha1.NodeProvision) { np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Zone: "nope"} }); err == nil {
		t.Error("malformed zone")
	}
	// given zone without the machine type.
	if _, err := run(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Zone: "us-central1-f"}
	}); err == nil || !strings.Contains(err.Error(), "not available in zone us-central1-f") {
		t.Errorf("unavailable machine type in the given zone: %v", err)
	}
}

func TestResolveDefaults_Failures(t *testing.T) {
	newF := func() *fakeCompute {
		f := newFakeCompute()
		f.zones = []*computepb.Zone{zoneUp("us-central1-a")}
		f.machines["us-central1-a/e2-standard-4"] = true
		useFake(t, f)
		return f
	}
	ctx := context.Background()

	f := newF()
	f.zones = nil
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "no available zone") {
		t.Errorf("no zones: %v", err)
	}
	f = newF()
	delete(f.machines, "us-central1-a/e2-standard-4")
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "not available in any zone") {
		t.Errorf("machine type nowhere: %v", err)
	}
	f = newF()
	f.listZonesErr = gerr(403, "forbidden", "Required 'compute.zones.list' permission")
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "roles/compute.instanceAdmin.v1") {
		t.Errorf("permission error must carry advice: %v", err)
	}
	f = newF()
	delete(f.networks, "default")
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "network") {
		t.Errorf("missing default network: %v", err)
	}
	f = newF()
	f.networks["default"] = &computepb.Network{Name: proto.String("default"), AutoCreateSubnetworks: proto.Bool(false)}
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "subnetwork is required") {
		t.Errorf("custom-mode network needs a subnetwork: %v", err)
	}
	f = newF()
	f.networks["default"] = &computepb.Network{Name: proto.String("default"), IPv4Range: proto.String("10.0.0.0/8")}
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Errorf("legacy network: %v", err)
	}
	f = newF()
	delete(f.images, "ubuntu-os-cloud/ubuntu-2204-lts")
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "image family") {
		t.Errorf("missing image family: %v", err)
	}
	f = newF()
	f.images["ubuntu-os-cloud/ubuntu-2204-lts"].Architecture = proto.String("ARM64")
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "X86_64") {
		t.Errorf("arm image: %v", err)
	}
	// no project anywhere
	newF()
	if _, err := ResolveDefaults(ctx, Credentials{JSON: []byte("{}")}, resolveNP(nil)); err == nil || !strings.Contains(err.Error(), "projectId") {
		t.Errorf("no project: %v", err)
	}
	// no machine type and no default for the label
	newF()
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.NodeLabel = "weird" })); err == nil ||
		!strings.Contains(err.Error(), "instanceType") {
		t.Errorf("unknown nodeLabel: %v", err)
	}
	// arm machine is rejected before any API call
	f = newF()
	if _, err := ResolveDefaults(ctx, testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) { np.Spec.InstanceType = "t2a-standard-4" })); err == nil ||
		!strings.Contains(err.Error(), "Arm") {
		t.Errorf("arm: %v", err)
	}
	if len(f.projects) != 0 {
		t.Error("a structurally invalid spec must fail before any API call")
	}
}

func TestResolveDefaults_ImageFamilyOverride(t *testing.T) {
	f := newFakeCompute()
	f.zones = []*computepb.Zone{zoneUp("us-central1-a")}
	f.machines["us-central1-a/e2-standard-4"] = true
	f.images["my-images/golden"] = &computepb.Image{SelfLink: proto.String("https://www.googleapis.com/compute/v1/projects/my-images/global/images/golden-v2")}
	useFake(t, f)
	res, err := ResolveDefaults(context.Background(), testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{ImageFamily: "golden", ImageProject: "my-images"}
	}))
	if err != nil || res.Config.SourceImage != "projects/my-images/global/images/golden-v2" {
		t.Errorf("got %+v err=%v", res, err)
	}
}

// addSubnet registers a PRIVATE, READY subnetwork of network (in netProject).
func addSubnet(f *fakeCompute, project, region, name, netProject, network string) *computepb.Subnetwork {
	sn := &computepb.Subnetwork{
		Name:    proto.String(name),
		Network: proto.String("https://www.googleapis.com/compute/v1/projects/" + netProject + "/global/networks/" + network),
		Purpose: proto.String("PRIVATE"),
		State:   proto.String("READY"),
	}
	f.subnets[project+"/"+region+"/"+name] = sn
	return sn
}

func subnetNP(network, subnetwork string) *mlv1alpha1.NodeProvision {
	return resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.Region = "us-east1"
		np.Spec.InstanceType = "n2-standard-4"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Network: network, Subnetwork: subnetwork}
	})
}

func subnetFake(t *testing.T) *fakeCompute {
	t.Helper()
	f := newFakeCompute()
	f.networks["custom"] = &computepb.Network{Name: proto.String("custom"), AutoCreateSubnetworks: proto.Bool(false)}
	f.networks["other"] = &computepb.Network{Name: proto.String("other"), AutoCreateSubnetworks: proto.Bool(false)}
	f.machines["us-east1-b/n2-standard-4"] = true
	f.machines["us-east1-c/n2-standard-4"] = true
	f.zones = append(f.zones, &computepb.Zone{Name: proto.String("us-east1-b"), Status: proto.String("UP")})
	useFake(t, f)
	return f
}

func TestResolveDefaults_SubnetworkAloneDerivesNetwork(t *testing.T) {
	f := subnetFake(t)
	addSubnet(f, "proj-1", "us-east1", "sub-1", "proj-1", "custom")
	res, err := ResolveDefaults(context.Background(), testCreds(), subnetNP("", "sub-1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Config.Network != "projects/proj-1/global/networks/custom" || res.Config.Subnetwork != "sub-1" {
		t.Errorf("network must come from the subnetwork, got %q / %q", res.Config.Network, res.Config.Subnetwork)
	}
}

func TestResolveDefaults_SharedVPCSubnetworkForms(t *testing.T) {
	f := subnetFake(t)
	addSubnet(f, "host", "us-east1", "shared-sub", "host", "shared")
	f.networks["shared"] = &computepb.Network{Name: proto.String("shared"), AutoCreateSubnetworks: proto.Bool(false)}
	for _, tc := range []struct{ name, network, sub string }{
		{"bare name, host network", "projects/host/global/networks/shared", "shared-sub"},
		{"short path, host network", "projects/host/global/networks/shared", "regions/us-east1/subnetworks/shared-sub"},
		{"full path, no network", "", "projects/host/regions/us-east1/subnetworks/shared-sub"},
		{"full URL, no network", "", "https://www.googleapis.com/compute/v1/projects/host/regions/us-east1/subnetworks/shared-sub"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ResolveDefaults(context.Background(), testCreds(), subnetNP(tc.network, tc.sub))
			if err != nil {
				t.Fatal(err)
			}
			if res.Config.Network != "projects/host/global/networks/shared" {
				t.Errorf("network = %q", res.Config.Network)
			}
		})
	}
}

func TestResolveDefaults_SubnetworkErrors(t *testing.T) {
	f := subnetFake(t)
	addSubnet(f, "proj-1", "us-east1", "sub-1", "proj-1", "custom")
	addSubnet(f, "proj-1", "us-west1", "west", "proj-1", "custom")
	addSubnet(f, "proj-1", "us-east1", "proxy", "proj-1", "custom").Purpose = proto.String("REGIONAL_MANAGED_PROXY")
	f.subnets["proj-1/us-east1/proxy"].Purpose = proto.String("REGIONAL_MANAGED_PROXY")
	addSubnet(f, "proj-1", "us-east1", "drain", "proj-1", "custom").State = proto.String("DRAINING")
	for _, tc := range []struct{ name, network, sub, want string }{
		{"not found", "", "nope", "not found in project"},
		{"wrong region", "", "projects/proj-1/regions/us-west1/subnetworks/west", "is in region"},
		{"network mismatch", "other", "sub-1", "belongs to network projects/proj-1/global/networks/custom"},
		{"proxy-only purpose", "", "proxy", "only PRIVATE"},
		{"draining", "", "drain", "not ready"},
		{"malformed", "", "a/b/c", "not a subnetwork name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveDefaults(context.Background(), testCreds(), subnetNP(tc.network, tc.sub))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseSubnetworkRef(t *testing.T) {
	for _, tc := range []struct{ in, p, r, n string }{
		{"s", "dp", "dr", "s"},
		{"regions/r1/subnetworks/s", "dp", "r1", "s"},
		{"projects/p1/regions/r1/subnetworks/s", "p1", "r1", "s"},
		{"https://www.googleapis.com/compute/v1/projects/p1/regions/r1/subnetworks/s", "p1", "r1", "s"},
	} {
		p, r, n, err := ParseSubnetworkRef("dp", "dr", tc.in)
		if err != nil || p != tc.p || r != tc.r || n != tc.n {
			t.Errorf("%q -> %q %q %q %v", tc.in, p, r, n, err)
		}
	}
	if _, _, _, err := ParseSubnetworkRef("dp", "dr", ""); err == nil {
		t.Error("empty ref must fail")
	}
}

// ── boot image / snapshot ───────────────────────────────────────────────────

func addImage(f *fakeCompute, project, name string, gb int64, mut func(*computepb.Image)) *computepb.Image {
	img := &computepb.Image{
		Name: proto.String(name), Status: proto.String("READY"), Architecture: proto.String("X86_64"), DiskSizeGb: proto.Int64(gb),
		SelfLink: proto.String("https://www.googleapis.com/compute/v1/projects/" + project + "/global/images/" + name),
	}
	if mut != nil {
		mut(img)
	}
	f.imgByName[project+"/"+name] = img
	return img
}

func addSnapshot(f *fakeCompute, project, name string, gb int64, mut func(*computepb.Snapshot)) {
	s := &computepb.Snapshot{Name: proto.String(name), Status: proto.String("READY"), DiskSizeGb: proto.Int64(gb)}
	if mut != nil {
		mut(s)
	}
	f.snapshots[project+"/"+name] = s
}

func bootNP(mut func(*mlv1alpha1.GCPConfig)) *mlv1alpha1.NodeProvision {
	return resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.Region = "us-east1"
		np.Spec.InstanceType = "n2-standard-4"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{}
		if mut != nil {
			mut(np.Spec.GCPConfig)
		}
	})
}

func bootFake(t *testing.T) *fakeCompute {
	t.Helper()
	f := newFakeCompute()
	f.machines["us-east1-b/n2-standard-4"] = true
	f.zones = append(f.zones, &computepb.Zone{Name: proto.String("us-east1-b"), Status: proto.String("UP")})
	useFake(t, f)
	return f
}

func TestParseImageRef(t *testing.T) {
	for _, tc := range []struct {
		in, p, n string
		fam      bool
	}{
		{"my-image", "dp", "my-image", false},
		{"global/images/my-image", "dp", "my-image", false},
		{"global/images/family/fam", "dp", "fam", true},
		{"projects/p1/global/images/my-image", "p1", "my-image", false},
		{"projects/p1/global/images/family/fam", "p1", "fam", true},
		{"https://www.googleapis.com/compute/v1/projects/p1/global/images/my-image", "p1", "my-image", false},
		{"https://www.googleapis.com/compute/v1/projects/p1/global/images/family/fam", "p1", "fam", true},
	} {
		p, n, fam, err := ParseImageRef("dp", tc.in)
		if err != nil || p != tc.p || n != tc.n || fam != tc.fam {
			t.Errorf("%q -> %q %q %v %v", tc.in, p, n, fam, err)
		}
	}
	for _, bad := range []string{"", "a/b", "projects/p1/global/disks/d", "projects/p1/images/x", "global/images/"} {
		if _, _, _, err := ParseImageRef("dp", bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

func TestParseSnapshotRef(t *testing.T) {
	for _, tc := range []struct{ in, p, n string }{
		{"snap", "dp", "snap"},
		{"global/snapshots/snap", "dp", "snap"},
		{"projects/p1/global/snapshots/snap", "p1", "snap"},
		{"https://www.googleapis.com/compute/v1/projects/p1/global/snapshots/snap", "p1", "snap"},
	} {
		p, n, err := ParseSnapshotRef("dp", tc.in)
		if err != nil || p != tc.p || n != tc.n {
			t.Errorf("%q -> %q %q %v", tc.in, p, n, err)
		}
	}
	for _, bad := range []string{"", "a/b", "projects/p1/global/images/x"} {
		if _, _, err := ParseSnapshotRef("dp", bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

func TestResolveDefaults_CustomImageForms(t *testing.T) {
	f := bootFake(t)
	addImage(f, "proj-1", "golden", 30, nil)
	addImage(f, "shared", "base", 30, nil)
	f.images["shared/golden-family"] = &computepb.Image{Name: proto.String("golden-v3"), Status: proto.String("READY"), Architecture: proto.String("X86_64"), DiskSizeGb: proto.Int64(30),
		SelfLink: proto.String("https://www.googleapis.com/compute/v1/projects/shared/global/images/golden-v3")}
	for _, tc := range []struct {
		name string
		mut  func(*mlv1alpha1.GCPConfig)
	}{
		{"bare name in the instance project", func(c *mlv1alpha1.GCPConfig) { c.SourceImage = "golden" }},
		{"bare name in imageProject", func(c *mlv1alpha1.GCPConfig) { c.SourceImage = "base"; c.ImageProject = "shared" }},
		{"full path", func(c *mlv1alpha1.GCPConfig) { c.SourceImage = "projects/shared/global/images/base" }},
		{"family path", func(c *mlv1alpha1.GCPConfig) { c.SourceImage = "projects/shared/global/images/family/golden-family" }},
		{"imageFamily + imageProject", func(c *mlv1alpha1.GCPConfig) { c.ImageFamily = "golden-family"; c.ImageProject = "shared" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ResolveDefaults(context.Background(), testCreds(), bootNP(tc.mut))
			if err != nil {
				t.Fatal(err)
			}
			if res.Config.SourceImage == "" || res.Config.SourceSnapshot != "" {
				t.Errorf("config = %+v", res.Config)
			}
		})
	}
}

func TestResolveDefaults_CustomImageErrors(t *testing.T) {
	f := bootFake(t)
	addImage(f, "proj-1", "pending", 10, func(i *computepb.Image) { i.Status = proto.String("PENDING") })
	addImage(f, "proj-1", "arm", 10, func(i *computepb.Image) { i.Architecture = proto.String("ARM64") })
	addImage(f, "proj-1", "gone", 10, func(i *computepb.Image) {
		i.Deprecated = &computepb.DeprecationStatus{State: proto.String("DELETED")}
	})
	addImage(f, "proj-1", "old", 10, func(i *computepb.Image) {
		i.Deprecated = &computepb.DeprecationStatus{State: proto.String("DEPRECATED")}
	})
	addImage(f, "proj-1", "winlic", 10, func(i *computepb.Image) {
		i.Licenses = []string{"https://www.googleapis.com/compute/v1/projects/windows-cloud/global/licenses/windows-server-2022-dc"}
	})
	addImage(f, "proj-1", "winfeat", 10, func(i *computepb.Image) {
		i.GuestOsFeatures = []*computepb.GuestOsFeature{{Type: proto.String("WINDOWS")}}
	})
	addImage(f, "proj-1", "big", 120, nil)
	for _, tc := range []struct {
		name, image string
		size        int32
		want        string
	}{
		{"missing", "nope", 0, "not found"},
		{"not ready", "pending", 0, "not ready (status PENDING)"},
		{"arm", "arm", 0, "only X86_64"},
		{"deleted", "gone", 0, "DELETED and can no longer be used"},
		{"windows license", "winlic", 0, "Windows image"},
		{"windows feature", "winfeat", 0, "Windows image"},
		{"disk too small", "big", 100, "bootDiskSizeGB 100 is smaller than the boot source (120 GB)"},
		{"malformed", "a/b", 0, "not an image name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveDefaults(context.Background(), testCreds(), bootNP(func(c *mlv1alpha1.GCPConfig) { c.SourceImage = tc.image; c.BootDiskSizeGB = tc.size }))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	// A deprecated (not deleted) image still works.
	if _, err := ResolveDefaults(context.Background(), testCreds(), bootNP(func(c *mlv1alpha1.GCPConfig) { c.SourceImage = "old" })); err != nil {
		t.Errorf("deprecated image: %v", err)
	}
	// An image bigger than the default boot disk grows an unset bootDiskSizeGB.
	res, err := ResolveDefaults(context.Background(), testCreds(), bootNP(func(c *mlv1alpha1.GCPConfig) { c.SourceImage = "big" }))
	if err != nil || res.Config.BootDiskSizeGB != 120 {
		t.Errorf("boot disk = %d err %v", res.Config.BootDiskSizeGB, err)
	}
}

func TestResolveDefaults_Snapshot(t *testing.T) {
	f := bootFake(t)
	addSnapshot(f, "proj-1", "golden", 40, nil)
	addSnapshot(f, "shared", "base", 200, nil)
	addSnapshot(f, "proj-1", "busy", 40, func(s *computepb.Snapshot) { s.Status = proto.String("CREATING") })
	addSnapshot(f, "proj-1", "win", 40, func(s *computepb.Snapshot) {
		s.Licenses = []string{"projects/windows-cloud/global/licenses/windows-server-2019-dc"}
	})
	ok := func(ref string, size int32) (*Resolved, error) {
		return ResolveDefaults(context.Background(), testCreds(), bootNP(func(c *mlv1alpha1.GCPConfig) { c.SourceSnapshot = ref; c.BootDiskSizeGB = size }))
	}
	res, err := ok("golden", 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Config.SourceSnapshot != "golden" || res.Config.SourceImage != "" || res.Config.BootDiskSizeGB != 0 {
		t.Errorf("a 40 GB snapshot fits the default disk and no image is resolved: %+v", res.Config)
	}
	if res, err = ok("projects/shared/global/snapshots/base", 0); err != nil || res.Config.BootDiskSizeGB != 200 {
		t.Errorf("a 200 GB snapshot grows an unset disk: %+v %v", res, err)
	}
	for ref, want := range map[string]string{
		"nope": "not found in project", "busy": "not ready (status CREATING)", "win": "Windows disk", "a/b": "not a snapshot name",
	} {
		if _, err := ok(ref, 0); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want %q", ref, err, want)
		}
	}
	if _, err := ok("projects/shared/global/snapshots/base", 100); err == nil || !strings.Contains(err.Error(), "smaller than the boot source (200 GB)") {
		t.Errorf("explicit disk too small: %v", err)
	}
}

func TestValidateGCPConfig_BootSourceIsExclusive(t *testing.T) {
	for _, mut := range []func(*mlv1alpha1.GCPConfig){
		func(c *mlv1alpha1.GCPConfig) { c.SourceImage = "img" },
		func(c *mlv1alpha1.GCPConfig) { c.ImageFamily = "fam" },
		func(c *mlv1alpha1.GCPConfig) { c.ImageProject = "proj" },
	} {
		_, err := ResolveDefaults(context.Background(), testCreds(), bootNP(func(c *mlv1alpha1.GCPConfig) { c.SourceSnapshot = "snap"; mut(c) }))
		if err == nil || !strings.Contains(err.Error(), "choose one boot source") {
			t.Errorf("err = %v", err)
		}
	}
}
