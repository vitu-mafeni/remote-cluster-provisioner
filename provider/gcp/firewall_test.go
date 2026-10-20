package gcp

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/protobuf/proto"
)

func plan(noVPN bool, sources ...string) firewallPlan {
	return firewallPlan{
		Instance: "worker-1", NetProject: "proj-1", NetworkName: "default", NoVPN: noVPN,
		VPNServerIP: "203.0.113.9", WGPort: 51820, SSHSources: sources,
		Description: firewallOwnerMarker + ": NodeProvision ns/worker-1 (uid " + testUID + ")",
	}
}

func allowedStrings(fw *computepb.Firewall) []string {
	return allowedKey(fw.GetAllowed())
}

func TestDesiredFirewalls_VPNMode(t *testing.T) {
	rules, err := desiredFirewalls(plan(false))
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 {
		t.Fatalf("VPN mode without sshSources needs exactly the WireGuard rule (SSH rides inside the tunnel), got %d", len(rules))
	}
	wg := rules[0]
	if wg.GetName() != FirewallName("worker-1", "wg") {
		t.Errorf("name %q", wg.GetName())
	}
	if got := wg.GetSourceRanges(); len(got) != 1 || got[0] != "203.0.113.9/32" {
		t.Errorf("WireGuard must be reachable from the VPN server ONLY, got %v", got)
	}
	if got := allowedStrings(wg); len(got) != 1 || got[0] != "udp:51820" {
		t.Errorf("allowed = %v", got)
	}
	if wg.GetDirection() != "INGRESS" || len(wg.GetTargetTags()) != 1 || wg.GetTargetTags()[0] != NodeTag("worker-1") {
		t.Errorf("must be an ingress rule targeted at the node's own tag: %v %v", wg.GetDirection(), wg.GetTargetTags())
	}
	if wg.GetNetwork() != "projects/proj-1/global/networks/default" {
		t.Errorf("network %q", wg.GetNetwork())
	}

	// A custom port and an explicit SSH source add the ssh rule.
	p := plan(false, "198.51.100.0/24")
	p.WGPort = 443
	rules, err = desiredFirewalls(p)
	if err != nil || len(rules) != 2 {
		t.Fatalf("rules=%d err=%v", len(rules), err)
	}
	if allowedStrings(rules[0])[0] != "udp:443" {
		t.Errorf("custom WireGuard port: %v", allowedStrings(rules[0]))
	}
	if ssh := rules[1]; ssh.GetName() != FirewallName("worker-1", "ssh") || allowedStrings(ssh)[0] != "tcp:22" ||
		ssh.GetSourceRanges()[0] != "198.51.100.0/24" {
		t.Errorf("ssh rule: %v %v %v", ssh.GetName(), allowedStrings(ssh), ssh.GetSourceRanges())
	}
	// Port zero falls back to the WireGuard default.
	p = plan(false)
	p.WGPort = 0
	rules, _ = desiredFirewalls(p)
	if allowedStrings(rules[0])[0] != "udp:51820" {
		t.Error("default WireGuard port")
	}
}

func TestDesiredFirewalls_VPNServerMustBeAnIP(t *testing.T) {
	p := plan(false)
	p.VPNServerIP = "vpn.example.com"
	if _, err := desiredFirewalls(p); err == nil || !strings.Contains(err.Error(), "not an IP address") {
		t.Errorf("a hostname cannot be turned into a source range: %v", err)
	}
	p.VPNServerIP = "2001:db8::1"
	rules, err := desiredFirewalls(p)
	if err != nil || rules[0].GetSourceRanges()[0] != "2001:db8::1/128" {
		t.Errorf("IPv6 server: %v %v", rules, err)
	}
}

func TestDesiredFirewalls_NoVPNMode(t *testing.T) {
	rules, err := desiredFirewalls(plan(true))
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 || rules[0].GetName() != FirewallName("worker-1", "mgmt") {
		t.Fatalf("VPN-less mode needs one management rule, got %v", rules)
	}
	r := rules[0]
	for _, rule := range rules {
		for _, a := range rule.GetAllowed() {
			if a.GetIPProtocol() == "udp" && contains(a.GetPorts(), "51820") {
				t.Error("no WireGuard rule without a VPN")
			}
		}
	}
	if !sameStrings(sortedCopy(r.GetSourceRanges()), sortedCopy(PrivateRanges)) {
		t.Errorf("default sources are the private ranges, got %v", r.GetSourceRanges())
	}
	got := strings.Join(allowedStrings(r), " ")
	for _, want := range []string{"tcp:10250,22", "udp:8472", "icmp:"} {
		if !strings.Contains(got, want) {
			t.Errorf("allowed %q lacks %q (control plane -> kubelet, flannel VXLAN, ping, SSH)", got, want)
		}
	}
	// Explicit control-plane ranges replace the defaults.
	rules, _ = desiredFirewalls(plan(true, "172.20.0.0/16"))
	if got := rules[0].GetSourceRanges(); len(got) != 1 || got[0] != "172.20.0.0/16" {
		t.Errorf("explicit ranges: %v", got)
	}
	// The VPN server is irrelevant (and may be empty) without a VPN.
	p := plan(true)
	p.VPNServerIP = ""
	if _, err := desiredFirewalls(p); err != nil {
		t.Errorf("VPN-less mode must not look at the VPN server: %v", err)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func TestEnsureFirewalls_CreatesOnceAndIsIdempotent(t *testing.T) {
	f := newFakeCompute()
	p := plan(false)
	if err := ensureFirewalls(context.Background(), f, p); err != nil {
		t.Fatal(err)
	}
	if len(f.insertedFW) != 1 {
		t.Fatalf("inserted %d rules", len(f.insertedFW))
	}
	if err := ensureFirewalls(context.Background(), f, p); err != nil {
		t.Fatal(err)
	}
	if len(f.insertedFW) != 1 || len(f.deletedFW) != 0 {
		t.Errorf("a second ensure must change nothing: inserted=%d deleted=%d", len(f.insertedFW), len(f.deletedFW))
	}
	for _, project := range f.projects {
		if project != "proj-1" {
			t.Errorf("firewall calls must target the network's project, got %q", project)
		}
	}
}

func TestEnsureFirewalls_SharedVPCUsesTheHostProject(t *testing.T) {
	f := newFakeCompute()
	p := plan(true)
	p.NetProject = "host-proj"
	if err := ensureFirewalls(context.Background(), f, p); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.insertedFW[0].GetNetwork(), "projects/host-proj/") {
		t.Errorf("network %q", f.insertedFW[0].GetNetwork())
	}
	for _, project := range f.projects {
		if project != "host-proj" {
			t.Errorf("project %q", project)
		}
	}
}

func TestEnsureFirewalls_DriftIsRepairedButForeignRulesAreNeverTouched(t *testing.T) {
	ctx := context.Background()
	f := newFakeCompute()
	p := plan(false)
	rules, _ := desiredFirewalls(p)
	// An owned rule whose source changed (e.g. the VPN server moved).
	drifted := proto.Clone(rules[0]).(*computepb.Firewall)
	drifted.SourceRanges = []string{"198.18.0.1/32"}
	f.firewalls[drifted.GetName()] = drifted
	if err := ensureFirewalls(ctx, f, p); err != nil {
		t.Fatal(err)
	}
	if got := f.firewalls[rules[0].GetName()].GetSourceRanges(); len(got) != 1 || got[0] != "203.0.113.9/32" {
		t.Errorf("an owned drifted rule must be recreated, got %v", got)
	}
	if len(f.deletedFW) != 1 || len(f.insertedFW) != 1 {
		t.Errorf("deleted=%v inserted=%d", f.deletedFW, len(f.insertedFW))
	}

	// A rule with the node's name that this controller did not create.
	f = newFakeCompute()
	foreign := proto.Clone(rules[0]).(*computepb.Firewall)
	foreign.Description = proto.String("hand made")
	foreign.SourceRanges = []string{"0.0.0.0/0"}
	f.firewalls[foreign.GetName()] = foreign
	err := ensureFirewalls(ctx, f, p)
	if err == nil || !strings.Contains(err.Error(), "not created by this controller") {
		t.Errorf("a foreign rule must be an error, got %v", err)
	}
	if len(f.deletedFW) != 0 || len(f.insertedFW) != 0 {
		t.Error("a foreign rule must be left untouched")
	}
}

func TestEnsureFirewalls_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	f := newFakeCompute()
	f.getFirewallErr = gerr(403, "forbidden", "Required 'compute.firewalls.get' permission")
	if err := ensureFirewalls(ctx, f, plan(false)); !IsPermissionDenied(err) {
		t.Errorf("permission errors must keep their class for the caller: %v", err)
	}
	f = newFakeCompute()
	f.insertFirewallErr = gerr(400, "invalid", "bad rule")
	if err := ensureFirewalls(ctx, f, plan(false)); err == nil {
		t.Error("insert errors must surface")
	}
	// A concurrent creator winning the race is fine.
	f = newFakeCompute()
	f.insertFirewallErr = gerr(409, "alreadyExists", "exists")
	if err := ensureFirewalls(ctx, f, plan(false)); err != nil {
		t.Errorf("409 on insert is success: %v", err)
	}
}

func TestDeleteFirewalls_OnlyOwnedRulesAndNotFoundIsSuccess(t *testing.T) {
	ctx := context.Background()
	f := newFakeCompute()
	if err := ensureFirewalls(ctx, f, plan(false, "198.51.100.0/24")); err != nil { // wg + ssh
		t.Fatal(err)
	}
	// someone else's rule that merely shares the prefix must survive
	f.firewalls["np-worker-1-mgmt"] = &computepb.Firewall{Name: proto.String("np-worker-1-mgmt"), Description: proto.String("hand made")}
	f.firewalls["unrelated"] = &computepb.Firewall{Name: proto.String("unrelated")}

	if err := deleteFirewalls(ctx, f, "proj-1", "worker-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.firewalls[FirewallName("worker-1", "wg")]; ok {
		t.Error("the wg rule must be deleted")
	}
	if _, ok := f.firewalls[FirewallName("worker-1", "ssh")]; ok {
		t.Error("the ssh rule must be deleted")
	}
	if _, ok := f.firewalls["np-worker-1-mgmt"]; !ok {
		t.Error("a rule not created by the controller must be left alone")
	}
	if _, ok := f.firewalls["unrelated"]; !ok {
		t.Error("unrelated rules must never be touched")
	}
	// second run: everything NotFound -> success, no calls to delete
	n := len(f.deletedFW)
	if err := deleteFirewalls(ctx, f, "proj-1", "worker-1"); err != nil || len(f.deletedFW) != n {
		t.Errorf("idempotent: err=%v extra deletes=%d", err, len(f.deletedFW)-n)
	}
	// an API error is reported (and the other rules are still attempted)
	f = newFakeCompute()
	_ = ensureFirewalls(ctx, f, plan(false, "198.51.100.0/24"))
	f.deleteFirewallErr = gerr(403, "forbidden", "Required 'compute.firewalls.delete' permission")
	if err := deleteFirewalls(ctx, f, "proj-1", "worker-1"); !IsPermissionDenied(err) {
		t.Errorf("delete errors must surface with their class: %v", err)
	}
	if len(f.deletedFW) != 2 {
		t.Errorf("every owned rule must be attempted even after a failure, attempted %v", f.deletedFW)
	}
}

func TestFirewallEquivalent(t *testing.T) {
	rules, _ := desiredFirewalls(plan(true))
	want := rules[0]
	have := proto.Clone(want).(*computepb.Firewall)
	have.Network = proto.String("https://www.googleapis.com/compute/v1/projects/proj-1/global/networks/default")
	have.SourceRanges = []string{"192.168.0.0/16", "10.0.0.0/8", "172.16.0.0/12"} // order is irrelevant
	if !firewallEquivalent(have, want) {
		t.Error("network URL form and range order must not matter")
	}
	disabled := proto.Clone(want).(*computepb.Firewall)
	disabled.Disabled = proto.Bool(true)
	if firewallEquivalent(disabled, want) {
		t.Error("a disabled rule is not equivalent")
	}
	other := proto.Clone(want).(*computepb.Firewall)
	other.Allowed = []*computepb.Allowed{{IPProtocol: proto.String("tcp"), Ports: []string{"22"}}}
	if firewallEquivalent(other, want) {
		t.Error("different allowed ports are not equivalent")
	}
}
