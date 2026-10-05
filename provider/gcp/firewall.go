package gcp

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/protobuf/proto"
)

// PrivateRanges (RFC1918) are the default firewallSourceRanges: the controller
// and the control plane reach the node's internal address.
var PrivateRanges = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}

const (
	// Ports of a node as the control plane sees it without a VPN.
	kubeletPort  = "10250" // API server -> kubelet (logs, exec)
	flannelPort  = "8472"  // flannel VXLAN (default backend)
	sshPort      = "22"
	defaultWGUDP = 51820

	firewallOwnerMarker = "node-provision-controller"
)

// firewallPlan is everything needed to derive a node's firewall rules.
type firewallPlan struct {
	Instance    string
	NetProject  string
	NetworkName string
	NoVPN       bool
	VPNServerIP string // VPN mode: the only source of the WireGuard rule
	WGPort      int
	// SSHSources are the CIDRs allowed to SSH (and, without a VPN, reach the
	// kubelet and flannel). Empty in VPN mode means "no SSH rule".
	SSHSources  []string
	Description string
}

// desiredFirewalls returns the rules of the plan, all INGRESS, ALLOW, targeted
// at the node's own network tag so no other instance is affected:
//
//	VPN mode:  <node>-wg   udp/<wg port> from the VPN server only
//	           <node>-ssh  tcp/22 from SSHSources (only when configured: with a
//	                       VPN the controller reaches SSH inside the tunnel)
//	no VPN:    <node>-mgmt tcp/22, tcp/10250 (kubelet), udp/8472 (flannel VXLAN)
//	                       and ICMP from SSHSources (default: RFC1918)
func desiredFirewalls(p firewallPlan) ([]*computepb.Firewall, error) {
	network := fmt.Sprintf("projects/%s/global/networks/%s", p.NetProject, p.NetworkName)
	tag := NodeTag(p.Instance)
	mk := func(suffix string, sources []string, allowed ...*computepb.Allowed) *computepb.Firewall {
		return &computepb.Firewall{
			Name:         proto.String(FirewallName(p.Instance, suffix)),
			Description:  proto.String(p.Description),
			Network:      proto.String(network),
			Direction:    proto.String("INGRESS"),
			Priority:     proto.Int32(1000),
			SourceRanges: sources,
			TargetTags:   []string{tag},
			Allowed:      allowed,
		}
	}
	proto2 := func(name string, ports ...string) *computepb.Allowed {
		return &computepb.Allowed{IPProtocol: proto.String(name), Ports: ports}
	}

	if p.NoVPN {
		src := p.SSHSources
		if len(src) == 0 {
			src = PrivateRanges
		}
		return []*computepb.Firewall{mk("mgmt", src,
			proto2("tcp", sshPort, kubeletPort), proto2("udp", flannelPort), proto2("icmp"))}, nil
	}

	ip := net.ParseIP(strings.TrimSpace(p.VPNServerIP))
	if ip == nil {
		return nil, fmt.Errorf("VPN server public address %q is not an IP address; cannot restrict the WireGuard firewall rule to it", p.VPNServerIP)
	}
	cidr := ip.String() + "/32"
	if ip.To4() == nil {
		cidr = ip.String() + "/128"
	}
	port := p.WGPort
	if port <= 0 {
		port = defaultWGUDP
	}
	rules := []*computepb.Firewall{mk("wg", []string{cidr}, proto2("udp", strconv.Itoa(port)))}
	if len(p.SSHSources) > 0 {
		rules = append(rules, mk("ssh", p.SSHSources, proto2("tcp", sshPort)))
	}
	return rules, nil
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func allowedKey(a []*computepb.Allowed) []string {
	var out []string
	for _, x := range a {
		out = append(out, strings.ToLower(x.GetIPProtocol())+":"+strings.Join(sortedCopy(x.GetPorts()), ","))
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// firewallEquivalent reports whether an existing rule already has the desired
// semantics (network is compared by its trailing path).
func firewallEquivalent(have, want *computepb.Firewall) bool {
	tail := func(s string) string { return s[strings.LastIndex(s, "/")+1:] }
	return strings.EqualFold(have.GetDirection(), want.GetDirection()) &&
		!have.GetDisabled() &&
		tail(have.GetNetwork()) == tail(want.GetNetwork()) &&
		sameStrings(sortedCopy(have.GetSourceRanges()), sortedCopy(want.GetSourceRanges())) &&
		sameStrings(sortedCopy(have.GetTargetTags()), sortedCopy(want.GetTargetTags())) &&
		sameStrings(allowedKey(have.GetAllowed()), allowedKey(want.GetAllowed()))
}

// ensureFirewalls creates the planned rules that are missing and replaces ones
// this controller owns that drifted. A rule with the node's name that the
// controller did not create is an error (never overwritten). Idempotent.
func ensureFirewalls(ctx context.Context, c computeAPI, p firewallPlan) error {
	rules, err := desiredFirewalls(p)
	if err != nil {
		return err
	}
	for _, want := range rules {
		name := want.GetName()
		have, err := c.GetFirewall(ctx, p.NetProject, name)
		switch {
		case err == nil:
			if firewallEquivalent(have, want) {
				continue
			}
			if !strings.Contains(have.GetDescription(), firewallOwnerMarker) {
				return fmt.Errorf("firewall rule %q already exists and was not created by this controller", name)
			}
			log.Printf("[INFO] Firewall rule %s drifted from the desired state; recreating", name)
			if derr := c.DeleteFirewall(ctx, p.NetProject, name); derr != nil && !IsNotFound(derr) {
				return apiErrorf(derr, "replacing firewall rule %s", name)
			}
		case !IsNotFound(err):
			return apiErrorf(err, "looking up firewall rule %s", name)
		}
		if ierr := c.InsertFirewall(ctx, p.NetProject, want); ierr != nil && !IsAlreadyExists(ierr) {
			return apiErrorf(ierr, "creating firewall rule %s", name)
		}
		log.Printf("[INFO] Firewall rule %s ensured (network %s)", name, p.NetworkName)
	}
	return nil
}

// deleteFirewalls removes every rule a node may own, treating NotFound as
// success. It derives the names from the instance name alone, so it works even
// when the launch never got far enough to record anything.
func deleteFirewalls(ctx context.Context, c computeAPI, netProject, instance string) error {
	var firstErr error
	for _, suffix := range nodeFirewallSuffixes {
		name := FirewallName(instance, suffix)
		have, err := c.GetFirewall(ctx, netProject, name)
		if err != nil {
			if IsNotFound(err) {
				continue
			}
			if firstErr == nil {
				firstErr = apiErrorf(err, "looking up firewall rule %s", name)
			}
			continue
		}
		// Only delete what this controller created for this node.
		if !strings.Contains(have.GetDescription(), firewallOwnerMarker) {
			log.Printf("[WARN] Firewall rule %s was not created by this controller; leaving it", name)
			continue
		}
		if err := c.DeleteFirewall(ctx, netProject, name); err != nil && !IsNotFound(err) {
			if firstErr == nil {
				firstErr = apiErrorf(err, "deleting firewall rule %s", name)
			}
			continue
		}
		log.Printf("[INFO] Deleted firewall rule %s", name)
	}
	return firstErr
}
