package kubeadm

import (
	"strings"
	"testing"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
)

func TestResolveNodeIP_DisableVPNRequiresIPHost(t *testing.T) {
	for _, host := range []string{"node.example.com", "", "10.0.0"} {
		c := &infrav1.RemoteCluster{Spec: infrav1.RemoteClusterSpec{DisableVPN: true, Host: host}}
		// nil client: validation must fail before any SSH command runs.
		_, err := ResolveNodeIP(nil, c)
		if err == nil || !strings.Contains(err.Error(), "spec.host") {
			t.Errorf("host %q: expected spec.host validation error, got %v", host, err)
		}
	}
}

func TestFlannelIfaceStep(t *testing.T) {
	novpn := flannelIfaceStep(true)
	if strings.Contains(novpn, "wg0") {
		t.Errorf("VPN disabled: flannel must not be pinned to wg0, got %q", novpn)
	}
	if !strings.Contains(novpn, "--iface=$(FLANNEL_NODE_IP)") || !strings.Contains(novpn, "status.hostIP") {
		t.Errorf("VPN disabled: flannel must be pinned to the node IP via the downward API, got %q", novpn)
	}
	if got := flannelIfaceStep(false); !strings.Contains(got, "--iface=wg0") || strings.Contains(got, "FLANNEL_NODE_IP") {
		t.Errorf("VPN enabled: expected --iface=wg0 pinning only, got %q", got)
	}
}
