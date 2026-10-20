package aws

import (
	"os/exec"
	"strings"
	"testing"
)

func baseParams() CloudInitParams {
	return CloudInitParams{
		JoinCommand:            "kubeadm join 10.0.0.1:6443 --token abc.def --discovery-token-ca-cert-hash sha256:00",
		KubernetesVersion:      "1.35.0",
		KubernetesMinorVersion: "1.35",
		NodeName:               "worker-1",
	}
}

func TestRenderBootstrapScript_NoVPN(t *testing.T) {
	p := baseParams()
	p.NoVPN = true

	if err := validateParams(p); err != nil {
		t.Fatalf("NoVPN must not require WGConfig/VpnIP: %v", err)
	}
	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"wg-quick", "/etc/wireguard", "Configuring WireGuard", " wireguard ", "VPN tunnel", "wg0"} {
		if strings.Contains(script, unwanted) {
			t.Errorf("NoVPN script still contains %q", unwanted)
		}
	}
	for _, wanted := range []string{"meta-data/local-ipv4", `--node-ip=${NODE_IP}`} {
		if !strings.Contains(script, wanted) {
			t.Errorf("NoVPN script missing %q", wanted)
		}
	}
	assertBashSyntax(t, script)
}

func TestRenderBootstrapScript_VPN(t *testing.T) {
	p := baseParams()
	p.WGConfig = "[Interface]\nAddress = 10.8.0.5/24\n"
	p.VpnIP = "10.8.0.5"

	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{"wg-quick@wg0", "WireGuard is ready", " wireguard ", "reachable over the VPN tunnel"} {
		if !strings.Contains(script, wanted) {
			t.Errorf("VPN script missing %q", wanted)
		}
	}
	if strings.Contains(script, "meta-data/local-ipv4") {
		t.Error("VPN script must not resolve the node IP from instance metadata")
	}
	assertBashSyntax(t, script)
}

func TestValidateParams_VPNRequiresWireGuardInputs(t *testing.T) {
	if err := validateParams(baseParams()); err == nil {
		t.Fatal("expected error when neither NoVPN nor WGConfig/VpnIP is set")
	}
}

func assertBashSyntax(t *testing.T, script string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	cmd := exec.Command(bash, "-n")
	cmd.Stdin = strings.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rendered script has a bash syntax error: %v\n%s", err, out)
	}
}

func TestRequiredIngressRules(t *testing.T) {
	has := func(rules []ingressRule, proto string, port int32) bool {
		for _, r := range rules {
			if r.proto == proto && r.port == port {
				return true
			}
		}
		return false
	}
	with := requiredIngressRules(true)
	without := requiredIngressRules(false)
	if !has(with, "tcp", 22) || !has(with, "udp", 51820) {
		t.Errorf("VPN cluster needs SSH and WireGuard: %v", with)
	}
	if !has(without, "tcp", 22) || has(without, "udp", 51820) {
		t.Errorf("VPN-less cluster needs SSH but no WireGuard: %v", without)
	}
}
