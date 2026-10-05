package aws

import (
	"strings"
	"testing"
)

// The bootstrap script is shared with other clouds (provider/gcp). These tests
// pin the contract that keeps the AWS rendering unchanged by that reuse.

func TestRenderBootstrapScript_DefaultsKeepTheEC2NodeIPLookupAndAWSLabel(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	p.IsGPUNode = true
	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script, defaultNodeIPScript) {
		t.Error("with no override the EC2 IMDS lookup must be rendered verbatim")
	}
	if !strings.Contains(script, "ml.dcn.ssu.ac.kr/provider=AWS") {
		t.Error("the default provider label is AWS")
	}
}

func TestRenderBootstrapScript_CustomNodeIPScriptAndProviderLabel(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	p.IsGPUNode = true
	p.NodeIPScript = "NODE_IP=10.9.9.9 # no trailing newline"
	p.ProviderLabel = "GCP"
	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script, "NODE_IP=10.9.9.9 # no trailing newline\n") {
		t.Error("a custom lookup must be rendered, newline-terminated")
	}
	if strings.Contains(script, "IMDS=") || strings.Contains(script, "provider=AWS") {
		t.Error("the AWS-specific parts must be replaced, not appended to")
	}
	if !strings.Contains(script, "provider=GCP") {
		t.Error("custom provider label")
	}
	assertBashSyntax(t, script)

	// The override only matters without a VPN.
	p.NoVPN = false
	p.WGConfig, p.VpnIP = "[Interface]\n", "10.8.0.5"
	script, err = renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(script, "10.9.9.9") {
		t.Error("the node-IP override must not appear in VPN mode")
	}
}

func TestRenderBootstrapScriptExportedMatchesInternalAndValidates(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	raw, err := RenderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := renderBootstrapScript(p)
	if raw != want {
		t.Error("RenderBootstrapScript must return the same raw script the AWS user-data wraps")
	}
	bad := p
	bad.JoinCommand = "kubeadm join 10.0.0.1:6443; rm -rf /"
	if _, err := RenderBootstrapScript(bad); err == nil {
		t.Error("invalid parameters must be rejected")
	}
}
