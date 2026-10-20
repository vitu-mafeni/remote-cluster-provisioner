package argocd

import (
	"os/exec"
	"strings"
	"testing"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
)

func TestYAMLStrCannotBreakOut(t *testing.T) {
	hostile := "x\"\ninjected: true\n#$(id)`id`"
	got := yamlStr(hostile)
	if strings.Contains(got, "\n") {
		t.Fatalf("scalar must be a single line, got %q", got)
	}
	if !strings.HasPrefix(got, `"`) || !strings.HasSuffix(got, `"`) {
		t.Fatalf("scalar must be double-quoted, got %q", got)
	}
	// The embedded quote must be escaped so it cannot terminate the scalar.
	if strings.Contains(got[1:len(got)-1], "\"") && !strings.Contains(got, `\"`) {
		t.Fatalf("embedded quote not escaped: %q", got)
	}
}

func TestConfigureArgoCDRejectsBadClusterName(t *testing.T) {
	for _, name := range []string{"", "Has-Upper", "a b", "x\ny", "$(id)", "-lead", "trail-", "a;b"} {
		c := &infrav1.RemoteCluster{Spec: infrav1.RemoteClusterSpec{ClusterName: name}}
		// nil client: validation must fail before any SSH command is attempted.
		if err := ConfigureArgoCD(nil, c); err == nil || !strings.Contains(err.Error(), "spec.clusterName") {
			t.Errorf("cluster name %q: expected a clusterName validation error, got %v", name, err)
		}
	}
}

func TestApplyManifestCmdHeredocIsQuoted(t *testing.T) {
	manifest := "metadata:\n  name: " + yamlStr("$(echo PWNED) `echo PWNED` ${HOME}") + "\n"
	cmd := applyManifestCmd(manifest)
	if !strings.Contains(cmd, "<<'ARGOCD_MANIFEST_EOF'") {
		t.Fatalf("heredoc delimiter must be quoted so the body is not expanded:\n%s", cmd)
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	// Stub kubectl so it echoes the manifest it would have applied.
	script := "kubectl() { cat; }\n" + cmd + "\n"
	out, err := exec.Command(bash, "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, out)
	}
	got := string(out)
	if strings.Contains(got, "PWNED\n") && !strings.Contains(got, "$(echo PWNED)") {
		t.Fatalf("shell expanded the manifest body:\n%s", got)
	}
	for _, literal := range []string{"$(echo PWNED)", "`echo PWNED`", "${HOME}"} {
		if !strings.Contains(got, literal) {
			t.Errorf("manifest body was altered, %q missing from:\n%s", literal, got)
		}
	}
}
