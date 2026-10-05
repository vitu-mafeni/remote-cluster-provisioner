package runtime

import (
	"os/exec"
	"strings"
	"testing"
)

func bashSyntax(t *testing.T, script string) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n failed: %v\n%s\n--- script ---\n%s", err, out, script)
	}
}

func TestInstallScriptsSyntaxAndQuoting(t *testing.T) {
	cfg := Config{Username: "us'er", Token: `tok'en$(id)"` + "`x`"}
	for i, step := range InstallSteps(cfg) {
		bashSyntax(t, step)
		if i == 1 {
			if strings.Contains(step, `tok'en`) {
				t.Errorf("token appears unquoted in step:\n%s", step)
			}
			if !strings.Contains(step, "--registry-config") || !strings.Contains(step, "trap 'rm -rf \"$AUTH_DIR\"' EXIT") || strings.Contains(step, "AUTH=$(mktemp)") {
				t.Errorf("login must use a private auth dir removed on every exit path (executed in install_exec_test.go)")
			}
		}
	}
	bashSyntax(t, "report(){ :; }\n"+InstallScript(cfg))
}

func TestInstallScriptChecksumVerification(t *testing.T) {
	steps := InstallSteps(Config{})
	for _, s := range []string{steps[0], InstallScript(Config{})} {
		if !strings.Contains(s, "_checksums.txt") || !strings.Contains(s, "sha256sum -c") {
			t.Errorf("ORAS tarball is not verified against the release checksums:\n%s", s)
		}
	}
}

func TestValidateRejectsInjection(t *testing.T) {
	good := Config{}
	if err := good.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	for _, bad := range []Config{
		{Registry: "ghcr.io'; touch /tmp/x #"},
		{Repository: "a/b c"},
		{Version: "1.0'$(id)"},
		{OrasVersion: "1.3.2; id"},
		{Registry: "ghcr.io\nX=1"},
	} {
		if err := bad.Validate(); err == nil {
			t.Errorf("Validate accepted %+v", bad)
		}
	}
	for _, ok := range []Config{
		{Registry: "harbor.example.com:8443", Repository: "team/cnlab-runtime", Version: "1.0.0-beta", OrasVersion: "1.3.2"},
	} {
		if err := ok.Validate(); err != nil {
			t.Errorf("Validate rejected %+v: %v", ok, err)
		}
	}
}

func TestInjectedValuesAreQuoted(t *testing.T) {
	cfg := Config{Version: "1.0'; touch /tmp/pwned; echo '"}
	for _, s := range []string{InstallSteps(cfg)[1], InstallScript(cfg)} {
		if strings.Contains(s, "VERSION='1.0'; touch") {
			t.Errorf("version breaks out of its quoting:\n%s", s)
		}
	}
	bashSyntax(t, InstallSteps(cfg)[1])
}
