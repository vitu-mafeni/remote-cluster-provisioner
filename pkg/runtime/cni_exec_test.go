package runtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCurl mimics the one curl call the CNI snippet makes: it records argv and
// writes a tarball of the standard plugins to the -o target, or fails like
// `curl -f` on an HTTP error when FAKE_CURL_FAIL is set.
const fakeCurl = `#!/bin/bash
printf '%s\n' "$@" >> "$FAKE_LOG/curl.argv"
out=""; prev=""
for a in "$@"; do [ "$prev" = "-o" ] && out=$a; prev=$a; done
[ -z "${FAKE_CURL_FAIL:-}" ] || exit 22
w=$(mktemp -d)
for p in portmap bridge host-local loopback flannel; do printf '#!/bin/sh\n' > "$w/$p"; chmod 755 "$w/$p"; done
tar -czf "$out" -C "$w" .
rm -rf "$w"
`

func cniEnv(t *testing.T) (env []string, logDir, tmp, bin string) {
	t.Helper()
	env, logDir, tmp = stubEnv(t)
	stubs := ""
	for _, e := range env {
		if strings.HasPrefix(e, "PATH=") {
			stubs = strings.SplitN(strings.TrimPrefix(e, "PATH="), ":", 2)[0]
		}
	}
	if err := os.WriteFile(filepath.Join(stubs, "curl"), []byte(fakeCurl), 0o755); err != nil {
		t.Fatal(err)
	}
	bin = filepath.Join(t.TempDir(), "cni-bin")
	env = append(env, "CNI_BIN="+bin)
	return env, logDir, tmp, bin
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func TestCNIPluginsInstallScript_FreshInstall(t *testing.T) {
	env, logDir, tmp, bin := cniEnv(t)
	if _, se, err := runScript(t, CNIPluginsInstallScript(), env); err != nil {
		t.Fatalf("install failed: %v\n%s", err, se)
	}
	for _, p := range []string{"portmap", "bridge", "host-local", "loopback"} {
		if !exists(filepath.Join(bin, p)) {
			t.Errorf("%s not installed into %s", p, bin)
		}
	}
	argv := readLog(t, logDir, "curl.argv")
	if !strings.Contains(argv, CNIPluginsVersion) || !strings.Contains(argv, "cni-plugins-linux-amd64-") {
		t.Errorf("unexpected download URL/arch: %s", argv)
	}
	if strings.Contains(argv, "/tmp/cni-plugins.tgz") {
		t.Errorf("download must not use the fixed /tmp path: %s", argv)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "*")); len(left) != 0 {
		t.Errorf("temp files left behind in TMPDIR: %v", left)
	}
}

func TestCNIPluginsInstallScript_AlreadyInstalledIsNoOp(t *testing.T) {
	env, logDir, _, bin := cniEnv(t)
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"portmap", "bridge", "host-local", "loopback"} {
		if err := os.WriteFile(filepath.Join(bin, p), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Even with a download that would fail, an existing install must succeed
	// without touching the network.
	env = append(env, "FAKE_CURL_FAIL=1")
	if _, se, err := runScript(t, CNIPluginsInstallScript(), env); err != nil {
		t.Fatalf("already-installed run must not fail: %v\n%s", err, se)
	}
	if exists(filepath.Join(logDir, "curl.argv")) {
		t.Error("curl was called although all plugins were already present")
	}
}

func TestCNIPluginsInstallScript_RerunAfterFullInstall(t *testing.T) {
	env, _, _, _ := cniEnv(t)
	for i := 0; i < 3; i++ {
		if _, se, err := runScript(t, CNIPluginsInstallScript(), env); err != nil {
			t.Fatalf("run %d failed: %v\n%s", i+1, err, se)
		}
	}
}

func TestCNIPluginsInstallScript_PartialInstallIsCompleted(t *testing.T) {
	env, _, _, bin := cniEnv(t)
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	// Only portmap present (e.g. an interrupted earlier run): the rest is
	// installed and the existing file is overwritten without error.
	if err := os.WriteFile(filepath.Join(bin, "portmap"), []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, se, err := runScript(t, CNIPluginsInstallScript(), env); err != nil {
		t.Fatalf("partial install not completed: %v\n%s", err, se)
	}
	if !exists(filepath.Join(bin, "loopback")) {
		t.Error("missing plugins were not installed")
	}
}

func TestCNIPluginsInstallScript_StaleFixedTmpFileDoesNotMatter(t *testing.T) {
	env, _, tmp, bin := cniEnv(t)
	// A leftover file at the old fixed path (possibly owned by another user,
	// which fails under fs.protected_regular=2) must be irrelevant.
	stale := filepath.Join(tmp, "cni-plugins.tgz")
	if err := os.WriteFile(stale, []byte("stale"), 0o000); err != nil {
		t.Fatal(err)
	}
	if _, se, err := runScript(t, CNIPluginsInstallScript(), env); err != nil {
		t.Fatalf("install failed despite stale tmp file: %v\n%s", err, se)
	}
	if !exists(filepath.Join(bin, "portmap")) {
		t.Error("portmap not installed")
	}
}

func TestCNIPluginsInstallScript_RealDownloadFailureStillFails(t *testing.T) {
	env, _, tmp, _ := cniEnv(t)
	env = append(env, "FAKE_CURL_FAIL=1")
	_, se, err := runScript(t, CNIPluginsInstallScript(), env)
	if err == nil {
		t.Fatal("a failed download with nothing installed must fail the step")
	}
	if !strings.Contains(se, "installing CNI plugins") {
		t.Errorf("error should explain the failure, got: %q", se)
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, "*")); len(left) != 0 {
		t.Errorf("temp dir not cleaned up after failure: %v", left)
	}
}

func TestCNIPluginsInstallScript_SyntaxAndSteps(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	for name, script := range map[string]string{
		"standalone": CNIPluginsInstallScript(),
		"cloud-init": InstallScript(Config{}),
	} {
		cmd := exec.Command("bash", "-n")
		cmd.Stdin = strings.NewReader(script)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("%s: bash -n failed: %v\n%s", name, err, out)
		}
	}
	found := false
	for _, s := range InstallSteps(Config{}) {
		if strings.Contains(s, "portmap") {
			found = true
		}
	}
	if !found {
		t.Error("InstallSteps must include the CNI plugins step")
	}
	if !strings.Contains(InstallScript(Config{}), "portmap") {
		t.Error("InstallScript (cloud-init) must include the CNI plugins step")
	}
}
