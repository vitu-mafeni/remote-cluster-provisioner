package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeOSRelease(t *testing.T, id, versionID string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "os-release")
	body := "NAME=\"x\"\nID=" + id + "\nVERSION_ID=\"" + versionID + "\"\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// pulledRef returns the OCI reference passed to `oras pull`.
func pulledRef(t *testing.T, logs string) string {
	t.Helper()
	argv := strings.Split(readLog(t, logs, "oras.argv"), "---\n")
	for _, call := range argv {
		lines := strings.Split(strings.TrimSpace(call), "\n")
		if len(lines) > 0 && lines[0] == "pull" {
			for _, l := range lines {
				if strings.Contains(l, "/cnlab-runtime:") {
					return l
				}
			}
		}
	}
	return ""
}

func TestOSVariantAuto_PicksTagFromNodeOS(t *testing.T) {
	cases := []struct {
		id, ver, wantTag string
	}{
		{"ubuntu", "20.04", "1.0.2-ubuntu20"},
		{"ubuntu", "21.10", "1.0.2-ubuntu20"},
		{"ubuntu", "22.04", "1.0.2-ubuntu22"},
		{"ubuntu", "24.04", "1.0.2-ubuntu22"},
		{"ubuntu", "26.04", "1.0.2-ubuntu26"},
	}
	for _, sc := range scriptCases() {
		for _, c := range cases {
			t.Run(sc.name+"/"+c.ver, func(t *testing.T) {
				env, logs, tmp := stubEnv(t)
				env = append(env, "CNLAB_OS_RELEASE_FILE="+writeOSRelease(t, c.id, c.ver))
				cfg := Config{Version: "1.0.2", OSVariant: OSVariantAuto}
				cfg.ApplyDefaults()
				out, errOut, err := runScript(t, sc.build(cfg), env)
				if err != nil {
					t.Fatalf("script failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
				}
				want := "ghcr.io/vitu-mafeni/cnlab-runtime:" + c.wantTag
				if got := pulledRef(t, logs); got != want {
					t.Errorf("pulled %q, want %q", got, want)
				}
				assertNoLeftovers(t, tmp)
			})
		}
	}
}

func TestOSVariantAuto_RegistryWithPortKeepsHost(t *testing.T) {
	for _, sc := range scriptCases() {
		t.Run(sc.name, func(t *testing.T) {
			env, logs, _ := stubEnv(t)
			env = append(env, "CNLAB_OS_RELEASE_FILE="+writeOSRelease(t, "ubuntu", "20.04"))
			cfg := Config{Registry: "harbor.local:5000", Repository: "ml/cnlab-runtime", Version: "1.0.2", OSVariant: OSVariantAuto}
			cfg.ApplyDefaults()
			if _, errOut, err := runScript(t, sc.build(cfg), env); err != nil {
				t.Fatalf("%v\n%s", err, errOut)
			}
			if got, want := pulledRef(t, logs), "harbor.local:5000/ml/cnlab-runtime:1.0.2-ubuntu20"; got != want {
				t.Errorf("pulled %q, want %q", got, want)
			}
		})
	}
}

func TestOSVariantAuto_UnsupportedNodesFailBeforePulling(t *testing.T) {
	cases := []struct{ name, id, ver, wantMsg string }{
		{"ubuntu 18.04", "ubuntu", "18.04", "not supported"},
		{"ubuntu 16.04", "ubuntu", "16.04", "not supported"},
		{"debian", "debian", "12", "needs Ubuntu"},
		{"no version id", "ubuntu", "", "needs Ubuntu"},
	}
	for _, sc := range scriptCases() {
		for _, c := range cases {
			t.Run(sc.name+"/"+c.name, func(t *testing.T) {
				env, logs, tmp := stubEnv(t)
				env = append(env, "CNLAB_OS_RELEASE_FILE="+writeOSRelease(t, c.id, c.ver))
				cfg := Config{Version: "1.0.2", OSVariant: OSVariantAuto}
				cfg.ApplyDefaults()
				_, errOut, err := runScript(t, sc.build(cfg), env)
				if err == nil || !strings.Contains(errOut, c.wantMsg) {
					t.Fatalf("want failure containing %q, err=%v stderr=%s", c.wantMsg, err, errOut)
				}
				if got := readLog(t, logs, "oras.calls"); got != "" {
					t.Errorf("registry was contacted on an unsupported node: %q", got)
				}
				assertNoLeftovers(t, tmp)
			})
		}
	}
}

func TestOSVariantAuto_MissingOSReleaseFails(t *testing.T) {
	for _, sc := range scriptCases() {
		t.Run(sc.name, func(t *testing.T) {
			env, _, _ := stubEnv(t)
			env = append(env, "CNLAB_OS_RELEASE_FILE="+filepath.Join(t.TempDir(), "nope"))
			cfg := Config{Version: "1.0.2", OSVariant: OSVariantAuto}
			cfg.ApplyDefaults()
			_, errOut, err := runScript(t, sc.build(cfg), env)
			if err == nil || !strings.Contains(errOut, "needs Ubuntu") {
				t.Fatalf("want a clear failure, err=%v stderr=%s", err, errOut)
			}
		})
	}
}

// The idempotency check must compare the RESOLVED tag: a node that already has
// the ubuntu22 build skips, but one asked for ubuntu20 reinstalls.
func TestOSVariantAuto_IdempotencyUsesResolvedTag(t *testing.T) {
	for _, sc := range scriptCases() {
		t.Run(sc.name+"/same variant skips", func(t *testing.T) {
			env, logs, _ := stubEnv(t)
			env = append(env, "CNLAB_OS_RELEASE_FILE="+writeOSRelease(t, "ubuntu", "22.04"),
				`FAKE_HAVE=runtimeVersion: "1.0.2-ubuntu22"`)
			cfg := Config{Version: "1.0.2", OSVariant: OSVariantAuto}
			cfg.ApplyDefaults()
			if _, errOut, err := runScript(t, sc.build(cfg), env); err != nil {
				t.Fatalf("%v\n%s", err, errOut)
			}
			if readLog(t, logs, "oras.calls") != "" {
				t.Error("pull attempted although the resolved version is installed")
			}
		})
		t.Run(sc.name+"/other variant reinstalls", func(t *testing.T) {
			env, logs, _ := stubEnv(t)
			env = append(env, "CNLAB_OS_RELEASE_FILE="+writeOSRelease(t, "ubuntu", "20.04"),
				`FAKE_HAVE=runtimeVersion: "1.0.2-ubuntu22"`)
			cfg := Config{Version: "1.0.2", OSVariant: OSVariantAuto}
			cfg.ApplyDefaults()
			if _, errOut, err := runScript(t, sc.build(cfg), env); err != nil {
				t.Fatalf("%v\n%s", err, errOut)
			}
			if got := pulledRef(t, logs); !strings.HasSuffix(got, ":1.0.2-ubuntu20") {
				t.Errorf("pulled %q, want the ubuntu20 tag", got)
			}
		})
	}
}

// Without osVariant the scripts must not read os-release at all.
func TestOSVariantEmpty_ScriptsUnchanged(t *testing.T) {
	for _, s := range append(InstallSteps(Config{Version: "1.0.2-beta"}), InstallScript(Config{Version: "1.0.2-beta"})) {
		if strings.Contains(s, "os-release") || strings.Contains(s, "CNLAB_OS_") {
			t.Errorf("osVariant is empty but the script resolves the OS:\n%s", s)
		}
	}
}

func TestOSVariantAuto_ScriptsHaveValidSyntax(t *testing.T) {
	cfg := Config{Version: "1.0.2", OSVariant: OSVariantAuto, Username: "u", Token: "t"}
	for _, step := range InstallSteps(cfg) {
		bashSyntax(t, step)
	}
	bashSyntax(t, "report(){ :; }\n"+InstallScript(cfg))
}

func TestValidateOSVariant(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"auto with base version", Config{Version: "1.0.2", OSVariant: OSVariantAuto}, ""},
		{"empty variant keeps full tag", Config{Version: "1.0.2-ubuntu22"}, ""},
		{"legacy tag untouched", Config{Version: "1.0.2-beta"}, ""},
		{"unknown variant", Config{Version: "1.0.2", OSVariant: "manual"}, "not supported"},
		{"auto needs explicit version", Config{OSVariant: OSVariantAuto}, "explicit base version"},
		{"auto rejects suffixed version", Config{Version: "1.0.2-ubuntu22", OSVariant: OSVariantAuto}, "already has an OS suffix"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.cfg.Validate()
			switch {
			case c.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}
