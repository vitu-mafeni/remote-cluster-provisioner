package runtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const fakeOras = `#!/bin/bash
# Fake oras mimicking oras-go v2.5.0 config.Load: an EXISTING but EMPTY
# --registry-config file is rejected ("invalid config format"); a missing one is fine.
printf '%s\n' "$@" >> "$FAKE_LOG/oras.argv"
printf -- '---\n' >> "$FAKE_LOG/oras.argv"
cfg=""; out=""; prev=""
for a in "$@"; do
  [ "$prev" = "--registry-config" ] && cfg=$a
  [ "$prev" = "-o" ] && out=$a
  prev=$a
done
check_cfg() {
  if [ -n "$cfg" ] && [ -e "$cfg" ] && [ ! -s "$cfg" ]; then
    echo "Error: invalid config format: unexpected end of JSON input" >&2
    exit 1
  fi
}
case "$1" in
  version) echo "Version:    $FAKE_ORAS_VERSION" ;;
  login)
    check_cfg
    cat > "$FAKE_LOG/oras.stdin"
    echo login >> "$FAKE_LOG/oras.calls"
    mkdir -p "$(dirname "$cfg")"
    (umask 077; echo '{"auths":{}}' > "$cfg")
    ;;
  pull)
    check_cfg
    echo pull >> "$FAKE_LOG/oras.calls"
    mkdir -p "$out/artifact"
    if [ -z "${FAKE_NO_DEB:-}" ]; then echo deb > "$out/artifact/cnlab-runtime_1.0.0_amd64.deb"; else echo readme > "$out/artifact/README"; fi
    (cd "$out/artifact" && sha256sum * > SHA256SUMS)
    [ -z "${FAKE_BAD_SUMS:-}" ] || echo "0000000000000000000000000000000000000000000000000000000000000000  README" > "$out/artifact/SHA256SUMS"
    ;;
esac
`

// stubEnv creates a directory of fake binaries and returns the environment for
// executing a script against them plus the log dir.
func stubEnv(t *testing.T) (env []string, logDir, tmp string) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	stubs := t.TempDir()
	logDir = t.TempDir()
	tmp = t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(stubs, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("oras", fakeOras)
	write("dpkg", "#!/bin/bash\n[ \"$1\" = --print-architecture ] && { echo amd64; exit 0; }\necho \"dpkg $*\" >> \"$FAKE_LOG/dpkg.calls\"\n")
	write("apt-get", "#!/bin/bash\necho \"apt-get $*\" >> \"$FAKE_LOG/dpkg.calls\"\n[ \"${FAKE_APT_INSTALL_FAIL:-0}\" != 1 ] || [ \"$1\" != install ] || exit 100\n")
	write("cnlab-runtime", "#!/bin/bash\necho \"$*\" >> \"$FAKE_LOG/cnlab-runtime.calls\"\n[ \"$1\" = version ] && echo \"${FAKE_HAVE:-}\"\nexit 0\n")
	write("crio", "#!/bin/bash\n") // the cloud-init idempotency check needs crio on PATH
	write("sudo", "#!/bin/bash\nexec env \"$@\"\n")
	env = append(os.Environ(),
		"PATH="+stubs+":"+os.Getenv("PATH"),
		"FAKE_LOG="+logDir,
		"TMPDIR="+tmp,
		"HOME="+t.TempDir(),
		"FAKE_ORAS_VERSION="+DefaultOrasVersion,
	)
	return env, logDir, tmp
}

func runScript(t *testing.T, script string, env []string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := exec.Command("bash", "-s")
	cmd.Env = env
	cmd.Stdin = strings.NewReader(script)
	var so, se strings.Builder
	cmd.Stdout, cmd.Stderr = &so, &se
	err = cmd.Run()
	return so.String(), se.String(), err
}

func readLog(t *testing.T, dir, name string) string {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join(dir, name))
	return string(b)
}

func assertNoLeftovers(t *testing.T, tmp string) {
	t.Helper()
	left, _ := os.ReadDir(tmp)
	for _, e := range left {
		t.Errorf("temp entry left behind in TMPDIR (auth dir must be removed on every exit path): %s", e.Name())
	}
}

const testToken = "s3cr3t-registry-token-XYZ"

type scriptCase struct {
	name string
	// build returns the script under test (preamble included).
	build func(cfg Config) string
}

func scriptCases() []scriptCase {
	return []scriptCase{
		{"ssh installRuntimeCmd", func(cfg Config) string { return installRuntimeCmd(cfg) }},
		{"cloud-init InstallScript", func(cfg Config) string {
			return "set -Eeuo pipefail\nreport(){ echo \"$*\"; }\n" +
				"export CNLAB_REGISTRY_USER=" + shq(cfg.Username) + "\nexport CNLAB_REGISTRY_TOKEN=" + shq(cfg.Token) + "\n" +
				InstallScript(cfg)
		}},
	}
}

func shq(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// Regression for the empty-auth-file bug: mktemp used to create an EMPTY file
// that oras then refused to load, so login/pull always failed.
func TestInstallScriptsExecute_WithTokenAndPublicRegistry(t *testing.T) {
	for _, sc := range scriptCases() {
		t.Run(sc.name+"/with token", func(t *testing.T) {
			env, logs, tmp := stubEnv(t)
			cfg := Config{Username: "robot", Token: testToken}
			cfg.ApplyDefaults()
			out, errOut, err := runScript(t, sc.build(cfg), env)
			if err != nil {
				t.Fatalf("script failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
			}
			if calls := readLog(t, logs, "oras.calls"); calls != "login\npull\n" {
				t.Errorf("oras calls = %q, want login then pull", calls)
			}
			if got := readLog(t, logs, "oras.stdin"); got != testToken {
				t.Errorf("token must reach oras login via stdin, got %q", got)
			}
			if argv := readLog(t, logs, "oras.argv"); strings.Contains(argv, testToken) {
				t.Errorf("token leaked into oras argv:\n%s", argv)
			} else if !strings.Contains(argv, "--password-stdin") || !strings.Contains(argv, "--username\nrobot") {
				t.Errorf("unexpected login argv:\n%s", argv)
			}
			installCalls := readLog(t, logs, "dpkg.calls")
			if !strings.Contains(installCalls, "apt-get install -y") || !strings.Contains(installCalls, "cnlab-runtime_1.0.0_amd64.deb") {
				t.Errorf("deb was not installed through apt:\n%s", installCalls)
			}
			if strings.Contains(installCalls, "dpkg -i") || strings.Contains(installCalls, "apt-get install -f") {
				t.Errorf("runtime install must not leave dependency resolution to a later repair:\n%s", installCalls)
			}
			assertNoLeftovers(t, tmp)
		})
		t.Run(sc.name+"/public registry", func(t *testing.T) {
			env, logs, tmp := stubEnv(t)
			cfg := Config{}
			cfg.ApplyDefaults()
			out, errOut, err := runScript(t, sc.build(cfg), env)
			if err != nil {
				t.Fatalf("script failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
			}
			if calls := readLog(t, logs, "oras.calls"); calls != "pull\n" {
				t.Errorf("public registry must not log in, calls = %q", calls)
			}
			assertNoLeftovers(t, tmp)
		})
		t.Run(sc.name+"/already installed skips everything", func(t *testing.T) {
			env, logs, _ := stubEnv(t)
			cfg := Config{}
			cfg.ApplyDefaults()
			env = append(env, "FAKE_HAVE="+cfg.Version)
			if _, errOut, err := runScript(t, sc.build(cfg), env); err != nil {
				t.Fatalf("%v\n%s", err, errOut)
			}
			if readLog(t, logs, "oras.calls") != "" {
				t.Error("pull attempted although the requested version is installed")
			}
		})
		t.Run(sc.name+"/checksum failure cleans up and fails", func(t *testing.T) {
			env, _, tmp := stubEnv(t)
			env = append(env, "FAKE_BAD_SUMS=1", "FAKE_NO_DEB=1")
			cfg := Config{Username: "robot", Token: testToken}
			cfg.ApplyDefaults()
			_, errOut, err := runScript(t, sc.build(cfg), env)
			if err == nil || !strings.Contains(errOut, "checksum verification FAILED") {
				t.Fatalf("expected checksum failure, err=%v stderr=%s", err, errOut)
			}
			assertNoLeftovers(t, tmp)
		})
	}
}

// Regression: under set -e + pipefail a no-match `ls | head` aborted the script
// before the friendly message.
func TestInstallScriptsExecute_NoDebPrintsFriendlyError(t *testing.T) {
	for _, sc := range scriptCases() {
		t.Run(sc.name, func(t *testing.T) {
			env, _, tmp := stubEnv(t)
			env = append(env, "FAKE_NO_DEB=1")
			cfg := Config{}
			cfg.ApplyDefaults()
			_, errOut, err := runScript(t, sc.build(cfg), env)
			if err == nil {
				t.Fatal("expected failure")
			}
			if !strings.Contains(errOut, "no .deb found in pulled artifact") {
				t.Errorf("friendly error not printed, stderr:\n%s", errOut)
			}
			assertNoLeftovers(t, tmp)
		})
	}
}

func TestInstallScriptsStopWhenAptCannotInstallRuntime(t *testing.T) {
	for _, sc := range scriptCases() {
		t.Run(sc.name, func(t *testing.T) {
			env, logs, tmp := stubEnv(t)
			env = append(env, "FAKE_APT_INSTALL_FAIL=1")
			cfg := Config{}
			cfg.ApplyDefaults()
			out, errOut, err := runScript(t, sc.build(cfg), env)
			if err == nil {
				t.Fatalf("expected apt installation failure, stdout:\n%s\nstderr:\n%s", out, errOut)
			}
			if calls := readLog(t, logs, "cnlab-runtime.calls"); strings.Count(calls, "version\n") != 1 {
				t.Errorf("runtime must not be queried after apt installation fails, calls:\n%s", calls)
			}
			assertNoLeftovers(t, tmp)
		})
	}
}
