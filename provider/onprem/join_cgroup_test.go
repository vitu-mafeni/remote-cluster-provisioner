package onprem

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// runJoinStep EXECUTES the on-prem join step against a kubeadm stub that behaves
// like the real one on a cgroups v1 host (SystemVerification preflight fails unless
// ignored; the kubelet refuses to start unless a patch sets failCgroupV1: false).
func runJoinStep(t *testing.T, version string, v1 bool) (out, kubeadmCalls string, err error) {
	t.Helper()
	if _, e := exec.LookPath("bash"); e != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	dir := t.TempDir()
	cg := filepath.Join(dir, "cgroup.controllers")
	if !v1 {
		if e := os.WriteFile(cg, []byte("cpu\n"), 0o644); e != nil {
			t.Fatal(e)
		}
	}
	s.Write("kubeadm", `#!/bin/bash
echo "kubeadm $*" >> "$FAKE_LOG/kubeadm.calls"
if [ "$1" = join ] && [ ! -e "$CNLAB_CGROUP_CONTROLLERS" ] && [ "${FAKE_K8S_MINOR:-35}" -ge 35 ]; then
  case "$*" in *--ignore-preflight-errors=SystemVerification*) ;; *)
    echo "[ERROR SystemVerification]: cgroups v1 support is deprecated" >&2; exit 1 ;; esac
  pdir="$(printf '%s' "$*" | sed -n 's/.*--patches=\([^ ]*\).*/\1/p')"
  cat "$pdir"/kubeletconfiguration*.yaml 2>/dev/null | grep -qx 'failCgroupV1: false' || { echo "kubelet refuses cgroup v1" >&2; exit 1; }
fi
exit 0
`)
	minor := strings.Split(version, ".")[1]
	cmd := exec.Command("bash", "-s")
	cmd.Env = s.Env("CNLAB_CGROUP_CONTROLLERS="+cg, "CNLAB_PATCH_DIR="+filepath.Join(dir, "patches"), "FAKE_K8S_MINOR="+minor, "FAKE_SUDO_QUIET=1")
	// The step is run by the provisioner as "set -o pipefail\n"+command.
	cmd.Stdin = strings.NewReader("set -o pipefail\n" + joinNodeCommand(version, "  "+flowJoinCmd+"\n"))
	b, e := cmd.CombinedOutput()
	return string(b), s.Log("kubeadm.calls"), e
}

func TestJoinNodeCommand_CgroupsV1WithKubernetes135JoinsWithTheFix(t *testing.T) {
	for _, v := range []string{"1.35.0", "1.36.2"} {
		out, calls, err := runJoinStep(t, v, true)
		if err != nil {
			t.Fatalf("%s: %v\n%s", v, err, out)
		}
		for _, want := range []string{flowJoinCmd, "--ignore-preflight-errors=SystemVerification", "--patches=", "--cri-socket=unix:///var/run/crio/crio.sock"} {
			if !strings.Contains(calls, want) {
				t.Errorf("%s: kubeadm call missing %q:\n%s", v, want, calls)
			}
		}
	}
}

func TestJoinNodeCommand_OtherHostsJoinPlain(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version string
		v1      bool
	}{{"cgroups v2", "1.35.0", false}, {"k8s 1.34 on cgroups v1", "1.34.9", true}} {
		out, calls, err := runJoinStep(t, tc.version, tc.v1)
		if err != nil {
			t.Fatalf("%s: %v\n%s", tc.name, err, out)
		}
		if strings.Contains(calls, "--ignore-preflight-errors") || strings.Contains(calls, "--patches") {
			t.Errorf("%s must join plain:\n%s", tc.name, calls)
		}
		if !strings.Contains(calls, flowJoinCmd+" --cri-socket=unix:///var/run/crio/crio.sock") {
			t.Errorf("%s: the join itself must be unchanged:\n%s", tc.name, calls)
		}
	}
}

// Without the version the join would be plain and fail on cgroups v1, which is
// exactly the field failure; the version is always the cleaned cluster version.
func TestJoinNodeCommand_WithoutTheFixTheV1PreflightFails(t *testing.T) {
	if _, e := exec.LookPath("bash"); e != nil {
		t.Skip("bash not available")
	}
	_, _, err := runJoinStep(t, "1.35.0", true)
	if err != nil {
		t.Fatal(err)
	}
	// The same host with the pre-fix command (no snippet) fails the preflight.
	s := sshtest.NewStubs(t)
	s.Write("kubeadm", "#!/bin/bash\necho '[ERROR SystemVerification]: cgroups v1 support is deprecated' >&2; exit 1\n")
	cmd := exec.Command("bash", "-s")
	cmd.Env = s.Env("FAKE_SUDO_QUIET=1")
	cmd.Stdin = strings.NewReader("set -o pipefail\nsudo timeout 600 " + flowJoinCmd + " --cri-socket=unix:///var/run/crio/crio.sock\n")
	if out, e := cmd.CombinedOutput(); e == nil || !strings.Contains(string(out), "cgroups v1 support is deprecated") {
		t.Fatalf("the pre-fix command must fail on cgroups v1: %v\n%s", e, out)
	}
}
