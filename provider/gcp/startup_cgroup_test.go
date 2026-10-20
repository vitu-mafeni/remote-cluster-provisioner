package gcp

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// gceJoinSection returns the kubeadm-join block of the GCE startup script.
func gceJoinSection(t *testing.T, noVPN bool) string {
	t.Helper()
	script, err := BuildStartupScript(startupNP(noVPN, false))
	if err != nil {
		t.Fatal(err)
	}
	const begin, end = "# BEGIN kubeadm join\n", "# END kubeadm join\n"
	i, j := strings.Index(script, begin), strings.Index(script, end)
	if i < 0 || j < i {
		t.Fatal("join markers missing from the startup script")
	}
	return script[i+len(begin) : j]
}

// joinGCE runs the join block against a kubeadm that behaves like the real one on
// a cgroups v1 host (preflight fails unless SystemVerification is ignored; the
// kubelet refuses to start unless a patch sets failCgroupV1: false).
func joinGCE(t *testing.T, noVPN, v1 bool) (out string, calls string, err error) {
	t.Helper()
	if _, e := exec.LookPath("bash"); e != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	dir := t.TempDir()
	conf, cg, patches := filepath.Join(dir, "kubelet.conf"), filepath.Join(dir, "cgroup.controllers"), filepath.Join(dir, "patches")
	if !v1 {
		if e := os.WriteFile(cg, []byte("cpu\n"), 0o644); e != nil {
			t.Fatal(e)
		}
	}
	s.Write("kubeadm", `#!/bin/bash
echo "kubeadm $*" >> "$FAKE_LOG/kubeadm.calls"
case "$1" in
  join)
    if [ ! -e "$CNLAB_CGROUP_CONTROLLERS" ]; then
      case "$*" in *--ignore-preflight-errors=SystemVerification*) ;; *)
        echo "[ERROR SystemVerification]: cgroups v1 support is deprecated" >&2; exit 1 ;; esac
      pdir="$(printf '%s' "$*" | sed -n 's/.*--patches=\([^ ]*\).*/\1/p')"
      cat "$pdir"/kubeletconfiguration*.yaml 2>/dev/null | grep -qx 'failCgroupV1: false' || { echo "kubelet refuses cgroup v1" >&2; exit 1; }
    fi
    echo "server: https://10.8.0.1:6443" > "$CNLAB_KUBELET_CONF" ;;
  reset) rm -f "$CNLAB_KUBELET_CONF" ;;
esac
`)
	s.Write("kubectl", "#!/bin/bash\nexit 1\n")
	s.Write("systemctl", "#!/bin/bash\n[ \"$1\" = is-active ]\n")
	s.Write("curl", "#!/bin/bash\nexit 0\n")
	s.Write("sleep", "#!/bin/bash\n:\n")
	prelude := "set -Eeuo pipefail\nreport() { echo \"$*\"; }\nrestart_crio_and_wait() { :; }\nCRIO_SOCKET=/var/run/crio/crio.sock\n"
	cmd := exec.Command("bash", "-s")
	cmd.Env = s.Env("CNLAB_KUBELET_CONF="+conf, "CNLAB_CGROUP_CONTROLLERS="+cg, "CNLAB_PATCH_DIR="+patches)
	cmd.Stdin = strings.NewReader(prelude + gceJoinSection(t, noVPN))
	b, err := cmd.CombinedOutput()
	return string(b), s.Log("kubeadm.calls"), err
}

func TestGCEStartupScript_JoinOnCgroupsV1HostSucceedsWithTheFix(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		out, calls, err := joinGCE(t, noVPN, true)
		if err != nil {
			t.Fatalf("noVPN=%v: %v\n%s", noVPN, err, out)
		}
		if !strings.Contains(calls, "--ignore-preflight-errors=SystemVerification") || !strings.Contains(calls, "--patches=") ||
			strings.Count(calls, "kubeadm join") != 1 || strings.Contains(calls, "kubeadm reset") {
			t.Errorf("noVPN=%v: calls:\n%s", noVPN, calls)
		}
		if !strings.Contains(out, "Cluster join succeeded") {
			t.Errorf("noVPN=%v:\n%s", noVPN, out)
		}
	}
}

func TestGCEStartupScript_JoinOnCgroupsV2HostIsUnchanged(t *testing.T) {
	out, calls, err := joinGCE(t, true, false)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if strings.Contains(calls, "--ignore-preflight-errors") || strings.Contains(calls, "--patches") {
		t.Errorf("a cgroups v2 host must join plain:\n%s", calls)
	}
}
