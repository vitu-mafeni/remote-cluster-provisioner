package aws

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// joinSection returns the kubeadm-join part of the rendered bootstrap script (the
// span between the BEGIN/END markers) so it can be EXECUTED in isolation.
func joinSection(t *testing.T) string {
	t.Helper()
	p := baseParams()
	p.NoVPN = true
	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	const begin, end = "# BEGIN kubeadm join\n", "# END kubeadm join\n"
	i, j := strings.Index(script, begin), strings.Index(script, end)
	if i < 0 || j < i {
		t.Fatal("join section markers missing from the bootstrap template")
	}
	return script[i+len(begin) : j]
}

type joinRun struct {
	*sshtest.Stubs
	conf string
}

func newJoinRun(t *testing.T) *joinRun {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	s.Write("kubeadm", `#!/bin/bash
echo "kubeadm $1" >> "$FAKE_LOG/kubeadm.calls"
case "$1" in
  join)
    n=$(cat "$FAKE_LOG/join.count" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$FAKE_LOG/join.count"
    if [ -e "$CNLAB_KUBELET_CONF" ]; then echo "already exists" >&2; exit 1; fi
    if [ "$n" -le "${FAKE_JOIN_FAILS:-0}" ]; then exit 1; fi
    echo "server: https://10.0.0.1:6443" > "$CNLAB_KUBELET_CONF" ;;
  reset) rm -f "$CNLAB_KUBELET_CONF" ;;
esac
`)
	s.Write("kubectl", "#!/bin/bash\n[ \"${FAKE_API_HEALTHY:-1}\" = 1 ]\n")
	s.Write("systemctl", "#!/bin/bash\n[ \"$1\" = is-active ]\n")
	s.Write("curl", "#!/bin/bash\nexit 0\n")
	s.Write("sleep", "#!/bin/bash\n:\n")
	return &joinRun{Stubs: s, conf: filepath.Join(t.TempDir(), "kubelet.conf")}
}

func (j *joinRun) run(t *testing.T, extra ...string) (string, error) {
	t.Helper()
	prelude := "set -Eeuo pipefail\nreport() { echo \"$*\"; }\nrestart_crio_and_wait() { echo restart >> \"$FAKE_LOG/restart.calls\"; }\nCRIO_SOCKET=/var/run/crio/crio.sock\n"
	cmd := exec.Command("bash", "-s")
	cmd.Env = j.Env(append([]string{"CNLAB_KUBELET_CONF=" + j.conf}, extra...)...)
	cmd.Stdin = strings.NewReader(prelude + joinSection(t))
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func callSeq(j *joinRun) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(j.Log("kubeadm.calls"), "kubeadm ", "")), ",")
}

func TestCloudInitJoin_FreshNodeJoinsWithoutReset(t *testing.T) {
	j := newJoinRun(t)
	out, err := j.run(t)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if got := callSeq(j); got != "join" {
		t.Errorf("calls = %q, want just join", got)
	}
}

func TestCloudInitJoin_FailedAttemptIsResetThenRetried(t *testing.T) {
	j := newJoinRun(t)
	out, err := j.run(t, "FAKE_JOIN_FAILS=1")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if got := callSeq(j); got != "join,reset,join" {
		t.Errorf("calls = %q, want join,reset,join", got)
	}
	if !strings.Contains(j.Log("restart.calls"), "restart") {
		t.Error("CRI-O must be restarted between attempts")
	}
}

func TestCloudInitJoin_GivesUpAfterFiveAttempts(t *testing.T) {
	j := newJoinRun(t)
	out, err := j.run(t, "FAKE_JOIN_FAILS=99")
	if err == nil || !strings.Contains(out, "failed after 5 attempts") {
		t.Fatalf("expected failure, err=%v\n%s", err, out)
	}
	if got := callSeq(j); got != "join,reset,join,reset,join,reset,join,reset,join" {
		t.Errorf("calls = %q", got)
	}
}

func TestCloudInitJoin_AlreadyJoinedNodeSkipsJoinAndNeverResets(t *testing.T) {
	j := newJoinRun(t)
	if err := os.WriteFile(j.conf, []byte("server: https://10.0.0.1:6443\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := j.run(t)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if calls := j.Log("kubeadm.calls"); calls != "" {
		t.Errorf("kubeadm must not run on a healthily joined node:\n%s", calls)
	}
	if !strings.Contains(out, "already joined") {
		t.Errorf("skip must be logged:\n%s", out)
	}
	if _, err := os.Stat(j.conf); err != nil {
		t.Error("kubelet.conf must survive")
	}
}

func TestCloudInitJoin_UnhealthyJoinedNodeIsRejoined(t *testing.T) {
	j := newJoinRun(t)
	_ = os.WriteFile(j.conf, []byte("server: https://10.0.0.1:6443\n"), 0o600)
	out, err := j.run(t, "FAKE_API_HEALTHY=0")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if got := callSeq(j); got != "join,reset,join" {
		t.Errorf("an unhealthy node must go through reset and rejoin, got %q", got)
	}
}
