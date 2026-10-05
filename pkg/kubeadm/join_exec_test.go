package kubeadm

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

const testJoinCmd = "kubeadm join 10.8.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:00"

type joinEnv struct {
	*sshtest.Stubs
	conf string // stand-in for /etc/kubernetes/kubelet.conf
}

func newJoinEnv(t *testing.T) *joinEnv {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	j := &joinEnv{Stubs: s, conf: filepath.Join(t.TempDir(), "kubelet.conf")}
	s.Write("kubeadm", `#!/bin/bash
echo "kubeadm $1" >> "$FAKE_LOG/kubeadm.calls"
case "$1" in
  join)
    n=$(cat "$FAKE_LOG/join.count" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$FAKE_LOG/join.count"
    # a real join refuses to run over an existing kubelet.conf
    if [ -e "$CNLAB_KUBELET_CONF" ]; then echo "[preflight] FileAvailable--etc-kubernetes-kubelet.conf already exists" >&2; exit 1; fi
    if [ "$n" -le "${FAKE_JOIN_FAILS:-0}" ]; then
      [ -z "${FAKE_JOIN_WRITES_CONF:-}" ] || echo "server: https://10.8.0.1:6443" > "$CNLAB_KUBELET_CONF"
      exit 1
    fi
    echo "server: https://10.8.0.1:6443" > "$CNLAB_KUBELET_CONF"
    ;;
  reset) rm -f "$CNLAB_KUBELET_CONF" ;;
esac
`)
	s.Write("kubectl", `#!/bin/bash
echo "kubectl $*" >> "$FAKE_LOG/kubectl.calls"
[ "${FAKE_API_HEALTHY:-1}" = 1 ] && { echo ok; exit 0; }
exit 1
`)
	s.Write("systemctl", `#!/bin/bash
[ "$1" = is-active ] && [ "${FAKE_KUBELET_ACTIVE:-1}" = 1 ]
`)
	s.Write("curl", "#!/bin/bash\nexit 0\n")
	s.Write("sleep", "#!/bin/bash\necho \"sleep $*\" >> \"$FAKE_LOG/sleep.calls\"\n")
	return j
}

func (j *joinEnv) env(extra ...string) []string {
	return j.Env(append([]string{"CNLAB_KUBELET_CONF=" + j.conf}, extra...)...)
}

// runSteps executes steps the way the SSH runner does (one `bash -s` per step,
// under pipefail), stopping at the first failing step.
func runSteps(t *testing.T, steps []string, env []string) (out string, err error) {
	t.Helper()
	for _, st := range steps {
		cmd := exec.Command("bash", "-s")
		cmd.Env = env
		cmd.Stdin = strings.NewReader(pipefailPrefix + st)
		b, e := cmd.CombinedOutput()
		out += string(b)
		if e != nil {
			return out, e
		}
	}
	return out, nil
}

func count(log, sub string) int { return strings.Count(log, sub) }

func TestJoinWorkerSteps_FreshNodeJoinsWithoutReset(t *testing.T) {
	j := newJoinEnv(t)
	out, err := runSteps(t, joinWorkerSteps(testJoinCmd), j.env())
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	calls := j.Log("kubeadm.calls")
	if count(calls, "kubeadm join") != 1 || count(calls, "kubeadm reset") != 0 {
		t.Errorf("fresh node: want exactly one join and no reset, got:\n%s", calls)
	}
	if !strings.Contains(out, "kubeadm join succeeded") {
		t.Errorf("output:\n%s", out)
	}
}

func TestJoinWorkerSteps_FailedAttemptIsResetThenRetried(t *testing.T) {
	j := newJoinEnv(t)
	out, err := runSteps(t, joinWorkerSteps(testJoinCmd), j.env("FAKE_JOIN_FAILS=1"))
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	calls := strings.Fields(strings.ReplaceAll(j.Log("kubeadm.calls"), "kubeadm ", ""))
	if strings.Join(calls, ",") != "join,reset,join" {
		t.Errorf("want join,reset,join; got %v", calls)
	}
	if !strings.Contains(j.Log("sleep.calls"), "sleep 15") {
		t.Errorf("no back-off between attempts: %q", j.Log("sleep.calls"))
	}
}

func TestJoinWorkerSteps_GivesUpAfterFiveAttempts(t *testing.T) {
	j := newJoinEnv(t)
	out, err := runSteps(t, joinWorkerSteps(testJoinCmd), j.env("FAKE_JOIN_FAILS=99"))
	if err == nil || !strings.Contains(out, "failed after 5 attempts") {
		t.Fatalf("expected failure after 5 attempts, err=%v\n%s", err, out)
	}
	calls := j.Log("kubeadm.calls")
	if count(calls, "kubeadm join") != 5 || count(calls, "kubeadm reset") != 4 {
		t.Errorf("want 5 joins and 4 resets:\n%s", calls)
	}
}

func TestJoinWorkerSteps_AlreadyJoinedNodeIsNeverResetOrRejoined(t *testing.T) {
	j := newJoinEnv(t)
	if err := os.WriteFile(j.conf, []byte("server: https://10.8.0.1:6443\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runSteps(t, joinWorkerSteps(testJoinCmd), j.env())
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if calls := j.Log("kubeadm.calls"); calls != "" {
		t.Errorf("healthy joined node: kubeadm must not be called at all, got:\n%s", calls)
	}
	if !strings.Contains(out, "already joined") {
		t.Errorf("skip must be logged:\n%s", out)
	}
	if !strings.Contains(j.Log("kubectl.calls"), "--kubeconfig "+j.conf) || !strings.Contains(j.Log("kubectl.calls"), "get --raw /healthz") {
		t.Errorf("health must be verified against the API with kubelet.conf: %q", j.Log("kubectl.calls"))
	}
	if _, err := os.Stat(j.conf); err != nil {
		t.Error("kubelet.conf must survive")
	}
}

func TestJoinWorkerSteps_NotSkippedWhenKubeletOrAPIUnhealthyOrOtherCluster(t *testing.T) {
	for name, tc := range map[string]struct {
		conf  string
		extra []string
	}{
		"api unhealthy":   {"server: https://10.8.0.1:6443\n", []string{"FAKE_API_HEALTHY=0"}},
		"kubelet stopped": {"server: https://10.8.0.1:6443\n", []string{"FAKE_KUBELET_ACTIVE=0"}},
		"other cluster":   {"server: https://192.168.9.9:6443\n", nil},
	} {
		t.Run(name, func(t *testing.T) {
			j := newJoinEnv(t)
			_ = os.WriteFile(j.conf, []byte(tc.conf), 0o600)
			out, err := runSteps(t, joinWorkerSteps(testJoinCmd), j.env(tc.extra...))
			// The stale conf makes the first join fail; the reset (allowed: node is
			// not healthily joined to THIS cluster) clears it and the retry works.
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			calls := strings.Fields(strings.ReplaceAll(j.Log("kubeadm.calls"), "kubeadm ", ""))
			if strings.Join(calls, ",") != "join,reset,join" {
				t.Errorf("%s: want join,reset,join; got %v", name, calls)
			}
		})
	}
}

func TestJoinWorkerSteps_JoinErrorAfterKubeletRegisteredIsNotReset(t *testing.T) {
	j := newJoinEnv(t)
	out, err := runSteps(t, joinWorkerSteps(testJoinCmd), j.env("FAKE_JOIN_FAILS=1", "FAKE_JOIN_WRITES_CONF=1"))
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	calls := j.Log("kubeadm.calls")
	if count(calls, "kubeadm reset") != 0 || count(calls, "kubeadm join") != 1 {
		t.Errorf("a healthily joined node must not be reset after a late join error:\n%s", calls)
	}
	if !strings.Contains(out, "not resetting") {
		t.Errorf("output:\n%s", out)
	}
}

func TestJoinEndpoint(t *testing.T) {
	for in, want := range map[string]string{
		testJoinCmd:                             "10.8.0.1:6443",
		"kubeadm join [fd00::1]:6443 --token a": "[fd00::1]:6443",
		"kubeadm join $(id):6443 --token a":     "",
		"kubeadm join":                          "",
		"kubeadm join host":                     "",
	} {
		if got := JoinEndpoint(in); got != want {
			t.Errorf("JoinEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

func envPath() string { return os.Getenv("PATH") }
