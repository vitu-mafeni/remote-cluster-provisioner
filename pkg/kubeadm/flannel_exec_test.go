package kubeadm

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// flannelEnv fakes the flannel DaemonSet: its container args and env names live
// in files that the stub kubectl reads for `get` and rewrites for the JSON
// patches the step issues, so the step can be run repeatedly against state.
type flannelEnv struct {
	*sshtest.Stubs
	args, envNames string
}

func newFlannelEnv(t *testing.T) *flannelEnv {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	f := &flannelEnv{Stubs: s, args: filepath.Join(s.LogDir, "ds.args"), envNames: filepath.Join(s.LogDir, "ds.env")}
	f.resetArgs()
	_ = os.WriteFile(f.envNames, []byte("POD_NAME POD_NAMESPACE EVENT_QUEUE_DEPTH\n"), 0o644)
	s.Write("kubectl", `#!/bin/bash
echo "kubectl $*" >> "$FAKE_LOG/kubectl.calls"
all="$*"
case "$all" in
  *"get daemonset"*"env[*].name"*) cat "$FAKE_LOG/ds.env" ;;
  *"get daemonset"*"args"*) cat "$FAKE_LOG/ds.args" ;;
  *"patch daemonset"*)
    case "$all" in *"args/-"*) echo "$all" | grep -o -- '--iface=[^"]*' | head -1 >> "$FAKE_LOG/ds.args" ;; esac
    case "$all" in *"env/-"*) echo "FLANNEL_NODE_IP" >> "$FAKE_LOG/ds.env" ;; esac
    ;;
esac
exit 0
`)
	return f
}

// resetArgs restores the upstream manifest's args (what `kubectl apply` does).
func (f *flannelEnv) resetArgs() {
	_ = os.WriteFile(f.args, []byte(`["--ip-masq","--kube-subnet-mgr"]`+"\n"), 0o644)
}

// run executes the step like the CNI phase does and returns the CHANGED flag
// and the kubectl calls made by this run.
func (f *flannelEnv) run(t *testing.T, disableVPN bool) (changed string, calls string) {
	t.Helper()
	_ = os.Remove(filepath.Join(f.LogDir, "kubectl.calls"))
	out, err := runSteps(t, []string{"CHANGED=0\n" + flannelIfaceStep(disableVPN) + "echo CHANGED=$CHANGED"}, f.Env())
	if err != nil {
		t.Fatalf("step failed: %v\n%s", err, out)
	}
	return out[strings.LastIndex(out, "CHANGED="):], f.Log("kubectl.calls")
}

func TestFlannelIfaceStep_NoVPNPinsNodeIPAndIsIdempotent(t *testing.T) {
	f := newFlannelEnv(t)

	changed, calls := f.run(t, true)
	if !strings.HasPrefix(changed, "CHANGED=1") {
		t.Fatalf("first run must report a change, got %q", changed)
	}
	if strings.Contains(calls, "wg0") {
		t.Errorf("no-VPN run must never mention wg0:\n%s", calls)
	}
	if strings.Count(calls, "env/-") != 1 || !strings.Contains(calls, "status.hostIP") {
		t.Errorf("expected exactly one env patch from status.hostIP:\n%s", calls)
	}
	if strings.Count(calls, "args/-") != 1 || !strings.Contains(calls, "--iface=$(FLANNEL_NODE_IP)") {
		t.Errorf("expected exactly one args patch pinning --iface to the node IP:\n%s", calls)
	}

	// Second run on the already-patched DaemonSet: nothing to do, no bounce.
	changed, calls = f.run(t, true)
	if !strings.HasPrefix(changed, "CHANGED=0") || strings.Contains(calls, "patch") {
		t.Errorf("second run must be a no-op, got %q with calls:\n%s", changed, calls)
	}

	// `kubectl apply` of the upstream manifest resets the args but keeps the env
	// var we added: only the args are patched again (a duplicate env var would
	// be rejected / ambiguous).
	f.resetArgs()
	changed, calls = f.run(t, true)
	if !strings.HasPrefix(changed, "CHANGED=1") || strings.Count(calls, "args/-") != 1 || strings.Contains(calls, "env/-") {
		t.Errorf("after an apply only the args must be re-patched, got %q with calls:\n%s", changed, calls)
	}
}

func TestFlannelIfaceStep_VPNPinsWg0(t *testing.T) {
	f := newFlannelEnv(t)
	changed, calls := f.run(t, false)
	if !strings.HasPrefix(changed, "CHANGED=1") || !strings.Contains(calls, "--iface=wg0") || strings.Contains(calls, "FLANNEL_NODE_IP") {
		t.Errorf("VPN run must pin wg0 only, got %q with calls:\n%s", changed, calls)
	}
	if changed, calls = f.run(t, false); !strings.HasPrefix(changed, "CHANGED=0") || strings.Contains(calls, "patch") {
		t.Errorf("second VPN run must be a no-op, got %q:\n%s", changed, calls)
	}
}
