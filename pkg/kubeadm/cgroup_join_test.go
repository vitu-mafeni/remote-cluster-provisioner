package kubeadm

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// realisticKubeadm replaces the join stub with one that behaves like kubeadm on a
// cgroups v1 host with Kubernetes >= 1.35 (FAKE_K8S_MINOR, default 35): the
// SystemVerification preflight fails with the real message unless it is ignored,
// and the kubelet refuses to start unless a kubeletconfiguration patch sets
// failCgroupV1: false. On a cgroups v2 host (the probe file exists) or an older
// Kubernetes it just joins.
func (j *joinEnv) realisticKubeadm() {
	j.Write("kubeadm", `#!/bin/bash
echo "kubeadm $*" >> "$FAKE_LOG/kubeadm.calls"
case "$1" in
  join)
    if [ -e "$CNLAB_KUBELET_CONF" ]; then echo "[preflight] FileAvailable--etc-kubernetes-kubelet.conf already exists" >&2; exit 1; fi
    if [ ! -e "$CNLAB_CGROUP_CONTROLLERS" ] && [ "${FAKE_K8S_MINOR:-35}" -ge 35 ]; then
      case "$*" in
        *--ignore-preflight-errors=SystemVerification*) ;;
        *)
          echo "[preflight] Some fatal errors occurred:" >&2
          echo "        [ERROR SystemVerification]: cgroups v1 support is deprecated and will be removed in a future release. Please migrate to cgroups v2." >&2
          echo "error: error execution phase preflight: preflight checks failed" >&2
          exit 1 ;;
      esac
      pdir="$(printf '%s' "$*" | sed -n 's/.*--patches=\([^ ]*\).*/\1/p')"
      if ! cat "$pdir"/kubeletconfiguration*.yaml 2>/dev/null | grep -qx 'failCgroupV1: false'; then
        echo "kubelet is configured to not run on a host using cgroup v1" >&2
        exit 1
      fi
    fi
    echo "server: https://10.8.0.1:6443" > "$CNLAB_KUBELET_CONF" ;;
  reset) rm -f "$CNLAB_KUBELET_CONF" ;;
esac
`)
}

func (j *joinEnv) cgroupV1() {
	if err := os.Remove(j.cgroup); err != nil {
		panic(err)
	}
}

// ── the snippet ────────────────────────────────────────────────────────────

func TestCgroupV1Applies(t *testing.T) {
	for v, want := range map[string]bool{
		"": false, "x": false, "1": false, "1.x": false, "x.35": false,
		"1.34.9": false, "1.34": false, "1.35": true, "1.35.0": true, "1.36.1": true, "1.100.0": true, "2.0.0": true, "0.99.0": false,
	} {
		if got := cgroupV1Applies(v); got != want {
			t.Errorf("cgroupV1Applies(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestCgroupV1JoinSnippet_NoopBeforeKubernetes135(t *testing.T) {
	for _, v := range []string{"", "bad", "1.34.9", "1.30.0"} {
		if got := CgroupV1JoinSnippet(v, "sudo"); got != `CGROUP_V1_JOIN_ARGS=""` {
			t.Errorf("CgroupV1JoinSnippet(%q) = %q", v, got)
		}
	}
}

// evalSnippet runs the snippet in bash and prints the resulting arguments.
func evalSnippet(t *testing.T, j *joinEnv, version, sudo string) (args, out string) {
	t.Helper()
	cmd := exec.Command("bash", "-s")
	cmd.Env = j.env()
	cmd.Stdin = strings.NewReader("set -Eeuo pipefail\n" + CgroupV1JoinSnippet(version, sudo) + "\nprintf 'ARGS=[%s]\\n' \"$CGROUP_V1_JOIN_ARGS\"\n")
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	out = string(b)
	i := strings.Index(out, "ARGS=[")
	j2 := strings.LastIndex(out, "]")
	return out[i+len("ARGS=[") : j2], out
}

func TestCgroupV1JoinSnippet_Matrix(t *testing.T) {
	for _, sudo := range []string{"", "sudo"} {
		for _, tc := range []struct {
			name    string
			version string
			v1      bool
			want    bool // join args expected
		}{
			{"v2 host, k8s 1.35", "1.35.0", false, false},
			{"v1 host, k8s 1.35", "1.35.0", true, true},
			{"v1 host, k8s 1.36", "1.36.2", true, true},
			{"v1 host, k8s 1.34", "1.34.9", true, false},
			{"v2 host, k8s 1.34", "1.34.9", false, false},
			{"v1 host, unknown version", "", true, false},
		} {
			t.Run(tc.name+"/sudo="+sudo, func(t *testing.T) {
				j := newJoinEnv(t)
				if tc.v1 {
					j.cgroupV1()
				}
				args, out := evalSnippet(t, j, tc.version, sudo)
				if !tc.want {
					if args != "" {
						t.Fatalf("args = %q, want none\n%s", args, out)
					}
					if _, err := os.Stat(j.patches); err == nil {
						t.Error("no patch directory may be created")
					}
					return
				}
				if args != "--ignore-preflight-errors=SystemVerification --patches="+j.patches {
					t.Fatalf("args = %q", args)
				}
				b, err := os.ReadFile(j.patches + "/kubeletconfiguration+merge.yaml")
				if err != nil || string(b) != "failCgroupV1: false\n" {
					t.Fatalf("patch = %q, %v", b, err)
				}
				if !strings.Contains(out, "cgroups v1 host") {
					t.Errorf("the decision must be logged:\n%s", out)
				}
			})
		}
	}
}

// Running the snippet twice (retries, resumed bootstrap) is harmless.
func TestCgroupV1JoinSnippet_Idempotent(t *testing.T) {
	j := newJoinEnv(t)
	j.cgroupV1()
	a1, _ := evalSnippet(t, j, "1.35.0", "")
	a2, _ := evalSnippet(t, j, "1.35.0", "")
	if a1 != a2 || a1 == "" {
		t.Fatalf("%q vs %q", a1, a2)
	}
}

// ── the SSH worker join (RemoteCluster workers, and the shared step list) ────

func TestJoinWorkerStepsFor_CgroupV1Matrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		v1      bool
		version string
		minor   string
		wantFix bool
		wantErr bool
	}{
		{"cgroups v1 + 1.35 joins with the fix", true, "1.35.0", "35", true, false},
		{"cgroups v1 + 1.36 joins with the fix", true, "1.36.1", "36", true, false},
		{"cgroups v2 + 1.35 joins plain", false, "1.35.0", "35", false, false},
		{"cgroups v1 + 1.34 joins plain (kubelet still allows v1)", true, "1.34.9", "34", false, false},
		// The regression this fixes: without the version the join is plain and the
		// real kubeadm rejects a cgroups v1 host.
		{"cgroups v1 + 1.35 WITHOUT the fix fails like the field report", true, "", "35", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := newJoinEnv(t)
			j.realisticKubeadm()
			if tc.v1 {
				j.cgroupV1()
			}
			out, err := runSteps(t, joinWorkerStepsFor(testJoinCmd, tc.version), j.env("FAKE_K8S_MINOR="+tc.minor, "FAKE_JOIN_FAILS=0"))
			if tc.wantErr {
				if err == nil || !strings.Contains(out, "cgroups v1 support is deprecated") {
					t.Fatalf("expected the cgroups v1 preflight failure, err=%v\n%s", err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			calls := j.Log("kubeadm.calls")
			if has := strings.Contains(calls, "--ignore-preflight-errors=SystemVerification"); has != tc.wantFix {
				t.Errorf("SystemVerification ignored = %v, want %v:\n%s", has, tc.wantFix, calls)
			}
			if has := strings.Contains(calls, "--patches="); has != tc.wantFix {
				t.Errorf("patches passed = %v, want %v:\n%s", has, tc.wantFix, calls)
			}
			if !strings.Contains(calls, "--cri-socket=unix:///var/run/crio/crio.sock") || !strings.Contains(calls, testJoinCmd) {
				t.Errorf("the join itself must be unchanged:\n%s", calls)
			}
			if count(calls, "kubeadm reset") != 0 || count(calls, "kubeadm join") != 1 {
				t.Errorf("one clean join expected:\n%s", calls)
			}
		})
	}
}

// Retries keep carrying the fix: a failed first attempt is reset and the second
// attempt still gets the cgroups v1 arguments.
func TestJoinWorkerStepsFor_RetryKeepsTheCgroupV1Arguments(t *testing.T) {
	j := newJoinEnv(t)
	j.Write("kubeadm", `#!/bin/bash
echo "kubeadm $*" >> "$FAKE_LOG/kubeadm.calls"
case "$1" in
  join)
    n=$(cat "$FAKE_LOG/join.count" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$FAKE_LOG/join.count"
    [ "$n" -le 1 ] && exit 1
    echo "server: https://10.8.0.1:6443" > "$CNLAB_KUBELET_CONF" ;;
  reset) rm -f "$CNLAB_KUBELET_CONF" ;;
esac
`)
	j.cgroupV1()
	out, err := runSteps(t, joinWorkerStepsFor(testJoinCmd, "1.35.0"), j.env())
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(j.Log("kubeadm.calls")), "\n")
	joins := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "kubeadm join") {
			joins++
			if !strings.Contains(l, "--ignore-preflight-errors=SystemVerification") || !strings.Contains(l, "--patches=") {
				t.Errorf("attempt %d lost the cgroups v1 arguments: %s", joins, l)
			}
		}
	}
	if joins != 2 {
		t.Fatalf("joins = %d\n%s", joins, j.Log("kubeadm.calls"))
	}
}

// A node that is already healthily joined is never touched, cgroup fix or not.
func TestJoinWorkerStepsFor_AlreadyJoinedNodeIsLeftAlone(t *testing.T) {
	j := newJoinEnv(t)
	j.realisticKubeadm()
	j.cgroupV1()
	if err := os.WriteFile(j.conf, []byte("server: https://10.8.0.1:6443\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runSteps(t, joinWorkerStepsFor(testJoinCmd, "1.35.0"), j.env()); err != nil || j.Log("kubeadm.calls") != "" {
		t.Fatalf("err=%v calls=%q\n%s", err, j.Log("kubeadm.calls"), out)
	}
}
