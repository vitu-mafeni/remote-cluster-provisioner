package kubeadm

import (
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"dcn.ssu.ac.kr/infra/internal/e2esandbox"
)

// Container end-to-end tests of the SSH-driven kubeadm paths: the control-plane
// `kubeadm init` and the RemoteCluster worker `kubeadm join`. The REAL step text is
// executed as root in a disposable Ubuntu container (see internal/e2esandbox) the way
// the SSH runner does — one `bash -s` per step under pipefail, stopping at the first
// failing step — against a kubeadm that behaves like the real one on a cgroups v1
// host. They skip when docker or the ubuntu:22.04 image is unavailable (or -short).

// stepsScript composes steps into one script that runs each in its own bash (like the
// SSH runner) and stops at the first failing step.
func stepsScript(steps []string) string {
	var b strings.Builder
	for i, st := range steps {
		delim := fmt.Sprintf("__STEP_%d_EOF__", i)
		fmt.Fprintf(&b, "bash -s <<'%s'\n%s%s\n%s\nrc=$?\n[ $rc -eq 0 ] || exit $rc\n", delim, pipefailPrefix, st, delim)
	}
	return b.String()
}

// probe makes the hard-coded cgroups probe path of a step point at the sandbox's
// stand-in (the container itself runs on this machine's cgroups v2 hierarchy).
func probe(steps []string, v1 bool) []string {
	p := "/work/out/cgroup.controllers"
	if v1 {
		p = "/work/out/none"
	}
	out := make([]string, len(steps))
	for i, s := range steps {
		out[i] = strings.ReplaceAll(s, "/sys/fs/cgroup/cgroup.controllers", p)
	}
	return out
}

func e2eEnv(v1 bool, minor string, fails int) (env []string, setup string) {
	cg := "/work/out/cgroup.controllers"
	if v1 {
		cg = "/work/out/none"
	} else {
		setup = "touch /work/out/cgroup.controllers\n"
	}
	return []string{"CNLAB_CGROUP_CONTROLLERS=" + cg, "FAKE_K8S_MINOR=" + minor, fmt.Sprintf("FAKE_JOIN_FAILS=%d", fails)}, setup
}

func kubeadmCalls(calls, verb string) (lines []string) {
	for _, l := range strings.Split(calls, "\n") {
		if strings.HasPrefix(l, "kubeadm "+verb) {
			lines = append(lines, l)
		}
	}
	return
}

func hasCgroupFix(line string) bool {
	return strings.Contains(line, "--ignore-preflight-errors=SystemVerification") &&
		strings.Contains(line, "--patches=/var/lib/node-bootstrap-kubeadm-patches")
}

// ── control plane: kubeadm init ───────────────────────────────────────────────

func TestE2E_ControlPlaneInit_CgroupV1Matrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		v1      bool
		version string
		minor   string
		wantFix bool
	}{
		{"cgroups v1 + 1.35", true, "1.35.0", "35", true},
		{"cgroups v1 + 1.36", true, "1.36.1", "36", true},
		{"cgroups v2 + 1.35", false, "1.35.0", "35", false},
		{"cgroups v1 + 1.34", true, "1.34.9", "34", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := kubeadmInitConfig("10.0.0.5", tc.version, "c1")
			cfgStep, arg := cgroupV1CompatibilitySettings(tc.version)
			env, setup := e2eEnv(tc.v1, tc.minor, 0)
			r := e2esandbox.Run(t, e2esandbox.Options{
				Script: stepsScript(probe(kubeadmInitSteps(cfg, cfgStep, arg), tc.v1)),
				Env:    env, Setup: setup, Runs: 2, // a retry after success must not init again
				After: "cp /tmp/kubeadm-config.yaml $OUT/kubeadm-config.yaml\n",
			})
			if r.Exits[0] != 0 || r.Exits[1] != 0 {
				t.Fatalf("exits %v\n%s", r.Exits, r.Dump())
			}
			inits := kubeadmCalls(r.Calls(), "init")
			if len(inits) != 1 {
				t.Fatalf("`kubeadm init` must run once (guarded by admin.conf), ran %d times:\n%s", len(inits), r.Calls())
			}
			if hasFix := strings.Contains(inits[0], "--ignore-preflight-errors=SystemVerification"); hasFix != tc.wantFix {
				t.Errorf("preflight ignored = %v, want %v: %s", hasFix, tc.wantFix, inits[0])
			}

			// The written config is valid multi-document YAML and failCgroupV1 landed in
			// the KubeletConfiguration document only (never in another document).
			docs := strings.Split(r.File("kubeadm-config.yaml"), "\n---\n")
			if len(docs) != 4 {
				t.Fatalf("want 4 YAML documents, got %d:\n%s", len(docs), r.File("kubeadm-config.yaml"))
			}
			for _, d := range docs {
				var obj map[string]any
				if err := yaml.Unmarshal([]byte(d), &obj); err != nil {
					t.Fatalf("invalid YAML document: %v\n%s", err, d)
				}
				kind, _ := obj["kind"].(string)
				_, has := obj["failCgroupV1"]
				if kind == "KubeletConfiguration" {
					if has != tc.wantFix {
						t.Errorf("KubeletConfiguration failCgroupV1 present = %v, want %v:\n%s", has, tc.wantFix, d)
					}
					if tc.wantFix && obj["failCgroupV1"] != false {
						t.Errorf("failCgroupV1 = %v, want false", obj["failCgroupV1"])
					}
					gates, _ := obj["featureGates"].(map[string]any)
					if gates["DynamicResourceAllocation"] != true || gates["DRAConsumableCapacity"] != true {
						t.Errorf("the kubelet feature gates must survive the edit: %v", obj["featureGates"])
					}
					if obj["cgroupDriver"] != "systemd" {
						t.Errorf("cgroupDriver = %v", obj["cgroupDriver"])
					}
				} else if has {
					t.Errorf("failCgroupV1 leaked into the %s document:\n%s", kind, d)
				}
			}
		})
	}
}

// Without the cgroups v1 handling the control plane fails like the field report.
func TestE2E_ControlPlaneInit_WithoutTheFixFailsOnCgroupV1(t *testing.T) {
	cfg := kubeadmInitConfig("10.0.0.5", "1.35.0", "c1")
	env, setup := e2eEnv(true, "35", 0)
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: stepsScript(probe(kubeadmInitSteps(cfg, "", ""), true)), Env: env, Setup: setup,
	})
	if r.ExitCode == 0 || !strings.Contains(r.Log, "cgroups v1 support is deprecated") {
		t.Fatalf("expected the cgroups v1 failure:\n%s", r.Dump())
	}
}

// ── RemoteCluster workers: kubeadm join over SSH ──────────────────────────────

const e2eJoinCmd = "kubeadm join 10.0.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:00"

func TestE2E_WorkerJoin_CgroupV1Matrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		v1      bool
		version string
		minor   string
		fails   int
		wantFix bool
	}{
		{"cgroups v1 + 1.35", true, "1.35.0", "35", 0, true},
		{"cgroups v1 + 1.36 with two failed attempts", true, "1.36.1", "36", 2, true},
		{"cgroups v2 + 1.35", false, "1.35.0", "35", 0, false},
		{"cgroups v1 + 1.34", true, "1.34.9", "34", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, setup := e2eEnv(tc.v1, tc.minor, tc.fails)
			r := e2esandbox.Run(t, e2esandbox.Options{
				Script: stepsScript(joinWorkerStepsFor(e2eJoinCmd, tc.version)), Env: env, Setup: setup,
			})
			if r.ExitCode != 0 || !strings.Contains(r.Log, "kubeadm join succeeded") {
				t.Fatalf("the join failed:\n%s", r.Dump())
			}
			joins := kubeadmCalls(r.Calls(), "join")
			if len(joins) != tc.fails+1 || len(kubeadmCalls(r.Calls(), "reset")) != tc.fails {
				t.Fatalf("want %d joins and %d resets:\n%s", tc.fails+1, tc.fails, r.Calls())
			}
			for i, j := range joins {
				if hasCgroupFix(j) != tc.wantFix || !strings.Contains(j, "--cri-socket=unix:///var/run/crio/crio.sock") {
					t.Errorf("attempt %d: fix present = %v, want %v: %s", i+1, hasCgroupFix(j), tc.wantFix, j)
				}
			}
		})
	}
}

// A node that is already healthily joined is never rejoined or reset.
func TestE2E_WorkerJoin_AlreadyJoinedNodeIsLeftAlone(t *testing.T) {
	env, _ := e2eEnv(true, "35", 0)
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: stepsScript(joinWorkerStepsFor(e2eJoinCmd, "1.35.0")), Env: env,
		Setup: "mkdir -p /etc/kubernetes\necho 'server: https://10.0.0.1:6443' > /etc/kubernetes/kubelet.conf\n: > $OUT/active.kubelet\n",
	})
	if r.ExitCode != 0 || !strings.Contains(r.Log, "already joined") {
		t.Fatalf("%s", r.Dump())
	}
	if c := r.Calls(); strings.Contains(c, "kubeadm join") || strings.Contains(c, "kubeadm reset") {
		t.Errorf("a healthy node must not be touched:\n%s", c)
	}
}

func TestE2E_WorkerJoin_WithoutTheVersionFailsOnCgroupV1(t *testing.T) {
	env, setup := e2eEnv(true, "35", 0)
	r := e2esandbox.Run(t, e2esandbox.Options{Script: stepsScript(joinWorkerSteps(e2eJoinCmd)), Env: env, Setup: setup})
	if r.ExitCode == 0 || !strings.Contains(r.Log, "cgroups v1 support is deprecated") {
		t.Fatalf("expected the field failure:\n%s", r.Dump())
	}
}
