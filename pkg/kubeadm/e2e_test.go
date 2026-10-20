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

// ── shared checkpoint storage (NFS) ───────────────────────────────────────────

const nfsAfter = "cat /etc/fstab > $OUT/f.fstab 2>/dev/null\n[ -d /shared-pvc/checkpoint-storage ] && echo yes > $OUT/f.mountdir\n"

func TestE2E_WorkerSharedStoragePhase(t *testing.T) {
	phases := sharedStoragePhases(e2eJoinCmd)
	if len(phases) != 1 || phases[0].Name != "Shared Storage" || len(phases[0].Steps) != 1 {
		t.Fatalf("phases = %+v", phases)
	}
	// It must be the LAST worker phase, after the join, so earlier phase numbers
	// (stored as progress on in-flight provisioning) never shift.
	if WorkerPhaseSharedStorage != WorkerPhaseJoin+1 {
		t.Fatalf("WorkerPhaseSharedStorage = %d, WorkerPhaseJoin = %d", WorkerPhaseSharedStorage, WorkerPhaseJoin)
	}
	if got := sharedStoragePhases("kubeadm join not-an-endpoint --token x"); got != nil {
		t.Errorf("no usable control-plane host: no phase, got %+v", got)
	}

	for _, tc := range []struct {
		name string
		env  []string
		runs int
	}{
		{"mounts the control plane's checkpoint directory", nil, 1},
		{"is idempotent across retries", nil, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := e2esandbox.Run(t, e2esandbox.Options{Script: stepsScript(phases[0].Steps), Env: tc.env, Runs: tc.runs, After: nfsAfter})
			for i, c := range r.Exits {
				if c != 0 {
					t.Fatalf("run %d failed:\n%s", i+1, r.Dump())
				}
			}
			calls := r.Calls()
			if n := strings.Count(calls, "mount -t nfs 10.0.0.1:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage"); n != 1 {
				t.Errorf("want exactly one mount of the checkpoint export, got %d:\n%s", n, calls)
			}
			if !strings.Contains(calls, "apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=5 install -y nfs-common") {
				t.Errorf("nfs-common must be installed on a node without it:\n%s", calls)
			}
			if r.File("f.mountdir") != "yes\n" {
				t.Error("the mount point must exist")
			}
			if n := strings.Count(r.File("f.fstab"), "/shared-pvc/checkpoint-storage"); n != 1 || !strings.Contains(r.File("f.fstab"), "10.0.0.1:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage nfs defaults,_netdev,nofail") {
				t.Errorf("fstab = %q", r.File("f.fstab"))
			}
		})
	}
}

// An unreachable NFS server never fails the worker phase.
func TestE2E_WorkerSharedStorage_FailureDoesNotFailTheNode(t *testing.T) {
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: stepsScript(sharedStoragePhases(e2eJoinCmd)[0].Steps), Env: []string{"FAKE_NFS_FAIL=1"}, After: nfsAfter,
	})
	if r.ExitCode != 0 || !strings.Contains(r.Log, "WARNING: could not mount 10.0.0.1:/srv/nfs/k8s/checkpoint-storage") {
		t.Fatalf("a failed mount must be a warning:\n%s", r.Dump())
	}
	if !strings.Contains(r.File("f.fstab"), "10.0.0.1:/srv/nfs/k8s/checkpoint-storage") {
		t.Error("the fstab entry must still be written so the boot-time mount can succeed later")
	}
}

// The control-plane side: the directory every worker mounts is created, idempotently,
// readable by workers, under the existing /srv/nfs/k8s export.
func TestE2E_ControlPlane_CheckpointExportDirectory(t *testing.T) {
	step := checkpointExportStep()
	if !strings.Contains(step, CheckpointNFSExport) || !strings.HasPrefix(CheckpointNFSExport, CheckpointNFSParentExport+"/") {
		t.Fatalf("step = %q", step)
	}
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: stepsScript([]string{step}), Runs: 2,
		After: "stat -c '%U:%G %a' /srv/nfs/k8s/checkpoint-storage > $OUT/f.stat\n",
	})
	if r.Exits[0] != 0 || r.Exits[1] != 0 {
		t.Fatalf("%s", r.Dump())
	}
	if got := strings.TrimSpace(r.File("f.stat")); got != "nobody:nogroup 755" {
		t.Errorf("directory owner/mode = %q", got)
	}
}
