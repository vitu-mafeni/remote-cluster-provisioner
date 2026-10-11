package onprem

import (
	"fmt"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/internal/e2esandbox"
	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
)

// Container end-to-end test of the on-prem join step: the REAL command the
// provisioner runs (joinNodeCommand, under `set -o pipefail` and the 10-minute
// timeout) executed as root in a disposable Ubuntu container against a kubeadm that
// behaves like the real one on a cgroups v1 host. Skips when docker or the
// ubuntu:22.04 image is unavailable (or with -short).
func TestE2E_OnPremJoinStep_CgroupV1Matrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		v1      bool
		version string
		minor   string
		wantFix bool
	}{
		{"Ubuntu 20.04 (cgroups v1) + 1.35", true, "1.35.0", "35", true},
		{"Ubuntu 20.04 (cgroups v1) + 1.36", true, "1.36.2", "36", true},
		{"Ubuntu 22.04 (cgroups v2) + 1.35", false, "1.35.0", "35", false},
		{"Ubuntu 20.04 (cgroups v1) + 1.34", true, "1.34.9", "34", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cg, setup := "/work/out/none", ""
			if !tc.v1 {
				cg, setup = "/work/out/cgroup.controllers", "touch /work/out/cgroup.controllers\n"
			}
			r := e2esandbox.Run(t, e2esandbox.Options{
				Script: "set -o pipefail\n" + joinNodeCommand(tc.version, "  "+flowJoinCmd+"\n"),
				Env:    []string{"CNLAB_CGROUP_CONTROLLERS=" + cg, "FAKE_K8S_MINOR=" + tc.minor, "FAKE_JOIN_FAILS=0"},
				Setup:  setup,
			})
			if r.ExitCode != 0 {
				t.Fatalf("the join step failed:\n%s", r.Dump())
			}
			var joins []string
			for _, l := range strings.Split(r.Calls(), "\n") {
				if strings.HasPrefix(l, "kubeadm join") {
					joins = append(joins, l)
				}
			}
			if len(joins) != 1 {
				t.Fatalf("want exactly one join:\n%s", r.Calls())
			}
			fix := strings.Contains(joins[0], "--ignore-preflight-errors=SystemVerification") &&
				strings.Contains(joins[0], "--patches=/var/lib/node-bootstrap-kubeadm-patches")
			if fix != tc.wantFix {
				t.Errorf("cgroups v1 arguments present = %v, want %v: %s", fix, tc.wantFix, joins[0])
			}
			if !strings.Contains(joins[0], flowJoinCmd) || !strings.Contains(joins[0], "--cri-socket=unix:///var/run/crio/crio.sock") {
				t.Errorf("the join itself must be unchanged: %s", joins[0])
			}
		})
	}
}

func TestE2E_OnPremJoinStep_WithoutTheFixFailsOnCgroupV1(t *testing.T) {
	// The pre-fix command (no cgroups v1 handling) fails like the field report.
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: "set -o pipefail\n" + fmt.Sprintf("sudo timeout 600 %s --cri-socket=unix:///var/run/crio/crio.sock\n", flowJoinCmd),
		Env:    []string{"CNLAB_CGROUP_CONTROLLERS=/work/out/none", "FAKE_K8S_MINOR=35", "FAKE_JOIN_FAILS=0"},
	})
	if r.ExitCode == 0 || !strings.Contains(r.Log, "cgroups v1 support is deprecated") {
		t.Fatalf("expected the field failure:\n%s", r.Dump())
	}
}

// The on-prem step mounts the control plane's shared checkpoint directory with sudo,
// and a failure never fails the step.
func TestE2E_OnPremSharedStorageStep(t *testing.T) {
	step := kubeadmSharedStorageStep(t)
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: "set -o pipefail\n" + step,
		After:  "cat /etc/fstab > $OUT/f.fstab\n",
	})
	if r.ExitCode != 0 || !strings.Contains(r.Calls(), "mount -t nfs 10.0.0.9:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage") {
		t.Fatalf("%s", r.Dump())
	}
	if !strings.Contains(r.File("f.fstab"), "10.0.0.9:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage nfs") {
		t.Errorf("fstab = %q", r.File("f.fstab"))
	}
	bad := e2esandbox.Run(t, e2esandbox.Options{Script: "set -o pipefail\n" + step, Env: []string{"FAKE_NFS_FAIL=1"}})
	if bad.ExitCode != 0 || !strings.Contains(bad.Log, "WARNING: could not mount") {
		t.Fatalf("a failed mount must be a warning:\n%s", bad.Dump())
	}
}

func kubeadmSharedStorageStep(t *testing.T) string {
	t.Helper()
	s := kubeadm.CheckpointStorageMountSnippet(kubeadm.NFSServerFromJoinCommand(flowJoinCmd), "sudo")
	if s == "" {
		t.Fatal("no snippet for " + flowJoinCmd)
	}
	return s
}
