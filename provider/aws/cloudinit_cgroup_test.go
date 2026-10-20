package aws

import (
	"os"
	"strings"
	"testing"
)

// realisticKubeadm makes the join stub behave like kubeadm on a cgroups v1 host
// with Kubernetes >= 1.35: the SystemVerification preflight fails with the real
// message unless ignored, and the kubelet refuses to start unless a
// kubeletconfiguration patch sets failCgroupV1: false.
func (j *joinRun) realisticKubeadm() {
	j.Write("kubeadm", `#!/bin/bash
echo "kubeadm $*" >> "$FAKE_LOG/kubeadm.calls"
case "$1" in
  join)
    if [ -e "$CNLAB_KUBELET_CONF" ]; then echo "already exists" >&2; exit 1; fi
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
    echo "server: https://10.0.0.1:6443" > "$CNLAB_KUBELET_CONF" ;;
  reset) rm -f "$CNLAB_KUBELET_CONF" ;;
esac
`)
}

func (j *joinRun) cgroupV1(t *testing.T) {
	t.Helper()
	if err := os.Remove(j.cgroup); err != nil {
		t.Fatal(err)
	}
}

func TestCloudInitJoin_CgroupV1Matrix(t *testing.T) {
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
			j := newJoinRun(t)
			j.realisticKubeadm()
			j.version = tc.version
			if tc.v1 {
				j.cgroupV1(t)
			}
			out, err := j.run(t, "FAKE_K8S_MINOR="+tc.minor)
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			calls := j.Log("kubeadm.calls")
			if has := strings.Contains(calls, "--ignore-preflight-errors=SystemVerification"); has != tc.wantFix {
				t.Errorf("SystemVerification ignored = %v, want %v:\n%s", has, tc.wantFix, calls)
			}
			if has := strings.Contains(calls, "--patches="); has != tc.wantFix {
				t.Errorf("patches = %v, want %v:\n%s", has, tc.wantFix, calls)
			}
			if !strings.Contains(calls, "--cri-socket=unix:///var/run/crio/crio.sock") {
				t.Errorf("--cri-socket missing:\n%s", calls)
			}
			if strings.Count(calls, "kubeadm join") != 1 || strings.Contains(calls, "kubeadm reset") || !strings.Contains(out, "Cluster join succeeded") {
				t.Errorf("one clean join expected:\n%s\n%s", calls, out)
			}
			if tc.wantFix {
				b, err := os.ReadFile(j.patches + "/kubeletconfiguration+merge.yaml")
				if err != nil || string(b) != "failCgroupV1: false\n" {
					t.Errorf("patch = %q, %v", b, err)
				}
			}
		})
	}
}

// The rendered script keeps the fix in every variant (VPN, no VPN, GPU).
func TestRenderBootstrapScript_CarriesTheCgroupV1HandlingInEveryVariant(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		p := baseParams()
		p.NoVPN = noVPN
		if !noVPN {
			p.WGConfig = "[Interface]\nAddress = 10.8.0.5/24\n"
			p.VpnIP = "10.8.0.5"
		}
		script, err := renderBootstrapScript(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{
			`CGROUP_V1_JOIN_ARGS=""`, "failCgroupV1: false", "--ignore-preflight-errors=SystemVerification",
			`bash -c "$JOIN_CMD $CGROUP_V1_JOIN_ARGS --cri-socket=unix://${CRIO_SOCKET}"`,
		} {
			if !strings.Contains(script, want) {
				t.Errorf("noVPN=%v: script missing %q", noVPN, want)
			}
		}
		assertBashSyntax(t, script)
	}
	// Before Kubernetes 1.35 the handling is a no-op.
	p := baseParams()
	p.NoVPN = true
	p.KubernetesVersion = "1.34.2"
	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(script, "failCgroupV1: false") || strings.Contains(script, "--ignore-preflight-errors") || !strings.Contains(script, `CGROUP_V1_JOIN_ARGS=""`) {
		t.Error("k8s 1.34: no cgroups v1 handling expected")
	}
}
