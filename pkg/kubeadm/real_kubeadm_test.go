package kubeadm

import (
	"os"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/internal/e2esandbox"
)

// TestRealKubeadm_CgroupV1JoinArgumentsAreHonoured checks the cgroups v1 join
// arguments against the REAL kubeadm instead of a stub: that `kubeadm join` accepts
// --patches and --ignore-preflight-errors, and that the patch file the snippet writes
// is found, applied to the KubeletConfiguration and leaves failCgroupV1: false in the
// kubelet config kubeadm writes.
//
// It needs a kubeadm >= 1.35 binary and runs only when CNLAB_REAL_KUBEADM points at
// one (docker and the ubuntu:22.04 image are needed as for the other e2e tests):
//
//	curl -fsSLo /tmp/kubeadm https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubeadm
//	chmod +x /tmp/kubeadm && CNLAB_REAL_KUBEADM=/tmp/kubeadm go test ./pkg/kubeadm -run RealKubeadm -v
func TestRealKubeadm_CgroupV1JoinArgumentsAreHonoured(t *testing.T) {
	bin := os.Getenv("CNLAB_REAL_KUBEADM")
	if bin == "" {
		t.Skip("set CNLAB_REAL_KUBEADM to a kubeadm >= 1.35 binary to run this test")
	}
	// The snippet runs on a cgroups v1 host (probe file missing) and writes the patch;
	// real kubeadm then reads it through `patches.directory` — the config-file form of
	// --patches, which shares its code (kubeadm refuses --config together with flags).
	script := CgroupV1JoinSnippet("1.35.0", "") + `
echo "ARGS=$CGROUP_V1_JOIN_ARGS"
mkdir -p /var/lib/kubelet /etc/kubernetes
cat > /tmp/cfg.yaml <<CFG
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: 10.0.0.5
nodeRegistration:
  criSocket: unix:///var/run/crio/crio.sock
patches:
  directory: ${CNLAB_PATCH_DIR}
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: "v1.35.0"
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
CFG
echo "--- join flags"
n=$(/kubeadm join --help 2>&1 | grep -c -e '--patches string' -e '--ignore-preflight-errors strings')
echo "flags-found=$n"
echo "--- kubelet-start"
KUBEADM_INIT_DRYRUN_DIR=/tmp/dry /kubeadm init phase kubelet-start --config /tmp/cfg.yaml --dry-run -v=5 2>&1 | grep -E '^\[patches\]|^error'
cat /tmp/dry/config.yaml
`
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: script,
		Env:    []string{"CNLAB_CGROUP_CONTROLLERS=/work/out/none", "CNLAB_PATCH_DIR=/tmp/patches"},
		Mounts: []string{bin + ":/kubeadm"},
	})
	for _, want := range []string{
		"ARGS=--ignore-preflight-errors=SystemVerification --patches=/tmp/patches",
		"flags-found=2",
		`Found the following patch files: [kubeletconfiguration+merge.yaml]`,
		`Applied patch of type "application/merge-patch+json" to target "kubeletconfiguration"`,
		"failCgroupV1: false",
		"cgroupDriver: systemd", // the rest of the kubelet config is untouched
	} {
		if !strings.Contains(r.Log, want) {
			t.Errorf("real kubeadm output missing %q:\n%s", want, r.Log)
		}
	}
	if strings.Contains(r.Log, "error:") {
		t.Errorf("kubeadm reported an error:\n%s", r.Log)
	}
}

// The control-plane config, after the cgroups v1 edit, must be accepted by the real
// `kubeadm config validate` (it strictly checks the KubeletConfiguration fields).
func TestRealKubeadm_InitConfigWithTheCgroupV1EditValidates(t *testing.T) {
	bin := os.Getenv("CNLAB_REAL_KUBEADM")
	if bin == "" {
		t.Skip("set CNLAB_REAL_KUBEADM to a kubeadm >= 1.35 binary to run this test")
	}
	for _, v1 := range []bool{true, false} {
		cfg := kubeadmInitConfig("10.0.0.5", "1.35.0", "c1")
		cfgStep, arg := cgroupV1CompatibilitySettings("1.35.0")
		steps := probe(kubeadmInitSteps(cfg, cfgStep, arg), v1)[:2] // mkdir + write/edit the config
		script := stepsScript(steps) + "/kubeadm config validate --config /tmp/kubeadm-config.yaml 2>&1; echo \"validate-exit=$?\"\ngrep -c failCgroupV1 /tmp/kubeadm-config.yaml | sed 's/^/failCgroupV1-lines=/'\n"
		env, setup := e2eEnv(v1, "35", 0)
		r := e2esandbox.Run(t, e2esandbox.Options{Script: script, Env: env, Setup: setup, Mounts: []string{bin + ":/kubeadm"}})
		if !strings.Contains(r.Log, "validate-exit=0") {
			t.Fatalf("v1=%v: the real kubeadm rejected the init config:\n%s", v1, r.Log)
		}
		want := "failCgroupV1-lines=0"
		if v1 {
			want = "failCgroupV1-lines=1"
		}
		if !strings.Contains(r.Log, want) {
			t.Errorf("v1=%v: want %s:\n%s", v1, want, r.Log)
		}
	}
}
