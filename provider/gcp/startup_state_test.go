package gcp

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// gceStateSection returns the image-state block of the GCE startup script.
func gceStateSection(t *testing.T, noVPN bool) string {
	t.Helper()
	script, err := BuildStartupScript(startupNP(noVPN, false))
	if err != nil {
		t.Fatal(err)
	}
	const begin, end = "# BEGIN image-state\n", "# END image-state\n"
	i, j := strings.Index(script, begin), strings.Index(script, end)
	if i < 0 || j < i {
		t.Fatal("image-state markers missing from the startup script")
	}
	return script[i+len(begin) : j]
}

// bootGCE runs the block as GCE instance instanceID ("" = metadata unreachable)
// on a disk prepared by setup, and returns the output, whether bootstrap would
// proceed, and the sandbox.
func bootGCE(t *testing.T, noVPN bool, instanceID string, setup func(dir string)) (string, bool, string, *sshtest.Stubs) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	// Only the GCE metadata server answers, and only for its instance id.
	s.Write("curl", `#!/bin/bash
case "$*" in
  *"Metadata-Flavor: Google"*computeMetadata/v1/instance/id*) [ -n "${FAKE_INSTANCE_ID:-}" ] && { printf '%s' "$FAKE_INSTANCE_ID"; exit 0; } ;;
esac
exit 22
`)
	s.Write("sleep", "#!/bin/bash\n:\n")
	s.Write("systemctl", "#!/bin/bash\n:\n")
	s.Write("kubeadm", "#!/bin/bash\necho \"kubeadm $1\" >> \"$FAKE_LOG/kubeadm.calls\"\n")
	dir := t.TempDir()
	for _, sub := range []string{"state", "k8s", "kubelet", "cni"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	setup(dir)
	prelude := strings.Join([]string{
		"set -Eeuo pipefail",
		`report() { echo "$*"; }`,
		"STATUS_FILE=" + dir + "/state/status",
		"COMPLETE_FILE=" + dir + "/state/complete",
		"INSTANCE_FILE=" + dir + "/state/instance",
		"K8S_DIR=" + dir + "/k8s",
		"KUBELET_DIR=" + dir + "/kubelet",
		"KUBELET_DEFAULTS=" + dir + "/state/default-kubelet",
		"CNI_DIR=" + dir + "/cni",
		"WG_CONF=" + dir + "/state/wg0.conf",
		"CRIO_SOCKET=/var/run/crio/crio.sock",
		`NODE_NAME="worker-1"`,
	}, "\n") + "\n"
	cmd := exec.Command("bash", "-s")
	cmd.Env = s.Env("FAKE_INSTANCE_ID=" + instanceID)
	cmd.Stdin = strings.NewReader(prelude + gceStateSection(t, noVPN) + "echo PROCEED\n")
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script failed: %v\n%s", err, b)
	}
	return string(b), strings.Contains(string(b), "PROCEED"), dir, s
}

func write(t *testing.T, p, content string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGCEStartupScript_ImageOfABootstrappedNodeIsResetOnANewInstance(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		out, ok, dir, s := bootGCE(t, noVPN, "2222", func(dir string) {
			write(t, dir+"/state/complete", "")
			write(t, dir+"/state/status", "SUCCESS\n")
			write(t, dir+"/state/instance", "1111\n") // bootstrapped by instance 1111
			write(t, dir+"/k8s/kubelet.conf", "server: https://10.0.0.1:6443\n")
			write(t, dir+"/kubelet/kubeadm-flags.env", "x")
		})
		if !ok || strings.Contains(out, "already completed") {
			t.Fatalf("noVPN=%v: a new GCE instance booted from that image must bootstrap:\n%s", noVPN, out)
		}
		if !strings.Contains(s.Log("kubeadm.calls"), "kubeadm reset") {
			t.Errorf("noVPN=%v: kubeadm reset must run", noVPN)
		}
		for _, gone := range []string{"/state/complete", "/state/status", "/k8s", "/kubelet"} {
			if _, err := os.Stat(dir + gone); err == nil {
				t.Errorf("noVPN=%v: %s must be removed", noVPN, gone)
			}
		}
		if b, _ := os.ReadFile(dir + "/state/instance"); strings.TrimSpace(string(b)) != "2222" {
			t.Errorf("noVPN=%v: the new instance id must be recorded, got %q", noVPN, b)
		}
	}
}

func TestGCEStartupScript_RebootOfTheSameInstanceIsANoOp(t *testing.T) {
	out, ok, dir, s := bootGCE(t, true, "1111", func(dir string) {
		write(t, dir+"/state/complete", "")
		write(t, dir+"/state/instance", "1111\n")
		write(t, dir+"/k8s/kubelet.conf", "server: https://10.0.0.1:6443\n")
	})
	if ok || !strings.Contains(out, "Bootstrap already completed; nothing to do") {
		t.Fatalf("proceeded=%v\n%s", ok, out)
	}
	if strings.Contains(s.Log("kubeadm.calls"), "reset") {
		t.Error("a healthy reboot must never reset the node")
	}
	if _, err := os.Stat(dir + "/k8s/kubelet.conf"); err != nil {
		t.Error("kubelet.conf must be preserved")
	}
}

func TestGCEStartupScript_MetadataUnavailableNeverWipes(t *testing.T) {
	out, ok, _, s := bootGCE(t, true, "", func(dir string) {
		write(t, dir+"/state/complete", "")
		write(t, dir+"/state/instance", "1111\n")
	})
	if ok || strings.Contains(s.Log("kubeadm.calls"), "reset") || !strings.Contains(out, "already completed") {
		t.Fatalf("proceeded=%v\n%s", ok, out)
	}
}

// Each provider's script talks only to its own metadata server.
func TestStartupScripts_InstanceIDLookupIsProviderSpecific(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		sec := gceStateSection(t, noVPN)
		if !strings.Contains(sec, "computeMetadata/v1/instance/id") {
			t.Errorf("noVPN=%v: GCE script must read the id from the GCE metadata server", noVPN)
		}
		for _, aws := range []string{"169.254.169.254/latest", "X-aws-ec2-metadata-token", "meta-data/instance-id"} {
			if strings.Contains(sec, aws) {
				t.Errorf("noVPN=%v: GCE script must not use EC2 IMDS (%q)", noVPN, aws)
			}
		}
	}
}
