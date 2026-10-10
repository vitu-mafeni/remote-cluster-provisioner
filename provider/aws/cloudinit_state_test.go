package aws

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// stateSection returns the image-state block of the rendered bootstrap script.
func stateSection(t *testing.T, noVPN bool) string {
	t.Helper()
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
	const begin, end = "# BEGIN image-state\n", "# END image-state\n"
	i, j := strings.Index(script, begin), strings.Index(script, end)
	if i < 0 || j < i {
		t.Fatal("image-state markers missing from the bootstrap template")
	}
	return script[i+len(begin) : j]
}

// disk is a sandbox standing in for / during one boot of the bootstrap script.
type disk struct {
	*sshtest.Stubs
	dir string
}

func newDisk(t *testing.T) *disk {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	// EC2 IMDSv2: a token, then the instance id (FAKE_INSTANCE_ID; empty = unreachable).
	s.Write("curl", `#!/bin/bash
[ -n "${FAKE_INSTANCE_ID:-}" ] || exit 22
case "$*" in
  *latest/api/token*) printf 'tok' ;;
  *meta-data/instance-id*) printf '%s' "$FAKE_INSTANCE_ID" ;;
  *) exit 22 ;;
esac
`)
	s.Write("sleep", "#!/bin/bash\n:\n")
	s.Write("systemctl", "#!/bin/bash\necho \"systemctl $*\" >> \"$FAKE_LOG/systemctl.calls\"\n")
	s.Write("kubeadm", "#!/bin/bash\necho \"kubeadm $1\" >> \"$FAKE_LOG/kubeadm.calls\"\n")
	d := &disk{Stubs: s, dir: t.TempDir()}
	for _, sub := range []string{"state", "k8s/pki", "kubelet", "cni"} {
		if err := os.MkdirAll(filepath.Join(d.dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

func (d *disk) path(rel string) string { return filepath.Join(d.dir, rel) }

func (d *disk) write(t *testing.T, rel, content string) {
	t.Helper()
	if err := os.WriteFile(d.path(rel), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (d *disk) exists(rel string) bool { _, err := os.Stat(d.path(rel)); return err == nil }

// kubeletIdentity writes a kubelet.conf whose client certificate has CN cn.
func (d *disk) kubeletIdentity(t *testing.T, cn string) {
	t.Helper()
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not available")
	}
	pem := d.path("kubelet/pki-client.pem")
	out, err := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
		"-keyout", d.path("kubelet/key.pem"), "-out", pem, "-subj", "/O=system:nodes/CN="+cn).CombinedOutput()
	if err != nil {
		t.Fatalf("openssl: %v\n%s", err, out)
	}
	d.write(t, "k8s/kubelet.conf", "users:\n- user:\n    client-certificate: "+pem+"\n")
}

// boot runs the block as the instance with id instanceID ("" = metadata
// unreachable) and reports whether bootstrap would proceed.
func (d *disk) boot(t *testing.T, noVPN bool, instanceID string) (out string, proceeded bool) {
	t.Helper()
	prelude := strings.Join([]string{
		"set -Eeuo pipefail",
		`report() { echo "$*"; }`,
		"STATUS_FILE=" + d.path("state/status"),
		"COMPLETE_FILE=" + d.path("state/complete"),
		"INSTANCE_FILE=" + d.path("state/instance"),
		"K8S_DIR=" + d.path("k8s"),
		"KUBELET_DIR=" + d.path("kubelet"),
		"KUBELET_DEFAULTS=" + d.path("state/default-kubelet"),
		"CNI_DIR=" + d.path("cni"),
		"WG_CONF=" + d.path("state/wg0.conf"),
		"CRIO_SOCKET=/var/run/crio/crio.sock",
		`NODE_NAME="worker-1"`,
	}, "\n") + "\n"
	cmd := exec.Command("bash", "-s")
	cmd.Env = d.Env("FAKE_INSTANCE_ID=" + instanceID)
	cmd.Stdin = strings.NewReader(prelude + stateSection(t, noVPN) + "echo PROCEED\n")
	b, err := cmd.CombinedOutput()
	out = string(b)
	if err != nil {
		t.Fatalf("script failed: %v\n%s", err, out)
	}
	return out, strings.Contains(out, "PROCEED")
}

func (d *disk) resets() bool { return strings.Contains(d.Log("kubeadm.calls"), "kubeadm reset") }

func TestImageState_FreshDiskProceedsAndRecordsTheInstance(t *testing.T) {
	d := newDisk(t)
	out, ok := d.boot(t, true, "1111")
	if !ok || d.resets() {
		t.Fatalf("fresh disk must bootstrap without a reset:\n%s", out)
	}
	if got := strings.TrimSpace(readFile(t, d.path("state/instance"))); got != "1111" {
		t.Errorf("recorded instance = %q", got)
	}
}

func TestImageState_CloneOfABootstrappedNodeIsResetAndBootstrapped(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		d := newDisk(t)
		// State as an image of node A (instance 1111) leaves it.
		d.write(t, "state/complete", "")
		d.write(t, "state/status", "SUCCESS\n")
		d.write(t, "state/instance", "1111\n")
		d.kubeletIdentity(t, "system:node:node-a")
		d.write(t, "kubelet/kubeadm-flags.env", "KUBELET_KUBEADM_ARGS=--node-ip=10.0.0.9")
		d.write(t, "state/default-kubelet", "KUBELET_EXTRA_ARGS=--node-ip=10.0.0.9")
		d.write(t, "state/wg0.conf", "[Interface]\nAddress = 10.8.0.2/24\n")
		d.write(t, "cni/10-flannel.conflist", "{}")
		d.write(t, "cni/20-keepme.conf", "{}")

		out, ok := d.boot(t, noVPN, "2222") // a NEW instance boots from that image
		if !ok {
			t.Fatalf("noVPN=%v: a clone must go on to bootstrap, not stop at 'already completed':\n%s", noVPN, out)
		}
		if strings.Contains(out, "already completed") || !strings.Contains(out, "bootstrapped by instance 1111, this is 2222") {
			t.Errorf("noVPN=%v: output:\n%s", noVPN, out)
		}
		if !d.resets() {
			t.Errorf("noVPN=%v: kubeadm reset must run", noVPN)
		}
		for _, gone := range []string{"state/complete", "state/status", "k8s/kubelet.conf", "kubelet/kubeadm-flags.env", "state/default-kubelet", "cni/10-flannel.conflist"} {
			if d.exists(gone) {
				t.Errorf("noVPN=%v: %s must be removed", noVPN, gone)
			}
		}
		if got := strings.TrimSpace(readFile(t, d.path("state/instance"))); got != "2222" {
			t.Errorf("noVPN=%v: the new instance must be recorded, got %q", noVPN, got)
		}
		if wg := strings.Contains(d.Log("systemctl.calls"), "wg-quick@wg0"); wg == noVPN {
			t.Errorf("noVPN=%v: stale wg0 handling = %v", noVPN, wg)
		}
		if d.exists("state/wg0.conf") == !noVPN {
			t.Errorf("noVPN=%v: stale wg0.conf must be removed only for VPN clusters", noVPN)
		}
		if !d.exists("cni/20-keepme.conf") {
			t.Errorf("noVPN=%v: unrelated CNI config must be kept", noVPN)
		}
		if !strings.Contains(d.Log("systemctl.calls"), "stop kubelet") {
			t.Errorf("noVPN=%v: kubelet must be stopped", noVPN)
		}
	}
}

// An image of a node bootstrapped by the previous script has no instance record:
// the kubelet identity alone must reveal the clone.
func TestImageState_CloneFromOlderScriptDetectedByKubeletIdentity(t *testing.T) {
	d := newDisk(t)
	d.write(t, "state/complete", "")
	d.kubeletIdentity(t, "system:node:node-a")
	out, ok := d.boot(t, true, "2222")
	if !ok || !d.resets() || !strings.Contains(out, "kubelet.conf belongs to system:node:node-a, this node is worker-1") {
		t.Fatalf("proceeded=%v resets=%v\n%s", ok, d.resets(), out)
	}
}

func TestImageState_RebootOfTheSameInstanceIsLeftAlone(t *testing.T) {
	d := newDisk(t)
	d.write(t, "state/complete", "")
	d.write(t, "state/instance", "1111\n")
	d.kubeletIdentity(t, "system:node:worker-1")
	out, ok := d.boot(t, true, "1111")
	if ok || d.resets() || !strings.Contains(out, "Bootstrap already completed; nothing to do") {
		t.Fatalf("a healthy reboot must be a no-op (proceeded=%v resets=%v):\n%s", ok, d.resets(), out)
	}
	if !d.exists("state/complete") || !d.exists("k8s/kubelet.conf") {
		t.Error("a healthy node's state must be preserved")
	}
}

// Nodes bootstrapped before this change have a completion marker but no instance
// record. A reboot must not wipe them, and the instance gets recorded.
func TestImageState_NodeFromOlderScriptRebootIsLeftAloneAndRecorded(t *testing.T) {
	d := newDisk(t)
	d.write(t, "state/complete", "")
	d.kubeletIdentity(t, "system:node:worker-1")
	out, ok := d.boot(t, true, "1111")
	if ok || d.resets() || !strings.Contains(out, "already completed") {
		t.Fatalf("proceeded=%v resets=%v\n%s", ok, d.resets(), out)
	}
	if got := strings.TrimSpace(readFile(t, d.path("state/instance"))); got != "1111" {
		t.Errorf("recorded instance = %q", got)
	}
}

// Without metadata nothing can be judged by instance id, and a kubelet identity
// that matches must never trigger a wipe.
func TestImageState_UnreachableMetadataNeverWipesAHealthyNode(t *testing.T) {
	d := newDisk(t)
	d.write(t, "state/complete", "")
	d.write(t, "state/instance", "1111\n")
	d.kubeletIdentity(t, "system:node:worker-1")
	out, ok := d.boot(t, true, "")
	if ok || d.resets() || !strings.Contains(out, "already completed") {
		t.Fatalf("proceeded=%v resets=%v\n%s", ok, d.resets(), out)
	}
	if got := strings.TrimSpace(readFile(t, d.path("state/instance"))); got != "1111" {
		t.Errorf("the record must not be altered without a current id: %q", got)
	}
}

func TestImageState_NodeNameMatchIsCaseInsensitive(t *testing.T) {
	d := newDisk(t)
	d.write(t, "state/complete", "")
	d.kubeletIdentity(t, "system:node:Worker-1")
	if out, ok := d.boot(t, true, "1"); ok || d.resets() {
		t.Fatalf("proceeded=%v resets=%v\n%s", ok, d.resets(), out)
	}
}

// A partial bootstrap that was interrupted by a reboot resumes: same instance, no marker.
func TestImageState_InterruptedBootstrapResumesWithoutReset(t *testing.T) {
	d := newDisk(t)
	d.write(t, "state/instance", "1111\n")
	d.kubeletIdentity(t, "system:node:worker-1")
	out, ok := d.boot(t, true, "1111")
	if !ok || d.resets() {
		t.Fatalf("proceeded=%v resets=%v\n%s", ok, d.resets(), out)
	}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
