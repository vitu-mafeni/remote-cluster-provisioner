package kubeadm

import (
	"os/exec"
	"strings"
	"testing"
)

func TestCRIOCDIDropInSteps(t *testing.T) {
	steps := crioCDIDropInSteps()
	joined := strings.Join(steps, "\n")
	for _, want := range []string{"enable_cdi = true", "/etc/crio/crio.conf.d/99-cdi.conf", "/etc/cdi", "/var/run/cdi"} {
		if !strings.Contains(joined, want) {
			t.Errorf("CDI drop-in steps missing %q", want)
		}
	}
	// Idempotent: the config file is only written when absent, so a retry or a
	// node that already has it never fails or rewrites it.
	if !strings.Contains(joined, "test -f /etc/crio/crio.conf.d/99-cdi.conf ||") {
		t.Error("CDI drop-in must be guarded by an existence check")
	}
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	for i, s := range steps {
		cmd := exec.Command("bash", "-n")
		cmd.Stdin = strings.NewReader(s)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("step %d: bash -n failed: %v\n%s", i, err, out)
		}
	}
}
