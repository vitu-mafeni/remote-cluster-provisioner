package kubeadm

import (
	"os/exec"
	"strings"
	"testing"
)

func TestTimeSyncScripts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script string
		want   []string
	}{
		{"VPN control plane", ControlPlaneTimeSyncStep("10.8.0.1", true),
			[]string{"server time.google.com iburst", "bindaddress 10.8.0.1\nallow\n", "After=wg-quick@wg0.service"}},
		{"SSH worker", WorkerTimeSyncSnippet("kubeadm join 10.8.0.1:6443 --token abc", "sudo"),
			[]string{"sudo bash -e -s", "server 10.8.0.1 iburst", "port 0"}},
		{"cloud worker", WorkerTimeSyncSnippet("kubeadm join [fd00::1]:6443 --token abc", ""),
			[]string{"server fd00::1 iburst", "port 0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, want := range append(tc.want, "set-timezone Asia/Seoul", "makestep 1.0 -1", "timeout 125 chronyc waitsync", "enable --now chrony.service") {
				if !strings.Contains(tc.script, want) {
					t.Errorf("time setup missing %q", want)
				}
			}
			// Bash does not syntax-check the inner script when it is a here-document.
			_, body, _ := strings.Cut(tc.script, "\n")
			body = strings.TrimSuffix(body, "RCP_TIME_SYNC\n")
			cmd := exec.Command("bash", "-n")
			cmd.Stdin = strings.NewReader(body)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("invalid bash: %v\n%s", err, out)
			}
		})
	}
}
