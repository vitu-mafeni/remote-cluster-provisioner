package onprem

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
)

const flowJoinCmd = "kubeadm join 10.0.0.9:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:00"

// flowFake is a fake node/VPN-server host for NewInClusterProvisioner. SAFETY: its
// sudo executes only read-only helpers; everything else is logged and skipped, and
// curl/gpg always fail, so a regression in the skip logic can neither touch the
// machine running the tests nor reach the network.
func flowFake(t *testing.T, conf string, extraEnv ...string) *vpnFake {
	t.Helper()
	f := newVPNFake(t, append([]string{"TMPDIR=" + t.TempDir(), "CNLAB_KUBELET_CONF=" + conf}, extraEnv...)...)
	f.Write("sudo", `#!/bin/bash
[ "$1" = "-n" ] && shift
case "$1" in
  grep|timeout|kubectl|env|wg) exec "$@" ;;
  *) echo "sudo $*" >> "$FAKE_LOG/sudo.calls"; exit 0 ;;
esac
`)
	f.Write("kubectl", "#!/bin/bash\n[ \"${FAKE_API_HEALTHY:-1}\" = 1 ]\n")
	f.Write("systemctl", "#!/bin/bash\n[ \"$1\" = is-active ]\n")
	f.Write("curl", "#!/bin/bash\nexit 22\n")
	f.Write("gpg", "#!/bin/bash\nexit 1\n")
	return f
}

func flowInputs(vpn bool) (*mlv1alpha1.NodeProvision, *mlv1alpha1.NodeProvisionNetConfig) {
	np := &mlv1alpha1.NodeProvision{}
	np.Name = "node-1"
	nc := &mlv1alpha1.NodeProvisionNetConfig{}
	nc.Spec.SoftwareConfig.KubernetesVersion = "1.35.0"
	nc.Status.ClusterJoinCommand = flowJoinCmd
	if vpn {
		rng := "10.8.0.0/24"
		nc.Spec.VPNRange = &rng
		nc.Spec.VPNServerPublicConfig.PublicIP = "203.0.113.9"
	} else {
		np.Spec.DisableVPN = true
		np.Spec.IPAddress = "10.0.0.5"
	}
	return np, nc
}

func TestNewInClusterProvisioner_HealthyJoinedNodeIsNotTouched(t *testing.T) {
	conf := filepath.Join(t.TempDir(), "kubelet.conf")
	if err := os.WriteFile(conf, []byte("server: https://10.0.0.9:6443\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := flowFake(t, conf)
	f.Write("ip", "#!/bin/bash\necho '2: eth0    inet 10.0.0.5/24 scope global eth0'\n")
	np, nc := flowInputs(false)

	var steps []string
	ip, key, err := NewInClusterProvisioner(context.Background(), np, nil, f.client, nil, nc, func(s string) { steps = append(steps, s) }, pkgruntime.Config{})
	if err != nil {
		t.Fatalf("joined node: %v", err)
	}
	if ip != "10.0.0.5" || key != "" {
		t.Errorf("ip=%q key=%q", ip, key)
	}
	if calls := f.Log("sudo.calls"); calls != "" {
		t.Errorf("nothing may be executed on a healthy joined node, got:\n%s", calls)
	}
	if !strings.Contains(strings.Join(steps, "|"), "already joined") {
		t.Errorf("skip not reported: %v", steps)
	}
}

// wg0 exists but never handshakes (VPN server rebuilt / re-keyed): the node's
// identity must NOT be adopted; a fresh key is registered instead.
func TestNewInClusterProvisioner_AdoptsWG0OnlyWithRecentHandshake(t *testing.T) {
	for name, tc := range map[string]struct {
		handshakeAge time.Duration // 0 = never
		adopt        bool
	}{
		"recent handshake adopts":        {20 * time.Second, true},
		"stale handshake is not adopted": {10 * time.Minute, false},
		"no handshake is not adopted":    {0, false},
	} {
		t.Run(name, func(t *testing.T) {
			np, nc := flowInputs(true)
			serverConf := filepath.Join(t.TempDir(), "wg0.conf")
			_ = os.WriteFile(serverConf, []byte(wgIface), 0o600)
			f2 := flowFake(t, filepath.Join(t.TempDir(), "kubelet.conf"), "WG_CONF="+serverConf)
			f2.set(t, "pubkey", keyC+"\n")
			f2.set(t, "port", "51820\n")
			f2.set(t, "serveraddr", "5: wg0    inet 10.8.0.1/24 scope global wg0\n")
			f2.set(t, "nodeaddr", "    inet 10.8.0.7/24 scope global wg0\n")
			hs := keyA + "\t0\n"
			if tc.handshakeAge > 0 {
				hs = fmt.Sprintf("%s\t%d\n", keyA, time.Now().Add(-tc.handshakeAge).Unix())
			}
			f2.set(t, "handshakes", hs)

			ip, key, err := NewInClusterProvisioner(context.Background(), np, nil, f2.client, f2.client, nc, nil, pkgruntime.Config{})
			// The run always ends in an error here (the stubbed environment cannot
			// install the runtime); what matters is the identity it registered.
			if err == nil {
				t.Fatal("expected the stubbed run to stop with an error")
			}
			t.Logf("run stopped as expected: %.160s", strings.ReplaceAll(err.Error(), "\n", " "))
			if tc.adopt {
				if ip != "10.8.0.7" || key != keyC {
					t.Errorf("recent handshake: existing identity must be adopted, got ip=%q key=%q err=%v", ip, key, err)
				}
			} else {
				if key == keyC || key == "" || ip == "10.8.0.7" {
					t.Errorf("no recent handshake: a fresh identity must be registered, got ip=%q key=%q err=%v", ip, key, err)
				}
				if !strings.Contains(f2.Log("sudo.calls"), "wg-quick") {
					t.Errorf("tunnel must be reconfigured:\n%s", f2.Log("sudo.calls"))
				}
			}
		})
	}
}

// The VPN allocation lock wait honours the context.
func TestNewInClusterProvisioner_ReturnsWhileWaitingForVPNLock(t *testing.T) {
	f := flowFake(t, filepath.Join(t.TempDir(), "kubelet.conf"))
	np, nc := flowInputs(true)
	f.set(t, "pubkey", keyC+"\n")
	unlock := LockVPNAllocation()
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := NewInClusterProvisioner(ctx, np, nil, f.client, f.client, nc, nil, pkgruntime.Config{})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("must return promptly with an error, err=%v after %v", err, time.Since(start))
	}
}

// Without the VPN spec.ipAddress becomes the kubelet node IP, which must be bound
// locally. A wrong (e.g. NATed public) address has to fail before any install
// step ran, not after the runtime and packages were installed.
func TestNewInClusterProvisioner_VPNlessUnboundIPFailsBeforeAnyStep(t *testing.T) {
	f := flowFake(t, filepath.Join(t.TempDir(), "kubelet.conf"))
	f.Write("ip", "#!/bin/bash\necho '2: eth0    inet 192.168.1.5/24 scope global eth0'\n") // not 10.0.0.5
	np, nc := flowInputs(false)

	var steps []string
	_, _, err := NewInClusterProvisioner(context.Background(), np, nil, f.client, nil, nc, func(s string) { steps = append(steps, s) }, pkgruntime.Config{})
	if err == nil || !strings.Contains(err.Error(), "spec.ipAddress") {
		t.Fatalf("expected the spec.ipAddress error, got %v", err)
	}
	if calls := f.Log("sudo.calls"); calls != "" {
		t.Errorf("no provisioning step may run before the IP check, got:\n%s", calls)
	}
	if len(steps) == 0 || !strings.Contains(steps[len(steps)-1], "verifying") {
		t.Errorf("the IP check must be the first reported step: %v", steps)
	}
}
