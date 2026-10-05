package onprem

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// vpnFake starts a fake SSH host whose commands run locally against stubs. The
// default sudo stub prints "sudo: unable to resolve host" on stderr.
type vpnFake struct {
	*sshtest.Stubs
	client *sshhelper.Client
	dir    string
}

func newVPNFake(t *testing.T, env ...string) *vpnFake {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	stubs := rootStubs(t)
	dir := t.TempDir()
	stubs.Write("wg", `#!/bin/bash
case "$*" in
  "show wg0 dump") cat "$FAKE_DIR/dump" 2>/dev/null ;;
  "show wg0 public-key") cat "$FAKE_DIR/pubkey" 2>/dev/null ;;
  "show wg0 listen-port") cat "$FAKE_DIR/port" 2>/dev/null ;;
  "show wg0 latest-handshakes") cat "$FAKE_DIR/handshakes" 2>/dev/null ;;
  set*) echo "wg $*" >> "$FAKE_LOG/wg.calls" ;;
esac
`)
	stubs.Write("ip", `#!/bin/bash
case "$*" in
  "-4 addr show wg0") cat "$FAKE_DIR/nodeaddr" 2>/dev/null ;;
  "-4 -o addr show dev wg0") cat "$FAKE_DIR/serveraddr" 2>/dev/null ;;
esac
`)
	all := append([]string{"PATH=" + stubs.Dir + ":" + os.Getenv("PATH"), "FAKE_LOG=" + stubs.LogDir, "FAKE_DIR=" + dir}, env...)
	srv := sshtest.Start(t, sshtest.Options{Env: all})
	return &vpnFake{Stubs: stubs, client: sshClient(srv.Dial(t)), dir: dir}
}

func (f *vpnFake) set(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestBuildClientWGConfigParsesStdoutOnlyDespiteSudoWarning(t *testing.T) {
	f := newVPNFake(t)
	f.Write("wg", `#!/bin/bash
case "$*" in
  "show wg0 public-key") echo "`+keyC+`" ;;
  "show wg0 listen-port") echo 51999 ;;
esac
`)
	cfg, err := buildClientWGConfig(context.Background(), f.client, "10.8.0.9", "10.8.0.0/24", "203.0.113.9", 51820, "PRIV")
	if err != nil {
		t.Fatalf("a sudo host-resolution warning must not break parsing: %v", err)
	}
	// The real command lines are `sudo wg show ...`; the sudo stub prints the
	// host-resolution warning on stderr before running the wg stub.
	for _, want := range []string{"PublicKey = " + keyC, "Endpoint = 203.0.113.9:51999", "Address = 10.8.0.9/24"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("config missing %q:\n%s", want, cfg)
		}
	}
}

func TestBuildClientWGConfigListenPortFallbacks(t *testing.T) {
	for name, tc := range map[string]struct {
		port string
		want string
	}{
		"none":         {"(none)", "203.0.113.9:4242"},
		"empty":        {"", "203.0.113.9:4242"},
		"garbage":      {"51820abc", "203.0.113.9:4242"},
		"out of range": {"70000", "203.0.113.9:4242"},
		"valid":        {"51999", "203.0.113.9:51999"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVPNFake(t)
			f.Write("wg", "#!/bin/bash\ncase \"$*\" in\n  \"show wg0 public-key\") echo "+keyC+" ;;\n  \"show wg0 listen-port\") echo '"+tc.port+"' ;;\nesac\n")
			cfg, err := buildClientWGConfig(context.Background(), f.client, "10.8.0.9", "10.8.0.0/24", "203.0.113.9", 4242, "PRIV")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(cfg, "Endpoint = "+tc.want) {
				t.Errorf("want endpoint %s:\n%s", tc.want, cfg)
			}
		})
	}
}

func TestBuildClientWGConfigRejectsBadServerKey(t *testing.T) {
	f := newVPNFake(t)
	f.Write("wg", "#!/bin/bash\necho 'not a key'\n")
	if _, err := buildClientWGConfig(context.Background(), f.client, "10.8.0.9", "10.8.0.0/24", "203.0.113.9", 51820, "PRIV"); err == nil {
		t.Fatal("invalid server public key must be rejected")
	}
}

// End to end with the REAL command lines ("sudo wg show ...") so the sudo stub's
// warning really lands on stderr.
func TestVPNServerReadersIgnoreSudoWarning(t *testing.T) {
	f := newVPNFake(t)
	f.set(t, "dump", "SERVERPRIV\t"+keyC+"\t51820\toff\n"+keyA+"\t(none)\t1.2.3.4:5\t10.8.0.2/32\t0\t0\t0\t25\n"+keyB+"\t(none)\t(none)\t10.8.0.3/32,10.9.0.0/24\t0\t0\t0\t25\n")
	f.set(t, "serveraddr", "5: wg0    inet 10.8.0.1/24 scope global wg0\n")
	f.set(t, "pubkey", keyC+"\n")
	f.set(t, "port", "51888\n")
	f.set(t, "nodeaddr", "    inet 10.8.0.7/24 scope global wg0\n")

	peers, err := readVPNServerPeers(context.Background(), f.client)
	if err != nil || peers["10.8.0.2"] != keyA || peers["10.8.0.3"] != keyB || peers["10.9.0.0"] != keyB || len(peers) != 3 {
		t.Fatalf("peers = %v err = %v", peers, err)
	}
	addrs, err := readVPNServerAddresses(context.Background(), f.client)
	if err != nil || len(addrs) != 1 || addrs[0] != "10.8.0.1" {
		t.Fatalf("addrs = %v err = %v", addrs, err)
	}
	cfg, err := buildClientWGConfig(context.Background(), f.client, "10.8.0.9", "10.8.0.0/24", "203.0.113.9", 51820, "PRIV")
	if err != nil || !strings.Contains(cfg, "Endpoint = 203.0.113.9:51888") || !strings.Contains(cfg, "PublicKey = "+keyC) {
		t.Fatalf("cfg = %q err = %v", cfg, err)
	}
	ip, key := probeNodeWG(context.Background(), f.client)
	if ip != "10.8.0.7" || key != keyC {
		t.Fatalf("probeNodeWG = %q %q", ip, key)
	}
	ip2, err := allocateVPNIP(context.Background(), f.client, "10.8.0.0/24", nil)
	if err != nil || ip2 != "10.8.0.4" {
		t.Fatalf("allocateVPNIP = %q err = %v (server .1, peers .2 .3)", ip2, err)
	}
}

func TestParseWGDump(t *testing.T) {
	got := parseWGDump("iface\tline\n" + keyA + "\t(none)\t-\t10.8.0.2/32\t0\t0\t0\t25\nnot-a-key\t(none)\t-\t10.8.0.9/32\n\n" + keyB + "\t(none)\t-\t(none)\t0\n")
	if len(got) != 1 || got["10.8.0.2"] != keyA {
		t.Errorf("got %v", got)
	}
}

func TestParseListenPort(t *testing.T) {
	for in, ok := range map[string]bool{"51820": true, " 51820\n": true, "1": true, "65535": true, "0": false, "65536": false, "(none)": false, "": false, "12x": false, "-5": false} {
		if _, got := parseListenPort(in); got != ok {
			t.Errorf("parseListenPort(%q) ok=%v want %v", in, got, ok)
		}
	}
}

func TestHasRecentHandshake(t *testing.T) {
	now := int64(1_800_000_000)
	line := func(age int64) string { return fmt.Sprintf("%s\t%d\n", keyA, now-age) }
	for name, tc := range map[string]struct {
		out  string
		want bool
	}{
		"fresh":          {fmt.Sprintf("%d\n%s", now, line(10)), true},
		"just under 3m":  {fmt.Sprintf("%d\n%s", now, line(179)), true},
		"stale":          {fmt.Sprintf("%d\n%s", now, line(181)), false},
		"never (0)":      {fmt.Sprintf("%d\n%s\t0\n", now, keyA), false},
		"no peers":       {fmt.Sprintf("%d\n", now), false},
		"empty":          {"", false},
		"garbage":        {"x\ny\tz\n", false},
		"future skewed":  {fmt.Sprintf("%d\n%s", now, line(-500)), false},
		"one of several": {fmt.Sprintf("%d\n%s\t0\n%s", now, keyB, line(30)), true},
	} {
		if got := hasRecentHandshake(tc.out, 3*time.Minute); got != tc.want {
			t.Errorf("%s: got %v want %v", name, got, tc.want)
		}
	}
}

func TestNodeHasRecentHandshakeOverFakeSSH(t *testing.T) {
	f := newVPNFake(t)
	f.set(t, "handshakes", fmt.Sprintf("%s\t%d\n", keyA, time.Now().Add(-20*time.Second).Unix()))
	if !nodeHasRecentHandshake(context.Background(), f.client) {
		t.Error("a 20s old handshake is recent")
	}
	f.set(t, "handshakes", fmt.Sprintf("%s\t%d\n", keyA, time.Now().Add(-10*time.Minute).Unix()))
	if nodeHasRecentHandshake(context.Background(), f.client) {
		t.Error("a 10 minute old handshake means the server no longer talks to this wg0")
	}
	f.set(t, "handshakes", keyA+"\t0\n")
	if nodeHasRecentHandshake(context.Background(), f.client) {
		t.Error("a never-handshaked tunnel must not be adopted")
	}
}

func TestReadersReturnPromptlyOnCancelledContext(t *testing.T) {
	f := newVPNFake(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readVPNServerPeers(ctx, f.client); !errors.Is(err, context.Canceled) {
		t.Errorf("readVPNServerPeers err = %v", err)
	}
	if _, err := allocateVPNIP(ctx, f.client, "10.8.0.0/24", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("allocateVPNIP must surface the cancellation instead of falling back to CR-only allocation: %v", err)
	}
}

func TestLockVPNAllocationCtx(t *testing.T) {
	unlock, err := LockVPNAllocationCtx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// A waiter gives up promptly with the context error while the lock is held.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	u2, err := LockVPNAllocationCtx(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || u2 != nil {
		t.Fatalf("waiter must fail with the ctx error, got unlock=%v err=%v", u2 != nil, err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("waiter took %v", d)
	}
	// Release wakes a blocked waiter.
	got := make(chan error, 1)
	go func() {
		u, err := LockVPNAllocationCtx(context.Background())
		if err == nil {
			u()
		}
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	unlock()
	unlock() // idempotent: must not release someone else's hold
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was never woken by unlock")
	}
	// An already-cancelled context never takes a free lock.
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if u, err := LockVPNAllocationCtx(cctx); err == nil {
		u()
		t.Fatal("cancelled context acquired the lock")
	}
	u3, err := LockVPNAllocationCtx(context.Background())
	if err != nil {
		t.Fatalf("lock must be free again: %v", err)
	}
	u3()
}

// ── already-joined guard for the on-prem cleanup ───────────────────────────────

func joinedStubs(t *testing.T) (*sshtest.Stubs, string) {
	t.Helper()
	s := sshtest.NewStubs(t)
	conf := filepath.Join(t.TempDir(), "kubelet.conf")
	s.Write("kubeadm", "#!/bin/bash\necho \"kubeadm $*\" >> \"$FAKE_LOG/kubeadm.calls\"\n")
	s.Write("kubectl", "#!/bin/bash\n[ \"${FAKE_API_HEALTHY:-1}\" = 1 ]\n")
	s.Write("systemctl", "#!/bin/bash\n[ \"$1\" = is-active ]\n")
	return s, conf
}

func runCleanup(t *testing.T, s *sshtest.Stubs, conf, endpoint string, extra ...string) string {
	t.Helper()
	cmd := exec.Command("bash", "-s")
	cmd.Env = s.Env(append([]string{"CNLAB_KUBELET_CONF=" + conf}, extra...)...)
	cmd.Stdin = strings.NewReader("set -o pipefail\n" + kubeadmResetCmd(endpoint))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cleanup step failed: %v\n%s", err, out)
	}
	return string(out)
}

func TestKubeadmResetCmdNeverResetsHealthyJoinedNode(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	const ep = "10.8.0.1:6443"
	t.Run("healthy joined node is left alone", func(t *testing.T) {
		s, conf := joinedStubs(t)
		_ = os.WriteFile(conf, []byte("server: https://"+ep+"\n"), 0o600)
		out := runCleanup(t, s, conf, ep)
		if s.Log("kubeadm.calls") != "" || !strings.Contains(out, "not resetting") {
			t.Errorf("reset ran on a healthy joined node: calls=%q out=%s", s.Log("kubeadm.calls"), out)
		}
	})
	t.Run("fresh node is reset", func(t *testing.T) {
		s, conf := joinedStubs(t)
		runCleanup(t, s, conf, ep)
		if !strings.Contains(s.Log("kubeadm.calls"), "kubeadm reset --force") {
			t.Errorf("reset must still run on a node that is not joined: %q", s.Log("kubeadm.calls"))
		}
	})
	t.Run("node joined to another cluster is reset", func(t *testing.T) {
		s, conf := joinedStubs(t)
		_ = os.WriteFile(conf, []byte("server: https://192.168.1.1:6443\n"), 0o600)
		runCleanup(t, s, conf, ep)
		if !strings.Contains(s.Log("kubeadm.calls"), "kubeadm reset --force") {
			t.Errorf("reset must run when the node belongs to a different cluster")
		}
	})
	t.Run("unhealthy API means reset", func(t *testing.T) {
		s, conf := joinedStubs(t)
		_ = os.WriteFile(conf, []byte("server: https://"+ep+"\n"), 0o600)
		runCleanup(t, s, conf, ep, "FAKE_API_HEALTHY=0")
		if !strings.Contains(s.Log("kubeadm.calls"), "kubeadm reset --force") {
			t.Errorf("a joined node whose API is unreachable is not healthy: reset expected")
		}
	})
}

func TestNodeAlreadyJoinedOverFakeSSH(t *testing.T) {
	s, conf := joinedStubs(t)
	srv := sshtest.Start(t, sshtest.Options{Env: []string{"PATH=" + s.Dir + ":" + os.Getenv("PATH"), "FAKE_LOG=" + s.LogDir, "CNLAB_KUBELET_CONF=" + conf}})
	c := sshClient(srv.Dial(t))
	if nodeAlreadyJoined(context.Background(), c, "10.8.0.1:6443") {
		t.Error("no kubelet.conf: not joined")
	}
	_ = os.WriteFile(conf, []byte("server: https://10.8.0.1:6443\n"), 0o600)
	if !nodeAlreadyJoined(context.Background(), c, "10.8.0.1:6443") {
		t.Error("healthy kubelet.conf for this cluster: joined")
	}
	if nodeAlreadyJoined(context.Background(), c, "10.99.0.1:6443") {
		t.Error("kubelet.conf for a different cluster is not 'joined to this cluster'")
	}
}
