package onprem

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

func TestGetNextAvailableIP(t *testing.T) {
	tests := []struct {
		name    string
		rng     string
		used    []string
		want    string
		wantErr bool
	}{
		{"first host skips network address", "10.8.0.0/24", nil, "10.8.0.1", false},
		{"host-form range still starts at .1", "10.8.0.1/24", nil, "10.8.0.1", false},
		{"skips used", "10.8.0.0/24", []string{"10.8.0.1", "10.8.0.2"}, "10.8.0.3", false},
		{"reuses released hole", "10.8.0.0/24", []string{"10.8.0.1", "10.8.0.3"}, "10.8.0.2", false},
		{"never returns broadcast", "10.8.0.0/30", []string{"10.8.0.1", "10.8.0.2"}, "", true},
		{"/30 last host", "10.8.0.0/30", []string{"10.8.0.1"}, "10.8.0.2", false},
		{"exhausted /24", "10.8.0.0/24", allHosts("10.8.0.", 1, 254), "", true},
		{"crosses octet", "10.8.0.0/23", allHosts("10.8.0.", 1, 255), "10.8.1.0", false},
		{"invalid", "nope", nil, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getNextAvailableIP(tt.rng, tt.used)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func allHosts(prefix string, from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, prefix+itoa(i))
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestIPUsableInRange(t *testing.T) {
	for ip, ok := range map[string]bool{
		"10.8.0.5":   true,
		"10.8.0.0":   false, // network
		"10.8.0.255": false, // broadcast
		"10.9.0.5":   false, // outside
		"junk":       false,
	} {
		if err := ipUsableInRange("10.8.0.0/24", ip); (err == nil) != ok {
			t.Errorf("ipUsableInRange(%s) err=%v, want ok=%v", ip, err, ok)
		}
	}
}

func TestRenderClientWGConfigUsesRangePrefix(t *testing.T) {
	cfg := renderClientWGConfig("PRIV", "10.8.3.4", 16, "SRVPUB", "203.0.113.9", 51820, "10.8.0.0/16")
	for _, want := range []string{"Address = 10.8.3.4/16", "Endpoint = 203.0.113.9:51820", "AllowedIPs = 10.8.0.0/16"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("config missing %q:\n%s", want, cfg)
		}
	}
	if strings.Contains(cfg, "/24") {
		t.Errorf("config still hardcodes /24:\n%s", cfg)
	}
}

func TestWGKeyRE(t *testing.T) {
	priv, pub, err := generateWireGuardKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	if !wgKeyRE.MatchString(pub) || !wgKeyRE.MatchString(priv) {
		t.Fatalf("generated keys must match wgKeyRE: %q %q", priv, pub)
	}
	if wgKeyRE.MatchString("x'; rm -rf / #") {
		t.Fatal("wgKeyRE accepted garbage")
	}
}

// runPersist executes the real persistPeerScript against a temp wg0.conf
// (WG_CONF override) with no-op chown/install stubs standing in for root.
func runPersist(t *testing.T, conf, ip, key string, stubExtra ...func(*sshtest.Stubs)) (string, string, error) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "wg0.conf")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	stubs := rootStubs(t)
	for _, f := range stubExtra {
		f(stubs)
	}
	cmd := exec.Command("bash", "-s")
	cmd.Stdin = strings.NewReader(persistPeerScript)
	cmd.Env = stubs.Env("WG_CONF="+path, "TARGET_IP="+ip, "OUR_KEY="+key)
	out, err := cmd.CombinedOutput()
	got, _ := os.ReadFile(path)
	st, _ := os.Stat(path)
	if st != nil && st.Mode().Perm() != 0o600 {
		t.Errorf("wg0.conf mode = %v, want 0600", st.Mode().Perm())
	}
	return string(got), string(out), err
}

// rootStubs fakes the root-only bits (chown, install -o/-g, flock) so the real
// server-side scripts can run unprivileged.
func rootStubs(t *testing.T) *sshtest.Stubs {
	t.Helper()
	stubs := sshtest.NewStubs(t)
	stubs.Write("chown", "#!/bin/bash\nexit 0\n")
	stubs.Write("install", `#!/bin/bash
args=()
while [ $# -gt 0 ]; do
  case "$1" in -o|-g) shift 2 ;; *) args+=("$1"); shift ;; esac
done
exec /usr/bin/install "${args[@]}"
`)
	stubs.Write("flock", `#!/bin/bash
# flock -w N LOCKFILE cmd...
[ "$1" = -w ] && shift 2
echo "flock $1" >> "$FAKE_LOG/flock.calls"
shift
exec "$@"
`)
	return stubs
}

const (
	testKeyA = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	testKeyB = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="
	testKeyC = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC="
)

func TestPersistPeerScript(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	iface := "[Interface]\nAddress = 10.8.0.1/24\nListenPort = 51820\nPrivateKey = SERVERPRIVATEKEY\n"

	t.Run("appends peer, keeps header and mode", func(t *testing.T) {
		got, out, err := runPersist(t, iface, "10.8.0.5", testKeyA)
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if !strings.Contains(got, "PrivateKey = SERVERPRIVATEKEY") {
			t.Errorf("lost server private key:\n%s", got)
		}
		if strings.Count(got, "[Peer]") != 1 || !strings.Contains(got, "PublicKey = "+testKeyA) || !strings.Contains(got, "AllowedIPs = 10.8.0.5/32") {
			t.Errorf("peer not appended correctly:\n%s", got)
		}
	})

	t.Run("peer blocks without blank separators keep their [Peer] header", func(t *testing.T) {
		conf := iface +
			"[Peer]\nPublicKey = " + testKeyB + "\nAllowedIPs = 10.8.0.6/32\n" + // no blank line before next header
			"[Peer]\nPublicKey = " + testKeyC + "\nAllowedIPs = 10.8.0.7/32\n"
		got, out, err := runPersist(t, conf, "10.8.0.5", testKeyA)
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if n := strings.Count(got, "[Peer]"); n != 3 {
			t.Errorf("want 3 [Peer] headers, got %d:\n%s", n, got)
		}
		for _, k := range []string{testKeyA, testKeyB, testKeyC} {
			if !strings.Contains(got, "PublicKey = "+k) {
				t.Errorf("missing key %s:\n%s", k, got)
			}
		}
	})

	t.Run("stale block claiming our IP under another key is removed", func(t *testing.T) {
		conf := iface +
			"\n[Peer]\nPublicKey = " + testKeyB + "\nAllowedIPs = 10.8.0.5/32\nPersistentKeepalive = 25\n" +
			"\n[Peer]\nPublicKey = " + testKeyC + "\nAllowedIPs = 10.8.0.55/32\n"
		got, out, err := runPersist(t, conf, "10.8.0.5", testKeyA)
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if strings.Contains(got, testKeyB) {
			t.Errorf("stale peer for our IP survived:\n%s", got)
		}
		if !strings.Contains(got, testKeyC) || !strings.Contains(got, "10.8.0.55/32") {
			t.Errorf("unrelated peer (10.8.0.55) was wrongly removed:\n%s", got)
		}
		if !strings.Contains(got, "PublicKey = "+testKeyA) {
			t.Errorf("our peer missing:\n%s", got)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		first, _, err := runPersist(t, iface, "10.8.0.5", testKeyA)
		if err != nil {
			t.Fatal(err)
		}
		second, out, err := runPersist(t, first, "10.8.0.5", testKeyA)
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if strings.Count(second, "[Peer]") != 1 {
			t.Errorf("re-run duplicated the peer:\n%s", second)
		}
	})

	t.Run("refuses to clobber a config without [Interface]", func(t *testing.T) {
		conf := "[Peer]\nPublicKey = " + testKeyB + "\nAllowedIPs = 10.8.0.6/32\n"
		got, _, err := runPersist(t, conf, "10.8.0.5", testKeyA)
		if err == nil {
			t.Fatal("expected sanity check failure")
		}
		if got != conf {
			t.Errorf("live config was modified despite failure:\n%s", got)
		}
	})
}

func TestPersistPeerScriptSyntax(t *testing.T) {
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(persistPeerScript)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n: %v\n%s", err, out)
	}
}

func TestLockVPNAllocationUnlockIsIdempotent(t *testing.T) {
	u1 := LockVPNAllocation()
	u1()
	u1() // unlock is idempotent
	u2 := LockVPNAllocation()
	u2()
}
