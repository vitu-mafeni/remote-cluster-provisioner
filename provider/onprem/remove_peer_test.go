package onprem

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	keyA = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	keyB = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="
	keyC = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC="
)

func runRemovePeerScript(t *testing.T, conf, key string) (string, error) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	path := filepath.Join(t.TempDir(), "wg0.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bash, "-s")
	cmd.Env = append(os.Environ(), "WG_CONF="+path, "OUR_KEY="+key)
	cmd.Stdin = strings.NewReader(removePeerScript)
	out, runErr := cmd.CombinedOutput()
	got, _ := os.ReadFile(path)
	if runErr == nil {
		if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
			t.Errorf("wg0.conf must end up 0600, got %v (err %v)", st.Mode().Perm(), err)
		}
	}
	return string(got), func() error {
		if runErr != nil {
			return &scriptErr{string(out)}
		}
		return nil
	}()
}

type scriptErr struct{ out string }

func (e *scriptErr) Error() string { return e.out }

func serverConf(blankSeparated bool) string {
	sep := "\n"
	if !blankSeparated {
		sep = ""
	}
	return "[Interface]\nAddress = 10.8.0.1/24\nPrivateKey = SERVERPRIVATEKEY\nListenPort = 51820\n\n" +
		"[Peer]\nPublicKey = " + keyA + "\nAllowedIPs = 10.8.0.2/32\n" + sep +
		"[Peer]\nPublicKey = " + keyB + "\nAllowedIPs = 10.8.0.3/32\n" + sep +
		"[Peer]\nPublicKey = " + keyC + "\nAllowedIPs = 10.8.0.4/32\n"
}

func TestRemovePeerScript_RemovesOnlyTheTargetPeer(t *testing.T) {
	for _, blank := range []bool{true, false} {
		got, err := runRemovePeerScript(t, serverConf(blank), keyB)
		if err != nil {
			t.Fatalf("blank=%v: %v", blank, err)
		}
		if strings.Contains(got, keyB) || strings.Contains(got, "10.8.0.3") {
			t.Errorf("blank=%v: target peer still present:\n%s", blank, got)
		}
		for _, want := range []string{"[Interface]", "PrivateKey = SERVERPRIVATEKEY", keyA, "10.8.0.2/32", keyC, "10.8.0.4/32"} {
			if !strings.Contains(got, want) {
				t.Errorf("blank=%v: lost %q:\n%s", blank, want, got)
			}
		}
		if n := strings.Count(got, "[Peer]"); n != 2 {
			t.Errorf("blank=%v: expected 2 remaining [Peer] headers, got %d:\n%s", blank, n, got)
		}
	}
}

func TestRemovePeerScript_UnknownKeyLeavesConfigIntact(t *testing.T) {
	conf := serverConf(true)
	got, err := runRemovePeerScript(t, conf, "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD=")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(got, "[Peer]") != 3 || !strings.Contains(got, "SERVERPRIVATEKEY") {
		t.Errorf("config changed for an unknown key:\n%s", got)
	}
}

func TestRemovePeerScript_RefusesToWriteWithoutInterface(t *testing.T) {
	// A config that would lose [Interface]/PrivateKey must not replace the original.
	conf := "[Peer]\nPublicKey = " + keyA + "\nAllowedIPs = 10.8.0.2/32\n"
	got, err := runRemovePeerScript(t, conf, keyA)
	if err == nil {
		t.Fatal("expected the sanity check to refuse the rewrite")
	}
	if got != conf {
		t.Errorf("original config must be left untouched, got:\n%s", got)
	}
}

func TestRemoveVPNPeerFromServerConf_RejectsMalformedKey(t *testing.T) {
	for _, k := range []string{"", "not-a-key", keyA + "'; rm -rf /; '", strings.Repeat("A", 44)} {
		if err := RemoveVPNPeerFromServerConf(nil, k); err == nil || !strings.Contains(err.Error(), "not a WireGuard public key") {
			t.Errorf("key %q: expected validation error, got %v", k, err)
		}
	}
}
