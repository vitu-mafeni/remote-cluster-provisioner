package ssh

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	cryptossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

func writeKnownHosts(t *testing.T, addr string, keys ...cryptossh.PublicKey) string {
	t.Helper()
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(knownhosts.Line([]string{addr}, k) + "\n")
	}
	p := filepath.Join(t.TempDir(), "known_hosts")
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestKnownHostKeyAlgorithms(t *testing.T) {
	ed, ec := sshtest.NewSigners(t)
	addr := "10.0.0.5:22"
	cb, err := knownhosts.New(writeKnownHosts(t, addr, ed.PublicKey()))
	if err != nil {
		t.Fatal(err)
	}
	got := knownHostKeyAlgorithms(cb, addr)
	if len(got) == 0 || got[0] != cryptossh.KeyAlgoED25519 {
		t.Fatalf("expected ed25519 only, got %v", got)
	}
	for _, a := range got {
		if strings.HasPrefix(a, "ecdsa") {
			t.Errorf("ecdsa offered although only ed25519 is listed: %v", got)
		}
	}
	if algos := knownHostKeyAlgorithms(cb, "10.9.9.9:22"); algos != nil {
		t.Errorf("unknown host must keep the defaults (verification stays fail-closed): %v", algos)
	}
	both, _ := knownhosts.New(writeKnownHosts(t, addr, ec.PublicKey(), ed.PublicKey()))
	if got := knownHostKeyAlgorithms(both, addr); len(got) < 3 || got[0] != cryptossh.KeyAlgoED25519 || !strings.HasPrefix(got[2], "ecdsa") {
		t.Errorf("both types expected, preference ed25519 then ecdsa: %v", got)
	}
	if knownHostKeyAlgorithms(cryptossh.InsecureIgnoreHostKey(), addr) != nil || knownHostKeyAlgorithms(nil, addr) != nil {
		t.Error("non known_hosts callbacks must not restrict algorithms")
	}
}

// The server offers ecdsa AND ed25519; known_hosts lists only ed25519. Without
// the algorithm restriction the client prefers ecdsa and fails with a mismatch.
func TestConnectSucceedsWhenKnownHostsHasOnlyNonPreferredKeyType(t *testing.T) {
	ed, ec := sshtest.NewSigners(t)
	srv := sshtest.Start(t, sshtest.Options{HostKeys: []cryptossh.Signer{ec, ed}})
	addr := net.JoinHostPort(srv.Host, strconv.Itoa(srv.Port))
	t.Setenv(KnownHostsEnv, writeKnownHosts(t, addr, ed.PublicKey()))

	c, err := Connect(srv.Host, srv.Port, "test", "pw")
	if err != nil {
		t.Fatalf("connect with ed25519-only known_hosts failed: %v", err)
	}
	_ = c.Conn.Close()
}

func TestConnectFailsClosedOnWrongOrMissingKey(t *testing.T) {
	ed, ec := sshtest.NewSigners(t)
	otherEd, _ := sshtest.NewSigners(t)
	srv := sshtest.Start(t, sshtest.Options{HostKeys: []cryptossh.Signer{ec, ed}})
	addr := net.JoinHostPort(srv.Host, strconv.Itoa(srv.Port))

	// Listed under a different (attacker) key of the same type.
	t.Setenv(KnownHostsEnv, writeKnownHosts(t, addr, otherEd.PublicKey()))
	if c, err := Connect(srv.Host, srv.Port, "test", "pw"); err == nil {
		_ = c.Conn.Close()
		t.Fatal("connection must fail when the presented key differs from known_hosts")
	}
	// Host not listed at all.
	t.Setenv(KnownHostsEnv, writeKnownHosts(t, "10.1.1.1:22", ed.PublicKey()))
	if c, err := Connect(srv.Host, srv.Port, "test", "pw"); err == nil {
		_ = c.Conn.Close()
		t.Fatal("connection must fail for an unlisted host")
	}
}
