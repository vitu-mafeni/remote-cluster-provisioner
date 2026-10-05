package onprem

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

const wgIface = "[Interface]\nAddress = 10.8.0.1/24\nListenPort = 51820\nPrivateKey = SERVERPRIVATEKEY\n"

func labelled(label, key, ip string) string {
	return "\n# " + label + "\n[Peer]\nPublicKey = " + key + "\nAllowedIPs = " + ip + "\nPersistentKeepalive = 25\n"
}

// blockOf returns the lines of got from the "# label" comment up to (excluding)
// the next "# " comment: the block that label belongs to.
func blockOf(got, label string) string {
	i := strings.Index(got, "# "+label+"\n")
	if i < 0 {
		return ""
	}
	rest := got[i+2:]
	if j := strings.Index(rest, "\n# "); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

func TestRemovePeerScript_CommentsTravelWithTheFollowingPeer(t *testing.T) {
	conf := wgIface + labelled("node-a", keyA, "10.8.0.2/32") + labelled("node-b", keyB, "10.8.0.3/32") + labelled("node-c", keyC, "10.8.0.4/32")
	got, err := runRemovePeerScript(t, conf, keyB)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "node-b") || strings.Contains(got, keyB) {
		t.Errorf("removed peer (or its label) survived:\n%s", got)
	}
	// The old bug attached "# node-c" to the block of the removed peer, so removing
	// B took C's label with it and left B's label on C.
	if b := blockOf(got, "node-c"); !strings.Contains(b, keyC) || strings.Contains(b, keyA) {
		t.Errorf("node-c label detached from its peer:\n%s", got)
	}
	if b := blockOf(got, "node-a"); !strings.Contains(b, keyA) || strings.Contains(b, keyC) {
		t.Errorf("node-a label wrong:\n%s", got)
	}
	want := wgIface + labelled("node-a", keyA, "10.8.0.2/32") + labelled("node-c", keyC, "10.8.0.4/32")
	if got != want {
		t.Errorf("unexpected result.\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestRemovePeerScript_RemovingNothingIsByteIdentical(t *testing.T) {
	conf := wgIface + "\n# trailing comment on interface\n" + labelled("node-a", keyA, "10.8.0.2/32") + "\n\n# end of file comment\n"
	got, err := runRemovePeerScript(t, conf, "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD=")
	if err != nil {
		t.Fatal(err)
	}
	if got != conf {
		t.Errorf("config not preserved byte for byte.\ngot:\n%q\nwant:\n%q", got, conf)
	}
}

func TestRemovePeerScript_CRLF(t *testing.T) {
	conf := strings.ReplaceAll(wgIface+labelled("node-a", keyA, "10.8.0.2/32")+labelled("node-b", keyB, "10.8.0.3/32"), "\n", "\r\n")
	got, err := runRemovePeerScript(t, conf, keyB)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, keyB) || strings.Contains(got, "node-b") {
		t.Errorf("CRLF peer not removed:\n%q", got)
	}
	want := strings.ReplaceAll(wgIface+labelled("node-a", keyA, "10.8.0.2/32"), "\n", "\r\n")
	if got != want {
		t.Errorf("CRLF interface/peer A must be untouched.\ngot: %q\nwant: %q", got, want)
	}
}

func TestRemovePeerScript_PostUpLoadedKeyWorks(t *testing.T) {
	// No PrivateKey line: the server loads its key via PostUp. The old sanity check
	// (`grep PrivateKey`) refused every rewrite for such servers.
	iface := "[Interface]\nAddress = 10.8.0.1/24\nPostUp = wg set %i private-key /etc/wireguard/server.key\n"
	conf := iface + labelled("node-a", keyA, "10.8.0.2/32") + labelled("node-b", keyB, "10.8.0.3/32")
	got, err := runRemovePeerScript(t, conf, keyB)
	if err != nil {
		t.Fatalf("PostUp-keyed server must be supported: %v", err)
	}
	if got != iface+labelled("node-a", keyA, "10.8.0.2/32") {
		t.Errorf("unexpected result:\n%s", got)
	}
}

func TestRemovePeerScript_MultipleAllowedIPsAndInterleavedComments(t *testing.T) {
	conf := wgIface + "\n# node-a\n[Peer]\n# key of a\nPublicKey = " + keyA + "\nAllowedIPs = 10.8.0.2/32, 192.168.5.0/24\n" +
		"\n# node-b\n[Peer]\nPublicKey = " + keyB + "\nAllowedIPs = 10.8.0.3/32\nAllowedIPs = 10.9.0.0/24\n"
	got, err := runRemovePeerScript(t, conf, keyA)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, keyA) || strings.Contains(got, "192.168.5.0") || strings.Contains(got, "key of a") || strings.Contains(got, "node-a") {
		t.Errorf("multi-AllowedIPs peer with an inner comment not fully removed:\n%s", got)
	}
	if !strings.Contains(got, "# node-b\n[Peer]\nPublicKey = "+keyB) || !strings.Contains(got, "10.9.0.0/24") {
		t.Errorf("neighbour damaged:\n%s", got)
	}
}

// A rewrite that changes anything outside the [Peer] blocks must be refused and
// leave the live config untouched. An awk stub corrupts [Interface] whenever the
// real rewrite runs (recognised by its -v our_key argument).
func TestPersistPeerScript_RefusesRewriteThatTouchesInterface(t *testing.T) {
	conf := wgIface + labelled("node-a", keyA, "10.8.0.2/32")
	got, out, err := runPersist(t, conf, "10.8.0.5", keyB, func(s *sshtest.Stubs) {
		s.Write("awk", `#!/bin/bash
for a in "$@"; do
  if [ "$a" = "our_key=$OUR_KEY" ] || [[ "$a" == our_key=* ]]; then
    /usr/bin/awk "$@" | sed 's/ListenPort = 51820/ListenPort = 1/'
    exit ${PIPESTATUS[0]}
  fi
done
exec /usr/bin/awk "$@"
`)
	})
	if err == nil || !strings.Contains(out, "outside the [Peer] blocks") {
		t.Fatalf("expected refusal, err=%v out=%s", err, out)
	}
	if got != conf {
		t.Errorf("live config modified despite refusal:\n%s", got)
	}
}

func TestPersistPeerScript_CommentsAndDroppedStalePeer(t *testing.T) {
	// node-b claims OUR ip under another key and is dropped; node-c's label must stay with node-c.
	conf := wgIface + labelled("node-b", keyB, "10.8.0.5/32") + labelled("node-c", keyC, "10.8.0.7/32")
	got, out, err := runPersist(t, conf, "10.8.0.5", keyA)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if strings.Contains(got, keyB) || strings.Contains(got, "node-b") {
		t.Errorf("stale peer and its label must go:\n%s", got)
	}
	if b := blockOf(got, "node-c"); !strings.Contains(b, keyC) {
		t.Errorf("node-c lost its label:\n%s", got)
	}
	if !strings.Contains(got, "PublicKey = "+keyA+"\nAllowedIPs = 10.8.0.5/32") {
		t.Errorf("our peer missing:\n%s", got)
	}
}

func TestPersistPeerScript_CRLFPostUpAndMultiAllowedIPs(t *testing.T) {
	iface := "[Interface]\nAddress = 10.8.0.1/24\nPostUp = wg set %i private-key /etc/wireguard/server.key\n"
	// Our key already registered with several AllowedIPs including our IP: keep, do not duplicate.
	conf := iface + "\n# ours\n[Peer]\nPublicKey = " + keyA + "\nAllowedIPs = 10.8.0.5/32, 172.16.0.0/16\n"
	got, out, err := runPersist(t, conf, "10.8.0.5", keyA)
	if err != nil {
		t.Fatalf("PostUp-keyed server: %v\n%s", err, out)
	}
	if got != conf {
		t.Errorf("already-registered peer must stay byte-identical:\n%q", got)
	}
	crlf := strings.ReplaceAll(iface+labelled("node-b", keyB, "10.8.0.6/32"), "\n", "\r\n")
	got, out, err = runPersist(t, crlf, "10.8.0.5", keyA)
	if err != nil {
		t.Fatalf("CRLF: %v\n%s", err, out)
	}
	if !strings.Contains(got, keyB) || !strings.Contains(got, "PublicKey = "+keyA) || !strings.HasPrefix(got, strings.ReplaceAll(iface, "\n", "\r\n")) {
		t.Errorf("CRLF config mangled:\n%q", got)
	}
}

// Drive the REAL `sudo env ... flock ... bash -s <<EOF` wrappers through the fake
// SSH server, with stubbed sudo/flock/wg on PATH.
func TestVPNPeerWrappersOverFakeSSH(t *testing.T) {
	stubs := rootStubs(t)
	dir := t.TempDir()
	conf := filepath.Join(dir, "wg0.conf")
	base := wgIface + labelled("node-a", keyA, "10.8.0.2/32")
	if err := os.WriteFile(conf, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	dump := filepath.Join(dir, "dump")
	_ = os.WriteFile(dump, []byte("SERVERPRIV\tSERVERPUB\t51820\toff\n"+keyA+"\t(none)\t1.2.3.4:5\t10.8.0.2/32\t0\t0\t0\t25\n"), 0o600)
	stubs.Write("wg", `#!/bin/bash
case "$*" in
  "show wg0 dump") cat "$FAKE_WG_DUMP" ;;
  set*) echo "wg $*" >> "$FAKE_LOG/wg.calls"; [ -z "${FAKE_WG_SET_FAIL:-}" ] || exit 1 ;;
esac
`)
	stubs.Write("ip", "#!/bin/bash\necho '5: wg0    inet 10.8.0.1/24 scope global wg0'\n")
	srv := sshtest.Start(t, sshtest.Options{Env: []string{
		"PATH=" + stubs.Dir + ":" + os.Getenv("PATH"), "FAKE_LOG=" + stubs.LogDir, "WG_CONF=" + conf, "FAKE_WG_DUMP=" + dump,
	}})
	client := sshClient(srv.Dial(t))

	if err := registerVPNPeer(t.Context(), client, keyB, "10.8.0.3"); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, _ := os.ReadFile(conf)
	if !strings.Contains(string(got), "PublicKey = "+keyB+"\nAllowedIPs = 10.8.0.3/32") || !strings.Contains(string(got), keyA) {
		t.Errorf("peer not persisted through the sudo/flock wrapper:\n%s", got)
	}
	if !strings.Contains(stubs.Log("flock.calls"), "flock /run/lock/wg0-conf.lock") {
		t.Errorf("flock wrapper not exercised: %q", stubs.Log("flock.calls"))
	}
	if !strings.Contains(stubs.Log("wg.calls"), "set wg0 peer "+keyB+" allowed-ips 10.8.0.3/32") {
		t.Errorf("running interface not updated: %q", stubs.Log("wg.calls"))
	}

	// UnregisterVPNPeer removes from the interface and the file, and is safe twice.
	for i := 0; i < 2; i++ {
		if err := UnregisterVPNPeer(client, keyB); err != nil {
			t.Fatalf("unregister #%d: %v", i+1, err)
		}
	}
	got, _ = os.ReadFile(conf)
	if strings.Contains(string(got), keyB) || !strings.Contains(string(got), keyA) {
		t.Errorf("unregister result wrong:\n%s", got)
	}
	if n := strings.Count(stubs.Log("wg.calls"), "peer "+keyB+" remove"); n != 2 {
		t.Errorf("wg set remove calls = %d, want 2 (%q)", n, stubs.Log("wg.calls"))
	}
	if err := UnregisterVPNPeer(client, "junk"); err == nil {
		t.Error("malformed key must be refused")
	}
}

// Registration that fails AFTER `wg set` succeeded must not leave a live peer.
func TestRegisterVPNPeerFailureReleasesTheLivePeer(t *testing.T) {
	stubs := rootStubs(t)
	dir := t.TempDir()
	// A directory as wg0.conf makes the persist step fail after `wg set` ran.
	badConf := filepath.Join(dir, "conf-is-a-dir")
	if err := os.Mkdir(badConf, 0o700); err != nil {
		t.Fatal(err)
	}
	dump := filepath.Join(dir, "dump")
	_ = os.WriteFile(dump, []byte("SERVERPRIV\tSERVERPUB\t51820\toff\n"+keyA+"\t(none)\t1.2.3.4:5\t10.8.0.2/32\t0\t0\t0\t25\n"), 0o600)
	stubs.Write("wg", `#!/bin/bash
case "$*" in
  "show wg0 dump") cat "$FAKE_WG_DUMP" ;;
  set*) echo "wg $*" >> "$FAKE_LOG/wg.calls" ;;
esac
`)
	srv := sshtest.Start(t, sshtest.Options{Env: []string{
		"PATH=" + stubs.Dir + ":" + os.Getenv("PATH"), "FAKE_LOG=" + stubs.LogDir, "WG_CONF=" + badConf, "FAKE_WG_DUMP=" + dump,
	}})
	client := sshClient(srv.Dial(t))

	if err := registerVPNPeer(t.Context(), client, keyB, "10.8.0.3"); err == nil {
		t.Fatal("persist into a directory must fail")
	}
	calls := stubs.Log("wg.calls")
	if !strings.Contains(calls, "set wg0 peer "+keyB+" allowed-ips") || !strings.Contains(calls, "peer "+keyB+" remove") {
		t.Errorf("half-registered peer was not released: %q", calls)
	}

	// A peer that was ALREADY registered before the call is not removed on failure.
	stubs2Calls := stubs.LogDir
	_ = os.Remove(filepath.Join(stubs2Calls, "wg.calls"))
	if err := registerVPNPeer(t.Context(), client, keyA, "10.8.0.2"); err == nil {
		t.Fatal("persist into a directory must fail")
	}
	if strings.Contains(stubs.Log("wg.calls"), "peer "+keyA+" remove") {
		t.Errorf("pre-existing peer must survive a failed re-registration: %q", stubs.Log("wg.calls"))
	}
}
