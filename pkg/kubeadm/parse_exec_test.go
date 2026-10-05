package kubeadm

import (
	"strings"
	"testing"

	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// The fake SSH server runs commands locally with a sudo stub that emits the
// common "sudo: unable to resolve host" warning on stderr.
func fakeNode(t *testing.T, stubs *sshtest.Stubs) *sshhelper.Client {
	t.Helper()
	srv := sshtest.Start(t, sshtest.Options{Env: []string{"PATH=" + stubs.Dir + ":" + envPath(), "FAKE_LOG=" + stubs.LogDir}})
	return &sshhelper.Client{Conn: srv.Dial(t)}
}

func TestGetJoinCommandIgnoresSudoHostWarning(t *testing.T) {
	stubs := sshtest.NewStubs(t)
	stubs.Write("kubeadm", "#!/bin/bash\necho \"I0101 preflight noise\" >&2\necho '"+testJoinCmd+"'\n")
	got, err := getJoinCommand(fakeNode(t, stubs))
	if err != nil {
		t.Fatal(err)
	}
	if got != testJoinCmd {
		t.Fatalf("join command = %q", got)
	}
	if err := ValidateJoinCommand(got); err != nil {
		t.Errorf("parsed join command must validate: %v", err)
	}
}

func TestGetTunIPIgnoresSudoHostWarning(t *testing.T) {
	stubs := sshtest.NewStubs(t)
	// `ip` itself prints a warning on stderr, as sudo-wrapped tools do.
	stubs.Write("ip", "#!/bin/bash\necho 'sudo: unable to resolve host box' >&2\necho '    inet 10.8.0.7/24 scope global wg0'\n")
	ip, err := GetTunIP(fakeNode(t, stubs))
	if err != nil || ip != "10.8.0.7" {
		t.Fatalf("ip = %q err = %v", ip, err)
	}
	stubs.Write("ip", "#!/bin/bash\necho 'sudo: unable to resolve host box' >&2\n")
	if _, err := GetTunIP(fakeNode(t, stubs)); err == nil {
		t.Error("a warning alone must not be mistaken for an address")
	}
}

func TestExtractJoinCommand(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		wantErr  bool
	}{
		{testJoinCmd + "\n", testJoinCmd, false},
		{"\n\n" + testJoinCmd, testJoinCmd, false},
		{"junk\n" + testJoinCmd + "\n", testJoinCmd, false},
		{"only-one-unprefixed-line", "only-one-unprefixed-line", false}, // caller's ValidateJoinCommand rejects it
		{"", "", true},
		{"a\nb\n", "", true},
	} {
		got, err := extractJoinCommand(tc.in)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("extractJoinCommand(%q) = %q, %v", tc.in, got, err)
		}
	}
	if !strings.HasPrefix(testJoinCmd, "kubeadm join ") {
		t.Fatal("test fixture")
	}
}
