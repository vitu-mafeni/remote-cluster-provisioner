package controller

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	cryptossh "golang.org/x/crypto/ssh"
)

// fakeSSH is an in-process SSH server that accepts any password and answers
// every `bash -s` session by handing the piped script to a test handler. It lets
// the tests drive the real sshhelper client code paths without a host.
type fakeSSH struct {
	ln      net.Listener
	handler func(script string) (stdout, stderr string, exit int)

	mu      sync.Mutex
	scripts []string
}

func startFakeSSH(t *testing.T, handler func(script string) (string, string, int)) *fakeSSH {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := cryptossh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &cryptossh.ServerConfig{
		PasswordCallback: func(cryptossh.ConnMetadata, []byte) (*cryptossh.Permissions, error) { return nil, nil },
	}
	cfg.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSSH{ln: ln, handler: handler}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(nc, cfg)
		}
	}()
	return f
}

func (f *fakeSSH) port() string {
	return strconv.Itoa(f.ln.Addr().(*net.TCPAddr).Port)
}

// ran counts the recorded scripts containing substr.
func (f *fakeSSH) ran(substr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.scripts {
		if strings.Contains(s, substr) {
			n++
		}
	}
	return n
}

// find returns the first recorded script containing substr ("" if none).
func (f *fakeSSH) find(substr string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.scripts {
		if strings.Contains(s, substr) {
			return s
		}
	}
	return ""
}

func (f *fakeSSH) serve(nc net.Conn, cfg *cryptossh.ServerConfig) {
	conn, chans, reqs, err := cryptossh.NewServerConn(nc, cfg)
	if err != nil {
		_ = nc.Close()
		return
	}
	defer conn.Close()
	go cryptossh.DiscardRequests(reqs)
	for nch := range chans {
		if nch.ChannelType() != "session" {
			_ = nch.Reject(cryptossh.UnknownChannelType, "session only")
			continue
		}
		ch, chReqs, err := nch.Accept()
		if err != nil {
			return
		}
		go f.session(ch, chReqs)
	}
}

func (f *fakeSSH) session(ch cryptossh.Channel, reqs <-chan *cryptossh.Request) {
	defer ch.Close()
	for req := range reqs {
		if req.Type != "exec" {
			_ = req.Reply(false, nil)
			continue
		}
		_ = req.Reply(true, nil)
		script, _ := io.ReadAll(ch) // the client closes stdin after sending the script
		f.mu.Lock()
		f.scripts = append(f.scripts, string(script))
		f.mu.Unlock()

		stdout, stderr, exit := f.handler(string(script))
		_, _ = io.WriteString(ch, stdout)
		_, _ = io.WriteString(ch.Stderr(), stderr)
		_, _ = ch.SendRequest("exit-status", false, cryptossh.Marshal(struct{ Status uint32 }{uint32(exit)}))
		return
	}
}
