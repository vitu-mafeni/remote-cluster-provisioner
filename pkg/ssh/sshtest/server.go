// Package sshtest provides an in-process SSH server for tests. Every "exec"
// request is run locally under `bash -c`, in its own session (like sshd does
// for non-pty commands), so code that drives remote hosts through pkg/ssh can
// be exercised end to end with stubbed binaries on PATH and no real host.
package sshtest

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"

	cryptossh "golang.org/x/crypto/ssh"
)

// Options configures a Server.
type Options struct {
	// Env entries (KEY=VALUE) appended to the environment of executed commands.
	// Later entries win, so PATH may be prefixed with a stub directory.
	Env []string
	// HostKeys are the host key signers; defaults to one ed25519 and one ecdsa key.
	HostKeys []cryptossh.Signer
}

// Server is a running fake SSH server.
type Server struct {
	Host     string
	Port     int
	HostKeys []cryptossh.Signer

	opts Options
	ln   net.Listener
	wg   sync.WaitGroup
}

// NewSigners returns an ed25519 and an ecdsa (P-256) signer.
func NewSigners(t testing.TB) (ed, ec cryptossh.Signer) {
	t.Helper()
	_, edPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ed, err = cryptossh.NewSignerFromKey(edPriv)
	if err != nil {
		t.Fatal(err)
	}
	ecPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ec, err = cryptossh.NewSignerFromKey(ecPriv)
	if err != nil {
		t.Fatal(err)
	}
	return ed, ec
}

// Start launches a server on 127.0.0.1 and stops it when the test ends.
func Start(t testing.TB, opts Options) *Server {
	t.Helper()
	if len(opts.HostKeys) == 0 {
		ed, ec := NewSigners(t)
		opts.HostKeys = []cryptossh.Signer{ed, ec}
	}
	cfg := &cryptossh.ServerConfig{NoClientAuth: true}
	for _, k := range opts.HostKeys {
		cfg.AddHostKey(k)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	s := &Server{Host: "127.0.0.1", Port: addr.Port, HostKeys: opts.HostKeys, opts: opts, ln: ln}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(nc, cfg)
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// Dial connects a client that trusts any host key.
func (s *Server) Dial(t testing.TB) *cryptossh.Client {
	t.Helper()
	c, err := cryptossh.Dial("tcp", net.JoinHostPort(s.Host, fmt.Sprint(s.Port)), &cryptossh.ClientConfig{
		User:            "test",
		HostKeyCallback: cryptossh.InsecureIgnoreHostKey(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (s *Server) serve(nc net.Conn, cfg *cryptossh.ServerConfig) {
	defer nc.Close()
	conn, chans, reqs, err := cryptossh.NewServerConn(nc, cfg)
	if err != nil {
		return
	}
	defer conn.Close()
	go func() {
		for r := range reqs {
			if r.WantReply {
				_ = r.Reply(r.Type == "keepalive@openssh.com", nil)
			}
		}
	}()
	for nch := range chans {
		if nch.ChannelType() != "session" {
			_ = nch.Reject(cryptossh.UnknownChannelType, "only sessions")
			continue
		}
		ch, chReqs, err := nch.Accept()
		if err != nil {
			continue
		}
		go s.session(ch, chReqs)
	}
}

func (s *Server) session(ch cryptossh.Channel, reqs <-chan *cryptossh.Request) {
	var cmd *exec.Cmd
	for r := range reqs {
		switch r.Type {
		case "exec":
			if cmd != nil {
				_ = r.Reply(false, nil)
				continue
			}
			var p struct{ Command string }
			if err := cryptossh.Unmarshal(r.Payload, &p); err != nil {
				_ = r.Reply(false, nil)
				continue
			}
			c := exec.Command("bash", "-c", p.Command)
			c.Env = append(os.Environ(), s.opts.Env...)
			// sshd runs non-pty commands in their own session.
			c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			cmd = c
			_ = r.Reply(true, nil)
			go run(ch, c)
		case "signal":
			var p struct{ Signal string }
			if err := cryptossh.Unmarshal(r.Payload, &p); err == nil && cmd != nil && cmd.Process != nil {
				if sig, ok := map[string]syscall.Signal{"KILL": syscall.SIGKILL, "TERM": syscall.SIGTERM, "INT": syscall.SIGINT}[p.Signal]; ok {
					_ = cmd.Process.Signal(sig) // only the direct child, like sshd
				}
			}
		default:
			if r.WantReply {
				_ = r.Reply(r.Type == "env", nil)
			}
		}
	}
}

func run(ch cryptossh.Channel, c *exec.Cmd) {
	stdin, err := c.StdinPipe()
	if err != nil {
		_ = ch.Close()
		return
	}
	stdout, _ := c.StdoutPipe()
	stderr, _ := c.StderrPipe()
	if err := c.Start(); err != nil {
		fmt.Fprintf(ch.Stderr(), "start: %v\n", err)
		sendExit(ch, 127)
		_ = ch.Close()
		return
	}
	go func() {
		_, _ = io.Copy(stdin, ch)
		_ = stdin.Close()
	}()
	var copies sync.WaitGroup
	copies.Add(2)
	go func() { defer copies.Done(); _, _ = io.Copy(ch, stdout) }()
	go func() { defer copies.Done(); _, _ = io.Copy(ch.Stderr(), stderr) }()
	// Like sshd: report the exit status as soon as the child exits, but keep
	// the channel open until every holder of the output pipes is gone.
	code := 0
	if werr := waitNoPipes(c); werr != nil {
		var ee *exec.ExitError
		if errors.As(werr, &ee) {
			code = ee.ExitCode()
			if code < 0 {
				code = 255
			}
		} else {
			code = 255
		}
	}
	sendExit(ch, code)
	copies.Wait()
	_ = ch.Close()
}

// waitNoPipes waits for the process only (os/exec's Wait would also close the
// stdout/stderr readers, truncating output still held by grandchildren).
func waitNoPipes(c *exec.Cmd) error {
	st, err := c.Process.Wait()
	if err != nil {
		return err
	}
	if st.Success() {
		return nil
	}
	return &exec.ExitError{ProcessState: st}
}

func sendExit(ch cryptossh.Channel, code int) {
	_, _ = ch.SendRequest("exit-status", false, cryptossh.Marshal(struct{ Status uint32 }{uint32(code)}))
}
