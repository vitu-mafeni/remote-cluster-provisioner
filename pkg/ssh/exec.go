package ssh

import (
	"context"
	"strings"

	cryptossh "golang.org/x/crypto/ssh"
)

// Run executes cmd on the remote host under bash and respects ctx cancellation.
// The script is piped to "bash -s" via stdin so that bash-specific features
// (set -euo pipefail, process substitution, etc.) work regardless of the
// remote user's login shell or sshd's default exec shell (/bin/sh on Ubuntu).
//
// When ctx is cancelled or times out, the remote process receives SIGKILL and
// the session is closed. The function returns ctx.Err() in that case.
func Run(client *Client, cmd string) (string, error) {
	return RunCtx(context.Background(), client, cmd)
}

// RunCtx is the context-aware variant of Run.
func RunCtx(ctx context.Context, client *Client, cmd string) (string, error) {
	session, err := client.Conn.NewSession()
	if err != nil {
		return "", err
	}

	type result struct {
		out string
		err error
	}
	done := make(chan result, 1)

	session.Stdin = strings.NewReader(cmd)
	go func() {
		output, err := session.CombinedOutput("bash -s")
		done <- result{string(output), err}
	}()

	select {
	case <-ctx.Done():
		// Best-effort: signal the remote process then close the session.
		_ = session.Signal(cryptossh.SIGKILL)
		_ = session.Close()
		// Drain to unblock the goroutine.
		<-done
		return "", ctx.Err()
	case r := <-done:
		_ = session.Close()
		return r.out, r.err
	}
}
