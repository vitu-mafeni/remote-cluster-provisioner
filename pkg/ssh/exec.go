package ssh

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	cryptossh "golang.org/x/crypto/ssh"
)

// DefaultRunTimeout bounds every Run call so a hung remote command (e.g. a
// stalled package download) cannot block a reconcile worker forever.
const DefaultRunTimeout = 30 * time.Minute

// safeBuffer is a bytes.Buffer that can be read (for partial output on
// cancellation) while the session's copy goroutines are still writing.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Run executes cmd on the remote host under bash. The script is piped to
// "bash -s" via stdin so that bash-specific features (set -euo pipefail,
// process substitution, etc.) work regardless of the remote user's login shell
// or sshd's default exec shell (/bin/sh on Ubuntu).
//
// Run applies DefaultRunTimeout. Use RunCtx for caller-controlled cancellation.
func Run(client *Client, cmd string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultRunTimeout)
	defer cancel()
	return RunCtx(ctx, client, cmd)
}

// RunCtx is the context-aware variant of Run. stderr is merged into the
// returned output; use RunStdout/RunSplit when the output is parsed.
//
// When ctx is cancelled or times out the remote shell is sent SIGKILL and the
// remote process GROUP (the command runs under setsid) is killed from a second
// session (best effort), the session is closed and the function returns
// immediately with whatever output was produced so far and ctx.Err(); it never
// waits for the remote side to acknowledge.
func RunCtx(ctx context.Context, client *Client, cmd string) (string, error) {
	out := &safeBuffer{}
	err := runSession(ctx, client, cmd, out, out)
	return out.String(), err
}

// RunSplit executes cmd like RunCtx but keeps stdout and stderr apart.
func RunSplit(ctx context.Context, client *Client, cmd string) (stdout, stderr string, err error) {
	so, se := &safeBuffer{}, &safeBuffer{}
	err = runSession(ctx, client, cmd, so, se)
	return so.String(), se.String(), err
}

// RunStdout executes cmd (with DefaultRunTimeout) and returns ONLY stdout, for
// callers that parse the output. Common stderr noise such as
// "sudo: unable to resolve host <name>" therefore cannot corrupt parsed values
// (join commands, WireGuard keys, ports, IPs). On failure the error carries the
// tail of stderr.
func RunStdout(client *Client, cmd string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultRunTimeout)
	defer cancel()
	return RunStdoutCtx(ctx, client, cmd)
}

// RunStdoutCtx is the context-aware variant of RunStdout.
func RunStdoutCtx(ctx context.Context, client *Client, cmd string) (string, error) {
	stdout, stderr, err := RunSplit(ctx, client, cmd)
	if err != nil {
		if t := strings.TrimSpace(Tail(stderr, 2048)); t != "" {
			return stdout, fmt.Errorf("%w: %s", err, t)
		}
		return stdout, err
	}
	return stdout, nil
}

// killTimeout bounds the best-effort remote group kill issued on cancellation.
const killTimeout = 10 * time.Second

// pgidFile is the remote file (path expression, evaluated by the remote shell)
// in which the wrapper records the process-group id of a running command.
func pgidFile(runID string) string {
	return "${TMPDIR:-/tmp}/.cnlab-run-" + runID + ".pgid"
}

func newRunID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// wrapRemoteCommand returns the bash program that runs the script arriving on
// stdin ("bash -s") in a NEW session/process group (setsid) and records that
// group's id (the pid of the inner bash, which is the group leader) in
// pgidFile(runID). Every descendant (apt-get, kubeadm join, oras, ...) then
// shares the group and can be killed together by killRemoteScript. The exit
// status of the script is preserved and the id file is removed on normal exit.
// Without setsid the script simply runs unwrapped (only its pid is recorded).
func wrapRemoteCommand(runID string) string {
	return `f="` + pgidFile(runID) + `"
if command -v setsid >/dev/null 2>&1; then
  setsid -w bash -c 'echo $$ > "$0"; exec bash -s' "$f"
else
  bash -c 'echo $$ > "$0"; exec bash -s' "$f"
fi
rc=$?
rm -f "$f"
exit $rc`
}

// killRemoteScript returns the bash program that SIGKILLs the process group
// recorded by wrapRemoteCommand plus every descendant of the group leader
// (children that moved to their own session, e.g. commands run under sudo with
// use_pty). It uses sudo -n when available because the killed processes may run
// as root. It waits briefly for the id file to appear (cancellation can race
// with the remote start) and is a no-op when the command already finished.
func killRemoteScript(runID string) string {
	return `f="` + pgidFile(runID) + `"
i=0
while [ ! -s "$f" ] && [ "$i" -lt 20 ]; do sleep 0.1; i=$((i+1)); done
pg=$(cat "$f" 2>/dev/null || true)
case "$pg" in ''|*[!0-9]*) exit 0;; esac
[ -d "/proc/$pg" ] || { rm -f "$f"; exit 0; }
K="kill"
if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then K="sudo -n kill"; fi
desc=$(ps -e -o pid= -o ppid= 2>/dev/null | awk -v root="$pg" '{p[$1]=$2} END{for (c in p){x=c; while ((x in p) && x!=root) x=p[x]; if (x==root && c!=root) print c}}')
$K -KILL -- "-$pg" >/dev/null 2>&1 || true
[ -z "$desc" ] || $K -KILL $desc >/dev/null 2>&1 || true
$K -KILL "$pg" >/dev/null 2>&1 || true
rm -f "$f"
exit 0`
}

// bashC returns the ssh exec command line that runs program under bash. The
// program is passed as a single quoted argument so it works with any login
// shell of the remote user.
func bashC(program string) string { return "bash -c " + ShellQuote(program) }

// killRemote runs killRemoteScript in a second, short-lived session. Best
// effort: every error is ignored and the call never outlives killTimeout.
func killRemote(client *Client, runID string) {
	s, err := client.Conn.NewSession()
	if err != nil {
		return
	}
	defer s.Close()
	done := make(chan struct{})
	go func() {
		_ = s.Run(bashC(killRemoteScript(runID)))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(killTimeout):
	}
}

func runSession(ctx context.Context, client *Client, cmd string, stdout, stderr io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session, err := client.Conn.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	session.Stdin = strings.NewReader(cmd)
	session.Stdout = stdout
	session.Stderr = stderr

	runID := newRunID()
	if err := session.Start(bashC(wrapRemoteCommand(runID))); err != nil {
		return err
	}

	done := make(chan error, 1) // buffered: the goroutine never blocks after we return
	go func() { done <- session.Wait() }()

	select {
	case <-ctx.Done():
		// Best effort: signal the remote shell, kill the whole remote process
		// group from a second session (grandchildren such as apt-get or
		// kubeadm join would otherwise keep running and collide with the next
		// retry), then close the session so the Wait goroutine unblocks. None
		// of this is waited for.
		_ = session.Signal(cryptossh.SIGKILL)
		go killRemote(client, runID)
		_ = session.Close()
		return ctx.Err()
	case err := <-done:
		return err
	}
}
