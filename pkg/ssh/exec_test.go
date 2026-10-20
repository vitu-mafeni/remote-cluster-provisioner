package ssh

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

func needBash(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
}

func alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// A zombie still answers signal 0; check /proc state when available.
	if b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat"); err == nil {
		f := strings.Fields(string(b))
		return len(f) > 2 && f[2] != "Z"
	}
	return syscall.Kill(pid, 0) == nil
}

func waitDead(t *testing.T, pid int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if !alive(pid) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	t.Fatalf("process %d is still running", pid)
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n > 0 {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("pid file %s never appeared", path)
	return 0
}

// grandchildScript spawns a long sleep as a grandchild (via a subshell), records
// its pid and then waits.
func grandchildScript(pidFile string) string {
	return "( sleep 300 & echo $! > " + pidFile + "; wait ) &\nwait\n"
}

// The wrapper and killer are plain bash: execute them locally (no SSH) and check
// that the whole group dies, including a grandchild the shell itself never signalled.
func TestWrapperAndKillerKillTheWholeGroup(t *testing.T) {
	needBash(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	pidFile := filepath.Join(tmp, "gc.pid")
	id := newRunID()

	wrapper := exec.Command("bash", "-c", wrapRemoteCommand(id))
	wrapper.Stdin = strings.NewReader(grandchildScript(pidFile))
	// Own session, like sshd gives an exec'd command.
	wrapper.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := wrapper.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{})
	go func() { _ = wrapper.Wait(); close(waited) }()

	gc := readPID(t, pidFile)
	t.Cleanup(func() { _ = syscall.Kill(gc, syscall.SIGKILL) })
	if !alive(gc) {
		t.Fatal("grandchild should be running before the kill")
	}
	if _, err := os.Stat(filepath.Join(tmp, ".cnlab-run-"+id+".pgid")); err != nil {
		t.Fatalf("wrapper did not record the process group id: %v", err)
	}

	// Killing just the wrapper's shell (what the old code did to `bash -s`)
	// must NOT be enough: the grandchild survives it.
	_ = wrapper.Process.Signal(syscall.SIGKILL)
	<-waited
	time.Sleep(200 * time.Millisecond)
	if !alive(gc) {
		t.Fatal("test premise broken: killing only the shell already killed the grandchild")
	}

	out, err := exec.Command("bash", "-c", killRemoteScript(id)).CombinedOutput()
	if err != nil {
		t.Fatalf("killer failed: %v\n%s", err, out)
	}
	waitDead(t, gc, 5*time.Second)
	if _, err := os.Stat(filepath.Join(tmp, ".cnlab-run-"+id+".pgid")); !os.IsNotExist(err) {
		t.Error("killer must remove the pgid file")
	}
}

func TestWrapperPreservesExitStatusAndCleansUp(t *testing.T) {
	needBash(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	id := newRunID()
	c := exec.Command("bash", "-c", wrapRemoteCommand(id))
	c.Stdin = strings.NewReader("echo out; echo err >&2; exit 7\n")
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	var stdout, stderr strings.Builder
	c.Stdout, c.Stderr = &stdout, &stderr
	err := c.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 7 {
		t.Fatalf("exit status not preserved: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != "out" || strings.TrimSpace(stderr.String()) != "err" {
		t.Errorf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if left, _ := filepath.Glob(filepath.Join(tmp, ".cnlab-run-*")); len(left) != 0 {
		t.Errorf("pgid file left behind: %v", left)
	}
}

func TestKillerIsNoOpWhenCommandFinished(t *testing.T) {
	needBash(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	// A stale file pointing at a dead pid must not kill anything or fail.
	id := newRunID()
	if err := os.WriteFile(filepath.Join(tmp, ".cnlab-run-"+id+".pgid"), []byte("999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("bash", "-c", killRemoteScript(id)).CombinedOutput(); err != nil {
		t.Fatalf("killer failed: %v\n%s", err, out)
	}
}

// End to end over a real (in-process) SSH connection: RunCtx must return
// promptly on timeout with partial output AND the remote grandchild must die.
func TestRunCtxCancelKillsRemoteProcessGroup(t *testing.T) {
	needBash(t)
	tmp := t.TempDir()
	srv := sshtest.Start(t, sshtest.Options{Env: []string{"TMPDIR=" + tmp}})
	client := &Client{Conn: srv.Dial(t)}
	pidFile := filepath.Join(tmp, "gc.pid")

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	out, err := RunCtx(ctx, client, "echo started\n"+grandchildScript(pidFile))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("RunCtx did not return promptly: %v", time.Since(start))
	}
	if !strings.Contains(out, "started") {
		t.Errorf("partial output lost: %q", out)
	}
	gc := readPID(t, pidFile)
	t.Cleanup(func() { _ = syscall.Kill(gc, syscall.SIGKILL) })
	waitDead(t, gc, 10*time.Second)
}

func TestRunStdoutKeepsStderrOutOfParsedOutput(t *testing.T) {
	needBash(t)
	srv := sshtest.Start(t, sshtest.Options{})
	client := &Client{Conn: srv.Dial(t)}
	out, err := RunStdout(client, "echo 'sudo: unable to resolve host box' >&2\necho value")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "value" {
		t.Errorf("stdout = %q", out)
	}
	merged, err := Run(client, "echo warn >&2\necho value")
	if err != nil || !strings.Contains(merged, "warn") {
		t.Errorf("Run must still merge stderr: %q %v", merged, err)
	}
	_, err = RunStdout(client, "echo boom >&2; exit 3")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("failure should carry the stderr tail: %v", err)
	}
}
