package sshtest

import (
	"os"
	"path/filepath"
	"testing"
)

// Stubs is a directory of fake executables that is prepended to PATH, used to
// execute provisioning scripts for real without touching the host. The default
// `sudo` stub prints the ubiquitous "sudo: unable to resolve host" warning on
// stderr (like a freshly renamed node does) and then runs its arguments, so
// tests also prove that parsed output is not polluted by it.
type Stubs struct {
	Dir    string
	LogDir string
	t      testing.TB
}

// NewStubs creates the stub directory with the default sudo stub.
func NewStubs(t testing.TB) *Stubs {
	t.Helper()
	s := &Stubs{Dir: t.TempDir(), LogDir: t.TempDir(), t: t}
	s.Write("sudo", `#!/bin/bash
[ -z "${FAKE_SUDO_QUIET:-}" ] && echo "sudo: unable to resolve host box: Name or service not known" >&2
[ "$1" = "-n" ] && shift
exec env "$@"
`)
	return s
}

// Write installs an executable stub.
func (s *Stubs) Write(name, body string) {
	s.t.Helper()
	if err := os.WriteFile(filepath.Join(s.Dir, name), []byte(body), 0o755); err != nil {
		s.t.Fatal(err)
	}
}

// Env returns the process environment with the stub dir first on PATH, FAKE_LOG
// pointing at the log dir, and extra KEY=VALUE entries appended.
func (s *Stubs) Env(extra ...string) []string {
	env := append(os.Environ(), "PATH="+s.Dir+":"+os.Getenv("PATH"), "FAKE_LOG="+s.LogDir)
	return append(env, extra...)
}

// Log returns the contents of a log file the stubs appended to ("" if absent).
func (s *Stubs) Log(name string) string {
	b, _ := os.ReadFile(filepath.Join(s.LogDir, name))
	return string(b)
}
