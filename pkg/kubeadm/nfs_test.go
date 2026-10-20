package kubeadm

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

func TestNFSServerFromJoinCommand(t *testing.T) {
	for in, want := range map[string]string{
		"kubeadm join 10.8.0.1:6443 --token a.b --discovery-token-ca-cert-hash sha256:00": "10.8.0.1",
		"kubeadm join 172.168.0.10:6443 --token a.b":                                      "172.168.0.10",
		"kubeadm join cp.example.com:6443 --token a.b":                                    "cp.example.com",
		"kubeadm join [fd00::1]:6443 --token a.b":                                         "fd00::1",
		"kubeadm join 10.8.0.1 --token a.b":                                               "", // no port: not a plain endpoint
		"kubeadm join $(id):6443 --token a.b":                                             "",
		"kubeadm join 10.0.0.1;rm:6443":                                                   "",
		"":                                                                                "",
		"kubeadm reset":                                                                   "",
	} {
		if got := NFSServerFromJoinCommand(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestCheckpointStorageMountSnippet_RejectsUnsafeServers(t *testing.T) {
	for _, bad := range []string{"", "a b", "10.0.0.1; rm -rf /", "$(id)", "-oProxy", "a'b", "host/../x"} {
		if got := CheckpointStorageMountSnippet(bad, "sudo"); got != "" {
			t.Errorf("%q must yield no snippet, got:\n%s", bad, got)
		}
	}
	for _, ok := range []string{"10.8.0.1", "cp.example.com", "fd00::1"} {
		if CheckpointStorageMountSnippet(ok, "") == "" {
			t.Errorf("%q must be accepted", ok)
		}
	}
	if s := CheckpointStorageMountSnippet("fd00::1", ""); !strings.Contains(s, "NFS_SRC_HOST='[fd00::1]'") {
		t.Errorf("an IPv6 server needs brackets in NFS syntax:\n%s", s)
	}
}

// nfsEnv is a sandbox for the snippet: stub mount/umount/mountpoint/apt-get on PATH,
// the mount point and fstab redirected into temp paths.
type nfsEnv struct {
	*sshtest.Stubs
	mnt, fstab string
}

func newNFSEnv(t *testing.T, withMountNFS bool) *nfsEnv {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	s := sshtest.NewStubs(t)
	d := t.TempDir()
	e := &nfsEnv{Stubs: s, mnt: filepath.Join(d, "shared-pvc", "checkpoint-storage"), fstab: filepath.Join(d, "fstab")}
	// mount -t nfs SRC DST. The checkpoint export exists when FAKE_EXPORT_EXISTS=1 or
	// after the parent export was mounted (the snippet then creates it, simulated).
	s.Write("mount", `#!/bin/bash
echo "mount $*" >> "$FAKE_LOG/mount.calls"
src="$3"; dst="$4"
case "$src" in
  *:/srv/nfs/k8s/checkpoint-storage)
    n=$(cat "$FAKE_LOG/export.attempts" 2>/dev/null || echo 0); echo $((n+1)) > "$FAKE_LOG/export.attempts"
    if [ "${FAKE_NEVER_MOUNT:-0}" = 1 ]; then echo "mount.nfs: Connection timed out" >&2; exit 32; fi
    if [ "${FAKE_EXPORT_EXISTS:-0}" = 1 ] || [ -e "$FAKE_LOG/export.created" ]; then
      : > "$FAKE_LOG/mounted"; exit 0
    fi
    echo "mount.nfs: mounting $src failed, reason given by server: No such file or directory" >&2; exit 32 ;;
  *:/srv/nfs/k8s)
    [ "${FAKE_PARENT_OK:-1}" = 1 ] || exit 32
    : > "$FAKE_LOG/export.created"; exit 0 ;;
esac
exit 32
`)
	s.Write("umount", "#!/bin/bash\necho \"umount $*\" >> \"$FAKE_LOG/mount.calls\"\n")
	s.Write("mountpoint", "#!/bin/bash\n[ -e \"$FAKE_LOG/mounted\" ]\n")
	s.Write("timeout", "#!/bin/bash\nshift\nexec \"$@\"\n")
	s.Write("sleep", "#!/bin/bash\necho \"sleep $*\" >> \"$FAKE_LOG/sleep.calls\"\n")
	s.Write("apt-get", "#!/bin/bash\necho \"apt-get $*\" >> \"$FAKE_LOG/apt.calls\"\n")
	if withMountNFS {
		s.Write("mount.nfs", "#!/bin/bash\n")
	}
	return e
}

// run executes the snippet (as the given sudo flavour) and returns its output.
func (e *nfsEnv) run(t *testing.T, server, sudo string, extra ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", "-s")
	cmd.Env = e.Env(append([]string{"CNLAB_NFS_MOUNT=" + e.mnt, "CNLAB_FSTAB=" + e.fstab, "FAKE_SUDO_QUIET=1"}, extra...)...)
	cmd.Stdin = strings.NewReader("set -Eeuo pipefail\n" + CheckpointStorageMountSnippet(server, sudo) + "\necho SNIPPET-DONE\n")
	b, err := cmd.CombinedOutput()
	return string(b), err
}

func (e *nfsEnv) fstabLines() []string {
	b, _ := os.ReadFile(e.fstab)
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func TestCheckpointStorageSnippet_MountsExactlyLikeTheManualCommands(t *testing.T) {
	for _, sudo := range []string{"", "sudo"} {
		t.Run("sudo="+sudo, func(t *testing.T) {
			e := newNFSEnv(t, true)
			out, err := e.run(t, "10.8.0.1", sudo, "FAKE_EXPORT_EXISTS=1")
			if err != nil || !strings.Contains(out, "SNIPPET-DONE") {
				t.Fatalf("%v\n%s", err, out)
			}
			if got := strings.TrimSpace(e.Log("mount.calls")); got != "mount -t nfs 10.8.0.1:/srv/nfs/k8s/checkpoint-storage "+e.mnt {
				t.Errorf("mount calls = %q", got)
			}
			if st, err := os.Stat(e.mnt); err != nil || !st.IsDir() {
				t.Errorf("the mount point must be created: %v", err)
			}
			if e.Log("apt.calls") != "" {
				t.Errorf("nfs-common is present: nothing to install, got %q", e.Log("apt.calls"))
			}
			if !strings.Contains(out, "Mounted 10.8.0.1:/srv/nfs/k8s/checkpoint-storage") {
				t.Errorf("output:\n%s", out)
			}
		})
	}
}

func TestCheckpointStorageSnippet_InstallsNFSCommonOnlyWhenMissing(t *testing.T) {
	e := newNFSEnv(t, false) // no mount.nfs
	if out, err := e.run(t, "10.8.0.1", "sudo", "FAKE_EXPORT_EXISTS=1"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	apt := e.Log("apt.calls")
	if !strings.Contains(apt, "update") || !strings.Contains(apt, "install -y nfs-common") {
		t.Errorf("nfs-common must be installed:\n%s", apt)
	}
	// ...and a failed install does not fail the caller.
	e2 := newNFSEnv(t, false)
	e2.Write("apt-get", "#!/bin/bash\nexit 100\n")
	out, err := e2.run(t, "10.8.0.1", "", "FAKE_EXPORT_EXISTS=1")
	if err != nil || !strings.Contains(out, "WARNING: could not mount") || !strings.Contains(out, "SNIPPET-DONE") {
		t.Fatalf("an install failure must be a warning, not a failure: %v\n%s", err, out)
	}
}

func TestCheckpointStorageSnippet_IsIdempotent(t *testing.T) {
	e := newNFSEnv(t, true)
	for i := 0; i < 3; i++ {
		if out, err := e.run(t, "10.8.0.1", "", "FAKE_EXPORT_EXISTS=1"); err != nil {
			t.Fatalf("run %d: %v\n%s", i+1, err, out)
		}
	}
	if n := strings.Count(e.Log("mount.calls"), "mount -t nfs"); n != 1 {
		t.Errorf("an already mounted path must not be mounted again: %d mount calls\n%s", n, e.Log("mount.calls"))
	}
	if lines := e.fstabLines(); len(lines) != 1 {
		t.Errorf("fstab must hold exactly one entry, got %v", lines)
	}
}

func TestCheckpointStorageSnippet_FstabEntryIsPersistentAndOtherLinesAreKept(t *testing.T) {
	e := newNFSEnv(t, true)
	other := "UUID=abcd / ext4 defaults 0 1\n10.9.9.9:/srv/other /mnt/other nfs defaults 0 0\n"
	if err := os.WriteFile(e.fstab, []byte(other), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := e.run(t, "10.8.0.1", "", "FAKE_EXPORT_EXISTS=1"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	lines := e.fstabLines()
	if len(lines) != 3 || lines[0] != "UUID=abcd / ext4 defaults 0 1" || lines[1] != "10.9.9.9:/srv/other /mnt/other nfs defaults 0 0" {
		t.Fatalf("existing lines must be untouched: %v", lines)
	}
	want := "10.8.0.1:/srv/nfs/k8s/checkpoint-storage " + e.mnt + " nfs defaults,_netdev,nofail,x-systemd.automount,x-systemd.mount-timeout=30 0 0"
	if lines[2] != want {
		t.Errorf("fstab entry = %q\nwant        %q", lines[2], want)
	}
}

// A node re-provisioned against another control plane (an image/clone) must not keep
// a stale entry pointing at the old server.
func TestCheckpointStorageSnippet_ReplacesAStaleEntryForTheSameMountPoint(t *testing.T) {
	e := newNFSEnv(t, true)
	stale := "10.8.0.99:/srv/nfs/k8s/checkpoint-storage " + e.mnt + " nfs defaults 0 0\nUUID=abcd / ext4 defaults 0 1\n"
	if err := os.WriteFile(e.fstab, []byte(stale), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := e.run(t, "10.8.0.1", "", "FAKE_EXPORT_EXISTS=1"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	b, _ := os.ReadFile(e.fstab)
	if strings.Contains(string(b), "10.8.0.99") || strings.Count(string(b), e.mnt) != 1 || !strings.Contains(string(b), "UUID=abcd / ext4") {
		t.Errorf("fstab = %q", b)
	}
}

// A control plane provisioned before checkpoint-storage existed: the worker creates
// the directory through the parent export, then mounts it.
func TestCheckpointStorageSnippet_CreatesTheExportDirectoryOnAnOlderControlPlane(t *testing.T) {
	e := newNFSEnv(t, true)
	e.Write("mkdir", "#!/bin/bash\necho \"mkdir $*\" >> \"$FAKE_LOG/mkdir.calls\"\nexec /bin/mkdir \"$@\"\n")
	out, err := e.run(t, "10.8.0.1", "sudo") // export does not exist yet
	if err != nil || !strings.Contains(out, "Mounted 10.8.0.1:/srv/nfs/k8s/checkpoint-storage") {
		t.Fatalf("%v\n%s", err, out)
	}
	calls := strings.Split(strings.TrimSpace(e.Log("mount.calls")), "\n")
	// 1) the checkpoint export (fails: no such dir) 2) the parent export 3) umount 4) the checkpoint export again
	if len(calls) != 4 || !strings.HasPrefix(calls[0], "mount -t nfs 10.8.0.1:/srv/nfs/k8s/checkpoint-storage") ||
		!strings.HasPrefix(calls[1], "mount -t nfs 10.8.0.1:/srv/nfs/k8s ") || !strings.HasPrefix(calls[2], "umount ") ||
		!strings.HasPrefix(calls[3], "mount -t nfs 10.8.0.1:/srv/nfs/k8s/checkpoint-storage "+e.mnt) {
		t.Fatalf("unexpected mount sequence:\n%s", strings.Join(calls, "\n"))
	}
	if !strings.Contains(e.Log("mkdir.calls"), "/checkpoint-storage") {
		t.Errorf("the export directory must be created through the parent mount:\n%s", e.Log("mkdir.calls"))
	}
}

// A control plane that cannot be reached never fails the node: five time-bounded
// attempts, then a warning, exit status 0.
func TestCheckpointStorageSnippet_FailureIsAWarningNeverAnError(t *testing.T) {
	e := newNFSEnv(t, true)
	out, err := e.run(t, "10.8.0.1", "sudo", "FAKE_NEVER_MOUNT=1", "FAKE_PARENT_OK=0")
	if err != nil {
		t.Fatalf("must not fail: %v\n%s", err, out)
	}
	if !strings.Contains(out, "WARNING: could not mount 10.8.0.1:/srv/nfs/k8s/checkpoint-storage") || !strings.Contains(out, "SNIPPET-DONE") {
		t.Errorf("output:\n%s", out)
	}
	if n := strings.Count(e.Log("mount.calls"), "checkpoint-storage"); n < 5 {
		t.Errorf("want 5 attempts, got %d:\n%s", n, e.Log("mount.calls"))
	}
	if len(e.fstabLines()) != 1 {
		t.Error("the fstab entry is still written so the boot-time mount can succeed later")
	}
}
