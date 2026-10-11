// Package e2esandbox runs a rendered node bootstrap script END TO END, as root,
// inside a disposable Ubuntu container whose system-changing commands (systemctl,
// apt-get, kubeadm, the metadata servers, ...) are stubs. It is test support only:
// the container filesystem is thrown away, nothing on the host is touched, and the
// container has no network.
//
// The stubs are deliberately simple state machines (see defaultStubs): kubeadm
// behaves like the real one on a cgroups v1 host, the metadata servers answer with
// configured ids, and every call is logged so tests assert on what the script
// actually did.
package e2esandbox

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Image is the container image the scripts run in. It must already exist locally
// (the container never pulls).
const Image = "ubuntu:22.04"

// Available reports whether docker can run the sandbox here.
func Available() bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "image", "inspect", Image).Run() == nil
}

// Options describe one boot of a script.
type Options struct {
	// Script is the bootstrap script to run (bash).
	Script string
	// Env are KEY=VALUE variables exported to the container.
	Env []string
	// Setup is bash run (as root, before the script) to prepare the disk, e.g. the
	// state an image of a bootstrapped node leaves behind.
	Setup string
	// After is bash run once the script finished (to dump extra state into $OUT).
	After string
	// Runs is how many times the script is run back to back in the SAME container
	// (i.e. boots of the same disk); default 1. Each run's log is in Result.Logs.
	Runs int
	// Stubs add or replace stub commands (name -> bash body).
	Stubs map[string]string
	// Mounts are extra read-only bind mounts, "hostpath:containerpath".
	Mounts []string
}

// Result is what a boot produced.
type Result struct {
	// Log is the combined stdout/stderr of the LAST run; Logs has every run's.
	Log  string
	Logs []string
	// ExitCode is the exit status of the LAST run; Exits has every run's.
	ExitCode int
	Exits    []int
	// Dir is the host directory of the container's /work/out files.
	Dir string
}

// File returns a file the run left in $OUT ("" if absent).
func (r Result) File(name string) string {
	b, _ := os.ReadFile(filepath.Join(r.Dir, name))
	return string(b)
}

// Calls returns the stub call log (one "cmd args" line per call).
func (r Result) Calls() string { return r.File("calls.log") }

// Run boots opts.Script in a fresh container and returns the result. It skips the
// test when docker or the image is unavailable.
func Run(t *testing.T, opts Options) Result {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping the container end-to-end test in -short mode")
	}
	if !Available() {
		t.Skip("docker or the " + Image + " image is not available")
	}
	work := t.TempDir()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(work, "stubs"), 0o755))
	must(os.MkdirAll(filepath.Join(work, "out"), 0o777))
	must(os.WriteFile(filepath.Join(work, "script.sh"), []byte(opts.Script), 0o755))
	stubs := defaultStubs()
	for k, v := range opts.Stubs {
		stubs[k] = v
	}
	for name, body := range stubs {
		must(os.WriteFile(filepath.Join(work, "stubs", name), []byte("#!/bin/bash\n"+body), 0o755))
	}
	must(os.WriteFile(filepath.Join(work, "env.sh"), []byte(envFile), 0o644))
	runs := opts.Runs
	if runs < 1 {
		runs = 1
	}
	run := "set +e\nexport PATH=/work/stubs:$PATH FAKE_LOG=/work/out OUT=/work/out BASH_ENV=/work/env.sh\n" +
		baseSystem + opts.Setup + fmt.Sprintf("\nfor i in $(seq 1 %d); do\n  bash /work/script.sh > /work/out/script.$i.log 2>&1\n  echo $? > /work/out/exit.$i\ndone\n", runs) +
		opts.After + "\nchmod -R a+rwX /work\n"
	must(os.WriteFile(filepath.Join(work, "run.sh"), []byte(run), 0o755))

	args := []string{"run", "--rm", "--network", "none", "--pull", "never", "-v", work + ":/work"}
	if p, err := exec.LookPath("openssl"); err == nil { // the base image has no openssl binary
		args = append(args, "-v", p+":/usr/local/bin/openssl:ro")
	}
	if _, err := os.Stat("/usr/lib/ssl/openssl.cnf"); err == nil { // `openssl req` needs its config
		args = append(args, "-v", "/usr/lib/ssl/openssl.cnf:/usr/lib/ssl/openssl.cnf:ro")
	}
	for _, m := range opts.Mounts {
		args = append(args, "-v", m+":ro")
	}
	for _, e := range opts.Env {
		args = append(args, "-e", e)
	}
	args = append(args, "--entrypoint", "bash", Image, "/work/run.sh")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out.String())
	}
	res := Result{Dir: filepath.Join(work, "out")}
	for i := 1; i <= runs; i++ {
		code, err := strconv.Atoi(strings.TrimSpace(res.File(fmt.Sprintf("exit.%d", i))))
		if err != nil {
			t.Fatalf("run %d recorded no exit status (%v)\ncontainer output:\n%s", i, err, out.String())
		}
		res.Logs = append(res.Logs, res.File(fmt.Sprintf("script.%d.log", i)))
		res.Exits = append(res.Exits, code)
	}
	res.Log, res.ExitCode = res.Logs[runs-1], res.Exits[runs-1]
	return res
}

// baseSystem creates the directories and files a real Ubuntu host has and the
// minimal container image lacks.
const baseSystem = `mkdir -p /etc/modules-load.d /etc/sysctl.d /etc/default /etc/systemd/system /etc/systemd/system/kubelet.service.d \
  /etc/containers /etc/cni/net.d /etc/crio /etc/sudoers.d /etc/wireguard /etc/apt/keyrings /etc/apt/sources.list.d \
  /etc/ssh /etc/cdi /var/lib/kubelet /usr/local/bin /usr/local/sbin /var/log /var/lock /run/crio
touch /etc/fstab /etc/hosts
mkdir -p /opt/cni/bin
for p in portmap bridge host-local loopback flannel; do printf '#!/bin/sh\n' > /opt/cni/bin/$p; chmod +x /opt/cni/bin/$p; done
`

// envFile is sourced by every bash in the container: a command the stubs do not
// know is logged and succeeds, so the script never fails on a missing tool.
const envFile = `command_not_found_handle() { echo "UNKNOWN $*" >> /work/out/unknown.calls; return 0; }
`

// defaultStubs returns the stub commands every run gets.
func defaultStubs() map[string]string {
	logCall := `echo "$(basename "$0") $*" >> "$FAKE_LOG/calls.log"` + "\n"
	m := map[string]string{
		"sleep": ":\n",
		// systemd: everything succeeds; is-active reflects units the test marks active.
		"systemctl": logCall + `cmd="$1"; shift
now=0; units=()
for a in "$@"; do
  case "$a" in --now) now=1 ;; -*) ;; *) units+=("${a%.service}") ;; esac
done
case "$cmd" in
  is-active) [ -e "$FAKE_LOG/active.${units[0]}" ] || [ "${FAKE_ALL_ACTIVE:-0}" = 1 ]; exit ;;
  start|restart|reload-or-restart) for u in "${units[@]}"; do : > "$FAKE_LOG/active.$u"; done ;;
  enable) [ "$now" = 1 ] && for u in "${units[@]}"; do : > "$FAKE_LOG/active.$u"; done ;;
  stop) for u in "${units[@]}"; do rm -f "$FAKE_LOG/active.$u"; done ;;
  disable) [ "$now" = 1 ] && for u in "${units[@]}"; do rm -f "$FAKE_LOG/active.$u"; done ;;
esac
exit 0
`,
		"apt-get":  logCall + "exit 0\n",
		"apt-mark": logCall + "exit 0\n",
		"hostnamectl": logCall + `[ "$1" = set-hostname ] && echo "$2" > "$FAKE_LOG/hostname"
exit 0
`,
		"modprobe": logCall + "exit 0\n",
		"sysctl":   logCall + "exit 0\n",
		"swapoff":  logCall + "exit 0\n",
		"dpkg": `case "$1" in --print-architecture) echo amd64 ;; *) echo "dpkg $*" >> "$FAKE_LOG/calls.log" ;; esac
exit 0
`,
		"fuser":  "exit 1\n", // nobody holds the dpkg locks
		"visudo": logCall + "exit 0\n",
		// The container has no sudo and runs as root: pass the command through.
		"sudo": `[ "$1" = "-n" ] && shift
exec "$@"
`,
		"killall":    logCall + "exit 0\n",
		"journalctl": "exit 0\n",
		"cloud-init": logCall + "exit 0\n",
		"gpg":        "cat >/dev/null 2>&1; exit 0\n",
		"ip": `case "$*" in
  *"addr show wg0"*) [ -n "${FAKE_VPN_IP:-}" ] && echo "    inet ${FAKE_VPN_IP}/24 scope global wg0" ;;
esac
exit 0
`,
		"wg":       logCall + "exit 0\n",
		"wg-quick": logCall + "exit 0\n",
		// Container runtime pieces the OCI artifact would install: already present.
		"oras":          `[ "$1" = version ] && echo "Version:    1.3.2"; exit 0` + "\n",
		"cnlab-runtime": `[ "$1" = version ] && echo "cnlab-runtime 1.0.0-beta"; exit 0` + "\n",
		"crio":          "exit 0\n",
		"crictl":        logCall + "echo '{}'\nexit 0\n",
		"kubelet":       "exit 0\n",
		"kubectl": logCall + `# healthy only when kubelet.conf exists (a joined node)
[ -e "${CNLAB_KUBELET_CONF:-/etc/kubernetes/kubelet.conf}" ] && [ "${FAKE_API_HEALTHY:-1}" = 1 ]
`,
		"curl": `args="$*"
echo "curl $args" >> "$FAKE_LOG/curl.calls"
case "$args" in
  # GCE metadata server
  *computeMetadata/v1/instance/id*) [ -n "${FAKE_GCE_ID:-}" ] && { printf '%s' "$FAKE_GCE_ID"; exit 0; }; exit 22 ;;
  *computeMetadata/v1/instance/network-interfaces/0/ip*) [ -n "${FAKE_NODE_IP:-}" ] && { printf '%s' "$FAKE_NODE_IP"; exit 0; }; exit 22 ;;
  # EC2 IMDSv2
  *latest/api/token*) [ -n "${FAKE_EC2_ID:-}" ] && { printf 'tok'; exit 0; }; exit 22 ;;
  *meta-data/instance-id*) [ -n "${FAKE_EC2_ID:-}" ] && { printf '%s' "$FAKE_EC2_ID"; exit 0; }; exit 22 ;;
  *meta-data/local-ipv4*) [ -n "${FAKE_NODE_IP:-}" ] && { printf '%s' "$FAKE_NODE_IP"; exit 0; }; exit 22 ;;
  # control-plane API server health
  */healthz*) exit 0 ;;
esac
exit 0
`,
		// kubeadm behaves like the real one: on a cgroups v1 host with Kubernetes >=
		// 1.35 the SystemVerification preflight fails unless ignored, and the kubelet
		// refuses to start unless a kubeletconfiguration patch sets failCgroupV1: false.
		"kubeadm": logCall + `case "$1" in
  join)
    conf="${CNLAB_KUBELET_CONF:-/etc/kubernetes/kubelet.conf}"
    if [ -e "$conf" ]; then echo "[preflight] FileAvailable--etc-kubernetes-kubelet.conf already exists" >&2; exit 1; fi
    if [ ! -e "${CNLAB_CGROUP_CONTROLLERS:-/sys/fs/cgroup/cgroup.controllers}" ] && [ "${FAKE_K8S_MINOR:-35}" -ge 35 ]; then
      case "$*" in
        *--ignore-preflight-errors=SystemVerification*) ;;
        *)
          echo "[preflight] Some fatal errors occurred:" >&2
          echo "        [ERROR SystemVerification]: cgroups v1 support is deprecated and will be removed in a future release. Please migrate to cgroups v2." >&2
          echo "error: error execution phase preflight: preflight checks failed" >&2
          exit 1 ;;
      esac
      pdir="$(printf '%s' "$*" | sed -n 's/.*--patches=\([^ ]*\).*/\1/p')"
      if ! cat "$pdir"/kubeletconfiguration*.yaml 2>/dev/null | grep -qx 'failCgroupV1: false'; then
        echo "kubelet is configured to not run on a host using cgroup v1" >&2
        exit 1
      fi
    fi
    n=$(cat "$FAKE_LOG/join.count" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$FAKE_LOG/join.count"
    [ "$n" -le "${FAKE_JOIN_FAILS:-0}" ] && exit 1
    mkdir -p "$(dirname "$conf")" /var/lib/kubelet
    echo "server: https://$(printf '%s' "$2")" > "$conf"
    : > "$FAKE_LOG/active.kubelet" ;;
  init)
    # control plane: same cgroups v1 rules; the kubelet config comes from --config
    cfg="$(printf '%s' "$*" | sed -n 's/.*--config \([^ ]*\).*/\1/p')"
    if [ ! -e "${CNLAB_CGROUP_CONTROLLERS:-/sys/fs/cgroup/cgroup.controllers}" ] && [ "${FAKE_K8S_MINOR:-35}" -ge 35 ]; then
      case "$*" in
        *--ignore-preflight-errors=SystemVerification*) ;;
        *)
          echo "[ERROR SystemVerification]: cgroups v1 support is deprecated and will be removed in a future release." >&2
          exit 1 ;;
      esac
      grep -qx 'failCgroupV1: false' "$cfg" 2>/dev/null || { echo "kubelet is configured to not run on a host using cgroup v1" >&2; exit 1; }
    fi
    mkdir -p /etc/kubernetes
    echo "kind: Config" > /etc/kubernetes/admin.conf
    : > "$FAKE_LOG/active.kubelet" ;;
  reset)
    rm -f "${CNLAB_KUBELET_CONF:-/etc/kubernetes/kubelet.conf}"; rm -f "$FAKE_LOG/active.kubelet" ;;
esac
exit 0
`,
	}
	return m
}

// Dump formats a failed result for t.Fatalf.
func (r Result) Dump() string {
	return fmt.Sprintf("exit=%d\n--- script log ---\n%s\n--- calls ---\n%s\n--- unknown commands ---\n%s",
		r.ExitCode, r.Log, r.Calls(), r.File("unknown.calls"))
}
