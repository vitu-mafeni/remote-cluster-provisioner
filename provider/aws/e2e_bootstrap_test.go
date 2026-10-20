package aws

import (
	"fmt"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/internal/e2esandbox"
)

// These tests run the WHOLE rendered bootstrap script as root in a disposable
// Ubuntu container (see internal/e2esandbox): hostname, kernel prerequisites,
// runtime, kubelet install, WireGuard, the kubeadm join with its retries, the
// completion marker and the image/clone handling, with the system-changing
// commands stubbed. kubeadm behaves like the real one on a cgroups v1 host. They
// skip when docker or the ubuntu:22.04 image is unavailable (or with -short).

// e2eScript renders the AWS bootstrap script.
func e2eScript(t *testing.T, noVPN bool, version string, mut func(*CloudInitParams)) string {
	t.Helper()
	p := baseParams()
	p.NoVPN = noVPN
	if version != "" {
		p.KubernetesVersion = version
		parts := strings.Split(version, ".")
		p.KubernetesMinorVersion = parts[0] + "." + parts[1]
	}
	if !noVPN {
		p.WGConfig = "[Interface]\nAddress = 10.8.0.5/24\nPrivateKey = x\n"
		p.VpnIP = "10.8.0.5"
	}
	if mut != nil {
		mut(&p)
	}
	script, err := RenderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	return script
}

type e2eHost struct {
	noVPN     bool
	v1        bool   // cgroups v1 host (no unified hierarchy)
	minor     string // Kubernetes minor, e.g. "35"
	id        string // EC2 instance id ("" = metadata unreachable)
	joinFails int
	runs      int
	setup     string
}

func (h e2eHost) env() []string {
	cg := "/work/out/cgroup.controllers"
	if h.v1 {
		cg = "/work/out/none"
	}
	env := []string{"CNLAB_CGROUP_CONTROLLERS=" + cg, "FAKE_K8S_MINOR=" + h.minor, fmt.Sprintf("FAKE_JOIN_FAILS=%d", h.joinFails)}
	if h.id != "" {
		env = append(env, "FAKE_EC2_ID="+h.id)
	}
	if h.noVPN {
		env = append(env, "FAKE_NODE_IP=10.0.0.7")
	} else {
		env = append(env, "FAKE_VPN_IP=10.8.0.5")
	}
	return env
}

// finalState is dumped after the last run.
const finalState = `cat /var/lib/node-bootstrap-status > $OUT/f.status 2>/dev/null
cat /var/lib/node-bootstrap-instance > $OUT/f.instance 2>/dev/null
[ -e /var/lib/node-bootstrap-complete ] && echo yes > $OUT/f.complete
cat /etc/kubernetes/kubelet.conf > $OUT/f.kubelet.conf 2>/dev/null
cat /etc/default/kubelet > $OUT/f.default-kubelet 2>/dev/null
cat /var/lib/node-bootstrap-kubeadm-patches/kubeletconfiguration+merge.yaml > $OUT/f.patch 2>/dev/null
cat /etc/wireguard/wg0.conf > $OUT/f.wg0 2>/dev/null
[ -e /var/lib/kubelet/pki ] && echo yes > $OUT/f.pki
cat /etc/fstab > $OUT/f.fstab 2>/dev/null
[ -d /shared-pvc/checkpoint-storage ] && echo yes > $OUT/f.mountdir
`

func (h e2eHost) run(t *testing.T, script string) e2esandbox.Result {
	t.Helper()
	setup := ""
	if !h.v1 {
		setup = "touch /work/out/cgroup.controllers\n"
	}
	return e2esandbox.Run(t, e2esandbox.Options{
		Script: script, Env: h.env(), Setup: setup + h.setup, After: finalState, Runs: h.runs,
	})
}

func joinLines(calls string) (joins []string, resets int) {
	for _, l := range strings.Split(calls, "\n") {
		switch {
		case strings.HasPrefix(l, "kubeadm join"):
			joins = append(joins, l)
		case strings.HasPrefix(l, "kubeadm reset"):
			resets++
		}
	}
	return
}

// hasFix reports whether a join line carries the cgroups v1 arguments.
func hasFix(line string) bool {
	return strings.Contains(line, "--ignore-preflight-errors=SystemVerification") &&
		strings.Contains(line, "--patches=/var/lib/node-bootstrap-kubeadm-patches")
}

func requireBootstrapped(t *testing.T, r e2esandbox.Result) {
	t.Helper()
	if r.ExitCode != 0 || !strings.Contains(r.Log, "Bootstrap complete") || r.File("f.complete") != "yes\n" ||
		!strings.Contains(r.File("f.status"), "Bootstrap complete") {
		t.Fatalf("the node did not bootstrap:\n%s", r.Dump())
	}
	if u := r.File("unknown.calls"); u != "" {
		t.Errorf("the script ran commands outside the known set (a typo or a new dependency):\n%s", u)
	}
}

// ── fresh nodes ────────────────────────────────────────────────────────────────

func TestE2E_FreshNode(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		for _, tc := range []struct {
			name    string
			v1      bool
			version string
			minor   string
			wantFix bool
		}{
			{"cgroups v2 + Kubernetes 1.35", false, "1.35.0", "35", false},
			{"cgroups v1 + Kubernetes 1.35 (Ubuntu 20.04)", true, "1.35.0", "35", true},
			{"cgroups v1 + Kubernetes 1.36", true, "1.36.2", "36", true},
			{"cgroups v1 + Kubernetes 1.34", true, "1.34.9", "34", false},
			{"cgroups v2 + Kubernetes 1.34", false, "1.34.9", "34", false},
		} {
			t.Run(fmt.Sprintf("noVPN=%v/%s", noVPN, tc.name), func(t *testing.T) {
				h := e2eHost{noVPN: noVPN, v1: tc.v1, minor: tc.minor, id: "i-aaaa"}
				r := h.run(t, e2eScript(t, noVPN, tc.version, nil))
				requireBootstrapped(t, r)

				joins, resets := joinLines(r.Calls())
				if len(joins) != 1 || resets != 0 {
					t.Fatalf("want exactly one join and no reset, got %d joins %d resets:\n%s", len(joins), resets, r.Calls())
				}
				if hasFix(joins[0]) != tc.wantFix {
					t.Errorf("cgroups v1 arguments present = %v, want %v:\n%s", hasFix(joins[0]), tc.wantFix, joins[0])
				}
				if !strings.Contains(joins[0], "--cri-socket=unix:///var/run/crio/crio.sock") || !strings.Contains(joins[0], "kubeadm join 10.0.0.1:6443") {
					t.Errorf("the join itself is wrong: %s", joins[0])
				}
				if tc.wantFix && r.File("f.patch") != "failCgroupV1: false\n" {
					t.Errorf("kubelet patch = %q", r.File("f.patch"))
				}
				if !tc.wantFix && r.File("f.patch") != "" {
					t.Errorf("no kubelet patch expected, got %q", r.File("f.patch"))
				}
				if strings.TrimSpace(r.File("f.instance")) != "i-aaaa" {
					t.Errorf("instance record = %q", r.File("f.instance"))
				}
				if !strings.Contains(r.File("f.kubelet.conf"), "server: https://10.0.0.1:6443") {
					t.Errorf("kubelet.conf = %q", r.File("f.kubelet.conf"))
				}
				wantIP := "10.0.0.7"
				if !noVPN {
					wantIP = "10.8.0.5"
				}
				if got := r.File("f.default-kubelet"); !strings.Contains(got, "--node-ip="+wantIP) {
					t.Errorf("kubelet node-ip = %q, want %s", got, wantIP)
				}
				if strings.TrimSpace(r.File("hostname")) != "worker-1" {
					t.Errorf("hostname = %q", r.File("hostname"))
				}
				if noVPN && r.File("f.wg0") != "" {
					t.Error("no WireGuard config without a VPN")
				}
				if !noVPN && !strings.Contains(r.File("f.wg0"), "Address = 10.8.0.5/24") {
					t.Errorf("wg0.conf = %q", r.File("f.wg0"))
				}
			})
		}
	}
}

// A node whose metadata server is unreachable still bootstraps (it just cannot
// record an instance id).
func TestE2E_FreshNodeWithoutMetadataStillBootstraps(t *testing.T) {
	h := e2eHost{noVPN: false, v1: true, minor: "35"} // no instance id; VPN mode needs no metadata for the node IP
	r := h.run(t, e2eScript(t, false, "1.35.0", nil))
	requireBootstrapped(t, r)
	if r.File("f.instance") != "" {
		t.Errorf("no instance id is known, none must be recorded: %q", r.File("f.instance"))
	}
}

// ── reboots ────────────────────────────────────────────────────────────────────

func TestE2E_RebootOfABootstrappedNodeIsANoOp(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		t.Run(fmt.Sprintf("noVPN=%v", noVPN), func(t *testing.T) {
			h := e2eHost{noVPN: noVPN, v1: true, minor: "35", id: "i-aaaa", runs: 3}
			r := h.run(t, e2eScript(t, noVPN, "1.35.0", nil))
			if r.Exits[0] != 0 || r.Exits[1] != 0 || r.Exits[2] != 0 {
				t.Fatalf("exits = %v\n%s", r.Exits, r.Dump())
			}
			for i := 1; i < 3; i++ {
				if !strings.Contains(r.Logs[i], "Bootstrap already completed; nothing to do") {
					t.Errorf("boot %d must be a no-op:\n%s", i+1, r.Logs[i])
				}
			}
			joins, resets := joinLines(r.Calls())
			if len(joins) != 1 || resets != 0 {
				t.Fatalf("reboots must never rejoin or reset: %d joins %d resets\n%s", len(joins), resets, r.Calls())
			}
			if !strings.Contains(r.File("f.kubelet.conf"), "server: https://10.0.0.1:6443") {
				t.Error("the joined node's kubelet.conf must survive reboots")
			}
		})
	}
}

// ── images and snapshots of bootstrapped nodes ────────────────────────────────

// nodeAState is what an image/snapshot of a fully bootstrapped, joined node "node-a"
// (instance i-old) leaves on the disk.
func nodeAState(withInstanceRecord, vpn bool) string {
	s := `mkdir -p /etc/kubernetes /var/lib/kubelet/pki
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -keyout /var/lib/kubelet/pki/key.pem \
  -out /var/lib/kubelet/pki/kubelet-client-current.pem -subj "/O=system:nodes/CN=system:node:node-a" 2>/dev/null
printf 'server: https://10.0.0.1:6443\nusers:\n- user:\n    client-certificate: /var/lib/kubelet/pki/kubelet-client-current.pem\n' > /etc/kubernetes/kubelet.conf
echo 'KUBELET_EXTRA_ARGS=--node-ip=10.0.0.99' > /etc/default/kubelet
echo SUCCESS > /var/lib/node-bootstrap-status
touch /var/lib/node-bootstrap-complete
: > $OUT/active.kubelet
`
	if withInstanceRecord {
		s += "echo i-old > /var/lib/node-bootstrap-instance\n"
	}
	if vpn {
		s += "printf '[Interface]\\nAddress = 10.8.0.99/24\\n' > /etc/wireguard/wg0.conf\n: > $OUT/active.wg-quick@wg0\n"
	}
	return s
}

func TestE2E_CloneOfABootstrappedNodeIsResetAndBootstrapped(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		for _, withRecord := range []bool{true, false} {
			name := fmt.Sprintf("noVPN=%v/instance record=%v", noVPN, withRecord)
			t.Run(name, func(t *testing.T) {
				h := e2eHost{noVPN: noVPN, v1: true, minor: "35", id: "i-new", setup: nodeAState(withRecord, !noVPN)}
				r := h.run(t, e2eScript(t, noVPN, "1.35.0", nil))
				requireBootstrapped(t, r)

				if strings.Contains(r.Log, "Node is already joined") || strings.Contains(r.Log, "already completed") {
					t.Fatalf("the clone was mistaken for the original node:\n%s", r.Log)
				}
				reason := "bootstrapped by instance i-old, this is i-new"
				if !withRecord {
					reason = "kubelet.conf belongs to system:node:node-a, this node is worker-1"
				}
				if !strings.Contains(r.Log, reason) {
					t.Errorf("the reset reason must be reported (%q):\n%s", reason, r.Log)
				}
				calls := r.Calls()
				joins, resets := joinLines(calls)
				if resets < 1 || len(joins) != 1 {
					t.Fatalf("want a reset then one join, got %d resets %d joins:\n%s", resets, len(joins), calls)
				}
				if strings.Index(calls, "kubeadm reset") > strings.Index(calls, "kubeadm join") {
					t.Error("the stale state must be reset BEFORE joining")
				}
				if !hasFix(joins[0]) {
					t.Errorf("the re-join on a cgroups v1 host needs the fix: %s", joins[0])
				}
				if strings.TrimSpace(r.File("f.instance")) != "i-new" {
					t.Errorf("the new instance must be recorded, got %q", r.File("f.instance"))
				}
				wantIP := "10.0.0.7"
				if !noVPN {
					wantIP = "10.8.0.5"
				}
				if got := r.File("f.default-kubelet"); !strings.Contains(got, "--node-ip="+wantIP) || strings.Contains(got, "10.0.0.99") {
					t.Errorf("the old node-ip must be replaced: %q", got)
				}
				if r.File("f.pki") != "" {
					t.Error("the old node's kubelet client certificate must be gone")
				}
				if !noVPN {
					if !strings.Contains(calls, "systemctl disable --now wg-quick@wg0") {
						t.Error("the stale tunnel must be taken down")
					}
					if w := r.File("f.wg0"); !strings.Contains(w, "10.8.0.5") || strings.Contains(w, "10.8.0.99") {
						t.Errorf("wg0.conf must be the new node's: %q", w)
					}
				}
			})
		}
	}
}

// Rebooting the clone afterwards must be a plain no-op (it is now its own node).
func TestE2E_ClonedNodeRebootsCleanlyAfterItsOwnBootstrap(t *testing.T) {
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-new", runs: 2, setup: nodeAState(true, false)}
	r := h.run(t, e2eScript(t, true, "1.35.0", nil))
	if r.Exits[0] != 0 || r.Exits[1] != 0 {
		t.Fatalf("exits %v\n%s", r.Exits, r.Dump())
	}
	if !strings.Contains(r.Logs[1], "already completed") {
		t.Errorf("second boot must be a no-op:\n%s", r.Logs[1])
	}
	if joins, resets := joinLines(r.Calls()); len(joins) != 1 || resets != 1 {
		t.Errorf("one reset (clone) + one join in total, got %d/%d:\n%s", resets, len(joins), r.Calls())
	}
}

// ── join retries ──────────────────────────────────────────────────────────────

func TestE2E_JoinRetriesKeepTheCgroupV1Fix(t *testing.T) {
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa", joinFails: 2}
	r := h.run(t, e2eScript(t, true, "1.35.0", nil))
	requireBootstrapped(t, r)
	joins, resets := joinLines(r.Calls())
	if len(joins) != 3 || resets != 2 {
		t.Fatalf("2 failures then success = 3 joins + 2 resets, got %d/%d:\n%s", len(joins), resets, r.Calls())
	}
	for i, j := range joins {
		if !hasFix(j) {
			t.Errorf("attempt %d lost the cgroups v1 arguments: %s", i+1, j)
		}
	}
}

// A first boot that fails (5 attempts) leaves no completion marker, so the next
// boot tries again and succeeds.
func TestE2E_FailedBootstrapIsRetriedOnTheNextBoot(t *testing.T) {
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa", joinFails: 5, runs: 2}
	r := h.run(t, e2eScript(t, true, "1.35.0", nil))
	if r.Exits[0] == 0 || !strings.Contains(r.Logs[0], "ERROR: kubeadm join failed after 5 attempts") {
		t.Fatalf("boot 1 must fail after 5 attempts (exit %d):\n%s", r.Exits[0], r.Logs[0])
	}
	if r.Exits[1] != 0 || !strings.Contains(r.Logs[1], "Bootstrap complete") {
		t.Fatalf("boot 2 must succeed (exit %d):\n%s", r.Exits[1], r.Logs[1])
	}
	if r.File("f.complete") != "yes\n" || !strings.Contains(r.File("f.status"), "Bootstrap complete") {
		t.Errorf("final state: complete=%q status=%q", r.File("f.complete"), r.File("f.status"))
	}
	joins, _ := joinLines(r.Calls())
	if len(joins) != 6 {
		t.Errorf("5 failed + 1 successful join, got %d", len(joins))
	}
}

// ── regression guard: without the fix the real failure reproduces ─────────────

// The same container, the same script with the cgroups v1 arguments removed, fails
// exactly like the field report. This is what proves the e2e setup really detects
// the bug (and that the fix is what makes the join pass).
func TestE2E_WithoutTheFixTheJoinFailsLikeTheFieldReport(t *testing.T) {
	script := e2eScript(t, true, "1.35.0", nil)
	broken := strings.Replace(script, `bash -c "$JOIN_CMD $CGROUP_V1_JOIN_ARGS --cri-socket=unix://${CRIO_SOCKET}"`,
		`bash -c "$JOIN_CMD --cri-socket=unix://${CRIO_SOCKET}"`, 1)
	if broken == script {
		t.Fatal("test setup: the join line was not found")
	}
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa"}
	r := h.run(t, broken)
	if r.ExitCode == 0 || !strings.Contains(r.Log, "cgroups v1 support is deprecated") ||
		!strings.Contains(r.Log, "ERROR: kubeadm join failed after 5 attempts") || r.File("f.complete") != "" {
		t.Fatalf("expected the cgroups v1 preflight failure:\n%s", r.Dump())
	}
	// ...and the same script on a cgroups v2 host is fine, so it is the host that mattered.
	h.v1 = false
	if r := h.run(t, broken); r.ExitCode != 0 {
		t.Fatalf("cgroups v2 host must be unaffected:\n%s", r.Dump())
	}
}

// ── every optional parameter at once ───────────────────────────────────────────

func TestE2E_KitchenSinkVariantBootstraps(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		t.Run(fmt.Sprintf("noVPN=%v", noVPN), func(t *testing.T) {
			h := e2eHost{noVPN: noVPN, v1: true, minor: "35", id: "i-aaaa"}
			r := h.run(t, e2eScript(t, noVPN, "1.35.0", func(p *CloudInitParams) {
				p.IsGPUNode = true
				p.SSHUsername = "ubuntu"
				p.Labels = []string{"team=ml"}
				p.InsecureRegistries = []string{"harbor.example.com:30002"}
				p.RuntimeRegistryUser, p.RuntimeRegistryToken = "u", "t0ken"
			}))
			requireBootstrapped(t, r)
			if strings.Contains(r.Log, "t0ken") {
				t.Error("the registry token must never be logged")
			}
		})
	}
}

// An image made by an OLDER script has no instance record, so it is recognised by
// the kubelet certificate's CN. On a minimal image without openssl the CN is read
// from the decoded certificate instead.
func TestE2E_CloneFromOlderScriptIsDetectedEvenWithoutOpenSSL(t *testing.T) {
	setup := nodeAState(false, false) + "export PATH=\"$(printf '%s' \"$PATH\" | sed 's#/usr/local/bin:##')\"\ncommand -v openssl >/dev/null && echo 'openssl still on PATH' > $OUT/openssl.present\n"
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-new", setup: setup}
	r := h.run(t, e2eScript(t, true, "1.35.0", nil))
	if r.File("openssl.present") != "" {
		t.Fatal("test setup: openssl must be unavailable to the script")
	}
	requireBootstrapped(t, r)
	if !strings.Contains(r.Log, "kubelet.conf belongs to another node, this node is worker-1") {
		t.Fatalf("the clone must be recognised without openssl:\n%s", r.Log)
	}
	if joins, resets := joinLines(r.Calls()); len(joins) != 1 || resets < 1 {
		t.Errorf("want a reset and one join, got %d/%d", resets, len(joins))
	}
}

// A node that legitimately kept its identity (same name) is left alone even
// without openssl: the CN matches, so there is nothing to reset.
func TestE2E_SameNodeWithoutOpenSSLIsNeverWiped(t *testing.T) {
	setup := strings.ReplaceAll(nodeAState(false, false), "system:node:node-a", "system:node:worker-1") +
		"export PATH=\"$(printf '%s' \"$PATH\" | sed 's#/usr/local/bin:##')\"\n"
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa", setup: setup}
	r := h.run(t, e2eScript(t, true, "1.35.0", nil))
	if r.ExitCode != 0 || !strings.Contains(r.Log, "Bootstrap already completed; nothing to do") {
		t.Fatalf("a healthy node must be a no-op:\n%s", r.Dump())
	}
	if joins, resets := joinLines(r.Calls()); len(joins) != 0 || resets != 0 {
		t.Errorf("nothing may be rejoined or reset: %d joins %d resets", len(joins), resets)
	}
}

// Node names that are prefixes of each other must not be confused when the CN is
// matched on raw bytes: worker-10's disk on worker-1 is a clone; worker-1's disk on
// worker-1 is not (and a cert for worker-1 must not match a node named worker-10).
func TestE2E_PrefixNamesAreNotConfusedWithoutOpenSSL(t *testing.T) {
	noSSL := "export PATH=\"$(printf '%s' \"$PATH\" | sed 's#/usr/local/bin:##')\"\n"
	t.Run("worker-10's disk on worker-1 is a clone", func(t *testing.T) {
		h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-new",
			setup: strings.ReplaceAll(nodeAState(false, false), "system:node:node-a", "system:node:worker-10") + noSSL}
		r := h.run(t, e2eScript(t, true, "1.35.0", nil))
		requireBootstrapped(t, r)
		if _, resets := joinLines(r.Calls()); resets < 1 {
			t.Fatalf("worker-10's state must be reset on worker-1:\n%s", r.Log)
		}
	})
	t.Run("worker-1's own disk on worker-1 is left alone", func(t *testing.T) {
		h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa",
			setup: strings.ReplaceAll(nodeAState(false, false), "system:node:node-a", "system:node:worker-1") + noSSL}
		r := h.run(t, e2eScript(t, true, "1.35.0", nil))
		if r.ExitCode != 0 || !strings.Contains(r.Log, "already completed") {
			t.Fatalf("must be a no-op:\n%s", r.Dump())
		}
	})
	t.Run("a node named worker-10 is not mistaken for worker-1's disk", func(t *testing.T) {
		h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa",
			setup: strings.ReplaceAll(nodeAState(false, false), "system:node:node-a", "system:node:worker-1") + noSSL}
		r := h.run(t, e2eScript(t, true, "1.35.0", func(p *CloudInitParams) { p.NodeName = "worker-10" }))
		requireBootstrapped(t, r)
		if _, resets := joinLines(r.Calls()); resets < 1 {
			t.Fatalf("worker-1's state must be reset on worker-10:\n%s", r.Log)
		}
	})
}

// ── shared checkpoint storage (NFS from the control plane) ────────────────────

const checkpointMount = "mount -t nfs 10.0.0.1:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage"

func TestE2E_WorkerMountsTheControlPlanesCheckpointStorage(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		for _, v1 := range []bool{true, false} {
			t.Run(fmt.Sprintf("noVPN=%v/cgroups v1=%v", noVPN, v1), func(t *testing.T) {
				h := e2eHost{noVPN: noVPN, v1: v1, minor: "35", id: "i-aaaa"}
				r := h.run(t, e2eScript(t, noVPN, "1.35.0", nil))
				requireBootstrapped(t, r)
				calls := r.Calls()
				if n := strings.Count(calls, checkpointMount); n != 1 {
					t.Fatalf("want exactly one checkpoint mount, got %d:\n%s", n, calls)
				}
				// The control plane's address is the join endpoint; the mount comes after the
				// join so a slow or unreachable NFS server can never delay it.
				if strings.Index(calls, "kubeadm join") > strings.Index(calls, checkpointMount) {
					t.Error("the mount must come after the join")
				}
				if !strings.Contains(calls, "install -y nfs-common") {
					t.Errorf("nfs-common must be installed:\n%s", calls)
				}
				if r.File("f.mountdir") != "yes\n" {
					t.Error("the mount point must exist")
				}
				if n := strings.Count(r.File("f.fstab"), "/shared-pvc/checkpoint-storage"); n != 1 || !strings.Contains(r.File("f.fstab"), "10.0.0.1:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage nfs defaults,_netdev,nofail") {
					t.Errorf("fstab = %q", r.File("f.fstab"))
				}
			})
		}
	}
}

// An unreachable NFS server is a warning: the node still joins and completes.
func TestE2E_UnreachableNFSNeverFailsTheBootstrap(t *testing.T) {
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa"}
	script := e2eScript(t, true, "1.35.0", nil)
	r := e2esandbox.Run(t, e2esandbox.Options{
		Script: script, Env: append(h.env(), "FAKE_NFS_FAIL=1"), After: finalState,
	})
	requireBootstrapped(t, r)
	if !strings.Contains(r.Log, "WARNING: could not mount 10.0.0.1:/srv/nfs/k8s/checkpoint-storage") {
		t.Errorf("the problem must be reported:\n%s", r.Log)
	}
	if !strings.Contains(r.File("f.fstab"), "10.0.0.1:/srv/nfs/k8s/checkpoint-storage") {
		t.Error("the fstab entry must still be written (the boot-time mount retries)")
	}
	if joins, _ := joinLines(r.Calls()); len(joins) != 1 {
		t.Errorf("the join must be unaffected: %d", len(joins))
	}
}

// Reboots never remount or duplicate the fstab entry.
func TestE2E_CheckpointStorageSurvivesRebootsWithoutDuplicates(t *testing.T) {
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-aaaa", runs: 3}
	r := h.run(t, e2eScript(t, true, "1.35.0", nil))
	if r.Exits[2] != 0 {
		t.Fatalf("%s", r.Dump())
	}
	if n := strings.Count(r.Calls(), checkpointMount); n != 1 {
		t.Errorf("reboots must not remount: %d mounts", n)
	}
	if n := strings.Count(r.File("f.fstab"), "/shared-pvc/checkpoint-storage"); n != 1 {
		t.Errorf("fstab must hold one entry, got %d: %q", n, r.File("f.fstab"))
	}
}

// A custom image of a node of ANOTHER cluster carries an fstab entry for the old
// control plane; the new node must end up with exactly one entry, for its own.
func TestE2E_CloneReplacesTheStaleCheckpointFstabEntry(t *testing.T) {
	stale := nodeAState(true, false) +
		"echo '10.0.0.99:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage nfs defaults 0 0' >> /etc/fstab\n"
	h := e2eHost{noVPN: true, v1: true, minor: "35", id: "i-new", setup: stale}
	r := h.run(t, e2eScript(t, true, "1.35.0", nil))
	requireBootstrapped(t, r)
	f := r.File("f.fstab")
	if strings.Contains(f, "10.0.0.99") || strings.Count(f, "/shared-pvc/checkpoint-storage") != 1 || !strings.Contains(f, "10.0.0.1:/srv/nfs/k8s/checkpoint-storage") {
		t.Errorf("fstab = %q", f)
	}
}

// A join command without a usable control-plane host produces a script without the
// storage step at all (and nothing breaks).
func TestRenderBootstrapScript_NoStorageStepWithoutAControlPlaneHost(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	p.JoinCommand = "kubeadm join cp.example.com:6443 --token abc.def --discovery-token-ca-cert-hash sha256:00"
	s, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s, "NFS_SRC_HOST='cp.example.com'") {
		t.Error("a DNS name is a valid NFS server")
	}
	assertBashSyntax(t, s)
}
