package gcp

import (
	"fmt"
	"strings"
	"testing"

	"dcn.ssu.ac.kr/infra/internal/e2esandbox"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

// These tests run the WHOLE rendered GCE startup script as root in a disposable
// Ubuntu container (see internal/e2esandbox) with the GCE metadata server and the
// system-changing commands stubbed; kubeadm behaves like the real one on a
// cgroups v1 host (Ubuntu 20.04). They skip when docker or the ubuntu:22.04 image is
// unavailable (or with -short).

func e2eScript(t *testing.T, noVPN bool, version string) string {
	t.Helper()
	np := testNP()
	np.Spec.DisableVPN = noVPN
	wg, ip := "", ""
	if !noVPN {
		wg, ip = "[Interface]\nAddress = 10.8.0.5/24\nPrivateKey = x\n", "10.8.0.5"
	}
	parts := strings.Split(version, ".")
	p := StartupParams(np, testJoin, version, parts[0]+"."+parts[1], pkgruntimeConfig(), wg, ip)
	script, err := BuildStartupScript(p)
	if err != nil {
		t.Fatal(err)
	}
	return script
}

type host struct {
	noVPN     bool
	v1        bool
	minor     string
	id        string // GCE instance id ("" = metadata unreachable)
	joinFails int
	runs      int
	setup     string
}

func (h host) run(t *testing.T, script string) e2esandbox.Result {
	t.Helper()
	cg := "/work/out/cgroup.controllers"
	setup := "touch /work/out/cgroup.controllers\n"
	if h.v1 {
		cg, setup = "/work/out/none", ""
	}
	env := []string{"CNLAB_CGROUP_CONTROLLERS=" + cg, "FAKE_K8S_MINOR=" + h.minor, fmt.Sprintf("FAKE_JOIN_FAILS=%d", h.joinFails)}
	if h.id != "" {
		env = append(env, "FAKE_GCE_ID="+h.id)
	}
	if h.noVPN {
		env = append(env, "FAKE_NODE_IP=10.128.0.7")
	} else {
		env = append(env, "FAKE_VPN_IP=10.8.0.5")
	}
	return e2esandbox.Run(t, e2esandbox.Options{
		Script: script, Env: env, Setup: setup + h.setup, Runs: h.runs,
		After: `cat /var/lib/node-bootstrap-instance > $OUT/f.instance 2>/dev/null
[ -e /var/lib/node-bootstrap-complete ] && echo yes > $OUT/f.complete
cat /var/lib/node-bootstrap-status > $OUT/f.status 2>/dev/null
cat /etc/default/kubelet > $OUT/f.default-kubelet 2>/dev/null
cat /var/lib/node-bootstrap-kubeadm-patches/kubeletconfiguration+merge.yaml > $OUT/f.patch 2>/dev/null
cat /etc/wireguard/wg0.conf > $OUT/f.wg0 2>/dev/null
[ -e /var/lib/kubelet/pki ] && echo yes > $OUT/f.pki
`,
	})
}

func joins(calls string) (joinLines []string, resets int) {
	for _, l := range strings.Split(calls, "\n") {
		switch {
		case strings.HasPrefix(l, "kubeadm join"):
			joinLines = append(joinLines, l)
		case strings.HasPrefix(l, "kubeadm reset"):
			resets++
		}
	}
	return
}

func hasFix(line string) bool {
	return strings.Contains(line, "--ignore-preflight-errors=SystemVerification") &&
		strings.Contains(line, "--patches=/var/lib/node-bootstrap-kubeadm-patches")
}

func requireBootstrapped(t *testing.T, r e2esandbox.Result) {
	t.Helper()
	if r.ExitCode != 0 || !strings.Contains(r.Log, "Bootstrap complete") || r.File("f.complete") != "yes\n" {
		t.Fatalf("the node did not bootstrap:\n%s", r.Dump())
	}
	if u := r.File("unknown.calls"); u != "" {
		t.Errorf("the script ran commands outside the known set:\n%s", u)
	}
}

func TestE2E_GCE_FreshNode(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		for _, tc := range []struct {
			name    string
			v1      bool
			version string
			minor   string
			wantFix bool
		}{
			{"Ubuntu 22.04 (cgroups v2) + 1.35", false, "1.35.0", "35", false},
			{"Ubuntu 20.04 (cgroups v1) + 1.35", true, "1.35.0", "35", true},
			{"Ubuntu 20.04 (cgroups v1) + 1.36", true, "1.36.2", "36", true},
			{"Ubuntu 20.04 (cgroups v1) + 1.34", true, "1.34.9", "34", false},
		} {
			t.Run(fmt.Sprintf("noVPN=%v/%s", noVPN, tc.name), func(t *testing.T) {
				h := host{noVPN: noVPN, v1: tc.v1, minor: tc.minor, id: "2764011173981916909"}
				r := h.run(t, e2eScript(t, noVPN, tc.version))
				requireBootstrapped(t, r)
				j, resets := joins(r.Calls())
				if len(j) != 1 || resets != 0 {
					t.Fatalf("want one join, no reset: %d/%d\n%s", len(j), resets, r.Calls())
				}
				if hasFix(j[0]) != tc.wantFix {
					t.Errorf("cgroups v1 arguments present = %v, want %v: %s", hasFix(j[0]), tc.wantFix, j[0])
				}
				if strings.TrimSpace(r.File("f.instance")) != "2764011173981916909" {
					t.Errorf("instance record = %q (read from the GCE metadata server)", r.File("f.instance"))
				}
				wantIP := "10.128.0.7"
				if !noVPN {
					wantIP = "10.8.0.5"
				}
				if !strings.Contains(r.File("f.default-kubelet"), "--node-ip="+wantIP) {
					t.Errorf("kubelet node-ip = %q, want %s", r.File("f.default-kubelet"), wantIP)
				}
				// GCE only: the node IP comes from the GCE metadata server, never EC2's.
				for _, l := range strings.Split(r.File("curl.calls"), "\n") {
					if strings.Contains(l, "latest/api/token") || strings.Contains(l, "meta-data/") {
						t.Errorf("the GCE script must never call EC2 IMDS: %s", l)
					}
				}
			})
		}
	}
}

func TestE2E_GCE_RebootIsANoOp(t *testing.T) {
	h := host{noVPN: true, v1: true, minor: "35", id: "111", runs: 3}
	r := h.run(t, e2eScript(t, true, "1.35.0"))
	if r.Exits[0] != 0 || r.Exits[1] != 0 || r.Exits[2] != 0 || !strings.Contains(r.Logs[2], "already completed") {
		t.Fatalf("exits %v\n%s", r.Exits, r.Dump())
	}
	if j, resets := joins(r.Calls()); len(j) != 1 || resets != 0 {
		t.Errorf("reboots must not rejoin/reset: %d/%d", len(j), resets)
	}
}

// The exact scenario of the field report: a custom image made from a node that had
// already joined, booted as a NEW instance.
func nodeAState(record, vpn bool) string {
	s := `mkdir -p /etc/kubernetes /var/lib/kubelet/pki
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -keyout /var/lib/kubelet/pki/key.pem \
  -out /var/lib/kubelet/pki/kubelet-client-current.pem -subj "/O=system:nodes/CN=system:node:node-a" 2>/dev/null
printf 'server: https://10.0.0.1:6443\nusers:\n- user:\n    client-certificate: /var/lib/kubelet/pki/kubelet-client-current.pem\n' > /etc/kubernetes/kubelet.conf
echo 'KUBELET_EXTRA_ARGS=--node-ip=10.128.0.99' > /etc/default/kubelet
echo SUCCESS > /var/lib/node-bootstrap-status
touch /var/lib/node-bootstrap-complete
: > $OUT/active.kubelet
`
	if record {
		s += "echo 999 > /var/lib/node-bootstrap-instance\n"
	}
	if vpn {
		s += "printf '[Interface]\\nAddress = 10.8.0.99/24\\n' > /etc/wireguard/wg0.conf\n: > $OUT/active.wg-quick@wg0\n"
	}
	return s
}

func TestE2E_GCE_CustomImageOfAJoinedNodeIsResetAndJoinsAgain(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		for _, record := range []bool{true, false} {
			t.Run(fmt.Sprintf("noVPN=%v/instance record=%v", noVPN, record), func(t *testing.T) {
				h := host{noVPN: noVPN, v1: true, minor: "35", id: "222", setup: nodeAState(record, !noVPN)}
				r := h.run(t, e2eScript(t, noVPN, "1.35.0"))
				requireBootstrapped(t, r)
				if strings.Contains(r.Log, "already completed") || strings.Contains(r.Log, "Node is already joined") {
					t.Fatalf("the clone was mistaken for the original node (the field failure):\n%s", r.Log)
				}
				j, resets := joins(r.Calls())
				if resets < 1 || len(j) != 1 || !hasFix(j[0]) {
					t.Fatalf("want a reset then one join with the cgroups v1 fix: %d resets, joins %v", resets, j)
				}
				if strings.TrimSpace(r.File("f.instance")) != "222" {
					t.Errorf("instance record = %q", r.File("f.instance"))
				}
				if got := r.File("f.default-kubelet"); strings.Contains(got, "10.128.0.99") {
					t.Errorf("the old node-ip must be replaced: %q", got)
				}
				if r.File("f.pki") != "" {
					t.Error("the old node's kubelet certificate must be gone")
				}
				if !noVPN && strings.Contains(r.File("f.wg0"), "10.8.0.99") {
					t.Errorf("the old tunnel config must be replaced: %q", r.File("f.wg0"))
				}
			})
		}
	}
}

func TestE2E_GCE_JoinRetriesKeepTheFixAndFailedBootRetriesOnNextBoot(t *testing.T) {
	h := host{noVPN: true, v1: true, minor: "35", id: "111", joinFails: 5, runs: 2}
	r := h.run(t, e2eScript(t, true, "1.35.0"))
	if r.Exits[0] == 0 || !strings.Contains(r.Logs[0], "kubeadm join failed after 5 attempts") {
		t.Fatalf("boot 1 must fail after 5 attempts:\n%s", r.Logs[0])
	}
	if r.Exits[1] != 0 || r.File("f.complete") != "yes\n" {
		t.Fatalf("boot 2 must succeed:\n%s", r.Logs[1])
	}
	j, _ := joins(r.Calls())
	if len(j) != 6 {
		t.Fatalf("joins = %d", len(j))
	}
	for i, l := range j {
		if !hasFix(l) {
			t.Errorf("attempt %d lost the cgroups v1 arguments: %s", i+1, l)
		}
	}
}

// Same container, same script, fix removed: the real failure of the field report.
func TestE2E_GCE_WithoutTheFixTheJoinFailsLikeTheFieldReport(t *testing.T) {
	script := e2eScript(t, true, "1.35.0")
	broken := strings.Replace(script, `bash -c "$JOIN_CMD $CGROUP_V1_JOIN_ARGS --cri-socket=unix://${CRIO_SOCKET}"`,
		`bash -c "$JOIN_CMD --cri-socket=unix://${CRIO_SOCKET}"`, 1)
	if broken == script {
		t.Fatal("test setup: the join line was not found")
	}
	r := host{noVPN: true, v1: true, minor: "35", id: "111"}.run(t, broken)
	if r.ExitCode == 0 || !strings.Contains(r.Log, "cgroups v1 support is deprecated") || r.File("f.complete") != "" {
		t.Fatalf("expected the field failure:\n%s", r.Dump())
	}
}

// The GCE script and the AWS-default script differ only where intended: both carry
// the same cgroups v1 handling.
func TestE2E_GCE_AndAWSScriptsShareTheCgroupV1Handling(t *testing.T) {
	gce := e2eScript(t, true, "1.35.0")
	aws, err := awsprovision.RenderBootstrapScript(awsprovision.CloudInitParams{
		JoinCommand: testJoin, KubernetesVersion: "1.35.0", KubernetesMinorVersion: "1.35", NodeName: "worker-1", NoVPN: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range map[string]string{"gce": gce, "aws": aws} {
		for _, want := range []string{"CGROUP_V1_JOIN_ARGS", "failCgroupV1: false", "--ignore-preflight-errors=SystemVerification"} {
			if !strings.Contains(s, want) {
				t.Errorf("script missing %q", want)
			}
		}
	}
}
