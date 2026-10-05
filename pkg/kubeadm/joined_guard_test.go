package kubeadm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	"dcn.ssu.ac.kr/infra/pkg/ssh/sshtest"
)

// guardEnv is a fake node reachable over a real (in-process) SSH server. The
// stubs record every kubeadm call so a test can prove a healthy node is never
// reset or re-joined.
type guardEnv struct {
	*joinEnv
	client *sshhelper.Client
}

func newGuardEnv(t *testing.T, extraEnv ...string) *guardEnv {
	t.Helper()
	j := newJoinEnv(t)
	// Node-name resolution on the "control plane" (same fake host): kubectl is
	// only asked to list nodes, jq turns that into "<name>\t<ip>".
	j.Write("jq", "#!/bin/bash\nprintf 'worker-1\\t127.0.0.1\\n'\n")
	env := append([]string{
		"PATH=" + j.Dir + ":" + envPath(),
		"FAKE_LOG=" + j.LogDir,
		"CNLAB_KUBELET_CONF=" + j.conf,
	}, extraEnv...)
	srv := sshtest.Start(t, sshtest.Options{Env: env})
	c := &sshhelper.Client{Conn: srv.Dial(t)}
	t.Cleanup(func() { _ = c.Conn.Close() })
	return &guardEnv{joinEnv: j, client: c}
}

func (g *guardEnv) writeKubeletConf(server string) {
	if err := os.WriteFile(g.conf, []byte("server: https://"+server+"\n"), 0o600); err != nil {
		panic(err)
	}
}

func TestNodeHealthilyJoined(t *testing.T) {
	cases := []struct {
		name  string
		setup func(g *guardEnv)
		env   []string
		want  bool
	}{
		{"healthy node of this cluster", func(g *guardEnv) { g.writeKubeletConf("10.8.0.1:6443") }, nil, true},
		{"no kubelet.conf", func(g *guardEnv) {}, nil, false},
		{"kubelet.conf of another cluster", func(g *guardEnv) { g.writeKubeletConf("192.0.2.9:6443") }, nil, false},
		{"kubelet inactive", func(g *guardEnv) { g.writeKubeletConf("10.8.0.1:6443") }, []string{"FAKE_KUBELET_ACTIVE=0"}, false},
		{"api server unhealthy", func(g *guardEnv) { g.writeKubeletConf("10.8.0.1:6443") }, []string{"FAKE_API_HEALTHY=0"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newGuardEnv(t, tc.env...)
			tc.setup(g)
			got, err := nodeHealthilyJoined(g.client, testJoinCmd)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("joined = %v, want %v", got, tc.want)
			}
		})
	}
}

// A transport failure must surface as an error. Reporting "not joined" here
// would let the caller run `kubeadm reset` on a node it simply could not reach.
func TestNodeHealthilyJoined_TransportErrorIsNotAnswer(t *testing.T) {
	g := newGuardEnv(t)
	g.writeKubeletConf("10.8.0.1:6443")
	_ = g.client.Conn.Close()
	if joined, err := nodeHealthilyJoined(g.client, testJoinCmd); err == nil {
		t.Fatalf("expected an error for a dead connection, got joined=%v", joined)
	}
}

func guardTestCluster() (*infrav1.RemoteCluster, *infrav1.RemoteCluster) {
	worker := &infrav1.RemoteCluster{Spec: infrav1.RemoteClusterSpec{
		ClusterName: "c1",
		Host:        "127.0.0.1",
		DisableVPN:  true, // node IP = spec.host, no wg0 needed
		NodeInfo:    infrav1.NodeInfo{NodeType: "worker", HardwareType: "cpu"},
	}}
	cp := &infrav1.RemoteCluster{Spec: infrav1.RemoteClusterSpec{
		ClusterName: "c1",
		NodeInfo: infrav1.NodeInfo{
			NodeType: "control-plane", HardwareType: "cpu",
			SoftwareConfig: infrav1.SoftwareConfig{KubernetesVersion: "1.34.2"},
		},
	}}
	return worker, cp
}

// The bug: a provisioning run that restarts at phase 0 on an already-joined,
// healthy worker ran Cleanup's `kubeadm reset -f` and wiped it.
func TestJoinWorkerNode_HealthyJoinedNodeIsNeverReset(t *testing.T) {
	g := newGuardEnv(t)
	g.writeKubeletConf("10.8.0.1:6443")
	worker, cp := guardTestCluster()

	var completed []int
	err, nodeIP := JoinWorkerNode(g.client, g.client, worker, testJoinCmd, cp, 0,
		func(i int) { completed = append(completed, i) }, pkgruntime.Config{})
	if err != nil {
		t.Fatalf("JoinWorkerNode: %v", err)
	}
	if nodeIP != "127.0.0.1" {
		t.Errorf("node IP = %q", nodeIP)
	}
	if calls := g.Log("kubeadm.calls"); calls != "" {
		t.Fatalf("kubeadm must not be invoked on a healthy joined node, got:\n%s", calls)
	}
	if _, statErr := os.Stat(g.conf); statErr != nil {
		t.Fatalf("kubelet.conf must survive: %v", statErr)
	}
	if len(completed) != 1 || completed[0] != WorkerPhaseJoin {
		t.Errorf("progress callback = %v, want [%d]", completed, WorkerPhaseJoin)
	}
	if !strings.Contains(g.Log("kubectl.calls"), "label node") {
		t.Errorf("the post-join labelling must still run:\n%s", g.Log("kubectl.calls"))
	}
}

// Sanity check that the guard does not swallow real work: a node with no
// kubelet.conf still goes through the Cleanup phase (kubeadm reset is called).
func TestJoinWorkerNode_UnjoinedNodeStillProvisions(t *testing.T) {
	g := newGuardEnv(t)
	worker, cp := guardTestCluster()

	_, _ = JoinWorkerNode(g.client, g.client, worker, testJoinCmd, cp, 0, nil, pkgruntime.Config{})
	if !strings.Contains(g.Log("kubeadm.calls"), "kubeadm reset") {
		t.Fatalf("an un-joined node must still run the Cleanup phase; kubeadm calls:\n%q", g.Log("kubeadm.calls"))
	}
	_ = filepath.Join // keep import used if the log path helper changes
}
