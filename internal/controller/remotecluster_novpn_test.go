package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBuildNodeResetScript(t *testing.T) {
	with := buildNodeResetScript(false, false)
	without := buildNodeResetScript(true, false)

	if !strings.Contains(with, "wg-quick down wg0") || !strings.Contains(with, "apt-get purge -y wireguard") {
		t.Error("VPN reset script must tear WireGuard down")
	}
	if strings.Contains(without, "wg-quick") || strings.Contains(without, "purge -y wireguard") {
		t.Error("no-VPN reset script must leave WireGuard alone")
	}
	for name, s := range map[string]string{"with": with, "without": without} {
		if !strings.Contains(s, "kubeadm reset --force") {
			t.Errorf("%s: missing kubeadm reset", name)
		}
		if !strings.HasSuffix(strings.TrimSpace(s), `echo "node reset complete"`) {
			t.Errorf("%s: script must end with the completion marker", name)
		}
	}
}

func TestDesiredNetConfigFields_VPNDisabledOmitsVPN(t *testing.T) {
	cp := &infrav1.RemoteCluster{Spec: infrav1.RemoteClusterSpec{
		ClusterName: "c1",
		DisableVPN:  true,
		VPNConfig: infrav1.VPNConfig{
			IP:                "10.8.0.2",
			VPNServerPublicIP: "203.0.113.9",
		},
	}}
	spec := desiredNodeProvisionNetConfigFields(cp)
	if spec.VPNRange != nil || spec.VPNServerPublicConfig.PublicIP != "" {
		t.Errorf("VPN fields must be empty when the VPN is disabled: %+v", spec)
	}
	if spec.ClusterName != "c1" {
		t.Errorf("clusterName lost: %q", spec.ClusterName)
	}
	if !spec.DisableVPN {
		t.Error("the cluster's VPN mode must be published in the NetConfig")
	}

	cp.Spec.DisableVPN = false
	spec = desiredNodeProvisionNetConfigFields(cp)
	if spec.VPNRange == nil || *spec.VPNRange != "10.8.0.0/24" || spec.VPNServerPublicConfig.PublicIP != "203.0.113.9" {
		t.Errorf("VPN fields must be populated when the VPN is enabled: %+v", spec)
	}
}

func rc(name, nodeType string, disable bool) *infrav1.RemoteCluster {
	return &infrav1.RemoteCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: infrav1.RemoteClusterSpec{
			ClusterName: "c1",
			DisableVPN:  disable,
			NodeInfo:    infrav1.NodeInfo{NodeType: nodeType},
		},
	}
}

func TestReconcileVPNMode_WorkerInheritsFromControlPlane(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := infrav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cp, worker := rc("cp", "control-plane", true), rc("w", "worker", false)
	r := &RemoteClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp, worker).Build()}

	res, done, err := r.reconcileVPNMode(context.Background(), worker)
	if err != nil || !done || !res.Requeue {
		t.Fatalf("expected update + immediate requeue, got res=%+v done=%v err=%v", res, done, err)
	}
	got := &infrav1.RemoteCluster{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(worker), got); err != nil {
		t.Fatal(err)
	}
	if !got.Spec.DisableVPN {
		t.Error("worker must persist the control-plane's disableVPN")
	}
}

func TestReconcileVPNMode_ControlPlaneAndConsistentWorkersUntouched(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := infrav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cases := []*infrav1.RemoteCluster{
		rc("cp", "control-plane", true),      // control-plane never follows anyone
		rc("w-vpn", "worker", false),         // VPN cluster, VPN worker
		rc("w-novpn", "worker", true),        // VPN-less cluster, VPN-less worker
		rc("w-orphan-novpn", "worker", true), // no control-plane found is a no-op too
	}
	for _, node := range cases {
		node := node
		objs := []client.Object{node}
		switch node.Name {
		case "w-vpn":
			objs = append(objs, rc("cp", "control-plane", false))
		case "w-novpn":
			objs = append(objs, rc("cp", "control-plane", true))
		}
		r := &RemoteClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()}
		key := client.ObjectKeyFromObject(node)
		before := &infrav1.RemoteCluster{}
		if err := r.Get(context.Background(), key, before); err != nil {
			t.Fatal(err)
		}
		if _, done, err := r.reconcileVPNMode(context.Background(), before.DeepCopy()); done || err != nil {
			t.Errorf("%s: expected no-op, got done=%v err=%v", node.Name, done, err)
		}
		// "Untouched" means the stored object is byte-for-byte what it was.
		after := &infrav1.RemoteCluster{}
		if err := r.Get(context.Background(), key, after); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Errorf("%s: reconcileVPNMode changed the stored object:\nbefore %+v\nafter  %+v", node.Name, before, after)
		}
	}
}

func TestBuildNodeResetScript_ControlPlaneKillsInitFirst(t *testing.T) {
	cp := buildNodeResetScript(true, true)
	kill, reset := strings.Index(cp, "pkill -f '[k]ubeadm init'"), strings.Index(cp, "kubeadm reset --force")
	if kill < 0 || reset < 0 || kill > reset {
		t.Errorf("control-plane reset must kill a running kubeadm init BEFORE kubeadm reset (kill=%d reset=%d)", kill, reset)
	}
	if strings.Contains(buildNodeResetScript(true, false), "pkill") {
		t.Error("worker reset must not kill kubeadm processes")
	}
	if !strings.Contains(buildNodeResetScript(false, true), "wg-quick down wg0") {
		t.Error("control-plane VPN reset must still tear WireGuard down")
	}
}

// netConfigPatch pushes the NetConfig patch for cp to a fake SSH server and
// returns the decoded merge-patch spec it sent.
func netConfigPatch(t *testing.T, cp *infrav1.RemoteCluster) map[string]any {
	t.Helper()
	srv := startFakeSSH(t, okHandler)
	port, err := strconv.Atoi(srv.port())
	if err != nil {
		t.Fatal(err)
	}
	c, err := sshhelper.Connect("127.0.0.1", port, "u", "pw")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Conn.Close() //nolint:errcheck
	if err := pushNetConfigViaSSH(c, cp); err != nil {
		t.Fatal(err)
	}
	script := srv.find("kubectl patch nodeprovisionnetconfig")
	if script == "" {
		t.Fatal("no NetConfig patch was sent")
	}
	i := strings.Index(script, "-p '")
	if i < 0 {
		t.Fatalf("no -p body in %q", script)
	}
	body := strings.TrimSpace(script[i+len("-p '"):])
	body = strings.TrimSuffix(body, "'")
	var patch struct {
		Spec map[string]any `json:"spec"`
	}
	if err := json.Unmarshal([]byte(body), &patch); err != nil {
		t.Fatalf("patch body is not JSON: %v\n%s", err, body)
	}
	return patch.Spec
}

// The remote (SSH) sync is a JSON merge patch, which leaves keys it omits
// alone: switching a cluster from the VPN to no VPN must explicitly delete the
// stale vpnRange / vpnServerPublicConfig, and must carry disableVPN.
func TestPushNetConfigViaSSH_VPNDisabledClearsStaleVPNFields(t *testing.T) {
	cp := &infrav1.RemoteCluster{Spec: infrav1.RemoteClusterSpec{
		ClusterName: "c1",
		DisableVPN:  true,
		VPNConfig:   infrav1.VPNConfig{IP: "10.8.0.2", VPNServerPublicIP: "203.0.113.9"},
	}}
	cp.Namespace = "default"
	spec := netConfigPatch(t, cp)
	if spec["disableVPN"] != true {
		t.Errorf("disableVPN must be carried, got %v", spec["disableVPN"])
	}
	for _, k := range []string{"vpnRange", "vpnServerPublicConfig"} {
		v, present := spec[k]
		if !present || v != nil {
			t.Errorf("%s must be sent as an explicit null to clear it remotely, got present=%v value=%v", k, present, v)
		}
	}
}

func TestPushNetConfigViaSSH_VPNEnabledCarriesVPNFields(t *testing.T) {
	cp := &infrav1.RemoteCluster{Spec: infrav1.RemoteClusterSpec{
		ClusterName: "c1",
		VPNConfig:   infrav1.VPNConfig{IP: "10.8.0.2", VPNServerPublicIP: "203.0.113.9"},
	}}
	cp.Namespace = "default"
	spec := netConfigPatch(t, cp)
	if spec["disableVPN"] != false {
		t.Errorf("disableVPN=false must be serialized so the mode can be switched back, got %v", spec["disableVPN"])
	}
	if spec["vpnRange"] != "10.8.0.0/24" {
		t.Errorf("vpnRange = %v", spec["vpnRange"])
	}
	cfg, _ := spec["vpnServerPublicConfig"].(map[string]any)
	if cfg["publicIP"] != "203.0.113.9" {
		t.Errorf("vpnServerPublicConfig = %v", spec["vpnServerPublicConfig"])
	}
}

// Creating the remote NetConfig of a VPN-less control-plane: the node IP is the
// primary local route address (no wg0 lookup), the object carries disableVPN and no VPN
// range/server, the VPN credentials secret is not copied (even if a stale
// vpnConfig is left in the spec) and no VPN IP is recorded in usedIPAddresses.
func TestHandleCreateUpdateNodeProvisionConfig_CreateWithoutVPN(t *testing.T) {
	cpSrv := startFakeSSH(t, func(script string) (string, string, int) {
		if strings.Contains(script, "ip -4 route get 1.1.1.1") {
			return "1.1.1.1 dev eth0 src 192.0.2.10", "", 0
		}
		return "", "", 0
	})
	cp := withSSH(novpnNode("cp", "control-plane"), cpSrv)
	cp.Spec.VPNConfig = infrav1.VPNConfig{
		IP: "10.8.0.2", VPNServerPublicIP: "203.0.113.9",
		VPNSSHCredentialsRef: infrav1.VPNSSHCredentialsRef{Name: "vpn-key"},
	}
	cp.Status.JoinCommand = "kubeadm join 127.0.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:00"
	// No Secret named vpn-key exists: copying it would fail the call.
	r := newTestReconciler(t, sshSecretObj(), cp)
	cpClient, err := r.getSSHClient(context.Background(), cp)
	if err != nil {
		t.Fatal(err)
	}
	defer cpClient.Conn.Close() //nolint:errcheck

	if _, err := r.handleCreateUpdateNodeProvisionConfig(context.Background(), cp, cp, cpClient, cp.Spec.VPNConfig.IP, "create"); err != nil {
		t.Fatalf("a VPN-less control-plane must not need VPN credentials: %v", err)
	}
	if cpSrv.ran("ip -4 addr show wg0") != 0 {
		t.Error("wg0 must not be queried without a VPN")
	}
	if cpSrv.ran("ip -4 route get 1.1.1.1") != 1 {
		t.Error("the primary local route address must be discovered")
	}
	if cpSrv.ran("ip -o addr show") != 1 {
		t.Error("the primary route address must be verified as bound locally")
	}
	nc := cpSrv.find("kind: NodeProvisionNetConfig\nmetadata")
	if !strings.Contains(nc, "disableVPN: true") {
		t.Errorf("remote NetConfig must carry disableVPN:\n%s", nc)
	}
	for _, unwanted := range []string{"vpnRange", "vpnServerPublicConfig", "vpn-key"} {
		if strings.Contains(nc, unwanted) {
			t.Errorf("remote NetConfig of a VPN-less cluster must not contain %q:\n%s", unwanted, nc)
		}
	}
	if st := cpSrv.find("--subresource=status -p"); strings.Contains(st, "usedIPAddresses") || !strings.Contains(st, "clusterJoinCommand") {
		t.Errorf("status patch must carry the join command and no VPN IP: %q", st)
	}
}

// The local (Go client) NetConfig sync follows the mode in both directions on an
// existing object: VPN -> no VPN clears the range/server, and no VPN -> VPN
// restores them, each carrying disableVPN.
func TestEnsureLocalNodeProvisionNetConfig_FlipsBothWays(t *testing.T) {
	cp := node("cp", "c1", "control-plane")
	cp.Spec.VPNConfig = infrav1.VPNConfig{IP: "10.8.0.2", VPNServerPublicIP: "203.0.113.9"}
	r := newTestReconciler(t, cp)
	ctx := context.Background()
	get := func() mlv1alpha1.NodeProvisionNetConfigSpec {
		nc := &mlv1alpha1.NodeProvisionNetConfig{}
		if err := r.Get(ctx, client.ObjectKey{Name: "c1-netconfig", Namespace: "default"}, nc); err != nil {
			t.Fatal(err)
		}
		return nc.Spec
	}

	for i, disable := range []bool{false, true, false, true} {
		cp.Spec.DisableVPN = disable
		if err := r.ensureLocalNodeProvisionNetConfig(ctx, cp, cp); err != nil {
			t.Fatal(err)
		}
		spec := get()
		if spec.DisableVPN != disable {
			t.Errorf("step %d: disableVPN = %v, want %v", i, spec.DisableVPN, disable)
		}
		hasVPN := spec.VPNRange != nil || spec.VPNServerPublicConfig.PublicIP != ""
		if hasVPN == disable {
			t.Errorf("step %d (disable=%v): VPN fields present=%v: %+v", i, disable, hasVPN, spec)
		}
	}
}
