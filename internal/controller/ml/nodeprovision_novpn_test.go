package ml

import (
	"context"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNodeSSHHost(t *testing.T) {
	cases := []struct {
		name string
		np   mlv1alpha1.NodeProvision
		want string
	}{
		{"vpn ip wins", mlv1alpha1.NodeProvision{
			Spec:   mlv1alpha1.NodeProvisionSpec{IPAddress: "203.0.113.5"},
			Status: mlv1alpha1.NodeProvisionStatus{VpnIP: "10.8.0.5", IPAddress: "10.8.0.5"},
		}, "10.8.0.5"},
		{"no vpn uses status ip", mlv1alpha1.NodeProvision{
			Spec:   mlv1alpha1.NodeProvisionSpec{DisableVPN: true, Hostname: "h"},
			Status: mlv1alpha1.NodeProvisionStatus{IPAddress: "172.31.1.9"},
		}, "172.31.1.9"},
		{"falls back to spec ip", mlv1alpha1.NodeProvision{
			Spec: mlv1alpha1.NodeProvisionSpec{DisableVPN: true, IPAddress: "203.0.113.5", Hostname: "h"},
		}, "203.0.113.5"},
		{"falls back to hostname", mlv1alpha1.NodeProvision{
			Spec: mlv1alpha1.NodeProvisionSpec{Hostname: "h"},
		}, "h"},
	}
	for _, c := range cases {
		if got := nodeSSHHost(&c.np); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// With VPN disabled and no allocated VPN IP, cleanup must return before it
// touches the API client (the zero-value reconciler would panic otherwise).
func TestCleanupVPNPeer_SkippedWhenVPNDisabled(t *testing.T) {
	np := &mlv1alpha1.NodeProvision{
		Spec:   mlv1alpha1.NodeProvisionSpec{DisableVPN: true},
		Status: mlv1alpha1.NodeProvisionStatus{IPAddress: "203.0.113.5"},
	}
	if err := (&NodeProvisionReconciler{}).cleanupVPNPeer(context.Background(), np); err != nil {
		t.Fatal(err)
	}
}

func TestBuildNodeResetScript(t *testing.T) {
	with := buildNodeResetScript(false)
	without := buildNodeResetScript(true)
	if !strings.Contains(with, "wg-quick down wg0") || !strings.Contains(with, "purge -y wireguard") {
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

func vpnModeReconciler(t *testing.T, objs ...client.Object) *NodeProvisionReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := mlv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &NodeProvisionReconciler{Client: c}
}

func netConfig(disable bool) *mlv1alpha1.NodeProvisionNetConfig {
	return &mlv1alpha1.NodeProvisionNetConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "c-netconfig", Namespace: "default"},
		Spec:       mlv1alpha1.NodeProvisionNetConfigSpec{DisableVPN: disable},
	}
}

func TestReconcileVPNMode_InheritsFromVPNlessCluster(t *testing.T) {
	np := &mlv1alpha1.NodeProvision{ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: "default"}}
	r := vpnModeReconciler(t, netConfig(true), np)

	res, done, err := r.reconcileVPNMode(context.Background(), np)
	if err != nil || !done || !res.Requeue {
		t.Fatalf("expected patch + immediate requeue, got res=%+v done=%v err=%v", res, done, err)
	}
	got := &mlv1alpha1.NodeProvision{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(np), got); err != nil {
		t.Fatal(err)
	}
	if !got.Spec.DisableVPN {
		t.Error("disableVPN must be persisted on the NodeProvision")
	}
}

func TestReconcileVPNMode_NoOpWhenConsistent(t *testing.T) {
	for _, mode := range []bool{false, true} {
		np := &mlv1alpha1.NodeProvision{
			ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: "default"},
			Spec:       mlv1alpha1.NodeProvisionSpec{DisableVPN: mode},
		}
		r := vpnModeReconciler(t, netConfig(mode), np)
		if _, done, err := r.reconcileVPNMode(context.Background(), np); done || err != nil {
			t.Errorf("mode %v: expected no-op, got done=%v err=%v", mode, done, err)
		}
	}
}

func TestReconcileVPNMode_NoNetConfigYet(t *testing.T) {
	np := &mlv1alpha1.NodeProvision{
		ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: "default"},
		Spec:       mlv1alpha1.NodeProvisionSpec{DisableVPN: true},
	}
	r := vpnModeReconciler(t, np)
	if _, done, err := r.reconcileVPNMode(context.Background(), np); done || err != nil {
		t.Errorf("expected no-op without a NetConfig, got done=%v err=%v", done, err)
	}
}
