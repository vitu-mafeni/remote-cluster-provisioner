package ml

import (
	"context"
	"errors"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clusterNC is a NodeProvisionNetConfig with a fresh join token (so
// requireNetConfig does not try to refresh it) for the given cluster.
func clusterNC(name, cluster string, disableVPN bool) *mlv1alpha1.NodeProvisionNetConfig {
	nc := readyNetConfig()
	nc.Name = name
	nc.Spec.ClusterName = cluster
	nc.Spec.DisableVPN = disableVPN
	return nc
}

func npForCluster(name, cluster string) *mlv1alpha1.NodeProvision {
	return newNP(name, func(np *mlv1alpha1.NodeProvision) { np.Spec.ClusterName = cluster })
}

func ncItems(ncs ...*mlv1alpha1.NodeProvisionNetConfig) []mlv1alpha1.NodeProvisionNetConfig {
	out := make([]mlv1alpha1.NodeProvisionNetConfig, 0, len(ncs))
	for _, nc := range ncs {
		out = append(out, *nc)
	}
	return out
}

func TestSelectNetConfig(t *testing.T) {
	a := clusterNC("a-netconfig", "cluster-a", false)
	b := clusterNC("b-netconfig", "cluster-b", true)
	dupB := clusterNC("b2-netconfig", "cluster-b", true)
	unnamed := clusterNC("legacy", "", false)

	cases := []struct {
		name    string
		items   []mlv1alpha1.NodeProvisionNetConfig
		cluster string
		want    string // selected NetConfig name
		wantErr error
		errHas  string
	}{
		{name: "single, no clusterName (backward compatible)", items: ncItems(a), want: "a-netconfig"},
		{name: "single without a clusterName of its own, np unnamed", items: ncItems(unnamed), want: "legacy"},
		{name: "single, matching clusterName", items: ncItems(a), cluster: "cluster-a", want: "a-netconfig"},
		{name: "single, non-matching clusterName", items: ncItems(a), cluster: "cluster-z", wantErr: errNetConfigNotFound, errHas: `"cluster-z"`},
		{name: "none, no clusterName", items: nil, wantErr: errNetConfigNotFound},
		{name: "none, clusterName set", items: nil, cluster: "cluster-a", wantErr: errNetConfigNotFound},
		{name: "two, matching name picks the right one", items: ncItems(a, b), cluster: "cluster-b", want: "b-netconfig"},
		{name: "two, matching name picks the right one (other)", items: ncItems(b, a), cluster: "cluster-a", want: "a-netconfig"},
		{name: "two, non-matching name", items: ncItems(a, b), cluster: "cluster-z", wantErr: errNetConfigNotFound, errHas: "a-netconfig"},
		{name: "two, empty clusterName is an error, never a guess", items: ncItems(a, b), wantErr: errNetConfigAmbiguous, errHas: "set spec.clusterName"},
		{name: "two claiming the same clusterName", items: ncItems(a, b, dupB), cluster: "cluster-b", wantErr: errNetConfigAmbiguous, errHas: "exactly one"},
		{name: "legacy unnamed is not matched by a named np", items: ncItems(unnamed, a), cluster: "cluster-a", want: "a-netconfig"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			np := npForCluster("n", c.cluster)
			got, err := selectNetConfig(c.items, np)
			if c.wantErr != nil {
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("want %v, got %v (nc=%v)", c.wantErr, err, got)
				}
				if c.errHas != "" && !strings.Contains(err.Error(), c.errHas) {
					t.Errorf("error %q must contain %q", err, c.errHas)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != c.want {
				t.Errorf("selected %q, want %q", got.Name, c.want)
			}
		})
	}
}

func TestRequireNetConfig_SelectsByClusterName(t *testing.T) {
	a := clusterNC("a-netconfig", "cluster-a", false)
	b := clusterNC("b-netconfig", "cluster-b", true)
	r := fixReconciler(t, a, b, npForCluster("na", "cluster-a"), npForCluster("nb", "cluster-b"), npForCluster("nc", ""))

	for cluster, want := range map[string]string{"cluster-a": "a-netconfig", "cluster-b": "b-netconfig"} {
		got, err := r.requireNetConfig(context.Background(), npForCluster("x", cluster))
		if err != nil {
			t.Fatalf("%s: %v", cluster, err)
		}
		if got.Name != want {
			t.Errorf("%s: got %q want %q", cluster, got.Name, want)
		}
	}
	if _, err := r.requireNetConfig(context.Background(), npForCluster("x", "")); !errors.Is(err, errNetConfigAmbiguous) {
		t.Errorf("two NetConfigs and no clusterName must be ambiguous, got %v", err)
	}
	if _, err := r.requireNetConfig(context.Background(), npForCluster("x", "nope")); !errors.Is(err, errNetConfigNotFound) {
		t.Errorf("unknown clusterName must be not-found, got %v", err)
	}
}

func TestRequireNetConfig_SingleNetConfigWithAndWithoutClusterName(t *testing.T) {
	r := fixReconciler(t, clusterNC("only", "cluster-a", false))
	for _, cluster := range []string{"", "cluster-a"} {
		got, err := r.requireNetConfig(context.Background(), npForCluster("x", cluster))
		if err != nil || got.Name != "only" {
			t.Errorf("clusterName=%q: got %v, %v", cluster, got, err)
		}
	}
	if _, err := r.requireNetConfig(context.Background(), npForCluster("x", "other")); !errors.Is(err, errNetConfigNotFound) {
		t.Errorf("a named np must not fall back to a different cluster's only NetConfig, got %v", err)
	}
}

// Two clusters in one namespace: the VPN-mode inheritance must follow the
// NetConfig of the node's own cluster.
func TestReconcileVPNMode_PicksTheNodesOwnClusterNetConfig(t *testing.T) {
	vpn := clusterNC("vpn-netconfig", "cluster-vpn", false)
	novpn := clusterNC("novpn-netconfig", "cluster-novpn", true)

	// Node of the VPN-less cluster inherits disableVPN=true.
	np := npForCluster("n1", "cluster-novpn")
	r := fixReconciler(t, vpn, novpn, np)
	res, done, err := r.reconcileVPNMode(context.Background(), np)
	if err != nil || !done || !res.Requeue {
		t.Fatalf("want patch+requeue, got res=%+v done=%v err=%v", res, done, err)
	}
	if !getNP(t, r, "n1").Spec.DisableVPN {
		t.Error("the node of the VPN-less cluster must inherit disableVPN=true")
	}

	// Node of the VPN cluster is left alone even though another NetConfig is VPN-less.
	np = npForCluster("n2", "cluster-vpn")
	r = fixReconciler(t, vpn, novpn, np)
	res, done, err = r.reconcileVPNMode(context.Background(), np)
	if err != nil || done || res.Requeue {
		t.Fatalf("a VPN-cluster node must be a no-op, got res=%+v done=%v err=%v", res, done, err)
	}
	if getNP(t, r, "n2").Spec.DisableVPN {
		t.Error("the node of the VPN cluster must not inherit the other cluster's disableVPN")
	}

	// Explicit disableVPN against the VPN cluster still fails, but against the VPN-less one it is fine.
	np = newNP("n3", func(np *mlv1alpha1.NodeProvision) { np.Spec.ClusterName = "cluster-novpn"; np.Spec.DisableVPN = true })
	r = fixReconciler(t, vpn, novpn, np)
	if _, done, err := r.reconcileVPNMode(context.Background(), np); err != nil || done {
		t.Fatalf("disableVPN matches its own cluster: want no-op, got done=%v err=%v", done, err)
	}
}

func TestReconcileVPNMode_AmbiguousNetConfigFailsWithoutConsumingRetry(t *testing.T) {
	np := npForCluster("n", "")
	r := fixReconciler(t, clusterNC("a-netconfig", "cluster-a", false), clusterNC("b-netconfig", "cluster-b", true), np)
	res, done, err := r.reconcileVPNMode(context.Background(), np)
	if err != nil || !done {
		t.Fatalf("want done without error, got res=%+v done=%v err=%v", res, done, err)
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Message, "spec.clusterName") {
		t.Errorf("message must tell the user to set spec.clusterName: %q", got.Status.Message)
	}
	if got.Status.ProvisionRetryCount != 0 {
		t.Errorf("a spec error must not consume the retry budget, count=%d", got.Status.ProvisionRetryCount)
	}
	if got.Spec.DisableVPN {
		t.Error("an ambiguous choice must never be inherited")
	}
}

func TestReconcileVPNMode_NamedClusterWithoutNetConfigIsNoOp(t *testing.T) {
	np := npForCluster("n", "cluster-z")
	r := fixReconciler(t, clusterNC("a-netconfig", "cluster-a", true), np)
	if _, done, err := r.reconcileVPNMode(context.Background(), np); err != nil || done {
		t.Fatalf("want no-op (requireNetConfig reports it later), got done=%v err=%v", done, err)
	}
	if getNP(t, r, "n").Spec.DisableVPN {
		t.Error("must not inherit from another cluster's NetConfig")
	}
}

func TestCleanupVPNPeer_UsesOwnClusterNetConfigAndNeverGuesses(t *testing.T) {
	dialed := 0
	setup := func(np *mlv1alpha1.NodeProvision, ncs ...*mlv1alpha1.NodeProvisionNetConfig) *NodeProvisionReconciler {
		objs := []client.Object{np}
		for _, nc := range ncs {
			objs = append(objs, nc)
		}
		r := fixReconciler(t, objs...)
		r.dialVPNServer = func(_ context.Context, nc *mlv1alpha1.NodeProvisionNetConfig) (vpnServer, error) {
			dialed++
			return nil, errors.New("dialed " + nc.Name)
		}
		return r
	}
	mk := func(cluster string) *mlv1alpha1.NodeProvision {
		np := npForCluster("n", cluster)
		np.Status.VpnIP = "10.9.0.5"
		return np
	}

	a := clusterNC("a-netconfig", "cluster-a", false)
	b := clusterNC("b-netconfig", "cluster-b", false)

	// Named: the right NetConfig's VPN server is used.
	np := mk("cluster-b")
	err := setup(np, a, b).cleanupVPNPeer(context.Background(), np)
	if err == nil || !strings.Contains(err.Error(), "b-netconfig") || strings.Contains(err.Error(), "a-netconfig") {
		t.Errorf("cleanup must use b-netconfig, got %v", err)
	}

	// Ambiguous: refuse, and never contact any VPN server.
	dialed = 0
	np = mk("")
	err = setup(np, a, b).cleanupVPNPeer(context.Background(), np)
	if !errors.Is(err, errNetConfigAmbiguous) {
		t.Errorf("want ambiguous error, got %v", err)
	}
	if dialed != 0 {
		t.Errorf("no VPN server may be contacted with an ambiguous NetConfig (dialed %d)", dialed)
	}

	// Named but no matching NetConfig: nothing to clean up, no error (as with no NetConfig at all).
	np = mk("cluster-z")
	if err := setup(np, a, b).cleanupVPNPeer(context.Background(), np); err != nil {
		t.Errorf("no matching NetConfig must skip cleanup, got %v", err)
	}
	if dialed != 0 {
		t.Errorf("dialed %d", dialed)
	}
}
