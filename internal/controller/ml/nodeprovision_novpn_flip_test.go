package ml

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// vpnlessTerminatingNP is an on-prem node that was provisioned without the VPN
// (real address recorded, no VPN IP) and is being deleted.
func vpnlessTerminatingNP(specDisableVPN bool) *mlv1alpha1.NodeProvision {
	np := terminatingNP(time.Minute)
	np.Spec.DisableVPN = specDisableVPN
	np.Status.VpnIP = ""
	np.Status.IPAddress = "172.31.1.9"
	return np
}

// spec.disableVPN is mutable: a node provisioned VPN-less whose flag was edited
// back to false must still be treated as VPN-less on deletion — no VPN server
// is contacted (a VPN-less cluster has none) and host-owned WireGuard is left
// alone — in both directions of the edit.
func TestHandleDelete_VPNlessNodeIgnoresLaterSpecEdit(t *testing.T) {
	for _, specDisable := range []bool{true, false} {
		np := vpnlessTerminatingNP(specDisable)
		r := fixReconciler(t, np, netConfig(true))
		r.dialVPNServer = func(context.Context, *mlv1alpha1.NodeProvisionNetConfig) (vpnServer, error) {
			t.Errorf("specDisableVPN=%v: a VPN-less node's deletion must never contact a VPN server", specDisable)
			return nil, errors.New("unexpected VPN dial")
		}
		r.nodeReset = func(context.Context, *mlv1alpha1.NodeProvision) bool { return true }

		res, err := r.handleDelete(context.Background(), getNP(t, r, "n"))
		if err != nil {
			t.Fatalf("specDisableVPN=%v: %v", specDisable, err)
		}
		if res.RequeueAfter != 0 {
			t.Errorf("specDisableVPN=%v: deletion must complete, got requeue %v", specDisable, res.RequeueAfter)
		}
		got := &mlv1alpha1.NodeProvision{}
		if err := r.Get(context.Background(), client.ObjectKey{Name: "n", Namespace: "default"}, got); err == nil && len(got.Finalizers) > 0 {
			t.Errorf("specDisableVPN=%v: finalizer must be removed, still %v", specDisable, got.Finalizers)
		}
	}
}

func TestWireGuardProvisioned_VPNlessNodeWithFlippedSpec(t *testing.T) {
	if wireGuardProvisioned(vpnlessTerminatingNP(false)) {
		t.Error("a VPN-less node (address recorded, no VPN IP) must not get WireGuard torn down after spec.disableVPN is edited to false")
	}
	if wireGuardProvisioned(vpnlessTerminatingNP(true)) {
		t.Error("a VPN-less node with spec.disableVPN=true must not get WireGuard torn down")
	}
	// A node that really has a peer is still cleaned up, whatever the flag says.
	np := vpnlessTerminatingNP(true)
	np.Status.VpnIP, np.Status.IPAddress = "10.8.0.6", "10.8.0.6"
	if !wireGuardProvisioned(np) {
		t.Error("a node with a recorded VPN IP must get WireGuard torn down even if spec.disableVPN was edited to true")
	}
}

func TestNodeSSHHost_VPNlessNodeKeepsStatusAddressAfterSpecEdit(t *testing.T) {
	// AWS has no spec address: the private IP in status is the only way in.
	np := vpnlessTerminatingNP(false)
	np.Spec.IPAddress, np.Spec.Hostname = "", ""
	if got := nodeSSHHost(np); got != "172.31.1.9" {
		t.Errorf("got %q, want the recorded node address", got)
	}
}

// A bootstrapped VPN-less node (address recorded, no VPN IP) must advance to the
// join step even if spec.disableVPN was edited back to false meanwhile, instead
// of re-running the SSH bootstrap on a working node.
func TestPollOnPremBootstrap_VPNlessNodeAdvancesDespiteSpecEdit(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = false
		np.Status.Phase = mlv1alpha1.NodeProvisionPhaseBootstrapping
		np.Status.IPAddress = "172.31.1.9"
	})
	r := fixReconciler(t, np)
	if _, err := r.pollOnPremBootstrap(context.Background(), getNP(t, r, "n"), nil); err != nil {
		t.Fatalf("expected the join step to take over, got %v", err)
	}
	if got := getNP(t, r, "n"); got.Status.Phase != mlv1alpha1.NodeProvisionPhaseRegisteringNode {
		t.Errorf("phase = %q, want RegisteringNode (join step)", got.Status.Phase)
	}
}

// requireNetConfig only gates on the NetConfig existing and carrying a fresh join
// command: a VPN-less cluster's NetConfig has no vpnRange / VPN server and must
// be accepted as is. (A VPN cluster without vpnRange is rejected where the VPN
// is actually set up, by the AWS and on-prem provisioners.)
func TestRequireNetConfig_VPNlessWithoutVPNFields(t *testing.T) {
	nc := readyNetConfig()
	nc.Spec.DisableVPN = true // no vpnRange, no vpnServerPublicConfig
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	r := fixReconciler(t, np, nc)
	got, err := r.requireNetConfig(context.Background(), np)
	if err != nil {
		t.Fatalf("a VPN-less NetConfig must be accepted without VPN fields, got %v", err)
	}
	if !got.Spec.DisableVPN || got.Spec.VPNRange != nil {
		t.Errorf("unexpected NetConfig: %+v", got.Spec)
	}
}

// Disabling the VPN under a VPN cluster is a spec error: the NodeProvision is
// failed with the reason, but repeated attempts never exhaust the retry budget,
// so the user can still fix the spec after any amount of time.
func TestReconcileVPNMode_MismatchFailsWithoutConsumingRetries(t *testing.T) {
	np := newNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	r := fixReconciler(t, np, netConfig(false))
	for i := 0; i < maxProvisionRetries+2; i++ {
		_, done, err := r.reconcileVPNMode(context.Background(), getNP(t, r, "n"))
		if err != nil || !done {
			t.Fatalf("attempt %d: expected the node to be failed, got done=%v err=%v", i, done, err)
		}
	}
	got := getNP(t, r, "n")
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed || !strings.Contains(got.Status.Message, "runs with the VPN") {
		t.Errorf("phase=%q message=%q", got.Status.Phase, got.Status.Message)
	}
	if got.Status.ProvisionRetryCount != 0 {
		t.Errorf("a spec mismatch must not consume retries, count=%d", got.Status.ProvisionRetryCount)
	}
	if strings.Contains(got.Status.Message, "manual intervention") {
		t.Errorf("must never become terminal: %q", got.Status.Message)
	}
}
