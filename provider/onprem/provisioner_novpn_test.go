package onprem

import (
	"context"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
)

func TestNewInClusterProvisioner_DisableVPNRequiresIP(t *testing.T) {
	np := &mlv1alpha1.NodeProvision{
		Spec: mlv1alpha1.NodeProvisionSpec{DisableVPN: true, IPAddress: "not-an-ip"},
	}
	// nil SSH/VPN clients: validation must fail before either is touched.
	_, _, err := NewInClusterProvisioner(context.Background(), np, nil, nil, nil,
		&mlv1alpha1.NodeProvisionNetConfig{}, nil, pkgruntime.Config{})
	if err == nil || !strings.Contains(err.Error(), "spec.ipAddress") {
		t.Fatalf("expected ipAddress validation error, got %v", err)
	}
}

func TestNewInClusterProvisioner_VPNRequiresServerClient(t *testing.T) {
	rng := "10.8.0.0/24"
	np := &mlv1alpha1.NodeProvision{}
	nc := &mlv1alpha1.NodeProvisionNetConfig{Spec: mlv1alpha1.NodeProvisionNetConfigSpec{VPNRange: &rng}}
	_, _, err := NewInClusterProvisioner(context.Background(), np, nil, nil, nil, nc, nil, pkgruntime.Config{})
	if err == nil || !strings.Contains(err.Error(), "VPN server connection is required") {
		t.Fatalf("expected VPN server connection error, got %v", err)
	}
}

// With the VPN enabled a missing vpnRange must still fail (before any SSH or VPN
// server use), while the disabled mode never needs one (see
// TestNewInClusterProvisioner_HealthyJoinedNodeIsNotTouched).
func TestNewInClusterProvisioner_VPNRequiresRange(t *testing.T) {
	empty := ""
	for name, rng := range map[string]*string{"nil": nil, "empty": &empty} {
		np := &mlv1alpha1.NodeProvision{}
		nc := &mlv1alpha1.NodeProvisionNetConfig{Spec: mlv1alpha1.NodeProvisionNetConfigSpec{VPNRange: rng}}
		_, _, err := NewInClusterProvisioner(context.Background(), np, nil, nil, nil, nc, nil, pkgruntime.Config{})
		if err == nil || !strings.Contains(err.Error(), "no vpnRange configured") {
			t.Errorf("%s range: expected the vpnRange error, got %v", name, err)
		}
	}
}
