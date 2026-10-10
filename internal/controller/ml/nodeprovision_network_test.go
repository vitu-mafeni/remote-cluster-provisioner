package ml

import (
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

type verifyCall struct{ region, vpcID, subnetID, host string }

// verifyRecorder returns a reconciler whose no-VPN guard records its calls and
// returns err.
func verifyRecorder(t *testing.T, err error) (*NodeProvisionReconciler, *[]verifyCall) {
	t.Helper()
	r := fixReconcilerFuncs(t, interceptor.Funcs{})
	var calls []verifyCall
	r.awsOverrides.VerifyControlPlaneInVPC = func(_ context.Context, region string, _ awsprovision.AWSCredentials, vpcID, subnetID, host string) error {
		calls = append(calls, verifyCall{region, vpcID, subnetID, host})
		return err
	}
	return r, &calls
}

func ncWithJoin(cmd string) *mlv1alpha1.NodeProvisionNetConfig {
	nc := &mlv1alpha1.NodeProvisionNetConfig{}
	nc.Status.ClusterJoinCommand = cmd
	return nc
}

const privateJoin = "kubeadm join 10.0.4.9:6443 --token a.b --discovery-token-ca-cert-hash sha256:x"

func TestCheckNoVPNControlPlane_RunsOnlyWithoutVPN(t *testing.T) {
	ctx := context.Background()
	noVPN := func(mut func(*mlv1alpha1.NodeProvision)) *mlv1alpha1.NodeProvision {
		return awsNP("n", func(np *mlv1alpha1.NodeProvision) {
			np.Spec.DisableVPN = true
			np.Spec.AWSConfig.VPCID = "vpc-1"
			if mut != nil {
				mut(np)
			}
		})
	}
	cases := []struct {
		name  string
		np    *mlv1alpha1.NodeProvision
		join  string
		calls int
	}{
		{"no VPN, private endpoint: checked", noVPN(nil), privateJoin, 1},
		{"VPN cluster: never checked", awsNP("n", nil), privateJoin, 0},
		{"skip flag: never checked", noVPN(func(np *mlv1alpha1.NodeProvision) { np.Spec.AWSConfig.SkipControlPlaneVPCCheck = true }), privateJoin, 0},
		{"no join command yet: nothing to check", noVPN(nil), "", 0},
		{"no awsConfig: nothing to check", noVPN(func(np *mlv1alpha1.NodeProvision) { np.Spec.AWSConfig = nil }), privateJoin, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, calls := verifyRecorder(t, nil)
			if err := r.checkNoVPNControlPlane(ctx, c.np, awsprovision.AWSCredentials{}, ncWithJoin(c.join)); err != nil {
				t.Fatal(err)
			}
			if len(*calls) != c.calls {
				t.Fatalf("guard called %d times, want %d", len(*calls), c.calls)
			}
		})
	}
}

func TestCheckNoVPNControlPlane_PassesNetworkAndEndpoint(t *testing.T) {
	r, calls := verifyRecorder(t, nil)
	np := awsNP("n", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.DisableVPN = true
		np.Spec.AWSConfig.VPCID = "vpc-1"
	})
	if err := r.checkNoVPNControlPlane(context.Background(), np, awsprovision.AWSCredentials{}, ncWithJoin(privateJoin)); err != nil {
		t.Fatal(err)
	}
	want := verifyCall{"us-east-1", "vpc-1", "subnet-1", "10.0.4.9"}
	if len(*calls) != 1 || (*calls)[0] != want {
		t.Fatalf("calls = %+v, want %+v", *calls, want)
	}
}

func TestCheckNoVPNControlPlane_PropagatesTheGuardError(t *testing.T) {
	boom := errors.New("endpoint 10.0.4.9 is not inside VPC vpc-1")
	r, _ := verifyRecorder(t, boom)
	np := awsNP("n", func(np *mlv1alpha1.NodeProvision) { np.Spec.DisableVPN = true })
	if err := r.checkNoVPNControlPlane(context.Background(), np, awsprovision.AWSCredentials{}, ncWithJoin(privateJoin)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

// ── status messages never carry cloud account identifiers ──────────────────

const leakyAWSError = "operation error EC2: DescribeSubnets, https response error StatusCode: 403, RequestID: 5f1c2d3e-aaaa-bbbb-cccc-0123456789ab, " +
	"api error UnauthorizedOperation: User: arn:aws:iam::123456789012:user/alice is not authorized to perform: ec2:DescribeSubnets " +
	"Encoded authorization failure message: AbCdEf0123456789_-xyz"

func TestSanitizeStatusMessage_RedactsAWSIdentifiers(t *testing.T) {
	got := sanitizeStatusMessage("AWS validation failed: " + leakyAWSError)
	for _, banned := range []string{"123456789012", "arn:aws", "alice", "5f1c2d3e", "AbCdEf0123456789"} {
		if strings.Contains(got, banned) {
			t.Errorf("status message leaks %q: %s", banned, got)
		}
	}
	if !strings.Contains(got, "UnauthorizedOperation") || !strings.Contains(got, "AWS validation failed") {
		t.Errorf("the actionable part must survive: %s", got)
	}
}

func TestFailNodeProvision_StatusMessageHasNoAWSIdentifiers(t *testing.T) {
	np := awsNP("leak", nil)
	r := fixReconcilerFuncs(t, interceptor.Funcs{}, np)
	if _, err := r.failNodeProvision(context.Background(), np, "resolving AWS network config: "+leakyAWSError); err != nil {
		t.Fatal(err)
	}
	got := &mlv1alpha1.NodeProvision{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "leak", Namespace: np.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"123456789012", "arn:aws", "alice", "5f1c2d3e", "AbCdEf0123456789"} {
		if strings.Contains(got.Status.Message, banned) {
			t.Errorf("persisted status leaks %q: %s", banned, got.Status.Message)
		}
	}
	if got.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		t.Errorf("phase = %s", got.Status.Phase)
	}
}

// ── boot source completeness (GCP) ─────────────────────────────────────────

func TestGCPDefaultsComplete_AcceptsAnImageOrASnapshot(t *testing.T) {
	np := newNP("g", func(np *mlv1alpha1.NodeProvision) {
		np.Spec.Provider = mlv1alpha1.CloudProviderGCP
		np.Spec.Region, np.Spec.InstanceType = "us-east1", "n2-standard-4"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{ProjectID: "p", Zone: "us-east1-b", Network: "default"}
	})
	if gcpDefaultsComplete(np) {
		t.Fatal("no boot source yet: defaults are incomplete")
	}
	np.Spec.GCPConfig.SourceImage = "projects/p/global/images/x"
	if !gcpDefaultsComplete(np) {
		t.Error("an image completes the defaults")
	}
	np.Spec.GCPConfig.SourceImage, np.Spec.GCPConfig.SourceSnapshot = "", "snap"
	if !gcpDefaultsComplete(np) {
		t.Error("a snapshot completes the defaults (no image is resolved for it)")
	}
}
