package aws

import (
	"context"
	"errors"
	"strings"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// fakeNet is an in-memory ec2NetworkAPI.
type fakeNet struct {
	vpcs    []types.Vpc
	subnets []types.Subnet
	sgs     []types.SecurityGroup

	routeTables []types.RouteTable
	// offerings: AZs offering any instance type; nil = describe call returns everything
	offeredAZs   []string
	offeringsErr error
	routeErr     error

	createdDefaultVPC bool
	authorized        []string // SG IDs that received ingress rules
}

func (f *fakeNet) DescribeRouteTables(_ context.Context, in *ec2.DescribeRouteTablesInput, _ ...func(*ec2.Options)) (*ec2.DescribeRouteTablesOutput, error) {
	if f.routeErr != nil {
		return nil, f.routeErr
	}
	var out []types.RouteTable
	for _, rt := range f.routeTables {
		if sf := filterValues(in.Filters, "association.subnet-id"); sf != nil {
			ok := false
			for _, a := range rt.Associations {
				ok = ok || contains(sf, awssdk.ToString(a.SubnetId))
			}
			if !ok {
				continue
			}
		}
		if hasFilter(in.Filters, "association.main", "true") {
			ok := false
			for _, a := range rt.Associations {
				ok = ok || awssdk.ToBool(a.Main)
			}
			if !ok || !contains(filterValues(in.Filters, "vpc-id"), awssdk.ToString(rt.VpcId)) {
				continue
			}
		}
		out = append(out, rt)
	}
	return &ec2.DescribeRouteTablesOutput{RouteTables: out}, nil
}

func (f *fakeNet) DescribeInstanceTypeOfferings(context.Context, *ec2.DescribeInstanceTypeOfferingsInput, ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error) {
	if f.offeringsErr != nil {
		return nil, f.offeringsErr
	}
	var o []types.InstanceTypeOffering
	for _, az := range f.offeredAZs {
		o = append(o, types.InstanceTypeOffering{Location: awssdk.String(az)})
	}
	return &ec2.DescribeInstanceTypeOfferingsOutput{InstanceTypeOfferings: o}, nil
}

func (f *fakeNet) DescribeVpcs(_ context.Context, in *ec2.DescribeVpcsInput, _ ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
	var out []types.Vpc
	for _, v := range f.vpcs {
		if len(in.VpcIds) > 0 && !contains(in.VpcIds, awssdk.ToString(v.VpcId)) {
			continue
		}
		if hasFilter(in.Filters, "isDefault", "true") && !awssdk.ToBool(v.IsDefault) {
			continue
		}
		out = append(out, v)
	}
	if len(in.VpcIds) > 0 && len(out) == 0 {
		return nil, &smithy.GenericAPIError{Code: "InvalidVpcID.NotFound"}
	}
	return &ec2.DescribeVpcsOutput{Vpcs: out}, nil
}

func (f *fakeNet) CreateDefaultVpc(context.Context, *ec2.CreateDefaultVpcInput, ...func(*ec2.Options)) (*ec2.CreateDefaultVpcOutput, error) {
	f.createdDefaultVPC = true
	v := types.Vpc{VpcId: awssdk.String("vpc-created"), IsDefault: awssdk.Bool(true), State: types.VpcStateAvailable}
	f.vpcs = append(f.vpcs, v)
	f.subnets = append(f.subnets, types.Subnet{SubnetId: awssdk.String("subnet-created"), VpcId: v.VpcId, State: types.SubnetStateAvailable, DefaultForAz: awssdk.Bool(true)})
	f.sgs = append(f.sgs, types.SecurityGroup{GroupId: awssdk.String("sg-created"), GroupName: awssdk.String("default"), VpcId: v.VpcId})
	return &ec2.CreateDefaultVpcOutput{Vpc: &v}, nil
}

func (f *fakeNet) DescribeSubnets(_ context.Context, in *ec2.DescribeSubnetsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	var out []types.Subnet
	for _, s := range f.subnets {
		if len(in.SubnetIds) > 0 && !contains(in.SubnetIds, awssdk.ToString(s.SubnetId)) {
			continue
		}
		if vf := filterValues(in.Filters, "vpc-id"); vf != nil && !contains(vf, awssdk.ToString(s.VpcId)) {
			continue
		}
		if sf := filterValues(in.Filters, "state"); sf != nil && !contains(sf, string(s.State)) {
			continue
		}
		out = append(out, s)
	}
	if len(in.SubnetIds) > 0 && len(out) == 0 {
		return nil, &smithy.GenericAPIError{Code: "InvalidSubnetID.NotFound"}
	}
	return &ec2.DescribeSubnetsOutput{Subnets: out}, nil
}

func (f *fakeNet) DescribeSecurityGroups(_ context.Context, in *ec2.DescribeSecurityGroupsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	var out []types.SecurityGroup
	vf := filterValues(in.Filters, "vpc-id")
	for _, g := range f.sgs {
		if vf != nil && !contains(vf, awssdk.ToString(g.VpcId)) {
			continue
		}
		if len(in.GroupIds) > 0 && !contains(in.GroupIds, awssdk.ToString(g.GroupId)) {
			continue
		}
		out = append(out, g)
	}
	if len(in.GroupIds) > 0 && len(out) < len(in.GroupIds) {
		return nil, &smithy.GenericAPIError{Code: "InvalidGroup.NotFound"}
	}
	return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: out}, nil
}

func (f *fakeNet) AuthorizeSecurityGroupIngress(_ context.Context, in *ec2.AuthorizeSecurityGroupIngressInput, _ ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	f.authorized = append(f.authorized, awssdk.ToString(in.GroupId))
	return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func filterValues(fs []types.Filter, name string) []string {
	for _, f := range fs {
		if awssdk.ToString(f.Name) == name {
			return f.Values
		}
	}
	return nil
}

func hasFilter(fs []types.Filter, name, val string) bool {
	return contains(filterValues(fs, name), val)
}

func vpc(id string, def bool) types.Vpc {
	cidr := "10.0.0.0/16"
	if def {
		cidr = defaultVPCCIDR
	}
	return types.Vpc{VpcId: awssdk.String(id), IsDefault: awssdk.Bool(def), State: types.VpcStateAvailable, CidrBlock: awssdk.String(cidr)}
}

func subnet(id, vpcID string, def, pub bool) types.Subnet {
	return types.Subnet{
		SubnetId: awssdk.String(id), VpcId: awssdk.String(vpcID), State: types.SubnetStateAvailable,
		AvailabilityZone: awssdk.String("us-east-1a"),
		DefaultForAz:     awssdk.Bool(def), MapPublicIpOnLaunch: awssdk.Bool(pub),
	}
}

func defaultSG(id, vpcID string) types.SecurityGroup {
	return types.SecurityGroup{GroupId: awssdk.String(id), GroupName: awssdk.String("default"), VpcId: awssdk.String(vpcID)}
}

// twoVPCs: a default VPC and a custom one, each with its own default SG.
func twoVPCs() *fakeNet {
	return &fakeNet{
		vpcs: []types.Vpc{vpc("vpc-default", true), vpc("vpc-custom", false)},
		subnets: []types.Subnet{
			subnet("subnet-default-a", "vpc-default", true, true),
			subnet("subnet-custom-z", "vpc-custom", false, false),
			subnet("subnet-custom-b", "vpc-custom", false, true),
			subnet("subnet-custom-a", "vpc-custom", false, true),
		},
		sgs: []types.SecurityGroup{defaultSG("sg-default", "vpc-default"), defaultSG("sg-custom", "vpc-custom")},
	}
}

func resolve(t *testing.T, f ec2NetworkAPI, req NetworkRequest) (*NetworkConfig, error) {
	t.Helper()
	return resolveNetworkConfig(context.Background(), f, "us-east-1", req)
}

func mustResolve(t *testing.T, f ec2NetworkAPI, req NetworkRequest) *NetworkConfig {
	t.Helper()
	got, err := resolve(t, f, req)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func wantErr(t *testing.T, err error, sub string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), sub) {
		t.Fatalf("err = %v, want it to contain %q", err, sub)
	}
}

func eq(t *testing.T, got *NetworkConfig, vpcID, subnetID string, sgs ...string) {
	t.Helper()
	if got.VPCID != vpcID || got.SubnetID != subnetID || strings.Join(got.SecurityGroupIDs, ",") != strings.Join(sgs, ",") {
		t.Fatalf("got %+v, want vpc=%s subnet=%s sgs=%v", *got, vpcID, subnetID, sgs)
	}
}

func TestResolveNetwork_NothingSetUsesDefaultVPC(t *testing.T) {
	f := twoVPCs()
	eq(t, mustResolve(t, f, NetworkRequest{}), "vpc-default", "subnet-default-a", "sg-default")
	if f.createdDefaultVPC {
		t.Fatal("must not create a default VPC when one exists")
	}
}

func TestResolveNetwork_NoDefaultVPCIsCreated(t *testing.T) {
	f := &fakeNet{}
	got := mustResolve(t, f, NetworkRequest{})
	if !f.createdDefaultVPC {
		t.Fatal("default VPC should be created")
	}
	eq(t, got, "vpc-created", "subnet-created", "sg-created")
}

func TestResolveNetwork_SubnetDerivesVPCAndSecurityGroup(t *testing.T) {
	eq(t, mustResolve(t, twoVPCs(), NetworkRequest{SubnetID: "subnet-custom-z"}), "vpc-custom", "subnet-custom-z", "sg-custom")
}

func TestResolveNetwork_SubnetAndMatchingVPC(t *testing.T) {
	eq(t, mustResolve(t, twoVPCs(), NetworkRequest{VPCID: "vpc-custom", SubnetID: "subnet-custom-b"}), "vpc-custom", "subnet-custom-b", "sg-custom")
}

func TestResolveNetwork_SubnetVPCMismatch(t *testing.T) {
	_, err := resolve(t, twoVPCs(), NetworkRequest{VPCID: "vpc-default", SubnetID: "subnet-custom-b"})
	wantErr(t, err, "belongs to VPC vpc-custom")
}

func TestResolveNetwork_VPCOnlyPicksPublicSubnetDeterministically(t *testing.T) {
	eq(t, mustResolve(t, twoVPCs(), NetworkRequest{VPCID: "vpc-custom"}), "vpc-custom", "subnet-custom-a", "sg-custom")
}

func TestResolveNetwork_UserVPCNeverCreatesDefaultVPC(t *testing.T) {
	f := &fakeNet{vpcs: []types.Vpc{vpc("vpc-custom", false)}}
	_, err := resolve(t, f, NetworkRequest{VPCID: "vpc-custom"})
	wantErr(t, err, "no available subnets")
	if f.createdDefaultVPC {
		t.Fatal("a user-specified VPC must never trigger default VPC creation")
	}
}

func TestResolveNetwork_UnknownVPCSubnetAndSG(t *testing.T) {
	f := twoVPCs()
	_, err := resolveNetworkConfig(context.Background(), f, "eu-west-1", NetworkRequest{VPCID: "vpc-nope"})
	wantErr(t, err, "vpc-nope not found in region eu-west-1")
	_, err = resolveNetworkConfig(context.Background(), f, "eu-west-1", NetworkRequest{SubnetID: "subnet-nope"})
	wantErr(t, err, "subnet-nope not found in region eu-west-1")
	_, err = resolveNetworkConfig(context.Background(), f, "eu-west-1", NetworkRequest{SecurityGroupIDs: []string{"sg-nope"}})
	wantErr(t, err, "not all found in region eu-west-1")
	if f.createdDefaultVPC {
		t.Fatal("must not create a default VPC")
	}
}

func TestResolveNetwork_UnavailableSubnetAndVPC(t *testing.T) {
	f := twoVPCs()
	f.subnets[1].State = types.SubnetStatePending
	_, err := resolve(t, f, NetworkRequest{SubnetID: "subnet-custom-z"})
	wantErr(t, err, "not available")
	f.vpcs[1].State = types.VpcStatePending
	_, err = resolve(t, f, NetworkRequest{VPCID: "vpc-custom"})
	wantErr(t, err, "not available")
}

func TestResolveNetwork_OtherAPIErrorsAreWrapped(t *testing.T) {
	boom := errors.New("throttled")
	f := &errNet{fakeNet: twoVPCs(), err: boom}
	if _, err := resolve(t, f, NetworkRequest{SubnetID: "subnet-custom-z"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

// errNet fails every DescribeSubnets call.
type errNet struct {
	*fakeNet
	err error
}

func (e *errNet) DescribeSubnets(context.Context, *ec2.DescribeSubnetsInput, ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	return nil, e.err
}

// ── user security groups ────────────────────────────────────────────────────

func TestResolveNetwork_UserSGsAreKeptAndNeverModified(t *testing.T) {
	f := twoVPCs()
	f.sgs = append(f.sgs, types.SecurityGroup{GroupId: awssdk.String("sg-mine"), GroupName: awssdk.String("mine"), VpcId: awssdk.String("vpc-custom")})
	eq(t, mustResolve(t, f, NetworkRequest{SubnetID: "subnet-custom-b", SecurityGroupIDs: []string{"sg-mine", "sg-custom"}}),
		"vpc-custom", "subnet-custom-b", "sg-mine", "sg-custom")
	if len(f.authorized) != 0 {
		t.Fatalf("user security groups must not be modified: %v", f.authorized)
	}
}

func TestResolveNetwork_SGInOtherVPCIsRejected(t *testing.T) {
	_, err := resolve(t, twoVPCs(), NetworkRequest{SubnetID: "subnet-custom-b", SecurityGroupIDs: []string{"sg-default"}})
	wantErr(t, err, "security group sg-default belongs to VPC vpc-default")
}

func TestResolveNetwork_SGsAcrossVPCsRejected(t *testing.T) {
	_, err := resolve(t, twoVPCs(), NetworkRequest{SecurityGroupIDs: []string{"sg-default", "sg-custom"}})
	wantErr(t, err, "span several VPCs")
}

func TestResolveNetwork_SGOnlyDerivesVPCAndPicksSubnetThere(t *testing.T) {
	f := twoVPCs()
	eq(t, mustResolve(t, f, NetworkRequest{SecurityGroupIDs: []string{"sg-custom"}}), "vpc-custom", "subnet-custom-a", "sg-custom")
	if f.createdDefaultVPC {
		t.Fatal("must not create a default VPC")
	}
}

// ── security-group rule management ─────────────────────────────────────────

func TestResolveNetwork_DefaultVPCSecurityGroupRulesAreManaged(t *testing.T) {
	f := twoVPCs()
	mustResolve(t, f, NetworkRequest{IncludeWireGuard: true})
	if len(f.authorized) != 1 || f.authorized[0] != "sg-default" {
		t.Fatalf("controller-chosen default network should get its rules ensured: %v", f.authorized)
	}
}

func TestResolveNetwork_UserNetworkDefaultSGIsLeftUntouched(t *testing.T) {
	for _, req := range []NetworkRequest{{VPCID: "vpc-custom"}, {SubnetID: "subnet-custom-b"}, {VPCID: "vpc-default"}, {SubnetID: "subnet-default-a"}} {
		f := twoVPCs()
		req.IncludeWireGuard = true
		mustResolve(t, f, req)
		if len(f.authorized) != 0 {
			t.Fatalf("%+v: a user-named VPC/subnet's security group must not be modified: %v", req, f.authorized)
		}
	}
}

// ── instance type vs AZ ─────────────────────────────────────────────────────

func multiAZ() *fakeNet {
	f := twoVPCs()
	f.subnets = []types.Subnet{
		subnet("subnet-a", "vpc-custom", false, true),
		subnet("subnet-b", "vpc-custom", false, true),
	}
	f.subnets[0].AvailabilityZone = awssdk.String("us-east-1a")
	f.subnets[1].AvailabilityZone = awssdk.String("us-east-1b")
	return f
}

func TestResolveNetwork_AutoPickSkipsAZsWithoutInstanceType(t *testing.T) {
	f := multiAZ()
	f.offeredAZs = []string{"us-east-1b"}
	got := mustResolve(t, f, NetworkRequest{VPCID: "vpc-custom", InstanceType: "p3.2xlarge"})
	eq(t, got, "vpc-custom", "subnet-b", "sg-custom")
	if got.AvailabilityZone != "us-east-1b" {
		t.Fatalf("az = %s", got.AvailabilityZone)
	}
}

func TestResolveNetwork_UserSubnetInAZWithoutInstanceTypeIsRejected(t *testing.T) {
	f := multiAZ()
	f.offeredAZs = []string{"us-east-1b"}
	_, err := resolve(t, f, NetworkRequest{SubnetID: "subnet-a", InstanceType: "p3.2xlarge"})
	wantErr(t, err, "not offered in us-east-1a")
}

func TestResolveNetwork_NoSubnetOffersInstanceType(t *testing.T) {
	f := multiAZ()
	f.offeredAZs = []string{"us-east-1c"}
	_, err := resolve(t, f, NetworkRequest{VPCID: "vpc-custom", InstanceType: "p3.2xlarge"})
	wantErr(t, err, "not offered in the availability zone of any subnet")
}

func TestResolveNetwork_OfferingsLookupFailureIsNotFatal(t *testing.T) {
	f := multiAZ()
	f.offeringsErr = errors.New("UnauthorizedOperation")
	eq(t, mustResolve(t, f, NetworkRequest{VPCID: "vpc-custom", InstanceType: "p3.2xlarge"}), "vpc-custom", "subnet-a", "sg-custom")
}

// ── egress warnings ─────────────────────────────────────────────────────────

func rt(vpcID, subnetID string, main bool, routes ...types.Route) types.RouteTable {
	a := types.RouteTableAssociation{Main: awssdk.Bool(main)}
	if subnetID != "" {
		a.SubnetId = awssdk.String(subnetID)
	}
	return types.RouteTable{VpcId: awssdk.String(vpcID), Associations: []types.RouteTableAssociation{a}, Routes: routes}
}

func defRoute(target string) types.Route {
	r := types.Route{DestinationCidrBlock: awssdk.String("0.0.0.0/0"), State: types.RouteStateActive}
	if strings.HasPrefix(target, "nat-") {
		r.NatGatewayId = awssdk.String(target)
	} else {
		r.GatewayId = awssdk.String(target)
	}
	return r
}

func TestResolveNetwork_EgressWarnings(t *testing.T) {
	cases := []struct {
		name    string
		rts     []types.RouteTable
		disable bool
		warn    string // substring; "" = none
	}{
		{"public via igw", []types.RouteTable{rt("vpc-custom", "subnet-custom-b", false, defRoute("igw-1"))}, false, ""},
		{"private via nat", []types.RouteTable{rt("vpc-custom", "subnet-custom-b", false, defRoute("nat-1"))}, true, ""},
		{"no default route", []types.RouteTable{rt("vpc-custom", "subnet-custom-b", false)}, false, "no default route"},
		{"igw but no public ip", []types.RouteTable{rt("vpc-custom", "subnet-custom-b", false, defRoute("igw-1"))}, true, "no public IP"},
		{"main table fallback", []types.RouteTable{rt("vpc-custom", "", true, defRoute("igw-1"))}, false, ""},
		{"main table fallback, none", []types.RouteTable{rt("vpc-custom", "", true)}, false, "no default route"},
		{"blackhole route", []types.RouteTable{rt("vpc-custom", "subnet-custom-b", false,
			types.Route{DestinationCidrBlock: awssdk.String("0.0.0.0/0"), GatewayId: awssdk.String("igw-1"), State: types.RouteStateBlackhole})}, false, "no default route"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := twoVPCs()
			f.routeTables = c.rts
			got := mustResolve(t, f, NetworkRequest{SubnetID: "subnet-custom-b", DisablePublicIP: c.disable})
			if c.warn == "" && len(got.Warnings) != 0 {
				t.Fatalf("unexpected warnings %v", got.Warnings)
			}
			if c.warn != "" && (len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], c.warn)) {
				t.Fatalf("warnings = %v, want one containing %q", got.Warnings, c.warn)
			}
		})
	}
}

func TestResolveNetwork_RouteTableLookupFailureIsNotFatal(t *testing.T) {
	f := twoVPCs()
	f.routeErr = errors.New("UnauthorizedOperation")
	got := mustResolve(t, f, NetworkRequest{SubnetID: "subnet-custom-b"})
	if len(got.Warnings) != 0 {
		t.Fatalf("warnings = %v", got.Warnings)
	}
}

func TestUserSpecified(t *testing.T) {
	if (NetworkRequest{}).UserSpecified() {
		t.Fatal("empty request is not user-specified")
	}
	for _, r := range []NetworkRequest{{VPCID: "v"}, {SubnetID: "s"}, {SecurityGroupIDs: []string{"g"}}} {
		if !r.UserSpecified() {
			t.Fatalf("%+v should be user-specified", r)
		}
	}
}

func TestBuildRunInstancesInput_PublicIPFollowsDisablePublicIP(t *testing.T) {
	for _, disable := range []bool{false, true} {
		np := &mlv1alpha1.NodeProvision{}
		np.Spec.InstanceType = "t3.micro"
		np.Spec.AWSConfig = &mlv1alpha1.AWSConfig{AMI: "ami-1", SubnetID: "subnet-1", SecurityGroupIDs: []string{"sg-1", "sg-2"}, DisablePublicIP: disable}
		nic := buildRunInstancesInput(np, "").NetworkInterfaces[0]
		if got := awssdk.ToBool(nic.AssociatePublicIpAddress); got == disable {
			t.Errorf("disablePublicIp=%v: AssociatePublicIpAddress=%v", disable, got)
		}
		if awssdk.ToString(nic.SubnetId) != "subnet-1" || len(nic.Groups) != 2 {
			t.Errorf("nic = %+v", nic)
		}
	}
}

// ── no-VPN control-plane guard ──────────────────────────────────────────────

func TestControlPlaneEndpointHost(t *testing.T) {
	for in, want := range map[string]string{
		"kubeadm join 10.0.3.7:6443 --token a.b --discovery-token-ca-cert-hash sha256:x": "10.0.3.7",
		"sudo kubeadm join cp.example.com:6443 --token a.b":                              "cp.example.com",
		"kubeadm join [fd00::1]:6443 --token a.b":                                        "fd00::1",
		"kubeadm join 10.0.3.7 --token a.b":                                              "10.0.3.7",
		"":                                                                               "",
		"kubeadm reset":                                                                  "",
		"kubeadm join":                                                                   "",
	} {
		if got := ControlPlaneEndpointHost(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestResolveNetwork_ControlPlaneGuard(t *testing.T) {
	cases := []struct {
		name string
		host string
		mut  func(*fakeNet)
		want string // error substring, "" = ok
	}{
		{"private IP inside VPC", "10.0.4.9", nil, ""},
		{"private IP outside VPC", "192.168.1.5", nil, "is not inside VPC vpc-custom (CIDR 10.0.0.0/16)"},
		{"public IP is not judged", "203.0.113.10", nil, ""},
		{"hostname is not judged", "cp.example.com", nil, ""},
		{"ipv6 is not judged", "fd00::1", nil, ""},
		{"no endpoint skips the check", "", nil, ""},
		{"associated secondary CIDR counts", "100.64.1.1", func(f *fakeNet) {
			f.vpcs[1].CidrBlockAssociationSet = []types.VpcCidrBlockAssociation{{
				CidrBlock: awssdk.String("100.64.0.0/16"), CidrBlockState: &types.VpcCidrBlockState{State: types.VpcCidrBlockStateCodeAssociated}}}
		}, ""},
		{"disassociated secondary CIDR does not", "100.64.1.1", func(f *fakeNet) {
			f.vpcs[1].CidrBlockAssociationSet = []types.VpcCidrBlockAssociation{{
				CidrBlock: awssdk.String("100.64.0.0/16"), CidrBlockState: &types.VpcCidrBlockState{State: types.VpcCidrBlockStateCodeDisassociated}}}
		}, "is not inside VPC vpc-custom"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := twoVPCs()
			if c.mut != nil {
				c.mut(f)
			}
			_, err := resolve(t, f, NetworkRequest{SubnetID: "subnet-custom-b", ControlPlaneHost: c.host})
			if c.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantErr(t, err, c.want)
			wantErr(t, err, "skipControlPlaneVpcCheck") // the way out is in the message
		})
	}
}

func TestResolveNetwork_ControlPlaneGuardSkippedWhenVPCUnreadable(t *testing.T) {
	f := &vpcErrNet{fakeNet: twoVPCs()}
	if _, err := resolve(t, f, NetworkRequest{SubnetID: "subnet-custom-b", ControlPlaneHost: "192.168.1.5"}); err != nil {
		t.Fatalf("a guard that cannot read the VPC must not block provisioning: %v", err)
	}
}

// vpcErrNet fails DescribeVpcs by ID (the guard's lookup) only.
type vpcErrNet struct{ *fakeNet }

func (v *vpcErrNet) DescribeVpcs(ctx context.Context, in *ec2.DescribeVpcsInput, o ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
	if len(in.VpcIds) > 0 {
		return nil, errors.New("UnauthorizedOperation")
	}
	return v.fakeNet.DescribeVpcs(ctx, in, o...)
}

// ── failed resolutions leave nothing behind ─────────────────────────────────

func TestResolveNetwork_FailuresNeverModifyOrCreate(t *testing.T) {
	cases := []struct {
		name string
		f    func() *fakeNet
		req  NetworkRequest
		want string
	}{
		{"control plane outside default VPC (zero config)", twoVPCs,
			NetworkRequest{ControlPlaneHost: "10.99.0.5", IncludeWireGuard: true}, "is not inside VPC vpc-default"},
		{"instance type nowhere in the VPC (zero config)", func() *fakeNet { f := twoVPCs(); f.offeredAZs = []string{"us-east-1z"}; return f },
			NetworkRequest{InstanceType: "p3.2xlarge", IncludeWireGuard: true}, "not offered"},
		{"no subnets in default VPC", func() *fakeNet { f := twoVPCs(); f.subnets = nil; return f },
			NetworkRequest{IncludeWireGuard: true}, "no available subnets"},
		{"security group in another VPC", twoVPCs,
			NetworkRequest{SubnetID: "subnet-custom-b", SecurityGroupIDs: []string{"sg-default"}}, "belongs to VPC vpc-default"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := c.f()
			_, err := resolve(t, f, c.req)
			wantErr(t, err, c.want)
			if len(f.authorized) != 0 {
				t.Errorf("security group rules were modified on a failed resolution: %v", f.authorized)
			}
			if f.createdDefaultVPC {
				t.Error("a default VPC was created on a failed resolution")
			}
		})
	}
}

func TestResolveNetwork_ControlPlaneMismatchDoesNotCreateDefaultVPC(t *testing.T) {
	f := &fakeNet{} // region has no default VPC yet
	_, err := resolve(t, f, NetworkRequest{ControlPlaneHost: "10.99.0.5"})
	wantErr(t, err, "new default VPC")
	if f.createdDefaultVPC || len(f.vpcs) != 0 {
		t.Fatal("the default VPC must not be created when the control plane cannot be inside it")
	}
	// ...but is created when the control plane is in the default VPC's CIDR.
	f = &fakeNet{}
	if _, err := resolve(t, f, NetworkRequest{ControlPlaneHost: "172.31.4.4"}); err != nil || !f.createdDefaultVPC {
		t.Fatalf("created=%v err=%v", f.createdDefaultVPC, err)
	}
}

// ── error text never carries account identifiers ───────────────────────────

const leakyMsg = "You are not authorized to perform this operation. User: arn:aws:iam::123456789012:user/alice " +
	"is not authorized to perform: ec2:DescribeSubnets on resource: arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1 " +
	"because no identity-based policy allows it. Encoded authorization failure message: AbCdEf0123456789_-xyz"

func TestRedactIdentifiers(t *testing.T) {
	for in, banned := range map[string][]string{
		leakyMsg: {"123456789012", "arn:aws", "alice", "AbCdEf0123456789"},
		"operation error EC2: DescribeSubnets, https response error StatusCode: 403, RequestID: 5f1c2d3e-aaaa-bbbb-cccc-0123456789ab, api error": {"5f1c2d3e"},
		"Request ID 0123456789ABCDEF failed":         {"0123456789ABCDEF"},
		"denied for account 123456789012":            {"123456789012"},
		"account-id=123456789012":                    {"123456789012"},
		"key AKIAABCDEFGHIJKLMNOP rejected":          {"AKIAABCDEFGHIJKLMNOP"},
		"role arn:aws-cn:iam::123456789012:role/x/y": {"123456789012", "role/x"},
	} {
		if got := RedactIdentifiers(in); got == in || func() bool {
			for _, b := range banned {
				if strings.Contains(got, b) {
					return true
				}
			}
			return false
		}() {
			t.Errorf("not redacted: %q -> %q", in, got)
		}
	}
	// Ordinary text, IDs of resources and 12-digit non-account numbers are kept.
	for _, keep := range []string{
		"subnet subnet-0123456789abcdef0 not found in region us-east-1",
		"vpc-0123456789abcdef0 is not available",
		"started at 202610102333 and ended", // bare 12 digits, no account context
		"request failed: connection refused",
	} {
		if got := RedactIdentifiers(keep); got != keep {
			t.Errorf("over-redacted: %q -> %q", keep, got)
		}
	}
}

func TestResolveNetwork_APIErrorsAreSanitizedButClassifiable(t *testing.T) {
	denied := &smithy.OperationError{
		ServiceID: "EC2", OperationName: "DescribeSubnets",
		Err: &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: leakyMsg},
	}
	for name, run := range map[string]func() error{
		"subnet": func() error {
			_, err := resolve(t, &errNet{fakeNet: twoVPCs(), err: denied}, NetworkRequest{SubnetID: "subnet-custom-z"})
			return err
		},
		"vpc subnets": func() error {
			_, err := resolve(t, &errNet{fakeNet: twoVPCs(), err: denied}, NetworkRequest{VPCID: "vpc-custom"})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run()
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, banned := range []string{"123456789012", "arn:aws", "alice", "AbCdEf0123456789", "OperationError", "operation error", "https response"} {
				if strings.Contains(err.Error(), banned) {
					t.Errorf("error leaks %q: %s", banned, err)
				}
			}
			if !strings.Contains(err.Error(), "UnauthorizedOperation") {
				t.Errorf("the API error code should survive: %s", err)
			}
			var api smithy.APIError
			if !errors.As(err, &api) || api.ErrorCode() != "UnauthorizedOperation" {
				t.Errorf("the cause must stay reachable for classification: %v", err)
			}
		})
	}
}

func TestDescribeAPIError_NonAPIErrors(t *testing.T) {
	if got := describeAPIError(errors.New("dial tcp: arn:aws:iam::123456789012:role/x timed out")); strings.Contains(got, "123456789012") {
		t.Errorf("leak: %s", got)
	}
	if describeAPIError(nil) != "" {
		t.Error("nil -> empty")
	}
	if got := describeAPIError(&smithy.GenericAPIError{Code: "Throttling"}); got != "Throttling" {
		t.Errorf("code only: %q", got)
	}
}
