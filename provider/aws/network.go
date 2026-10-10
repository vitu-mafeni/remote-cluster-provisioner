package aws

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// ec2NetworkAPI is the slice of the EC2 client network resolution needs (an
// interface so it can be unit-tested with a fake).
type ec2NetworkAPI interface {
	DescribeVpcs(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error)
	CreateDefaultVpc(ctx context.Context, params *ec2.CreateDefaultVpcInput, optFns ...func(*ec2.Options)) (*ec2.CreateDefaultVpcOutput, error)
	DescribeSubnets(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	DescribeSecurityGroups(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error)
	AuthorizeSecurityGroupIngress(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error)
	DescribeRouteTables(ctx context.Context, params *ec2.DescribeRouteTablesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeRouteTablesOutput, error)
	DescribeInstanceTypeOfferings(ctx context.Context, params *ec2.DescribeInstanceTypeOfferingsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error)
}

// NetworkRequest is what the user asked for (spec.awsConfig.*) plus what the
// launch needs; every network field may be empty.
type NetworkRequest struct {
	VPCID            string
	SubnetID         string
	SecurityGroupIDs []string
	// InstanceType restricts the subnet to AZs that offer it (and is checked
	// against a user-specified subnet). Empty skips the check.
	InstanceType string
	// DisablePublicIP is spec.awsConfig.disablePublicIp; it only affects the
	// egress-path warnings.
	DisablePublicIP bool
	// ControlPlaneHost is the host of the control plane's API endpoint, set for
	// clusters without a VPN (all nodes must then share the control plane's VPC).
	// A private IPv4 address outside the resolved VPC is an error; hostnames and
	// public addresses cannot be judged and are skipped. Empty skips the check.
	ControlPlaneHost string
	// IncludeWireGuard opens UDP 51820 on a controller-managed default security
	// group (clusters that run without a VPN pass false).
	IncludeWireGuard bool
}

// UserSpecified reports whether the user pinned any part of the network. A
// user-pinned network is validated and honoured, never created or modified.
func (r NetworkRequest) UserSpecified() bool {
	return r.VPCID != "" || r.SubnetID != "" || len(r.SecurityGroupIDs) > 0
}

// NetworkConfig holds the resolved AWS network identifiers.
type NetworkConfig struct {
	VPCID            string
	SubnetID         string
	SecurityGroupIDs []string
	AvailabilityZone string
	// Warnings are non-fatal findings (e.g. no route to the internet).
	Warnings []string
}

// ResolveOrCreateNetworkConfig resolves the VPC, subnet and security groups an
// instance launches into, validating everything the user supplied.
//
// The VPC is taken, in order, from: the subnet, spec vpcId, the security
// groups, the region's default VPC (created if the region has none, which also
// creates default subnets and a default security group). Anything the user set
// must agree with it (subnet and security groups must belong to the VPC) and
// exist in the region. A user-specified network never triggers default-VPC
// creation and never has its security-group rules modified.
func ResolveOrCreateNetworkConfig(ctx context.Context, region string, creds AWSCredentials, req NetworkRequest) (*NetworkConfig, error) {
	client, err := newEC2Client(ctx, region, creds)
	if err != nil {
		return nil, fmt.Errorf("creating EC2 client: %w", err)
	}
	return resolveNetworkConfig(ctx, client, region, req)
}

func resolveNetworkConfig(ctx context.Context, client ec2NetworkAPI, region string, req NetworkRequest) (*NetworkConfig, error) {
	cfg := &NetworkConfig{SubnetID: req.SubnetID, VPCID: req.VPCID}

	// AZs offering the instance type (nil = unknown / not checked).
	offered := offeredZones(ctx, client, req.InstanceType)

	// ── User security groups: exist, and all in one VPC ─────────────────────
	var sgs []types.SecurityGroup
	if len(req.SecurityGroupIDs) > 0 {
		var err error
		if sgs, err = describeUserSecurityGroups(ctx, client, region, req.SecurityGroupIDs); err != nil {
			return nil, err
		}
	}

	// ── Subnet: exists, available, offers the instance type ────────────────
	var userSubnet *types.Subnet
	if req.SubnetID != "" {
		sn, err := describeUserSubnet(ctx, client, region, req.SubnetID)
		if err != nil {
			return nil, err
		}
		if offered != nil && !offered[awssdk.ToString(sn.AvailabilityZone)] {
			return nil, fmt.Errorf("instance type %s is not offered in %s, the availability zone of spec.awsConfig.subnetId %s: use a subnet in another AZ or another instance type",
				req.InstanceType, awssdk.ToString(sn.AvailabilityZone), req.SubnetID)
		}
		userSubnet = sn
		if cfg.VPCID != "" && cfg.VPCID != awssdk.ToString(sn.VpcId) {
			return nil, fmt.Errorf("spec.awsConfig.subnetId %s belongs to VPC %s, not spec.awsConfig.vpcId %s", req.SubnetID, awssdk.ToString(sn.VpcId), cfg.VPCID)
		}
		cfg.VPCID = awssdk.ToString(sn.VpcId)
	}

	// ── VPC ──────────────────────────────────────────────────────────────────
	switch {
	case userSubnet != nil:
		// VPC already known from the subnet.
	case cfg.VPCID != "":
		if err := checkUserVPC(ctx, client, region, cfg.VPCID); err != nil {
			return nil, err
		}
	case len(sgs) > 0:
		cfg.VPCID = awssdk.ToString(sgs[0].VpcId)
	default:
		var err error
		// A VPC created here would outlive a failed check, so it is verified
		// against the CIDR every default VPC has before creating one.
		beforeCreate := func() error {
			return controlPlaneInCIDRs(req.ControlPlaneHost, "the region's new default VPC", []string{defaultVPCCIDR})
		}
		if cfg.VPCID, err = ensureDefaultVPC(ctx, client, region, beforeCreate); err != nil {
			return nil, err
		}
	}
	for _, g := range sgs {
		if awssdk.ToString(g.VpcId) != cfg.VPCID {
			return nil, fmt.Errorf("security group %s belongs to VPC %s, but the instance's VPC is %s (security groups must be in the subnet's VPC)",
				awssdk.ToString(g.GroupId), awssdk.ToString(g.VpcId), cfg.VPCID)
		}
	}

	// ── Control plane must be in this VPC (no VPN) ─────────────────────────
	// Before any security-group rule is touched, so a mismatch changes nothing.
	if err := checkControlPlaneInVPC(ctx, client, cfg.VPCID, req.ControlPlaneHost); err != nil {
		return nil, err
	}

	// ── Subnet (when not given) ─────────────────────────────────────────────
	if userSubnet == nil {
		sn, err := pickSubnet(ctx, client, cfg.VPCID, req.InstanceType, offered)
		if err != nil {
			return nil, err
		}
		userSubnet = sn
		cfg.SubnetID = awssdk.ToString(sn.SubnetId)
	}
	cfg.AvailabilityZone = awssdk.ToString(userSubnet.AvailabilityZone)
	log.Printf("[INFO] Network resolution: using VPC %s, subnet %s (%s)", cfg.VPCID, cfg.SubnetID, cfg.AvailabilityZone)

	// ── Security groups ──────────────────────────────────────────────────────
	if len(req.SecurityGroupIDs) > 0 {
		cfg.SecurityGroupIDs = append([]string(nil), req.SecurityGroupIDs...)
	} else {
		// The controller only edits the default security group when it chose the
		// whole network itself; a VPC/subnet the user named is shared
		// infrastructure whose rules are the user's to manage.
		sgID, err := resolveDefaultSecurityGroup(ctx, client, cfg.VPCID, req.IncludeWireGuard, !req.UserSpecified())
		if err != nil {
			return nil, err
		}
		cfg.SecurityGroupIDs = []string{sgID}
		if req.UserSpecified() {
			log.Printf("[INFO] Network resolution: using the VPC's default security group %s unchanged; set spec.awsConfig.securityGroupIds to attach your own", sgID)
		}
	}
	log.Printf("[INFO] Network resolution: using security groups %v", cfg.SecurityGroupIDs)

	cfg.Warnings = egressWarnings(ctx, client, cfg.VPCID, cfg.SubnetID, req.DisablePublicIP)
	return cfg, nil
}

// describeUserSecurityGroups looks up user-supplied security group IDs.
func describeUserSecurityGroups(ctx context.Context, client ec2NetworkAPI, region string, ids []string) ([]types.SecurityGroup, error) {
	out, err := client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: ids})
	if err != nil {
		if hasAPIErrorCode(err, "InvalidGroup.NotFound", "InvalidGroupId.Malformed", "InvalidParameterValue") {
			return nil, apiErrorf(err, "spec.awsConfig.securityGroupIds %v not all found in region %s (check the IDs and spec.region)", ids, region)
		}
		return nil, apiErrorf(err, "describing security groups %v", ids)
	}
	found := map[string]bool{}
	for _, g := range out.SecurityGroups {
		found[awssdk.ToString(g.GroupId)] = true
	}
	for _, id := range ids {
		if !found[id] {
			return nil, fmt.Errorf("spec.awsConfig.securityGroupIds: %s not found in region %s", id, region)
		}
	}
	vpcs := map[string]bool{}
	for _, g := range out.SecurityGroups {
		vpcs[awssdk.ToString(g.VpcId)] = true
	}
	if len(vpcs) > 1 {
		return nil, fmt.Errorf("spec.awsConfig.securityGroupIds %v span several VPCs; an instance's security groups must all be in one VPC", ids)
	}
	return out.SecurityGroups, nil
}

// describeUserSubnet validates a user-supplied subnet ID.
func describeUserSubnet(ctx context.Context, client ec2NetworkAPI, region, subnetID string) (*types.Subnet, error) {
	out, err := client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: []string{subnetID}})
	if err != nil {
		if hasAPIErrorCode(err, "InvalidSubnetID.NotFound", "InvalidSubnetID.Malformed") {
			return nil, apiErrorf(err, "spec.awsConfig.subnetId %s not found in region %s (check the ID and spec.region)", subnetID, region)
		}
		return nil, apiErrorf(err, "describing subnet %s", subnetID)
	}
	if len(out.Subnets) == 0 {
		return nil, fmt.Errorf("spec.awsConfig.subnetId %s not found in region %s", subnetID, region)
	}
	sn := out.Subnets[0]
	if sn.State != types.SubnetStateAvailable {
		return nil, fmt.Errorf("subnet %s is not available (state %q)", subnetID, sn.State)
	}
	return &sn, nil
}

// checkUserVPC validates that a user-supplied VPC exists in the region.
func checkUserVPC(ctx context.Context, client ec2NetworkAPI, region, vpcID string) error {
	out, err := client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{vpcID}})
	if err != nil {
		if hasAPIErrorCode(err, "InvalidVpcID.NotFound", "InvalidVpcID.Malformed") {
			return apiErrorf(err, "spec.awsConfig.vpcId %s not found in region %s (check the ID and spec.region)", vpcID, region)
		}
		return apiErrorf(err, "describing VPC %s", vpcID)
	}
	if len(out.Vpcs) == 0 {
		return fmt.Errorf("spec.awsConfig.vpcId %s not found in region %s", vpcID, region)
	}
	if st := out.Vpcs[0].State; st != types.VpcStateAvailable {
		return fmt.Errorf("VPC %s is not available (state %q)", vpcID, st)
	}
	return nil
}

// hasAPIErrorCode reports whether err is an AWS API error with one of codes.
func hasAPIErrorCode(err error, codes ...string) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	for _, c := range codes {
		if apiErr.ErrorCode() == c {
			return true
		}
	}
	return false
}

// offeredZones returns the set of AZs that offer instanceType, or nil when the
// answer is unknown (no instance type, or the lookup failed, e.g. missing IAM
// permission: the check is best-effort and must not block provisioning).
func offeredZones(ctx context.Context, client ec2NetworkAPI, instanceType string) map[string]bool {
	if instanceType == "" {
		return nil
	}
	zones := map[string]bool{}
	in := &ec2.DescribeInstanceTypeOfferingsInput{
		LocationType: types.LocationTypeAvailabilityZone,
		Filters:      []types.Filter{{Name: awssdk.String("instance-type"), Values: []string{instanceType}}},
	}
	for {
		out, err := client.DescribeInstanceTypeOfferings(ctx, in)
		if err != nil {
			log.Printf("[WARN] Network resolution: cannot check which AZs offer %s (continuing without): %s", instanceType, describeAPIError(err))
			return nil
		}
		for _, o := range out.InstanceTypeOfferings {
			zones[awssdk.ToString(o.Location)] = true
		}
		if out.NextToken == nil {
			break
		}
		in.NextToken = out.NextToken
	}
	return zones
}

// ensureDefaultVPC returns the ID of the region's default VPC, creating one
// if it does not yet exist.
//
// beforeCreate (may be nil) runs just before a missing default VPC would be
// created; an error from it aborts without creating anything.
func ensureDefaultVPC(ctx context.Context, client ec2NetworkAPI, region string, beforeCreate func() error) (string, error) {
	out, err := client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		Filters: []types.Filter{
			{Name: awssdk.String("isDefault"), Values: []string{"true"}},
		},
	})
	if err != nil {
		return "", apiErrorf(err, "describing VPCs in %s", region)
	}
	if len(out.Vpcs) > 0 {
		return awssdk.ToString(out.Vpcs[0].VpcId), nil
	}

	if beforeCreate != nil {
		if err := beforeCreate(); err != nil {
			return "", err
		}
	}
	log.Printf("[INFO] Network resolution: no default VPC in %s, creating one", region)
	created, err := client.CreateDefaultVpc(ctx, &ec2.CreateDefaultVpcInput{})
	if err != nil {
		return "", apiErrorf(err, "creating default VPC in %s", region)
	}
	if created.Vpc == nil {
		return "", fmt.Errorf("CreateDefaultVpc returned nil Vpc in %s", region)
	}
	return awssdk.ToString(created.Vpc.VpcId), nil
}

// pickSubnet chooses a subnet of the VPC. Only available subnets in AZs that
// offer the instance type (when known) are considered. Preference order:
// default-for-AZ, then subnets that auto-assign public IPs, then any subnet.
// Ties break on subnet ID so the choice is stable across reconciles.
func pickSubnet(ctx context.Context, client ec2NetworkAPI, vpcID, instanceType string, offered map[string]bool) (*types.Subnet, error) {
	out, err := client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		Filters: []types.Filter{
			{Name: awssdk.String("vpc-id"), Values: []string{vpcID}},
			{Name: awssdk.String("state"), Values: []string{string(types.SubnetStateAvailable)}},
		},
	})
	if err != nil {
		return nil, apiErrorf(err, "describing subnets for VPC %s", vpcID)
	}
	if len(out.Subnets) == 0 {
		return nil, fmt.Errorf("no available subnets found in VPC %s: create one or set spec.awsConfig.subnetId", vpcID)
	}
	var subnets []types.Subnet
	for _, s := range out.Subnets {
		if offered == nil || offered[awssdk.ToString(s.AvailabilityZone)] {
			subnets = append(subnets, s)
		}
	}
	if len(subnets) == 0 {
		return nil, fmt.Errorf("instance type %s is not offered in the availability zone of any subnet of VPC %s: add a subnet in an AZ that offers it, or set another instance type", instanceType, vpcID)
	}
	rank := func(s types.Subnet) int {
		switch {
		case awssdk.ToBool(s.DefaultForAz):
			return 0
		case awssdk.ToBool(s.MapPublicIpOnLaunch):
			return 1
		}
		return 2
	}
	sort.Slice(subnets, func(i, j int) bool {
		if ri, rj := rank(subnets[i]), rank(subnets[j]); ri != rj {
			return ri < rj
		}
		return awssdk.ToString(subnets[i].SubnetId) < awssdk.ToString(subnets[j].SubnetId)
	})
	return &subnets[0], nil
}

// egressWarnings inspects the subnet's default route. A node needs outbound
// internet for package/image downloads and, with a VPN, to reach the VPN
// server. Findings are warnings, not errors: routing can legitimately be
// unusual (proxies, VPC endpoints) and DescribeRouteTables may be denied.
func egressWarnings(ctx context.Context, client ec2NetworkAPI, vpcID, subnetID string, disablePublicIP bool) []string {
	rts, err := client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
		Filters: []types.Filter{{Name: awssdk.String("association.subnet-id"), Values: []string{subnetID}}},
	})
	if err == nil && len(rts.RouteTables) == 0 {
		// A subnet without an explicit association uses the VPC's main table.
		rts, err = client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
			Filters: []types.Filter{
				{Name: awssdk.String("vpc-id"), Values: []string{vpcID}},
				{Name: awssdk.String("association.main"), Values: []string{"true"}},
			},
		})
	}
	if err != nil {
		log.Printf("[WARN] Network resolution: cannot read route tables for subnet %s (skipping egress check): %s", subnetID, describeAPIError(err))
		return nil
	}
	if len(rts.RouteTables) == 0 {
		return nil
	}
	viaIGW, found := false, false
	for _, r := range rts.RouteTables[0].Routes {
		if awssdk.ToString(r.DestinationCidrBlock) != "0.0.0.0/0" || r.State == types.RouteStateBlackhole {
			continue
		}
		found = true
		viaIGW = strings.HasPrefix(awssdk.ToString(r.GatewayId), "igw-")
	}
	var w []string
	switch {
	case !found:
		w = append(w, fmt.Sprintf("subnet %s has no default route (0.0.0.0/0): the node cannot reach the internet for package and image downloads or the VPN server unless you provide another egress path (VPC endpoints, proxy)", subnetID))
	case viaIGW && disablePublicIP:
		w = append(w, fmt.Sprintf("subnet %s routes 0.0.0.0/0 through an internet gateway but spec.awsConfig.disablePublicIp is set: the node has no public IP and therefore no internet access; route through a NAT gateway or drop disablePublicIp", subnetID))
	}
	for _, m := range w {
		log.Printf("[WARN] Network resolution: %s", m)
	}
	return w
}

// defaultVPCCIDR is the CIDR AWS gives every default VPC.
const defaultVPCCIDR = "172.31.0.0/16"

// ControlPlaneEndpointHost returns the API endpoint host of a
// "kubeadm join <host>:<port> ..." command ("" when it cannot be found).
func ControlPlaneEndpointHost(joinCommand string) string {
	f := strings.Fields(joinCommand)
	for i, w := range f {
		if w == "join" && i+1 < len(f) {
			ep := f[i+1]
			if h, _, err := net.SplitHostPort(ep); err == nil {
				return h
			}
			return strings.Trim(ep, "[]")
		}
	}
	return ""
}

// cgnat is 100.64.0.0/10, a range AWS allows as a VPC CIDR (commonly a secondary
// one) although it is not RFC1918.
var _, cgnat, _ = net.ParseCIDR("100.64.0.0/10")

// isVPCRoutableIPv4 reports whether ip is an IPv4 address that can only live in
// a VPC or a network routed to it: RFC1918 or 100.64.0.0/10. Public addresses
// (an EIP can front a control plane inside the VPC), IPv6 and non-IPs are not.
func isVPCRoutableIPv4(ip net.IP) bool {
	return ip != nil && ip.To4() != nil && (ip.IsPrivate() || cgnat.Contains(ip))
}

// controlPlaneInCIDRs fails when host is a VPC-routable IPv4 address outside every
// CIDR. Hostnames, public addresses and IPv6 cannot be judged (an EIP or DNS
// name can front a control plane inside the VPC) and pass.
func controlPlaneInCIDRs(host, vpcDesc string, cidrs []string) error {
	ip := net.ParseIP(host)
	if !isVPCRoutableIPv4(ip) {
		return nil
	}
	for _, c := range cidrs {
		if _, n, err := net.ParseCIDR(c); err == nil && n.Contains(ip) {
			return nil
		}
	}
	return fmt.Errorf("the control plane's API endpoint %s is not inside %s (CIDR %s): without a VPN every node must be in the control plane's VPC; "+
		"set spec.awsConfig.vpcId or subnetId to the control plane's VPC, or set spec.awsConfig.skipControlPlaneVpcCheck if it is reachable by VPC peering or a transit gateway",
		host, vpcDesc, strings.Join(cidrs, ", "))
}

// checkControlPlaneInVPC verifies host against the VPC's CIDR blocks (primary
// and associated). It is a guard, not a requirement: when the VPC cannot be
// read the check is skipped.
func checkControlPlaneInVPC(ctx context.Context, client ec2NetworkAPI, vpcID, host string) error {
	if ip := net.ParseIP(host); !isVPCRoutableIPv4(ip) {
		return nil
	}
	out, err := client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{vpcID}})
	if err != nil || len(out.Vpcs) == 0 {
		log.Printf("[WARN] Network resolution: cannot read VPC %s to check the control plane endpoint (skipping): %s", vpcID, describeAPIError(err))
		return nil
	}
	v := out.Vpcs[0]
	var cidrs []string
	if c := awssdk.ToString(v.CidrBlock); c != "" {
		cidrs = append(cidrs, c)
	}
	for _, a := range v.CidrBlockAssociationSet {
		if a.CidrBlockState != nil && a.CidrBlockState.State == types.VpcCidrBlockStateCodeAssociated && a.CidrBlock != nil {
			if c := *a.CidrBlock; !containsString(cidrs, c) {
				cidrs = append(cidrs, c)
			}
		}
	}
	if len(cidrs) == 0 {
		return nil
	}
	return controlPlaneInCIDRs(host, "VPC "+vpcID, cidrs)
}

func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// VerifyControlPlaneInVPC is the standalone form of the no-VPN guard for specs
// that name their network completely (and so skip ResolveOrCreateNetworkConfig).
// vpcID may be empty when subnetID is set.
func VerifyControlPlaneInVPC(ctx context.Context, region string, creds AWSCredentials, vpcID, subnetID, host string) error {
	if ip := net.ParseIP(host); !isVPCRoutableIPv4(ip) {
		return nil
	}
	client, err := newEC2Client(ctx, region, creds)
	if err != nil {
		return fmt.Errorf("creating EC2 client: %w", err)
	}
	if vpcID == "" && subnetID != "" {
		sn, err := describeUserSubnet(ctx, client, region, subnetID)
		if err != nil {
			return err
		}
		vpcID = awssdk.ToString(sn.VpcId)
	}
	if vpcID == "" {
		return nil
	}
	return checkControlPlaneInVPC(ctx, client, vpcID, host)
}
