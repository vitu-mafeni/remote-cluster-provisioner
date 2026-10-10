package gcp

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/protobuf/proto"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	onprem "dcn.ssu.ac.kr/infra/provider/onprem"
)

// ProvisionResult contains the data returned after a launch (mirrors
// aws.ProvisionResult).
type ProvisionResult struct {
	// InstanceID is the GCE instance name (unique within the zone), which is what
	// every later call (get/delete) takes.
	InstanceID string
	VpnIP      string
	PublicKey  string
	// Adopted is true when ProvisionInstance found an instance already launched
	// for this NodeProvision (by name and UID label) and did NOT allocate a VPN
	// peer or launch another one. VpnIP/PublicKey are then empty: the caller must
	// recover the peer recorded for that instance.
	Adopted bool
}

// projectOf is the GCP project of the node: the spec's, else the key's.
func projectOf(np *mlv1alpha1.NodeProvision, creds Credentials) string {
	if np.Spec.GCPConfig != nil && np.Spec.GCPConfig.ProjectID != "" {
		return np.Spec.GCPConfig.ProjectID
	}
	return creds.ProjectID
}

func zoneOf(np *mlv1alpha1.NodeProvision) string {
	if np.Spec.GCPConfig == nil {
		return ""
	}
	return np.Spec.GCPConfig.Zone
}

// lookupInstance fetches the instance named name; owned reports whether its UID
// label matches uid. A missing instance is (nil, false, nil).
func lookupInstance(ctx context.Context, c computeAPI, project, zone, name, uid string) (inst *computepb.Instance, owned bool, err error) {
	inst, err = c.GetInstance(ctx, project, zone, name)
	if err != nil {
		if IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return inst, inst.GetLabels()[LabelUID] == sanitizeLabelValue(uid) || (uid == "" && inst.GetLabels()[LabelUID] == ""), nil
}

// FindInstance returns the name of the instance launched for this NodeProvision
// (matched by its deterministic name AND the UID label), or "" when there is none
// or the zone was never resolved. An instance of that name that belongs to
// something else is reported as absent: it is never adopted or deleted.
func FindInstance(ctx context.Context, np *mlv1alpha1.NodeProvision, creds Credentials) (string, error) {
	zone := zoneOf(np)
	project := projectOf(np, creds)
	if zone == "" || project == "" {
		return "", nil
	}
	c, err := newClient(ctx, creds)
	if err != nil {
		return "", fmt.Errorf("creating GCE client: %w", err)
	}
	defer c.Close() //nolint:errcheck
	name := InstanceName(np)
	inst, owned, err := lookupInstance(ctx, c, project, zone, name, string(np.UID))
	if err != nil {
		return "", fmt.Errorf("looking up instance %s: %w", name, err)
	}
	if inst == nil || !owned {
		return "", nil
	}
	return name, nil
}

// ProvisionInstance performs the full GCE provisioning workflow (the VPN steps
// are skipped when np.Spec.DisableVPN is set, in which case vpnServerClient may
// be nil):
//
//  0. Look for an instance with this NodeProvision's deterministic name. One that
//     carries its UID label is returned with Adopted=true and NOTHING else is
//     done (no VPN peer, no launch); one without it is ErrInstanceNameTaken.
//  1. Ensure the node's firewall rules (idempotent; no VPN server contact).
//  2. VPN mode: allocate a VPN IP, generate a WireGuard keypair, register the peer.
//  3. Render the startup script and insert the instance with it.
//
// Contract for callers (same as ProvisionEC2Node):
//   - Once a VPN peer is registered every error return also carries a non-nil
//     *ProvisionResult holding VpnIP/PublicKey so the caller can persist them and
//     release the peer later. The script is rendered and validated before any
//     peer is registered or the instance is inserted; invalid parameters fail
//     the call without side effects.
//   - The instance is labelled with the NodeProvision UID at insert and the
//     insert carries a requestId derived from the UID and
//     Status.ProvisionRetryCount (RequestIDForAttempt): a retry of the same
//     attempt is idempotent, a relaunch after a failed attempt is a new request.
//   - A 409 from the insert (an instance of that name appeared meanwhile) releases
//     the peer registered for this attempt and returns an error wrapping
//     ErrInstanceAlreadyLaunched with an EMPTY, non-nil result: requeue without
//     counting a failure; the next reconcile adopts the instance (step 0).
//   - The caller must persist ProvisionResult.InstanceID immediately.
func ProvisionInstance(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	creds Credentials,
	sshPublicKey []byte,
	vpnServerClient *sshhelper.Client,
	nc *mlv1alpha1.NodeProvisionNetConfig,
	runtimeCfg pkgruntime.Config,
) (*ProvisionResult, error) {
	name := np.Name
	noVPN := np.Spec.DisableVPN
	log.Printf("[INFO] NodeProvision/%s: GCP validation successful", name)

	// ── Parse + validate everything that needs no side effects first ───────
	if err := ValidateGCPConfig(np.Spec); err != nil {
		return nil, err
	}
	clean := strings.TrimPrefix(nc.Spec.SoftwareConfig.KubernetesVersion, "v")
	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid kubernetes version: %s", nc.Spec.SoftwareConfig.KubernetesVersion)
	}
	crioVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	params := StartupParams(np, nc.Status.ClusterJoinCommand, clean, crioVersion, runtimeCfg, "", "")
	insecureHosts, err := onprem.InsecureRegistryHosts(nc.Spec.SoftwareConfig)
	if err != nil {
		return nil, fmt.Errorf("NodeProvisionNetConfig softwareConfig.%w", err)
	}
	params.InsecureRegistries = insecureHosts
	dry := params
	if !noVPN {
		dry.WGConfig = "[Interface]\n"
		dry.VpnIP = "10.0.0.1"
	}
	if _, err := BuildStartupScript(dry); err != nil {
		return nil, fmt.Errorf("building startup script: %w", err)
	}
	sshKeys, err := SSHKeysMetadata(SSHUser(np.Spec.SSHUsernameOverride), sshPublicKey)
	if err != nil {
		return nil, err
	}
	var vpnRange string
	if !noVPN {
		if nc.Spec.VPNRange == nil || *nc.Spec.VPNRange == "" {
			return nil, fmt.Errorf("NodeProvisionNetConfig has no vpnRange configured")
		}
		vpnRange = *nc.Spec.VPNRange
		if vpnServerClient == nil {
			return nil, fmt.Errorf("VPN server connection is required unless spec.disableVPN is set")
		}
	}

	project := projectOf(np, creds)
	cfg := np.Spec.GCPConfig
	instance := InstanceName(np)
	plan := firewallPlan{
		Instance:    instance,
		NoVPN:       noVPN,
		SSHSources:  cfg.FirewallSourceRanges,
		Description: fmt.Sprintf("%s: NodeProvision %s/%s (uid %s)", firewallOwnerMarker, np.Namespace, np.Name, np.UID),
		WGPort:      parsePort(nc.Spec.VPNServerPublicConfig.VPNPort, defaultWGUDP),
		VPNServerIP: nc.Spec.VPNServerPublicConfig.PublicIP,
	}
	plan.NetProject, plan.NetworkName = ParseNetworkRef(project, cfg.Network)
	// Fail on an unusable plan (e.g. a VPN server given as a hostname) before
	// anything is created.
	if _, err := desiredFirewalls(plan); err != nil {
		return nil, err
	}
	instProto, err := buildInstance(np, project, instance, "", sshKeys)
	if err != nil {
		return nil, err
	}

	c, err := newClient(ctx, creds)
	if err != nil {
		return nil, fmt.Errorf("creating GCE client: %w", err)
	}
	defer c.Close() //nolint:errcheck

	// ── Adopt an instance a previous (crashed) attempt already launched ────
	// MUST come before any VPN peer is allocated: adopting needs no new peer, and
	// registering one first would leak it.
	existing, owned, err := lookupInstance(ctx, c, project, cfg.Zone, instance, string(np.UID))
	if err != nil {
		return nil, apiErrorf(err, "checking for an existing instance")
	}
	if existing != nil {
		if !owned {
			return nil, fmt.Errorf("%w: instance %q in zone %s", ErrInstanceNameTaken, instance, cfg.Zone)
		}
		log.Printf("[INFO] NodeProvision/%s: adopting existing GCE instance %s (no VPN peer allocated)", name, instance)
		return &ProvisionResult{InstanceID: instance, Adopted: true}, nil
	}

	// ── Firewall rules (no VPN server involved; cleaned up on deletion) ─────
	if err := ensureFirewalls(ctx, c, plan); err != nil {
		if !IsPermissionDenied(err) {
			return nil, err
		}
		// Same stance as the AWS security-group step: not being allowed to edit
		// firewall rules is not fatal (they may be managed centrally).
		log.Printf("[WARN] NodeProvision/%s: could not ensure firewall rules, continuing: %v", name, err)
	}

	var vpnIP, publicKey string
	if noVPN {
		log.Printf("[INFO] NodeProvision/%s: spec.disableVPN set — skipping VPN allocation", name)
	} else {
		peer, err := onprem.AllocateAndRegisterVPNPeer(
			ctx, vpnServerClient, vpnRange,
			nc.Status.UsedIPAddresses,
			nc.Spec.VPNServerPublicConfig.PublicIP,
			parsePort(nc.Spec.VPNServerPublicConfig.VPNPort, defaultWGUDP),
		)
		if err != nil {
			return nil, err
		}
		vpnIP, publicKey = peer.IP, peer.PublicKey
		params.WGConfig = peer.WGConfig
		params.VpnIP = vpnIP
		log.Printf("[INFO] NodeProvision/%s: VPN peer registered (vpnIP=%s)", name, vpnIP)
	}
	// vpnResult carries the VPN allocation so the caller can persist it even if
	// the launch fails — preventing orphaned peers on retry.
	vpnResult := &ProvisionResult{VpnIP: vpnIP, PublicKey: publicKey}

	script, err := BuildStartupScript(params)
	if err != nil {
		return vpnResult, fmt.Errorf("building startup script: %w", err)
	}
	instProto, err = buildInstance(np, project, instance, script, sshKeys)
	if err != nil {
		return vpnResult, err
	}

	log.Printf("[INFO] NodeProvision/%s: Creating GCE instance %s (machineType=%s zone=%s)",
		name, instance, np.Spec.InstanceType, cfg.Zone)
	if err := c.InsertInstance(ctx, project, cfg.Zone, RequestIDForAttempt(string(np.UID), np.Status.ProvisionRetryCount), instProto); err != nil {
		if IsAlreadyExists(err) {
			// An instance of this name exists but the lookup above did not see it
			// (created concurrently). The peer registered for THIS attempt will
			// never be used: release it and let the next reconcile adopt.
			if publicKey != "" {
				if rerr := onprem.UnregisterVPNPeer(vpnServerClient, publicKey); rerr != nil {
					log.Printf("[WARN] NodeProvision/%s: could not release VPN peer %s after a 409 on insert: %v", name, publicKey, rerr)
				}
			}
			return &ProvisionResult{}, fmt.Errorf("%w: insert: %v", ErrInstanceAlreadyLaunched, err)
		}
		return vpnResult, apiErrorf(err, "launching GCE instance")
	}
	vpnResult.InstanceID = instance
	log.Printf("[INFO] NodeProvision/%s: GCE instance %s created", name, instance)
	return vpnResult, nil
}

// buildInstance assembles the Instances.Insert body. script may be empty for a
// dry build (the real one is rendered after a VPN peer exists).
func buildInstance(np *mlv1alpha1.NodeProvision, project, instance, script, sshKeys string) (*computepb.Instance, error) {
	cfg := np.Spec.GCPConfig
	if cfg == nil {
		return nil, fmt.Errorf("spec.gcpConfig is required for GCP provider")
	}
	zone := cfg.Zone
	region, ok := RegionOfZone(zone)
	if !ok {
		return nil, fmt.Errorf("spec.gcpConfig.zone %q is not a valid zone name", zone)
	}
	if cfg.SourceImage == "" {
		return nil, fmt.Errorf("spec.gcpConfig.sourceImage is empty (defaults were not resolved)")
	}

	diskGB := int64(DefaultBootDiskGB)
	if cfg.BootDiskSizeGB > 0 {
		diskGB = int64(cfg.BootDiskSizeGB)
	}
	diskType := cfg.BootDiskType
	if diskType == "" {
		diskType = DefaultBootDiskType
	}
	if !strings.Contains(diskType, "/") {
		diskType = fmt.Sprintf("zones/%s/diskTypes/%s", zone, diskType)
	}

	netProject, netName := ParseNetworkRef(project, cfg.Network)
	nic := &computepb.NetworkInterface{
		Network: proto.String(fmt.Sprintf("projects/%s/global/networks/%s", netProject, netName)),
	}
	if cfg.Subnetwork != "" {
		subProject, subRegion, subName, err := ParseSubnetworkRef(netProject, region, cfg.Subnetwork)
		if err != nil {
			return nil, err
		}
		nic.Subnetwork = proto.String(fmt.Sprintf("projects/%s/regions/%s/subnetworks/%s", subProject, subRegion, subName))
	}
	if !cfg.DisableExternalIP {
		// Ephemeral external IPv4, like AssociatePublicIpAddress on EC2.
		nic.AccessConfigs = []*computepb.AccessConfig{{
			Name: proto.String("External NAT"),
			Type: proto.String("ONE_TO_ONE_NAT"),
		}}
	}

	labels := instanceLabels(np)
	tags := []string{NodeTag(instance)}
	seen := map[string]bool{tags[0]: true}
	for _, t := range cfg.NetworkTags {
		if !seen[t] {
			seen[t] = true
			tags = append(tags, t)
		}
	}

	// enable-oslogin=FALSE: with OS Login on (project/org default) the guest agent
	// ignores ssh-keys metadata and the controller could not log in.
	items := []*computepb.Items{
		{Key: proto.String("ssh-keys"), Value: proto.String(sshKeys)},
		{Key: proto.String("enable-oslogin"), Value: proto.String("FALSE")},
	}
	if script != "" {
		items = append([]*computepb.Items{{Key: proto.String("startup-script"), Value: proto.String(script)}}, items...)
	}

	inst := &computepb.Instance{
		Name:        proto.String(instance),
		Description: proto.String(fmt.Sprintf("%s: NodeProvision %s/%s", firewallOwnerMarker, np.Namespace, np.Name)),
		MachineType: proto.String(fmt.Sprintf("zones/%s/machineTypes/%s", zone, np.Spec.InstanceType)),
		Labels:      labels,
		Tags:        &computepb.Tags{Items: tags},
		Disks: []*computepb.AttachedDisk{{
			Boot:       proto.Bool(true),
			AutoDelete: proto.Bool(true),
			Type:       proto.String("PERSISTENT"),
			InitializeParams: &computepb.AttachedDiskInitializeParams{
				SourceImage: proto.String(cfg.SourceImage),
				DiskSizeGb:  proto.Int64(diskGB),
				DiskType:    proto.String(diskType),
				Labels:      labels,
			},
		}},
		NetworkInterfaces: []*computepb.NetworkInterface{nic},
		Metadata:          &computepb.Metadata{Items: items},
	}

	if a := cfg.Accelerator; a != nil && a.Type != "" {
		count := a.Count
		if count <= 0 {
			count = 1
		}
		inst.GuestAccelerators = []*computepb.AcceleratorConfig{{
			AcceleratorType:  proto.String(fmt.Sprintf("zones/%s/acceleratorTypes/%s", zone, a.Type)),
			AcceleratorCount: proto.Int32(count),
		}}
	}

	// GPU instances cannot live-migrate. Spot VMs cannot auto-restart and are
	// deleted when preempted, so a preempted node vanishes instead of lingering
	// stopped (and billed for its disk).
	switch {
	case cfg.Spot:
		inst.Scheduling = &computepb.Scheduling{
			ProvisioningModel:         proto.String("SPOT"),
			InstanceTerminationAction: proto.String("DELETE"),
			AutomaticRestart:          proto.Bool(false),
			OnHostMaintenance:         proto.String("TERMINATE"),
		}
	case UsesGPU(np.Spec.InstanceType, cfg):
		inst.Scheduling = &computepb.Scheduling{
			OnHostMaintenance: proto.String("TERMINATE"),
			AutomaticRestart:  proto.Bool(true),
		}
	}

	if cfg.ServiceAccountEmail != "" {
		scopes := cfg.ServiceAccountScopes
		if len(scopes) == 0 {
			scopes = []string{"https://www.googleapis.com/auth/cloud-platform"}
		}
		inst.ServiceAccounts = []*computepb.ServiceAccount{{
			Email:  proto.String(cfg.ServiceAccountEmail),
			Scopes: scopes,
		}}
	}
	return inst, nil
}

// WaitForInstanceRunning inspects the instance once and returns its internal and
// external IPs when it is RUNNING. ("", "", nil) means "not running yet,
// requeue". Like the AWS version, states that are never followed by RUNNING
// again (the controller never starts an instance) are an error so the
// NodeProvision fails instead of polling forever: STOPPING, STOPPED,
// TERMINATED (a Spot preemption or a crashed boot), SUSPENDING, SUSPENDED,
// DEPROVISIONING, and an instance that disappeared. API errors keep their
// class (see IsTransient) so the caller may retry a throttled call.
func WaitForInstanceRunning(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	creds Credentials,
	instanceName string,
) (internalIP, externalIP string, err error) {
	name := np.Name
	zone, project := zoneOf(np), projectOf(np, creds)
	if zone == "" || project == "" {
		return "", "", fmt.Errorf("spec.gcpConfig.zone/projectId are not resolved")
	}
	c, err := newClient(ctx, creds)
	if err != nil {
		return "", "", fmt.Errorf("creating GCE client: %w", err)
	}
	defer c.Close() //nolint:errcheck

	log.Printf("[INFO] NodeProvision/%s: Waiting for instance readiness", name)
	inst, err := c.GetInstance(ctx, project, zone, instanceName)
	if err != nil {
		if IsNotFound(err) {
			return "", "", fmt.Errorf("instance %s not found (deleted or preempted)", instanceName)
		}
		return "", "", fmt.Errorf("describing instance %s: %w", instanceName, err)
	}
	switch status := inst.GetStatus(); status {
	case "RUNNING":
		// read the addresses below
	case "STOPPING", "STOPPED", "TERMINATED", "SUSPENDING", "SUSPENDED", "DEPROVISIONING":
		return "", "", fmt.Errorf("instance %s entered terminal state %q and will never become running", instanceName, status)
	default:
		// PROVISIONING, STAGING, REPAIRING, or not reported yet: keep polling.
		return "", "", nil
	}

	private, public := instanceIPs(inst)
	if private == "" {
		return "", "", nil // running but the NIC is not populated yet
	}
	log.Printf("[INFO] NodeProvision/%s: Assigned internal IP %s", name, private)
	if public != "" {
		log.Printf("[INFO] NodeProvision/%s: Assigned external IP %s", name, public)
	}
	return private, public, nil
}

// instanceIPs returns the internal IP and external (NAT) IP of the primary NIC.
func instanceIPs(inst *computepb.Instance) (internal, external string) {
	if len(inst.GetNetworkInterfaces()) == 0 {
		return "", ""
	}
	nic := inst.GetNetworkInterfaces()[0]
	internal = nic.GetNetworkIP()
	for _, ac := range nic.GetAccessConfigs() {
		if ip := ac.GetNatIP(); ip != "" {
			external = ip
			break
		}
	}
	return internal, external
}

// DeleteInstance deprovisions a node: it deletes the instance (waiting for the
// operation) and the per-node firewall rules this controller created. It is
// idempotent — an instance or rule that is already gone is success — and
// instanceName may be empty when no instance was ever recorded (only the
// firewall rules are then removed). An instance that exists under that name but
// carries a different UID label is never deleted.
//
// The node's SSH key lives in the instance's metadata and disappears with it.
func DeleteInstance(ctx context.Context, np *mlv1alpha1.NodeProvision, creds Credentials, instanceName string) error {
	name := np.Name
	cfg := np.Spec.GCPConfig
	project, zone := projectOf(np, creds), zoneOf(np)
	if cfg == nil || zone == "" || project == "" {
		// The launch path never got past defaults resolution: nothing exists.
		log.Printf("[INFO] NodeProvision/%s: no resolved GCP location — nothing to delete", name)
		return nil
	}
	c, err := newClient(ctx, creds)
	if err != nil {
		return fmt.Errorf("creating GCE client: %w", err)
	}
	defer c.Close() //nolint:errcheck

	if instanceName != "" {
		log.Printf("[INFO] NodeProvision/%s: Deleting GCE instance %s", name, instanceName)
		inst, owned, lerr := lookupInstance(ctx, c, project, zone, instanceName, string(np.UID))
		switch {
		case lerr != nil:
			return fmt.Errorf("looking up instance %s: %w", instanceName, lerr)
		case inst == nil:
			log.Printf("[INFO] NodeProvision/%s: Instance %s already gone", name, instanceName)
		case !owned:
			log.Printf("[WARN] NodeProvision/%s: instance %s carries a different NodeProvision UID label; NOT deleting it", name, instanceName)
		default:
			if derr := c.DeleteInstance(ctx, project, zone, instanceName); derr != nil && !IsNotFound(derr) {
				return fmt.Errorf("deleting instance %s: %w", instanceName, derr)
			}
			log.Printf("[INFO] NodeProvision/%s: Instance %s deleted", name, instanceName)
		}
	}

	netProject, _ := ParseNetworkRef(project, cfg.Network)
	return deleteFirewalls(ctx, c, netProject, InstanceName(np))
}

// parsePort converts a string port value to int, returning def when empty or
// unparseable (VPN ports are stored as strings in the NetConfig).
func parsePort(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 || n > 65535 {
		return def
	}
	return n
}
