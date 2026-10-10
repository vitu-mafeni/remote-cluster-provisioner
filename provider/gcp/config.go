// Package gcp implements Google Compute Engine node provisioning for the
// NodeProvision controller. It mirrors the AWS provider: the controller
// generates a WireGuard keypair (unless spec.disableVPN), registers the peer on
// the VPN server over SSH, and passes the WireGuard client config plus the
// kubeadm join command to the instance in a startup script (the same
// cloud-neutral bootstrap script the AWS path renders into cloud-init). Without
// a VPN no VPN server is contacted and the instance's internal IP is the
// kubelet node IP.
package gcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/compute/apiv1/computepb"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

// Defaults applied when the spec leaves a field empty.
const (
	// DefaultImageFamily/DefaultImageProject select the latest Ubuntu 22.04 LTS
	// (x86_64), the same distro as the AWS path.
	DefaultImageFamily  = "ubuntu-2204-lts"
	DefaultImageProject = "ubuntu-os-cloud"
	DefaultBootDiskGB   = 50
	DefaultBootDiskType = "pd-balanced"
	DefaultNetwork      = "default"
	// DefaultSSHUser matches the AWS path and the controller's SSH default.
	DefaultSSHUser = "ubuntu"
	// DefaultGPUAccelerator is attached to a defaulted N1 GPU instance.
	DefaultGPUAccelerator = "nvidia-tesla-t4"
	minBootDiskGB         = 10
	maxAccelerators       = 8
)

// DefaultMachineTypeForLabel returns the machine type used for a nodeLabel when
// spec.instanceType is empty (cpu ~ t3.xlarge, gpu ~ a single-T4 N1 shape).
func DefaultMachineTypeForLabel(nodeLabel string) string {
	switch strings.ToLower(strings.TrimSpace(nodeLabel)) {
	case "cpu":
		return "e2-standard-4"
	case "gpu":
		return "n1-standard-8"
	}
	return ""
}

// ── naming ──────────────────────────────────────────────────────────────────

var (
	// RFC1035 resource name, as required for instances and firewall rules.
	gceNameRE   = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	labelKeyRE  = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValRE  = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
	zoneRE      = regexp.MustCompile(`^[a-z]+(?:-[a-z]+)?[0-9]+-[a-z]$`)
	regionRE    = regexp.MustCompile(`^[a-z]+(?:-[a-z]+)?[0-9]+$`)
	invalidChar = regexp.MustCompile(`[^a-z0-9-]+`)
)

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:8]
}

// InstanceName is the deterministic GCE instance name of a NodeProvision: its
// own name when that is a valid GCE name (so the instance hostname equals the
// Kubernetes node name the bootstrap script sets), otherwise a sanitised,
// truncated form with a hash of namespace/name appended. Determinism is what
// makes a launch retried after a crash idempotent.
func InstanceName(np *mlv1alpha1.NodeProvision) string {
	n := strings.ToLower(np.Name)
	if gceNameRE.MatchString(n) {
		return n
	}
	s := strings.Trim(invalidChar.ReplaceAllString(n, "-"), "-")
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		s = "np-" + s
	}
	if len(s) > 54 {
		s = strings.TrimRight(s[:54], "-")
	}
	return s + "-" + shortHash(np.Namespace+"/"+np.Name)
}

// nodeBase is the prefix of every per-node resource name ("np-<instance>"),
// shortened with a hash when needed so that base+suffix stays within 63 chars.
func nodeBase(instance, suffix string) string {
	base := "np-" + instance
	if len(base)+len(suffix) > 63 {
		keep := 63 - len(suffix) - 9 // "-" + 8 hex
		base = strings.TrimRight(base[:keep], "-") + "-" + shortHash(instance)
	}
	return base
}

// NodeTag is the network tag unique to one node; its firewall rules target it.
func NodeTag(instance string) string { return nodeBase(instance, "") }

// FirewallName returns the name of the node's firewall rule with the given
// suffix ("wg", "ssh" or "mgmt").
func FirewallName(instance, suffix string) string {
	return nodeBase(instance, "-"+suffix) + "-" + suffix
}

// nodeFirewallSuffixes are all rule names a node can own (used on cleanup, which
// must not depend on the spec that created them).
var nodeFirewallSuffixes = []string{"wg", "ssh", "mgmt"}

// Label keys the controller owns; user labels cannot override them.
const (
	LabelManagedBy = "managed-by"
	LabelName      = "nodeprovision-name"
	LabelNamespace = "nodeprovision-namespace"
	// LabelUID ties an instance to the NodeProvision that launched it (adoption
	// and cleanup, the equivalent of the AWS nodeprovision-uid tag).
	LabelUID = "nodeprovision-uid"
	// ManagedByValue is the managed-by label value.
	ManagedByValue = "node-provision-controller"
)

// sanitizeLabelValue lowercases v and maps characters GCE labels do not allow
// (dots, slashes, ...) to '-', truncating to 63 characters.
func sanitizeLabelValue(v string) string {
	v = strings.ToLower(v)
	var b strings.Builder
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := b.String()
	if len(out) > 63 {
		out = out[:63]
	}
	return out
}

// ControllerLabels are the labels the controller itself owns on the instance.
func ControllerLabels(np *mlv1alpha1.NodeProvision) map[string]string {
	l := map[string]string{
		LabelManagedBy: ManagedByValue,
		LabelName:      sanitizeLabelValue(np.Name),
		LabelNamespace: sanitizeLabelValue(np.Namespace),
	}
	if np.UID != "" {
		l[LabelUID] = sanitizeLabelValue(string(np.UID))
	}
	return l
}

// instanceLabels merges user labels under the controller's.
func instanceLabels(np *mlv1alpha1.NodeProvision) map[string]string {
	out := map[string]string{}
	if cfg := np.Spec.GCPConfig; cfg != nil {
		for k, v := range cfg.Labels {
			out[k] = v
		}
	}
	for k, v := range ControllerLabels(np) {
		out[k] = v
	}
	return out
}

// RequestIDForAttempt derives the insert requestId (a UUID) for one provisioning
// ATTEMPT: deterministic within an attempt, so a call retried after a crash or a
// lost response is ignored by the server instead of creating a second instance,
// and different for every attempt (Status.ProvisionRetryCount), because the
// server would otherwise replay the first attempt's (deleted) instance for an
// hour.
func RequestIDForAttempt(uid string, attempt int) string {
	if attempt < 0 {
		attempt = 0
	}
	sum := sha256.Sum256([]byte("np-gce/" + uid + "/" + strconv.Itoa(attempt)))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50 // version 5 layout
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// ── machine types ───────────────────────────────────────────────────────────

// MachineFamily returns the machine series of a machine type ("n1" for
// "n1-standard-8" and for the N1 "custom-*" shapes, "g2" for "g2-standard-8").
func MachineFamily(machineType string) string {
	mt := strings.ToLower(strings.TrimSpace(machineType))
	if strings.HasPrefix(mt, "custom-") || strings.HasPrefix(mt, "n1-custom") {
		return "n1"
	}
	if i := strings.Index(mt, "-"); i > 0 {
		return mt[:i]
	}
	return mt
}

// impliedGPUFamilies are the series whose machine types include their GPUs
// (accelerators are attached automatically and must not be declared).
var impliedGPUFamilies = map[string]bool{"a2": true, "a3": true, "a4": true, "g2": true, "g4": true}

// armFamilies are the Arm series; the bootstrap script and image are x86_64.
var armFamilies = map[string]bool{"t2a": true, "c4a": true, "n4a": true, "a4x": true}

// HasImpliedGPU reports whether the machine type is a GPU series with built-in GPUs.
func HasImpliedGPU(machineType string) bool { return impliedGPUFamilies[MachineFamily(machineType)] }

// UsesGPU reports whether the instance will have a GPU (and therefore needs
// onHostMaintenance=TERMINATE): an accelerator on N1 or a built-in-GPU series.
func UsesGPU(machineType string, cfg *mlv1alpha1.GCPConfig) bool {
	return HasImpliedGPU(machineType) || (cfg != nil && cfg.Accelerator != nil && cfg.Accelerator.Type != "")
}

// ── zones ───────────────────────────────────────────────────────────────────

// RegionOfZone returns the region of a zone ("us-central1-a" -> "us-central1").
func RegionOfZone(zone string) (string, bool) {
	if !zoneRE.MatchString(zone) {
		return "", false
	}
	return zone[:strings.LastIndex(zone, "-")], true
}

// ── validation ──────────────────────────────────────────────────────────────

// ValidateGCPConfig checks that all parameters needed to launch are present and
// consistent. It runs after defaults resolution (ResolveDefaults), so
// project, zone and machine type must be set by then.
//
// Region/zone semantics: spec.gcpConfig.zone is authoritative; spec.region, when
// set, must be the zone's region.
func ValidateGCPConfig(spec mlv1alpha1.NodeProvisionSpec) error {
	if spec.InstanceType == "" {
		return fmt.Errorf("spec.instanceType (the GCE machine type) is required for GCP provider")
	}
	cfg := spec.GCPConfig
	if cfg == nil {
		return fmt.Errorf("spec.gcpConfig is required for GCP provider")
	}
	if cfg.ProjectID == "" {
		return fmt.Errorf("spec.gcpConfig.projectId is required (or use a service-account key that carries project_id)")
	}
	if cfg.Zone == "" {
		return fmt.Errorf("spec.gcpConfig.zone or spec.region is required for GCP provider")
	}
	region, ok := RegionOfZone(cfg.Zone)
	if !ok {
		return fmt.Errorf("spec.gcpConfig.zone %q is not a valid zone name (expected e.g. us-central1-a)", cfg.Zone)
	}
	if spec.Region != "" && spec.Region != region {
		return fmt.Errorf("spec.gcpConfig.zone %q is in region %q but spec.region is %q", cfg.Zone, region, spec.Region)
	}
	fam := MachineFamily(spec.InstanceType)
	if armFamilies[fam] {
		return fmt.Errorf("machine type %q is an Arm shape; only x86_64 machine types are supported", spec.InstanceType)
	}
	if cfg.BootDiskSizeGB != 0 && cfg.BootDiskSizeGB < minBootDiskGB {
		return fmt.Errorf("spec.gcpConfig.bootDiskSizeGB must be at least %d", minBootDiskGB)
	}
	if a := cfg.Accelerator; a != nil && a.Type != "" {
		if impliedGPUFamilies[fam] {
			return fmt.Errorf("machine type %q already includes its GPUs; leave spec.gcpConfig.accelerator unset", spec.InstanceType)
		}
		if fam != "n1" {
			return fmt.Errorf("spec.gcpConfig.accelerator is only supported on N1 machine types, not %q (use an a2/a3/g2 type for built-in GPUs)", spec.InstanceType)
		}
		if a.Count < 0 || a.Count > maxAccelerators {
			return fmt.Errorf("spec.gcpConfig.accelerator.count must be between 1 and %d", maxAccelerators)
		}
	}
	if cfg.ServiceAccountEmail != "" && !strings.Contains(cfg.ServiceAccountEmail, "@") {
		return fmt.Errorf("spec.gcpConfig.serviceAccountEmail %q is not an email address", cfg.ServiceAccountEmail)
	}
	if len(cfg.ServiceAccountScopes) > 0 && cfg.ServiceAccountEmail == "" {
		return fmt.Errorf("spec.gcpConfig.serviceAccountScopes requires spec.gcpConfig.serviceAccountEmail")
	}
	for k, v := range cfg.Labels {
		if !labelKeyRE.MatchString(k) {
			return fmt.Errorf("spec.gcpConfig.labels key %q is invalid (lowercase letters, digits, '_' and '-', starting with a letter, max 63)", k)
		}
		if !labelValRE.MatchString(v) {
			return fmt.Errorf("spec.gcpConfig.labels[%q] value is invalid (lowercase letters, digits, '_' and '-', max 63)", k)
		}
	}
	if len(cfg.Labels)+len(controllerLabelKeys) > 64 {
		return fmt.Errorf("spec.gcpConfig.labels: too many labels (GCE allows 64 per instance, %d are reserved)", len(controllerLabelKeys))
	}
	for _, tag := range cfg.NetworkTags {
		if !gceNameRE.MatchString(tag) {
			return fmt.Errorf("spec.gcpConfig.networkTags entry %q is invalid (RFC1035: lowercase letters, digits, '-', max 63)", tag)
		}
	}
	for _, c := range cfg.FirewallSourceRanges {
		if _, _, err := net.ParseCIDR(c); err != nil {
			return fmt.Errorf("spec.gcpConfig.firewallSourceRanges entry %q is not a CIDR", c)
		}
	}
	if u := spec.SSHUsernameOverride; u != "" && (u == "root" || strings.ContainsAny(u, ": \t\n")) {
		return fmt.Errorf("spec.sshUsernameOverride %q cannot be used with GCE ssh-keys metadata", u)
	}
	return nil
}

var controllerLabelKeys = []string{LabelManagedBy, LabelName, LabelNamespace, LabelUID}

// ── defaults resolution ─────────────────────────────────────────────────────

// Resolved is the fully populated provider configuration produced by
// ResolveDefaults; the controller persists it into the spec.
type Resolved struct {
	Region       string
	InstanceType string
	Config       mlv1alpha1.GCPConfig
}

// ResolveDefaults fills in everything the spec left empty, validating what
// exists against the project with read-only Compute calls:
//   - project: from the service-account key
//   - instanceType: from nodeLabel (cpu -> e2-standard-4, gpu -> n1-standard-8 + 1x T4)
//   - zone: given zone checked for the machine type (and accelerator); with only
//     a region the first UP zone of it that offers them is chosen
//   - network/subnetwork: default network; custom-mode networks need a subnetwork
//   - sourceImage: latest image of imageFamily/imageProject (Ubuntu 22.04 LTS)
//
// Fields set by the user are never overwritten.
func ResolveDefaults(ctx context.Context, creds Credentials, np *mlv1alpha1.NodeProvision) (*Resolved, error) {
	res := &Resolved{Region: np.Spec.Region, InstanceType: np.Spec.InstanceType}
	if np.Spec.GCPConfig != nil {
		res.Config = *np.Spec.GCPConfig.DeepCopy()
	}
	cfg := &res.Config

	if cfg.ProjectID == "" {
		cfg.ProjectID = creds.ProjectID
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("spec.gcpConfig.projectId is required: the service-account key has no project_id")
	}

	instanceTypeDefaulted := false
	if res.InstanceType == "" {
		res.InstanceType = DefaultMachineTypeForLabel(np.Spec.NodeLabel)
		if res.InstanceType == "" {
			return nil, fmt.Errorf("spec.instanceType is required: no default machine type defined for nodeLabel %q", np.Spec.NodeLabel)
		}
		instanceTypeDefaulted = true
	}
	if instanceTypeDefaulted && cfg.Accelerator == nil && awsprovision.IsGPUNode(np) &&
		MachineFamily(res.InstanceType) == "n1" {
		cfg.Accelerator = &mlv1alpha1.GCPAccelerator{Type: DefaultGPUAccelerator, Count: 1}
	}
	if cfg.Accelerator != nil && cfg.Accelerator.Type != "" && cfg.Accelerator.Count == 0 {
		cfg.Accelerator.Count = 1
	}

	// Location consistency is checked before any API call.
	if cfg.Zone != "" {
		region, ok := RegionOfZone(cfg.Zone)
		if !ok {
			return nil, fmt.Errorf("spec.gcpConfig.zone %q is not a valid zone name (expected e.g. us-central1-a)", cfg.Zone)
		}
		if res.Region != "" && res.Region != region {
			return nil, fmt.Errorf("spec.gcpConfig.zone %q is in region %q but spec.region is %q", cfg.Zone, region, res.Region)
		}
		res.Region = region
	} else if res.Region == "" {
		return nil, fmt.Errorf("spec.region or spec.gcpConfig.zone is required for GCP provider")
	} else if !regionRE.MatchString(res.Region) {
		return nil, fmt.Errorf("spec.region %q is not a valid GCP region (expected e.g. us-central1)", res.Region)
	}

	// Cheap structural validation (arm, accelerator vs machine family, ...) so a
	// bad spec fails before it costs API calls. Project and zone are filled above
	// or below, so check a copy with a placeholder zone.
	probe := np.Spec
	probe.InstanceType = res.InstanceType
	probe.Region = res.Region
	pcfg := res.Config
	if pcfg.Zone == "" {
		pcfg.Zone = res.Region + "-a"
	}
	probe.GCPConfig = &pcfg
	if err := ValidateGCPConfig(probe); err != nil {
		return nil, err
	}

	c, err := newClient(ctx, creds)
	if err != nil {
		return nil, fmt.Errorf("creating GCE client: %w", err)
	}
	defer c.Close() //nolint:errcheck

	// ── zone ──────────────────────────────────────────────────────────────────
	if cfg.Zone != "" {
		if err := checkShape(ctx, c, cfg.ProjectID, cfg.Zone, res.InstanceType, cfg.Accelerator); err != nil {
			return nil, err
		}
	} else {
		zone, err := pickZone(ctx, c, cfg.ProjectID, res.Region, res.InstanceType, cfg.Accelerator)
		if err != nil {
			return nil, err
		}
		cfg.Zone = zone
	}

	// ── subnetwork ────────────────────────────────────────────────────────────
	// A subnetwork pins the network too: when spec.network is unset it is taken
	// from the subnetwork (otherwise it would silently default to "default" and
	// the instance create would fail with a network/subnetwork mismatch).
	if cfg.Subnetwork != "" {
		// A bare subnetwork name lives in the network's project (the Shared VPC
		// host project when spec.network names one).
		defProject, _ := ParseNetworkRef(cfg.ProjectID, cfg.Network)
		subProject, subRegion, subName, err := ParseSubnetworkRef(defProject, res.Region, cfg.Subnetwork)
		if err != nil {
			return nil, err
		}
		if subRegion != res.Region {
			return nil, fmt.Errorf("spec.gcpConfig.subnetwork %q is in region %q but the instance is in region %q (zone %s): a subnetwork is regional",
				cfg.Subnetwork, subRegion, res.Region, cfg.Zone)
		}
		sn, err := c.GetSubnetwork(ctx, subProject, subRegion, subName)
		if err != nil {
			if IsNotFound(err) {
				return nil, fmt.Errorf("subnetwork %q not found in project %q region %q: check spec.gcpConfig.subnetwork", subName, subProject, subRegion)
			}
			return nil, apiErrorf(err, "looking up subnetwork %q", subName)
		}
		if p := sn.GetPurpose(); p != "" && p != computepb.Subnetwork_PRIVATE.String() {
			return nil, fmt.Errorf("subnetwork %q has purpose %s: only PRIVATE subnetworks can host instances", subName, p)
		}
		if st := sn.GetState(); st != "" && st != computepb.Subnetwork_READY.String() {
			return nil, fmt.Errorf("subnetwork %q is not ready (state %s)", subName, st)
		}
		snProject, snName := ParseNetworkRef(subProject, sn.GetNetwork())
		if cfg.Network == "" {
			cfg.Network = fmt.Sprintf("projects/%s/global/networks/%s", snProject, snName)
		} else if netProject, netName := ParseNetworkRef(cfg.ProjectID, cfg.Network); netProject != snProject || netName != snName {
			return nil, fmt.Errorf("subnetwork %q belongs to network projects/%s/global/networks/%s, not spec.gcpConfig.network %q",
				subName, snProject, snName, cfg.Network)
		}
	}

	// ── network ───────────────────────────────────────────────────────────────
	netProject, netName := ParseNetworkRef(cfg.ProjectID, cfg.Network)
	nw, err := c.GetNetwork(ctx, netProject, netName)
	if err != nil {
		if IsNotFound(err) {
			return nil, fmt.Errorf("VPC network %q not found in project %q: create it or set spec.gcpConfig.network", netName, netProject)
		}
		return nil, apiErrorf(err, "looking up VPC network %q", netName)
	}
	if nw.GetIPv4Range() != "" {
		return nil, fmt.Errorf("VPC network %q is a legacy network, which is not supported", netName)
	}
	if cfg.Network == "" {
		cfg.Network = DefaultNetwork
	}
	if cfg.Subnetwork == "" && !nw.GetAutoCreateSubnetworks() {
		return nil, fmt.Errorf("VPC network %q is custom-mode: spec.gcpConfig.subnetwork is required", netName)
	}

	// ── image ─────────────────────────────────────────────────────────────────
	if cfg.SourceImage == "" {
		family, project := cfg.ImageFamily, cfg.ImageProject
		if family == "" {
			family = DefaultImageFamily
		}
		if project == "" {
			project = DefaultImageProject
		}
		img, err := c.GetImageFromFamily(ctx, project, family)
		if err != nil {
			if IsNotFound(err) {
				return nil, fmt.Errorf("image family %q not found in project %q", family, project)
			}
			return nil, apiErrorf(err, "resolving image family %s/%s", project, family)
		}
		if a := img.GetArchitecture(); a != "" && a != computepb.Image_X86_64.String() {
			return nil, fmt.Errorf("image family %s/%s resolves to a %s image; only X86_64 is supported", project, family, a)
		}
		cfg.SourceImage = compactResourcePath(img.GetSelfLink())
		if cfg.SourceImage == "" {
			return nil, fmt.Errorf("image family %s/%s has no selfLink", project, family)
		}
	}
	return res, nil
}

// compactResourcePath turns "https://www.googleapis.com/compute/v1/projects/p/..."
// into "projects/p/...".
func compactResourcePath(link string) string {
	if i := strings.Index(link, "projects/"); i >= 0 {
		return link[i:]
	}
	return link
}

// ParseNetworkRef splits a network reference into (project, name). ref may be a
// bare name (the instance's project), "projects/<p>/global/networks/<n>" or the
// full URL (shared VPC host project).
func ParseNetworkRef(defaultProject, ref string) (project, name string) {
	if ref == "" {
		return defaultProject, DefaultNetwork
	}
	ref = compactResourcePath(ref)
	parts := strings.Split(ref, "/")
	if len(parts) == 5 && parts[0] == "projects" && parts[2] == "global" && parts[3] == "networks" {
		return parts[1], parts[4]
	}
	return defaultProject, parts[len(parts)-1]
}

// ParseSubnetworkRef splits a subnetwork reference into (project, region, name).
// ref may be a bare name (defaultProject and defaultRegion),
// "regions/<r>/subnetworks/<n>", "projects/<p>/regions/<r>/subnetworks/<n>" or
// the full URL (Shared VPC host project).
func ParseSubnetworkRef(defaultProject, defaultRegion, ref string) (project, region, name string, err error) {
	ref = compactResourcePath(strings.TrimSpace(ref))
	parts := strings.Split(ref, "/")
	switch {
	case len(parts) == 1 && parts[0] != "":
		return defaultProject, defaultRegion, parts[0], nil
	case len(parts) == 4 && parts[0] == "regions" && parts[2] == "subnetworks":
		return defaultProject, parts[1], parts[3], nil
	case len(parts) == 6 && parts[0] == "projects" && parts[2] == "regions" && parts[4] == "subnetworks":
		return parts[1], parts[3], parts[5], nil
	}
	return "", "", "", fmt.Errorf("spec.gcpConfig.subnetwork %q is not a subnetwork name or a regions/<r>/subnetworks/<n> or projects/<p>/regions/<r>/subnetworks/<n> reference", ref)
}

// checkShape verifies that the machine type (and accelerator) exist in zone.
func checkShape(ctx context.Context, c computeAPI, project, zone, machineType string, acc *mlv1alpha1.GCPAccelerator) error {
	if _, err := c.GetMachineType(ctx, project, zone, machineType); err != nil {
		if IsNotFound(err) {
			return fmt.Errorf("machine type %s is not available in zone %s", machineType, zone)
		}
		return apiErrorf(err, "checking machine type %s in zone %s", machineType, zone)
	}
	if acc != nil && acc.Type != "" {
		if _, err := c.GetAcceleratorType(ctx, project, zone, acc.Type); err != nil {
			if IsNotFound(err) {
				return fmt.Errorf("accelerator type %s is not available in zone %s", acc.Type, zone)
			}
			return apiErrorf(err, "checking accelerator type %s in zone %s", acc.Type, zone)
		}
	}
	return nil
}

// pickZone returns the first UP zone of region (alphabetical) that offers the
// machine type and accelerator.
func pickZone(ctx context.Context, c computeAPI, project, region, machineType string, acc *mlv1alpha1.GCPAccelerator) (string, error) {
	zones, err := c.ListZones(ctx, project, region)
	if err != nil {
		return "", apiErrorf(err, "listing zones of region %s", region)
	}
	var names []string
	for _, z := range zones {
		if z.GetStatus() == "UP" || z.GetStatus() == "" {
			names = append(names, z.GetName())
		}
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no available zone found in region %s", region)
	}
	sort.Strings(names)
	var lastErr error
	for _, z := range names {
		err := checkShape(ctx, c, project, z, machineType, acc)
		if err == nil {
			return z, nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "is not available in zone") {
			return "", err // a real API error, not "this zone lacks it"
		}
	}
	what := "machine type " + machineType
	if acc != nil && acc.Type != "" {
		what += " with accelerator " + acc.Type
	}
	return "", fmt.Errorf("%s is not available in any zone of region %s (last: %v)", what, region, lastErr)
}
