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
	"log"
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
	if cfg.SourceSnapshot != "" {
		if cfg.SourceImage != "" || cfg.ImageFamily != "" || cfg.ImageProject != "" {
			return fmt.Errorf("spec.gcpConfig.sourceSnapshot cannot be combined with sourceImage, imageFamily or imageProject: choose one boot source")
		}
		if _, _, err := ParseSnapshotRef(cfg.ProjectID, cfg.SourceSnapshot); err != nil {
			return err
		}
	}
	if cfg.SourceImage != "" {
		if _, _, _, err := ParseImageRef(cfg.ProjectID, cfg.SourceImage); err != nil {
			return err
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

	// ── boot source: image or snapshot ────────────────────────────────────────
	minDiskGB, err := resolveBootSource(ctx, c, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.BootDiskSizeGB != 0 && int64(cfg.BootDiskSizeGB) < minDiskGB {
		return nil, fmt.Errorf("spec.gcpConfig.bootDiskSizeGB %d is smaller than the boot source (%d GB)", cfg.BootDiskSizeGB, minDiskGB)
	}
	if cfg.BootDiskSizeGB == 0 && minDiskGB > DefaultBootDiskGB {
		cfg.BootDiskSizeGB = int32(minDiskGB) // unset: grow to fit rather than fail
	}
	return res, nil
}

// resolveBootSource validates the boot snapshot, or the boot image (resolving
// the default family into cfg.SourceImage when none is set), and returns the
// smallest boot disk in GB it fits on.
func resolveBootSource(ctx context.Context, c computeAPI, cfg *mlv1alpha1.GCPConfig) (int64, error) {
	if cfg.SourceSnapshot != "" {
		project, name, err := ParseSnapshotRef(cfg.ProjectID, cfg.SourceSnapshot)
		if err != nil {
			return 0, err
		}
		sn, err := c.GetSnapshot(ctx, project, name)
		if err != nil {
			if IsNotFound(err) {
				return 0, fmt.Errorf("snapshot %q not found in project %q: check spec.gcpConfig.sourceSnapshot (and that the service account can read it)", name, project)
			}
			return 0, apiErrorf(err, "looking up snapshot %q", name)
		}
		if st := sn.GetStatus(); st != "" && st != computepb.Snapshot_READY.String() {
			return 0, fmt.Errorf("snapshot %q is not ready (status %s)", name, st)
		}
		if hasWindowsLicense(sn.GetLicenses()) {
			return 0, fmt.Errorf("snapshot %q is of a Windows disk; nodes need an Ubuntu Linux boot disk", name)
		}
		return sn.GetDiskSizeGb(), nil
	}

	var (
		img    *computepb.Image
		err    error
		what   string
		family bool
	)
	if cfg.SourceImage == "" {
		fam, project := cfg.ImageFamily, cfg.ImageProject
		if fam == "" {
			fam = DefaultImageFamily
		}
		if project == "" {
			project = DefaultImageProject
		}
		what, family = fmt.Sprintf("%s/%s", project, fam), true
		img, err = c.GetImageFromFamily(ctx, project, fam)
	} else {
		var project, name string
		project, name, family, err = ParseImageRef(defaultImageProject(cfg), cfg.SourceImage)
		if err != nil {
			return 0, err
		}
		what = fmt.Sprintf("%s/%s", project, name)
		if family {
			img, err = c.GetImageFromFamily(ctx, project, name)
		} else {
			img, err = c.GetImage(ctx, project, name)
		}
	}
	kind := "image"
	if family {
		kind = "image family"
	}
	if err != nil {
		if IsNotFound(err) {
			return 0, fmt.Errorf("%s %q not found: check spec.gcpConfig.sourceImage/imageFamily/imageProject (and that the service account can read it)", kind, what)
		}
		return 0, apiErrorf(err, "looking up %s %s", kind, what)
	}
	if st := img.GetStatus(); st != "" && st != computepb.Image_READY.String() {
		return 0, fmt.Errorf("%s %s is not ready (status %s)", kind, what, st)
	}
	switch img.GetDeprecated().GetState() {
	case computepb.DeprecationStatus_DELETED.String(), computepb.DeprecationStatus_OBSOLETE.String():
		return 0, fmt.Errorf("%s %s is %s and can no longer be used", kind, what, img.GetDeprecated().GetState())
	case computepb.DeprecationStatus_DEPRECATED.String():
		log.Printf("[WARN] GCP %s %s is deprecated; consider a newer image", kind, what)
	}
	if a := img.GetArchitecture(); a != "" && a != computepb.Image_X86_64.String() {
		return 0, fmt.Errorf("%s %s is a %s image; only X86_64 is supported", kind, what, a)
	}
	if hasWindowsLicense(img.GetLicenses()) || hasWindowsFeature(img.GetGuestOsFeatures()) {
		return 0, fmt.Errorf("%s %s is a Windows image; nodes need an Ubuntu Linux image", kind, what)
	}
	if cfg.SourceImage == "" {
		cfg.SourceImage = compactResourcePath(img.GetSelfLink())
		if cfg.SourceImage == "" {
			return 0, fmt.Errorf("%s %s has no selfLink", kind, what)
		}
	}
	return img.GetDiskSizeGb(), nil
}

func hasWindowsLicense(licenses []string) bool {
	for _, l := range licenses {
		if strings.Contains(strings.ToLower(l), "windows") {
			return true
		}
	}
	return false
}

func hasWindowsFeature(fs []*computepb.GuestOsFeature) bool {
	for _, f := range fs {
		if strings.EqualFold(f.GetType(), "WINDOWS") {
			return true
		}
	}
	return false
}

// defaultImageProject is where a bare image name is looked up: spec imageProject,
// else the instance's own project (custom images usually live there).
func defaultImageProject(cfg *mlv1alpha1.GCPConfig) string {
	if cfg.ImageProject != "" {
		return cfg.ImageProject
	}
	return cfg.ProjectID
}

// ParseImageRef splits an image reference into (project, name, isFamily). ref may
// be a bare image name (defaultProject), "global/images/<n>", "global/images/family/<f>",
// "projects/<p>/global/images/<n>", "projects/<p>/global/images/family/<f>" or
// the full URL of any of them.
func ParseImageRef(defaultProject, ref string) (project, name string, family bool, err error) {
	ref = compactResourcePath(strings.TrimSpace(ref))
	parts := strings.Split(ref, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		project, parts = parts[1], parts[2:]
	} else {
		project = defaultProject
	}
	switch {
	case len(parts) == 1 && parts[0] != "" && project != "":
		return project, parts[0], false, nil
	case len(parts) == 3 && parts[0] == "global" && parts[1] == "images" && parts[2] != "":
		return project, parts[2], false, nil
	case len(parts) == 4 && parts[0] == "global" && parts[1] == "images" && parts[2] == "family" && parts[3] != "":
		return project, parts[3], true, nil
	}
	return "", "", false, fmt.Errorf("spec.gcpConfig.sourceImage %q is not an image name, global/images/<n>, projects/<p>/global/images/<n> or projects/<p>/global/images/family/<f> reference", ref)
}

// ImageResource returns the canonical "projects/<p>/global/images[/family]/<n>"
// path of spec.gcpConfig.sourceImage for an instance of project.
func ImageResource(project string, cfg *mlv1alpha1.GCPConfig) (string, error) {
	p := cfg.ProjectID
	if p == "" {
		p = project
	}
	imgProject, name, family, err := ParseImageRef(firstNonEmpty(cfg.ImageProject, p), cfg.SourceImage)
	if err != nil {
		return "", err
	}
	if family {
		return fmt.Sprintf("projects/%s/global/images/family/%s", imgProject, name), nil
	}
	return fmt.Sprintf("projects/%s/global/images/%s", imgProject, name), nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ParseSnapshotRef splits a snapshot reference into (project, name). ref may be a
// bare name (defaultProject), "global/snapshots/<n>", "projects/<p>/global/snapshots/<n>"
// or the full URL.
func ParseSnapshotRef(defaultProject, ref string) (project, name string, err error) {
	ref = compactResourcePath(strings.TrimSpace(ref))
	parts := strings.Split(ref, "/")
	project = defaultProject
	if len(parts) >= 2 && parts[0] == "projects" {
		project, parts = parts[1], parts[2:]
	}
	switch {
	case len(parts) == 1 && parts[0] != "" && project != "":
		return project, parts[0], nil
	case len(parts) == 3 && parts[0] == "global" && parts[1] == "snapshots" && parts[2] != "":
		return project, parts[2], nil
	}
	return "", "", fmt.Errorf("spec.gcpConfig.sourceSnapshot %q is not a snapshot name, global/snapshots/<n> or projects/<p>/global/snapshots/<n> reference", ref)
}

// SnapshotResource returns the canonical "projects/<p>/global/snapshots/<n>" path.
func SnapshotResource(project string, cfg *mlv1alpha1.GCPConfig) (string, error) {
	p := cfg.ProjectID
	if p == "" {
		p = project
	}
	sp, name, err := ParseSnapshotRef(p, cfg.SourceSnapshot)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("projects/%s/global/snapshots/%s", sp, name), nil
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
