package gcp

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/compute/apiv1/computepb"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// This file is the single place that knows how GCE machine series differ:
// which boot disk types they accept, which NIC type they need, whether they can
// live-migrate and whether their GPUs are built in. Everything else (instance
// body, zone checks, fallbacks) asks the table.

// Boot disk type names.
const (
	diskPDBalanced         = "pd-balanced"
	diskPDSSD              = "pd-ssd"
	diskHyperdiskBalanced  = "hyperdisk-balanced"
	nicGVNIC               = "GVNIC"
	nicIDPF                = "IDPF"
	nicVirtio              = "VIRTIO_NET"
	vwsSuffix              = "-vws"
	hostMaintenanceMigrate = "MIGRATE"
	hostMaintenanceTerm    = "TERMINATE"
)

// seriesInfo describes one machine series.
type seriesInfo struct {
	// BootDisks is the ordered boot-disk-type preference: the first one the zone
	// offers wins.
	BootDisks []string
	// GVNIC is true when the series requires nicType GVNIC.
	GVNIC bool
	// BuiltinGPU is true when the machine type includes its GPUs (guestAccelerators
	// is then only used for a "-vws" workstation licence).
	BuiltinGPU bool
	// LiveMigrate is true when onHostMaintenance=MIGRATE is supported. GPU
	// machines and the other series that cannot live-migrate use TERMINATE.
	LiveMigrate bool
	// IDPF is true when the series needs nicType IDPF (bare metal).
	IDPF bool
	// SchedulingUnset leaves scheduling to Google's default for the series: the
	// docs do not say unambiguously whether it can live-migrate (h3, z3).
	SchedulingUnset bool
	// VWS is the one "-vws" workstation licence type valid with the series' built-in GPUs
	// ("" = none: a2/a3/a4 do not support vWS).
	VWS string
	// Arm marks Arm series (unsupported: the image and bootstrap are x86_64).
	Arm bool
}

var (
	// pdOnly: older series. Hyperdisk Balanced is listed for some of them only
	// behind "contact your account team" and the docs say to use Persistent Disk
	// for boot disks on 1st/2nd-generation machine series.
	pdOnly = []string{diskPDBalanced, diskPDSSD}
	// pdFirst: Persistent Disk and Hyperdisk Balanced are both supported.
	pdFirst = []string{diskPDBalanced, diskPDSSD, diskHyperdiskBalanced}
	// hyperdiskVal: Hyperdisk-only series.
	hyperdiskVal = []string{diskHyperdiskBalanced}
	// unknownSeries is used for series the table does not know (Google adds new
	// ones): try the common types; the create-time fallback covers the rest.
	unknownSeries = seriesInfo{BootDisks: []string{diskPDBalanced, diskHyperdiskBalanced}}
)

// seriesTable is keyed by seriesKey(machineType). Sources: Google Cloud docs
// (general-purpose, compute-/memory-/storage-/accelerator-optimized machines,
// machine-resource, hyperdisks, using-gvnic, host-maintenance-overview,
// setting-vm-host-options), checked 2026-11-10; see docs/controllers-user-guide.md.
var seriesTable = map[string]seriesInfo{
	"n1":  {BootDisks: pdOnly, LiveMigrate: true},
	"n2":  {BootDisks: pdOnly, LiveMigrate: true},
	"n2d": {BootDisks: pdOnly, LiveMigrate: true},
	"e2":  {BootDisks: pdOnly, LiveMigrate: true},
	"t2d": {BootDisks: pdOnly, LiveMigrate: true},
	"c2":  {BootDisks: pdOnly, LiveMigrate: true},
	"c2d": {BootDisks: pdOnly, LiveMigrate: true},
	"m1":  {BootDisks: pdFirst, LiveMigrate: true},
	"m2":  {BootDisks: pdFirst, LiveMigrate: true},
	"m3":  {BootDisks: pdFirst, GVNIC: true, LiveMigrate: true},
	"c3":  {BootDisks: pdFirst, GVNIC: true, LiveMigrate: true},
	"c3d": {BootDisks: pdFirst, GVNIC: true, LiveMigrate: true},
	// H3 offers no pd-ssd; it live-migrates.
	"h3": {BootDisks: []string{diskPDBalanced, diskHyperdiskBalanced}, GVNIC: true, LiveMigrate: true},
	// Z3 live-migrates only up to 18,000 GiB of Titanium SSD and TERMINATE cannot be
	// forced on the smaller shapes: leave scheduling to Google.
	"z3": {BootDisks: pdFirst, GVNIC: true, SchedulingUnset: true},

	"c4":  {BootDisks: hyperdiskVal, GVNIC: true, LiveMigrate: true},
	"c4d": {BootDisks: hyperdiskVal, GVNIC: true, LiveMigrate: true},
	"n4":  {BootDisks: hyperdiskVal, GVNIC: true, LiveMigrate: true},
	"m4":  {BootDisks: hyperdiskVal, GVNIC: true, LiveMigrate: true},
	// X4 is bare metal only: IDPF, no live migration.
	"x4": {BootDisks: hyperdiskVal, IDPF: true},

	// GPU series: cannot live-migrate.
	"a2":          {BootDisks: pdOnly, BuiltinGPU: true},
	"g2":          {BootDisks: pdOnly, BuiltinGPU: true, VWS: "nvidia-l4-vws"},
	"a3":          {BootDisks: []string{diskPDBalanced, diskPDSSD, diskHyperdiskBalanced}, GVNIC: true, BuiltinGPU: true},
	"a3-ultragpu": {BootDisks: hyperdiskVal, GVNIC: true, BuiltinGPU: true},
	"a4":          {BootDisks: hyperdiskVal, GVNIC: true, BuiltinGPU: true},
	"g4":          {BootDisks: hyperdiskVal, GVNIC: true, BuiltinGPU: true, VWS: "nvidia-rtx-pro-6000-vws"},

	// Arm series: rejected early.
	"t2a": {Arm: true},
	"c4a": {Arm: true},
	"n4a": {Arm: true},
	"a4x": {Arm: true},
}

// isMetal reports a bare metal machine type ("c3-standard-192-metal", ...).
// Bare metal instances need nicType IDPF, Hyperdisk Balanced boot disks and
// onHostMaintenance=TERMINATE, and cannot be Spot.
func isMetal(machineType string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(machineType)), "-metal")
}

// seriesKey is the table key of a machine type: the part before the first "-",
// except that "custom-*" shapes are N1 and a3-ultragpu is its own series.
func seriesKey(machineType string) string {
	fam := MachineFamily(machineType)
	if fam == "a3" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(machineType)), "a3-ultragpu") {
		return "a3-ultragpu"
	}
	return fam
}

// lookupSeries returns the table entry, and false for an unknown series.
func lookupSeries(machineType string) (seriesInfo, bool) {
	s, ok := seriesTable[seriesKey(machineType)]
	if !ok {
		s = unknownSeries
	}
	if isMetal(machineType) && !s.Arm {
		s.BootDisks, s.GVNIC, s.IDPF, s.LiveMigrate, s.SchedulingUnset = hyperdiskVal, false, true, false, false
	}
	return s, ok
}

// nicTypeFor is the nicType of the instance's interface: GVNIC where the series
// requires it, VIRTIO_NET otherwise; "" (API default) for an unknown series.
func nicTypeFor(machineType string) string {
	s, known := lookupSeries(machineType)
	switch {
	case s.IDPF:
		return nicIDPF
	case s.GVNIC:
		return nicGVNIC
	case known:
		return nicVirtio
	}
	return ""
}

// genericDiskFallbacks is the order tried when creation is rejected because the
// disk type does not suit the machine type.
var genericDiskFallbacks = []string{diskPDBalanced, diskHyperdiskBalanced, diskPDSSD}

// diskTypeMismatchRE matches the 400 GCE returns when the boot disk type is not
// supported by the machine type, e.g. "pd-balanced disk type cannot be used by
// g4-standard-12 machine type".
var diskTypeMismatchRE = regexp.MustCompile(`(?is)disk\s*type.*(cannot be used by|can not be used by|is not supported by|not supported for|not compatible with).*machine\s*type`)

// isDiskTypeMismatch reports whether err is the disk-type/machine-type mismatch.
func isDiskTypeMismatch(err error) bool {
	return err != nil && diskTypeMismatchRE.MatchString(err.Error())
}

// bootDiskCandidates returns the boot disk types to use, best first, restricted
// to those the zone offers. reason explains the choice for the log.
func bootDiskCandidates(machineType, explicit string, offered map[string]*computepb.DiskType, zone string) (cands []string, reason string, err error) {
	if explicit != "" {
		name := diskTypeName(explicit)
		if _, ok := offered[name]; !ok {
			return nil, "", Permanent(fmt.Errorf("spec.gcpConfig.bootDiskType %q is not offered in zone %s (offered: %s)",
				explicit, zone, strings.Join(sortedKeys(offered), ", ")))
		}
		return []string{name}, "explicitly set by spec.gcpConfig.bootDiskType", nil
	}
	s, known := lookupSeries(machineType)
	for _, d := range s.BootDisks {
		if _, ok := offered[d]; ok {
			cands = append(cands, d)
		}
	}
	if len(cands) == 0 {
		return nil, "", Permanent(fmt.Errorf("zone %s offers none of the boot disk types %s that machine type %s can use (offered: %s); set spec.gcpConfig.bootDiskType or pick another zone",
			zone, strings.Join(s.BootDisks, ", "), machineType, strings.Join(sortedKeys(offered), ", ")))
	}
	if known {
		reason = fmt.Sprintf("series %s prefers %s and the zone offers %s", seriesKey(machineType), strings.Join(s.BootDisks, " > "), cands[0])
	} else {
		reason = fmt.Sprintf("series %s is not in the series table; default preference %s, zone offers %s", seriesKey(machineType), strings.Join(s.BootDisks, " > "), cands[0])
	}
	return cands, reason, nil
}

// nextDiskFallback returns the first generic fallback type that was not tried
// and is offered in the zone ("" when none is left).
func nextDiskFallback(tried map[string]bool, offered map[string]*computepb.DiskType) string {
	for _, d := range genericDiskFallbacks {
		if _, ok := offered[d]; ok && !tried[d] {
			return d
		}
	}
	return ""
}

// diskTypeName reduces "zones/z/diskTypes/pd-ssd" or a full URL to "pd-ssd".
func diskTypeName(ref string) string {
	ref = strings.TrimSpace(ref)
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		return ref[i+1:]
	}
	return ref
}

// diskTypeURL is the full resource path used in initializeParams.diskType.
func diskTypeURL(project, zone, diskType string) string {
	return fmt.Sprintf("projects/%s/zones/%s/diskTypes/%s", project, zone, diskType)
}

func sortedKeys(m map[string]*computepb.DiskType) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// small, so a simple insertion sort keeps this file dependency-free
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

var validSizeRE = regexp.MustCompile(`^\s*(\d+)\s*(GB|TB)`)

// minDiskGB parses the lower bound of a DiskType.validDiskSize such as
// "10GB-65536GB" or "4GB-64TB"; 0 when it cannot be read.
func minDiskGB(d *computepb.DiskType) int64 {
	m := validSizeRE.FindStringSubmatch(d.GetValidDiskSize())
	if m == nil {
		return 0
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0
	}
	if m[2] == "TB" {
		n *= 1024
	}
	return n
}

// ── accelerators ────────────────────────────────────────────────────────────

// n1AttachableGPUs are the GPU types the docs list as attachable to an N1
// instance via guestAccelerators (gpus/about-gpus, GPU platforms), including the
// workstation "-vws" variants. P100/K80 no longer appear in the current docs.
var n1AttachableGPUs = map[string]bool{
	"nvidia-tesla-t4": true, "nvidia-tesla-t4-vws": true,
	"nvidia-tesla-p4": true, "nvidia-tesla-p4-vws": true,
	"nvidia-tesla-v100": true,
}

// spotOnlyMachine reports machine types that cannot be created on-demand:
// "When provisioning a3-highgpu-1g, a3-highgpu-2g, or a3-highgpu-4g machine
// types, you must create instances by using Spot VMs or Flex-start VMs."
func spotOnlyMachine(machineType string) bool {
	switch strings.ToLower(strings.TrimSpace(machineType)) {
	case "a3-highgpu-1g", "a3-highgpu-2g", "a3-highgpu-4g":
		return true
	}
	return false
}

// validateAccelerator checks the accelerator/machine-type combination without
// any API call. GPUs are attached only to N1; for built-in-GPU series the only
// accepted accelerator is a "-vws" workstation licence.
func validateAccelerator(machineType string, a *mlv1alpha1.GCPAccelerator) error {
	if a == nil || a.Type == "" {
		return nil
	}
	s, _ := lookupSeries(machineType)
	if a.Count < 0 || a.Count > maxAccelerators {
		return fmt.Errorf("spec.gcpConfig.accelerator.count must be between 1 and %d", maxAccelerators)
	}
	switch {
	case s.BuiltinGPU:
		if s.VWS != "" && a.Type != s.VWS {
			return fmt.Errorf("machine type %q already includes its GPUs; the only accelerator accepted with it is its workstation licence %q, not %q — leave spec.gcpConfig.accelerator unset for the built-in GPUs",
				machineType, s.VWS, a.Type)
		}
		if s.VWS == "" {
			return fmt.Errorf("machine type %q already includes its GPUs and has no %q workstation licence; leave spec.gcpConfig.accelerator unset (got %q)", machineType, vwsSuffix, a.Type)
		}
		if !strings.HasSuffix(a.Type, vwsSuffix) {
			return fmt.Errorf("machine type %q already includes its GPUs; spec.gcpConfig.accelerator may only name a %q workstation licence type (e.g. nvidia-l4-vws on g2, nvidia-rtx-pro-6000-vws on g4), not %q — leave it unset for the built-in GPUs",
				machineType, "<gpu>"+vwsSuffix, a.Type)
		}
	case seriesKey(machineType) == "n1":
		if !n1AttachableGPUs[a.Type] {
			return fmt.Errorf("accelerator type %q cannot be attached to an N1 machine type (attachable: nvidia-tesla-t4, nvidia-tesla-p4, nvidia-tesla-v100 and the -vws variants of t4/p4)", a.Type)
		}
	default:
		return fmt.Errorf("spec.gcpConfig.accelerator is only supported on N1 machine types (GPU attach) or as a %q licence on g2/g4/a-series, not on %q", vwsSuffix, machineType)
	}
	return nil
}

// builtinGPUCount is the GPU count of a machine type as reported by the API
// (0 when it does not report any).
func builtinGPUCount(mt *computepb.MachineType) int32 {
	var n int32
	for _, a := range mt.GetAccelerators() {
		n += a.GetGuestAcceleratorCount()
	}
	return n
}
