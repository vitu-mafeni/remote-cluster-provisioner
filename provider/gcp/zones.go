package gcp

import (
	"context"
	"fmt"
	"sort"
	"strings"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// ── zone fallback ───────────────────────────────────────────────────────────
//
// A zone that lists a machine type can still lack the capacity for it
// (ZONE_RESOURCE_POOL_EXHAUSTED...). With spec.gcpConfig.zoneFallback the
// provider then tries the other zones of the region that offer the machine type
// (and accelerator). Only capacity errors fall back; every other error stays a
// failure of the attempt. Subnetwork, firewall rules and the VPN peer are
// regional / zone-independent and are created once.

type ctxKey int

const exhaustedHookKey ctxKey = 1

// WithZoneExhaustedHook returns a context whose ProvisionInstance calls fn(zone)
// each time a zone turned out to have no capacity, so the caller can persist it
// (it is then skipped by a pass resumed after a restart).
func WithZoneExhaustedHook(ctx context.Context, fn func(zone string)) context.Context {
	return context.WithValue(ctx, exhaustedHookKey, fn)
}

func notifyZoneExhausted(ctx context.Context, zone string) {
	if fn, ok := ctx.Value(exhaustedHookKey).(func(string)); ok && fn != nil {
		fn(zone)
	}
}

// fallbackEnabled reports whether other zones may be tried. The default is false:
// an explicit zone is honoured (ResolveDefaults records true when it chose the
// zone itself because only a region was given).
func fallbackEnabled(cfg *mlv1alpha1.GCPConfig) bool {
	return cfg != nil && cfg.ZoneFallback != nil && *cfg.ZoneFallback
}

// zoneOffers reports whether zone offers the machine type and, when set, the
// accelerator type with the requested count (N1 attach limit).
func zoneOffers(ctx context.Context, c computeAPI, project, zone string, np *mlv1alpha1.NodeProvision) (bool, error) {
	if _, err := c.GetMachineType(ctx, project, zone, np.Spec.InstanceType); err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, apiErrorf(err, "checking machine type %s in zone %s", np.Spec.InstanceType, zone)
	}
	if a := np.Spec.GCPConfig.Accelerator; a != nil && a.Type != "" {
		at, err := c.GetAcceleratorType(ctx, project, zone, a.Type)
		if err != nil {
			if IsNotFound(err) {
				return false, nil
			}
			return false, apiErrorf(err, "checking accelerator type %s in zone %s", a.Type, zone)
		}
		if !HasImpliedGPU(np.Spec.InstanceType) {
			if max := at.GetMaximumCardsPerInstance(); max > 0 && a.Count > max {
				return false, nil
			}
		}
	}
	return true, nil
}

// regionZones lists the zones of the region (alphabetical), without the named zone.
func regionZones(ctx context.Context, c computeAPI, project, named string, upOnly bool) ([]string, error) {
	region, ok := RegionOfZone(named)
	if !ok {
		return nil, nil
	}
	zones, err := c.ListZones(ctx, project, region)
	if err != nil {
		return nil, apiErrorf(err, "listing zones of region %s", region)
	}
	var out []string
	for _, z := range zones {
		if z.GetName() == named || (upOnly && z.GetStatus() != "UP" && z.GetStatus() != "") {
			continue
		}
		out = append(out, z.GetName())
	}
	sort.Strings(out)
	return out, nil
}

// zoneCandidates returns the zones to try, in a stable order: the named zone
// first, then (with zoneFallback) the other UP zones of the region that offer
// the machine type and accelerator, alphabetically. Zones already recorded as
// exhausted are left out.
func zoneCandidates(ctx context.Context, c computeAPI, project string, np *mlv1alpha1.NodeProvision, exhausted map[string]bool) ([]string, error) {
	cfg := np.Spec.GCPConfig
	var out []string
	if !exhausted[cfg.Zone] {
		out = append(out, cfg.Zone)
	}
	if !fallbackEnabled(cfg) {
		return out, nil
	}
	others, err := regionZones(ctx, c, project, cfg.Zone, true)
	if err != nil {
		return nil, err
	}
	for _, z := range others {
		if exhausted[z] {
			continue
		}
		ok, err := zoneOffers(ctx, c, project, z, np)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, z)
		}
	}
	return out, nil
}

func exhaustedSet(np *mlv1alpha1.NodeProvision) map[string]bool {
	m := map[string]bool{}
	for _, z := range np.Status.ExhaustedZones {
		m[z] = true
	}
	return m
}

// NoCapacityError is returned when every candidate zone lacked capacity. It is
// retryable (a later pass may find capacity) unless every zone said the
// configuration is not supported there, in which case it is also marked Permanent.
type NoCapacityError struct {
	Machine, Accelerator, Region string
	// Zones are all the zones that were tried, in order.
	Zones []string
	// Fallback is whether other zones of the region were eligible.
	Fallback bool
	// Last is the last Google error.
	Last error
}

func (e *NoCapacityError) Unwrap() error { return e.Last }

func (e *NoCapacityError) Error() string {
	what := e.Machine
	if e.Accelerator != "" {
		what += " (+ " + e.Accelerator + ")"
	}
	scope := "tried only that zone (spec.gcpConfig.zoneFallback is not enabled)"
	if e.Fallback {
		scope = fmt.Sprintf("tried all zones of %s that offer it. Try again later or use another region", e.Region)
	}
	msg := fmt.Sprintf("no capacity for %s in %s; %s", what, strings.Join(e.Zones, ", "), scope)
	if e.Last != nil {
		msg += fmt.Sprintf(" (last error: %s)", Describe(e.Last))
	}
	return msg
}

// IsNoCapacity reports whether err is a NoCapacityError (every zone exhausted),
// as opposed to a configuration error.
func IsNoCapacity(err error) bool {
	var nc *NoCapacityError
	return asNoCapacity(err, &nc)
}

// ── per-zone cleanup ────────────────────────────────────────────────────────

// cleanupZoneAttempt removes what a failed insert may have left in zone: a
// half-created instance of this NodeProvision (with its auto-delete boot disk)
// and, if only the disk survived, the disk carrying its UID label. Resources
// that belong to something else are never touched.
func cleanupZoneAttempt(ctx context.Context, c computeAPI, project, zone, name, uid string) error {
	inst, owned, err := lookupInstance(ctx, c, project, zone, name, uid)
	if err != nil {
		return fmt.Errorf("looking up instance %s in %s: %w", name, zone, err)
	}
	if inst != nil && owned {
		if err := c.DeleteInstance(ctx, project, zone, name); err != nil && !IsNotFound(err) {
			return fmt.Errorf("deleting half-created instance %s in %s: %w", name, zone, err)
		}
	}
	d, err := c.GetDisk(ctx, project, zone, name)
	if err != nil {
		if IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("looking up boot disk %s in %s: %w", name, zone, err)
	}
	if d.GetLabels()[LabelUID] != sanitizeLabelValue(uid) {
		return nil
	}
	if err := c.DeleteDisk(ctx, project, zone, name); err != nil && !IsNotFound(err) {
		return fmt.Errorf("deleting orphaned boot disk %s in %s: %w", name, zone, err)
	}
	return nil
}

// instanceSearchZones are the zones an instance of np may live in: the recorded
// one, else the named zone and, with zoneFallback, the other zones of the region.
func instanceSearchZones(ctx context.Context, c computeAPI, project string, np *mlv1alpha1.NodeProvision) ([]string, error) {
	if np.Status.Zone != "" {
		return []string{np.Status.Zone}, nil
	}
	cfg := np.Spec.GCPConfig
	if cfg == nil || cfg.Zone == "" {
		return nil, nil
	}
	zones := []string{cfg.Zone}
	if fallbackEnabled(cfg) {
		others, err := regionZones(ctx, c, project, cfg.Zone, false)
		if err != nil {
			return nil, err
		}
		zones = append(zones, others...)
	}
	return zones, nil
}
