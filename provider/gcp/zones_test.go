package gcp

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/protobuf/proto"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

const (
	zA, zB, zC = "us-central1-a", "us-central1-b", "us-central1-c"
	// the capacity error GCE returns
	capacityMsg = "A e2-standard-4 VM instance is currently unavailable in the us-central1-a zone. The zone does not have enough resources available to fulfill the request."
)

func capacityErr() error { return gerr(503, "ZONE_RESOURCE_POOL_EXHAUSTED", capacityMsg) }

func fallbackOn(np *mlv1alpha1.NodeProvision) {
	t := true
	np.Spec.GCPConfig.ZoneFallback = &t
}

// threeZones makes a, b and c UP and offering the machine type.
func threeZones(f *fakeCompute) {
	f.zones = []*computepb.Zone{zoneUp(zA), zoneUp(zB), zoneUp(zC)}
	for _, z := range []string{zA, zB, zC} {
		f.machines[z+"/e2-standard-4"] = true
	}
}

func TestZoneFallback_ExplicitZoneExhaustedUsesTheNextZoneAndCleansTheFirst(t *testing.T) {
	var hooked []string
	ctx := WithZoneExhaustedHook(context.Background(), func(z string) { hooked = append(hooked, z) })
	f, res, err := launchEnvCtx(t, ctx, fallbackOn, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
		f.leftoverOnFail = true
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.insertedZones, ",") != zA+","+zB || res.Zone != zB || res.InstanceID != "worker-1" {
		t.Fatalf("zones tried %v, result %+v", f.insertedZones, res)
	}
	if len(f.disks) != 0 || len(f.deletedDisks) != 1 || f.deletedDisks[0] != zA+"/worker-1" {
		t.Errorf("the first zone's boot disk must be deleted: disks=%v deleted=%v", f.disks, f.deletedDisks)
	}
	if _, ok := f.instances[zB+"/worker-1"]; !ok {
		t.Error("instance must exist in the second zone")
	}
	if len(hooked) != 1 || hooked[0] != zA {
		t.Errorf("the exhausted zone must be reported to the caller: %v", hooked)
	}
	if f.insertRequestID[0] == f.insertRequestID[1] {
		t.Error("each zone's request needs its own requestId")
	}
	// the zone-independent resources are created once
	if len(f.insertedFW) == 0 || len(f.insertedFW) > 3 {
		t.Errorf("firewall rules must not be duplicated per zone: %d", len(f.insertedFW))
	}
}

func TestZoneFallback_CleansAHalfCreatedInstanceToo(t *testing.T) {
	f, _, err := launchEnv(t, fallbackOn, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
	})
	if err != nil {
		t.Fatal(err)
	}
	// a half-created instance of ours in zone a (visible for a moment after the failed insert)
	np := testNP()
	f2 := f
	f2.instances[zA+"/worker-1"] = ownedInstance(np, "PROVISIONING")
	if err := cleanupZoneAttempt(context.Background(), f2, "proj-1", zA, "worker-1", testUID); err != nil {
		t.Fatal(err)
	}
	if _, ok := f2.instances[zA+"/worker-1"]; ok {
		t.Error("the half-created instance must be deleted")
	}
	// something that is not ours is never touched
	foreign := ownedInstance(np, "RUNNING")
	foreign.Labels = map[string]string{LabelUID: "someone-else"}
	f2.instances[zC+"/worker-1"] = foreign
	f2.disks = map[string]*computepb.Disk{zC + "/worker-1": {Name: proto.String("worker-1"), Labels: map[string]string{LabelUID: "someone-else"}}}
	if err := cleanupZoneAttempt(context.Background(), f2, "proj-1", zC, "worker-1", testUID); err != nil {
		t.Fatal(err)
	}
	if _, ok := f2.instances[zC+"/worker-1"]; !ok || len(f2.disks) != 1 {
		t.Error("foreign resources must be left alone")
	}
}

func TestZoneFallback_RegionOnlyCandidatesOfferMachineAndAccelerator(t *testing.T) {
	f, res, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		fallbackOn(np)
		np.Spec.InstanceType = "n1-standard-8"
		np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 1}
	}, func(f *fakeCompute) {
		f.zones = []*computepb.Zone{zoneUp(zC), zoneUp(zA), zoneUp(zB), {Name: proto.String("us-central1-f"), Status: proto.String("DOWN")}}
		for _, z := range []string{zA, zB, zC, "us-central1-f"} {
			f.machines[z+"/n1-standard-8"] = true
		}
		f.accels[zA+"/nvidia-tesla-t4"] = true // c has the machine but no T4; f is DOWN
		f.accels[zB+"/nvidia-tesla-t4"] = true
		f.accels["us-central1-f/nvidia-tesla-t4"] = true
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.insertedZones, ",") != zA+","+zB || res.Zone != zB {
		t.Errorf("candidates must be a then b only (c lacks the T4, f is down): %v", f.insertedZones)
	}
}

func TestZoneFallback_AcceleratorCountLimitIsHonoured(t *testing.T) {
	// zone b only allows 2 T4 per instance, so it is not a candidate for 4
	f2, _, err2 := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		fallbackOn(np)
		np.Spec.InstanceType = "n1-standard-8"
		np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 4}
	}, func(f *fakeCompute) {
		f.zones = []*computepb.Zone{zoneUp(zA), zoneUp(zB)}
		f.machines[zA+"/n1-standard-8"], f.machines[zB+"/n1-standard-8"] = true, true
		f.accels[zA+"/nvidia-tesla-t4"], f.accels[zB+"/nvidia-tesla-t4"] = true, true
		f.accelInfo = map[string]*computepb.AcceleratorType{zB + "/nvidia-tesla-t4": {Name: proto.String("nvidia-tesla-t4"), MaximumCardsPerInstance: proto.Int32(2)}}
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
	})
	if err2 == nil || !IsNoCapacity(err2) || strings.Join(f2.insertedZones, ",") != zA {
		t.Fatalf("zone b must be skipped (max 2 per instance): zones=%v err=%v", f2.insertedZones, err2)
	}
}

func TestZoneFallback_NoCommonZoneFailsFastWithoutInsert(t *testing.T) {
	f := newFakeCompute()
	f.zones = []*computepb.Zone{zoneUp(zA), zoneUp(zB)}
	f.machines[zA+"/n1-standard-8"] = true
	f.accels[zB+"/nvidia-tesla-t4"] = true // machine and GPU never meet
	useFake(t, f)
	np := resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "n1-standard-8"
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Accelerator: &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 1}}
	})
	_, err := ResolveDefaults(context.Background(), testCreds(), np)
	if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), "is not available in any zone of region us-central1") {
		t.Fatalf("got %v", err)
	}
	if len(f.inserted) != 0 {
		t.Error("no insert may happen")
	}
}

func TestResolveDefaults_RegionOnlyEnablesZoneFallbackButAnExplicitZoneDoesNot(t *testing.T) {
	f := newFakeCompute()
	f.zones = []*computepb.Zone{zoneUp(zA), zoneUp(zB)}
	f.machines[zA+"/e2-standard-4"], f.machines[zB+"/e2-standard-4"] = true, true
	useFake(t, f)
	res, err := ResolveDefaults(context.Background(), testCreds(), resolveNP(nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.Config.Zone != zA || res.Config.ZoneFallback == nil || !*res.Config.ZoneFallback {
		t.Errorf("a region-only spec must default zoneFallback to true: %+v", res.Config.ZoneFallback)
	}
	res, err = ResolveDefaults(context.Background(), testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{Zone: zB}
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Config.ZoneFallback != nil {
		t.Errorf("an explicit zone must stay without fallback: %v", *res.Config.ZoneFallback)
	}
	// an explicit user choice is kept
	f2 := false
	res, err = ResolveDefaults(context.Background(), testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.GCPConfig = &mlv1alpha1.GCPConfig{ZoneFallback: &f2}
	}))
	if err != nil || res.Config.ZoneFallback == nil || *res.Config.ZoneFallback {
		t.Errorf("zoneFallback=false must be kept: %v %v", err, res.Config.ZoneFallback)
	}
}

func TestZoneFallback_NonCapacityErrorsAreNotRetriedInOtherZones(t *testing.T) {
	for name, e := range map[string]error{
		"invalid configuration": gerr(400, "invalid", "Invalid value for field 'resource.machineType'"),
		"quota":                 gerr(403, "quotaExceeded", "Quota 'CPUS' exceeded. Limit: 24.0 in region us-central1."),
		"permission":            gerr(403, "forbidden", "Required 'compute.instances.create' permission"),
		"disk type":             gerr(400, "invalid", mismatchMsg),
	} {
		t.Run(name, func(t *testing.T) {
			f, _, err := launchEnv(t, fallbackOn, func(f *fakeCompute) {
				threeZones(f)
				f.zoneInsertErrs = map[string]error{zA: e}
			})
			if err == nil || IsNoCapacity(err) {
				t.Fatalf("want a plain failure, got %v", err)
			}
			if got := strings.Join(f.insertedZones, ","); !strings.HasPrefix(got, zA) || strings.Contains(got, zB) || strings.Contains(got, zC) {
				t.Errorf("no other zone may be tried: %v", f.insertedZones)
			}
			if !strings.Contains(err.Error(), gerrText(e)) {
				t.Errorf("Google's text must be kept: %v", err)
			}
		})
	}
}

func gerrText(err error) string { return err.Error()[strings.Index(err.Error(), ": ")+2:] }

func TestZoneFallback_AllZonesExhaustedIsRetryableAndListsTheZones(t *testing.T) {
	var hooked []string
	ctx := WithZoneExhaustedHook(context.Background(), func(z string) { hooked = append(hooked, z) })
	f, _, err := launchEnvCtx(t, ctx, fallbackOn, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: capacityErr(), zB: capacityErr(), zC: capacityErr()}
	})
	if err == nil || !IsNoCapacity(err) {
		t.Fatalf("got %v", err)
	}
	if IsPermanent(err) {
		t.Error("no capacity is retryable: a later pass may find capacity")
	}
	msg := err.Error()
	for _, want := range []string{"no capacity for e2-standard-4 in us-central1-a, us-central1-b, us-central1-c", "tried all zones of us-central1", "Try again later or use another region"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message lacks %q: %s", want, msg)
		}
	}
	if len(f.insertedZones) != 3 || len(hooked) != 3 {
		t.Errorf("zones tried %v, reported %v", f.insertedZones, hooked)
	}
	// with an accelerator the message names it
	_, _, err = launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "g4-standard-48"
		np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-rtx-pro-6000-vws", Count: 1}
	}, func(f *fakeCompute) {
		f.accels[zA+"/nvidia-rtx-pro-6000-vws"] = true
		f.machineInfo = map[string]*computepb.MachineType{zA + "/g4-standard-48": {Name: proto.String("g4-standard-48"), Accelerators: []*computepb.Accelerators{{GuestAcceleratorCount: proto.Int32(1)}}}}
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
	})
	if err == nil || !strings.Contains(err.Error(), "no capacity for g4-standard-48 (+ nvidia-rtx-pro-6000-vws) in us-central1-a") {
		t.Errorf("got %v", err)
	}
}

func TestZoneFallback_DisabledWithAnExplicitZoneTriesOnlyThatZone(t *testing.T) {
	f, _, err := launchEnv(t, nil, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
	})
	if err == nil || !IsNoCapacity(err) || !strings.Contains(err.Error(), "zoneFallback is not enabled") {
		t.Fatalf("got %v", err)
	}
	if strings.Join(f.insertedZones, ",") != zA {
		t.Errorf("only the named zone may be tried: %v", f.insertedZones)
	}
	// and an explicit false behaves the same
	f, _, err = launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		off := false
		np.Spec.GCPConfig.ZoneFallback = &off
	}, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
	})
	if err == nil || len(f.insertedZones) != 1 {
		t.Errorf("zoneFallback=false: %v %v", err, f.insertedZones)
	}
}

func TestZoneFallback_ZonesRecordedAsExhaustedAreNotRetriedAfterARestart(t *testing.T) {
	// a restart after zone a was recorded: the pass continues with b
	f, res, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		fallbackOn(np)
		np.Status.ExhaustedZones = []string{zA}
	}, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: capacityErr()} // would fail again if retried
	})
	if err != nil || strings.Join(f.insertedZones, ",") != zB || res.Zone != zB {
		t.Fatalf("zones=%v res=%+v err=%v", f.insertedZones, res, err)
	}
	// every candidate already recorded: nothing is inserted, the pass fails as no capacity
	f, _, err = launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		fallbackOn(np)
		np.Status.ExhaustedZones = []string{zA, zB, zC}
	}, threeZones)
	if err == nil || !IsNoCapacity(err) || len(f.inserted) != 0 {
		t.Fatalf("err=%v inserts=%d", err, len(f.inserted))
	}
	if !strings.Contains(err.Error(), "us-central1-a, us-central1-b, us-central1-c") {
		t.Errorf("the recorded zones must be listed: %v", err)
	}
	// the exhausted zone of a pass that did not use fallback is skipped too
	f, _, err = launchEnv(t, func(np *mlv1alpha1.NodeProvision) { np.Status.ExhaustedZones = []string{zA} }, threeZones)
	if err == nil || !IsNoCapacity(err) || len(f.inserted) != 0 {
		t.Errorf("err=%v inserts=%d", err, len(f.inserted))
	}
}

func TestZoneFallback_UnsupportedHereTriesTheNextZoneButAllUnsupportedIsPermanent(t *testing.T) {
	unsupported := gerr(503, "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS", unsupportedConfigMsg)
	f, res, err := launchEnv(t, fallbackOn, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: unsupported}
	})
	if err != nil || res.Zone != zB || len(f.insertedZones) != 2 {
		t.Fatalf("a zone that does not support the configuration is skipped: %v %v", f.insertedZones, err)
	}
	_, _, err = launchEnv(t, fallbackOn, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: unsupported, zB: unsupported, zC: unsupported}
	})
	if err == nil || !IsPermanent(err) || IsNoCapacity(err) || !strings.Contains(err.Error(), "retrying will not help") {
		t.Errorf("every zone unsupported is not a capacity problem: %v", err)
	}
}

func TestZoneFallback_CleanupFailureStopsThePass(t *testing.T) {
	f, _, err := launchEnv(t, fallbackOn, func(f *fakeCompute) {
		threeZones(f)
		f.zoneInsertErrs = map[string]error{zA: capacityErr()}
		f.leftoverOnFail = true
		f.disks = map[string]*computepb.Disk{}
		f.deleteDiskErr = gerr(500, "backendError", "boom")
	})
	if err == nil || !strings.Contains(err.Error(), "cleaning up zone us-central1-a") {
		t.Fatalf("got %v", err)
	}
	if strings.Join(f.insertedZones, ",") != zA {
		t.Errorf("no further zone may be used while an orphan remains: %v", f.insertedZones)
	}
}

func TestZoneFallback_AdoptionAndDeletionFindAnInstanceInAnotherZone(t *testing.T) {
	np := testNP()
	// adoption by ProvisionInstance
	f, res, err := launchEnv(t, fallbackOn, func(f *fakeCompute) {
		threeZones(f)
		f.put(zB, ownedInstance(testNP(), "RUNNING"))
	})
	if err != nil || !res.Adopted || res.Zone != zB || len(f.inserted) != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	// FindInstance reports the zone
	useFake(t, f)
	fnp := testNP()
	fallbackOn(fnp)
	if id, zone, err := FindInstance(context.Background(), fnp, testCreds()); err != nil || id != "worker-1" || zone != zB {
		t.Errorf("FindInstance = %q %q %v", id, zone, err)
	}
	// without fallback only the named zone is searched
	if id, _, _ := FindInstance(context.Background(), np, testCreds()); id != "" {
		t.Errorf("must not look beyond the named zone: %q", id)
	}
	// the recorded zone wins and is used by DeleteInstance
	fnp.Status.Zone = zB
	if err := DeleteInstance(context.Background(), fnp, testCreds(), "worker-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.instances[zB+"/worker-1"]; ok {
		t.Error("the instance in the recorded zone must be deleted")
	}
	if zoneOf(fnp) != zB {
		t.Error("zoneOf must follow status.zone")
	}
}
