package gcp

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/protobuf/proto"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

const mismatchMsg = "Invalid value for field 'resource.disks[0].initializeParams.diskType': 'projects/proj-1/zones/us-central1-a/diskTypes/pd-balanced'. pd-balanced disk type cannot be used by g4-standard-12 machine type."

// launchEnv provisions a VPN-less node with a fake GCE that offers the machine.
func launchEnv(t *testing.T, mutate func(np *mlv1alpha1.NodeProvision), setup func(f *fakeCompute)) (*fakeCompute, *ProvisionResult, error) {
	t.Helper()
	return launchEnvCtx(t, context.Background(), mutate, setup)
}

func launchEnvCtx(t *testing.T, ctx context.Context, mutate func(np *mlv1alpha1.NodeProvision), setup func(f *fakeCompute)) (*fakeCompute, *ProvisionResult, error) {
	t.Helper()
	np := testNP()
	np.Spec.DisableVPN = true
	if mutate != nil {
		mutate(np)
	}
	f := newFakeCompute()
	f.machines[np.Spec.GCPConfig.Zone+"/"+np.Spec.InstanceType] = true
	if setup != nil {
		setup(f)
	}
	useFake(t, f)
	_, pub, err := testKey()
	if err != nil {
		t.Fatal(err)
	}
	res, err := ProvisionInstance(ctx, np, testCreds(), pub, nil, newProvEnvNetConfig(), pkgruntimeConfig())
	return f, res, err
}

func setType(mt string) func(np *mlv1alpha1.NodeProvision) {
	return func(np *mlv1alpha1.NodeProvision) { np.Spec.InstanceType = mt }
}

func TestSeriesDefaults_DiskNicScheduling(t *testing.T) {
	const pdb, pdssd, hdb = "pd-balanced", "pd-ssd", "hyperdisk-balanced"
	cases := []struct {
		machine, disk, nic, maint string // maint "" = no scheduling block
	}{
		{"n1-standard-8", pdb, "VIRTIO_NET", "MIGRATE"},
		{"n2-standard-4", pdb, "VIRTIO_NET", "MIGRATE"},
		{"n2d-standard-4", pdb, "VIRTIO_NET", "MIGRATE"},
		{"e2-standard-4", pdb, "VIRTIO_NET", "MIGRATE"},
		{"t2d-standard-4", pdb, "VIRTIO_NET", "MIGRATE"},
		{"c2-standard-4", pdb, "VIRTIO_NET", "MIGRATE"},
		{"c2d-standard-4", pdb, "VIRTIO_NET", "MIGRATE"},
		{"m1-megamem-96", pdb, "VIRTIO_NET", "MIGRATE"},
		{"m2-ultramem-208", pdb, "VIRTIO_NET", "MIGRATE"},
		{"m3-ultramem-32", pdb, "GVNIC", "MIGRATE"},
		{"c3-standard-8", pdb, "GVNIC", "MIGRATE"},
		{"c3d-standard-8", pdb, "GVNIC", "MIGRATE"},
		{"h3-standard-88", pdb, "GVNIC", "MIGRATE"},
		{"z3-highmem-14", pdb, "GVNIC", ""},
		{"a2-highgpu-1g", pdb, "VIRTIO_NET", "TERMINATE"},
		{"g2-standard-8", pdb, "VIRTIO_NET", "TERMINATE"},
		{"a3-highgpu-8g", pdb, "GVNIC", "TERMINATE"},
		{"a3-megagpu-8g", pdb, "GVNIC", "TERMINATE"},
		{"a3-ultragpu-8g", hdb, "GVNIC", "TERMINATE"},
		{"a4-highgpu-8g", hdb, "GVNIC", "TERMINATE"},
		{"g4-standard-12", hdb, "GVNIC", "TERMINATE"},
		{"c4-standard-8", hdb, "GVNIC", "MIGRATE"},
		{"c4d-standard-8", hdb, "GVNIC", "MIGRATE"},
		{"n4-standard-8", hdb, "GVNIC", "MIGRATE"},
		{"m4-megamem-56", hdb, "GVNIC", "MIGRATE"},
		{"x4-megamem-960-metal", hdb, "IDPF", "TERMINATE"},
		{"c3-standard-192-metal", hdb, "IDPF", "TERMINATE"},
		{"c4-standard-288-metal", hdb, "IDPF", "TERMINATE"},
		{"c4-standard-288-lssd-metal", hdb, "IDPF", "TERMINATE"},
		{"z3-highmem-192-metal", hdb, "IDPF", "TERMINATE"},
		{"n2-custom-8-32768", pdb, "VIRTIO_NET", "MIGRATE"},
		{"custom-8-30720", pdb, "VIRTIO_NET", "MIGRATE"},
		{"n1-custom-8-30720", pdb, "VIRTIO_NET", "MIGRATE"},
		{"q9-standard-4", pdb, "", ""}, // unknown series: pd-balanced, API defaults
	}
	for _, tc := range cases {
		t.Run(tc.machine, func(t *testing.T) {
			f, _, err := launchEnv(t, setType(tc.machine), nil)
			if err != nil {
				t.Fatal(err)
			}
			inst := f.inserted[0]
			wantURL := "projects/proj-1/zones/us-central1-a/diskTypes/" + tc.disk
			if got := inst.GetDisks()[0].GetInitializeParams().GetDiskType(); got != wantURL {
				t.Errorf("disk type = %q, want %q", got, wantURL)
			}
			if got := inst.GetNetworkInterfaces()[0].GetNicType(); got != tc.nic {
				t.Errorf("nicType = %q, want %q", got, tc.nic)
			}
			if tc.maint == "" {
				if inst.GetScheduling() != nil {
					t.Errorf("scheduling should be left to Google's default: %v", inst.GetScheduling())
				}
			} else if got := inst.GetScheduling().GetOnHostMaintenance(); got != tc.maint {
				t.Errorf("onHostMaintenance = %q, want %q", got, tc.maint)
			}
			if len(inst.GetGuestAccelerators()) != 0 {
				t.Error("no guestAccelerators without spec.gcpConfig.accelerator")
			}
			for _, d := range inst.GetDisks() {
				if d.GetType() != "PERSISTENT" {
					t.Errorf("only the boot disk is requested (local SSD is never added): %v", d)
				}
			}
		})
	}
}

func TestN1WithAcceleratorTerminates(t *testing.T) {
	f, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "n1-standard-8"
		np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 1}
	}, func(f *fakeCompute) { f.accels["us-central1-a/nvidia-tesla-t4"] = true })
	if err != nil {
		t.Fatal(err)
	}
	inst := f.inserted[0]
	if !strings.HasSuffix(inst.GetDisks()[0].GetInitializeParams().GetDiskType(), "/pd-balanced") ||
		inst.GetScheduling().GetOnHostMaintenance() != "TERMINATE" || len(inst.GetGuestAccelerators()) != 1 {
		t.Errorf("n1+t4: %v", inst)
	}
}

func TestBootDiskType_ExplicitWinsAndInvalidIsRefused(t *testing.T) {
	f, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) { np.Spec.GCPConfig.BootDiskType = "pd-ssd" }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.inserted[0].GetDisks()[0].GetInitializeParams().GetDiskType(); got != "projects/proj-1/zones/us-central1-a/diskTypes/pd-ssd" {
		t.Errorf("explicit type must win: %q", got)
	}
	// a full path is accepted too
	f, _, err = launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.GCPConfig.BootDiskType = "zones/us-central1-a/diskTypes/hyperdisk-balanced"
	}, nil)
	if err != nil || !strings.HasSuffix(f.inserted[0].GetDisks()[0].GetInitializeParams().GetDiskType(), "/hyperdisk-balanced") {
		t.Fatalf("full-path type: %v", err)
	}
	// a type the zone does not offer fails with a clear message and no insert
	f, _, err = launchEnv(t, func(np *mlv1alpha1.NodeProvision) { np.Spec.GCPConfig.BootDiskType = "pd-extreme" }, nil)
	if err == nil || !strings.Contains(err.Error(), `bootDiskType "pd-extreme" is not offered in zone us-central1-a`) || !IsPermanent(err) {
		t.Fatalf("want a permanent not-offered error, got %v", err)
	}
	if len(f.inserted) != 0 {
		t.Error("nothing may be inserted")
	}
}

func TestA3HighSmallShapesNeedSpot(t *testing.T) {
	if _, _, err := launchEnv(t, setType("a3-highgpu-2g"), nil); err == nil || !strings.Contains(err.Error(), "Spot") {
		t.Fatalf("on-demand must be refused: %v", err)
	}
	f, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "a3-highgpu-2g"
		np.Spec.GCPConfig.Spot = true
	}, nil)
	if err != nil || f.inserted[0].GetScheduling().GetProvisioningModel() != "SPOT" {
		t.Fatalf("spot: %v", err)
	}
}

func TestSpotRefusedOnBareMetal(t *testing.T) {
	_, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "c3-standard-192-metal"
		np.Spec.GCPConfig.Spot = true
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "bare metal") {
		t.Fatalf("got %v", err)
	}
}

func TestBootDiskType_ZoneWithoutAnyUsableType(t *testing.T) {
	_, _, err := launchEnv(t, setType("c4-standard-8"), func(f *fakeCompute) {
		f.diskTypes = map[string][]*computepb.DiskType{"us-central1-a": {diskTypeFixture("pd-balanced", "10GB-65536GB")}}
	})
	if err == nil || !strings.Contains(err.Error(), "offers none of the boot disk types hyperdisk-balanced") {
		t.Fatalf("got %v", err)
	}
}

func TestBootDiskType_FirstChoiceFollowsTheZoneListing(t *testing.T) {
	// n1 prefers pd-balanced, then pd-ssd: with only pd-ssd offered, pd-ssd wins.
	f, _, err := launchEnv(t, nil, func(f *fakeCompute) {
		f.diskTypes = map[string][]*computepb.DiskType{"us-central1-a": {diskTypeFixture("pd-ssd", "10GB-65536GB")}}
	})
	if err != nil || !strings.HasSuffix(f.inserted[0].GetDisks()[0].GetInitializeParams().GetDiskType(), "/pd-ssd") {
		t.Fatalf("err=%v", err)
	}
}

func TestDiskTypeFallback(t *testing.T) {
	mismatch := gerr(400, "invalid", mismatchMsg)
	// unknown series: first create is rejected, the retry uses the next candidate
	f, res, err := launchEnv(t, setType("q9-standard-4"), func(f *fakeCompute) { f.insertInstanceErrs = []error{mismatch} })
	if err != nil || res.InstanceID == "" {
		t.Fatalf("fallback must succeed: %v", err)
	}
	if len(f.inserted) != 2 {
		t.Fatalf("inserts = %d, want 2", len(f.inserted))
	}
	d0 := f.inserted[0].GetDisks()[0].GetInitializeParams().GetDiskType()
	d1 := f.inserted[1].GetDisks()[0].GetInitializeParams().GetDiskType()
	if !strings.HasSuffix(d0, "/pd-balanced") || !strings.HasSuffix(d1, "/hyperdisk-balanced") {
		t.Errorf("disk types: %s -> %s", d0, d1)
	}
	if f.insertRequestID[0] == f.insertRequestID[1] {
		t.Error("the changed request needs a new requestId (the server replays failed operations)")
	}

	// a second rejection is surfaced, verbatim, and not retried a third time
	f, _, err = launchEnv(t, setType("q9-standard-4"), func(f *fakeCompute) { f.insertInstanceErrs = []error{mismatch, mismatch} })
	if err == nil || !strings.Contains(err.Error(), "cannot be used by g4-standard-12 machine type") {
		t.Fatalf("second failure must be surfaced: %v", err)
	}
	if len(f.inserted) != 2 || !IsPermanent(err) {
		t.Errorf("inserts=%d permanent=%v", len(f.inserted), IsPermanent(err))
	}

	// an explicit bootDiskType is never replaced
	f, _, err = launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "q9-standard-4"
		np.Spec.GCPConfig.BootDiskType = "pd-ssd"
	}, func(f *fakeCompute) { f.insertInstanceErrs = []error{mismatch} })
	if err == nil || len(f.inserted) != 1 {
		t.Fatalf("explicit type: err=%v inserts=%d", err, len(f.inserted))
	}
}

func TestIsDiskTypeMismatch(t *testing.T) {
	for _, m := range []string{mismatchMsg, "Disk type hyperdisk-balanced cannot be used by n1-standard-4 machine type", "pd-ssd disk type is not supported by c4-standard-4 machine type"} {
		if !isDiskTypeMismatch(gerr(400, "invalid", m)) {
			t.Errorf("should match: %s", m)
		}
	}
	for _, m := range []string{"Quota 'CPUS' exceeded", "machine type not found", "disk size too small"} {
		if isDiskTypeMismatch(gerr(400, "invalid", m)) {
			t.Errorf("must not match: %s", m)
		}
	}
}

func TestAcceleratorRules(t *testing.T) {
	g4vws := func(count int32) func(np *mlv1alpha1.NodeProvision) {
		return func(np *mlv1alpha1.NodeProvision) {
			np.Spec.InstanceType = "g4-standard-48"
			np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-rtx-pro-6000-vws", Count: count}
		}
	}
	setup := func(f *fakeCompute) {
		f.accels["us-central1-a/nvidia-rtx-pro-6000-vws"] = true
		f.machineInfo = map[string]*computepb.MachineType{"us-central1-a/g4-standard-48": {
			Name: proto.String("g4-standard-48"), Accelerators: []*computepb.Accelerators{{GuestAcceleratorCount: proto.Int32(1)}}}}
	}
	// accepted, with the machine's GPU count (also when count is left unset)
	for _, count := range []int32{1, 0} {
		f, _, err := launchEnv(t, g4vws(count), setup)
		if err != nil {
			t.Fatalf("count %d: %v", count, err)
		}
		ga := f.inserted[0].GetGuestAccelerators()
		if len(ga) != 1 || ga[0].GetAcceleratorCount() != 1 ||
			ga[0].GetAcceleratorType() != "zones/us-central1-a/acceleratorTypes/nvidia-rtx-pro-6000-vws" {
			t.Errorf("guest accelerators: %v", ga)
		}
		if f.inserted[0].GetScheduling().GetOnHostMaintenance() != "TERMINATE" {
			t.Error("GPU machines terminate on maintenance")
		}
	}
	// count differing from the machine's GPUs is refused
	if _, _, err := launchEnv(t, g4vws(2), setup); err == nil || !strings.Contains(err.Error(), "must equal the machine's GPU count") {
		t.Errorf("count mismatch: %v", err)
	}
	// non-vws accelerators on built-in-GPU machines are refused
	for mt, acc := range map[string]string{"g4-standard-48": "nvidia-tesla-t4", "g2-standard-8": "nvidia-l4", "a3-highgpu-8g": "nvidia-h100-80gb", "g4-standard-12": "nvidia-l4-vws", "a2-highgpu-1g": "nvidia-tesla-a100-vws"} {
		_, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
			np.Spec.InstanceType = mt
			np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: acc, Count: 1}
		}, nil)
		if err == nil || !IsPermanent(err) {
			t.Errorf("%s + %s must be refused permanently, got %v", mt, acc, err)
		}
	}
	// g2 + l4-vws is fine; accelerators on non-N1 CPU series and non-attachable N1 GPUs are not
	f, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "g2-standard-4"
		np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-l4-vws", Count: 1}
	}, func(f *fakeCompute) { f.accels["us-central1-a/nvidia-l4-vws"] = true })
	if err != nil || len(f.inserted[0].GetGuestAccelerators()) != 1 {
		t.Errorf("g2+l4-vws: %v", err)
	}
	for _, mt := range []string{"e2-standard-4", "n2-standard-8", "c3-standard-8"} {
		if _, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
			np.Spec.InstanceType = mt
			np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: "nvidia-tesla-t4", Count: 1}
		}, nil); err == nil {
			t.Errorf("%s + accelerator must be refused", mt)
		}
	}
	for _, acc := range []string{"nvidia-l4", "nvidia-tesla-p100", "nvidia-tesla-k80"} {
		if _, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
			np.Spec.InstanceType = "n1-standard-8"
			np.Spec.GCPConfig.Accelerator = &mlv1alpha1.GCPAccelerator{Type: acc, Count: 1}
		}, nil); err == nil {
			t.Errorf("%s cannot be attached to N1", acc)
		}
	}
}

func TestArmSeriesRejectedEarly(t *testing.T) {
	for _, mt := range []string{"t2a-standard-4", "c4a-standard-4", "a4x-highgpu-4g", "n4a-standard-4"} {
		f, _, err := launchEnv(t, setType(mt), nil)
		if err == nil || !strings.Contains(err.Error(), "Arm") || !IsPermanent(err) || len(f.inserted) != 0 {
			t.Errorf("%s: %v", mt, err)
		}
	}
	// a series the table does not know but the API reports as ARM64
	_, _, err := launchEnv(t, setType("q9-standard-4"), func(f *fakeCompute) {
		f.machineInfo = map[string]*computepb.MachineType{"us-central1-a/q9-standard-4": {Name: proto.String("q9-standard-4"), Architecture: proto.String("ARM64")}}
	})
	if err == nil || !strings.Contains(err.Error(), "Arm") {
		t.Errorf("API-reported ARM64: %v", err)
	}
}

func TestBootSources_OnPDAndHyperdisk(t *testing.T) {
	for _, tc := range []struct{ machine, disk string }{{"e2-standard-4", "pd-balanced"}, {"c4-standard-8", "hyperdisk-balanced"}} {
		for _, snapshot := range []bool{false, true} {
			f, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
				np.Spec.InstanceType = tc.machine
				np.Spec.GCPConfig.BootDiskSizeGB = 100
				if snapshot {
					np.Spec.GCPConfig.SourceImage, np.Spec.GCPConfig.SourceSnapshot = "", "snapshot-1"
				}
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			ip := f.inserted[0].GetDisks()[0].GetInitializeParams()
			if ip.GetDiskType() != "projects/proj-1/zones/us-central1-a/diskTypes/"+tc.disk || ip.GetDiskSizeGb() != 100 {
				t.Errorf("%s snapshot=%v: %v", tc.machine, snapshot, ip)
			}
			if snapshot && (ip.GetSourceSnapshot() != "projects/proj-1/global/snapshots/snapshot-1" || ip.GetSourceImage() != "") {
				t.Errorf("snapshot source: %v", ip)
			}
			if !snapshot && (ip.GetSourceImage() == "" || ip.GetSourceSnapshot() != "") {
				t.Errorf("image source: %v", ip)
			}
		}
	}
}

func TestHyperdiskMinimumSize(t *testing.T) {
	_, _, err := launchEnv(t, func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = "c4-standard-8"
		np.Spec.GCPConfig.BootDiskSizeGB = 50
	}, func(f *fakeCompute) {
		f.diskTypes = map[string][]*computepb.DiskType{"us-central1-a": {diskTypeFixture("hyperdisk-balanced", "100GB-64TB")}}
	})
	if err == nil || !strings.Contains(err.Error(), "bootDiskSizeGB 50 is below the 100 GB minimum of disk type hyperdisk-balanced") {
		t.Fatalf("got %v", err)
	}
	if got := minDiskGB(diskTypeFixture("x", "4GB-64TB")); got != 4 {
		t.Errorf("min = %d", got)
	}
	if got := minDiskGB(diskTypeFixture("x", "1TB-64TB")); got != 1024 {
		t.Errorf("min = %d", got)
	}
	if got := minDiskGB(diskTypeFixture("x", "")); got != 0 {
		t.Errorf("min = %d", got)
	}
}

func TestErrorsAreRetriedOnlyWhenTransient(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		permanent bool
	}{
		{"400 invalid configuration", gerr(400, "invalid", "Invalid value for field 'resource.minCpuPlatform'"), true},
		{"quota", gerr(403, "quotaExceeded", "Quota 'CPUS' exceeded. Limit: 24.0 in region us-central1."), true},
		{"machine type not found in zone", gerr(404, "notFound", "The resource 'zones/z/machineTypes/x' was not found"), true},
		{"503", gerr(503, "backendError", "backend error"), false},
		{"500", gerr(500, "internalError", "internal"), false},
		{"429", gerr(429, "rateLimitExceeded", "slow down"), false},
		{"stockout", gerr(503, "ZONE_RESOURCE_POOL_EXHAUSTED", "The zone does not have enough resources available to fulfill the request."), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, _, err := launchEnv(t, nil, func(f *fakeCompute) { f.insertInstanceErr = tc.err })
			if err == nil {
				t.Fatal("want an error")
			}
			if IsPermanent(err) != tc.permanent {
				t.Errorf("permanent = %v, want %v (%v)", IsPermanent(err), tc.permanent, err)
			}
			if len(f.inserted) != 1 {
				t.Errorf("inserts = %d: a failed insert is never retried in-process", len(f.inserted))
			}
			// Google's text is kept unchanged
			if m := gerrMessage(tc.err); !strings.Contains(err.Error(), m) {
				t.Errorf("message %q lost Google's text %q", err.Error(), m)
			}
		})
	}
}

func gerrMessage(err error) string {
	return err.(interface{ Error() string }).Error()[strings.Index(err.Error(), ": ")+2:]
}

func TestMachineTypeNotInZoneNamesTheZonesThatOfferIt(t *testing.T) {
	np := testNP()
	f, _, err := launchEnv(t, setType("g2-standard-8"), func(f *fakeCompute) {
		f.zones = []*computepb.Zone{zoneUp("us-central1-a"), zoneUp("us-central1-b"), zoneUp("us-central1-c")}
		f.machines["us-central1-b/g2-standard-8"] = true
		delete(f.machines, "us-central1-a/g2-standard-8")
	})
	_ = np
	if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), "g2-standard-8 is not available in zone us-central1-a") ||
		!strings.Contains(err.Error(), "that offer it: us-central1-b") || strings.Contains(err.Error(), "us-central1-c") {
		t.Fatalf("got %v", err)
	}
	if len(f.inserted) != 0 {
		t.Error("nothing may be inserted")
	}
}

func TestBootDiskTypeNameValidation(t *testing.T) {
	spec := testNP().Spec
	spec.GCPConfig.BootDiskType = "PD Balanced!"
	if err := ValidateGCPConfig(spec); err == nil {
		t.Error("an invalid disk type name must be refused")
	}
}

// The 503 GCE returned for g4-standard-12 + nvidia-rtx-pro-6000-vws in
// asia-northeast3-a: stockout error code, but an unsupported configuration.
const unsupportedConfigMsg = `SERVICE UNAVAILABLE: errors:{code:"ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS" error_details:{localized_message:{locale:"en-US" message:"A g4-standard-12 VM instance with 1 nvidia-rtx-pro-6000-vws accelerator(s) is not supported in the asia-northeast3-a zone with your configuration.  Retry your request in a zone that supports the machine type and configuration you require."}} error_details:{error_info:{domain:"compute.googleapis.com" metadatas:{key:"attachment" value:"nvidia-rtx-pro-6000-vws:1"} metadatas:{key:"zonesAvailable" value:"us-central1-b,europe-west4-b"}}}}`

func TestUnsupportedConfigIsNotAStockoutAndIsNotRetried(t *testing.T) {
	gerrUnsupported := gerr(503, "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS", unsupportedConfigMsg)
	if Classify(gerrUnsupported) != KindUnsupportedConfig || IsStockout(gerrUnsupported) {
		t.Fatalf("kind = %v", Classify(gerrUnsupported))
	}
	// a genuine stockout stays a retryable stockout
	if k := Classify(gerr(503, "ZONE_RESOURCE_POOL_EXHAUSTED", "The zone 'z' does not have enough resources available to fulfill the request.")); k != KindStockout {
		t.Errorf("genuine stockout kind = %v", k)
	}
	f, _, err := launchEnv(t, nil, func(f *fakeCompute) { f.insertInstanceErr = gerrUnsupported })
	if err == nil || !IsPermanent(err) || len(f.inserted) != 1 {
		t.Fatalf("err=%v inserts=%d", err, len(f.inserted))
	}
	msg := err.Error()
	if !strings.Contains(msg, "retrying will not help") || !strings.Contains(msg, "Zones that support it: us-central1-b,europe-west4-b") ||
		!strings.Contains(msg, "is not supported in the asia-northeast3-a zone with your configuration") {
		t.Errorf("message must lead with the advice and keep Google's text: %s", msg)
	}
	if strings.Index(msg, "retrying will not help") > 200 {
		t.Error("the actionable part must come first (status messages are truncated)")
	}
}
