package gcp

import (
	"context"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// gpuCase describes one GPU model for the resolution table. Expectations are
// written by hand from the Google docs, not derived from the catalogue.
type gpuCase struct {
	name     string
	accel    string
	attached bool
	count    int32  // a valid count
	wantMT   string // machine type for that count when instanceType is empty
	wrongMT  string // explicit shape of another series
	badCount int32  // count no machine carries / N1 cannot attach
	// wantBadCountSub is text the wrong-count error must contain.
	wantBadCountSub string
	armOnly         bool
}

var gpuCases = []gpuCase{
	{name: "t4", accel: "nvidia-tesla-t4", attached: true, count: 2, wantMT: "n1-standard-8", wrongMT: "a2-highgpu-1g", badCount: 3, wantBadCountSub: "1, 2, 4"},
	{name: "p4", accel: "nvidia-tesla-p4", attached: true, count: 4, wantMT: "n1-standard-8", wrongMT: "g2-standard-4", badCount: 3, wantBadCountSub: "1, 2, 4"},
	{name: "v100", accel: "nvidia-tesla-v100", attached: true, count: 8, wantMT: "n1-standard-8", wrongMT: "n2-standard-8", badCount: 3, wantBadCountSub: "1, 2, 4, 8"},
	{name: "a100-40gb", accel: "nvidia-tesla-a100", count: 4, wantMT: "a2-highgpu-4g", wrongMT: "n2-standard-8", badCount: 3, wantBadCountSub: "a2-highgpu-1g"},
	{name: "a100-80gb", accel: "nvidia-a100-80gb", count: 2, wantMT: "a2-ultragpu-2g", wrongMT: "n2-standard-8", badCount: 3, wantBadCountSub: "a2-ultragpu-4g"},
	{name: "l4", accel: "nvidia-l4", count: 1, wantMT: "g2-standard-4", wrongMT: "n2-standard-8", badCount: 3, wantBadCountSub: "g2-standard-24"},
	{name: "h100-80gb", accel: "nvidia-h100-80gb", count: 8, wantMT: "a3-highgpu-8g", wrongMT: "n2-standard-8", badCount: 3, wantBadCountSub: "a3-highgpu-4g"},
	{name: "h100-mega", accel: "nvidia-h100-mega-80gb", count: 8, wantMT: "a3-megagpu-8g", wrongMT: "n2-standard-8", badCount: 4, wantBadCountSub: "a3-megagpu-8g"},
	{name: "h200", accel: "nvidia-h200-141gb", count: 8, wantMT: "a3-ultragpu-8g", wrongMT: "n2-standard-8", badCount: 4, wantBadCountSub: "a3-ultragpu-8g"},
	{name: "b200", accel: "nvidia-b200", count: 8, wantMT: "a4-highgpu-8g", wrongMT: "n2-standard-8", badCount: 4, wantBadCountSub: "a4-highgpu-8g"},
	{name: "gb200", accel: "nvidia-gb200", count: 4, wantMT: "a4x-highgpu-4g", wrongMT: "n2-standard-8", badCount: 8, wantBadCountSub: "a4x-highgpu-4g", armOnly: true},
	{name: "rtx-pro-6000", accel: "nvidia-rtx-pro-6000", count: 2, wantMT: "g4-standard-96", wrongMT: "n2-standard-8", badCount: 3, wantBadCountSub: "g4-standard-192"},
}

var allGPUTestMachines = []string{
	"n1-standard-8", "n1-highmem-8", "n2-standard-8", "a2-highgpu-1g", "a2-highgpu-4g", "a2-ultragpu-2g", "g2-standard-4", "g2-standard-8",
	"g2-standard-24", "a3-highgpu-8g", "a3-megagpu-8g", "a3-ultragpu-8g", "a4-highgpu-8g", "g4-standard-48", "g4-standard-96",
}

func resolveGPUSpec(t *testing.T, instanceType, accel string, count int32, hw string) (*Resolved, error) {
	t.Helper()
	f := newFakeCompute()
	for _, m := range allGPUTestMachines {
		f.machines["us-central1-a/"+m] = true
	}
	for _, a := range []string{"nvidia-tesla-t4", "nvidia-tesla-p4", "nvidia-tesla-v100", "nvidia-l4-vws", "nvidia-rtx-pro-6000-vws"} {
		f.accels["us-central1-a/"+a] = true
	}
	useFake(t, f)
	return ResolveDefaults(context.Background(), testCreds(), resolveNP(func(np *mlv1alpha1.NodeProvision) {
		np.Spec.InstanceType = instanceType
		np.Spec.HardwareType = hw
		cfg := &mlv1alpha1.GCPConfig{Zone: "us-central1-a"}
		if accel != "" {
			cfg.Accelerator = &mlv1alpha1.GCPAccelerator{Type: accel, Count: count}
		}
		np.Spec.GCPConfig = cfg
	}))
}

func TestResolveDefaults_GPUModels(t *testing.T) {
	for _, g := range gpuCases {
		t.Run(g.name, func(t *testing.T) {
			// instanceType empty; N1 generic default; explicit matching shape.
			for _, row := range []struct{ label, instanceType string }{
				{"empty", ""},
				{"n1-default", "n1-standard-8"},
				{"matching", g.wantMT},
			} {
				res, err := resolveGPUSpec(t, row.instanceType, g.accel, g.count, "")
				if g.armOnly {
					if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), "Arm") {
						t.Errorf("%s: want a permanent Arm error, got %v", row.label, err)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s: %v", row.label, err)
					continue
				}
				if res.InstanceType != g.wantMT {
					t.Errorf("%s: machine type %q, want %q", row.label, res.InstanceType, g.wantMT)
				}
				if g.attached {
					if a := res.Config.Accelerator; a == nil || a.Type != g.accel || a.Count != g.count {
						t.Errorf("%s: attached GPU must stay attached, got %+v", row.label, a)
					}
					if res.Message != "" {
						t.Errorf("%s: nothing was substituted, message %q", row.label, res.Message)
					}
					continue
				}
				if res.Config.Accelerator != nil {
					t.Errorf("%s: built-in GPU must clear the accelerator, got %+v", row.label, res.Config.Accelerator)
				}
				if res.Message == "" || !strings.Contains(res.Message, g.wantMT) {
					t.Errorf("%s: substitution must be recorded naming %s, got %q", row.label, g.wantMT, res.Message)
				}
			}

			// Explicit shape of another series: never rewritten, names the fix.
			spec := g.wrongMT
			res, err := resolveGPUSpec(t, spec, g.accel, g.count, "")
			if err == nil || !IsPermanent(err) {
				t.Fatalf("wrong series %s must fail permanently, got res=%+v err=%v", spec, res, err)
			}
			if res != nil {
				t.Errorf("no result may be produced for a conflict")
			}
			if !strings.Contains(err.Error(), g.wantMT) && !g.attached && !g.armOnly {
				t.Errorf("error must name the right machine type %s: %v", g.wantMT, err)
			}
			if g.attached && !strings.Contains(err.Error(), "N1") {
				t.Errorf("attached GPU error must point at N1: %v", err)
			}

			// Wrong count.
			_, err = resolveGPUSpec(t, "", g.accel, g.badCount, "")
			if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), g.wantBadCountSub) {
				t.Errorf("count %d: want permanent error containing %q, got %v", g.badCount, g.wantBadCountSub, err)
			}
		})
	}
}

// The explicit shape is the user's: a conflict is an error even when it is an N1
// shape other than the generic default, and a count that disagrees with an
// explicit built-in shape names the machine that matches.
func TestResolveDefaults_GPUExplicitConflictsAreNeverRewritten(t *testing.T) {
	_, err := resolveGPUSpec(t, "n1-highmem-8", "nvidia-h100-mega-80gb", 8, "")
	if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), "a3-megagpu-8g") {
		t.Errorf("explicit N1 + H100 Mega: %v", err)
	}
	_, err = resolveGPUSpec(t, "a2-highgpu-1g", "nvidia-tesla-a100", 4, "")
	if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), "a2-highgpu-4g") {
		t.Errorf("explicit a2-highgpu-1g with count 4: %v", err)
	}
	// The failure that motivated this: N1 default + H100 Mega. Count defaults to 1
	// and H100 Mega only exists as 8 GPUs, so the error lists the valid shape.
	res, err := resolveGPUSpec(t, "n1-standard-8", "nvidia-h100-mega-80gb", 0, "")
	if err == nil || !strings.Contains(err.Error(), "a3-megagpu-8g") {
		t.Errorf("1 x H100 Mega must fail listing a3-megagpu-8g, got %+v %v", res, err)
	}
	res, err = resolveGPUSpec(t, "n1-standard-8", "nvidia-h100-mega-80gb", 8, "")
	if err != nil || res.InstanceType != "a3-megagpu-8g" || res.Config.Accelerator != nil {
		t.Errorf("N1 default + 8 x H100 Mega: %+v %v", res, err)
	}
}

func TestResolveDefaults_GPUVWSKeepsLicenceAndSelectsMachine(t *testing.T) {
	for _, c := range []struct {
		accel, instanceType string
		count               int32
		wantMT              string
	}{
		{"nvidia-l4-vws", "", 1, "g2-standard-4"},
		{"nvidia-l4-vws", "n1-standard-8", 1, "g2-standard-4"},
		{"nvidia-l4-vws", "g2-standard-8", 0, "g2-standard-8"},
		{"nvidia-rtx-pro-6000-vws", "", 1, "g4-standard-48"},
	} {
		res, err := resolveGPUSpec(t, c.instanceType, c.accel, c.count, "")
		if err != nil {
			t.Errorf("%+v: %v", c, err)
			continue
		}
		if res.InstanceType != c.wantMT || res.Config.Accelerator == nil || res.Config.Accelerator.Type != c.accel {
			t.Errorf("%+v: got %q %+v", c, res.InstanceType, res.Config.Accelerator)
		}
	}
	// A non-vws L4 name never attaches, and a -vws name on the wrong series fails.
	if res, err := resolveGPUSpec(t, "g2-standard-8", "nvidia-l4", 1, ""); err != nil || res.Config.Accelerator != nil {
		t.Errorf("non-vws name with matching shape: %+v %v", res, err)
	}
	if _, err := resolveGPUSpec(t, "a3-megagpu-8g", "nvidia-l4-vws", 1, ""); err == nil {
		t.Error("L4 licence on an a3 machine must fail")
	}
}

func TestResolveDefaults_GPUHardwareTypeWithoutAnythingIsAnError(t *testing.T) {
	_, err := resolveGPUSpec(t, "", "", 0, "gpu")
	if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), "spec.gcpConfig.accelerator") {
		t.Errorf("hardwareType gpu with no accelerator and no instanceType: %v", err)
	}
	// An explicit machine type is the user's call (existing behaviour).
	if _, err := resolveGPUSpec(t, "g2-standard-4", "", 0, "gpu"); err != nil {
		t.Errorf("explicit machine type with hardwareType gpu: %v", err)
	}
}

func TestNeedsGPUResolution(t *testing.T) {
	acc := func(ty string, n int32) *mlv1alpha1.GCPAccelerator {
		return &mlv1alpha1.GCPAccelerator{Type: ty, Count: n}
	}
	for _, c := range []struct {
		mt   string
		a    *mlv1alpha1.GCPAccelerator
		want bool
	}{
		{"n1-standard-8", nil, false},
		{"n1-standard-8", acc("nvidia-tesla-t4", 1), false},
		{"n1-standard-8", acc("nvidia-h100-mega-80gb", 8), true}, // rewrite
		{"n2-standard-8", acc("nvidia-l4", 1), true},             // error
		{"g2-standard-4", acc("nvidia-l4", 1), true},             // accelerator cleared
		{"g2-standard-4", acc("nvidia-l4-vws", 1), false},
	} {
		if got := NeedsGPUResolution(c.mt, c.a); got != c.want {
			t.Errorf("%s %+v: got %v want %v", c.mt, c.a, got, c.want)
		}
	}
}
