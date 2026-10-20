package onprem

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
)

func TestInsecureRegistryHosts_MergesExplicitAndImagePrepulls(t *testing.T) {
	sw := mlv1alpha1.SoftwareConfig{
		InsecureRegistries: []string{"zeta.local:5000", "harbor.example.com:30002"},
		ImagePrepulls: []mlv1alpha1.ImagePrepull{
			{Image: "harbor.example.com:30002/team/a:1"},
			{Image: "10.0.0.5:8080/b:2"},
			{Image: "library/nginx"},
		},
	}
	got, err := InsecureRegistryHosts(sw)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.5:8080", "harbor.example.com:30002", "zeta.local:5000"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hosts = %v, want %v", got, want)
	}
	if got, err := InsecureRegistryHosts(mlv1alpha1.SoftwareConfig{}); err != nil || len(got) != 0 {
		t.Errorf("empty config -> no hosts, got %v, %v", got, err)
	}
	if _, err := InsecureRegistryHosts(mlv1alpha1.SoftwareConfig{InsecureRegistries: []string{"https://x"}}); err == nil {
		t.Error("an invalid explicit entry must be an error")
	}
}

func TestWithInsecureRegistries_AppendsAfterInstallAndIsNoOpWhenEmpty(t *testing.T) {
	steps := pkgruntime.InstallSteps(pkgruntime.Config{})
	n := len(steps)
	if got := withInsecureRegistries(append([]string(nil), steps...), nil); len(got) != n {
		t.Errorf("no hosts must add no step, got %d steps (was %d)", len(got), n)
	}
	got := withInsecureRegistries(append([]string(nil), steps...), []string{"harbor.example.com:30002"})
	if len(got) != n+1 {
		t.Fatalf("want one extra step, got %d (was %d)", len(got), n)
	}
	last := got[len(got)-1]
	for _, w := range []string{"sudo mkdir -p '/etc/containers/registries.conf.d'", `location = "harbor.example.com:30002"`, "insecure = true"} {
		if !strings.Contains(last, w) {
			t.Errorf("drop-in step missing %q:\n%s", w, last)
		}
	}
}

// An invalid explicit entry must be rejected before any host is touched.
func TestNewInClusterProvisioner_InvalidInsecureRegistryFailsBeforeAnyStep(t *testing.T) {
	f := flowFake(t, filepath.Join(t.TempDir(), "kubelet.conf"))
	np, nc := flowInputs(false)
	nc.Spec.SoftwareConfig.InsecureRegistries = []string{"http://harbor.example.com:30002"}

	var steps []string
	_, _, err := NewInClusterProvisioner(context.Background(), np, nil, f.client, nil, nc, func(s string) { steps = append(steps, s) }, pkgruntime.Config{})
	if err == nil || !strings.Contains(err.Error(), "insecureRegistries[0]") {
		t.Fatalf("expected an insecureRegistries error, got %v", err)
	}
	if len(steps) != 0 || f.Log("sudo.calls") != "" {
		t.Errorf("nothing may run, steps=%v calls=%q", steps, f.Log("sudo.calls"))
	}
}
