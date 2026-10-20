package gcp

import (
	"context"
	"strings"
	"testing"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

func TestBuildStartupScript_InsecureRegistries(t *testing.T) {
	p := startupNP(true, false)
	p.InsecureRegistries = []string{"harbor.example.com:30002"}
	script, err := BuildStartupScript(p)
	if err != nil {
		t.Fatal(err)
	}
	assertBashSyntax(t, script)
	for _, w := range []string{
		"mkdir -p '/etc/containers/registries.conf.d'",
		"| tee '/etc/containers/registries.conf.d/50-insecure-harbor-example-com-30002.conf' > /dev/null",
		"location = \"harbor.example.com:30002\"",
		"insecure = true",
	} {
		if !strings.Contains(script, w) {
			t.Errorf("startup script missing %q", w)
		}
	}
	if d, r := strings.Index(script, "registries.conf.d"), strings.Index(script, "systemctl enable crio\nrestart_crio_and_wait"); d < 0 || r < 0 || d > r {
		t.Errorf("drop-in must precede the CRI-O restart (drop=%d restart=%d)", d, r)
	}

	p.InsecureRegistries = []string{"https://bad"}
	if _, err := BuildStartupScript(p); err == nil {
		t.Error("an invalid registry must fail the build")
	}

	none, err := BuildStartupScript(startupNP(true, false))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(none, "registries.conf.d") {
		t.Error("no registries -> no drop-in")
	}
}

func TestProvisionInstance_InsecureRegistriesReachStartupScript(t *testing.T) {
	e := newProvEnv(t)
	e.np.Spec.DisableVPN = true
	e.nc.Spec.VPNRange = nil
	e.nc.Spec.VPNServerPublicConfig.PublicIP = ""
	e.nc.Spec.SoftwareConfig.InsecureRegistries = []string{"zeta.local:5000"}
	e.nc.Spec.SoftwareConfig.ImagePrepulls = []mlv1alpha1.ImagePrepull{{Image: "harbor.example.com:30002/team/img:1"}}

	if _, err := ProvisionInstance(context.Background(), e.np, testCreds(), e.pubKey, nil, e.nc, pkgruntimeConfig()); err != nil {
		t.Fatal(err)
	}
	script, _ := metaValue(e.gce.inserted[0], "startup-script")
	for _, h := range []string{"zeta.local:5000", "harbor.example.com:30002"} {
		if !strings.Contains(script, `location = "`+h+`"`) {
			t.Errorf("startup script lacks the drop-in for %s", h)
		}
	}

	bad := newProvEnv(t)
	bad.np.Spec.DisableVPN = true
	bad.nc.Spec.SoftwareConfig.InsecureRegistries = []string{"harbor/with/path"}
	if _, err := ProvisionInstance(context.Background(), bad.np, testCreds(), bad.pubKey, nil, bad.nc, pkgruntimeConfig()); err == nil {
		t.Fatal("an invalid insecureRegistries entry must fail the launch")
	}
	if bad.gce.getInstanceN != 0 || len(bad.gce.inserted) != 0 || len(bad.gce.insertedFW) != 0 {
		t.Error("validation must fail before any GCP call")
	}
}
