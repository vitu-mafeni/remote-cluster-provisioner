package aws

import (
	"strings"
	"testing"
)

// osVariant: auto must reach the cloud-init script so AWS/GCP nodes resolve
// their own runtime tag, exactly like SSH-provisioned nodes.
func TestRenderBootstrapScript_RuntimeOSVariantAuto(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	p.RuntimeVersion = "1.0.2"
	p.RuntimeOSVariant = "auto"

	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CNLAB_OS_RELEASE_FILE", "CNLAB_OS_TAG=ubuntu22", "CNLAB_OS_TAG=ubuntu20"} {
		if !strings.Contains(script, want) {
			t.Errorf("script is missing %q", want)
		}
	}
	assertBashSyntax(t, script)

	// Without osVariant the script must not look at the OS at all.
	p.RuntimeOSVariant = ""
	plain, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, "CNLAB_OS_TAG") {
		t.Error("osVariant is empty but the script resolves the OS")
	}
}

func TestBuildUserDataE_RuntimeOSVariantValidation(t *testing.T) {
	p := baseParams()
	p.NoVPN = true

	bad := p
	bad.RuntimeVersion = "1.0.2"
	bad.RuntimeOSVariant = "latest"
	if _, err := BuildUserDataE(bad); err == nil {
		t.Fatal("unknown osVariant must be rejected")
	}

	suffixed := p
	suffixed.RuntimeVersion = "1.0.2-ubuntu22"
	suffixed.RuntimeOSVariant = "auto"
	if _, err := BuildUserDataE(suffixed); err == nil {
		t.Fatal("osVariant auto with an already-suffixed version must be rejected")
	}

	ok := p
	ok.RuntimeVersion = "1.0.2"
	ok.RuntimeOSVariant = "auto"
	if _, err := BuildUserDataE(ok); err != nil {
		t.Fatalf("valid auto config rejected: %v", err)
	}
}
