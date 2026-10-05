package aws

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"strings"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
)

func TestRenderBootstrapScript_NoInsecureRegistriesIsUnchanged(t *testing.T) {
	for _, noVPN := range []bool{true, false} {
		p := baseParams()
		p.NoVPN = noVPN
		if !noVPN {
			p.WGConfig, p.VpnIP = "[Interface]\n", "10.8.0.5"
		}
		script, err := renderBootstrapScript(p)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(script, "registries.conf.d") || strings.Contains(script, "CNLAB_REG_EOF") {
			t.Errorf("noVPN=%v: no registries configured -> no drop-in in the script", noVPN)
		}
		// The blank-line layout around the (absent) block is the pre-feature one.
		if !strings.Contains(script, "/opt/cni/bin /etc/criu\n\nif [ ! -f /etc/crictl.yaml ]") {
			t.Errorf("noVPN=%v: script layout changed for an empty registry list", noVPN)
		}
	}
}

func TestRenderBootstrapScript_InsecureRegistriesWrittenBeforeCRIOStarts(t *testing.T) {
	p := baseParams()
	p.NoVPN = true
	p.InsecureRegistries = []string{"harbor.example.com:30002", "reg.local"}
	if err := validateParams(p); err != nil {
		t.Fatal(err)
	}
	script, err := renderBootstrapScript(p)
	if err != nil {
		t.Fatal(err)
	}
	assertBashSyntax(t, script)

	for _, w := range []string{
		"mkdir -p '/etc/containers/registries.conf.d'",
		"| tee '/etc/containers/registries.conf.d/50-insecure-harbor-example-com-30002.conf' > /dev/null",
		"[[registry]]\nlocation = \"harbor.example.com:30002\"\ninsecure = true\nCNLAB_REG_EOF",
		"| tee '/etc/containers/registries.conf.d/50-insecure-reg-local.conf' > /dev/null",
	} {
		if !strings.Contains(script, w) {
			t.Errorf("script missing %q", w)
		}
	}
	if strings.Contains(script, "sudo tee") || strings.Contains(script, "sudo mkdir") {
		t.Error("cloud-init runs as root: the drop-in writer must not depend on sudo")
	}

	drop := strings.Index(script, "registries.conf.d")
	restart := strings.Index(script, "restart_crio_and_wait\n")
	// The first *call* of restart_crio_and_wait (the function definition is earlier).
	call := strings.Index(script, "systemctl enable crio\nrestart_crio_and_wait")
	if drop < 0 || call < 0 || drop > call {
		t.Errorf("drop-ins must be written before CRI-O is (re)started (drop=%d, restart call=%d, any=%d)", drop, call, restart)
	}
	// ...and before the kubelet/kubeadm steps that pull images.
	if k := strings.Index(script, "Configuring Kubernetes apt repository"); drop > k {
		t.Error("drop-ins must precede the Kubernetes install")
	}
}

func TestValidateParams_RejectsBadInsecureRegistries(t *testing.T) {
	for _, bad := range []string{"http://harbor", "harbor/path", "ha rbor", "a';touch /x;'", "harbor:99999"} {
		p := baseParams()
		p.NoVPN = true
		p.InsecureRegistries = []string{bad}
		if err := validateParams(p); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
		if _, err := BuildUserDataE(p); err == nil {
			t.Errorf("%q must not produce user-data", bad)
		}
	}
}

// The NodeProvisionNetConfig's explicit list and its imagePrepulls hosts both
// reach the launched instance's user-data; an invalid entry fails before any
// AWS or VPN call.
func TestProvisionEC2Node_InsecureRegistriesReachUserData(t *testing.T) {
	e := newProvEnv(t)
	e.np.Spec.DisableVPN = true
	e.nc.Spec.VPNRange = nil
	e.nc.Spec.VPNServerPublicConfig.PublicIP = ""
	e.nc.Spec.SoftwareConfig.InsecureRegistries = []string{"zeta.local:5000"}
	e.nc.Spec.SoftwareConfig.ImagePrepulls = []mlv1alpha1.ImagePrepull{{Image: "harbor.example.com:30002/team/img:1"}}

	if _, err := ProvisionEC2Node(context.Background(), e.np, AWSCredentials{}, nil, e.nc, pkgruntime.Config{}); err != nil {
		t.Fatal(err)
	}
	ud, err := base64.StdEncoding.DecodeString(awssdk.ToString(e.ec2.runInputs[0].UserData))
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(ud))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"zeta.local:5000", "harbor.example.com:30002"} {
		if !strings.Contains(string(raw), `location = "`+h+`"`) {
			t.Errorf("user-data lacks the drop-in for %s", h)
		}
	}

	bad := newProvEnv(t)
	bad.np.Spec.DisableVPN = true
	bad.nc.Spec.SoftwareConfig.InsecureRegistries = []string{"http://harbor"}
	if _, err := ProvisionEC2Node(context.Background(), bad.np, AWSCredentials{}, nil, bad.nc, pkgruntime.Config{}); err == nil {
		t.Fatal("an invalid insecureRegistries entry must fail the launch")
	}
	if len(bad.ec2.runInputs) != 0 || bad.ec2.describes != 0 || bad.Log("wg.calls") != "" {
		t.Error("validation must fail before any AWS or VPN call")
	}
}
