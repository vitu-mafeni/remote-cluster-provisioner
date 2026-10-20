package controller

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

const samplesDir = "../../config/samples"

// remoteClusterSamples decodes every document of a sample file strictly: a
// renamed or removed API field must break this test, not a user's `kubectl
// apply`. It returns the RemoteClusters and the names of the Secrets defined.
func remoteClusterSamples(t *testing.T, file string) (rcs []infrav1.RemoteCluster, secrets map[string]corev1.Secret) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(samplesDir, file))
	if os.IsNotExist(err) {
		t.Skip("config/samples is not part of this build context")
	}
	if err != nil {
		t.Fatal(err)
	}
	secrets = map[string]corev1.Secret{}
	for _, doc := range strings.Split(string(raw), "\n---") {
		switch {
		case strings.Contains(doc, "\nkind: RemoteCluster\n"):
			var rc infrav1.RemoteCluster
			if err := yaml.UnmarshalStrict([]byte(doc), &rc); err != nil {
				t.Fatalf("%s: RemoteCluster does not decode strictly: %v\n%s", file, err, doc)
			}
			rcs = append(rcs, rc)
		case strings.Contains(doc, "\nkind: Secret\n"):
			var s corev1.Secret
			if err := yaml.UnmarshalStrict([]byte(doc), &s); err != nil {
				t.Fatalf("%s: Secret does not decode strictly: %v\n%s", file, err, doc)
			}
			secrets[s.Name] = s
		}
	}
	return rcs, secrets
}

func sampleAuthSecretName(rc infrav1.RemoteCluster) string {
	switch {
	case rc.Spec.Auth.PasswordSecretRef != nil:
		return rc.Spec.Auth.PasswordSecretRef.Name
	case rc.Spec.Auth.SSHPrivateKeySecretRef != nil:
		return rc.Spec.Auth.SSHPrivateKeySecretRef.Name
	}
	return ""
}

// checkSampleClusterFile verifies the rules the sample comments promise.
func checkSampleClusterFile(t *testing.T, file string, wantDisableVPN bool) {
	t.Helper()
	rcs, secrets := remoteClusterSamples(t, file)

	var cps, workers int
	clusterName := ""
	for _, rc := range rcs {
		sp := rc.Spec
		if clusterName == "" {
			clusterName = sp.ClusterName
		}
		if sp.ClusterName != clusterName {
			t.Errorf("%s/%s: clusterName %q != %q — workers must share the control-plane's clusterName", file, rc.Name, sp.ClusterName, clusterName)
		}
		if sp.NodeInfo.HardwareType != "cpu" && sp.NodeInfo.HardwareType != "gpu" {
			t.Errorf("%s/%s: hardwareType %q", file, rc.Name, sp.NodeInfo.HardwareType)
		}

		// every referenced Secret must be defined in the same file (a sample must apply as-is)
		if n := sampleAuthSecretName(rc); n == "" {
			t.Errorf("%s/%s: no SSH auth reference", file, rc.Name)
		} else if _, ok := secrets[n]; !ok {
			t.Errorf("%s/%s: auth Secret %q is not defined in the sample", file, rc.Name, n)
		}

		if sp.DisableVPN != wantDisableVPN {
			t.Errorf("%s/%s: disableVPN=%v, want %v (the whole cluster uses one mode)", file, rc.Name, sp.DisableVPN, wantDisableVPN)
		}
		if wantDisableVPN {
			if ip := net.ParseIP(sp.Host); ip == nil {
				t.Errorf("%s/%s: without a VPN spec.host must be an IP bound on the node, got %q", file, rc.Name, sp.Host)
			}
			if sp.VPNConfig != (infrav1.VPNConfig{}) {
				t.Errorf("%s/%s: vpnConfig is ignored without a VPN and must not appear in the sample", file, rc.Name)
			}
		} else if net.ParseIP(sp.VPNConfig.IP) == nil {
			t.Errorf("%s/%s: VPN node needs vpnConfig.ip, got %q", file, rc.Name, sp.VPNConfig.IP)
		}

		sw := sp.NodeInfo.SoftwareConfig
		if err := pkgruntime.ValidateInsecureRegistries(sw.InsecureRegistries); err != nil {
			t.Errorf("%s/%s: invalid insecureRegistries: %v", file, rc.Name, err)
		}
		if sw.ImagePullSecretRef != nil {
			if _, ok := secrets[sw.ImagePullSecretRef.Name]; !ok {
				t.Errorf("%s/%s: imagePullSecretRef %q is not defined in the sample", file, rc.Name, sw.ImagePullSecretRef.Name)
			}
		}
		if sw.CnlabRuntime != nil && sw.CnlabRuntime.CredentialsRef.Name != "" {
			if _, ok := secrets[sw.CnlabRuntime.CredentialsRef.Name]; !ok {
				t.Errorf("%s/%s: cnlabRuntime.credentialsRef %q is not defined in the sample", file, rc.Name, sw.CnlabRuntime.CredentialsRef.Name)
			}
		}

		switch sp.NodeInfo.NodeType {
		case "control-plane":
			cps++
			if sw.KubernetesVersion == "" {
				t.Errorf("%s/%s: the control-plane must set kubernetesVersion", file, rc.Name)
			}
			if !wantDisableVPN {
				v := sp.VPNConfig
				if v.VPNServerPublicIP == "" || v.VPNSSHCredentialsRef.Name == "" {
					t.Errorf("%s/%s: a VPN control-plane needs vpnServerPublicIP and vpnSshCredentialsRef", file, rc.Name)
				} else if _, ok := secrets[v.VPNSSHCredentialsRef.Name]; !ok {
					t.Errorf("%s/%s: VPN SSH Secret %q is not defined in the sample", file, rc.Name, v.VPNSSHCredentialsRef.Name)
				}
			}
		case "worker":
			workers++
		default:
			t.Errorf("%s/%s: nodeType %q", file, rc.Name, sp.NodeInfo.NodeType)
		}
	}
	if cps != 1 || workers < 1 {
		t.Errorf("%s: want exactly one control-plane and at least one worker, got %d / %d", file, cps, workers)
	}

	// No sample may ship something that looks like a live credential.
	raw, _ := os.ReadFile(filepath.Join(samplesDir, file))
	for _, marker := range []string{"ghp_", "dckr_pat_", "github_pat_", "AKIA", "MIIE", "BEGIN RSA PRIVATE KEY"} {
		if strings.Contains(string(raw), marker) {
			t.Errorf("%s contains %q: samples must only hold CHANGE_ME placeholders", file, marker)
		}
	}
}

func TestRemoteClusterSample_VPN(t *testing.T) {
	checkSampleClusterFile(t, "infra_v1_remotecluster_vpn.yaml", false)

	rcs, _ := remoteClusterSamples(t, "infra_v1_remotecluster_vpn.yaml")
	// The sample exists to demonstrate the new fields on the control-plane.
	for _, rc := range rcs {
		if rc.Spec.NodeInfo.NodeType != "control-plane" {
			continue
		}
		if len(rc.Spec.NodeInfo.SoftwareConfig.InsecureRegistries) == 0 {
			t.Error("VPN sample should demonstrate insecureRegistries")
		}
	}
}

func TestRemoteClusterSample_NoVPN(t *testing.T) {
	checkSampleClusterFile(t, "infra_v1_remotecluster_novpn.yaml", true)

	rcs, _ := remoteClusterSamples(t, "infra_v1_remotecluster_novpn.yaml")
	var gpuCP bool
	for _, rc := range rcs {
		if rc.Spec.NodeInfo.NodeType == "control-plane" && rc.Spec.NodeInfo.HardwareType == "gpu" {
			gpuCP = true
			if len(rc.Spec.NodeInfo.SoftwareConfig.InsecureRegistries) == 0 {
				t.Error("no-VPN sample should demonstrate insecureRegistries")
			}
		}
	}
	if !gpuCP {
		t.Error("no-VPN sample should demonstrate a GPU control-plane")
	}
}

// The minimal sample in the quick-start kustomization must keep decoding.
func TestRemoteClusterSample_MinimalDecodes(t *testing.T) {
	rcs, secrets := remoteClusterSamples(t, "infra_v1_remotecluster.yaml")
	if len(rcs) != 1 || !rcs[0].Spec.DisableVPN {
		t.Fatalf("minimal sample: want one VPN-less RemoteCluster, got %d", len(rcs))
	}
	if _, ok := secrets[sampleAuthSecretName(rcs[0])]; !ok {
		t.Error("minimal sample must define its SSH Secret")
	}
}
