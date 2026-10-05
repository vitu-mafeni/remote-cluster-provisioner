package gcp

import (
	"fmt"
	"strings"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

// maxStartupScriptBytes is GCE's limit for a single metadata value (256 KiB),
// kept with headroom: the instance's total metadata is also capped (512 KiB).
const maxStartupScriptBytes = 200 * 1024

// gceNodeIPScript replaces the EC2 IMDS lookup of the bootstrap script in
// no-VPN mode: it reads the instance's internal (VPC) IPv4 from the GCE metadata
// server. The Metadata-Flavor header is required by GCE (it is what protects the
// server from SSRF); the numeric address avoids a DNS dependency at boot.
const gceNodeIPScript = `GCE_MD=http://169.254.169.254/computeMetadata/v1
NODE_IP=""
for i in $(seq 1 30); do
  NODE_IP="$(curl -fsS -m 5 -H 'Metadata-Flavor: Google' "${GCE_MD}/instance/network-interfaces/0/ip" 2>/dev/null || true)"
  [[ -n "$NODE_IP" ]] && break
  sleep 2
done
[[ -n "$NODE_IP" ]] || { report "Could not read the internal IP from the GCE metadata server"; exit 1; }
report "Node IP is ${NODE_IP}"
`

// StartupParams builds the bootstrap parameters of a NodeProvision: the AWS
// mapping plus the GCE node-IP lookup and the "GCP" provider label.
// wgConfig/vpnIP are empty in no-VPN mode.
func StartupParams(np *mlv1alpha1.NodeProvision, joinCommand, kubernetesVersion, kubernetesMinor string,
	runtimeCfg pkgruntime.Config, wgConfig, vpnIP string) awsprovision.CloudInitParams {
	p := awsprovision.BuildCloudInitParams(np, joinCommand, kubernetesVersion, kubernetesMinor, runtimeCfg)
	p.NodeIPScript = gceNodeIPScript
	p.ProviderLabel = string(mlv1alpha1.CloudProviderGCP)
	if !np.Spec.DisableVPN {
		p.WGConfig = wgConfig
		p.VpnIP = vpnIP
	}
	return p
}

// BuildStartupScript validates p and returns the plain-text script for the
// instance's startup-script metadata key. Unlike EC2 user-data it is neither
// compressed nor base64 encoded: GCE hands the metadata value to the guest
// agent as is, which runs it as root on every boot (the script's own
// completion marker makes the later runs no-ops).
func BuildStartupScript(p awsprovision.CloudInitParams) (string, error) {
	script, err := awsprovision.RenderBootstrapScript(p)
	if err != nil {
		return "", err
	}
	if len(script) > maxStartupScriptBytes {
		return "", fmt.Errorf("startup script is %d bytes, over the %d byte budget for GCE metadata", len(script), maxStartupScriptBytes)
	}
	if strings.Contains(script, "\x00") {
		return "", fmt.Errorf("startup script contains a NUL byte")
	}
	return script, nil
}
