package aws

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

// CloudInitParams contains the values required to bootstrap an EC2 Kubernetes worker.
//
// NVIDIA/GPU configuration is intentionally absent. GPU Operator owns the NVIDIA
// driver, container toolkit, CRI-O NVIDIA integration, CDI and device plugin after
// the node joins the cluster.
type CloudInitParams struct {
	WGConfig string
	VpnIP    string
	// NoVPN skips WireGuard entirely. WGConfig and VpnIP are ignored and the
	// kubelet node IP is the instance's private IP, read from instance
	// metadata at boot.
	NoVPN                  bool
	JoinCommand            string
	KubernetesVersion      string
	KubernetesMinorVersion string
	NodeName               string
	Labels                 []string // Extra labels reserved for the control-plane reconciler.
	SSHUsername            string
	// IsGPUNode identifies this worker as a GPU node. It controls Kubernetes
	// node labels only; GPU Operator owns all NVIDIA software/runtime setup.
	IsGPUNode bool

	// Runtime registry credentials for pulling the cnlab-runtime OCI artifact.
	// These values come from a Kubernetes Secret resolved by the controller.
	// Token must never be logged.
	RuntimeRegistryUser  string
	RuntimeRegistryToken string
	RuntimeRegistry      string
	RuntimeRepository    string
	RuntimeVersion       string
	RuntimeOSVariant     string // "" or pkgruntime.OSVariantAuto
	RuntimeOrasVersion   string

	// InsecureRegistries are the registry hosts (host or host:port) CRI-O must
	// treat as insecure (plain HTTP / untrusted TLS). Callers pass the already
	// merged list (explicit softwareConfig.insecureRegistries plus hosts derived
	// from imagePrepulls — see onprem.InsecureRegistryHosts); each entry is
	// validated here and written as a registries.conf.d drop-in BEFORE CRI-O is
	// (re)started, since CRI-O only reads that directory at start. Empty adds
	// nothing and leaves the rendered script unchanged.
	InsecureRegistries []string

	// The two fields below let another cloud (see provider/gcp) reuse this
	// cloud-neutral bootstrap script. Both default to the AWS behaviour when
	// empty, so the rendered AWS output is unchanged.
	//
	// NodeIPScript replaces, in NoVPN mode only, the bash snippet that reads the
	// instance's private IP from the cloud's metadata service. It must set
	// NODE_IP (non-empty) or `exit 1`, and use the report helper. Trusted,
	// controller-authored text: never fill it from user input.
	NodeIPScript string
	// ProviderLabel is the value of the ml.dcn.ssu.ac.kr/provider kubelet label
	// stamped on GPU nodes at registration (default "AWS"); it must equal what the
	// NodeProvision controller stamps (Spec.Provider).
	ProviderLabel string
}

const (
	crioSocket = "/var/run/crio/crio.sock"
)

// defaultNodeIPScript is the EC2 (IMDSv2) lookup of the private IPv4 used as the
// kubelet node IP when no VPN is in use. Kept byte-for-byte identical to the
// text that used to live inline in bootstrapTemplate.
const defaultNodeIPScript = `IMDS=http://169.254.169.254/latest
IMDS_TOKEN="$(curl -fsS -m 5 -X PUT "${IMDS}/api/token" -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)"
NODE_IP=""
for i in $(seq 1 30); do
  NODE_IP="$(curl -fsS -m 5 ${IMDS_TOKEN:+-H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}"} "${IMDS}/meta-data/local-ipv4" 2>/dev/null || true)"
  [[ -n "$NODE_IP" ]] && break
  sleep 2
done
[[ -n "$NODE_IP" ]] || { report "Could not read the private IP from instance metadata"; exit 1; }
report "Node IP is ${NODE_IP}"
`

// BuildCloudInitParams is the exported form of buildCloudInitParams (the same
// NodeProvision -> bootstrap parameter mapping, including the shared GPU rule),
// for clouds that reuse the bootstrap script.
func BuildCloudInitParams(np *mlv1alpha1.NodeProvision, joinCommand, kubernetesVersion, kubernetesMinor string, runtimeCfg pkgruntime.Config) CloudInitParams {
	return buildCloudInitParams(np, joinCommand, kubernetesVersion, kubernetesMinor, runtimeCfg)
}

// RenderBootstrapScript validates p and returns the raw (not compressed, not
// base64) bootstrap script. BuildUserDataE wraps the same script for EC2
// user-data; other clouds that take a plain-text startup script (GCE's
// startup-script metadata) use this form. Invalid parameters are an error and
// the caller must not launch an instance.
func RenderBootstrapScript(p CloudInitParams) (string, error) {
	if err := validateParams(p); err != nil {
		return "", fmt.Errorf("invalid cloud-init parameters: %w", err)
	}
	script, err := renderBootstrapScript(p)
	if err != nil {
		return "", fmt.Errorf("failed to render cloud-init: %w", err)
	}
	return script, nil
}

var (
	versionRE = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	minorRE   = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)
	ipRE      = regexp.MustCompile(`^[0-9A-Fa-f:.]+$`)
	nameRE    = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$`)
	userRE    = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
)

// BuildUserDataE renders the bootstrap script, gzip-compresses it and returns
// standard-base64 encoded user-data suitable for EC2. Invalid parameters or a
// render failure are returned as an error; callers must not launch an instance
// in that case.
func BuildUserDataE(p CloudInitParams) (string, error) {
	if err := validateParams(p); err != nil {
		return "", fmt.Errorf("invalid cloud-init parameters: %w", err)
	}

	script, err := renderBootstrapScript(p)
	if err != nil {
		return "", fmt.Errorf("failed to render cloud-init: %w", err)
	}
	return encodeScript(script), nil
}

// BuildUserData is the legacy string-returning form of BuildUserDataE. On
// invalid input it returns user-data that explicitly fails (exit 2) instead of a
// potentially dangerous bootstrap script. New code should use BuildUserDataE and
// refuse to launch on error: launching with this script yields a running but
// never-bootstrapping instance.
func BuildUserData(p CloudInitParams) string {
	out, err := BuildUserDataE(p)
	if err != nil {
		return encodeScript(fmt.Sprintf("#!/bin/bash\necho %q >&2\nexit 2\n", err.Error()))
	}
	return out
}

func validateParams(p CloudInitParams) error {
	if !p.NoVPN {
		if strings.TrimSpace(p.WGConfig) == "" {
			return fmt.Errorf("WGConfig must not be empty")
		}
		if !ipRE.MatchString(strings.TrimSpace(p.VpnIP)) {
			return fmt.Errorf("VpnIP contains invalid characters")
		}
	}
	if !versionRE.MatchString(strings.TrimPrefix(strings.TrimSpace(p.KubernetesVersion), "v")) {
		return fmt.Errorf("KubernetesVersion must be a full semantic version such as 1.35.0")
	}
	if !minorRE.MatchString(strings.TrimPrefix(strings.TrimSpace(p.KubernetesMinorVersion), "v")) {
		return fmt.Errorf("KubernetesMinorVersion must be a minor version such as 1.35")
	}
	join := strings.TrimSpace(p.JoinCommand)
	if join == "" {
		return fmt.Errorf("JoinCommand must not be empty")
	}
	if strings.Contains(join, "--cri-socket") {
		return fmt.Errorf("JoinCommand must not already contain --cri-socket; cloud-init adds the CRI-O endpoint")
	}
	if p.NodeName != "" && !nameRE.MatchString(strings.ToLower(p.NodeName)) {
		return fmt.Errorf("NodeName is not a valid Kubernetes/DNS hostname")
	}
	if p.SSHUsername != "" && !userRE.MatchString(p.SSHUsername) {
		return fmt.Errorf("SSHUsername is not a valid Linux username")
	}
	// The join command is executed with `bash -c` on the node: only plain
	// `kubeadm join <args>` is acceptable.
	if err := kubeadm.ValidateJoinCommand(join); err != nil {
		return fmt.Errorf("JoinCommand: %w", err)
	}
	if err := pkgruntime.ValidateInsecureRegistries(p.InsecureRegistries); err != nil {
		return err
	}
	rc := pkgruntime.Config{
		Registry: p.RuntimeRegistry, Repository: p.RuntimeRepository,
		Version: p.RuntimeVersion, OrasVersion: p.RuntimeOrasVersion,
		OSVariant: p.RuntimeOSVariant,
	}
	if err := rc.Validate(); err != nil {
		return err
	}
	return nil
}

func encodeScript(script string) string {
	var buf bytes.Buffer
	gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return base64.StdEncoding.EncodeToString([]byte(script))
	}
	if _, err := gz.Write([]byte(script)); err != nil {
		_ = gz.Close()
		return base64.StdEncoding.EncodeToString([]byte(script))
	}
	if err := gz.Close(); err != nil {
		return base64.StdEncoding.EncodeToString([]byte(script))
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

type templateData struct {
	WGConfigB64          string
	VpnIP                string
	NoVPN                bool
	JoinCommand          string
	JoinCommandB64       string
	KubernetesVersion    string
	KubernetesMinor      string
	NodeName             string
	SSHUsername          string
	HasSSHUsername       bool
	IsGPUNode            bool
	KubeletNodeLabels    string
	CRIOSocket           string
	RuntimeCredentials   string // bash export block; Token value must never be logged
	RuntimeInstallScript string // rendered by pkgruntime.InstallScript
	NodeJoinedFunc       string // bash function node_already_joined (kubeadm.NodeAlreadyJoinedFunc)
	NodeIPScript         string // NoVPN node-IP lookup; trusted controller text, see CloudInitParams.NodeIPScript
	// InsecureRegistriesScript is the registries.conf.d drop-in writer
	// (pkgruntime.InsecureRegistriesScript, run as root); empty when none.
	InsecureRegistriesScript string
}

func kubeletNodeLabels(p CloudInitParams) string {
	if !p.IsGPUNode {
		return ""
	}
	// These are the same GPU identity labels applied authoritatively by the
	// kubeadm control-plane reconciler after the worker joins. Setting them at
	// registration time avoids a window where the GPU worker is unclassified.
	// The provider value matches what the NodeProvision controller stamps
	// (nodeProvisionProviderLabel = Spec.Provider, i.e. "AWS" for this path).
	provider := p.ProviderLabel
	if provider == "" {
		provider = "AWS"
	}
	return "hardware-type=gpu,gpu=on,ml.dcn.ssu.ac.kr/provider=" + provider
}

// nodeIPScript returns the NoVPN node-IP lookup: the caller's, else the EC2 one.
func nodeIPScript(p CloudInitParams) string {
	if p.NodeIPScript != "" {
		if !strings.HasSuffix(p.NodeIPScript, "\n") {
			return p.NodeIPScript + "\n"
		}
		return p.NodeIPScript
	}
	return defaultNodeIPScript
}

func renderBootstrapScript(p CloudInitParams) (string, error) {
	tmpl, err := template.New("aws-cloud-init").Parse(bootstrapTemplate)
	if err != nil {
		return "", fmt.Errorf("parse bootstrap template: %w", err)
	}

	runtimeCfg := pkgruntime.Config{
		Registry:    p.RuntimeRegistry,
		Repository:  p.RuntimeRepository,
		Version:     p.RuntimeVersion,
		OSVariant:   p.RuntimeOSVariant,
		OrasVersion: p.RuntimeOrasVersion,
		Username:    p.RuntimeRegistryUser,
		Token:       p.RuntimeRegistryToken,
	}
	runtimeCfg.ApplyDefaults()

	// Build the credentials block. Values are shell-quoted (embedded single
	// quotes are escaped), so no username/token can break out of the string.
	// Token is embedded in user-data but never logged per security constraints.
	runtimeCreds := "export CNLAB_REGISTRY_USER=" + sshhelper.ShellQuote(runtimeCfg.Username) + "\n" +
		"export CNLAB_REGISTRY_TOKEN=" + sshhelper.ShellQuote(runtimeCfg.Token)

	d := templateData{
		WGConfigB64:          base64.StdEncoding.EncodeToString([]byte(p.WGConfig)),
		VpnIP:                p.VpnIP,
		NoVPN:                p.NoVPN,
		JoinCommand:          p.JoinCommand,
		JoinCommandB64:       base64.StdEncoding.EncodeToString([]byte(p.JoinCommand)),
		KubernetesVersion:    strings.TrimPrefix(p.KubernetesVersion, "v"),
		KubernetesMinor:      strings.TrimPrefix(p.KubernetesMinorVersion, "v"),
		NodeName:             p.NodeName,
		SSHUsername:          p.SSHUsername,
		HasSSHUsername:       p.SSHUsername != "",
		IsGPUNode:            p.IsGPUNode,
		KubeletNodeLabels:    kubeletNodeLabels(p),
		CRIOSocket:           crioSocket,
		RuntimeCredentials:   runtimeCreds,
		RuntimeInstallScript: pkgruntime.InstallScript(runtimeCfg),
		NodeJoinedFunc:       kubeadm.NodeAlreadyJoinedFunc,
		NodeIPScript:         nodeIPScript(p),

		InsecureRegistriesScript: pkgruntime.InsecureRegistriesScript(p.InsecureRegistries, false),
	}

	var out bytes.Buffer
	if err := tmpl.Execute(&out, d); err != nil {
		return "", fmt.Errorf("execute bootstrap template: %w", err)
	}
	return out.String(), nil
}

const bootstrapTemplate = `#!/bin/bash
set -Eeuo pipefail

LOG=/var/log/node-bootstrap.log
STATUS_FILE=/var/lib/node-bootstrap-status
COMPLETE_FILE=/var/lib/node-bootstrap-complete
LOCK_FILE=/var/lock/node-bootstrap.lock

mkdir -p /var/log /var/lib
exec > >(tee -a "$LOG") 2>&1

report() {
  local msg="[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
  printf '%s\n' "$msg"
  printf '%s\n' "$msg" > "$STATUS_FILE"
  sync
}

on_error() {
  local rc=$?
  local line=${1:-unknown}
  local cmd=${2:-unknown}
  printf '[%s] FAILED at line %s: %s (exit %s)\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$line" "$cmd" "$rc" | tee -a "$LOG" >&2
  printf 'FAILED\n' > "$STATUS_FILE"
  {
    echo '===== systemd: crio ====='
    systemctl status crio --no-pager -l || true
    echo '===== journal: crio ====='
    journalctl -u crio --no-pager -n 120 || true
    echo '===== systemd: kubelet ====='
    systemctl status kubelet --no-pager -l || true
    echo '===== journal: kubelet ====='
    journalctl -u kubelet --no-pager -n 80 || true
  } >> "$LOG" 2>&1 || true
  exit "$rc"
}
trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR

# Serialize bootstrap and make retries safe. A second invocation must never
# destructively wipe a node that already completed provisioning.
exec 9>"$LOCK_FILE"
flock -n 9 || { report "Another bootstrap process is already running"; exit 0; }

if [[ -f "$COMPLETE_FILE" ]]; then
  report "Bootstrap already completed; nothing to do"
  exit 0
fi

K8S_VERSION="{{.KubernetesVersion}}"
K8S_MINOR="{{.KubernetesMinor}}"
CRIO_SOCKET="{{.CRIOSocket}}"
NODE_IP="{{.VpnIP}}" # empty with NoVPN; resolved from instance metadata below
NODE_NAME="{{.NodeName}}"

report "Bootstrap started"
report "Kubernetes version: v${K8S_VERSION}; repo: ${K8S_MINOR}"

# -----------------------------------------------------------------------------
# Validation and helpers
# -----------------------------------------------------------------------------
command -v bash >/dev/null 2>&1 || { echo 'bash is required'; exit 1; }
command -v systemctl >/dev/null 2>&1 || { echo 'systemd is required'; exit 1; }

apt_update() {
  DEBIAN_FRONTEND=noninteractive apt-get \
    -o DPkg::Lock::Timeout=300 \
    -o Acquire::Retries=5 update
}

apt_install() {
  # IMPORTANT: this helper owns the install verb. Callers pass packages only.
  DEBIAN_FRONTEND=noninteractive apt-get \
    -y \
    -o DPkg::Lock::Timeout=300 \
    -o Acquire::Retries=5 \
    -o Dpkg::Options::=--force-confnew \
    install "$@"
}

wait_for_apt() {
  local i holders
  for i in $(seq 1 36); do
    holders="$(fuser /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock /var/cache/apt/archives/lock 2>/dev/null || true)"
    if [[ -z "${holders//[[:space:]]/}" ]]; then
      return 0
    fi
    report "Waiting for apt/dpkg lock holders (${i}/36): ${holders}"
    sleep 5
  done
  echo 'Timed out waiting for dpkg/apt locks' >&2
  fuser -v /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock /var/cache/apt/archives/lock 2>/dev/null || true
  return 1
}

wait_for_service_active() {
  local service="$1"
  local timeout="${2:-90}"
  local end=$((SECONDS + timeout))
  while (( SECONDS < end )); do
    if systemctl is-active --quiet "$service"; then
      return 0
    fi
    sleep 2
  done
  systemctl status "$service" --no-pager -l || true
  return 1
}

wait_for_crio() {
  local i
  mkdir -p /run/crio /var/run/crio
  for i in $(seq 1 30); do
    if systemctl is-active --quiet crio && \
       crictl --runtime-endpoint "unix://${CRIO_SOCKET}" \
              --image-endpoint "unix://${CRIO_SOCKET}" info >/dev/null 2>&1; then
      report "CRI-O is ready"
      return 0
    fi
    report "Waiting for CRI-O (${i}/30)"
    sleep 3
  done
  report "CRI-O failed readiness check"
  systemctl status crio --no-pager -l || true
  journalctl -u crio --no-pager -n 150 || true
  return 1
}

restart_crio_and_wait() {
  systemctl daemon-reload
  systemctl restart crio || {
    systemctl status crio --no-pager -l || true
    journalctl -u crio --no-pager -n 150 || true
    return 1
  }
  wait_for_crio
}

# -----------------------------------------------------------------------------
# Hostname
# -----------------------------------------------------------------------------
if [[ -n "$NODE_NAME" ]]; then
  hostnamectl set-hostname "$NODE_NAME"
  report "Hostname set to $NODE_NAME"
else
  report "Using existing hostname: $(hostname)"
fi

# -----------------------------------------------------------------------------
# Kubernetes host prerequisites
# -----------------------------------------------------------------------------
report "Applying Kubernetes host prerequisites"
swapoff -a || true
sed -i -E '/[[:space:]]swap[[:space:]]/ s/^[[:space:]]*([^#])/#\1/' /etc/fstab || true
cat > /etc/modules-load.d/k8s.conf <<'MODULES'
overlay
br_netfilter
MODULES
modprobe overlay
modprobe br_netfilter
cat > /etc/sysctl.d/99-kubernetes-cri.conf <<'SYSCTL'
net.bridge.bridge-nf-call-iptables=1
net.bridge.bridge-nf-call-ip6tables=1
net.ipv4.ip_forward=1
SYSCTL
sysctl --system >/dev/null

# -----------------------------------------------------------------------------
# Network / WireGuard
# -----------------------------------------------------------------------------
report "Installing base OS packages"

# Stop Ubuntu's scheduled package jobs before waiting for the package database.
# Do not kill arbitrary apt/dpkg processes: cloud-init itself can be running apt.
systemctl stop unattended-upgrades apt-daily.service apt-daily-upgrade.service 2>/dev/null || true
systemctl disable unattended-upgrades apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true

wait_for_apt
dpkg --configure -a
apt_update
apt_install ca-certificates curl gnupg apt-transport-https lsof jq \
  {{if not .NoVPN}}wireguard {{end}}iproute2 socat conntrack

{{if .NoVPN}}report "VPN disabled; using the instance private IP as the node IP"
{{.NodeIPScript}}{{else}}report "Configuring WireGuard"
mkdir -p /etc/wireguard
printf '%s' '{{.WGConfigB64}}' | base64 -d > /etc/wireguard/wg0.conf
chmod 0600 /etc/wireguard/wg0.conf
systemctl enable wg-quick@wg0
systemctl restart wg-quick@wg0

# Verify the tunnel and requested VPN address. Do not continue with a kubelet
# node-ip that does not exist on the host.
for i in $(seq 1 30); do
  if ip -4 addr show wg0 2>/dev/null | grep -Eq 'inet[[:space:]]+'"$NODE_IP"'([/[:space:]]|$)'; then
    break
  fi
  sleep 2
done
ip -4 addr show wg0 | grep -Eq 'inet[[:space:]]+'"$NODE_IP"'([/[:space:]]|$)' || {
  report "WireGuard did not acquire expected VPN IP ${NODE_IP}"
  wg show wg0 || true
  ip -4 addr show wg0 || true
  exit 1
}
report "WireGuard is ready on ${NODE_IP}"
{{end}}
# -----------------------------------------------------------------------------
# cnlab-runtime OCI artifact install (ORAS-based, replaces all source builds)
# -----------------------------------------------------------------------------
# Credentials are set here and consumed by the install script below.
# Token is embedded in user-data which is only accessible from the instance
# itself via IMDSv2. It is NOT logged by this script.
{{.RuntimeCredentials}}

{{.RuntimeInstallScript}}

# Unset credentials immediately after the install script runs.
unset CNLAB_REGISTRY_USER CNLAB_REGISTRY_TOKEN

# -----------------------------------------------------------------------------
# CRI-O configuration drop-ins
# Each file is only written if cnlab-runtime did not already install it.
# -----------------------------------------------------------------------------
report "Configuring CRI-O drop-ins"
systemctl stop kubelet 2>/dev/null || true
systemctl stop crio 2>/dev/null || true
killall -9 crio conmon crun 2>/dev/null || true
rm -rf /run/crio /var/run/crio

mkdir -p /run/crio /var/run/crio /var/lib/crio /var/lib/containers/storage
mkdir -p /etc/crio/crio.conf.d /etc/containers /etc/cni/net.d /opt/cni/bin /etc/criu
{{if .InsecureRegistriesScript}}
# Registries served over plain HTTP / untrusted TLS (softwareConfig.insecureRegistries
# plus imagePrepulls hosts). Must be on disk before CRI-O (re)starts below.
{{.InsecureRegistriesScript}}{{end}}
if [ ! -f /etc/crictl.yaml ]; then
  cat > /etc/crictl.yaml <<CRICTL
runtime-endpoint: unix://${CRIO_SOCKET}
image-endpoint: unix://${CRIO_SOCKET}
timeout: 30s
debug: false
CRICTL
  chmod 0644 /etc/crictl.yaml
fi

# Only [crio.runtime] owns listen/conmon. Do not put listen under [crio.image].
if [ ! -f /etc/crio/crio.conf.d/10-paths.conf ]; then
  cat > /etc/crio/crio.conf.d/10-paths.conf <<'CRIOPATHS'
[crio.runtime]
  listen = "/var/run/crio/crio.sock"
  conmon = "/usr/local/bin/conmon"
CRIOPATHS
fi

if [ ! -f /etc/crio/crio.conf.d/999-runc.conf ]; then
  cat > /etc/crio/crio.conf.d/999-runc.conf <<'RUNTIME'
[crio]

  [crio.runtime]
    default_runtime = "runc"

    [crio.runtime.runtimes]
      [crio.runtime.runtimes.runc]
        runtime_path = "/usr/bin/runc"
        runtime_type = "oci"

      [crio.runtime.runtimes.nvidia]
        runtime_path = "/usr/bin/nvidia-container-runtime"
        runtime_type = "oci"
RUNTIME
fi

if [ ! -f /etc/crio/crio.conf.d/9999-nvidia.conf ]; then
  cat > /etc/crio/crio.conf.d/9999-nvidia.conf <<'RUNTIME'
[crio.runtime]
  [crio.runtime.runtimes]
    [crio.runtime.runtimes.nvidia]
      runtime_path = "/usr/local/nvidia/toolkit/nvidia-container-runtime"
      runtime_type = "oci"
    [crio.runtime.runtimes.nvidia-cdi]
      runtime_path = "/usr/local/nvidia/toolkit/nvidia-container-runtime.cdi"
      runtime_type = "oci"
RUNTIME
fi

if [ ! -f /etc/containers/policy.json ]; then
  cat > /etc/containers/policy.json <<'POLICY'
{"default":[{"type":"insecureAcceptAnything"}]}
POLICY
fi

if [ ! -f /etc/containers/registries.conf ]; then
  cat > /etc/containers/registries.conf <<'REGCONF'
unqualified-search-registries = ["docker.io"]

[[registry]]
location = "docker.io"
REGCONF
fi

# CRIU runtime configuration used by the checkpoint/restore path.
if [ ! -f /etc/criu/runc.conf ]; then
  cat > /etc/criu/runc.conf <<'CRIUCONF'
tcp-close
skip-in-flight
log-file /tmp/criu.log
ghost-limit 100M
enable-external-masters
external mnt[]
irmap-scan-path /home/jovyan
irmap-scan-path /usr
irmap-scan-path /opt/conda
irmap-scan-path /opt/remote-dev
allow-uprobes
CRIUCONF
fi
# Pre-existing runc.conf (older provisioning): make sure allow-uprobes is set.
grep -qx 'allow-uprobes' /etc/criu/runc.conf || echo 'allow-uprobes' >> /etc/criu/runc.conf
if [ ! -f /etc/criu/default.conf ]; then
  cp -f /etc/criu/runc.conf /etc/criu/default.conf
fi
grep -qx 'allow-uprobes' /etc/criu/default.conf || echo 'allow-uprobes' >> /etc/criu/default.conf

systemctl daemon-reload
systemctl enable crio
restart_crio_and_wait

# -----------------------------------------------------------------------------
# Kubernetes repository and packages
# -----------------------------------------------------------------------------
report "Configuring Kubernetes apt repository"
mkdir -p /etc/apt/keyrings
rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg
curl -fsSL "https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/Release.key" \
  | gpg --dearmor --yes -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
printf '%s\n' \
  "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/ /" \
  > /etc/apt/sources.list.d/kubernetes.list
apt_update

report "Installing Kubernetes packages"
apt_install "kubelet=${K8S_VERSION}-*" "kubeadm=${K8S_VERSION}-*" "kubectl=${K8S_VERSION}-*" \
  --allow-change-held-packages --allow-downgrades
apt-mark hold kubelet kubeadm kubectl
systemctl enable kubelet
systemctl stop kubelet 2>/dev/null || true

KUBELET_ARGS="--node-ip=${NODE_IP}"
if [[ -n "{{.KubeletNodeLabels}}" ]]; then
  KUBELET_ARGS="${KUBELET_ARGS} --node-labels={{.KubeletNodeLabels}}"
fi
printf 'KUBELET_EXTRA_ARGS=%s\n' "$KUBELET_ARGS" > /etc/default/kubelet
mkdir -p /etc/systemd/system/kubelet.service.d
cat > /etc/systemd/system/kubelet.service.d/10-crio.conf <<'UNIT'
[Unit]
After=crio.service
Requires=crio.service
UNIT
systemctl daemon-reload

# -----------------------------------------------------------------------------
# Final CRI-O gate immediately before kubeadm join
# -----------------------------------------------------------------------------
report "Performing final CRI-O health check before kubeadm join"
restart_crio_and_wait
crictl info >/dev/null

# -----------------------------------------------------------------------------
# kubeadm join
# -----------------------------------------------------------------------------
report "Joining cluster"
# BEGIN kubeadm join
{{.NodeJoinedFunc}}
JOIN_CMD="$(printf '%s' '{{.JoinCommandB64}}' | base64 -d)"

# kubeadm's own preflight check for the cluster-info ConfigMap uses a short
# (~10s) internal timeout, which is too tight for a worker whose WireGuard
# tunnel may not have finished its handshake/routing yet at boot — that
# transient race produces "couldn't validate the identity of the API Server
# ... context deadline exceeded". Wait for the control-plane API server to
# actually answer over the tunnel before spending a kubeadm attempt on it.
API_ENDPOINT="$(awk '{for(i=1;i<=NF;i++) if ($i=="join") {print $(i+1); exit}}' <<< "$JOIN_CMD")"

# Resuming (cloud-init re-run after a reboot or a failure after the join) must not
# wipe a node that is already healthily joined: skip the join entirely.
if node_already_joined "$API_ENDPOINT"; then
  report "Node is already joined to the cluster (kubelet healthy, API server reachable); skipping kubeadm join"
else
  if [[ -n "$API_ENDPOINT" ]]; then
    report "Waiting for control-plane API server ${API_ENDPOINT} to become reachable{{if not .NoVPN}} over the VPN tunnel{{end}}..."
    for i in $(seq 1 60); do
      curl -sk --connect-timeout 3 --max-time 5 "https://${API_ENDPOINT}/healthz" -o /dev/null && { report "Control-plane API server reachable"; break; }
      sleep 5
    done
  fi

  # Join command is generated by the control plane. It is intentionally executed
  # only after CRI-O is proven healthy. --cri-socket prevents kubeadm from probing
  # an unintended container runtime.
  for attempt in 1 2 3 4 5; do
    if bash -c "$JOIN_CMD --cri-socket=unix://${CRIO_SOCKET}"; then
      report "Cluster join succeeded"
      break
    fi
    # kubeadm can report an error after the kubelet already registered; a
    # healthily joined node must never be reset.
    if node_already_joined "$API_ENDPOINT"; then
      report "kubeadm join reported an error but the node is healthily joined; not resetting"
      break
    fi
    if [[ "$attempt" == "5" ]]; then
      report "ERROR: kubeadm join failed after 5 attempts"
      exit 1
    fi
    # A failed join can leave partial state (kubelet config, certs, static pod
    # manifests) that fails the next attempt's preflight: reset first (only
    # reached when the node is NOT healthily joined).
    kubeadm reset --force --cri-socket="unix://${CRIO_SOCKET}" >/dev/null 2>&1 || true
    sleep $((attempt * 15))
    restart_crio_and_wait
  done
fi
# END kubeadm join

# kubeadm should have created kubelet configuration. Do not blindly restart it
# until CRI-O is healthy; the systemd dependency also enforces the relationship.
systemctl daemon-reload
systemctl restart kubelet
wait_for_service_active kubelet 120

# Verify kubelet is talking to CRI-O. A running process alone is insufficient.
for i in $(seq 1 30); do
  if systemctl is-active --quiet kubelet && crictl info >/dev/null 2>&1; then
    break
  fi
  sleep 3
done
systemctl is-active --quiet kubelet || {
  journalctl -u kubelet --no-pager -n 120 || true
  exit 1
}
crictl info >/dev/null

# -----------------------------------------------------------------------------
# GPU ownership boundary
# -----------------------------------------------------------------------------
# There is deliberately NO NVIDIA setup in this script.
# GPU Operator is expected to install and configure:
#   - NVIDIA driver
#   - NVIDIA container toolkit
#   - CRI-O NVIDIA runtime integration
#   - CDI
#   - NVIDIA device plugin / GPU resources
# Any NVIDIA runtime configuration here would race with GPU Operator and can
# make CRI-O unavailable before the operator is ready.
report "GPU configuration deferred entirely to GPU Operator"

# GPU identity labels are supplied to kubelet at registration time when
# IsGPUNode=true. The control-plane reconciler remains authoritative and will
# re-apply the same labels and the GPU taint after resolving the joined node.
# No kubectl/admin kubeconfig is required on the worker.

# -----------------------------------------------------------------------------
# SSH access
# -----------------------------------------------------------------------------
{{if .HasSSHUsername}}
cat > /etc/sudoers.d/nopasswd-{{.SSHUsername}} <<'SUDOERS'
{{.SSHUsername}} ALL=(ALL) NOPASSWD:ALL
SUDOERS
chmod 0440 /etc/sudoers.d/nopasswd-{{.SSHUsername}}
visudo -cf /etc/sudoers.d/nopasswd-{{.SSHUsername}}
{{end}}

# -----------------------------------------------------------------------------
# Completion marker
# -----------------------------------------------------------------------------
printf 'SUCCESS\n' > "$STATUS_FILE"
touch "$COMPLETE_FILE"
sync
report "Bootstrap complete"
`
