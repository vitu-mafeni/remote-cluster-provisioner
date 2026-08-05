package aws

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
)

// CloudInitParams contains the values required to bootstrap an EC2 Kubernetes worker.
//
// NVIDIA/GPU configuration is intentionally absent. GPU Operator owns the NVIDIA
// driver, container toolkit, CRI-O NVIDIA integration, CDI and device plugin after
// the node joins the cluster.
type CloudInitParams struct {
	WGConfig               string
	VpnIP                  string
	JoinCommand            string
	KubernetesVersion      string
	KubernetesMinorVersion string
	CRIOVersion            string
	NodeName               string
	Labels                 []string // Kept for API compatibility; applied by the controller after join.
	SSHUsername            string
}

const (
	crioSocket = "/var/run/crio/crio.sock"
)

var (
	versionRE = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	minorRE   = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)
	ipRE      = regexp.MustCompile(`^[0-9A-Fa-f:.]+$`)
	nameRE    = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$`)
	userRE    = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
)

// BuildUserData renders the bootstrap script, gzip-compresses it and returns
// standard-base64 encoded user-data suitable for EC2.
//
// Invalid input is rendered as an explicit shell failure rather than silently
// generating a potentially dangerous bootstrap script. Existing callers use a
// string-returning API, so the function cannot return validation errors.
func BuildUserData(p CloudInitParams) string {
	if err := validateParams(p); err != nil {
		return encodeScript(fmt.Sprintf("#!/bin/bash\necho %q >&2\nexit 2\n", "invalid cloud-init parameters: "+err.Error()))
	}

	script, err := renderBootstrapScript(p)
	if err != nil {
		return encodeScript(fmt.Sprintf("#!/bin/bash\necho %q >&2\nexit 2\n", "failed to render cloud-init: "+err.Error()))
	}
	return encodeScript(script)
}

func validateParams(p CloudInitParams) error {
	if strings.TrimSpace(p.WGConfig) == "" {
		return fmt.Errorf("WGConfig must not be empty")
	}
	if !ipRE.MatchString(strings.TrimSpace(p.VpnIP)) {
		return fmt.Errorf("VpnIP contains invalid characters")
	}
	if !versionRE.MatchString(strings.TrimPrefix(strings.TrimSpace(p.KubernetesVersion), "v")) {
		return fmt.Errorf("KubernetesVersion must be a full semantic version such as 1.35.0")
	}
	if !minorRE.MatchString(strings.TrimPrefix(strings.TrimSpace(p.KubernetesMinorVersion), "v")) {
		return fmt.Errorf("KubernetesMinorVersion must be a minor version such as 1.35")
	}
	if !minorRE.MatchString(strings.TrimPrefix(strings.TrimSpace(p.CRIOVersion), "v")) {
		return fmt.Errorf("CRIOVersion must be a minor version such as 1.35")
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
	WGConfigB64       string
	VpnIP             string
	JoinCommand       string
	JoinCommandB64    string
	KubernetesVersion string
	KubernetesMinor   string
	CRIOVersion       string
	NodeName          string
	SSHUsername       string
	HasSSHUsername    bool
	CRIOAsset         string
	CRIOCommit        string
	CRIUAsset         string
	CRIUGitID         string
	RuncVersion       string
	CRIOSocket        string
}

func renderBootstrapScript(p CloudInitParams) (string, error) {
	tmpl, err := template.New("aws-cloud-init").Parse(bootstrapTemplate)
	if err != nil {
		return "", fmt.Errorf("parse bootstrap template: %w", err)
	}

	d := templateData{
		WGConfigB64:       base64.StdEncoding.EncodeToString([]byte(p.WGConfig)),
		VpnIP:             p.VpnIP,
		JoinCommand:       p.JoinCommand,
		JoinCommandB64:    base64.StdEncoding.EncodeToString([]byte(p.JoinCommand)),
		KubernetesVersion: strings.TrimPrefix(p.KubernetesVersion, "v"),
		KubernetesMinor:   strings.TrimPrefix(p.KubernetesMinorVersion, "v"),
		CRIOVersion:       strings.TrimPrefix(p.CRIOVersion, "v"),
		NodeName:          p.NodeName,
		SSHUsername:       p.SSHUsername,
		HasSSHUsername:    p.SSHUsername != "",
		CRIOAsset:         kubeadm.CrioAsset,
		CRIOCommit:        kubeadm.CrioCommit,
		CRIUAsset:         kubeadm.CriuAsset,
		CRIUGitID:         kubeadm.CriuGitID,
		RuncVersion:       kubeadm.RuncVersion,
		CRIOSocket:        crioSocket,
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
CRIO_VERSION="{{.CRIOVersion}}"
CRIO_SOCKET="{{.CRIOSocket}}"
NODE_IP="{{.VpnIP}}"
NODE_NAME="{{.NodeName}}"

report "Bootstrap started"
report "Kubernetes version: v${K8S_VERSION}; Kubernetes repo: ${K8S_MINOR}; CRI-O repo: ${CRIO_VERSION}"

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
  local i
  for i in $(seq 1 30); do
    if flock -n /var/lib/dpkg/lock-frontend -c 'true' 2>/dev/null; then
      return 0
    fi
    report "Waiting for apt/dpkg lock (${i}/30)"
    sleep 5
  done
  echo 'Timed out waiting for dpkg lock' >&2
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

wait_for_apt
systemctl stop unattended-upgrades apt-daily.service apt-daily-upgrade.service 2>/dev/null || true
systemctl disable unattended-upgrades apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true

# Never kill arbitrary apt-get processes: cloud-init itself can be running apt.
# Wait for the lock and repair dpkg instead.
wait_for_apt
dpkg --configure -a || true
apt_update
apt_install ca-certificates curl gnupg apt-transport-https lsof jq git build-essential pkg-config \
  autoconf automake libtool python3-dev libyajl-dev libjson-c-dev \
  libcap2 libnl-3-200 libbsd0 libgnutls30 \
  wireguard iproute2 socat conntrack

report "Configuring WireGuard"
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

# -----------------------------------------------------------------------------
# CRI-O / CRI-O dependencies
# -----------------------------------------------------------------------------
report "Preparing CRI-O build dependencies"
apt_install build-essential gcc make pkg-config git \
  libgpgme-dev libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler \
  python3-protobuf uuid-dev libbsd-dev libnftables-dev libcap-dev libnl-3-dev \
  libnet1-dev libaio-dev libgnutls28-dev libdrm-dev xmlto asciidoc \
  libassuan-dev libglib2.0-dev libc6-dev libgpg-error-dev libseccomp-dev \
  libsystemd-dev libselinux1-dev libbtrfs-dev libudev-dev software-properties-common \
  go-md2man runc crun

dpkg --configure -a

# The custom CRI-O branch requires Go 1.26.4. Install it explicitly instead of
# depending on the Ubuntu release's older golang package.
report "Installing Go 1.26.4 for custom CRI-O"
GO_VERSION="1.26.4"
if [[ ! -x /usr/local/go/bin/go ]] || [[ "$(/usr/local/go/bin/go version 2>/dev/null || true)" != *"go${GO_VERSION}"* ]]; then
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tar.gz
  rm -rf /usr/local/go
  tar -C /usr/local -xzf /tmp/go.tar.gz
  rm -f /tmp/go.tar.gz
fi
/usr/local/go/bin/go version
export PATH="/usr/local/go/bin:${PATH}"

# Build conmon from source. CRI-O searches /usr/libexec/crio/conmon on Ubuntu;
# provide the exact path explicitly rather than relying on PATH lookup.
report "Building conmon"
rm -rf /tmp/conmon
git clone --depth=1 https://github.com/containers/conmon /tmp/conmon
(
  cd /tmp/conmon
  PATH="/usr/local/go/bin:${PATH}" make
  make install
)
mkdir -p /usr/libexec/crio
ln -sfn /usr/local/bin/conmon /usr/libexec/crio/conmon
command -v conmon

# Build the exact custom CRI-O tree used by pkg/kubeadm. This is important for
# checkpoint/restore behavior and avoids running the distribution CRI-O binary.
report "Building custom CRI-O"
rm -rf /tmp/custom-crio
git clone --depth=1 --branch 22-07-2026-checkpoint-restore \
  https://github.com/vitu-mafeni/leehun-cri-o.git /tmp/custom-crio
(
  cd /tmp/custom-crio
  PATH="/usr/local/go/bin:${PATH}" make
  make install
  make install.config
  make install.systemd
)
rm -rf /tmp/custom-crio

# The source-built CRI-O service must point to /usr/local/bin/crio. Do not allow
# an apt-provided /usr/bin/crio service and the custom binary to race each other.
if [[ -x /usr/local/bin/crio ]]; then
  ln -sfn /usr/local/bin/crio /usr/bin/crio
fi
command -v crio

# Replace the source-built executable with the exact release artifact used by
# pkg/kubeadm. The source build installs the service/config/hooks; the release
# asset makes the final executable deterministic and pins the expected commit.
report "Installing pinned custom CRI-O artifact"
CRIO_HAVE="$(crio version --json 2>/dev/null | jq -r '.gitCommit // empty' || true)"
if [[ "$CRIO_HAVE" != "{{.CRIOCommit}}" ]]; then
  curl -fsSL '{{.CRIOAsset}}' -o /tmp/crio
  chmod 0755 /tmp/crio
  CRIO_GOT="$(/tmp/crio version --json 2>/dev/null | jq -r '.gitCommit // empty' || true)"
  [[ "$CRIO_GOT" == "{{.CRIOCommit}}" ]] || {
    echo "CRI-O artifact gitCommit mismatch: got '$CRIO_GOT', expected '{{.CRIOCommit}}'" >&2
    exit 1
  }
  install -m 0755 /tmp/crio /usr/local/bin/crio
  rm -f /tmp/crio
  ln -sfn /usr/local/bin/crio /usr/bin/crio
fi
CRIO_FINAL="$(/usr/local/bin/crio version --json 2>/dev/null | jq -r '.gitCommit // empty' || true)"
[[ "$CRIO_FINAL" == "{{.CRIOCommit}}" ]] || {
  echo "Final CRI-O gitCommit mismatch: got '$CRIO_FINAL', expected '{{.CRIOCommit}}'" >&2
  exit 1
}
report "Pinned custom CRI-O commit verified: ${CRIO_FINAL}"

# -----------------------------------------------------------------------------
# CRI-O configuration
# -----------------------------------------------------------------------------
report "Configuring CRI-O"
systemctl stop kubelet 2>/dev/null || true
systemctl stop crio 2>/dev/null || true
killall -9 crio conmon crun 2>/dev/null || true
rm -rf /run/crio /var/run/crio

mkdir -p /run/crio /var/run/crio /var/lib/crio /var/lib/containers/storage
mkdir -p /etc/crio/crio.conf.d /etc/containers /etc/cni/net.d /opt/cni/bin

# Only [crio.runtime] owns listen/conmon. Do not put listen under [crio.image].
cat > /etc/crio/crio.conf.d/10-paths.conf <<'CRIOPATHS'
[crio.runtime]
  listen = "/var/run/crio/crio.sock"
  conmon = "/usr/libexec/crio/conmon"
CRIOPATHS

cat > /etc/crio/crio.conf.d/999-runc.conf <<'RUNTIME'
[crio]

  [crio.runtime]
    default_runtime = "runc"

    [crio.runtime.runtimes]
      [crio.runtime.runtimes.runc]
        runtime_path = "/usr/local/sbin/runc"
        runtime_type = "oci"
RUNTIME

cat > /etc/containers/policy.json <<'POLICY'
{"default":[{"type":"insecureAcceptAnything"}]}
POLICY

cat > /etc/containers/registries.conf <<'REGCONF'
unqualified-search-registries = ["docker.io"]

[[registry]]
location = "docker.io"
REGCONF

# CRIU runtime configuration used by the customized CRI-O/checkpoint path.
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
CRIUCONF
cp -f /etc/criu/runc.conf /etc/criu/default.conf

# Ensure the custom CRI-O restore hook is available if the custom installation
# supplied it. This is non-fatal because the binary may provide the hook itself.
if [[ -f /usr/libexec/crio/criu-device-restorer.sh ]]; then
  install -D -m 0755 /usr/libexec/crio/criu-device-restorer.sh \
    /usr/local/libexec/crio/criu-device-restorer.sh
fi

# Custom CRIU binary
report "Installing custom CRIU"
CRIU_BIN=/usr/sbin/criu
CRIU_HAVE=""
if command -v criu >/dev/null 2>&1; then
  CRIU_HAVE="$(criu --version 2>&1 | awk '/GitID:/{print $2}' | head -1 || true)"
fi
if [[ "$CRIU_HAVE" != "{{.CRIUGitID}}" ]]; then
  curl -fsSL '{{.CRIUAsset}}' -o /tmp/criu
  chmod 0755 /tmp/criu
  CRIU_GOT="$(/tmp/criu --version 2>&1 | awk '/GitID:/{print $2}' | head -1 || true)"
  [[ "$CRIU_GOT" == "{{.CRIUGitID}}" ]] || {
    echo "CRIU GitID mismatch: got '$CRIU_GOT', expected '{{.CRIUGitID}}'" >&2
    exit 1
  }
  install -m 0755 /tmp/criu "$CRIU_BIN"
  rm -f /tmp/criu
fi
setcap cap_checkpoint_restore+eip "$CRIU_BIN" 2>/dev/null || true
criu --version

# Custom runc, verified against the upstream checksum manifest.
report "Installing runc {{.RuncVersion}}"
RUNC_WANT="{{.RuncVersion}}"
RUNC_BIN=/usr/local/sbin/runc
RUNC_HAVE="$(runc --version 2>/dev/null | awk '/^runc version/{print "v"$3}' || true)"
if [[ "$RUNC_HAVE" != "$RUNC_WANT" ]]; then
  curl -fsSL "https://github.com/opencontainers/runc/releases/download/${RUNC_WANT}/runc.amd64" -o /tmp/runc
  curl -fsSL "https://github.com/opencontainers/runc/releases/download/${RUNC_WANT}/runc.sha256sum" -o /tmp/runc.sha256sum
  EXPECTED_SHA="$(awk '/ runc\.amd64$/{print $1; exit}' /tmp/runc.sha256sum)"
  ACTUAL_SHA="$(sha256sum /tmp/runc | awk '{print $1}')"
  [[ -n "$EXPECTED_SHA" && "$EXPECTED_SHA" == "$ACTUAL_SHA" ]] || {
    echo "runc checksum mismatch" >&2
    exit 1
  }
  install -m 0755 /tmp/runc "$RUNC_BIN"
  rm -f /tmp/runc /tmp/runc.sha256sum
fi
ln -sfn "$RUNC_BIN" /usr/bin/runc
runc --version

# CRI-O uses crun only when explicitly configured. Build it so the same runtime
# stack is available as in pkg/kubeadm, while keeping runc as the default.
report "Building crun"
rm -rf /tmp/crun
git clone --depth=1 https://github.com/containers/crun /tmp/crun
(
  cd /tmp/crun
  ./autogen.sh
  ./configure --disable-man-page
  make -j"$(nproc)"
  make install
)
rm -rf /tmp/crun
command -v crun

# -----------------------------------------------------------------------------
# Start and verify CRI-O before installing Kubernetes
# -----------------------------------------------------------------------------
# crictl from the Kubernetes SIG release, pinned to the Kubernetes version.
# This avoids relying on whichever cri-tools package the CRI-O repository exposes.
report "Installing crictl ${K8S_VERSION}"
CRICTL_VERSION="v${K8S_VERSION}"
case "$(dpkg --print-architecture)" in
  amd64) CRICTL_ARCH=amd64 ;;
  arm64) CRICTL_ARCH=arm64 ;;
  *) echo "Unsupported architecture: $(dpkg --print-architecture)" >&2; exit 1 ;;
esac
curl -fsSL "https://github.com/kubernetes-sigs/cri-tools/releases/download/${CRICTL_VERSION}/crictl-${CRICTL_VERSION}-linux-${CRICTL_ARCH}.tar.gz" -o /tmp/crictl.tar.gz
rm -f /usr/local/bin/crictl
install -d -m 0755 /usr/local/bin
tar -C /usr/local/bin -xzf /tmp/crictl.tar.gz crictl
rm -f /tmp/crictl.tar.gz
install -d -m 0755 /etc
cat > /etc/crictl.yaml <<CRICTL
runtime-endpoint: unix://${CRIO_SOCKET}
image-endpoint: unix://${CRIO_SOCKET}
timeout: 30s
debug: false
CRICTL
chmod 0644 /etc/crictl.yaml
crictl --version
crictl info >/dev/null
report "CRI-O is healthy and crictl is configured"

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

printf 'KUBELET_EXTRA_ARGS=--node-ip=%s\n' "$NODE_IP" > /etc/default/kubelet
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
JOIN_CMD="$(printf '%s' '{{.JoinCommandB64}}' | base64 -d)"

# Join command is generated by the control plane. It is intentionally executed
# only after CRI-O is proven healthy. --cri-socket prevents kubeadm from probing
# an unintended container runtime.
for attempt in 1 2 3 4 5; do
  if bash -c "$JOIN_CMD --cri-socket=unix://${CRIO_SOCKET}"; then
    report "Cluster join succeeded"
    break
  fi
  if [[ "$attempt" == "5" ]]; then
    report "ERROR: kubeadm join failed after 5 attempts"
    exit 1
  fi
  sleep $((attempt * 15))
  restart_crio_and_wait
done

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

# Labels are deliberately not applied here. An EC2 worker normally has no
# kubeconfig/admin.conf, and the provisioning controller already applies the
# authoritative labels/taints after resolving the joined node on the control
# plane. The Labels field remains for API compatibility.

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
