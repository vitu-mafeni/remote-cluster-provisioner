package aws

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"strings"

	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
)

const crioSocket = "unix:///var/run/crio/crio.sock"

// CloudInitParams contains the values required to bootstrap an EC2 Kubernetes
// worker/control-plane node. NVIDIA drivers, NVIDIA Container Toolkit, CDI,
// and NVIDIA CRI-O runtime configuration are intentionally NOT part of this
// package. Those components are installed and managed later by GPU Operator.
type CloudInitParams struct {
	// WireGuard client configuration file content.
	WGConfig string
	// VPN IP assigned to this node (used as kubelet node-ip).
	VpnIP string
	// kubeadm join command (without leading "sudo").
	JoinCommand string
	// Kubernetes full version, e.g. "1.35.2" or "v1.35.2".
	KubernetesVersion string
	// Kubernetes minor version, e.g. "1.35" or "v1.35". Used for the apt repo.
	KubernetesMinorVersion string
	// CRI-O minor version, e.g. "1.35" or "v1.35".
	CRIOVersion string
	// NodeName to set as hostname (optional).
	NodeName string
	// Extra labels to apply after join, formatted as "key=value" pairs.
	Labels []string
	// SSHUsername is the OS user that will SSH into the node post-join.
	// When empty, no sudoers entry is written.
	SSHUsername string
	// IsGPUNode only identifies a GPU node to the provisioning controller. It does
	// NOT install or configure any NVIDIA software; GPU Operator owns the complete
	// NVIDIA lifecycle.
	IsGPUNode bool
	// Deprecated: retained for source compatibility with older callers. NVIDIA
	// Container Toolkit installation is intentionally ignored by cloud-init and is
	// performed later by GPU Operator.
	NvidiaContainerToolkitVersion string
}

// BuildUserData renders an idempotent EC2 cloud-init bootstrap script, gzip
// compresses it, and returns base64-encoded user-data.
func BuildUserData(p CloudInitParams) string {
	script := renderBootstrapScript(p)
	var buf bytes.Buffer
	gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		// BestCompression is a valid gzip level; keep this defensive fallback so
		// BuildUserData can never panic on a standard-library configuration error.
		gz = gzip.NewWriter(&buf)
	}
	_, _ = gz.Write([]byte(script))
	_ = gz.Close()
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func renderBootstrapScript(p CloudInitParams) string {
	k8sVersion := strings.TrimPrefix(strings.TrimSpace(p.KubernetesVersion), "v")
	k8sMinor := normalizeMinor(p.KubernetesMinorVersion)
	crioMinor := normalizeMinor(p.CRIOVersion)

	labelCmd := buildLabelCommand(p)
	nopasswdBlock := buildSudoersBlock(p.SSHUsername)
	wgPayload := base64.StdEncoding.EncodeToString([]byte(p.WGConfig))

	return fmt.Sprintf(`#!/bin/bash
set -Eeuo pipefail

LOG=/var/log/node-bootstrap.log
STATUS_FILE=/var/lib/node-bootstrap-status
COMPLETE_FILE=/var/lib/node-bootstrap-complete
LOCK_FILE=/var/lock/node-bootstrap.lock

mkdir -p "$(dirname "$LOG")" "$(dirname "$STATUS_FILE")" "$(dirname "$LOCK_FILE")"
touch "$LOG" "$STATUS_FILE"

# Prevent two cloud-init/user-data executions from provisioning the same node
# concurrently. flock is released automatically if the process exits.
exec 9>"$LOCK_FILE"
flock -n 9 || {
  echo "Another node bootstrap process is already running; exiting."
  exit 0
}

# Cloud-init can run user-data more than once after a failed provisioning attempt.
# Check completion BEFORE destructive cleanup so a completed node is never wiped.
if [ -f "$COMPLETE_FILE" ]; then
  echo "Bootstrap already completed; nothing to do."
  exit 0
fi

err_trap() {
  local code=$?
  local line=${1:-unknown}
  local cmd=${2:-unknown}
  local ts
  ts=$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)
  printf '[%%s] FAILED at line %%s: %%s (exit %%s)\\n' "$ts" "$line" "$cmd" "$code" | tee -a "$LOG" >&2
  printf 'FAILED\\n' > "$STATUS_FILE"
  sync
  exit "$code"
}
trap 'err_trap "$LINENO" "$BASH_COMMAND"' ERR

exec > >(tee -a "$LOG") 2>&1

report() {
  local msg="[$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)] $*"
  echo "$msg"
  printf '%%s\\n' "$*" > "$STATUS_FILE"
  sync
}

fatal_with_logs() {
  local msg="$1"
  report "ERROR: $msg"
  systemctl status crio --no-pager -l 2>/dev/null || true
  journalctl -u crio --no-pager -n 150 2>/dev/null || true
  exit 1
}

# APT operations on fresh Ubuntu EC2 instances can race unattended-upgrades.
wait_for_dpkg() {
  local i
  for i in $(seq 1 60); do
    if ! fuser /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/cache/apt/archives/lock >/dev/null 2>&1; then
      return 0
    fi
    report "Waiting for another apt/dpkg process ($i/60)..."
    sleep 2
  done
  return 1
}

export DEBIAN_FRONTEND=noninteractive

APT=(apt-get -y -o DPkg::Lock::Timeout=120 -o Dpkg::Options::=--force-confnew)
apt_install() {
  wait_for_dpkg || fatal_with_logs "Timed out waiting for apt/dpkg locks"
  DEBIAN_FRONTEND=noninteractive "${APT[@]}" install "$@"
}
apt_update() {
  wait_for_dpkg || fatal_with_logs "Timed out waiting for apt/dpkg locks"
  "${APT[@]}" update
}

wait_for_crio() {
  local attempts=${1:-40}
  local i
  for i in $(seq 1 "$attempts"); do
    if crictl --runtime-endpoint %s --image-endpoint %s info >/dev/null 2>&1; then
      report "CRI-O is ready (attempt $i/$attempts)"
      return 0
    fi
    if [ "$i" -eq 1 ] || [ $((i %% 5)) -eq 0 ]; then
      report "Waiting for CRI-O gRPC readiness ($i/$attempts)..."
    fi
    sleep 3
  done

  report "CRI-O did not become ready"
  systemctl status crio --no-pager -l 2>/dev/null || true
  journalctl -u crio --no-pager -n 200 2>/dev/null || true
  return 1
}

restart_crio_and_wait() {
  report "Restarting CRI-O"
  systemctl daemon-reload
  systemctl restart crio || {
    systemctl status crio --no-pager -l || true
    journalctl -u crio --no-pager -n 200 || true
    return 1
  }
  wait_for_crio 40
}

# ── Versions ─────────────────────────────────────────────────────────────────
K8S_VERSION="v%s"
K8S_MINOR="%s"
CRIO_MINOR="%s"
CRIO_SOCKET="%s"

report "Bootstrap started"
report "Kubernetes version: $K8S_VERSION; Kubernetes repo: $K8S_MINOR; CRI-O repo: $CRIO_MINOR"

# ── Hostname ──────────────────────────────────────────────────────────────────
%s

# ── Passwordless sudo ─────────────────────────────────────────────────────────
%s

# ── Stop automatic apt jobs without killing unrelated processes ───────────────
report "Preparing apt/dpkg"
systemctl stop unattended-upgrades.service apt-daily.service apt-daily-upgrade.service 2>/dev/null || true
systemctl disable unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true
wait_for_dpkg || fatal_with_logs "apt/dpkg remained locked"
dpkg --configure -a
apt_install -f install

# ── Previous incomplete runtime state ─────────────────────────────────────────
# Only clean state that can safely be reconstructed. This is intentionally done
# before installing/replacing CRI-O, but NEVER on a completed node.
report "Cleaning incomplete runtime state"
systemctl stop kubelet.service 2>/dev/null || true
systemctl stop crio.service 2>/dev/null || true
rm -rf /run/crio /var/run/crio /var/lib/kubelet/kubeadm-flags.env
umount -l /var/lib/containers/storage/overlay/*/merged 2>/dev/null || true

# ── Base packages ─────────────────────────────────────────────────────────────
report "Installing base OS packages"
apt_update
apt_install ca-certificates curl gnupg apt-transport-https jq lsof psmisc software-properties-common \
  wireguard wireguard-tools iputils-ping libcap2 libnl-3-200 libbsd0 libgnutls30 \
  criu conmon crun

# ── crictl ────────────────────────────────────────────────────────────────────
# cri-tools releases are versioned by Kubernetes minor, so use v<minor>.0
# rather than the Kubernetes patch version. This avoids the common failure where
# Kubernetes 1.35.2 is requested but cri-tools only publishes v1.35.0.
CRICTL_VERSION="${K8S_MINOR}.0"
CRICTL_TARBALL="crictl-v${CRICTL_VERSION}-linux-amd64.tar.gz"
if ! command -v crictl >/dev/null 2>&1 || ! crictl --version 2>/dev/null | grep -q "v${CRICTL_VERSION}"; then
  report "Installing crictl ${CRICTL_VERSION}"
  rm -f /tmp/$CRICTL_TARBALL
  curl --fail --retry 5 --retry-delay 3 --retry-connrefused \
    -fsSL "https://github.com/kubernetes-sigs/cri-tools/releases/download/v${CRICTL_VERSION}/${CRICTL_TARBALL}" \
    -o "/tmp/$CRICTL_TARBALL"
  tar -xzf "/tmp/$CRICTL_TARBALL" -C /usr/local/bin crictl
  install -m 0755 /usr/local/bin/crictl /usr/bin/crictl
  rm -f "/tmp/$CRICTL_TARBALL"
fi
command -v crictl >/dev/null 2>&1 || fatal_with_logs "crictl is not installed"

cat >/etc/crictl.yaml <<EOF_CRictl
runtime-endpoint: ${CRIO_SOCKET}
image-endpoint: ${CRIO_SOCKET}
timeout: 30
debug: false
EOF_CRictl

# ── Kernel/system settings ────────────────────────────────────────────────────
report "Configuring Kubernetes kernel settings"
cat >/etc/modules-load.d/k8s.conf <<'EOF_MODULES'
overlay
br_netfilter
EOF_MODULES
modprobe overlay
modprobe br_netfilter

cat >/etc/sysctl.d/99-kubernetes.conf <<'EOF_SYSCTL'
net.bridge.bridge-nf-call-iptables=1
net.bridge.bridge-nf-call-ip6tables=1
net.ipv4.ip_forward=1
EOF_SYSCTL
sysctl --system

report "Disabling swap"
swapoff -a
sed -ri '/[[:space:]]swap[[:space:]]/ s/^/#/' /etc/fstab

# ── WireGuard ─────────────────────────────────────────────────────────────────
report "Configuring WireGuard"
mkdir -p /etc/wireguard
printf '%%s' '%s' | base64 -d > /etc/wireguard/wg0.conf
chmod 0600 /etc/wireguard/wg0.conf
systemctl enable wg-quick@wg0.service
systemctl restart wg-quick@wg0.service

report "Waiting for WireGuard IPv4 address"
VPN_UP=false
for i in $(seq 1 30); do
  if ip -4 addr show dev wg0 2>/dev/null | grep -q 'inet '; then
    VPN_UP=true
    break
  fi
  sleep 2
done
if [ "$VPN_UP" != "true" ]; then
  wg show wg0 2>&1 || true
  ip link show wg0 2>&1 || true
  fatal_with_logs "WireGuard wg0 did not obtain an IPv4 address"
fi

# Ensure the configured VPN IP is actually present before kubelet starts.
if ! ip -4 addr show dev wg0 | grep -qw '%s'; then
  report "WARNING: expected VPN IP %s is not currently assigned to wg0"
fi
report "WireGuard is ready"

# ── CRI-O repository ──────────────────────────────────────────────────────────
report "Configuring CRI-O ${CRIO_MINOR} repository"
install -m 0755 -d /etc/apt/keyrings
rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg
curl --fail --retry 5 --retry-delay 3 --retry-connrefused -fsSL \
  "https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v${CRIO_MINOR}/deb/Release.key" \
  | gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg
printf 'deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%%s/deb/ /\\n' \
  "$CRIO_MINOR" > /etc/apt/sources.list.d/cri-o.list
apt_update

# ── CRI-O package ─────────────────────────────────────────────────────────────
report "Installing CRI-O"
apt_install cri-o

# ── Runtime configuration ─────────────────────────────────────────────────────
# IMPORTANT: do not configure NVIDIA here. GPU Operator owns NVIDIA runtime,
# toolkit and CDI configuration and will add those drop-ins after the node joins.
report "Configuring CRI-O base runtime"
mkdir -p /etc/crio/crio.conf.d /etc/cni/net.d /opt/cni/bin /etc/containers

cat >/etc/crio/crio.conf.d/10-paths.conf <<'EOF_CRIO_PATHS'
[crio.runtime]
listen = "/var/run/crio/crio.sock"
conmon = "/usr/bin/conmon"
EOF_CRIO_PATHS

cat >/etc/crio/crio.conf.d/999-runc.conf <<'EOF_CRIO_RUNC'
[crio]

  [crio.runtime]
    default_runtime = "runc"

    [crio.runtime.runtimes]
      [crio.runtime.runtimes.runc]
        runtime_path = "/usr/local/sbin/runc"
        runtime_type = "oci"
EOF_CRIO_RUNC

# Short image names are used by some bootstrap/debug workflows. Keep the policy
# explicit; do not add insecure registry exceptions.
cat >/etc/containers/registries.conf <<'EOF_REGISTRIES'
unqualified-search-registries = ["docker.io"]

[[registry]]
location = "docker.io"
EOF_REGISTRIES

# CRI-O must be able to find the custom CRIU restorer used by checkpoint/restore.
if [ -f /usr/libexec/crio/criu-device-restorer.sh ]; then
  install -D -m 0755 /usr/libexec/crio/criu-device-restorer.sh \
    /usr/local/libexec/crio/criu-device-restorer.sh
elif [ -f /usr/local/libexec/crio/criu-device-restorer.sh ]; then
  chmod 0755 /usr/local/libexec/crio/criu-device-restorer.sh
else
  report "WARNING: CRIU device-restorer hook was not provided by the installed CRI-O package"
fi

# ── CRIU configuration ────────────────────────────────────────────────────────
report "Configuring CRIU"
mkdir -p /etc/criu
cat >/etc/criu/runc.conf <<'EOF_CRIU_RUNC'
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
EOF_CRIU_RUNC

cat >/etc/criu/default.conf <<'EOF_CRIU_DEFAULT'
tcp-close
skip-in-flight
ghost-limit 100M
enable-external-masters
external mnt[]
irmap-scan-path /home/jovyan
irmap-scan-path /usr
irmap-scan-path /opt/conda
irmap-scan-path /opt/remote-dev
EOF_CRIU_DEFAULT

# ── Custom CRIU ────────────────────────────────────────────────────────────────
report "Installing required custom CRIU"
CRIU_BIN=$(command -v criu || true)
[ -n "$CRIU_BIN" ] || CRIU_BIN=/usr/sbin/criu
WANTED_CRIU_GITID="%s"
CURRENT_CRIU_GITID=$(criu --version 2>&1 | awk '/GitID:/{print $2; exit}' || true)
if [ "$CURRENT_CRIU_GITID" != "$WANTED_CRIU_GITID" ]; then
  curl --fail --retry 5 --retry-delay 3 --retry-connrefused -fsSL \
    "%s" -o /tmp/criu.custom
  chmod 0755 /tmp/criu.custom
  INSTALLED_CRIU_GITID=$(/tmp/criu.custom --version 2>&1 | awk '/GitID:/{print $2; exit}' || true)
  [ "$INSTALLED_CRIU_GITID" = "$WANTED_CRIU_GITID" ] || \
    fatal_with_logs "Downloaded CRIU GitID $INSTALLED_CRIU_GITID does not match expected $WANTED_CRIU_GITID"
  install -m 0755 /tmp/criu.custom "$CRIU_BIN"
  rm -f /tmp/criu.custom
fi
setcap cap_checkpoint_restore+eip "$CRIU_BIN" 2>/dev/null || true
criu --version
criu check >/tmp/criu-check.log 2>&1 || report "WARNING: criu check reported issues; see /tmp/criu-check.log"

# ── Custom runc ───────────────────────────────────────────────────────────────
report "Installing required runc custom"
WANTED_RUNC_VERSION="%s"
RUNC_BIN=/usr/local/sbin/runc
CURRENT_RUNC_VERSION=$(runc --version 2>/dev/null | awk '/^runc version/{print "v"$3; exit}' || true)
if [ "$CURRENT_RUNC_VERSION" != "$WANTED_RUNC_VERSION" ]; then
  curl --fail --retry 5 --retry-delay 3 --retry-connrefused -fsSL \
    "https://github.com/opencontainers/runc/releases/download/${WANTED_RUNC_VERSION}/runc.amd64" \
    -o /tmp/runc
  curl --fail --retry 5 --retry-delay 3 --retry-connrefused -fsSL \
    "https://github.com/opencontainers/runc/releases/download/${WANTED_RUNC_VERSION}/runc.sha256sum" \
    -o /tmp/runc.sha256sum
  EXPECTED_SHA=$(awk '$2 == "runc.amd64" {print $1; exit}' /tmp/runc.sha256sum)
  ACTUAL_SHA=$(sha256sum /tmp/runc | awk '{print $1}')
  [ -n "$EXPECTED_SHA" ] && [ "$EXPECTED_SHA" = "$ACTUAL_SHA" ] || \
    fatal_with_logs "runc checksum verification failed"
  install -D -m 0755 /tmp/runc "$RUNC_BIN"
  ln -sfn "$RUNC_BIN" /usr/bin/runc
  rm -f /tmp/runc /tmp/runc.sha256sum
fi
ln -sfn "$RUNC_BIN" /usr/bin/runc
runc --version

# ── Custom CRI-O binary ───────────────────────────────────────────────────────
report "Installing required custom CRI-O binary"
WANTED_CRIO_COMMIT="%s"
CURRENT_CRIO_COMMIT=$(crio version --json 2>/dev/null | jq -r '.gitCommit // empty' || true)
if [ "$CURRENT_CRIO_COMMIT" != "$WANTED_CRIO_COMMIT" ]; then
  curl --fail --retry 5 --retry-delay 3 --retry-connrefused -fsSL \
    "%s" -o /tmp/crio.custom
  chmod 0755 /tmp/crio.custom
  DOWNLOADED_CRIO_COMMIT=$(/tmp/crio.custom version --json 2>/dev/null | jq -r '.gitCommit // empty' || true)
  [ "$DOWNLOADED_CRIO_COMMIT" = "$WANTED_CRIO_COMMIT" ] || \
    fatal_with_logs "Downloaded CRI-O commit $DOWNLOADED_CRIO_COMMIT does not match expected $WANTED_CRIO_COMMIT"

  systemctl stop crio.service 2>/dev/null || true
  install -m 0755 /tmp/crio.custom /usr/local/bin/crio
  ln -sfn /usr/local/bin/crio /usr/bin/crio
  rm -f /tmp/crio.custom
  systemctl daemon-reload
fi
ln -sfn /usr/local/bin/crio /usr/bin/crio

# Verify the service resolves to the intended custom binary. Prefer the source
# install path but tolerate an existing distro unit because /usr/bin/crio is a
# symlink to the same binary.
ACTUAL_CRIO_COMMIT=$(crio version --json 2>/dev/null | jq -r '.gitCommit // empty' || true)
[ "$ACTUAL_CRIO_COMMIT" = "$WANTED_CRIO_COMMIT" ] || \
  fatal_with_logs "Active CRI-O binary commit $ACTUAL_CRIO_COMMIT does not match expected $WANTED_CRIO_COMMIT"

# ── Clean storage after binary replacement ────────────────────────────────────
# The custom binary may use a different storage implementation/version than the
# distro package. Never reuse incompatible metadata after a binary swap.
report "Resetting CRI-O runtime state after binary installation"
systemctl stop kubelet.service crio.service 2>/dev/null || true
umount -l /var/lib/containers/storage/overlay/*/merged 2>/dev/null || true
rm -rf /run/crio /var/run/crio /var/lib/crio /var/lib/containers/storage
mkdir -p /var/lib/crio /var/lib/containers/storage /run/crio /var/run/crio

# Do not start kubelet yet. kubeadm join will initialize it after CRI-O has been
# verified healthy.
systemctl disable kubelet.service 2>/dev/null || true
systemctl enable crio.service
systemctl restart crio.service || {
  systemctl status crio --no-pager -l || true
  journalctl -u crio --no-pager -n 200 || true
  false
}
wait_for_crio 40 || fatal_with_logs "CRI-O failed initial readiness check"

# ── Kubernetes repository/packages ────────────────────────────────────────────
report "Installing Kubernetes ${k8sVersion} packages"
install -m 0755 -d /etc/apt/keyrings
rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg
curl --fail --retry 5 --retry-delay 3 --retry-connrefused -fsSL \
  "https://pkgs.k8s.io/core:/stable:/v${k8sMinor}/deb/Release.key" \
  | gpg --batch --yes --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
printf 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%%s/deb/ /\\n' \
  "$k8sMinor" > /etc/apt/sources.list.d/kubernetes.list
apt_update
apt_install kubelet="${k8sVersion}-*" kubeadm="${k8sVersion}-*" kubectl="${k8sVersion}-*" --allow-change-held-packages --allow-downgrades
apt-mark hold kubelet kubeadm kubectl

# ── Kubelet dependency/configuration ─────────────────────────────────────────
report "Configuring kubelet"
printf 'KUBELET_EXTRA_ARGS=--node-ip=%%s\\n' '%s' > /etc/default/kubelet
mkdir -p /etc/systemd/system/kubelet.service.d
cat >/etc/systemd/system/kubelet.service.d/10-crio.conf <<'EOF_KUBELET_CRIO'
[Unit]
After=crio.service
Requires=crio.service
EOF_KUBELET_CRIO
systemctl daemon-reload
systemctl enable kubelet.service
systemctl stop kubelet.service 2>/dev/null || true

# ── Final CRI-O validation immediately before join ────────────────────────────
# This is intentionally a real CRI gRPC check, not just a socket existence test.
# It prevents kubelet/GPU Operator from being started against a dead runtime.
report "Performing final CRI-O validation"
wait_for_crio 40 || fatal_with_logs "CRI-O is not healthy immediately before kubeadm join"

# ── kubeadm join ──────────────────────────────────────────────────────────────
report "Joining Kubernetes cluster"
JOIN_SUCCESS=false
for attempt in $(seq 1 5); do
  report "kubeadm join attempt $attempt/5"
  if %s --cri-socket=%s; then
    JOIN_SUCCESS=true
    break
  fi
  if [ "$attempt" -lt 5 ]; then
    report "kubeadm join failed; revalidating CRI-O before retry"
    wait_for_crio 20 || fatal_with_logs "CRI-O became unhealthy after kubeadm join failure"
    sleep $((attempt * 15))
  fi
done
$JOIN_SUCCESS || fatal_with_logs "kubeadm join failed after 5 attempts"

# kubeadm writes the actual node identity and kubelet configuration. Verify that
# kubelet becomes active and CRI-O remains healthy after the join.
report "Waiting for kubelet after kubeadm join"
for i in $(seq 1 40); do
  if systemctl is-active --quiet kubelet.service; then
    break
  fi
  sleep 3
done
systemctl is-active --quiet kubelet.service || {
  systemctl status kubelet --no-pager -l || true
  journalctl -u kubelet --no-pager -n 150 || true
  fatal_with_logs "kubelet did not become active after kubeadm join"
}
wait_for_crio 40 || fatal_with_logs "CRI-O became unhealthy after kubeadm join"

# ── Labels/taints ─────────────────────────────────────────────────────────────
%s

# ── Completion ────────────────────────────────────────────────────────────────
# Only mark complete after kubelet and CRI-O have both survived the join.
touch "$COMPLETE_FILE"
report "Bootstrap complete"
`,
		crioSocket,
		crioSocket,
		k8sVersion,
		k8sMinor,
		crioMinor,
		crioSocket,
		buildHostnameBlock(p.NodeName),
		nopasswdBlock,
		wgPayload,
		p.VpnIP,
		p.VpnIP,
		kubeadm.CriuGitID,
		kubeadm.CriuAsset,
		kubeadm.RuncVersion,
		kubeadm.CrioCommit,
		kubeadm.CrioAsset,
		p.VpnIP,
		p.JoinCommand,
		crioSocket,
		labelCmd,
	)
}

func normalizeMinor(version string) string {
	v := strings.TrimPrefix(strings.TrimSpace(version), "v")
	parts := strings.Split(v, ".")
	if len(parts) >= 2 {
		return parts[0] + "." + parts[1]
	}
	return v
}

func buildSudoersBlock(username string) string {
	username = strings.TrimSpace(username)
	if username == "" {
		return ""
	}
	// Username comes from the controller configuration. Reject shell metacharacters
	// rather than embedding an unsafe value into /etc/sudoers.
	if strings.ContainsAny(username, "'\\\n\r \t") {
		return "report \"ERROR: invalid SSH username for sudoers configuration\"; exit 1"
	}
	return fmt.Sprintf(`install -m 0440 /dev/null /etc/sudoers.d/nopasswd-%s
printf '%%s ALL=(ALL) NOPASSWD:ALL\\n' '%s' > /etc/sudoers.d/nopasswd-%s
visudo -cf /etc/sudoers.d/nopasswd-%s
`, username, username, username, username)
}

func buildLabelCommand(p CloudInitParams) string {
	labels := make([]string, 0, len(p.Labels)+1)
	for _, label := range p.Labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if strings.ContainsAny(label, "'\\\n\r\t ") {
			return `report "WARNING: invalid node label supplied to cloud-init; skipping labels"`
		}
		labels = append(labels, label)
	}
	if p.IsGPUNode {
		labels = append(labels, "gpu=on")
	}
	if len(labels) == 0 {
		return `report "No cloud-init node labels requested; labels will be managed by the provisioning controller"`
	}

	// Workers do not have /etc/kubernetes/admin.conf, so cloud-init cannot safely
	// label/taint a worker through kubectl. The controller already has cluster-admin
	// credentials and should apply these labels/taints after JoinWorkerNode. We keep
	// this block as a diagnostic rather than silently failing bootstrap.
	return fmt.Sprintf(`report "Requested node labels/taints are deferred to the provisioning controller: %s"`, strings.Join(labels, ","))
}

func buildHostnameBlock(nodeName string) string {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return `report "No explicit hostname requested; keeping cloud image hostname"`
	}
	if strings.ContainsAny(nodeName, "'\\\n\r\t /") {
		return `report "ERROR: invalid NodeName supplied"; exit 1`
	}
	return fmt.Sprintf(`hostnamectl set-hostname --static '%s'
report "Hostname set to %s"`, nodeName, nodeName)
}
