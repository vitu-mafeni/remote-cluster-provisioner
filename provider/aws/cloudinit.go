package aws

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// CloudInitParams holds all values needed to render the bootstrap user-data script.
type CloudInitParams struct {
	// WireGuard client configuration file content.
	WGConfig string
	// VPN IP assigned to this node (used as kubelet node-ip).
	VpnIP string
	// kubeadm join command (without leading "sudo").
	JoinCommand string
	// Kubernetes full version, e.g. "1.34.2" (no leading "v"). Used for package pin.
	KubernetesVersion string
	// Kubernetes minor version, e.g. "1.34". Used for the apt repo URL.
	KubernetesMinorVersion string
	// CRI-O minor version, e.g. "1.34".
	CRIOVersion string
	// NodeName to set as hostname (optional).
	NodeName string
	// Extra labels to apply after join, formatted as "key=value" pairs.
	Labels []string
}

// BuildUserData renders an idempotent cloud-init bash script and returns it
// base64-encoded, ready for use as EC2 UserData.
func BuildUserData(p CloudInitParams) string {
	script := renderBootstrapScript(p)
	return base64.StdEncoding.EncodeToString([]byte(script))
}

func renderBootstrapScript(p CloudInitParams) string {
	labelCmd := ""
	if len(p.Labels) > 0 {
		labelCmd = fmt.Sprintf("kubectl label node \"$(hostname)\" %s --overwrite 2>/dev/null || true", strings.Join(p.Labels, " "))
	}

	// Escape the WireGuard config for embedding in heredoc.
	wgConf := strings.ReplaceAll(p.WGConfig, `\`, `\\`)

	return fmt.Sprintf(`#!/bin/bash
set -eEuo pipefail

LOG=/var/log/node-bootstrap.log
STATUS_FILE=/var/lib/node-bootstrap-status

# Write directly to log file from the ERR trap to avoid tee buffering on exit.
err_trap() {
  local code=$? line=$1 cmd=$2
  echo "[$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)] FAILED at line $line: $cmd (exit $code)" \
    | tee -a "$LOG" >&2
  echo "FAILED" > "$STATUS_FILE"
  sync
}
trap 'err_trap "$LINENO" "$BASH_COMMAND"' ERR

exec > >(tee -a "$LOG") 2>&1

report() {
  local msg="[$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)] $*"
  echo "$msg"
  echo "$*" > "$STATUS_FILE"
  sync
}

report "Bootstrap started"

# ── Idempotency guard ────────────────────────────────────────────────────────
if [ -f /var/lib/node-bootstrap-complete ]; then
  report "Bootstrap already completed, skipping"
  exit 0
fi

# ── Kill anything holding apt/dpkg locks ────────────────────────────────────
report "Disabling unattended-upgrades"
systemctl stop unattended-upgrades apt-daily.service apt-daily-upgrade.service 2>/dev/null || true
systemctl disable unattended-upgrades apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true
# Give the services a moment to release locks
sleep 3
# Force-kill any remaining apt/dpkg processes
pkill -9 -x unattended-upgrades 2>/dev/null || true
pkill -9 -f "apt-get" 2>/dev/null || true
sleep 2

# Repair any interrupted dpkg state from prior runs
dpkg --configure -a 2>/dev/null || true

# Remove stale lock files (safe on a fresh instance that just booted)
rm -f /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/cache/apt/archives/lock 2>/dev/null || true

export DEBIAN_FRONTEND=noninteractive
APT="apt-get -y -o DPkg::Lock::Timeout=120 -o Dpkg::Options::=--force-confnew"

# ── OS base packages ─────────────────────────────────────────────────────────
report "Running apt-get update"
$APT update

report "Installing base packages"
$APT install -y ca-certificates curl gnupg apt-transport-https lsof

report "Installing WireGuard"
$APT install -y wireguard wireguard-tools iputils-ping

# ── Kernel modules & sysctl ─────────────────────────────────────────────────
report "Configuring kernel"
cat > /etc/modules-load.d/k8s.conf <<'EOF'
overlay
br_netfilter
EOF
modprobe overlay
modprobe br_netfilter

cat > /etc/sysctl.d/k8s.conf <<'EOF'
net.bridge.bridge-nf-call-iptables=1
net.bridge.bridge-nf-call-ip6tables=1
net.ipv4.ip_forward=1
EOF
sysctl --system

# ── Disable swap ─────────────────────────────────────────────────────────────
swapoff -a
sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab

# ── WireGuard VPN ────────────────────────────────────────────────────────────
report "Configuring WireGuard VPN"
mkdir -p /etc/wireguard
cat > /etc/wireguard/wg0.conf <<'WGEOF'
%s
WGEOF
chmod 600 /etc/wireguard/wg0.conf
systemctl enable wg-quick@wg0
systemctl restart wg-quick@wg0

# Wait for VPN tunnel to come up (up to 60s)
report "Waiting for WireGuard tunnel"
vpn_up=false
for i in $(seq 1 12); do
  if ip addr show wg0 2>/dev/null | grep -q 'inet '; then
    vpn_up=true
    break
  fi
  sleep 5
done
if [ "$vpn_up" != "true" ]; then
  report "ERROR: WireGuard tunnel failed to start"
  wg show wg0 2>&1 || true
  ip link show wg0 2>&1 || true
  exit 1
fi
report "VPN tunnel established"

# ── CRI-O ────────────────────────────────────────────────────────────────────
report "Installing CRI-O"
if ! which crio > /dev/null 2>&1; then
  mkdir -p /etc/apt/keyrings
  rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg
  curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
    | gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg
  echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
    > /etc/apt/sources.list.d/cri-o.list
  $APT update
  $APT install -y cri-o
fi
systemctl enable crio --now
systemctl restart crio
report "CRI-O installed"

# ── Kubernetes packages ──────────────────────────────────────────────────────
report "Installing Kubernetes packages"
rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg
mkdir -p /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key \
  | gpg --dearmor | tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null
echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] \
https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" \
  > /etc/apt/sources.list.d/kubernetes.list
$APT update
$APT install -y \
  kubelet=%s-* kubeadm=%s-* kubectl=%s-* \
  --allow-change-held-packages --allow-downgrades
apt-mark hold kubelet kubeadm kubectl
systemctl enable kubelet
report "Kubernetes packages installed"

# ── Kubelet node-ip ──────────────────────────────────────────────────────────
echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' > /etc/default/kubelet
systemctl daemon-reload
systemctl restart kubelet

# ── Join cluster ─────────────────────────────────────────────────────────────
report "Joining cluster"
for attempt in 1 2 3 4 5; do
  if %s; then
    report "Cluster join succeeded"
    break
  fi
  [ "$attempt" -eq 5 ] && { report "ERROR: cluster join failed after 5 attempts"; exit 1; }
  sleep $((attempt * 30))
done

# ── Labels ───────────────────────────────────────────────────────────────────
%s

# ── Done ─────────────────────────────────────────────────────────────────────
touch /var/lib/node-bootstrap-complete
report "Bootstrap complete"
`, wgConf, p.CRIOVersion, p.CRIOVersion,
		p.KubernetesMinorVersion, p.KubernetesMinorVersion,
		p.KubernetesVersion, p.KubernetesVersion, p.KubernetesVersion,
		p.VpnIP,
		p.JoinCommand,
		labelCmd,
	)
}
