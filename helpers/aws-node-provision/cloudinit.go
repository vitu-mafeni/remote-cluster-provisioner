package awsnodeprovision

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
	// Kubernetes version, e.g. "1.34.2" (no leading "v").
	KubernetesVersion string
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
set -uo pipefail
LOG=/var/log/node-bootstrap.log
exec > >(tee -a "$LOG") 2>&1

STATUS_FILE=/var/lib/node-bootstrap-status

report() { echo "[$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)] $*"; echo "$*" > "$STATUS_FILE"; }
trap 'report "FAILED at line $LINENO: $BASH_COMMAND (exit $?)"' ERR
set -e

report "Bootstrap started"

# ── Idempotency guard ────────────────────────────────────────────────────────
if [ -f /var/lib/node-bootstrap-complete ]; then
  report "Bootstrap already completed, skipping"
  exit 0
fi

# ── Kill unattended-upgrades before touching apt ─────────────────────────────
report "Stopping unattended-upgrades"
systemctl stop unattended-upgrades 2>/dev/null || true
systemctl disable unattended-upgrades 2>/dev/null || true
systemctl stop apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true
systemctl disable apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true

# Wait for any in-progress dpkg/apt lock to be released (up to 5 minutes)
wait_apt() {
  local timeout=300 elapsed=0
  while flock -n /var/lib/dpkg/lock-frontend true 2>/dev/null; do
    # lock is free — proceed
    return 0
  done
  report "Waiting for apt lock to be released..."
  while ! flock -n /var/lib/dpkg/lock-frontend true 2>/dev/null; do
    sleep 5
    elapsed=$((elapsed + 5))
    if [ "$elapsed" -ge "$timeout" ]; then
      report "Killing processes holding apt lock"
      lsof /var/lib/dpkg/lock-frontend 2>/dev/null | awk 'NR>1 {print $2}' | xargs -r kill -9 || true
      sleep 2
      return 0
    fi
  done
}
wait_apt

export DEBIAN_FRONTEND=noninteractive
APT="apt-get -y -o DPkg::Lock::Timeout=300 -o Dpkg::Options::=--force-confnew"

# ── OS update & base packages ────────────────────────────────────────────────
report "Installing base packages"
$APT update
$APT install -y ca-certificates curl gnupg apt-transport-https \
  wireguard wireguard-tools iputils-ping lsof

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
report "Configuring VPN"
mkdir -p /etc/wireguard
cat > /etc/wireguard/wg0.conf <<'WGEOF'
%s
WGEOF
chmod 600 /etc/wireguard/wg0.conf
systemctl enable wg-quick@wg0
systemctl restart wg-quick@wg0

# Wait for VPN tunnel to come up (up to 60s)
report "Waiting for WireGuard tunnel"
for i in $(seq 1 12); do
  ip addr show wg0 2>/dev/null | grep -q 'inet ' && break || true
  sleep 5
done
ip addr show wg0 | grep -q 'inet ' || { report "ERROR: WireGuard tunnel failed to start"; exit 1; }
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
  wait_apt
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
wait_apt
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
		p.KubernetesVersion, p.KubernetesVersion,
		p.KubernetesVersion, p.KubernetesVersion, p.KubernetesVersion,
		p.VpnIP,
		p.JoinCommand,
		labelCmd,
	)
}
