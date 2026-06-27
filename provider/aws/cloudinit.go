package aws

import (
	"encoding/base64"
	"fmt"
	"strings"

	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
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
	// SSHUsername is the OS user that will SSH into the node post-join
	// (e.g. for image pre-pull).  NOPASSWD sudo is granted to this user so
	// that non-interactive SSH sessions can run privileged commands.
	// When empty, no sudoers entry is written.
	SSHUsername string
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

	nopasswdBlock := ""
	if p.SSHUsername != "" {
		nopasswdBlock = fmt.Sprintf(
			"echo '%s ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/nopasswd-%s\nchmod 0440 /etc/sudoers.d/nopasswd-%s\n",
			p.SSHUsername, p.SSHUsername, p.SSHUsername,
		)
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

# ── Passwordless sudo for SSH user ──────────────────────────────────────────
# Written only when SSHUsernameOverride is set on the NodeProvision.
%s
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
  rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg
  curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
    | gpg --dearmor | tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null
  echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
    > /etc/apt/sources.list.d/cri-o.list
  $APT update
  $APT install -y jq criu crun conmon cri-o
  CRUN_VER=$(curl -fsSL https://api.github.com/repos/containers/crun/releases/latest 2>/dev/null | jq -r .tag_name 2>/dev/null)
  { [ -n "$CRUN_VER" ] && [ "$CRUN_VER" != "null" ]; } || CRUN_VER=1.17
  curl -fsSL "https://github.com/containers/crun/releases/download/${CRUN_VER}/crun-${CRUN_VER}-linux-amd64" \
    -o /usr/local/bin/crun
  chmod 0755 /usr/local/bin/crun
  cp -f /usr/local/bin/crun /usr/bin/crun
  crun --version
  mkdir -p /etc/crio/crio.conf.d
  printf '[crio.runtime.runtimes.crun]\nruntime_path = "/usr/local/bin/crun"\nruntime_type = "oci"\nruntime_root = "/run/crun"\n' \
    > /etc/crio/crio.conf.d/10-crun.conf
  systemctl enable crio --now || { journalctl -xeu crio.service --no-pager >&2; false; }
fi

# Ensure criu runtime dependencies are installed (libnl, libcap, libbsd, libgnutls)
$APT install -y libcap2 libnl-3-200 libbsd0 libgnutls30

# Swap in custom criu (device-restore-with-hook), idempotent on GitID
WANT="%s"
CRIU_BIN=$(command -v criu || echo /usr/sbin/criu)
HAVE=$(criu --version 2>&1 | awk '/GitID:/{print $2}')
if [ "$HAVE" = "$WANT" ]; then
  echo "custom criu $WANT already at $CRIU_BIN, skipping"
else
  curl -fsSL %s -o /tmp/criu
  chmod 0755 /tmp/criu
  GOT=$(/tmp/criu --version 2>&1 | awk '/GitID:/{print $2}')
  [ "$GOT" = "$WANT" ] && \
  install -m 0755 /tmp/criu "$CRIU_BIN" && \
  rm -f /tmp/criu && \
  echo "installed custom criu $WANT at $CRIU_BIN"
fi
criu --version || true
# Grant CAP_CHECKPOINT_RESTORE capability so criu can run
sudo setcap cap_checkpoint_restore+eip /usr/sbin/criu || true
criu check 2>&1 | head -1 || true

# Install latest runc, idempotent on version
WANT="%s"
RUNC_BIN=$(command -v runc || echo /usr/local/sbin/runc)
HAVE=$(runc --version 2>/dev/null | awk '/^runc version/{print "v"$3}')
if [ "$HAVE" = "$WANT" ]; then
  echo "runc $WANT already installed at $RUNC_BIN, skipping"
else
  curl -fsSL https://github.com/opencontainers/runc/releases/download/$WANT/runc.amd64 -o /tmp/runc
  curl -fsSL https://github.com/opencontainers/runc/releases/download/$WANT/runc.sha256sum -o /tmp/runc.sha256sum
  WSHA=$(awk '/ runc\.amd64$/{print $1}' /tmp/runc.sha256sum)
  GSHA=$(sha256sum /tmp/runc | awk '{print $1}')
  [ -n "$WSHA" ] && [ "$WSHA" = "$GSHA" ] && \
  install -m 0755 /tmp/runc "$RUNC_BIN" && \
  rm -f /tmp/runc /tmp/runc.sha256sum && \
  echo "installed runc $WANT at $RUNC_BIN"
fi
runc --version || true

WANT=%s
HAVE=$(crio version --json 2>/dev/null | jq -r .gitCommit)
if [ "$HAVE" != "$WANT" ]; then
  curl -fsSL %s -o /tmp/crio
  chmod 0755 /tmp/crio
  GOT=$(/tmp/crio version --json | jq -r .gitCommit)
  if [ "$GOT" = "$WANT" ]; then
    systemctl stop crio
    install -m 0755 /tmp/crio /usr/bin/crio
    systemctl daemon-reload
    systemctl restart crio || { journalctl -xeu crio.service --no-pager >&2; false; }
    rm -f /tmp/crio
  fi
fi
test -f /usr/local/libexec/crio/criu-device-restorer.sh || \
  install -D -m 0755 /usr/libexec/crio/criu-device-restorer.sh \
  /usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || \
  echo "WARN: criu-device-restorer.sh missing; restore-from-file may fail"
systemctl enable crio --now || { journalctl -xeu crio.service --no-pager >&2; false; }
systemctl restart crio || { journalctl -xeu crio.service --no-pager >&2; false; }
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
# Always restart CRI-O here to pick up any config changes made above
# (e.g. new crun path). A passive "is-active || start" misses the case where
# CRI-O is active but using stale config from a previous failed provisioning run.
report "Restarting CRI-O and waiting for socket readiness"
systemctl daemon-reload
systemctl restart crio || { journalctl -xeu crio.service --no-pager >&2; false; }

# Wait for CRI-O socket to be ready (up to 60s)
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if test -S /var/run/crio/crio.sock; then
    report "CRI-O socket ready"
    break
  fi
  echo "Waiting for CRI-O socket ($i/20)..."
  sleep 3
done
test -S /var/run/crio/crio.sock || { journalctl -xeu crio.service --no-pager -n 100 >&2; false; }

report "Joining cluster"
for attempt in 1 2 3 4 5; do
  # Append --cri-socket to use CRI-O instead of defaulting to containerd
  if %s --cri-socket=unix:///var/run/crio/crio.sock; then
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
`, nopasswdBlock,
		wgConf,
		p.CRIOVersion, p.CRIOVersion,
		kubeadm.CriuGitID, kubeadm.CriuAsset,
		kubeadm.RuncVersion,
		kubeadm.CrioCommit, kubeadm.CrioAsset,
		p.KubernetesMinorVersion, p.KubernetesMinorVersion,
		p.KubernetesVersion, p.KubernetesVersion, p.KubernetesVersion,
		p.VpnIP,
		p.JoinCommand,
		labelCmd,
	)
}
