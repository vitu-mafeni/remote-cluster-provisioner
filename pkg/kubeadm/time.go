package kubeadm

import (
	"net"
	"strings"
)

// ControlPlaneTimeSyncStep configures the control plane to serve upstream time
// on the address workers use to reach it.
func ControlPlaneTimeSyncStep(nodeIP string, useVPN bool) string {
	if net.ParseIP(nodeIP) == nil {
		return "echo 'ERROR: invalid control-plane IP for time synchronization' >&2; exit 1"
	}
	ordering := ""
	if useVPN {
		ordering = `mkdir -p /etc/systemd/system/chrony.service.d
printf '[Unit]\nWants=wg-quick@wg0.service\nAfter=wg-quick@wg0.service\n' > /etc/systemd/system/chrony.service.d/10-wireguard.conf
systemctl daemon-reload
`
	}
	return timeSyncSnippet("time.google.com", "bindaddress "+nodeIP+"\nallow\n", ordering, "sudo")
}

// WorkerTimeSyncSnippet uses the same control-plane address as kubeadm join.
func WorkerTimeSyncSnippet(joinCmd, sudo string) string {
	server := NFSServerFromJoinCommand(joinCmd)
	if server == "" {
		return "echo 'ERROR: cannot resolve control-plane endpoint for time synchronization' >&2; exit 1\n"
	}
	return timeSyncSnippet(server, "port 0\n", "", sudo)
}

func timeSyncSnippet(server, serving, ordering, sudo string) string {
	prefix := strings.TrimSpace(sudo + " bash -e -s")
	return prefix + ` <<'RCP_TIME_SYNC'
echo "Configuring host time synchronization with ` + server + `"
timedatectl set-timezone Asia/Seoul
if ! command -v chronyd >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=5 update -qq
  apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=5 install -y chrony
fi
if systemctl is-active --quiet systemd-timesyncd.service || systemctl is-enabled --quiet systemd-timesyncd.service; then
  systemctl disable --now systemd-timesyncd.service
fi
conf="$(mktemp)"
trap 'rm -f "$conf"' EXIT
cat > "$conf" <<'CHRONY_CONF'
server ` + server + ` iburst maxpoll 6
driftfile /var/lib/chrony/chrony.drift
makestep 1.0 -1
rtcsync
` + serving + `CHRONY_CONF
` + ordering + `if ! cmp -s "$conf" /etc/chrony/chrony.conf; then
  install -m 0644 "$conf" /etc/chrony/chrony.conf
  systemctl restart chrony.service
fi
systemctl enable --now chrony.service
if ! timeout 125 chronyc waitsync 60 0.1 0 2; then
  chronyc sources -v >&2 || true
  echo "ERROR: clock did not synchronize with ` + server + `; check NTP reachability (UDP 123)" >&2
  exit 1
fi
RCP_TIME_SYNC
`
}
