#!/usr/bin/env bash
# Read-only preflight: checks whether an existing Docker/containerd (or other
# leftover state) on a node could cause problems when the provisioner installs
# CRI-O via cnlab-runtime. Changes nothing.
#
# Usage: sudo [REGISTRY=host[:port]] [API_ENDPOINT=host:port] \
#          ./preflight-runtime-check.sh [/path/to/cnlab-runtime_*.deb]
#   deb           enables the exact file-overlap check (4)
#   REGISTRY      runtime artifact registry to test (default ghcr.io)
#   API_ENDPOINT  control-plane address to test for reachability (18)
#
# Exit code: 0 = no problems found, 1 = at least one WARN/FAIL (see summary).

set -u

DEB="${1:-}"
WARNS=0
FAILS=0

ok()   { printf '  [ OK ] %s\n' "$*"; }
info() { printf '  [INFO] %s\n' "$*"; }
warn() { printf '  [WARN] %s\n' "$*"; WARNS=$((WARNS + 1)); }
fail() { printf '  [FAIL] %s\n' "$*"; FAILS=$((FAILS + 1)); }
hdr()  { printf '\n== %s\n' "$*"; }

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root (sudo) for accurate results." >&2
  exit 2
fi

have() { command -v "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------------------
hdr "1. Container runtimes installed / running"
for svc in docker containerd crio; do
  state=$(systemctl is-active "$svc" 2>/dev/null || true)
  enabled=$(systemctl is-enabled "$svc" 2>/dev/null || true)
  case "$svc:$state" in
    crio:active)         info "crio is already active (enabled=$enabled)" ;;
    crio:*)              ok   "crio not running" ;;
    *:active)            warn "$svc is active (enabled=$enabled) - it will keep running beside CRI-O" ;;
    *)                   ok   "$svc not running (state=${state:-unknown})" ;;
  esac
done
for sock in /run/containerd/containerd.sock /var/run/docker.sock /var/run/crio/crio.sock; do
  [ -S "$sock" ] && info "socket present: $sock"
done

# ---------------------------------------------------------------------------
hdr "2. Existing containers that would be orphaned from Kubernetes"
if have docker && docker info >/dev/null 2>&1; then
  n=$(docker ps -q 2>/dev/null | wc -l)
  [ "$n" -gt 0 ] && warn "docker: $n running container(s)" || ok "docker: no running containers"
fi
if have ctr && [ -S /run/containerd/containerd.sock ]; then
  for ns in k8s.io moby default; do
    n=$(ctr -n "$ns" tasks ls 2>/dev/null | tail -n +2 | wc -l)
    [ "$n" -gt 0 ] && warn "containerd ns '$ns': $n running task(s)"
  done
  ok "containerd namespaces checked (k8s.io, moby, default)"
fi

# ---------------------------------------------------------------------------
hdr "3. Ownership of runc / containerd / crictl (deb is installed with --force-overwrite)"
found_bin=0
for f in /usr/bin/runc /usr/local/bin/runc /usr/sbin/runc \
         /usr/bin/containerd /usr/bin/crictl /usr/local/bin/crictl \
         /usr/bin/crio /usr/local/bin/crio; do
  [ -e "$f" ] || continue
  found_bin=1
  owner=$(dpkg -S "$f" 2>/dev/null | head -1 | cut -d: -f1)
  ver=$("$f" --version 2>&1 | head -1)
  if [ -n "$owner" ]; then
    warn "$f owned by package '$owner' ($ver) - may be overwritten"
  else
    info "$f not owned by any package ($ver)"
  fi
done
[ "$found_bin" -eq 0 ] && ok "no runc/containerd/crictl/crio binaries found"

# ---------------------------------------------------------------------------
hdr "4. Exact file overlap with the cnlab-runtime deb"
if [ -n "$DEB" ] && [ -f "$DEB" ]; then
  overlap=0
  while read -r f; do
    p=$(dpkg -S "$f" 2>/dev/null | head -1 | cut -d: -f1)
    if [ -n "$p" ] && [ "$p" != "cnlab-runtime" ]; then
      warn "$f would overwrite file owned by '$p'"
      overlap=1
    fi
  done < <(dpkg -c "$DEB" | awk '{print $6}' | sed 's|^\./|/|' | grep -v '/$')
  [ "$overlap" -eq 0 ] && ok "no overlap with files owned by other packages"
else
  info "skipped (pass the cnlab-runtime .deb path as the first argument)"
fi

# ---------------------------------------------------------------------------
hdr "5. Related packages"
pkgs=$(dpkg -l 2>/dev/null | awk '/^ii/ {print $2" "$3}' \
  | grep -Ei '^(docker|containerd|runc|cri-o|podman|buildah|skopeo|kubelet|kubeadm|kubectl|cnlab-runtime)')
if [ -n "$pkgs" ]; then
  echo "$pkgs" | sed 's/^/         /'
  echo "$pkgs" | grep -Eiq '^(containerd|docker|runc)' && warn "Docker/containerd/runc packages installed (see checks 3-4)"
else
  ok "none installed"
fi

# ---------------------------------------------------------------------------
hdr "6. Docker iptables / bridge interference with pod networking"
if have iptables; then
  pol=$(iptables -S FORWARD 2>/dev/null | awk '/^-P FORWARD/ {print $3}')
  if [ "$pol" = "DROP" ]; then
    warn "iptables FORWARD policy is DROP (Docker sets this) - can block pod traffic"
  else
    ok "iptables FORWARD policy: ${pol:-unknown}"
  fi
  iptables -S 2>/dev/null | grep -q 'DOCKER-USER' && warn "DOCKER-USER chain present"
fi
br=$(ip -br link show type bridge 2>/dev/null | awk '{print $1}' | tr '\n' ' ')
[ -n "$br" ] && info "bridges: $br" || ok "no bridge interfaces"

# ---------------------------------------------------------------------------
hdr "7. Leftover Kubernetes / CNI state"
for d in /etc/kubernetes /var/lib/kubelet /var/lib/etcd; do
  [ -e "$d" ] && warn "$d exists - node was a cluster member; the provisioner's reset path will run"
done
if [ -d /etc/cni/net.d ] && [ -n "$(ls -A /etc/cni/net.d 2>/dev/null)" ]; then
  warn "/etc/cni/net.d is not empty (deleted by the provisioner's cleanup): $(ls /etc/cni/net.d | tr '\n' ' ')"
else
  ok "no CNI config"
fi
for l in flannel.1 cni0 kube-ipvs0; do
  ip link show "$l" >/dev/null 2>&1 && info "stale interface: $l"
done

# ---------------------------------------------------------------------------
hdr "8. Data the cleanup step deletes (/var/lib/containers, /etc/containers)"
for d in /var/lib/containers /etc/containers; do
  if [ -e "$d" ]; then
    warn "$d exists ($(du -sh "$d" 2>/dev/null | cut -f1)) - will be removed"
  fi
done
for t in podman buildah skopeo; do
  have "$t" && info "$t installed - its storage lives under /var/lib/containers"
done
[ ! -e /var/lib/containers ] && [ ! -e /etc/containers ] && ok "nothing to lose"

# ---------------------------------------------------------------------------
hdr "9. cgroup version"
cg=$(stat -fc %T /sys/fs/cgroup 2>/dev/null)
case "$cg" in
  cgroup2fs) ok "cgroup v2" ;;
  tmpfs)     info "cgroup v1 (e.g. Ubuntu 20.04) - K8s >= 1.35 needs the CgroupV1 handling" ;;
  *)         info "cgroup fs type: ${cg:-unknown}" ;;
esac
[ -f /etc/containerd/config.toml ] && info "containerd config: $(grep -E 'SystemdCgroup|sandbox_image' /etc/containerd/config.toml | tr -s ' ' | tr '\n' ';')"

# ---------------------------------------------------------------------------
hdr "10. Platform (the runtime ships as a .deb; amd64/arm64 only)"
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  info "OS: ${PRETTY_NAME:-unknown}"
  case "${ID:-}${ID_LIKE:-}" in
    *ubuntu*|*debian*) ok "Debian-family OS" ;;
    *) fail "not Debian/Ubuntu - the dpkg/apt-based install will not work" ;;
  esac
fi
arch=$(dpkg --print-architecture 2>/dev/null || uname -m)
case "$arch" in
  amd64|arm64|x86_64|aarch64) ok "architecture: $arch" ;;
  *) fail "unsupported architecture: $arch" ;;
esac
[ "$(ps -p 1 -o comm= 2>/dev/null)" = "systemd" ] && ok "systemd is PID 1" || fail "PID 1 is not systemd (container/WSL?) - services cannot be managed"
info "kernel: $(uname -r)"
if [ -n "${SUDO_USER:-}" ]; then
  if sudo -l -U "$SUDO_USER" 2>/dev/null | grep -q NOPASSWD; then
    ok "user '$SUDO_USER' has passwordless sudo (provisioner runs 'sudo ...' over SSH)"
  else
    warn "user '$SUDO_USER' has no NOPASSWD sudo - provisioner's non-interactive sudo steps will fail"
  fi
fi

# ---------------------------------------------------------------------------
hdr "11. Resources"
cpus=$(nproc 2>/dev/null || echo 0)
[ "$cpus" -ge 2 ] && ok "CPUs: $cpus" || warn "CPUs: $cpus (kubeadm preflight needs >= 2 on control planes)"
mem_mb=$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo)
[ "$mem_mb" -ge 1700 ] && ok "memory: ${mem_mb} MiB" || warn "memory: ${mem_mb} MiB (kubeadm needs >= ~1700 MiB)"
for pair in "/var:5" "/tmp:1" "/:10"; do
  mp=${pair%%:*}; need=${pair##*:}
  free_gb=$(df -BG --output=avail "$mp" 2>/dev/null | tail -1 | tr -dc '0-9')
  [ -z "$free_gb" ] && continue
  [ "$free_gb" -ge "$need" ] && ok "$mp free: ${free_gb}G" || warn "$mp free: ${free_gb}G (< ${need}G; image pulls and runtime install need space)"
done
if awk '$2 ~ /^\/var\/lib\/(containers|crio)/' /proc/mounts | grep -q .; then
  info "mounts under /var/lib/containers or /var/lib/crio (provisioner lazily unmounts them):"
  awk '$2 ~ /^\/var\/lib\/(containers|crio)/ {print "         " $2}' /proc/mounts | head -5
fi

# ---------------------------------------------------------------------------
hdr "12. apt / dpkg health (the install runs apt-get non-interactively)"
if have fuser; then
  for lk in /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock; do
    [ -e "$lk" ] && fuser "$lk" >/dev/null 2>&1 && warn "$lk is held by another process (unattended-upgrades?) - apt-get will hang or fail"
  done
fi
systemctl is-active --quiet unattended-upgrades 2>/dev/null && info "unattended-upgrades active (provisioner stops and disables it)"
pkgproc=$(pgrep -x 'apt|apt-get|dpkg|aptitude' -l 2>/dev/null | head -3 | tr '\n' ' ')
[ -n "$pkgproc" ] && warn "package manager process running: $pkgproc"
audit=$(dpkg --audit 2>&1)
[ -z "$audit" ] && ok "dpkg --audit clean" || fail "dpkg reports half-installed/broken packages: $(echo "$audit" | head -3 | tr '\n' ' ')"
if have apt-get; then
  apt-get check >/dev/null 2>&1 && ok "apt-get check passed" || fail "apt-get check failed (broken dependencies)"
fi
held=$(apt-mark showhold 2>/dev/null | tr '\n' ' ')
[ -n "$held" ] && info "held packages: $held" || ok "no held packages"

# ---------------------------------------------------------------------------
hdr "13. Kernel / sysctl prerequisites (provisioner sets most of these itself)"
swapon --show --noheadings 2>/dev/null | grep -q . && info "swap is on now (provisioner runs swapoff -a)"
if grep -Eq '^[^#].*\sswap\s' /etc/fstab 2>/dev/null; then
  info "swap entry in /etc/fstab (provisioner comments it out so swap stays off after reboot)"
fi
for m in overlay br_netfilter; do
  if lsmod | grep -q "^$m "; then ok "module loaded: $m"
  elif modinfo "$m" >/dev/null 2>&1; then info "module available, not loaded: $m (provisioner modprobes it)"
  else fail "kernel module not available: $m"
  fi
done
[ "$(sysctl -n net.ipv4.ip_forward 2>/dev/null)" = "1" ] && ok "ip_forward=1" || info "ip_forward=0 (provisioner sets it)"
cfg="/boot/config-$(uname -r)"
if [ -r "$cfg" ]; then
  grep -q '^CONFIG_CHECKPOINT_RESTORE=y' "$cfg" && ok "kernel has CONFIG_CHECKPOINT_RESTORE (CRIU)" || warn "kernel lacks CONFIG_CHECKPOINT_RESTORE - CRIU checkpoint/restore will not work"
fi
inot=$(sysctl -n fs.inotify.max_user_instances 2>/dev/null || echo 0)
[ "$inot" -ge 512 ] && ok "inotify max_user_instances=$inot" || warn "inotify max_user_instances=$inot (low; kubelet/pods may hit 'too many open files')"

# ---------------------------------------------------------------------------
hdr "14. Port conflicts (kubeadm / flannel / WireGuard)"
if have ss; then
  for p in 6443 2379 2380 10250 10257 10259 10256; do
    ss -Hltn "sport = :$p" 2>/dev/null | grep -q . || continue
    owner=$(ss -Hltnp "sport = :$p" 2>/dev/null | grep -o 'users:(("[^"]*"' | head -1 | sed 's/users:(("//;s/"$//')
    case "$owner" in
      kube-*|etcd|kubelet) info "TCP :$p held by existing Kubernetes component '$owner' - node is already part of a cluster" ;;
      *) warn "TCP :$p already in use by '${owner:-unknown}'" ;;
    esac
  done
  for p in 8472 51820; do
    ss -Hlun "sport = :$p" 2>/dev/null | grep -q . && warn "UDP :$p already in use (flannel VXLAN=8472, WireGuard=51820)"
  done
fi

# ---------------------------------------------------------------------------
hdr "15. Existing kubelet config overriding the runtime endpoint"
for f in /etc/default/kubelet /var/lib/kubelet/kubeadm-flags.env /var/lib/kubelet/config.yaml; do
  [ -f "$f" ] || continue
  hit=$(grep -HEi 'container-runtime-endpoint|containerRuntimeEndpoint|containerd|dockershim|cri-dockerd' "$f" 2>/dev/null)
  [ -n "$hit" ] && { warn "$f references a runtime endpoint:"; echo "$hit" | sed 's/^/         /'; }
done
if [ -d /etc/systemd/system/kubelet.service.d ] || [ -d /usr/lib/systemd/system/kubelet.service.d ]; then
  grep -rHEi 'container-runtime-endpoint|containerd' /etc/systemd/system/kubelet.service.d /usr/lib/systemd/system/kubelet.service.d 2>/dev/null \
    | head -3 | sed 's/^/         /'
fi
have kubelet && info "kubelet installed: $(kubelet --version 2>/dev/null)"
have kubeadm && info "kubeadm installed: $(kubeadm version -o short 2>/dev/null)"

# ---------------------------------------------------------------------------
hdr "16. Firewall / iptables backend"
if have ufw && ufw status 2>/dev/null | grep -q 'Status: active'; then
  warn "ufw is active - may block 6443/10250/8472/51820 and pod traffic"
else
  ok "ufw not active"
fi
if have iptables; then
  ver=$(iptables --version 2>/dev/null)
  info "$ver"
  if have iptables-legacy && have iptables-nft; then
    leg=$(iptables-legacy-save 2>/dev/null | grep -c '^-A')
    nft=$(iptables-nft-save 2>/dev/null | grep -c '^-A')
    if [ "${leg:-0}" -gt 0 ] && [ "${nft:-0}" -gt 0 ]; then
      warn "rules exist in BOTH legacy ($leg) and nft ($nft) iptables - kube-proxy/flannel/Docker may use different backends and conflict"
    fi
  fi
fi

# ---------------------------------------------------------------------------
hdr "17. Network overlaps and VPN"
# kubeadm podSubnet 10.244.0.0/16, serviceSubnet 10.96.0.0/12
while read -r r; do
  warn "host route $r overlaps the pod (10.244.0.0/16) or service (10.96.0.0/12) CIDR"
done < <(ip -4 route show 2>/dev/null | grep -vE ' dev (flannel\.1|cni0|kube-ipvs0|cali|weave)' \
  | awk '{print $1}' | grep -E '^10\.244\.|^10\.(9[6-9]|10[0-9]|11[01])\.')
if ip link show wg0 >/dev/null 2>&1; then
  info "wg0 already exists: $(ip -4 -br addr show wg0 | awk '{print $3}') (provisioner may adopt it or replace it)"
fi
have wg && [ -n "$(wg show interfaces 2>/dev/null)" ] && info "WireGuard interfaces: $(wg show interfaces)"
ip -4 route show default 2>/dev/null | sed 's/^/         default route: /'
[ "$(ip -4 route show default 2>/dev/null | wc -l)" -gt 1 ] && warn "multiple default routes - flannel's interface autodetection (no-VPN mode) may pick the wrong NIC"
hn=$(hostname)
getent hosts "$hn" >/dev/null 2>&1 && ok "hostname '$hn' resolves" || warn "hostname '$hn' does not resolve - kubelet/kubeadm may warn or fail"
[ "$hn" != "$(echo "$hn" | tr 'A-Z' 'a-z')" ] && warn "hostname has uppercase letters - kubeadm node names must be lowercase"
px=$(env | grep -iE '^(http|https|no)_proxy=' | sed 's#//[^@/]*@#//***@#')
[ -n "$px" ] && { info "proxy variables set (apt/oras/crio may need them too):"; echo "$px" | sed 's/^/         /'; }
timedatectl show -p NTPSynchronized --value 2>/dev/null | grep -q yes && ok "clock synchronized" || warn "clock not NTP-synchronized - TLS and kubeadm certificate checks can fail"

# ---------------------------------------------------------------------------
hdr "18. Connectivity (registry, apt, API server)"
REGISTRY="${REGISTRY:-ghcr.io}"
for host in "$REGISTRY" pkgs.k8s.io github.com; do
  if getent hosts "${host%%:*}" >/dev/null 2>&1; then
    if have curl; then
      code=$(curl -s -o /dev/null -m 8 -w '%{http_code}' "https://${host}/" 2>/dev/null)
      [ "${code:-000}" != "000" ] && ok "$host reachable (HTTP $code)" || warn "$host resolves but HTTPS failed (firewall/proxy/TLS?)"
    else
      info "$host resolves (curl missing - cannot test HTTPS)"
    fi
  else
    warn "cannot resolve $host (DNS)"
  fi
done
if [ -n "${API_ENDPOINT:-}" ]; then
  h=${API_ENDPOINT%%:*}; p=${API_ENDPOINT##*:}
  timeout 5 bash -c "exec 3<>/dev/tcp/$h/$p" 2>/dev/null && ok "API server $API_ENDPOINT reachable" || fail "cannot reach API server $API_ENDPOINT"
else
  info "set API_ENDPOINT=host:port to test control-plane reachability"
fi

# ---------------------------------------------------------------------------
hdr "19. Registry credentials / mirrors / GPU config that will NOT carry over to CRI-O"
if [ -f /etc/docker/daemon.json ]; then
  grep -Eq 'insecure-registries|registry-mirrors' /etc/docker/daemon.json && warn "/etc/docker/daemon.json has insecure-registries/mirrors - CRI-O does not read them (provisioner configures its own)"
fi
if [ -f /etc/containerd/config.toml ]; then
  grep -Eq 'registry\.(mirrors|configs)|config_path' /etc/containerd/config.toml && warn "/etc/containerd/config.toml has registry mirror/auth config - not used by CRI-O"
fi
for f in /root/.docker/config.json ${SUDO_USER:+/home/$SUDO_USER/.docker/config.json}; do
  [ -f "$f" ] && grep -q '"auth"' "$f" 2>/dev/null && warn "$f holds registry credentials - private images need a pull secret in Kubernetes; CRI-O will not use these"
done
if have nvidia-ctk || [ -d /etc/nvidia-container-runtime ]; then
  warn "NVIDIA container toolkit present - it may have patched docker/containerd, not CRI-O; GPU pods need 'nvidia-ctk runtime configure --runtime=crio'"
  have nvidia-smi && nvidia-smi -L 2>/dev/null | head -2 | sed 's/^/         /'
fi
[ -d /etc/crio ] && info "/etc/crio already exists (provisioner removes it on cleanup)"

# ---------------------------------------------------------------------------
hdr "20. Machine identity (duplicates break joins when nodes are cloned)"
uuid=$(cat /sys/class/dmi/id/product_uuid 2>/dev/null)
[ -n "$uuid" ] && info "product_uuid: $uuid"
[ -s /etc/machine-id ] && info "machine-id: $(cat /etc/machine-id)" || warn "empty /etc/machine-id - cloned image? regenerate before joining"
info "compare both IDs against other nodes: 'kubectl get nodes -o wide' / 'kubectl describe node | grep -i -E \"machine|system uuid\"'"

# ---------------------------------------------------------------------------
hdr "Summary"
printf '  warnings: %d, failures: %d\n' "$WARNS" "$FAILS"
if [ "$WARNS" -eq 0 ] && [ "$FAILS" -eq 0 ]; then
  echo "  No problems found."
  exit 0
fi
echo "  Review the WARN/FAIL lines above before provisioning."
exit 1
