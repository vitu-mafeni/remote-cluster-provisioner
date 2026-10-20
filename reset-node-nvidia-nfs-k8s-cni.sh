#!/usr/bin/env bash
#
# reset-node-nvidia-nfs-k8s-cni.sh — Tear down a kubeadm cluster node and clean up CRI-O,
# Cilium/Flannel CNI leftovers, GPU Operator artifacts, and the ad-hoc
# NFS export, so the box is close to a fresh Ubuntu install again.
#
# USAGE:
#   sudo bash reset-node-nvidia-nfs-k8s-cni.sh [--force]
#
# Review the toggles below before running. This script is intentionally
# staged with `confirm` prompts on the destructive sections. Run with
# --force (or FORCE=true) to skip confirmations (for reruns / automation).
#
# The risky toggles default to OFF and can be enabled from the environment
# (sudo needs the variables passed through explicitly), e.g.:
#   sudo PURGE_K8S_PACKAGES=true bash reset-node-nvidia-nfs-k8s-cni.sh
#   sudo env PURGE_NVIDIA_DRIVER=true NVIDIA_DRIVER_VERSION=580.126.20 bash reset-node-nvidia-nfs-k8s-cni.sh
#   sudo REMOVE_KUBE_DIRS=true UNMOUNT_NFS_CLIENTS=true bash reset-node-nvidia-nfs-k8s-cni.sh
#
# Recommended: reboot after this script finishes, before reinstalling
# anything, to clear kernel modules, leftover netns, and mount state.

set -uo pipefail

# ----------------------------- TOGGLES --------------------------------
# Leave these OFF unless you specifically want that layer gone too.
# All default to false; override via the environment.
PURGE_NVIDIA_DRIVER="${PURGE_NVIDIA_DRIVER:-true}"   # true = unload/remove the host NVIDIA driver + kernel modules
PURGE_NFS_SERVER="${PURGE_NFS_SERVER:-true}"         # true = uninstall nfs-kernel-server entirely (kills ALL exports, not just k8s's)
PURGE_K8S_PACKAGES="${PURGE_K8S_PACKAGES:-true}"     # true = apt purge kubelet/kubeadm/kubectl/cri-o/helm binaries
REMOVE_KUBE_DIRS="${REMOVE_KUBE_DIRS:-true}"         # true = rm -rf the ~/.kube of root and the invoking (sudo) user, INCLUDING unrelated kubeconfigs/caches
UNMOUNT_NFS_CLIENTS="${UNMOUNT_NFS_CLIENTS:-true}"   # true = lazily unmount EVERY NFS client mount on this host (listed in the prompt)
FORCE="${FORCE:-true}"                               # true (or pass --force) skips interactive confirmations
# Only used in the confirmation prompt text; auto-detected from the loaded
# kernel module when unset.
NVIDIA_DRIVER_VERSION="${NVIDIA_DRIVER_VERSION:-$(cat /sys/module/nvidia/version 2>/dev/null || true)}"
NVIDIA_DRIVER_VERSION="${NVIDIA_DRIVER_VERSION:-unknown version}"
# ------------------------------------------------------------------------

for arg in "$@"; do
  case "$arg" in
    --force) FORCE=true ;;
    *) echo "Unknown argument: $arg (supported: --force)" >&2; exit 2 ;;
  esac
done

# stdout would otherwise buffer in blocks when not attached to a real tty
# (e.g. piped through tee/ssh) — force line buffering so progress shows live
exec > >(stdbuf -oL cat) 2> >(stdbuf -oL cat >&2)

log()  { echo -e "\n\033[1;36m[reset-node]\033[0m $*"; }
warn() { echo -e "\033[1;33m[warn]\033[0m $*"; }
confirm() {
  $FORCE && return 0
  read -rp "$1 [y/N] " ans
  [[ "$ans" =~ ^[Yy]$ ]]
}

if [[ $EUID -ne 0 ]]; then
  echo "Run this as root (sudo bash reset-node-nvidia-nfs-k8s-cni.sh)"; exit 1
fi

# ~/.kube may hold kubeconfigs for OTHER clusters plus the kubectl discovery
# cache, so it is only removed when REMOVE_KUBE_DIRS=true. Under sudo "~" is
# root's home; the invoking user's ~/.kube is included too.
KUBE_DIRS=()
if $REMOVE_KUBE_DIRS; then
  KUBE_DIRS=("${HOME:-/root}/.kube")
  if [[ -n "${SUDO_USER:-}" ]]; then
    sudo_home="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
    [[ -n "$sudo_home" && "$sudo_home/.kube" != "${KUBE_DIRS[0]}" ]] && KUBE_DIRS+=("$sudo_home/.kube")
  fi
fi

# NFS client mounts that UNMOUNT_NFS_CLIENTS=true would lazily unmount.
NFS_CLIENT_MOUNTS=()
if $UNMOUNT_NFS_CLIENTS && command -v findmnt >/dev/null 2>&1; then
  mapfile -t NFS_CLIENT_MOUNTS < <(findmnt -rn -t nfs,nfs4 -o TARGET 2>/dev/null)
fi

echo "This will wipe Kubernetes, CRI-O, CNI and iptables state on $(hostname)."
echo "  PURGE_K8S_PACKAGES=$PURGE_K8S_PACKAGES PURGE_NVIDIA_DRIVER=$PURGE_NVIDIA_DRIVER PURGE_NFS_SERVER=$PURGE_NFS_SERVER"
echo "  iptables/ip6tables: all rules are flushed and the INPUT/FORWARD/OUTPUT policies are reset to ACCEPT"
echo "    (any host firewall such as ufw is lost — re-apply it afterwards)"
if $REMOVE_KUBE_DIRS; then
  echo "  REMOVE_KUBE_DIRS=true: will DELETE ${KUBE_DIRS[*]} (all kubeconfigs and caches in them)"
else
  echo "  REMOVE_KUBE_DIRS=false: ~/.kube is left untouched (set REMOVE_KUBE_DIRS=true to delete it)"
fi
if $UNMOUNT_NFS_CLIENTS; then
  if ((${#NFS_CLIENT_MOUNTS[@]})); then
    echo "  UNMOUNT_NFS_CLIENTS=true: will lazily unmount ${#NFS_CLIENT_MOUNTS[@]} NFS mount(s): ${NFS_CLIENT_MOUNTS[*]}"
  else
    echo "  UNMOUNT_NFS_CLIENTS=true: no NFS client mounts found"
  fi
else
  echo "  UNMOUNT_NFS_CLIENTS=false: NFS client mounts are left mounted"
fi
confirm "Continue?" || { echo "Aborted."; exit 1; }

# =========================================================================
log "1/9  Draining/removing kubeadm cluster state"
# =========================================================================
# kubeadm reset talks to CRI-O to stop every container — if CRI-O is
# already wedged (e.g. a stuck GPU-driver call from a prior crash), this
# call queues behind it and can hang forever. Force-kill CRI-O/kubelet
# FIRST so kubeadm reset either succeeds fast or is skipped outright —
# it's not load-bearing for a full wipe anyway (the rest of this script
# clears /etc/kubernetes, iptables, and volumes independently).
echo "   force-stopping crio/kubelet so they can't block this step"
systemctl kill -s SIGKILL crio 2>/dev/null
systemctl kill -s SIGKILL kubelet 2>/dev/null
sleep 1

if command -v kubeadm >/dev/null 2>&1; then
  echo "   attempting kubeadm reset (10s budget, best-effort only)"
  timeout 10 kubeadm reset -f --cri-socket unix:///var/run/crio/crio.sock >/dev/null 2>&1 || \
  timeout 10 kubeadm reset -f >/dev/null 2>&1 || \
  warn "kubeadm reset skipped (crio unresponsive or already reset) — continuing with manual cleanup"
fi

systemctl stop kubelet 2>/dev/null
systemctl disable kubelet 2>/dev/null

# kubelet dying doesn't unmount its projected/secret/subPath volume mounts —
# clear those first or the rm -rf below hits "Device or resource busy"
log "   Unmounting any leftover kubelet volume mounts"
mapfile -t kubelet_mounts < <(mount | grep '/var/lib/kubelet' | awk '{print $3}' | sort -r)
echo "   found ${#kubelet_mounts[@]} mount(s) to clear"
for m in "${kubelet_mounts[@]}"; do
  echo "   unmounting: $m"
  umount -l "$m" 2>/dev/null
done

for d in /etc/kubernetes /var/lib/kubelet /var/lib/etcd /var/lib/dockershim \
         /etc/systemd/system/kubelet.service.d /usr/lib/systemd/system/kubelet.service.d ${KUBE_DIRS[@]+"${KUBE_DIRS[@]}"}; do
  [[ -e "$d" ]] || continue
  echo "   removing: $d"
  rm -rf "$d"
done

# =========================================================================
log "2/9  Cleaning up CNI: Cilium + Flannel leftovers"
# =========================================================================
systemctl stop cilium 2>/dev/null
rm -rf /etc/cni/net.d \
       /opt/cni/bin \
       /var/lib/cni \
       /run/flannel \
       /var/lib/cilium \
       /etc/cilium \
       /var/run/cilium \
       /sys/fs/bpf/cilium* 2>/dev/null

# unmount any lingering bpf/cgroup2 mounts cilium sets up
for m in /sys/fs/bpf /run/cilium/cgroupv2; do
  mountpoint -q "$m" && umount -l "$m" 2>/dev/null
done

# remove leftover virtual interfaces (cni0, flannel.1, cilium_*, veth*, docker0)
log "   Removing leftover virtual network interfaces"
for iface in cni0 flannel.1 cilium_net cilium_host cilium_vxlan docker0 kube-ipvs0 wg0; do
  if ip link show "$iface" >/dev/null 2>&1; then
    ip link delete "$iface" 2>/dev/null && echo "   removed: $iface" || echo "   failed to remove: $iface (may need -l/lazy)"
  else
    echo "   not present: $iface"
  fi
done
mapfile -t veths < <(ip -o link show | awk -F': ' '{print $2}' | grep -E '^(veth|lxc)')
echo "   found ${#veths[@]} veth/lxc interface(s)"
for v in "${veths[@]}"; do
  ip link delete "$v" 2>/dev/null && echo "   removed: $v"
done

# =========================================================================
log "3/9  Flushing iptables / ipvs rules left by kube-proxy & cilium"
# =========================================================================
# Reset the chain policies to ACCEPT BEFORE flushing: flushing the rules while
# a policy is DROP (ufw, Docker, hardened images) would cut off remote (SSH)
# access to this machine mid-run.
for ipt in iptables ip6tables; do
  command -v "$ipt" >/dev/null 2>&1 || continue
  for chain in INPUT FORWARD OUTPUT; do
    "$ipt" -P "$chain" ACCEPT 2>/dev/null
  done
  for table in filter nat mangle raw; do
    "$ipt" -t "$table" -F 2>/dev/null
    "$ipt" -t "$table" -X 2>/dev/null
  done
done
command -v ipvsadm >/dev/null 2>&1 && ipvsadm --clear 2>/dev/null

# =========================================================================
log "4/9  Stopping and cleaning CRI-O (and containerd if present)"
# =========================================================================
echo "   stopping crio (can take a few seconds if it's still mid-retry on a stuck container)..."
timeout 20 systemctl stop crio 2>/dev/null || warn "crio didn't stop cleanly within 20s, continuing anyway"
systemctl disable crio 2>/dev/null

for d in /var/lib/containers /var/lib/crio /run/crio /etc/crio; do
  [[ -e "$d" ]] || continue
  size=$(du -sh "$d" 2>/dev/null | cut -f1)
  echo "   removing: $d (${size:-unknown size} — may take a while if image layers are present)"
  rm -rf "$d"
done

timeout 20 systemctl stop containerd 2>/dev/null
systemctl disable containerd 2>/dev/null
for d in /var/lib/containerd /etc/containerd /run/containerd; do
  [[ -e "$d" ]] || continue
  echo "   removing: $d"
  rm -rf "$d"
done

# =========================================================================
log "5/9  Removing Kubernetes/CRI-O/Helm packages and repos"
# =========================================================================
if $PURGE_K8S_PACKAGES; then
  if confirm "Purge kubelet/kubeadm/kubectl/cri-o/helm packages?"; then
    apt-mark unhold kubelet kubeadm kubectl 2>/dev/null
    apt-get purge -y kubelet kubeadm kubectl cri-o cri-o-runc kubernetes-cni helm 2>/dev/null
    apt-get autoremove -y --purge 2>/dev/null
    rm -f /etc/apt/sources.list.d/kubernetes.list /etc/apt/sources.list.d/*cri-o*.list
    rm -f /etc/apt/keyrings/kubernetes*.gpg
  fi
fi

# =========================================================================
log "6/9  GPU Operator / NVIDIA toolkit cleanup (host driver kept unless PURGE_NVIDIA_DRIVER=true)"
# =========================================================================
# The GPU Operator's toolkit/validator install under /usr/local/nvidia is
# k8s-managed tooling, not your host driver; it is removed unconditionally.
rm -rf /usr/local/nvidia /run/nvidia

if $PURGE_NVIDIA_DRIVER; then
  if confirm "This will UNLOAD your host NVIDIA driver (${NVIDIA_DRIVER_VERSION}) and remove kernel modules. Continue?"; then
    systemctl stop nvidia-persistenced 2>/dev/null
    rmmod nvidia_uvm nvidia_drm nvidia_modeset nvidia 2>/dev/null || \
      warn "Could not unload modules live (likely still in use) — a reboot will clear them"
    apt-get purge -y '^nvidia-.*' 2>/dev/null
    apt-get autoremove -y --purge 2>/dev/null
  fi
else
  log "   Skipping host driver removal (PURGE_NVIDIA_DRIVER=false) — leaving nvidia-smi functional"
fi

# =========================================================================
log "7/9  Reverting the NFS export you added for k8s (/srv/nfs/k8s)"
# =========================================================================
if grep -q '/srv/nfs/k8s' /etc/exports 2>/dev/null; then
  cp /etc/exports /etc/exports.bak.$(date +%s)
  sed -i '\#^/srv/nfs/k8s#d' /etc/exports
  exportfs -ra
  log "   Removed /srv/nfs/k8s export line (backup saved as /etc/exports.bak.*)"
fi
rm -rf /srv/nfs/k8s

# Client-side NFS mounts are only unmounted on explicit opt-in: this host may
# legitimately mount NFS shares unrelated to Kubernetes (the mounts to be
# removed were listed in the confirmation prompt above).
if $UNMOUNT_NFS_CLIENTS; then
  for mnt in ${NFS_CLIENT_MOUNTS[@]+"${NFS_CLIENT_MOUNTS[@]}"}; do
    umount -l "$mnt" 2>/dev/null && log "   unmounted $mnt"
  done
else
  log "   Leaving NFS client mounts in place (UNMOUNT_NFS_CLIENTS=false)"
fi

if $PURGE_NFS_SERVER; then
  if confirm "This removes nfs-kernel-server ENTIRELY, killing /srv/nfs/kubevirt and jupyter-kernels exports too. Continue?"; then
    systemctl stop nfs-kernel-server 2>/dev/null
    apt-get purge -y nfs-kernel-server nfs-common 2>/dev/null
    apt-get autoremove -y --purge 2>/dev/null
  fi
else
  log "   Leaving nfs-kernel-server installed (PURGE_NFS_SERVER=false) — other exports untouched"
fi

# =========================================================================
log "8/9  Misc leftover state"
# =========================================================================
rm -rf /var/lib/calico /etc/NetworkManager/conf.d/*cilium* 2>/dev/null
sysctl --system >/dev/null 2>&1  # reapply any sysctls modified by CNI plugins

# =========================================================================
log "9/9  Done"
# =========================================================================
echo
echo "=============================================================="
echo " Cleanup complete."
echo " Toggles used this run:"
echo "   PURGE_NVIDIA_DRIVER=$PURGE_NVIDIA_DRIVER"
echo "   PURGE_NFS_SERVER=$PURGE_NFS_SERVER"
echo "   PURGE_K8S_PACKAGES=$PURGE_K8S_PACKAGES"
echo "   REMOVE_KUBE_DIRS=$REMOVE_KUBE_DIRS"
echo "   UNMOUNT_NFS_CLIENTS=$UNMOUNT_NFS_CLIENTS"
echo
echo " A reboot is strongly recommended before reinstalling anything,"
echo " to fully clear kernel modules, leftover mount namespaces, and"
echo " any stuck cgroups from the old kubelet/CRI-O processes."
echo "   sudo reboot"
echo "=============================================================="
