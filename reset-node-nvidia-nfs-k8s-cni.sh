#!/usr/bin/env bash
#
# reset-node.sh — Tear down a kubeadm cluster node and clean up CRI-O,
# Cilium/Flannel CNI leftovers, GPU Operator artifacts, and the ad-hoc
# NFS export, so the box is close to a fresh Ubuntu install again.
#
# USAGE:
#   sudo bash reset-node.sh
#
# Review the toggles below before running. This script is intentionally
# staged with `confirm` prompts on the destructive sections. Run with
# --force to skip confirmations (for reruns / automation).
#
# Recommended: reboot after this script finishes, before reinstalling
# anything, to clear kernel modules, leftover netns, and mount state.

set -uo pipefail

# ----------------------------- TOGGLES --------------------------------
# Leave these OFF unless you specifically want that layer gone too.
PURGE_NVIDIA_DRIVER=true   # true = unload/remove GPU Operator's driver + kernel modules
PURGE_NFS_SERVER=true      # true = uninstall nfs-kernel-server entirely (kills ALL exports, not just k8s's)
PURGE_K8S_PACKAGES=true     # true = apt purge kubelet/kubeadm/kubectl/cri-o/helm binaries
FORCE=true                 # set true (or pass --force) to skip interactive confirmations
# ------------------------------------------------------------------------

[[ "${1:-}" == "--force" ]] && FORCE=true
[[ "${2:-}" == "--force" ]] && FORCE=true

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
  echo "Run this as root (sudo bash reset-node.sh)"; exit 1
fi

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
         /etc/systemd/system/kubelet.service.d /usr/lib/systemd/system/kubelet.service.d ~/.kube; do
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
if command -v iptables-save >/dev/null 2>&1; then
  iptables-save | grep -E 'KUBE-|CILIUM' | iptables-restore --noflush 2>/dev/null
  for table in filter nat mangle raw; do
    iptables -t "$table" -F 2>/dev/null
    iptables -t "$table" -X 2>/dev/null
  done
  ip6tables -F 2>/dev/null; ip6tables -X 2>/dev/null
fi
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
log "6/9  GPU Operator / NVIDIA toolkit cleanup (host driver kept unless enabled)"
# =========================================================================
# The GPU Operator's toolkit/validator install under /usr/local/nvidia is
# always safe to remove — it's k8s-managed tooling, not your host driver.
rm -rf /usr/local/nvidia /run/nvidia

if $PURGE_NVIDIA_DRIVER; then
  if confirm "This will UNLOAD your host NVIDIA driver (580.126.20) and remove kernel modules. Continue?"; then
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

# unmount any client-side NFS mounts left over from testing
mount | grep 'type nfs' | awk '{print $3}' | while read -r mnt; do
  umount -l "$mnt" 2>/dev/null && log "   unmounted $mnt"
done

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
echo
echo " A reboot is strongly recommended before reinstalling anything,"
echo " to fully clear kernel modules, leftover mount namespaces, and"
echo " any stuck cgroups from the old kubelet/CRI-O processes."
echo "   sudo reboot"
echo "=============================================================="
