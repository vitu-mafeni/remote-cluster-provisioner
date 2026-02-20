#!/bin/bash
set -e

echo "======================================="
echo "   Kubernetes Full Cleanup Starting"
echo "======================================="

# -----------------------------
# Reset kubeadm (if installed)
# -----------------------------
if command -v kubeadm >/dev/null 2>&1; then
  echo "Running kubeadm reset..."
  sudo kubeadm reset -f || true
fi

# -----------------------------
# Stop services
# -----------------------------
echo "Stopping services..."
sudo systemctl stop kubelet 2>/dev/null || true
sudo systemctl stop containerd 2>/dev/null || true

# -----------------------------
# Remove Kubernetes packages
# -----------------------------
echo "Removing Kubernetes packages..."
sudo apt-get purge -y kubeadm kubelet kubectl kubernetes-cni || true
sudo apt-get autoremove -y
sudo apt-get autoclean

# -----------------------------
# Remove containerd
# -----------------------------
echo "Removing containerd..."
sudo apt-get purge -y containerd containerd.io || true

# -----------------------------
# Remove directories
# -----------------------------
echo "Removing Kubernetes directories..."
sudo rm -rf /etc/kubernetes
sudo rm -rf /var/lib/kubelet
sudo rm -rf /var/lib/etcd
sudo rm -rf /var/lib/cni
sudo rm -rf /etc/cni
sudo rm -rf /opt/cni
sudo rm -rf /etc/containerd
sudo rm -rf /var/run/containerd
sudo rm -rf $HOME/.kube

# -----------------------------
# Remove apt repositories
# -----------------------------
echo "Removing apt repositories..."
sudo rm -f /etc/apt/sources.list.d/kubernetes.list
sudo rm -f /etc/apt/sources.list.d/docker.list
sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg
sudo rm -f /etc/apt/keyrings/docker.gpg

sudo apt-get update

# -----------------------------
# Clean iptables rules
# -----------------------------
echo "Flushing iptables..."
sudo iptables -F || true
sudo iptables -t nat -F || true
sudo iptables -t mangle -F || true
sudo iptables -X || true

# -----------------------------
# Remove CNI interfaces
# -----------------------------
echo "Removing CNI network interfaces..."
sudo ip link delete cni0 2>/dev/null || true
sudo ip link delete flannel.1 2>/dev/null || true
sudo ip link delete docker0 2>/dev/null || true

# -----------------------------
# Remove kernel configs
# -----------------------------
echo "Cleaning kernel module configs..."
sudo rm -f /etc/modules-load.d/k8s.conf
sudo rm -f /etc/sysctl.d/k8s.conf
sudo sysctl --system || true

# -----------------------------
# Re-enable swap (optional)
# -----------------------------
echo "Re-enabling swap (if previously disabled)..."
sudo sed -i '/ swap / s/^#\(.*\)$/\1/g' /etc/fstab || true
sudo swapon -a || true

echo ""
echo "======================================="
echo " Kubernetes cleanup completed."
echo " Reboot is strongly recommended."
echo "======================================="
