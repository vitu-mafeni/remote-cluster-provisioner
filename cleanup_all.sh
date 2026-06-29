#!/bin/bash
set -e

echo "====== Full Cluster Cleanup ======"

# On control plane, delete the worker node from K8s
echo "Deleting worker node from Kubernetes..."
kubectl delete node dcn-gpu-0 --ignore-not-found=true || true
sleep 10

# Give it time to finalize deletion
echo "Waiting for node deletion to finalize..."
sleep 30

# SSH to worker and clean up
echo "Cleaning up worker node..."
ssh ubuntu@192.168.30.87 << 'WORKER_CLEANUP'
  echo "Running cleanup on worker node..."

  # Stop kubelet and services
  sudo systemctl stop kubelet || true
  sudo systemctl stop crio || true
  sudo systemctl disable crio || true
  sudo systemctl disable kubelet || true

  # Reset kubeadm
  sudo kubeadm reset -f --cri-socket=unix:///var/run/crio/crio.sock 2>/dev/null || true

  # Remove CRI-O service files
  sudo rm -f /lib/systemd/system/crio.service
  sudo rm -f /etc/systemd/system/crio.service
  sudo rm -f /etc/systemd/system/crio.service.d/*

  # Remove CRI-O packages
  sudo apt-get remove --purge -y cri-o cri-tools 2>/dev/null || true

  # Remove kubernetes packages
  sudo apt-mark unhold kubeadm kubelet kubectl 2>/dev/null || true
  sudo apt-get remove --purge -y kubeadm kubelet kubectl kubernetes-cni 2>/dev/null || true
  sudo apt-get autoremove -y 2>/dev/null || true

  # Remove CRI-O binaries everywhere
  sudo rm -f /usr/bin/crio
  sudo rm -f /usr/bin/crio-status
  sudo rm -f /usr/local/bin/crio
  sudo rm -f /usr/local/bin/crio-status
  sudo rm -f /usr/local/bin/criu
  sudo rm -f /usr/bin/criu

  # Remove CRI-O config and storage
  sudo rm -rf /etc/crio
  sudo rm -rf /etc/cni
  sudo rm -rf /opt/cni
  sudo rm -rf /var/lib/crio
  sudo rm -rf /var/lib/containers
  sudo rm -rf /var/lib/cni
  sudo rm -rf /run/crio
  sudo rm -rf /run/flannel
  sudo rm -rf /run/xtables.lock

  # Remove kubernetes directories
  sudo rm -rf /etc/kubernetes /var/lib/kubelet /var/lib/etcd

  # Remove iptables rules
  sudo iptables -F || true
  sudo iptables -X || true

  # Remove old files and configs
  sudo rm -f /etc/containers/auth.json
  sudo rm -f /etc/systemd/system/crio.service
  sudo rm -f /etc/systemd/system/kubelet.service
  sudo rm -rf /etc/systemd/system/kubelet.service.d

  # Clean systemd
  sudo systemctl daemon-reload || true

  # Remove containerd if present
  sudo rm -rf /var/lib/containerd /run/containerd
  sudo apt-get remove -y containerd.io 2>/dev/null || true

  # Remove apt repos added by provisioner
  sudo rm -f /etc/apt/sources.list.d/kubernetes.list
  sudo rm -f /etc/apt/sources.list.d/cri-o.list
  sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg
  sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg
  sudo apt-get update -y 2>/dev/null || true

  # Remove CNI network interfaces
  sudo ip link delete flannel.1 2>/dev/null || true

  echo "Worker node cleaned up"
WORKER_CLEANUP

echo ""
echo "====== Cleanup Complete ======"
echo "To re-provision:"
echo "1. Make sure controller has latest code: 'cd /home/ubuntu/remote-cluster-provisioner && make run'"
echo "2. Create NodeProvision CR to provision dcn-gpu-0 again"
