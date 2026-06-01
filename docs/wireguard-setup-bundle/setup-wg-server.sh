#!/bin/bash
set -e

echo "Installing WireGuard..."
sudo apt update
sudo apt install -y wireguard iptables-persistent

echo "Enabling IP forwarding..."
echo "net.ipv4.ip_forward=1" | sudo tee /etc/sysctl.d/99-wireguard.conf
sudo sysctl --system

echo ""
echo "Generate keys with:"
echo "wg genkey | tee privatekey | wg pubkey > publickey"
echo ""
echo "Then edit:"
echo "sudo nano /etc/wireguard/wg0.conf"
echo ""
echo "Start WireGuard with:"
echo "sudo systemctl enable wg-quick@wg0"
echo "sudo systemctl start wg-quick@wg0"
