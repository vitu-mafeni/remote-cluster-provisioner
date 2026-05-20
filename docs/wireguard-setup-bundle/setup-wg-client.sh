#!/bin/bash
set -e

echo "Installing WireGuard..."
sudo apt update
sudo apt install -y wireguard

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
