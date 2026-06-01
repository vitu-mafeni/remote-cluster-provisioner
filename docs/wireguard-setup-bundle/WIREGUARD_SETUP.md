# WireGuard VPN Setup Guide

## Architecture

| Device | LAN IP | WG IP |
|---|---|---|
| WireGuard Server | 192.168.3.92 | 10.9.0.1 |
| Client Example | Any | 10.9.0.10 |

---

# 1. Install WireGuard

Run on BOTH server and clients:

```bash
sudo apt update
sudo apt install wireguard -y
```

---

# 2. Generate Keys

Run on BOTH machines:

```bash
mkdir -p ~/wg
cd ~/wg

wg genkey | tee privatekey | wg pubkey > publickey

cat privatekey
cat publickey
```

---

# 3. Router Port Forwarding

On ASUS Router:

Go to WAN → Virtual Server / Port Forwarding the add:
- Protocol: BOTH
- External Port: 51820
- Internal Port: 51820
- Internal IP: 192.168.3.92

---

# 4. Server Configuration

Create:

```bash
sudo nano /etc/wireguard/wg0.conf
```

Example:

```ini
[Interface]
Address = 10.9.0.1/24
ListenPort = 51820
PrivateKey = SERVER_PRIVATE_KEY

PostUp = sysctl -w net.ipv4.ip_forward=1
PostUp = iptables -A FORWARD -i wg0 -j ACCEPT
PostUp = iptables -A FORWARD -o wg0 -j ACCEPT
PostUp = iptables -t nat -A POSTROUTING -s 10.9.0.0/24 -o eno2 -j MASQUERADE

PostDown = iptables -D FORWARD -i wg0 -j ACCEPT
PostDown = iptables -D FORWARD -o wg0 -j ACCEPT
PostDown = iptables -t nat -D POSTROUTING -s 10.9.0.0/24 -o eno2 -j MASQUERADE

[Peer]
PublicKey = CLIENT_PUBLIC_KEY
AllowedIPs = 10.9.0.10/32
```

---

# 5. Client Configuration

```bash
sudo nano /etc/wireguard/wg0.conf
```

```ini
[Interface]
PrivateKey = CLIENT_PRIVATE_KEY
Address = 10.9.0.10/24

[Peer]
PublicKey = SERVER_PUBLIC_KEY
Endpoint = dcnnephio.asuscomm.com:51820
AllowedIPs = 10.9.0.0/24,192.168.3.0/24
PersistentKeepalive = 25
```

---

# 6. Start WireGuard

```bash
sudo chmod 600 /etc/wireguard/wg0.conf

sudo systemctl enable wg-quick@wg0
sudo systemctl start wg-quick@wg0
```

---

# 7. Verification

## On server

```bash
sudo wg
```

## On client

```bash
ping 10.9.0.1
ping 192.168.3.234
```

---

# 8. Useful Commands

```bash
sudo wg
sudo systemctl restart wg-quick@wg0
sudo systemctl stop wg-quick@wg0
```

---

# 9. Firewall

```bash
sudo ufw allow 51820/udp
```

---

# 10. Troubleshooting

Check handshake:

```bash
sudo wg
```

Check listening port:

```bash
sudo ss -tulpn | grep 51820
```
