# remote-cluster-provisioner
## Prerequisites
### Management cluster
### GPU Node/Cluster
- use ubuntu 22 jammy
- make passwordless user
- Install GPU Drivers


## Troubleshooting
You initialized Kubernetes but:

CNI plugins were not installed
or

/opt/cni/bin is empty
or

The CNI tarball was never extracted
```bash
sudo mkdir -p /opt/cni/bin

CNI_VERSION="v1.5.1"

wget https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-amd64-${CNI_VERSION}.tgz

sudo tar -C /opt/cni/bin -xzf cni-plugins-linux-amd64-${CNI_VERSION}.tgz

```

```bash 
kubectl delete packagevariants enterprise-gateway-variant gpu-operator-variant harbor-variant k8s-dra-driver-gpu-variant kai-scheduler-variant keycloak-variant kubeflow-variant kueue-variant kyverno-variant minio-variant nfs-provisioner-variant prometheus-stack-variant ml-platform-admin platform-overlays-variant post-install-config-variant


# add finalizer delete for
kubectl get repository.infra.nephio.org

kubectl delete repository.infra.nephio.org


kubectl patch svc dex -n auth --type=json -p='[{"op":"replace","path":"/spec/selector","value":{"app":"dex"}}]'


kubectl patch kustomization ml-platform-system \
-n ml-platform-system \
--type merge \
-p '{"spec":{"suspend":true}}'
```

## How to configure OpenVPN with persistent static IP addresses

This guide assigns a fixed `tun0` IP to each node so your Kubernetes cluster always sees the same addresses across reboots and reconnects.

### Part 1 — Decide your IP plan

**Assign a static IP to every node before you start.**

Each node gets one IP from the VPN subnet. The ASUS router uses `10.8.0.0/24` by default. Avoid `.1` — that is typically the router/server itself.

| Node | Static `tun0` IP | Role |
|------|------------------|------|
| master / control-plane | `10.8.0.2` | k8s master |
| worker-1 | `10.8.0.3` | k8s worker |
| worker-2 | `10.8.0.4` | k8s worker |
| worker-3 | `10.8.0.5` | k8s worker |
| worker-N | `10.8.0.(N+2)` | k8s worker |

> Write these down — you will use them in Part 3. These are the IPs your cluster will use permanently.

### Part 2 — Modify the .ovpn file (done once, shared across all nodes)

**Add the up-script hook to the `.ovpn` file.**

Edit the shared `.ovpn` file and add these two lines anywhere before the `<ca>` block:

```
script-security 2
up /etc/openvpn/set-static-ip.sh
```

> `script-security 2` allows OpenVPN to run external scripts. `up` runs `set-static-ip.sh` every time the tunnel comes up — on first connect, reconnect, and after reboot.  
> `/etc/openvpn/set-static-ip.sh` must exist on every node before OpenVPN connects. You will create it in Part 3.

**Your `.ovpn` file should now look similar to this:**

```
tls-cipher DEFAULT:@SECLEVEL=0
remote dcnnephio.asuscomm.com 8000
float
nobind
proto tcp-client
dev tun
sndbuf 0
rcvbuf 0
keepalive 10 30
comp-lzo yes
auth-user-pass
client
auth SHA1
cipher AES-128-CBC
remote-cert-tls server
script-security 2
up /etc/openvpn/set-static-ip.sh
<ca>
...
</ca>
<cert>
...
</cert>
<key>
...
</key>
```

> This file is identical for all nodes. The only per-node difference is the static IP set in Part 3.

### Part 3 — Create the static IP script on each node

**SSH into each node and run the block below. Replace `10.8.0.X` with the IP assigned to that node in Part 1.**

```bash
# Replace 10.8.0.X with this node's IP (e.g. 10.8.0.3 for worker-1)
sudo tee /etc/openvpn/set-static-ip.sh <<'EOF'
#!/bin/bash
STATIC_IP="10.8.0.X"
SUBNET="24"

sleep 1

ip addr flush dev tun0 2>/dev/null || true
ip addr add ${STATIC_IP}/${SUBNET} dev tun0
ip link set tun0 up

echo "$(date): tun0 set to ${STATIC_IP}/${SUBNET}" >> /var/log/openvpn-static-ip.log
EOF

sudo chmod +x /etc/openvpn/set-static-ip.sh

# Verify it exists and is executable
ls -la /etc/openvpn/set-static-ip.sh
```

> master → `10.8.0.2`, worker-1 → `10.8.0.3`, worker-2 → `10.8.0.4`, etc.

### Part 4 — Install the .ovpn file and connect

**Copy the modified `.ovpn` to each node.**

```bash
# From your local machine
scp client.ovpn user@NODE_IP:/tmp/client.ovpn

# On the node — rename to .conf so systemd picks it up automatically
sudo cp /tmp/client.ovpn /etc/openvpn/client/client.conf
sudo chmod 600 /etc/openvpn/client/client.conf
```

**Handle `auth-user-pass` (username/password).**

Your `.ovpn` uses `auth-user-pass`. Without a credentials file, OpenVPN will hang waiting for a password prompt on boot and never connect. Create one now:

```bash
sudo tee /etc/openvpn/client/credentials.txt <<EOF
YOUR_VPN_USERNAME
YOUR_VPN_PASSWORD
EOF

sudo chmod 600 /etc/openvpn/client/credentials.txt

# Point auth-user-pass at the credentials file
sudo sed -i 's|auth-user-pass|auth-user-pass /etc/openvpn/client/credentials.txt|' \
  /etc/openvpn/client/client.conf
```

**Enable and start OpenVPN.**

```bash
sudo systemctl enable openvpn-client@client
sudo systemctl start openvpn-client@client
sudo systemctl status openvpn-client@client
```

#### Verify the static IP

```bash
# tun0 should show your assigned static IP
ip addr show tun0

# Confirm the script ran
cat /var/log/openvpn-static-ip.log
# Expected: Mon May 18 10:23:01 UTC 2026: tun0 set to 10.8.0.3/24
```

#### Test that the IP survives a reconnect

```bash
sudo systemctl restart openvpn-client@client
sleep 5 && ip addr show tun0

# Log should show a new entry with the same IP
cat /var/log/openvpn-static-ip.log
```


#### Get AWS user keypair
```bash
kubectl get secret aws-node-001-ssh-key -o jsonpath='{.data.ssh-privatekey}' | base64 -d > aws-node-001.pem
chmod 600 aws-node-001.pem
ssh -i aws-node-001.pem ubuntu@<public-ip>
```