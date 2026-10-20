# remote-cluster-provisioner

A Kubernetes operator that provisions and manages remote GPU clusters from a central management cluster. It handles the full lifecycle of remote nodes: SSH-based bootstrapping, WireGuard VPN registration, CRI-O runtime installation via a prebuilt OCI artifact (`cnlab-runtime`), kubeadm join, and Nephio/Porch platform deployment.

---

## Architecture

```
Management Cluster
└── RemoteClusterReconciler
    ├── SSHes into control-plane → kubeadm init, creates NodeProvisionNetConfig
    ├── SSHes into workers → kubeadm join
    ├── Syncs cnlab-runtime credentials to remote cluster
    └── Deploys Nephio PackageVariants (platform stack)

Remote Cluster (autonomous after initial provisioning)
└── NodeProvisionReconciler
    ├── Reads NodeProvisionNetConfig (join command, VPN config, credentials)
    ├── Provisions on-prem nodes via SSH
    ├── Provisions AWS EC2 nodes via cloud-init
    ├── Registers WireGuard peers on VPN server
    └── Syncs cnlab-runtime registry credentials to each node
```

The remote cluster's `NodeProvisionReconciler` is **fully autonomous** — it refreshes bootstrap tokens against the local Kubernetes API and can add new nodes even when disconnected from the management cluster.

---

## Prerequisites

### Management cluster
- Kubernetes cluster with CRDs installed (see [Deploy](#deploy))
- Nephio/Porch installed (for PackageVariant deployment)
- SSH access to all remote nodes from the management cluster pod network (via WireGuard)

### Remote nodes
- Ubuntu 22.04 (Jammy)
- Passwordless `sudo` for the SSH user
- WireGuard VPN connectivity to the management cluster's VPN server
- GPU nodes: NVIDIA drivers pre-installed (driver version configured in `softwareConfig`)

### WireGuard VPN server
- Required for node-to-node and management connectivity
- See [docs/wireguard-setup-bundle/WIREGUARD_SETUP.md](docs/wireguard-setup-bundle/WIREGUARD_SETUP.md) for setup

---

## Deploy

```bash
# Install CRDs and deploy the controller
kubectl apply -k config/default
```

After deploy, apply your cluster credentials and config:

```bash
# SSH credentials for the control-plane node
kubectl apply -f config/samples/infra_v1_remotecluster_cnlab_runtime.yaml

# On-prem node provisioning config (on the remote cluster)
kubectl apply -f config/samples/ml_v1alpha1_nodeprovision.yaml
```

---

## CRDs

### `RemoteCluster` — provision a remote node

Managed by the **management cluster** controller. One CR per physical node.

```yaml
apiVersion: infra.dcn.ssu.ac.kr/v1
kind: RemoteCluster
metadata:
  name: ml-cluster-cp
spec:
  clusterName: ml-cluster        # shared across all nodes in the same cluster
  nodeInfo:
    nodeType: control-plane      # or: worker
    hardwareType: cpu            # or: gpu
    softwareConfig:
      kubernetesVersion: v1.34.2
      cnlabRuntime:
        registry: ghcr.io
        repository: vitu-mafeni/cnlab-runtime
        version: 1.0.0-beta
        orasVersion: 1.3.2
        credentialsRef:
          name: cnlab-runtime-registry   # Secret with username + token keys
          namespace: default
  host: 192.168.3.234
  port: "22"
  user: ubuntu
  auth:
    sshPrivateKeySecretRef:
      name: cp-node-ssh-secret
      key: password
  vpnConfig:
    ip: 10.9.0.13
    vpnServerPublicIP: 13.215.206.108
    vpnServerSSHPort: "22"
    vpnServerSSHUsername: ubuntu
    vpnSshCredentialsRef:
      name: vpn-server-ssh-secret
      namespace: default
      key: id_rsa
```

**Status fields:**

| Field | Description |
|---|---|
| `phase` | `Provisioning` → `Ready` → `Failed` |
| `message` | Human-readable status including retry count on failure |
| `provisionRetryCount` | Consecutive provisioning failures (resets to 0 on success) |
| `cnlabSyncRetryCount` | Consecutive credential-sync failures (resets to 0 on success) |
| `joinCommand` | Kubeadm join command cached from the remote control-plane |

---

### `NodeProvisionNetConfig` — cluster-wide config on the remote cluster

Created automatically by the management cluster controller. Can also be applied manually for testing.

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvisionNetConfig
metadata:
  name: my-cluster-netconfig
spec:
  clusterName: my-cluster
  vpnRange: "10.9.0.0/24"
  vpnServerPublicConfig:
    publicIP: 13.215.206.108
    sshPort: 22
    sshUsername: ubuntu
    vpnSshCredentialsRef:
      name: vpn-server-secret
      namespace: default
      key: id_rsa
  softwareConfig:
    kubernetesVersion: v1.34.2
    nvidiaDriverVersion: "550"
    nvidiaContainerToolkitVersion: 1.17.3-1
    k8sDevicePluginVersion: v0.17.1
    cnlabRuntime:
      registry: ghcr.io
      repository: vitu-mafeni/cnlab-runtime
      version: 1.0.0-beta
      orasVersion: 1.3.2
      credentialsRef:
        name: cnlab-runtime-registry
        namespace: default
```

---

### `NodeProvision` — provision a node from the remote cluster

Managed by the **remote cluster** controller. Supports on-prem (SSH) and AWS (EC2 cloud-init).

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: gpu-worker-01
spec:
  provider: OnPrem            # or: AWS
  role: worker
  hardwareType: gpu           # or: cpu — controls which images are pre-pulled
  ipAddress: 192.168.28.150
  sshPort: 22
  sshUsernameOverride: ubuntu
  credentialsRef:
    name: gpu-worker-01-ssh-secret
    namespace: default
    key: id_rsa
```

For AWS provisioning:

```yaml
spec:
  provider: AWS
  region: ap-southeast-1
  instanceType: g4dn.xlarge
  awsConfig:
    vpcId: vpc-xxxxxxxx
    subnetId: subnet-xxxxxxxx
    securityGroupIds: [sg-xxxxxxxx]
    ami: ami-xxxxxxxx
    keyPairName: my-keypair
    iamInstanceProfile: my-profile
    rootVolumeSizeGB: 100
  credentialsRef:
    name: aws-credentials-secret
    namespace: default
```

**Status fields:**

| Field | Description |
|---|---|
| `phase` | `Pending` → `Provisioning` → `Bootstrapping` → `Joining` → `Ready` → `Failed` |
| `message` | Human-readable status including retry count on failure |
| `provisionRetryCount` | Consecutive provisioning failures (resets to 0 on success) |
| `runtimeCredentialsHash` | SHA-256 of last synced registry credentials — triggers re-sync on rotation |
| `vpnIp` | WireGuard IP allocated for this node |
| `publicIp` / `privateIp` | Cloud provider IPs (AWS only) |
| `instanceId` | Cloud provider instance ID (AWS only) |

---

## Secrets

### SSH credential

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cp-node-ssh-secret
type: Opaque
stringData:
  # Private-key auth (auto-detected when value starts with "-----BEGIN")
  id_rsa: |
    -----BEGIN OPENSSH PRIVATE KEY-----
    ...
    -----END OPENSSH PRIVATE KEY-----
  # Or password auth:
  # password: "your-password"
```

### cnlab-runtime registry credentials

Required when pulling from a private registry (GHCR). Needs `packages:read` scope minimum.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cnlab-runtime-registry
type: Opaque
stringData:
  username: "your-github-username"
  token: "ghp_xxxx"
```

Referenced from `softwareConfig.cnlabRuntime.credentialsRef` in both `RemoteCluster` and `NodeProvisionNetConfig`.

---

## Retry Behavior

All provisioning failures are counted. After **5 consecutive failures** the controller stops retrying and the resource enters a terminal `Failed` state with a message indicating manual intervention is required.

**`RemoteCluster`** — provisioning retries:

```bash
# Check retry count
kubectl get remotecluster ml-cluster-cp -o jsonpath='{.status.provisionRetryCount}'

# Reset to re-enable retries after fixing the underlying problem
kubectl patch remotecluster ml-cluster-cp --subresource=status --type=merge \
  -p '{"status":{"provisionRetryCount":0}}'
```

**`RemoteCluster`** — cnlab-runtime credential sync retries (VPN may be down):

```bash
kubectl patch remotecluster ml-cluster-cp --subresource=status --type=merge \
  -p '{"status":{"cnlabSyncRetryCount":0}}'
```

**`NodeProvision`** — provisioning retries:

```bash
kubectl patch nodeprovision gpu-worker-01 --subresource=status --type=merge \
  -p '{"status":{"provisionRetryCount":0}}'
```

Logs include attempt number and max retries on every failure:

```
ERROR  provisioning failed — will retry  attempt=2 maxRetries=5
ERROR  provisioning failed — retry limit reached, no further retries  attempts=5 maxRetries=5
```

---

## cnlab-runtime Credential Sync

The `cnlab-runtime` OCI artifact is pulled during node bootstrapping. When credentials are added or rotated:

- The management cluster controller (`RemoteClusterReconciler`) detects the change via a SHA-256 hash annotation and SSHes into the remote control-plane to push the updated secret and patch the `NodeProvisionNetConfig`.
- The remote cluster controller (`NodeProvisionReconciler`) detects the change via `status.runtimeCredentialsHash` and SSHes into each `Ready` node to run `oras login`.

No manual restart is needed — rotation propagates automatically on the next reconcile (~23h for the management cluster, immediately for the remote cluster when the `NodeProvisionNetConfig` changes).

---

## Bootstrap Token Refresh

Kubeadm bootstrap tokens expire after 24 hours. The controller refreshes them proactively:

- **Management cluster**: SSHes into the remote control-plane every 23 hours to run `kubeadm token create --ttl 24h`
- **Remote cluster** (autonomous): calls the local Kubernetes API directly — no SSH dependency, works while disconnected from the management cluster

Logs:

```
INFO  Bootstrap token still valid  age=2h0m expiresIn=21h0m
INFO  Bootstrap token missing or expired — refreshing via local Kubernetes API  tokenAge=never
```

---

## Monitoring Logs

Controller logs are structured (JSON in production, text in development). Key log entries:

```bash
# Follow management cluster controller
kubectl logs -n remote-cluster-provisioner-system \
  deployment/remote-cluster-provisioner-controller-manager -f

# Follow remote cluster controller
kubectl logs -n remote-cluster-provisioner-system \
  deployment/remote-cluster-provisioner-controller-manager -f
```

Key events to watch:

| Message | Meaning |
|---|---|
| `Cluster fully ready` | RemoteCluster in Ready phase; shows `nextTokenRefreshIn` |
| `provisioning failed — will retry` | Transient failure; shows `attempt` and `maxRetries` |
| `retry limit reached, no further retries` | Terminal failure; manual reset required |
| `Runtime registry credentials changed — syncing` | Credential rotation detected |
| `cnlab-runtime credential sync reached retry limit` | VPN to remote cluster may be down |
| `Bootstrap token missing or expired — refreshing` | Token renewal in progress |

---

## Troubleshooting

### Node stuck in `Bootstrapping` phase

The on-prem provisioning goroutine is running in the background. Check progress:

```bash
kubectl get nodeprovision gpu-worker-01 -o jsonpath='{.status.message}'
```

If the goroutine is hung (e.g. `apt-get install` blocked on a lock), SSH into the node and check:

```bash
# Check if dpkg is locked
ssh ubuntu@<node-ip> 'sudo fuser /var/lib/dpkg/lock-frontend'

# Check provisioning log
ssh ubuntu@<node-ip> 'tail -50 /var/log/node-provision.log'
```

### AWS `oras pull` returns `unauthorized`

The `NodeProvisionNetConfig` is missing `cnlabRuntime.credentialsRef`. Apply the credentials:

```bash
kubectl apply -f config/samples/ml_v1alpha1_nodeprovision.yaml  # includes the secret
```

Check the bootstrap log on the EC2 instance:

```bash
# Get public IP from status
kubectl get nodeprovision aws-node-001 -o jsonpath='{.status.publicIp}'

# Retrieve SSH key
kubectl get secret aws-node-001-ssh-key \
  -o jsonpath='{.data.ssh-privatekey}' | base64 -d > aws-node-001.pem
chmod 600 aws-node-001.pem

# Tail the cloud-init log
ssh -i aws-node-001.pem ubuntu@<public-ip> \
  'sudo tail -100 /var/log/cloud-init-output.log'
```

### CNI plugins missing after kubeadm init

```bash
sudo mkdir -p /opt/cni/bin
CNI_VERSION="v1.5.1"
wget https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-amd64-${CNI_VERSION}.tgz
sudo tar -C /opt/cni/bin -xzf cni-plugins-linux-amd64-${CNI_VERSION}.tgz
```

### PackageVariants not deploying

```bash
# Check repository and package variants
kubectl get repository.infra.nephio.org
kubectl get packagevariants

# Delete stale variants to force re-creation
kubectl delete packagevariants \
  enterprise-gateway-variant gpu-operator-variant harbor-variant \
  keycloak-variant kubeflow-variant prometheus-stack-variant \
  ml-platform-admin platform-overlays-variant post-install-config-variant
```

### Cluster ready but Dex service selector broken

```bash
kubectl patch svc dex -n auth --type=json \
  -p='[{"op":"replace","path":"/spec/selector","value":{"app":"dex"}}]'
```

---

## VPN Reference

- **WireGuard setup**: [docs/wireguard-setup-bundle/WIREGUARD_SETUP.md](docs/wireguard-setup-bundle/WIREGUARD_SETUP.md)
- The controller dynamically registers/removes WireGuard peers via SSH on the VPN server as nodes are provisioned and deleted.
- The VPN range is configured in `NodeProvisionNetConfig.spec.vpnRange`. IPs are allocated from the start of the range and released IPs are reused.
