# remote-cluster-provisioner

A Kubernetes operator that provisions and manages remote GPU clusters from a central management cluster. It handles the full lifecycle of remote nodes: SSH-based bootstrapping, WireGuard VPN registration (optional, see [Running without WireGuard](#running-without-wireguard-disablevpn)), CRI-O runtime installation via a prebuilt OCI artifact (`cnlab-runtime`), kubeadm join, and Nephio/Porch platform deployment.

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
    ├── Provisions Google Cloud (GCE) nodes via a startup script
    ├── Registers WireGuard peers on VPN server (skipped when disableVPN is set)
    └── Syncs cnlab-runtime registry credentials to each node
```

The remote cluster's `NodeProvisionReconciler` is **fully autonomous** — it refreshes bootstrap tokens against the local Kubernetes API and can add new nodes even when disconnected from the management cluster.

---

## Prerequisites

### Management cluster
- Kubernetes cluster with CRDs installed (see [Deploy](#deploy))
- Nephio/Porch installed (for PackageVariant deployment)
- SSH access to all remote nodes from the management cluster pod network (via WireGuard, or directly when the cluster runs with `disableVPN: true`)

### Remote nodes
- Ubuntu 22.04 (Jammy)
- Passwordless `sudo` for the SSH user
- WireGuard VPN connectivity to the management cluster's VPN server (**only when `disableVPN` is `false`**, the default; not needed when the cluster runs without a VPN)
- Direct network reachability between the controller and the node (and between nodes) when the cluster runs with `disableVPN: true`
- GPU nodes: the controller does not install or version-manage the NVIDIA driver, container toolkit or device plugin (there are no such fields in `softwareConfig`); provide them on the host or via the GPU Operator

### WireGuard VPN server
- **Required only when `disableVPN` is `false`** (the default) — it provides node-to-node and management connectivity. Clusters created with `disableVPN: true` never contact a VPN server.
- See [docs/wireguard-setup-bundle/WIREGUARD_SETUP.md](docs/wireguard-setup-bundle/WIREGUARD_SETUP.md) for setup

---

## Deploy

```bash
# Build and push (or pick an already published) controller image, then install
# the CRDs and deploy the controller with that image:
make deploy IMG=<registry>/remote-cluster-provisioner:<tag>
```

`config/manager/manager.yaml` only carries the placeholder image `controller:latest`;
`make deploy IMG=...` rewrites it via `kustomize edit set image`, so a bare
`kubectl apply -k config/default` would deploy an unpullable image.

Alternatively, the static manifests in `deploy/` (CRDs, RBAC, metrics Service, Deployment
in namespace `remote-cluster-provisioner-system`) can be applied with kustomize after setting
the image; the placeholder `ghcr.io/<ORG>/remote-cluster-provisioner` with tag
`REPLACE_WITH_RELEASE_TAG` in `deploy/kustomization.yaml` must be replaced first (it is deliberately
not a valid image reference, so an unedited apply fails with `InvalidImageName`):

```bash
cd deploy
kustomize edit set image \
  ghcr.io/<ORG>/remote-cluster-provisioner=ghcr.io/<your-org>/remote-cluster-provisioner:<tag>
kubectl apply -k .
```

The controller image is **not obfuscated** by default. To build an obfuscated (garble) image,
opt in explicitly with `docker build --build-arg OBFUSCATE=true .` (or set the `OBFUSCATE`
repository variable to `true` for the publish workflow); this makes crashes much harder to debug.
The Go toolchain used for the image comes from `go.mod` (Dockerfile `ARG GO_VERSION`, kept in sync
by the publish workflow), the same version CI tests with.

### SSH host key verification (optional)

By default the controller does **not** verify SSH host keys (it logs a warning once). To enable
verification, set `SSH_KNOWN_HOSTS_FILE` on the controller container to the path of an OpenSSH
`known_hosts` file (mount it from a ConfigMap or Secret; see the commented example in
`deploy/deployment.yaml`). When set:

- the controller **fails closed**: connections to hosts that are not listed in the file are
  rejected, so every node (including new ones you are about to provision, on-prem hosts and
  the VPN server) must be added to the file **before** you create its resource;
- only the host key types present in the file for a host are negotiated, so include the type
  the host actually offers (`ssh-keyscan -t ed25519,ecdsa,rsa <host>`);
- an unreadable or malformed file makes every SSH connection fail rather than silently falling
  back to no verification.

There is intentionally no trust-on-first-use cache: reinstalled nodes get new host keys and
must be re-seeded in the file.

After deploy, apply your cluster credentials and config:

```bash
# SSH credentials for the control-plane node
kubectl apply -f config/samples/infra_v1_remotecluster_cnlab_runtime.yaml

# On-prem node provisioning config (on the remote cluster)
kubectl apply -f config/samples/ml_v1alpha1_nodeprovision.yaml
```

Complete, copy-and-edit cluster samples (all secrets are `CHANGE_ME` placeholders):

| Sample | What it shows |
|---|---|
| `config/samples/infra_v1_remotecluster_vpn.yaml` | Cluster **with** WireGuard: CPU control-plane + GPU and CPU workers, `insecureRegistries`, `imagePrepulls` |
| `config/samples/infra_v1_remotecluster_novpn.yaml` | Cluster **without** a VPN (`disableVPN: true`): GPU control-plane (also a compute node) + GPU and CPU workers |

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

Set `disableVPN: true` on the **control-plane** `RemoteCluster` to run the whole cluster without
WireGuard (see [Running without WireGuard](#running-without-wireguard-disablevpn)); `vpnConfig` is
then ignored.

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
  disableVPN: false             # true = the cluster runs without WireGuard; vpnRange/vpnServerPublicConfig are then unused
  vpnRange: "10.9.0.0/24"
  vpnServerPublicConfig:
    publicIP: 13.215.206.108
    sshPort: "22"
    sshUsername: ubuntu
    vpnPort: "51820"
    vpnSshCredentialsRef:
      name: vpn-server-secret
      namespace: default
      key: id_rsa
  softwareConfig:
    kubernetesVersion: v1.34.2
    insecureRegistries:         # optional: registries served over plain HTTP / untrusted TLS (host or host:port)
      - harbor.example.com:30002
    cnlabRuntime:
      registry: ghcr.io
      repository: vitu-mafeni/cnlab-runtime
      version: 1.0.0-beta
      orasVersion: 1.3.2
      credentialsRef:
        name: cnlab-runtime-registry
        namespace: default
```

A `NodeProvision` picks its config with `spec.clusterName` (matched against the NetConfig's
`spec.clusterName`). It may be omitted only while the namespace has exactly one
`NodeProvisionNetConfig`; with several, a `NodeProvision` without it fails with an error asking for it.

`insecureRegistries` are written as CRI-O `registries.conf.d` drop-ins (`insecure = true`) on every
provisioned node before CRI-O starts; hosts of `imagePrepulls` images are also marked insecure
(backward compatible). Already-provisioned nodes need a one-time manual drop-in and
`systemctl restart crio` — see the controllers user guide, section 5.2.

---

### `NodeProvision` — provision a node from the remote cluster

Managed by the **remote cluster** controller. Supports on-prem (SSH), AWS (EC2 cloud-init) and
Google Cloud (GCE startup script).

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: gpu-worker-01
spec:
  provider: OnPrem            # or: AWS, GCP
  clusterName: my-cluster     # selects the NodeProvisionNetConfig; required when a namespace has several clusters
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

For Google Cloud (GCE) provisioning (full guide: [docs §5.4](docs/controllers-user-guide.md);
sample: `config/samples/ml_v1alpha1_nodeprovision_gcp.yaml`):

```yaml
spec:
  provider: GCP
  nodeLabel: cpu              # cpu -> e2-standard-4 | gpu -> n1-standard-8 + 1x nvidia-tesla-t4
  region: us-central1         # or gcpConfig.zone: us-central1-a (authoritative when both are set)
  gcpConfig:                  # optional; project, zone, network and image are auto-resolved
    projectId: my-project
    bootDiskSizeGB: 100
  credentialsRef:
    name: gcp-node-credentials   # Secret with the service-account key under `credentials.json`
```

Needs the Compute Engine API, a service account with `roles/compute.instanceAdmin.v1` (and
`roles/compute.securityAdmin` for the per-node firewall rules) and a JSON key in the Secret. With a
VPN the node joins over WireGuard (UDP 51820 admitted from the VPN server only); with
`disableVPN` no VPN server is contacted and the VM's internal IP is the kubelet node IP.

`spec.disableVPN` normally does not need to be set on a `NodeProvision`: it is inherited from the
cluster (see [Running without WireGuard](#running-without-wireguard-disablevpn)).

**Status fields:**

| Field | Description |
|---|---|
| `phase` | One of `Pending`, `Validating`, `Provisioning`, `CreatingInstance` (AWS, GCP), `WaitingForInstance` (AWS, GCP), `ConfiguringVPN`, `Bootstrapping`, `Joining`, `RegisteringNode`, `VerifyingHealth`, `PrePullingImages`, `Ready`, `Failed`, `Deleting`. Typical path: `Pending` → `Validating` → (`CreatingInstance` → `WaitingForInstance`, AWS/GCP) → `ConfiguringVPN` → `Bootstrapping` → `Joining` → `RegisteringNode` → (`PrePullingImages`) → `Ready`; the exact sequence differs by provider. `ConfiguringVPN` is skipped when the VPN is disabled. `Failed` is terminal after 5 consecutive failures; `Deleting` is shown during teardown |
| `message` | Human-readable status including retry count on failure |
| `provisionRetryCount` | Consecutive provisioning failures (resets to 0 on success) |
| `runtimeCredentialsHash` | SHA-256 of last synced registry credentials — triggers re-sync on rotation |
| `vpnIp` | WireGuard IP allocated for this node |
| `publicIp` / `privateIp` | Cloud provider IPs (AWS, GCP) |
| `instanceId` | Cloud provider instance ID (AWS) / instance name (GCP) |

---

## Running without WireGuard (`disableVPN`)

WireGuard is only required when `disableVPN` is `false` (the default). Set
`spec.disableVPN: true` when every node is directly reachable (public or otherwise routable IPs):
no VPN server is contacted, no VPN range/credentials are published, no peers or WireGuard
packages/config are created or removed, no AWS security-group / GCP firewall rule for the WireGuard UDP port is added, and
flannel is pinned per node to the node's own address (`--iface=$(FLANNEL_NODE_IP)` from the pod's
`status.hostIP`, i.e. the kubelet node IP) instead of `wg0`.

- **Inheritance:** the mode is a property of the whole cluster. Set it on the **control-plane**
  `RemoteCluster`; it is published in the cluster's `NodeProvisionNetConfig.spec.disableVPN` and
  inherited by worker `RemoteCluster`s and by every `NodeProvision` (on-prem, AWS and GCP) in that
  cluster. Flow: control-plane `RemoteCluster` → `NodeProvisionNetConfig` → `NodeProvision`.
- **Mismatch:** setting `disableVPN: true` on a worker or `NodeProvision` under a cluster that
  runs a VPN is rejected with a clear message because a mixed cluster cannot work: a
  `RemoteCluster` worker is failed with condition `VPNModeMismatch` without consuming the retry
  budget, a `NodeProvision` goes to `Failed` with the reason in `status.message` (also without
  consuming retries, so it never becomes terminal). Fix the spec.
- **Fixed once provisioned:** the mode a `RemoteCluster` node was provisioned with is recorded in
  the annotation `infra.dcn.ssu.ac.kr/provisioned-vpn-mode` (`vpn` or `novpn`). Later flips of
  `spec.disableVPN` are ignored for that node (condition `VPNModeChangeIgnored`; a provisioned
  worker that disagrees with its control-plane gets `VPNModeMismatch` and is left untouched);
  re-provision the node to change its mode. For a `NodeProvision`, changing `disableVPN` on an
  existing resource is likewise unsupported: deletion cleanup follows what was recorded in
  `status` (`vpnIp` for a VPN node, only `ipAddress` for a VPN-less one), not the current spec.
- **Networking is yours to manage:** allow TCP 6443 to the control-plane, kubelet TCP 10250 and
  flannel VXLAN UDP 8473 between nodes. Pod traffic then crosses the network unencrypted.
- `spec.host` / `ipAddress` must be an IP bound to an interface on the node (a NATed public IP
  is rejected early); for AWS and GCP the instance's private / internal IP is used.

See [docs/controllers-user-guide.md](docs/controllers-user-guide.md) (§5.1, §7.3) for details.

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

### GCP service-account key

For `provider: GCP`. The key must be of `"type": "service_account"`; the data key is `credentials.json`
(or `serviceAccountKey`, or the name set in `credentialsRef.key`). The Secret must be in the
NodeProvision's namespace.

```bash
kubectl create secret generic gcp-node-credentials --from-file=credentials.json=./sa-key.json
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

## cnlab-runtime Build per Node OS

`cnlab-runtime` is published once per OS target: `<version>-ubuntu22` (Ubuntu 22.04
and newer) and `<version>-ubuntu20` (Ubuntu 20.04). Two ways to choose:

- **One OS for the whole cluster:** set the full tag, e.g. `version: "1.0.2-ubuntu22"`.
  Used exactly as given.
- **Mixed OS (or let each node decide):** set the base version and `osVariant: auto`.
  Each node reads its own `/etc/os-release` and installs the matching build:

  ```yaml
  cnlabRuntime:
    version: "1.0.2"      # base version, no -ubuntuNN suffix
    osVariant: auto
  ```

  | Node OS | Tag pulled |
  |---|---|
  | Ubuntu 20.04 (20.x, 21.x) | `1.0.2-ubuntu20` |
  | Ubuntu 22.04 through 25.x | `1.0.2-ubuntu22` |
  | Ubuntu 26.04 and newer | `1.0.2-ubuntu26` |
  | Ubuntu older than 20.04, or not Ubuntu | install fails before contacting the registry |

  `osVariant: auto` requires an explicit `version` without an OS suffix; it works
  for SSH-provisioned nodes and for AWS/GCP cloud-init nodes alike. It applies to
  both `RemoteCluster` and `NodeProvisionNetConfig` (`softwareConfig.cnlabRuntime`).

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
- All of the above applies only when `disableVPN` is `false`; clusters running with `disableVPN: true` do not use a VPN server at all.
