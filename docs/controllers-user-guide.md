# remote-cluster-provisioner — Controllers User Guide

> **Repository:** [Github](https://github.com/vitu-mafeni/remote-cluster-provisioner/tree/harbor)

## Table of contents

1. [Overview](#1-overview)
2. [Architecture & Diagrams](#2-architecture--diagrams)
3. [Prerequisites](#3-prerequisites)
4. [Installation](#4-installation)
5. [Usage](#5-usage)
6. [Examples](#6-examples)
7. [Logs & Troubleshooting](#7-logs--troubleshooting)
8. [Controller Lifecycle / Reconciliation](#8-controller-lifecycle--reconciliation)
9. [Operations](#9-operations)
10. [Reference](#10-reference)

---

## 1. Overview

### 1.1 What the controllers are

`remote-cluster-provisioner` runs as a single controller Deployment that manages three kinds of
resources (CRDs), across two areas of responsibility:

| Resource (Kind) | Where you create it | What it's for |
|---|---|---|
| `RemoteCluster` | **Management cluster** | Bootstraps a brand-new remote cluster (control plane + workers) from bare hosts |
| `NodeProvision` | **Remote (workload) cluster** | Adds worker nodes to a cluster that already exists |
| `NodeProvisionNetConfig` | **Remote (workload) cluster** | Shared cluster-wide configuration (VPN, join command, registry credentials) that `NodeProvision` reads from |

The same controller image runs on both the management cluster and every remote cluster it
creates — which behavior you get is simply a matter of which of these resources you create on
which cluster.

`NodeProvisionNetConfig` is **not** something you interact with much directly: it's created and
kept up to date automatically, and its only job is to hold shared settings (VPN range, join
command, registry credentials) that `NodeProvision` reads when adding a node. You mainly touch it
when you want to change a cluster-wide setting (e.g. rotate registry credentials) without editing
every node individually.

### 1.2 What each controller does

**Cluster bootstrap (`RemoteCluster`, applied on the management cluster)** — given SSH access to a
bare Ubuntu 22.04 host and a `RemoteCluster` resource, it turns that host into either:
- a fully working Kubernetes **control-plane** node (Kubernetes itself, container runtime,
  networking, and a GitOps platform stack), or
- a **worker** node joined to a control-plane you already created in the same logical cluster.

Once a cluster is up, it also keeps that cluster's shared configuration in sync (VPN settings,
software versions, registry credentials), rotates its join token automatically, and cleanly tears
a node back down (removes it from Kubernetes, from the VPN, and resets it) when you delete the
`RemoteCluster` resource.

**Cluster scale-out (`NodeProvision`, applied on the remote/workload cluster itself)** — given a
`NodeProvision` resource, it adds **one more worker node** to a cluster that a `RemoteCluster` has
already bootstrapped, using either:
- **`OnPrem`** — an existing bare-metal/VM host you already have, reached over SSH, or
- **`AWS`** — a brand-new EC2 instance it launches for you (it can pick a sensible instance type,
  AMI, and network config automatically if you don't specify one).

This one keeps working even if the management cluster becomes unreachable — it can add nodes to
its own cluster independently.

### 1.3 How they interact with each other

```
Management cluster                          Remote (workload) cluster
┌───────────────────────┐                    ┌──────────────────────────┐
│ Cluster bootstrap      │── SSH: install ──▶│ Control-plane / worker    │
│ (RemoteCluster)         │   Kubernetes,      │ nodes                    │
└──────────┬──────────────┘   VPN, platform    └──────────┬───────────────┘
           │ writes shared config over SSH                │
           ▼                                               ▼
   NodeProvisionNetConfig  ◀────────────────────  Cluster scale-out
   (join command, VPN,                            (NodeProvision)
    registry credentials)                          reads shared config
```

Cluster bootstrap is the only one of the two that talks to your GitOps platform (Nephio/Porch) and
the only one that does the very first Kubernetes install on a control-plane node. Cluster scale-out
never does that first install — it only ever adds a node to a cluster whose join information is
already available.

### 1.4 When to use which

| You want to... | Use this, applied here |
|---|---|
| Stand up a brand-new cluster from bare hosts | `RemoteCluster`, `nodeInfo.nodeType: control-plane`, on the **management cluster** |
| Add a worker while you're still building out a new cluster | `RemoteCluster`, `nodeInfo.nodeType: worker`, on the **management cluster** |
| Add on-prem/bare-metal capacity to a cluster that's already running | `NodeProvision`, `provider: OnPrem`, on the **remote cluster** |
| Burst into the cloud with extra capacity | `NodeProvision`, `provider: AWS`, on the **remote cluster** |
| Rotate registry credentials, change the VPN range, or bump the Kubernetes version for future nodes | Edit `NodeProvisionNetConfig` (or the parent `RemoteCluster`, which keeps it in sync) |

---

## 2. Architecture & Diagrams

### 2.1 Overall architecture

<figure>
<img src="diagrams/architecture.svg" alt="Overall architecture: the management cluster's bootstrap controller installs control-plane and worker nodes over SSH and writes shared config to the remote cluster; the remote cluster's scale-out controller reads that config to add more nodes and refreshes tokens against its own Kubernetes API; both controllers register VPN peers on the WireGuard server.">
<figcaption>Both controllers install/join nodes over SSH, keep the remote cluster's shared config in sync, and manage VPN peers; only the bootstrap controller talks to the GitOps platform.</figcaption>
</figure>

Numbered flow:

1. Cluster bootstrap connects over SSH to install Kubernetes, the container runtime, networking
   (over the WireGuard VPN), and — for control planes — a GitOps agent plus platform baseline.
2. On success, it writes the cluster's shared configuration (join command, VPN range, software
   versions, and — if configured — registry credentials) to that cluster's own
   `NodeProvisionNetConfig`.
3. Cluster scale-out, running on the remote cluster, reads that shared configuration whenever it
   needs a join command, VPN server details, or registry credentials for a **new** node.
4. Back on the management cluster, cluster bootstrap deploys your GitOps platform stack onto the
   new cluster (only if you opted into that).
5. Cluster scale-out provisions the new node — over SSH for on-prem hosts, or by launching an EC2
   instance for AWS — and joins it using the cached join command.
6. Once joined, cluster scale-out refreshes the cluster's own join token directly against its own
   Kubernetes API (no dependency on the management cluster) and labels the new node so
   GPU/CPU-specific workloads schedule onto it correctly.

Both controllers register and remove WireGuard peers on the VPN server automatically as nodes are
added and removed.

### 2.2 `RemoteCluster`: what happens, phase by phase

<figure>
<img src="diagrams/remotecluster-phases.svg" alt="RemoteCluster phase flow: a new resource starts Provisioning, moves to Ready on success or Failed on error; Failed retries up to 5 times before requiring manual attention; a Ready cluster receives routine upkeep and is cleaned up on deletion.">
<figcaption>RemoteCluster moves Provisioning → Ready, retries up to 5 times on failure, and is cleaned up on deletion.</figcaption>
</figure>

### 2.3 `NodeProvision`: what happens, phase by phase

<figure>
<img src="diagrams/nodeprovision-phases.svg" alt="NodeProvision phase flow: a new resource is validated, then provisioned over SSH (on-prem) or via EC2 and cloud-init (AWS), joins the cluster, optionally pre-pulls images on GPU nodes, then becomes Ready; failures retry up to 5 times before requiring manual attention.">
<figcaption>NodeProvision validates, provisions via SSH or EC2, joins the cluster, optionally pre-pulls GPU images, then becomes Ready.</figcaption>
</figure>

`status.phase` takes the values listed in [§10.2](#102-nodeprovision) (`Pending`, `Validating`,
`Provisioning`, `CreatingInstance`, `WaitingForInstance`, `ConfiguringVPN`, `Bootstrapping`,
`Joining`, `RegisteringNode`, `VerifyingHealth`, `PrePullingImages`, `Ready`, `Failed`,
`Deleting`); which ones you see depends on the provider. `ConfiguringVPN` is skipped when the VPN
is disabled (`disableVPN: true`).

---

## 3. Prerequisites

### 3.1 Cluster software and versions

- **A running management-cluster Kubernetes API** to install the CRDs and the controller into.
- **Target Kubernetes version for the clusters you're building** — set per cluster via
  `kubernetesVersion` (e.g. `v1.34.2`) in your `RemoteCluster`/`NodeProvisionNetConfig` — this is
  entirely independent of whatever Kubernetes version your management cluster runs.
- **Nephio/Porch**, on the management cluster, **only if** you turn on `gitConfig.enable: "true"`
  to have the platform stack deployed via GitOps.

### 3.2 Environment/configuration

- **SSH reachability** — the controller must be able to reach every host you list (by LAN IP or
  its VPN IP) on the SSH port you configure (default `22`). There's no bastion/jump-host support;
  it connects directly.
- **Passwordless `sudo`** for the SSH user on every host you provision.
- **Ubuntu 22.04 (Jammy)** on every host that becomes a cluster node.
- **A WireGuard VPN server** reachable over SSH — **required only when `disableVPN` is `false`**
  (the default); clusters created with `disableVPN: true` never contact a VPN server (see
  [§5.1](#51-remotecluster--createmanage-a-cluster-from-the-management-cluster)). See
  [WIREGUARD_SETUP.md](wireguard-setup-bundle/WIREGUARD_SETUP.md) if you need to stand one up.
  Without a VPN, the nodes must be directly reachable from the controller and from each other.
- **GPU nodes**: NVIDIA drivers must already be installed on the host beforehand — this project
  does not install GPU drivers for you.
- **AWS credentials**, if you'll provision nodes with `provider: AWS` — either an access key/secret
  in a Secret, or an IAM role attached to the controller's pod.

### 3.3 Permissions

The controller needs a `ClusterRole` covering the `RemoteCluster`, `NodeProvision`, and
`NodeProvisionNetConfig` resources (including their status), Kubernetes `Node` objects, `Secret`s
(for join-token management, including in `kube-system`), read access (`get`) to the single
`kube-public/cluster-info` `ConfigMap` through a namespaced `Role` in `kube-public` (no
cluster-wide `ConfigMap` access is granted), `Job`s (for GPU image pre-pulling), and — only if you
enable GitOps deployment — the Nephio/Porch resource types. All of this is already defined in the
manifests you'll apply in [§4](#4-installation); you don't need to hand-write any RBAC.

#### SSH host key verification

By default the controller does **not** verify SSH host keys (it logs one warning), so make sure
the network path to your nodes and VPN server (e.g. the VPN tunnel itself) is one you trust. To
turn verification on, set the environment variable **`SSH_KNOWN_HOSTS_FILE`** on the controller
container to the path of an OpenSSH `known_hosts` file (mount it from a ConfigMap or Secret; a
commented example is in `deploy/deployment.yaml`). This is opt-in — defaults are unchanged. When it is set:

- the controller **fails closed**: a host that is not listed in the file is rejected, so every
  node, on-prem host and VPN server must be **pre-seeded** in the file *before* the resource that
  uses it is created (`ssh-keyscan -t ed25519,ecdsa,rsa <host>`; for a new AWS instance the host
  key is not known in advance, so verification will reject it unless you have a way to
  pre-seed it);
- only the host key types present in the file for a host are negotiated, so list the type the
  host actually offers;
- if the file cannot be read or parsed, **every** SSH connection fails (there is no silent fallback
  to "no verification");
- there is no trust-on-first-use cache: a reinstalled node presents a new host key and must be
  re-seeded.

---

## 4. Installation

> This guide assumes the controller container image is already built and pushed somewhere your
> cluster can pull it from. If you don't have that yet, ask whoever manages your image builds for
> the image reference before continuing.

### 4.1 Install the CRDs

```bash
make install
```

This applies the three CRDs to your management cluster:

- `RemoteCluster`
- `NodeProvision`
- `NodeProvisionNetConfig`

Verify:

```bash
kubectl get crds | grep -E 'dcn.ssu.ac.kr'
```

Expected output:

```
nodeprovisionnetconfigs.ml.dcn.ssu.ac.kr   2026-01-01T00:00:00Z
nodeprovisions.ml.dcn.ssu.ac.kr            2026-01-01T00:00:00Z
remoteclusters.infra.dcn.ssu.ac.kr         2026-01-01T00:00:00Z
```

### 4.2 Deploy the controller

```bash
make deploy IMG=<registry>/remote-cluster-provisioner:<tag>
```

Point `IMG` at the pre-built image you were given. This creates:

- Namespace **`remote-cluster-provisioner-system`**
- The controller's service account and permissions
- `Deployment/remote-cluster-provisioner-controller-manager` (1 replica)

> If you install from the static manifests in `deploy/` instead (`kubectl apply -k deploy/`, after
> setting the image as described in `deploy/kustomization.yaml`), the Deployment and service
> account are named `remote-cluster-provisioner` (no `-controller-manager` suffix) in the same
> namespace; substitute that name in the `deployment/...` commands used throughout this guide.
> RBAC there grants only `get` on `kube-public/cluster-info` for ConfigMaps (namespaced Role).

The same steps (§4.1 and §4.2) are what you'll also run **on every remote cluster** you create,
before you start applying `NodeProvision` resources there.

### 4.3 Apply your first resources

```bash
# On the management cluster — SSH credentials + a RemoteCluster
kubectl apply -f config/samples/infra_v1_remotecluster_gpu_worker.yaml

# On the remote cluster, once it exists — add a node via NodeProvision
kubectl apply -f config/samples/ml_v1alpha1_nodeprovision.yaml
```

(See [§6](#6-examples) for a full walk-through with expected output at each step.)

### 4.4 Verify the installation

```bash
# Controller pod is Running
kubectl get pods -n remote-cluster-provisioner-system

# Health/readiness endpoints (port-forward first)
kubectl port-forward -n remote-cluster-provisioner-system \
  deployment/remote-cluster-provisioner-controller-manager 8083:8083
curl -sf localhost:8083/healthz && echo OK
curl -sf localhost:8083/readyz  && echo OK

# CRDs are established
kubectl get crd remoteclusters.infra.dcn.ssu.ac.kr \
  -o jsonpath='{.status.conditions[?(@.type=="Established")].status}'
```

---

## 5. Usage

### 5.1 `RemoteCluster` — create/manage a cluster from the management cluster

Full field reference is in [§10.1](#101-remotecluster). Minimal control-plane example:

```yaml
apiVersion: infra.dcn.ssu.ac.kr/v1
kind: RemoteCluster
metadata:
  name: ml-cluster-cp
spec:
  clusterName: ml-cluster        # shared across every node in this logical cluster
  host: 192.168.3.234
  port: "22"
  user: ubuntu
  nodeInfo:
    nodeType: control-plane       # or: worker
    hardwareType: cpu             # or: gpu
    softwareConfig:
      kubernetesVersion: v1.34.2
      cnlabRuntime:
        registry: ghcr.io
        repository: vitu-mafeni/cnlab-runtime
        version: 1.0.0-beta
        orasVersion: 1.3.2
        credentialsRef:
          name: cnlab-runtime-registry
          namespace: default
  auth:
    sshPrivateKeySecretRef:
      name: cp-node-ssh-secret
      key: id_rsa
  vpnConfig:
    ip: 10.9.0.13
    vpnServerPublicIP: 13.215.206.108
    vpnServerSSHPort: "22"
    vpnServerSSHUsername: ubuntu
    vpnSshCredentialsRef:
      name: vpn-server-ssh-secret
      namespace: default
      key: id_rsa
  gitConfig:
    enable: "true"                # omit/false to skip GitOps platform deployment entirely
    gitServer: "http://192.168.3.99:31810"
    gitUsername: "nephio"
    upstreamPlatformRepo: "catalog-workloads-mlplatform"
    packageRevision: "v1.0.0"
```

Key fields to get right:

- **`nodeInfo.nodeType`** — `control-plane` installs a brand-new cluster on this host;
  `worker` joins this host to whichever `RemoteCluster` in the same namespace has a matching
  `spec.clusterName` and `nodeType: control-plane`. If that control-plane isn't `Ready` yet, the
  worker just waits and checks again periodically.
- **`auth`** — exactly one of `sshPrivateKeySecretRef` or `passwordSecretRef`. Key auto-detection:
  a secret value starting with `-----BEGIN` is treated as a private key; anything else as a
  password.
- **`vpnConfig.ip`** — once set, the controller talks to the node over its VPN IP instead of
  `spec.host`, which keeps working even after the node moves onto a different network path.
- **`disableVPN`** (default `false`) — run the **whole cluster** without WireGuard, when
  `spec.host` is a public (or otherwise directly routable) IP that is fully reachable by the
  controller and by the other nodes. Set it on the **control-plane**; it decides for the cluster:
  the control-plane publishes it in the cluster's `NodeProvisionNetConfig`, and every worker
  (`RemoteCluster` workers and `NodeProvision`s, on-prem and AWS) inherits it. A node that sets
  `disableVPN: true` under a VPN control-plane is failed with a clear message, because a mixed
  cluster cannot work. Inheritance flows control-plane `RemoteCluster` → `NodeProvisionNetConfig`
  → `NodeProvision`/worker (see [§7.5](#75-operational-notes) for the resulting conditions).
  - `spec.host` must be an **IP bound to a local interface on the node** (a NATed public IP fails
    early with a clear error); it becomes the kubelet node IP and, for the control-plane, the API
    server advertise address, in place of the `wg0` address.
  - Nothing VPN-related is touched: no VPN server contact, no VPN range/credentials published or
    copied, no peer allocation or removal, no WireGuard install or teardown, no AWS security
    group rule for UDP 51820. Instead of `--iface=wg0`, flannel is pinned **per node to the
    node's own address** (`--iface=$(FLANNEL_NODE_IP)`, taken from the pod's `status.hostIP`, which
    is the kubelet node IP), so the VXLAN endpoint is always the interface that carries the node
    IP, even on multi-homed hosts. `vpnConfig` is ignored.
  - Nodes must reach the address the control-plane advertises. Pod traffic (flannel VXLAN,
    UDP 8473) then crosses the network unencrypted.
  - Firewalling is yours to manage: the controller only opens what it needs itself (for AWS
    nodes the default security group it creates gets **SSH only**). The kubelet port
    (**TCP 10250**) and flannel VXLAN (**UDP 8473**) between the cluster's nodes must be allowed
    by your own security group / firewall (and TCP 6443 to the control-plane for joins).
  - Choose the mode when you create the cluster; switching an existing cluster is not supported.
- **`gitConfig.enable`** — a **string** `"true"`/`"false"`, not a bare `true`/`false`. Only when
  `"true"` does the platform stack get deployed via GitOps.

### 5.2 `NodeProvisionNetConfig` — shared cluster settings

Normally created and kept up to date for you automatically (see [§8.1](#81-remotecluster-createupdatedelete)),
but you can also apply it directly on a remote cluster you're testing in isolation:

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvisionNetConfig
metadata:
  name: my-cluster-netconfig
spec:
  clusterName: my-cluster
  disableVPN: false             # true = cluster without WireGuard; vpnRange/vpnServerPublicConfig are then unused
  vpnRange: "10.9.0.0/24"
  vpnServerPublicConfig:
    publicIP: "13.215.206.108"
    sshPort: "22"
    sshUsername: ubuntu
    vpnPort: "51820"
    vpnSshCredentialsRef:
      name: vpn-server-secret
      namespace: default
      key: id_rsa
  softwareConfig:
    kubernetesVersion: "v1.34.2"
    insecureRegistries:         # optional; see "Insecure (plain-HTTP) registries" below
      - harbor.example.com:30002
    cnlabRuntime:
      registry: "ghcr.io"
      repository: "vitu-mafeni/cnlab-runtime"
      version: "1.0.0-beta"
      orasVersion: "1.3.2"
      credentialsRef:
        name: cnlab-runtime-registry
        namespace: default
```

**Which NetConfig does a `NodeProvision` use?** `spec.clusterName` of the `NodeProvision` selects
the `NodeProvisionNetConfig` whose `spec.clusterName` is equal (`RemoteCluster.spec.clusterName`
is what the controller writes there). Provisioning fails with a clear error when none, or more
than one, matches. For backward compatibility the field may be left empty **only while the
namespace holds exactly one `NodeProvisionNetConfig`**: that one is then used, as before. As soon
as a namespace holds several (several clusters), a `NodeProvision` without `spec.clusterName`
is failed (without consuming a retry) with an error asking you to set it — the controller never
guesses, because a wrong pick would use another cluster's VPN range, VPN mode, pre-pull images
and registry secret. A VPN peer is likewise never released from a guessed config when such a node
is deleted: deletion keeps reporting the error until `spec.clusterName` is set.

#### Insecure (plain-HTTP) registries

`softwareConfig.insecureRegistries` (also on `RemoteCluster.spec.nodeInfo.softwareConfig`, which
syncs it here, including clearing it when removed) lists registries (`host` or `host:port`, e.g.
`harbor.example.com:30002`) that are served over **plain HTTP or with an untrusted certificate**.
Every node the operator provisions — on-prem control-plane/worker (`RemoteCluster`), on-prem,
AWS and GCP `NodeProvision` — gets a `/etc/containers/registries.conf.d/50-insecure-<host>.conf`
drop-in with `insecure = true`, written **before** CRI-O (re)starts. Use it for registries that
other workloads pull from but that are not in `imagePrepulls`.

- Entries are bare `host` or `host:port` (port 1-65535): no `http://`, path, spaces, quotes or
  shell characters; at most 32. An invalid entry is rejected by the CRD and, on the controller
  side, fails the `RemoteCluster` (condition/message `InvalidInsecureRegistries`) or the
  `NodeProvision` with a message naming the entry.
- **Backward compatibility:** the registry host of every fully-qualified image in `imagePrepulls`
  (e.g. `harbor.example.com:30002` from `harbor.example.com:30002/team/img:1`) is **also** marked
  insecure, exactly as before. The effective list is the de-duplicated, sorted union of both.
  There is no opt-out for the derived hosts.
- **Already-provisioned nodes** are not touched, and CRI-O only reads `registries.conf.d` when it
  starts. On each existing node run once (ideally after cordoning/draining it, since restarting
  CRI-O restarts its containers):

  ```bash
  sudo mkdir -p /etc/containers/registries.conf.d
  sudo tee /etc/containers/registries.conf.d/50-insecure-harbor-example-com-30002.conf >/dev/null <<'EOF'
  [[registry]]
  location = "harbor.example.com:30002"
  insecure = true
  EOF
  sudo systemctl restart crio
  ```

  (the file name is only a label: `.` and `:` in the host are replaced by `-`).

`disableVPN` is normally set for you: the control-plane `RemoteCluster` publishes it here, and every
`NodeProvision` reads it (see [§5.1](#51-remotecluster--createmanage-a-cluster-from-the-management-cluster)
and [§7.5](#75-operational-notes)). Only set it by hand on a NetConfig you manage yourself, and
keep it consistent with the cluster's real mode.

> Older examples showed `softwareConfig.nvidiaDriverVersion`, `nvidiaContainerToolkitVersion`,
> and `k8sDevicePluginVersion`. **These are not fields of this resource** (the API server rejects
> them) — GPU driver/toolkit/device-plugin versions aren't configurable here; the controller does
> not install them for you.

### 5.3 `NodeProvision` — add a node from the remote cluster

**On-prem** (an existing host, reached over SSH):

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: gpu-worker-01
  namespace: default
spec:
  provider: OnPrem
  clusterName: my-cluster        # which NodeProvisionNetConfig (spec.clusterName) to use; required when the namespace has several
  role: worker
  hardwareType: gpu              # controls which pre-pull images target this node
  nodeLabel: gpu
  ipAddress: 192.168.28.150
  sshPort: 22
  sshUsernameOverride: ubuntu
  credentialsRef:
    name: gpu-worker-01-ssh-secret
    namespace: default
    key: id_rsa
```

**AWS** (a new EC2 instance — most fields auto-resolve):

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: aws-node-001
  namespace: default
spec:
  provider: AWS
  clusterName: my-cluster        # which NodeProvisionNetConfig (spec.clusterName) to use; required when the namespace has several
  role: worker
  nodeLabel: cpu                 # "cpu" → t3.xlarge | "gpu" → p3.2xlarge (auto-resolved)
  region: ap-northeast-2
  credentialsRef:
    name: aws-node-credentials
    namespace: default
  # awsConfig is entirely optional — every field below is auto-populated when omitted:
  # awsConfig:
  #   instanceType: p3.2xlarge   # overrides the nodeLabel → instance-type lookup
  #   ami: ami-0c9c942bd7bf113a2
  #   vpcId: vpc-xxxxxxxx
  #   subnetId: subnet-xxxxxxxx
  #   securityGroupIds: [sg-xxxxxxxx]
  #   keyPairName: my-keypair
  #   iamInstanceProfile: my-profile
  #   rootVolumeSizeGB: 100
  #   tags: {environment: production}
```

Important fields:

- **`disableVPN`** (default `false`) joins the node without a WireGuard tunnel — no VPN server
  connection, IP allocation, peer, WireGuard package or AWS security group rule. It is a property
  of the whole cluster, so you normally don't set it here: it is inherited from the cluster's
  `NodeProvisionNetConfig` (see `RemoteCluster.spec.disableVPN`), and setting it against a VPN
  cluster fails the provision. Use it only when the node is directly reachable from the
  controller and the control plane, **and** the control plane's API endpoint (the address in the
  kubeadm join command, i.e. what the control plane advertises) is reachable from the node. The
  node's own address becomes the kubelet node IP:
  - **OnPrem** — `spec.ipAddress`, which must be an IP actually bound to an interface on the
    node (a NATed public IP fails early with a clear error).
  - **AWS** — the instance's *private* IP (a public EC2 IP is NATed and cannot be a kubelet node
    IP), so the control plane must be able to route to it (same or peered VPC).
  - **GCP** — the instance's *internal* (VPC) IP, for the same reason; see [§5.4](#54-nodeprovision-on-google-cloud-gcp).

  Set it when you create the resource; changing it on an existing node is not supported.
- **`nodeLabel`** drives the AWS defaults — `"cpu"` → `t3.xlarge`, `"gpu"` → `p3.2xlarge`. Set
  `spec.awsConfig.instanceType` (or the top-level `spec.instanceType`) to pick an exact instance
  type instead.
- **`hardwareType`** (separate from `nodeLabel`) controls which pre-pull images target this node
  — only relevant for `gpu` nodes when your cluster config lists images to pre-pull.
- **`role`** — always `worker` in practice; this resource only ever joins nodes to an existing
  cluster, it never bootstraps a brand-new one (that's what `RemoteCluster` is for).
- **`credentialsRef.key`** — if omitted, the controller tries a few common key names
  automatically (`privateKey`, `id_rsa`, `ssh-privatekey`, `password`, `key`).

---

### 5.4 `NodeProvision` on Google Cloud (GCP)

`provider: GCP` creates a Compute Engine (GCE) VM, bootstraps it with a startup script (the same
bootstrap the AWS path renders into cloud-init: CRI-O, the cnlab-runtime, kubeadm packages,
WireGuard when a VPN is used, and `kubeadm join` with a reachability wait and retries) and joins it
to the cluster. The lifecycle mirrors AWS: `Validating` → (`ConfiguringVPN`) → `CreatingInstance` →
`WaitingForInstance` → `Bootstrapping` → `RegisteringNode` → (`PrePullingImages`) → `Ready`.

**Prerequisites**

- A GCP project with the **Compute Engine API** enabled (`gcloud services enable compute.googleapis.com`).
- A **service account** the controller uses, with a JSON key. It needs the permissions of
  `roles/compute.instanceAdmin.v1` (instances, disks, images, zone/machine/accelerator lookups) and
  `roles/compute.securityAdmin` (per-node firewall rules; optional, see "Firewall rules" below).
  Attaching a service account to the VM (`gcpConfig.serviceAccountEmail`) additionally needs
  `roles/iam.serviceAccountUser` on that account. On a Shared VPC add `roles/compute.networkUser` on
  the subnetwork and let the controller create firewall rules in the host project.
- A VPC network. The default is the `default` network; if your project has no `default` network (or it
  is custom-mode) set `gcpConfig.network` and `gcpConfig.subnetwork`.
- **Quota** for the machine type (and GPUs) in the region.
- An organisation policy that **enforces OS Login** (`compute.requireOsLogin`) is not supported: the
  controller logs in with an SSH key from instance metadata, which OS Login ignores.

**Secret format** — the service-account key JSON, under `credentials.json` (or `serviceAccountKey`;
or any key named in `credentialsRef.key`). Only `"type": "service_account"` keys are accepted. The
Secret must live in the NodeProvision's namespace. There is no MFA/STS equivalent: the Google client
libraries renew OAuth2 tokens from the key themselves.

```bash
kubectl create secret generic gcp-node-credentials -n default \
  --from-file=credentials.json=./sa-key.json
```

**Location** — `spec.gcpConfig.zone` (e.g. `us-central1-a`) is authoritative. `spec.region` is
optional and, when both are set, must be the zone's region. With only `region`, the controller picks
the first `UP` zone of that region that offers the machine type (and GPU) and records it in
`gcpConfig.zone`. The machine type is the top-level `spec.instanceType`
(`nodeLabel: cpu` → `e2-standard-4`; `nodeLabel: gpu` → `n1-standard-8` + one `nvidia-tesla-t4`).
Resolved defaults (project from the key, zone, network, boot image — latest Ubuntu 22.04 LTS from
`ubuntu-os-cloud`) are patched into the spec on the first pass; values you set are never overwritten.

**With a VPN** (the default; the cluster runs WireGuard):

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: gcp-node-001            # must be a valid GCE instance name (it becomes the instance + node name)
  namespace: default
spec:
  provider: GCP
  role: worker
  nodeLabel: cpu
  region: us-central1           # or set gcpConfig.zone instead
  credentialsRef:
    name: gcp-node-credentials
  # gcpConfig is optional — everything below is auto-populated when omitted:
  # gcpConfig:
  #   projectId: my-project
  #   zone: us-central1-a
  #   network: default
  #   subnetwork: default
  #   bootDiskSizeGB: 100
  #   bootDiskType: pd-ssd
  #   labels: {team: ml}
  #   networkTags: [ml-nodes]
  #   spot: false
```

The controller registers a WireGuard peer on the VPN server before the VM boots; the node reaches the
control plane over `wg0` and its kubelet node IP is the tunnel IP.

**Without a VPN** (`disableVPN: true` on the control-plane `RemoteCluster`, inherited by the node):

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: gcp-node-002
  namespace: default
spec:
  provider: GCP
  role: worker
  nodeLabel: gpu                # n1-standard-8 + 1x nvidia-tesla-t4
  hardwareType: gpu
  gcpConfig:
    zone: europe-west4-a
    firewallSourceRanges: ["172.20.0.0/16"]   # the control plane / controller (default: RFC1918)
  credentialsRef:
    name: gcp-node-credentials
```

No VPN server is contacted. The kubelet node IP is the VM's **internal** IP (read from the GCE
metadata server), so the control plane must be able to route to it (same VPC, VPC peering, Cloud
VPN/Interconnect), and the control plane's API endpoint must be reachable from the VM.

**GPUs** — N1: `gcpConfig.accelerator: {type: nvidia-tesla-t4, count: 1}`. A2/A3/G2 machine types
(`a2-highgpu-1g`, `g2-standard-8`, …) include their GPUs: leave `accelerator` unset. GPU VMs use
`onHostMaintenance: TERMINATE`. As on AWS, the bootstrap installs no NVIDIA software: the GPU Operator
owns the driver, container toolkit and CDI (leave Secure Boot off, the default, so its driver loads).

**Firewall rules** — created per node, targeted at the node's own network tag, named
`np-<instance>-<wg|ssh|mgmt>`, and deleted with the node:

| Mode | Rule | Allows |
|---|---|---|
| VPN | `wg` | UDP `vpnPort` (default 51820) **from the VPN server's IP only** |
| VPN | `ssh` (only if `firewallSourceRanges` is set) | TCP 22 from those ranges — with a VPN, SSH travels inside the tunnel |
| no VPN | `mgmt` | TCP 22, TCP 10250 (kubelet), UDP 8472 (flannel VXLAN) and ICMP from `firewallSourceRanges` (default RFC1918) |

If the service account may not create firewall rules the controller logs a warning and carries on
(rules managed centrally); pre-create equivalent rules in that case.

**What is created and deleted** — the instance (boot disk auto-deleted), the firewall rules above, the
VPN peer (VPN mode) and the `<name>-ssh-key` Secret (the generated private key; its public half is
injected through the instance's `ssh-keys` metadata for user `ubuntu`, or `sshUsernameOverride`).
`status.instanceId` is the GCE instance name. A relaunch after a controller crash adopts the existing
instance (matched by name **and** the `nodeprovision-uid` label) instead of creating a second one; an
instance of the same name that belongs to something else is never adopted or deleted.

**Notes**

- The startup script (which embeds the registry token and, in VPN mode, the WireGuard private key) is
  stored in instance metadata and is readable by anyone with `compute.instances.get` on the project —
  the same exposure as EC2 user-data. Restrict that permission; the node needs no service account.
- Pods can reach the GCE metadata server unless you block `169.254.169.254` with a NetworkPolicy.
- Spot VMs (`spot: true`) are deleted when preempted; the NodeProvision then fails and retries.
- `disableExternalIP: true` creates the VM without a public address; give it Cloud NAT for package
  downloads (and, with a VPN, to reach the VPN server).

---

## 6. Examples

### 6.1 End-to-end: bootstrap a control plane, add a worker, scale with `NodeProvision`

**Step 1 — apply the SSH secret and control-plane `RemoteCluster`** (on the management cluster):

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: cp-node-ssh-secret
type: Opaque
stringData:
  id_rsa: |
    -----BEGIN OPENSSH PRIVATE KEY-----
    ...
    -----END OPENSSH PRIVATE KEY-----
---
apiVersion: infra.dcn.ssu.ac.kr/v1
kind: RemoteCluster
metadata:
  name: ml-cluster-cp
spec:
  clusterName: ml-cluster
  host: 192.168.3.234
  port: "22"
  user: ubuntu
  nodeInfo:
    nodeType: control-plane
    hardwareType: cpu
    softwareConfig:
      kubernetesVersion: v1.34.2
  auth:
    sshPrivateKeySecretRef:
      name: cp-node-ssh-secret
      key: id_rsa
EOF
```

**Expect:** `status.phase` moves `"" → Provisioning` immediately, then stays `Provisioning` for
roughly 5–15 minutes while the control plane is installed. Watch it:

```bash
kubectl get remotecluster ml-cluster-cp -w
```

```
NAME             PHASE          MESSAGE
ml-cluster-cp    Provisioning   Provisioning in progress
ml-cluster-cp    Ready          Provisioned
```

Controller logs during this step (`kubectl logs -n remote-cluster-provisioner-system
deployment/remote-cluster-provisioner-controller-manager -f`) — an **illustrative transcript** of
what you'll typically see, for a cluster with `vpnConfig` configured (as in the full samples under
`config/samples/`). Exact timestamps and the number of "in progress" lines will vary with how long
the install actually takes on your hardware:

```
2026-09-08T10:15:03.412+0900   INFO    remotecluster   Starting provisioning node for cluster  {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "nodeType": "control-plane", "phase": ""}
2026-09-08T10:15:03.498+0900   INFO    remotecluster   Control plane init goroutine started    {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "startPhase": 0}
2026-09-08T10:15:33.501+0900   INFO    remotecluster   Control plane init in progress, requeueing      {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:16:03.512+0900   INFO    remotecluster   Control plane init in progress, requeueing      {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
                                        ⋮  (repeats roughly every 30s while Kubernetes, the container runtime, networking, and ArgoCD are installed over SSH; typically 5-15 minutes)  ⋮
2026-09-08T10:23:41.220+0900   INFO    remotecluster   Control plane init completed    {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "joinCommand": true}
2026-09-08T10:23:42.005+0900   INFO    remotecluster   Kubeadm bootstrap token has never been explicitly refreshed; will refresh now  {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:23:44.115+0900   INFO    remotecluster   Refreshed kubeadm bootstrap token       {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:23:44.900+0900   INFO    remotecluster   Creating PackageVariants        {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:23:45.760+0900   INFO    remotecluster   Core PackageVariants created — requeueing to allow platform sync before overlay step   {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "delay": "2m0s"}
2026-09-08T10:25:46.003+0900   INFO    remotecluster   Creating PackageVariants        {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:25:47.410+0900   INFO    remotecluster   PackageVariants created; cluster is fully ready        {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:25:48.020+0900   INFO    remotecluster   Kubeadm bootstrap token still valid     {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "refreshedAt": "2026-09-08T10:23:44+09:00", "nextRefreshIn": "22h58m0s"}
2026-09-08T10:25:48.021+0900   INFO    remotecluster   Cluster fully ready     {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "nextTokenRefreshIn": "22h58m0s"}
```

> A couple of lines appear more than once above — that's expected, not a duplicate or a retry: the
> platform stack is deployed in two waves (core, then the rest), and each wave's completion
> triggers its own follow-up check.

**Step 2 — add a worker** to the same `clusterName`:

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: worker-01-ssh-secret
type: Opaque
stringData:
  id_rsa: |
    -----BEGIN OPENSSH PRIVATE KEY-----
    ...
    -----END OPENSSH PRIVATE KEY-----
---
apiVersion: infra.dcn.ssu.ac.kr/v1
kind: RemoteCluster
metadata:
  name: ml-cluster-worker-01
spec:
  clusterName: ml-cluster        # must match the control-plane's clusterName
  host: 192.168.3.235
  port: "22"
  user: ubuntu
  nodeInfo:
    nodeType: worker
    hardwareType: gpu
  auth:
    sshPrivateKeySecretRef:
      name: worker-01-ssh-secret
      key: id_rsa
EOF
```

**Expect:** once the control plane is confirmed `Ready`, this host is joined to it. On success the
resulting Kubernetes node is labeled so GPU-specific workloads (like image pre-pull jobs) can
schedule onto it.

Controller logs during this step — illustrative transcript, same caveats as Step 1. Note the gap:
joining a worker happens as one continuous step, so there's no repeated "in progress" line — the
log simply goes quiet for a few minutes between the first line and the last:

```
2026-09-08T10:31:02.104+0900   INFO    remotecluster   Synced VPN server config from control-plane     {"cluster": "ml-cluster-worker-01", "clusterName": "ml-cluster", "cp": "ml-cluster-cp"}
                                        ⋮  (no further log lines while the join runs over SSH — typically a few minutes)  ⋮
2026-09-08T10:34:18.760+0900   INFO    remotecluster   Worker node joined to cluster   {"cluster": "ml-cluster-worker-01", "clusterName": "ml-cluster"}
2026-09-08T10:34:19.902+0900   INFO    remotecluster   Labeled worker node for DaemonSet targeting     {"cluster": "ml-cluster-worker-01", "node": "ml-cluster-worker-01", "hardwareType": "gpu"}
```

**Step 3 — scale out with `NodeProvision`**, applied **on the remote cluster** (`ml-cluster`)
once it's up:

```bash
kubectl --kubeconfig=ml-cluster.kubeconfig apply -f config/samples/ml_v1alpha1_nodeprovision.yaml
```

That sample file applies, in order: the `NodeProvision` resource (`provider: OnPrem`), its SSH
Secret, a `NodeProvisionNetConfig` (harmless to re-apply — it will already exist from Step 1/2),
the registry credentials Secret, and the VPN server SSH Secret.

```bash
kubectl --kubeconfig=ml-cluster.kubeconfig get nodeprovision gpu-worker-01 -w
```

```
NAME            PHASE          MESSAGE
gpu-worker-01   Pending
gpu-worker-01   Provisioning   Validation successful
gpu-worker-01   Bootstrapping  On-prem bootstrap goroutine started
gpu-worker-01   RegisteringNode
gpu-worker-01   Joining
gpu-worker-01   Ready          Node reached Ready state
```

Controller logs during this step — illustrative transcript, same caveats as Step 1, this time on
the **remote cluster's** controller:

```
2026-09-08T11:02:10.301+0900   INFO    nodeprovision   Request received, starting provisioning         {"nodeprovision": "gpu-worker-01", "provider": "OnPrem"}
2026-09-08T11:02:10.940+0900   INFO    nodeprovision   Validation successful   {"nodeprovision": "gpu-worker-01"}
2026-09-08T11:02:11.205+0900   INFO    nodeprovision   On-prem bootstrap goroutine started     {"nodeprovision": "gpu-worker-01"}
                                        ⋮  (installing Kubernetes and the container runtime over SSH — typically several minutes)  ⋮
2026-09-08T11:07:52.114+0900   INFO    nodeprovision   On-prem bootstrap completed     {"nodeprovision": "gpu-worker-01"}
2026-09-08T11:07:52.560+0900   INFO    nodeprovision   On-prem node provisioned, waiting for cluster join      {"nodeprovision": "gpu-worker-01"}
2026-09-08T11:08:22.601+0900   INFO    nodeprovision   Node not yet visible in cluster — waiting for kubelet to register      {"nodeprovision": "gpu-worker-01"}
2026-09-08T11:08:52.744+0900   INFO    nodeprovision   Node registered with control plane      {"nodeprovision": "gpu-worker-01", "nodeName": "gpu-worker-01"}
2026-09-08T11:08:53.310+0900   INFO    nodeprovision   Stamped ownership metadata on node      {"nodeprovision": "gpu-worker-01", "hardwareType": "gpu"}
2026-09-08T11:08:53.815+0900   INFO    nodeprovision   Node reached Ready state        {"nodeprovision": "gpu-worker-01"}
```

Confirm the node joined:

```bash
kubectl --kubeconfig=ml-cluster.kubeconfig get nodes -l infra.dcn.ssu.ac.kr/worker=true
```

### 6.2 Example: rotating registry credentials

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: cnlab-runtime-registry
type: Opaque
stringData:
  username: "your-github-username"
  token: "ghp_newtoken..."
EOF
```

No restart needed. The change is picked up automatically — the management cluster pushes it out
to the remote cluster's shared config, and each node re-authenticates with the registry the next
time it checks.

Expected log line (management side):

```
INFO  cnlab-runtime credentials changed — syncing to remote cluster
```

Expected log line (remote side, per node):

```
INFO  Runtime registry credentials changed — syncing to node via oras login
INFO  Runtime registry credentials synced to node
```

---

## 7. Logs & Troubleshooting

### 7.1 Viewing/filtering controller logs

```bash
# Follow the controller (same command on both the management cluster
# and every remote cluster — namespace is always
# remote-cluster-provisioner-system)
kubectl logs -n remote-cluster-provisioner-system \
  deployment/remote-cluster-provisioner-controller-manager -f

# Filter for a specific RemoteCluster or NodeProvision by name
kubectl logs -n remote-cluster-provisioner-system \
  deployment/remote-cluster-provisioner-controller-manager | grep 'ml-cluster-cp'

# Filter for failures only
kubectl logs -n remote-cluster-provisioner-system \
  deployment/remote-cluster-provisioner-controller-manager | grep -i error
```

By default, logs print in a human-readable console format (timestamp, level, a short logger name,
the message, then any extra details as key/value pairs) rather than JSON — that's fine to read
directly, or to pipe through `grep`/`jq`-style tools if your log format needs adjusting for your
log pipeline.

### 7.2 Representative example logs

> Labeled **representative** — these are typical messages you'll see, but the exact surrounding
> details (cluster name, attempt numbers, timestamps) will vary per run.

**Normal cluster provisioning:**

```
INFO  Starting provisioning node for cluster            cluster=ml-cluster-cp clusterName=ml-cluster nodeType=control-plane
INFO  Control plane init goroutine started               startPhase=0
INFO  Control plane init in progress, requeueing
INFO  Control plane init completed                        joinCommand=true
INFO  Cluster fully ready                                  nextTokenRefreshIn=23h0m0s
```

**Join-token refresh (automatic, roughly every 23 hours):**

```
INFO  Kubeadm bootstrap token due for refresh
INFO  Refreshed kubeadm bootstrap token
INFO  Kubeadm bootstrap token still valid                 refreshedAt=... nextRefreshIn=21h0m0s
```

**Retry / terminal failure:**

```
ERROR RemoteCluster provisioning failed — will retry       attempt=2 maxRetries=5
ERROR RemoteCluster provisioning reached retry limit — no further retries   attempts=5 maxRetries=5
```

**Registry credential sync:**

```
INFO  cnlab-runtime credentials changed — syncing to remote cluster
ERROR cnlab-runtime credential sync reached retry limit — no further retries
```

**Normal on-prem node provisioning:**

```
INFO  Request received, starting provisioning
INFO  Validation successful
INFO  On-prem bootstrap goroutine started
INFO  On-prem bootstrap completed
INFO  On-prem node provisioned, waiting for cluster join
INFO  Node not yet visible in cluster — waiting for kubelet to register
INFO  Node registered with control plane
INFO  Stamped ownership metadata on node
INFO  Node reached Ready state
```

**AWS node provisioning:**

```
INFO  AWS validation successful
INFO  Resolved instance type from nodeLabel
INFO  Resolved AMI
INFO  Resolved network config
INFO  Resolved EC2 key pair
INFO  Creating EC2 instance
INFO  EC2 instance created
INFO  EC2 instance created, waiting for it to become running
INFO  EC2 instance running
INFO  Executing cloud-init bootstrap
```

**Stall / retry:**

```
INFO  NodeProvision stalled without an InstanceID, failing for retry
INFO  On-prem bootstrap stalled with no progress, failing for retry
INFO  Node registration stalled, failing for retry
INFO  NodeProvision in terminal Failed state — retry limit reached, manual intervention required
INFO  Retrying NodeProvision after failure — releasing stale VPN IP and resetting phase
```

**Deletion:**

```
INFO  Deprovisioning RemoteCluster
INFO  Resetting node via SSH
INFO  Node reset complete
INFO  Removed WireGuard peer from VPN server
INFO  RemoteCluster cleanup complete
```

### 7.3 Common errors & how to resolve them

| Symptom / log message | Likely cause | Resolution |
|---|---|---|
| `SSHConnectionFailed` in `status.message` | Host unreachable, wrong credentials, or the VPN isn't up yet | Verify `spec.host`/`spec.vpnConfig.ip` is reachable; check the Secret's key name matches what you referenced |
| `RemoteCluster provisioning reached retry limit — no further retries` | 5 consecutive failures | Fix the underlying issue (see `status.message`), then reset: `kubectl patch remotecluster <name> --subresource=status --type=merge -p '{"status":{"provisionRetryCount":0}}'` |
| `NodeProvision in terminal Failed state` | 5 consecutive failures on the remote side | `kubectl patch nodeprovision <name> --subresource=status --type=merge -p '{"status":{"provisionRetryCount":0}}'` |
| `cnlab-runtime credential sync reached retry limit` | VPN link between clusters may be down, or the credentials Secret is malformed | Check VPN connectivity; reset via `kubectl patch remotecluster <name> --subresource=status --type=merge -p '{"status":{"cnlabSyncRetryCount":0}}'` |
| Node stuck in `Bootstrapping` | Install is legitimately still running (5–15 min is normal), or it's genuinely hung | `kubectl get nodeprovision <name> -o jsonpath='{.status.message}'`; SSH in and check `sudo fuser /var/lib/dpkg/lock-frontend` and `tail -50 /var/log/node-provision.log` |
| Registry pull returns `unauthorized` | Registry credentials missing/empty in the cluster's shared config | Apply the registry credentials Secret (or re-apply the sample, which includes it) |
| Networking plugins missing after cluster install | An install step was interrupted | Manually install the CNI plugins bundle (`v1.5.1`) to `/opt/cni/bin` on the affected node |
| GitOps platform resources not appearing / stuck | The platform hasn't synced the new cluster's repository yet, or a stale resource exists from a previous attempt | Check your Nephio/Porch resources; delete stale ones to force re-creation |
| Login page works after cluster is Ready, but auth fails | The identity provider's Service selector doesn't match its pod labels | Patch the Service's selector to match (see your platform's docs) |
| A `NodeProvision` never advances past `RegisteringNode` | Node isn't visible yet — kubelet failed to register, or its VPN IP doesn't match any Node's address | SSH into the node, check the Kubernetes agent status, and check the VPN peer list on both the node and VPN server |
| `VPNModeMismatch` condition on a `RemoteCluster` worker (or a `NodeProvision` failed with "spec.disableVPN is set but ... runs with the VPN") | The node's VPN mode differs from its cluster's | See [§7.5](#75-operational-notes): make the worker's `disableVPN` match the control-plane (usually just unset it) |
| `VPNModeChangeIgnored` condition | `spec.disableVPN` was edited after the node was provisioned | Nothing breaks; the provisioned mode is kept. Revert the edit, or delete and re-create the node to change its mode |
| `WorkerFinalizeFailed` condition on a worker `RemoteCluster` | The worker joined, but recording its Ready status / IP on the control-plane failed (e.g. SSH to the control-plane) | The controller retries automatically; fix SSH reachability to the control-plane (see `status.message`) and wait — no manual reset is needed |
| SSH fails with a host key error, or `cannot load SSH_KNOWN_HOSTS_FILE` | `SSH_KNOWN_HOSTS_FILE` is set and the host is not listed / the file is unreadable | Add the host's key (of the type it offers) to the file, or fix the mount/path; see [§3.3](#33-permissions) |
| `NodeProvision` failed with "namespace ... has N NodeProvisionNetConfigs ... does not set spec.clusterName" (or "N NodeProvisionNetConfigs ... have spec.clusterName ...") | Several clusters share a namespace and the node does not say which one it joins (or two NetConfigs claim the same `clusterName`) | Set `spec.clusterName` on the `NodeProvision` to the cluster's `clusterName`; make each `NodeProvisionNetConfig.spec.clusterName` unique ([§5.2](#52-nodeprovisionnetconfig--shared-cluster-settings)). The retry budget is not consumed |
| `no NodeProvisionNetConfig with spec.clusterName "x"` in the controller log | The `NodeProvision`'s `spec.clusterName` matches no NetConfig (typo, or the cluster's NetConfig is not synced yet) | Fix the name, or wait for the `RemoteCluster` sync; the node requeues |
| `InvalidInsecureRegistries` / `insecureRegistries[i]: ... is not a valid registry host[:port]` | An `insecureRegistries` entry has a scheme, path, spaces, quotes, or a bad port | Use bare `host` or `host:port` ([§5.2](#52-nodeprovisionnetconfig--shared-cluster-settings)) |
| Pull fails with `server gave HTTP response to HTTPS client` on a node | The registry is plain HTTP but is not in `insecureRegistries` / `imagePrepulls`, or the node was provisioned before it was added, or CRI-O was not restarted | Add it to `insecureRegistries`; on existing nodes apply the one-time drop-in + `systemctl restart crio` from [§5.2](#52-nodeprovisionnetconfig--shared-cluster-settings) |
| `not yet implemented` error for an Azure `provider` | Only `AWS`, `GCP` and `OnPrem` are supported today | Use one of those |
| GCP: `permission denied` / `GCP quota exceeded` / `no capacity` in `status.message` | The service account lacks a role or the Compute Engine API is off; the project quota for the machine type/GPU is used up; or the zone has no capacity | Grant the roles in [§5.4](#54-nodeprovision-on-google-cloud-gcp), enable the API, raise the quota, or set another `gcpConfig.zone` |
| GCP: `instance ... belongs to something else` / name already in use | An instance with the NodeProvision's name exists in the zone but was not created for it | Rename the NodeProvision or delete the stray instance; the controller never adopts or deletes it |
| GCP node stuck in `Bootstrapping`, or `Joining` never completes | The startup script failed, or (no VPN) the control plane cannot route to the VM's internal IP | `gcloud compute instances get-serial-port-output <name> --zone <zone>` and `sudo journalctl -u google-startup-scripts`; the log is `/var/log/node-bootstrap.log` |

### 7.4 Debugging / diagnostic commands

```bash
# Full status block for a RemoteCluster or NodeProvision
kubectl get remotecluster <name> -o yaml
kubectl get nodeprovision <name> -o yaml

# Full condition history (this list keeps every entry, not just the latest)
kubectl get remotecluster <name> -o jsonpath='{.status.conditions}' | jq

# Check what a stuck on-prem NodeProvision is doing
kubectl get nodeprovision <name> -o jsonpath='{.status.progress}{"\n"}{.status.message}'

# Inspect the image pre-pull job (GPU nodes only)
kubectl get jobs -l job-name=<nodeprovision-name>-prepull
kubectl logs job/<nodeprovision-name>-prepull

# On the remote control-plane host directly (bypasses the controller entirely)
ssh ubuntu@<cp-ip> 'sudo tail -50 /var/log/node-provision.log'
ssh ubuntu@<cp-ip> 'sudo crictl info'
ssh ubuntu@<cp-ip> 'wg show wg0'
```

### 7.5 Operational notes

**VPN mode conditions (`RemoteCluster.status.conditions`)**

- **`VPNModeMismatch`** — a worker's VPN mode disagrees with its control-plane's (the whole cluster
  must use one mode). An *unprovisioned* worker that sets `disableVPN: true` under a VPN
  control-plane is failed **without consuming a retry**; edit the worker to remove `disableVPN`
  (or change the control-plane) and it proceeds. A worker that is *already provisioned* is left
  untouched and re-checked every minute; re-provision the worker or the control-plane to make
  them agree. A `NodeProvision` in the same situation is set to `Failed` with the explanation in
  `status.message` (this one does count as a failure; reset `provisionRetryCount` after fixing the
  spec, see [§7.3](#73-common-errors--how-to-resolve-them)).
- **`VPNModeChangeIgnored`** — `spec.disableVPN` was changed on a node that is already
  provisioned. The mode the node was provisioned with (annotation
  `infra.dcn.ssu.ac.kr/provisioned-vpn-mode`, `vpn` or `novpn`) is authoritative and is what
  teardown uses; the edit is ignored. The condition clears when the spec matches again.
- **`WorkerFinalizeFailed`** — the worker has joined, but the follow-up bookkeeping (Ready status
  and its VPN/node IP entry on the control-plane) could not be completed. It is retried
  automatically and the pending IP is remembered in an annotation, so nothing is lost; resolve
  whatever the message says (typically SSH access to the control-plane).

**SSH host keys** — opt-in via `SSH_KNOWN_HOSTS_FILE`, fails closed for unlisted hosts; see
[§3.3](#33-permissions).

**AWS nodes**

- Instances are launched with **IMDSv2 required** (`HttpTokens: required`); IMDSv1 requests are
  rejected. The bootstrap script already uses IMDSv2 tokens; any other tooling on the node must too.
- The default IMDS hop limit (1) is kept, so the metadata service is **unreachable from containers/pods**
  on those nodes; workloads that need instance credentials must use another mechanism.
- Every instance (and its root volume) is tagged **`ml.dcn.ssu.ac.kr/nodeprovision-uid`** with the
  owning `NodeProvision`'s UID. The controller uses the tag to adopt or clean up an instance whose
  ID never reached `status` (for example a crash right after launch); do not remove or edit it.
  Launches are idempotent within one provisioning attempt (the EC2 client token combines the UID
  with `status.provisionRetryCount`), so a repeated launch call does not create a second instance;
  each retry after a failure terminates the old instance first and launches a fresh one.

**Failure, retry and deletion behaviour**

- **`NodeProvision` retries.** Before each retry of a failed AWS node the controller removes the
  joined Node, terminates the instance and releases its VPN peer, then clears the identity fields
  in `status`; if termination fails it waits and retries rather than resetting. An on-prem node that
  already registered in the cluster is left alone and provisioning resumes at the join step.
- **Terminal `Failed`.** When `provisionRetryCount` reaches the limit, AWS resources are released once
  (instance, Node, VPN peer; the status message gains `[resources released]`) so nothing keeps
  billing. Patching `status.provisionRetryCount` to 0 starts a fresh attempt. An already-registered
  on-prem node and its peer are deliberately not touched.
- **`NodeProvision` deletion.** The on-prem node reset runs once (annotation
  `ml.dcn.ssu.ac.kr/node-reset-done`). A failing VPN peer cleanup keeps the finalizer and is retried
  every 30 s; after 10 minutes it gives up loudly so an unreachable VPN server cannot block deletion.
- **`RemoteCluster` deletion.** Node drain/reset run once (annotation
  `infra.dcn.ssu.ac.kr/delete-node-cleanup-done`). Only deleting the **control-plane** removes the
  cluster's Porch repositories, Nephio tokens and PackageVariants; deleting a worker never does.
- **PackageVariants with several clusters.** Existing names (for example `harbor-variant`) are kept for
  the cluster that owns them. A second control-plane with a different `spec.clusterName` gets
  `<name>-<clusterName>` variants in the same namespace instead of failing; two control-planes with
  the *same* `clusterName` still share their objects.
- **Bootstrap token.** The control-plane reschedules itself to refresh the kubeadm bootstrap token
  before it expires; a token that is overdue is retried after one minute.
- **Redaction.** Secrets are redacted from `status.message` and logs (tokens, private keys, and values
  after `password`/`secret`/`token`/`key` style keys). Because the rule is deliberately broad, a
  diagnostic such as `secret: <word>` may lose the word that follows it.

**Controller image** — the published image is **no longer obfuscated**. Obfuscation (garble)
is opt-in at build time with `--build-arg OBFUSCATE=true` (or the `OBFUSCATE` repository variable
for the publish workflow) and makes crash traces much harder to read.

---

## 8. Controller Lifecycle / Reconciliation

### 8.1 `RemoteCluster` create/update/delete

**Create:** the resource moves to `Provisioning`. For a **control-plane** node, Kubernetes,
container runtime, networking, and (if enabled) the GitOps agent are installed — this typically
takes 5–15 minutes and can be watched via `status.phase`/`status.message`. For a **worker**, it's
joined to the sibling `RemoteCluster` with the same `clusterName` — this requires that
control-plane to already be `Ready`. On success, the cluster's shared configuration
(`NodeProvisionNetConfig`) is created/updated, and the resource moves to `Ready`. For a
control-plane, this is followed by deploying the GitOps platform stack in two waves.

**Update:** a `Ready` cluster is periodically re-checked to keep its join token fresh (roughly
every 23 hours) and to catch any configuration drift (VPN settings, software versions, registry
credentials) between the `RemoteCluster` resource and the cluster's actual shared config. If a
provisioning attempt is interrupted partway (e.g. a controller restart), the next attempt resumes
from where it left off rather than starting the entire install over.

**Delete:** deleting a `RemoteCluster` cleanly tears the node back down — it's drained and removed
from the cluster (workers only), reset back to a bare host (Kubernetes, container runtime, and
networking uninstalled), and removed from the VPN. All of this cleanup is best-effort: an
unreachable or already-gone node never blocks the resource from being deleted.

### 8.2 `NodeProvision` create/update/delete

**Create:** the resource works through the phases shown in [§2.3](#23-nodeprovision-what-happens-phase-by-phase).
For AWS, unset fields (instance type, AMI, networking, key pair) are automatically resolved for
you. Once the node is visible in the cluster, it's labeled so GPU/CPU-specific workloads schedule
onto it correctly.

**Update:** a `Ready` node is periodically checked for registry-credential drift and
re-authenticates automatically if credentials changed. A `Failed` node below the retry limit is
automatically reset for a clean retry — you don't need to delete and recreate it.

**Delete:** the node is removed from the cluster, its cloud instance is terminated (AWS) or it's
reset back to a bare host (on-prem), and it's removed from the VPN.

### 8.3 Retries, failures, and recovery

Every provisioning failure is counted. After **5 consecutive failures**, the resource is left in a
terminal `Failed` state and stops retrying automatically — this is intentional, since repeated
failures against the same host usually mean something needs a human to look at (wrong
credentials, host down, disk full, etc.). Once you've fixed the underlying issue, reset the retry
counter to resume — the exact `kubectl patch` command for each resource type is in
[§7.3](#73-common-errors--how-to-resolve-them).

`RemoteCluster.status.conditions` keeps a full history, not just the latest state — useful for
seeing exactly what happened across the resource's lifetime, not only its current status.

---

## 9. Operations

### 9.1 Health checks

```bash
kubectl get pods -n remote-cluster-provisioner-system
kubectl describe pod -n remote-cluster-provisioner-system \
  -l control-plane=controller-manager
```

The liveness/readiness probes only confirm the controller process itself is alive — not that any
particular SSH/AWS/VPN operation is succeeding. For that, check the resource's own
`status`/`conditions` and logs (see [§7](#7-logs--troubleshooting)).

### 9.2 Restart

```bash
kubectl rollout restart deployment/remote-cluster-provisioner-controller-manager \
  -n remote-cluster-provisioner-system
```

In-flight provisioning work resumes from where it left off after a restart rather than starting
over — see [§8.1](#81-remotecluster-createupdatedelete)/[§8.2](#82-nodeprovision-createupdatedelete).

### 9.3 Upgrade

```bash
make deploy IMG=<registry>/remote-cluster-provisioner:<new-tag>
```

Point `IMG` at whichever new pre-built image you've been given. If the new version adds new CRD
fields, re-run `make install` first so the CRDs are up to date before the new controller version
starts.

### 9.4 Uninstall

```bash
# Remove the controller and its permissions/namespace
make undeploy

# Remove the CRDs (only after — see the warning below)
make uninstall
```

> **Before removing anything**, delete any live `RemoteCluster`/`NodeProvision` resources first
> (`kubectl delete remotecluster --all`, `kubectl delete nodeprovision --all`) and wait for them to
> finish deleting, so nodes get reset and removed from the VPN properly. Removing the CRDs while
> resources still exist skips that cleanup — you'd be left with nodes still joined to the cluster
> and still registered on the VPN.

### 9.5 Inspecting status across your fleet

```bash
# All RemoteClusters and their current phase
kubectl get remotecluster -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,CLUSTER:.spec.clusterName

# All NodeProvisions on a remote cluster
kubectl get nodeprovision -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,PROVIDER:.spec.provider,IP:.status.ipAddress
```

### 9.6 Monitoring

Day-to-day observability is status/condition- and log-driven (see [§7](#7-logs--troubleshooting)
and [§9.5](#95-inspecting-status-across-your-fleet)) rather than dashboard-driven — there isn't a
bundled dashboard today. If your platform team wants Prometheus metrics, ask them to enable the
metrics endpoint on the controller Deployment.

---

## 10. Reference

### 10.1 `RemoteCluster`

**Spec:**

| Field | Type | Notes |
|---|---|---|
| `clusterName` | string | Shared across every node (control-plane + workers) of one logical cluster |
| `host` | string | LAN/public IP or hostname of the target node |
| `port` | string | SSH port (e.g. `"22"`) |
| `user` | string | SSH username |
| `nodeInfo.nodeType` | string | `control-plane` \| `worker` |
| `nodeInfo.hardwareType` | string | `cpu` \| `gpu` |
| `nodeInfo.softwareConfig.kubernetesVersion` | string | e.g. `v1.34.2` |
| `nodeInfo.softwareConfig.imagePrepulls[]` | `{image, nodeTarget}` | `nodeTarget` is `gpu` or `all` (default `all`) |
| `nodeInfo.softwareConfig.imagePullSecretRef` | secret ref | optional |
| `nodeInfo.softwareConfig.insecureRegistries[]` | string | optional; registries (`host[:port]`) reached over plain HTTP / untrusted TLS. `imagePrepulls` hosts are also marked insecure (see [§5.2](#52-nodeprovisionnetconfig--shared-cluster-settings)) |
| `nodeInfo.softwareConfig.cnlabRuntime` | object | optional; sensible defaults if omitted |
| `nodeInfo.softwareConfig.platformVariables[]` | `{key, value}` | optional; passed through to the GitOps platform stack |
| `auth.sshPrivateKeySecretRef` / `auth.passwordSecretRef` | secret ref | exactly one; auto-detects key vs. password auth |
| `vpnConfig.ip` | string | this node's VPN IP; preferred over `host` once set |
| `vpnConfig.vpnServerPublicIP` / `vpnServerSSHPort` / `vpnServerSSHUsername` | string | VPN server SSH connection details |
| `vpnConfig.vpnSshCredentialsRef` | secret ref | Secret with the VPN server's SSH credential |
| `disableVPN` | bool | default `false`; run the whole cluster without WireGuard (see [§5.1](#51-remotecluster--createmanage-a-cluster-from-the-management-cluster)); set it on the control-plane, workers inherit it. `vpnConfig` is ignored when `true` |
| `gitConfig.enable` | string | `"true"`/`"false"` — gates GitOps platform deployment |
| `gitConfig.gitServer` / `gitUsername` / `upstreamPlatformRepo` / `packageRevision` | string | GitOps source details |

**Status:**

| Field | Meaning |
|---|---|
| `phase` | `Provisioning` → `Ready` → `Failed` |
| `message` | Human-readable status, includes retry count on failure |
| `conditions[]` | Full history of status changes, most recent last |
| `joinCommand` | Cached join command, used when adding workers |
| `provisionRetryCount` | Consecutive provisioning failures; resets to 0 on `Ready` |
| `cnlabSyncRetryCount` | Consecutive registry-credential sync failures |

### 10.2 `NodeProvision`

**Spec:**

| Field | Type | Notes |
|---|---|---|
| `provider` | string | `OnPrem` \| `AWS` \| `GCP` (`Azure` is not implemented yet) |
| `clusterName` | string | which `NodeProvisionNetConfig` (`spec.clusterName` equal) the node uses. Optional only while the namespace has exactly one NetConfig; required (no guessing) when it has several |
| `role` | string | always `worker` in practice |
| `hardwareType` | string | `gpu`/`cpu` (or empty) — filters which images get pre-pulled |
| `nodeLabel` | string | drives AWS default instance-type resolution (`cpu`→`t3.xlarge`, `gpu`→`p3.2xlarge`) |
| `region` | string | AWS region, or GCP region (optional when `gcpConfig.zone` is set) |
| `instanceType` | string | overrides the `nodeLabel` lookup; the GCE machine type for GCP |
| `hostname` / `ipAddress` | string | on-prem target; `ipAddress` takes priority if both set |
| `sshPort` | int | default `22` |
| `sshUsernameOverride` | string | |
| `credentialsRef` | secret ref | key auto-detected if omitted |
| `disableVPN` | bool | default `false`; join the node without WireGuard. Normally **inherited** from the cluster (`RemoteCluster` control-plane → `NodeProvisionNetConfig` → `NodeProvision`); setting `true` against a VPN cluster fails the provision. Set at creation; not changeable afterwards (see [§5.3](#53-nodeprovision--add-a-node-from-the-remote-cluster), [§7.5](#75-operational-notes)) |
| `awsConfig` | object | `vpcId`, `subnetId`, `securityGroupIds[]`, `ami`, `keyPairName`, `iamInstanceProfile`, `tags{}`, `rootVolumeSizeGB` — all auto-resolved if omitted |
| `gcpConfig` | object | `projectId`, `zone`, `network`, `subnetwork`, `sourceImage` / `imageFamily` / `imageProject`, `bootDiskSizeGB`, `bootDiskType`, `labels{}`, `networkTags[]`, `serviceAccountEmail`, `serviceAccountScopes[]`, `accelerator{type,count}`, `spot`, `disableExternalIP`, `firewallSourceRanges[]` — all optional, auto-resolved if omitted (see [§5.4](#54-nodeprovision-on-google-cloud-gcp)) |

**Status:**

| Field | Meaning |
|---|---|
| `phase` | `Pending`, `Validating`, `Provisioning`, `CreatingInstance` and `WaitingForInstance` (AWS, GCP), `ConfiguringVPN` (skipped when the VPN is disabled), `Bootstrapping`, `Joining`, `RegisteringNode`, `VerifyingHealth`, `PrePullingImages`, `Ready`, `Failed`, `Deleting`; the order differs by provider — see [§2.3](#23-nodeprovision-what-happens-phase-by-phase) |
| `message` | Human-readable status |
| `instanceId` / `hostname` / `ipAddress` | Identity (`instanceId` is the instance name on GCP) |
| `publicIp` / `privateIp` | AWS and GCP (the instance's external / internal address) |
| `vpnIp` | VPN IP allocated for this node |
| `progress` | 0–100 |
| `nodeName` | Kubernetes node name once registered |
| `runtimeCredentialsHash` | Fingerprint of last-synced registry credentials |
| `provisionRetryCount` | Consecutive failures; resets to 0 on `Ready` |

### 10.3 `NodeProvisionNetConfig`

**Spec:**

| Field | Type | Notes |
|---|---|---|
| `clusterName` | string | the cluster this config belongs to; selected by `NodeProvision.spec.clusterName`. Must be unique among the NetConfigs of a namespace when several clusters share it |
| `disableVPN` | bool | default `false`; marks the whole cluster as running without WireGuard. Published by the control-plane `RemoteCluster`; inherited by every `NodeProvision`; `vpnRange`/`vpnServerPublicConfig` are then unused |
| `vpnRange` | string | CIDR, e.g. `10.9.0.0/24` (only when `disableVPN` is `false`) |
| `vpnServerPublicConfig.publicIP` | string | only used when `disableVPN` is `false` |
| `vpnServerPublicConfig.sshPort` / `sshUsername` | string | defaults `22` / `ubuntu` |
| `vpnServerPublicConfig.vpnPort` | string | default `51820` |
| `vpnServerPublicConfig.vpnSshCredentialsRef` | secret ref | |
| `softwareConfig.kubernetesVersion` | string | |
| `softwareConfig.imagePrepulls[]` | `{image, nodeTarget}` | |
| `softwareConfig.imagePullSecretRef` | secret ref | |
| `softwareConfig.insecureRegistries[]` | string | registries (`host[:port]`) served over plain HTTP / untrusted TLS; merged with the `imagePrepulls` hosts (see [§5.2](#52-nodeprovisionnetconfig--shared-cluster-settings)) |
| `softwareConfig.cnlabRuntime` | object | same shape/defaults as `RemoteCluster`'s |

**Status:**

| Field | Meaning |
|---|---|
| `usedIPAddresses[]` | VPN IPs already allocated in this cluster |
| `clusterJoinCommand` | Join command for workers |
| `joinTokenRefreshedAt` | Last token refresh timestamp |
| `vpnPeers[]` | `{nodeName, publicKey, vpnIP}` |
| `kubeconfig` | Base64 admin kubeconfig for this cluster |

> **Not fields of this resource** (older examples showed them; the API server rejects them):
> `softwareConfig.nvidiaDriverVersion`, `softwareConfig.nvidiaContainerToolkitVersion`,
> `softwareConfig.k8sDevicePluginVersion`.

### 10.4 Common `kubectl` commands

| Command | Effect |
|---|---|
| `kubectl get remotecluster -w` | Watch a cluster's provisioning progress |
| `kubectl get nodeprovision -w` | Watch a node's provisioning progress |
| `kubectl patch remotecluster <name> --subresource=status --type=merge -p '{"status":{"provisionRetryCount":0}}'` | Reset a stuck `RemoteCluster` after fixing the underlying issue |
| `kubectl patch remotecluster <name> --subresource=status --type=merge -p '{"status":{"cnlabSyncRetryCount":0}}'` | Reset a stuck registry-credential sync |
| `kubectl patch nodeprovision <name> --subresource=status --type=merge -p '{"status":{"provisionRetryCount":0}}'` | Reset a stuck `NodeProvision` |
| `kubectl delete remotecluster <name>` | Tear a node/cluster back down cleanly |
| `kubectl delete nodeprovision <name>` | Remove a node cleanly |

### 10.5 Install/upgrade commands

| Command | Effect |
|---|---|
| `make install` / `make uninstall` | Apply/remove the CRDs |
| `make deploy IMG=<image>` / `make undeploy` | Apply/remove the controller (pointing at an already-built image) |

### 10.6 Where to find example manifests

| Path | Contents |
|---|---|
| `config/samples/` | Example `RemoteCluster`/`NodeProvision`/`NodeProvisionNetConfig` resources — see [§6](#6-examples) and the table below |
| `config/samples/infra_v1_remotecluster_vpn.yaml` | Complete cluster **with** WireGuard: CPU control-plane, GPU worker, CPU worker, their Secrets, `insecureRegistries`, `imagePrepulls`, `cnlabRuntime` |
| `config/samples/infra_v1_remotecluster_novpn.yaml` | Complete cluster **without** a VPN (`disableVPN: true`): a GPU control-plane that also runs GPU workloads, a GPU worker and a CPU worker |
| `config/samples/infra_v1_remotecluster.yaml` | Minimal VPN-less control-plane (the quick-start sample) |
| `config/samples/infra_v1_remotecluster_cnlab_runtime.yaml` | Fully annotated control-plane + worker with `platformVariables` and GitOps (contains example credentials — replace them) |
| `config/samples/ml_v1alpha1_nodeprovision*.yaml` | `NodeProvision` samples for on-prem, AWS and GCP (set `spec.clusterName` when a namespace holds more than one cluster) |
| `docs/wireguard-setup-bundle/WIREGUARD_SETUP.md` | WireGuard VPN server/client setup guide |
| `README.md` | Top-level project overview |

### 10.7 Useful links

- Project README: [`../README.md`](../README.md)
- WireGuard setup guide: [`wireguard-setup-bundle/WIREGUARD_SETUP.md`](wireguard-setup-bundle/WIREGUARD_SETUP.md)
- Nephio/Porch documentation (for GitOps-based platform deployment): https://docs.nephio.org/

---

*This guide covers day-to-day use of the controllers — creating and managing clusters and nodes.
It intentionally leaves out anything about building the controller image or its source code.*
