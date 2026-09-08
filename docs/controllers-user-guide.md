# remote-cluster-provisioner — Controllers User Guide

> Scope: this guide documents the Kubernetes controllers implemented in this repository —
> `RemoteClusterReconciler`, `NodeProvisionReconciler`, and `NodeProvisionNetConfigReconciler` —
> from an operator/user perspective. Everything here was verified against the source in
> `internal/controller/`, `api/`, `config/`, `pkg/`, `cmd/main.go`, the `Makefile`, and the
> `Dockerfile` as of the current `harbor` branch. Where the repository's own sample files or
> README text disagree with the Go source, this guide follows the Go source and calls out the
> discrepancy explicitly.

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

`remote-cluster-provisioner` is a single Go binary (`cmd/main.go`, built via `controller-runtime`'s
`manager`) that registers **three** controllers against **three** Custom Resource Definitions
(CRDs), spread across two API groups:

| Controller | Reconciles | API group/version | Runs on |
|---|---|---|---|
| `RemoteClusterReconciler` | `RemoteCluster` | `infra.dcn.ssu.ac.kr/v1` | **Management cluster** |
| `NodeProvisionReconciler` | `NodeProvision` | `ml.dcn.ssu.ac.kr/v1alpha1` | **Remote (workload) cluster** |
| `NodeProvisionNetConfigReconciler` | `NodeProvisionNetConfig` | `ml.dcn.ssu.ac.kr/v1alpha1` | **Remote (workload) cluster** |

The same binary/image is deployed on both the management cluster and every remote cluster it
provisions — which controller "does something" is purely a function of which CRs exist on that
particular cluster, since all three controllers are always registered with the manager
(`cmd/main.go:185-212`).

> **Important accuracy note:** `NodeProvisionNetConfigReconciler.Reconcile()` is **unmodified
> kubebuilder scaffolding** — it does nothing but return `ctrl.Result{}, nil`
> (`internal/controller/ml/nodeprovisionnetconfig_controller.go:49-55`). `NodeProvisionNetConfig`
> is a real, watched CRD, but all of its actual behavior (bootstrap-token refresh, VPN peer
> bookkeeping, credential-secret propagation) happens as a *side effect* of the other two
> controllers reading and status-patching it — it has no independent reconcile loop of its own
> today. Treat it as a **shared config/state object**, not an active controller.

### 1.2 What each controller does

**`RemoteClusterReconciler`** (management cluster) — given SSH access and a `RemoteCluster` CR,
it turns a bare Ubuntu 22.04 host into either:
- a fully initialized Kubernetes **control-plane** node (kubeadm init, CNI, CRI-O, ArgoCD, and a
  Nephio/Porch `PackageVariant` platform stack), or
- a **worker** node joined to an already-initialized control-plane in the same `clusterName`.

It also keeps the resulting remote cluster's `NodeProvisionNetConfig` in sync (VPN range, VPN
server details, software config, `cnlab-runtime` registry credentials), refreshes the kubeadm
bootstrap token every 23 hours, and tears everything back down (kubeadm reset, WireGuard peer
removal, Porch object cleanup) when a `RemoteCluster` is deleted.

**`NodeProvisionReconciler`** (remote cluster) — given a `NodeProvision` CR, it provisions
**additional worker nodes** for a cluster that a `RemoteClusterReconciler` has already bootstrapped,
via one of two providers:
- **`OnPrem`** — SSH into an existing bare-metal/VM host and run the same kubeadm-join pipeline.
- **`AWS`** — launch an EC2 instance (auto-resolving AMI/instance type/VPC/subnet/security group
  when not specified) and bootstrap it via cloud-init.

It is described in the repository's own README as **fully autonomous**: it refreshes kubeadm
bootstrap tokens directly against the local Kubernetes API (no SSH to a control plane needed) and
can keep provisioning new nodes even while disconnected from the management cluster.

**`NodeProvisionNetConfigReconciler`** — registered and watching, but a no-op today (see the
callout above).

### 1.3 How the controllers interact with the rest of the system

```
Management cluster                          Remote (workload) cluster
┌─────────────────────────┐   SSH (kubeadm   ┌──────────────────────────┐
│ RemoteClusterReconciler │──init/join, wg,──▶│ (bootstrapped by SSH,    │
│  watches RemoteCluster  │   oras login)     │  not a controller)      │
└───────────┬─────────────┘                   └──────────┬───────────────┘
            │ creates/patches over SSH                    │
            ▼                                             ▼
   NodeProvisionNetConfig (remote)  <───────────  NodeProvisionReconciler
   (join cmd, VPN range, cnlab-runtime creds)      watches NodeProvision,
                                                    reads NodeProvisionNetConfig
```

`RemoteClusterReconciler` is the only controller that talks to Nephio/Porch (`PackageVariant`,
`Repository`, `Token` objects) and the only one that performs the *initial* kubeadm **init**.
`NodeProvisionReconciler` never runs `kubeadm init` — it only ever joins nodes to a cluster whose
join command is already recorded in a `NodeProvisionNetConfig`.

### 1.4 When/how users should use each controller

| Situation | Use |
|---|---|
| Stand up a brand-new remote cluster (control plane) from bare metal | `RemoteCluster` CR, `nodeInfo.nodeType: control-plane`, on the **management cluster** |
| Add a worker to that cluster during initial buildout, from the management cluster's inventory | `RemoteCluster` CR, `nodeInfo.nodeType: worker`, on the **management cluster** |
| Scale out a cluster **after** it is Ready, using on-prem/bare-metal capacity the remote cluster itself has access to | `NodeProvision` CR, `provider: OnPrem`, applied **on the remote cluster** |
| Scale out a cluster with cloud burst capacity | `NodeProvision` CR, `provider: AWS`, applied **on the remote cluster** |
| Rotate `cnlab-runtime` registry credentials, change the VPN range, or change `kubernetesVersion` for future nodes | Edit `NodeProvisionNetConfig.spec` directly (or edit the parent `RemoteCluster`, which resyncs it) |

---

## 2. Architecture & Diagrams

### 2.1 Overall controller architecture

```mermaid
flowchart TB
    subgraph MGMT["Management Cluster"]
        direction TB
        RCR["RemoteClusterReconciler\n(watches RemoteCluster)"]
        PORCH["Nephio / Porch\nRepository · Token · PackageVariant"]
        RCR -- "4 creates/updates" --> PORCH
    end

    subgraph REMOTE["Remote / Workload Cluster (one per clusterName)"]
        direction TB
        NPNC["NodeProvisionNetConfig\n(passive config + status object)"]
        NPR["NodeProvisionReconciler\n(watches NodeProvision)"]
        NPNCR["NodeProvisionNetConfigReconciler\n(registered, no-op today)"]
        K8SAPI["Local kube-apiserver\n(bootstrap tokens, Node objects)"]
        NPR -- "3 reads join cmd / VPN / creds" --> NPNC
        NPR -- "6 refreshes token, patches Node labels" --> K8SAPI
        NPNCR -. "watches, does nothing" .-> NPNC
    end

    subgraph CPNODE["Control-plane node (bare metal / VM)"]
        KUBEADM_CP["kubeadm control-plane\n+ CRI-O + CNI + ArgoCD"]
    end

    subgraph WNODE["Worker node (bare metal / VM / EC2)"]
        KUBEADM_W["kubelet + CRI-O\n(joined via kubeadm join)"]
    end

    VPN["WireGuard VPN server"]

    RCR -- "1 SSH: kubeadm init, deploy CNI/ArgoCD" --> KUBEADM_CP
    RCR -- "1 SSH: kubeadm join (RemoteCluster worker)" --> KUBEADM_W
    RCR -- "2 SSH: create/patch NodeProvisionNetConfig" --> NPNC
    NPR -- "5 SSH (OnPrem) / cloud-init (AWS): kubeadm join" --> KUBEADM_W
    RCR -- "register/remove peer" --> VPN
    NPR -- "register/remove peer" --> VPN
    KUBEADM_CP -. "wg0 tunnel" .-> VPN
    KUBEADM_W -. "wg0 tunnel" .-> VPN

    classDef ctrl fill:#eeeeee,stroke:#333,stroke-width:1px,color:#000;
    classDef passive fill:#ffffff,stroke:#999,stroke-width:1px,stroke-dasharray:3 3,color:#000;
    class RCR,NPR ctrl;
    class NPNC,NPNCR passive;
```

Numbered flow (mirrors the numbering style used in this repo's other architecture diagrams):

1. `RemoteClusterReconciler` SSHes into the target host to run `kubeadm init` (control-plane) or
   `kubeadm join` (worker), install CRI-O, CNI (Flannel over the `wg0` VPN interface), and — for
   control planes — ArgoCD plus a CRD/CNI/cert-manager baseline.
2. On success, it SSHes into the remote control-plane again to create/patch
   `NodeProvisionNetConfig` with the join command, VPN range, software config, and (if configured)
   `cnlab-runtime` registry credentials.
3. `NodeProvisionReconciler`, running on the remote cluster, reads that `NodeProvisionNetConfig`
   whenever it needs a join command, VPN server address/credentials, or runtime registry
   credentials for a **new** `NodeProvision`.
4. Back on the management cluster, `RemoteClusterReconciler` creates Nephio/Porch `Repository`,
   `Token`, and `PackageVariant` objects that drive GitOps deployment of the platform stack onto
   the new cluster (only if `spec.gitConfig.enable: "true"`).
5. `NodeProvisionReconciler` provisions the new node — over SSH for `OnPrem`, or via EC2 +
   cloud-init for `AWS` — and joins it with the cached join command.
6. Once joined, `NodeProvisionReconciler` refreshes the kubeadm bootstrap token directly against
   the **local** Kubernetes API (no SSH/management-cluster dependency) and labels the new Node for
   DaemonSet/Job scheduling (`infra.dcn.ssu.ac.kr/worker=true`,
   `infra.dcn.ssu.ac.kr/hardware-type=gpu|cpu`).

Both controllers register/deregister WireGuard peers directly on the VPN server over SSH
(`wg set wg0 peer … remove`, plus editing `/etc/wireguard/wg0.conf` so peers don't reappear after a
VPN server restart).

### 2.2 Controller → resource/workload flow

```mermaid
flowchart LR
    subgraph "RemoteCluster (management cluster)"
        A["kubectl apply\nRemoteCluster CR"] --> B["RemoteClusterReconciler.Reconcile"]
        B --> C{"nodeInfo.nodeType"}
        C -- "control-plane" --> D["reconcileControlPlane\n(background goroutine)\nkubeadm.InitializeControlPlane"]
        C -- "worker" --> E["reconcileWorker\nkubeadm.JoinWorkerNode"]
        D --> F["Status.JoinCommand cached"]
        F --> G["completeControlPlane:\ncreateClusterRepo,\nhandleCreateUpdateNodeProvisionConfig,\nphase=Ready"]
        E --> H["handleCreateUpdateNodeProvisionConfig\n(update)"]
        G --> I["reconcilePackageVariants:\ncore PackageVariants,\nthen overlay PackageVariants"]
    end

    subgraph "NodeProvision (remote cluster)"
        J["kubectl apply\nNodeProvision CR"] --> K["NodeProvisionReconciler.Reconcile"]
        K --> L{"spec.provider"}
        L -- "AWS" --> M["reconcileAWSProvisioning\nEC2 + cloud-init"]
        L -- "OnPrem" --> N["reconcileOnPremProvisioning\nSSH (background goroutine)"]
        M --> O["reconcileJoining:\nmatch Node by VPN IP,\nlabel + finalize"]
        N --> O
        O --> P{"hardwareType==gpu &&\nimagePrepulls set?"}
        P -- yes --> Q["PrePullingImages\n(batch Job via crictl)"]
        P -- no --> R["Ready"]
        Q --> R
    end
```

### 2.3 Important components and dependencies

```mermaid
flowchart TB
    subgraph "internal/controller"
        RC["RemoteClusterReconciler\nremotecluster_controller.go"]
    end
    subgraph "internal/controller/ml"
        NP["NodeProvisionReconciler\nnodeprovision_controller.go"]
        NPNC2["NodeProvisionNetConfigReconciler\n(no-op)"]
    end
    subgraph pkg
        KUBEADM["pkg/kubeadm\nInitializeControlPlane, JoinWorkerNode,\nInstallNvidiaContainerToolkit, GenerateCDI"]
        SSHPKG["pkg/ssh\nClient, Connect, ConnectWithPrivateKey, Run"]
        RUNTIME["pkg/runtime\ncnlab-runtime config + ORAS install steps"]
        ARGOCD["pkg/argocd\nConfigureArgoCD"]
    end
    subgraph provider
        AWSPROV["provider/aws\nEC2 provisioning, CredentialManager (STS/MFA)"]
        ONPREM["provider/onprem\nNewInClusterProvisioner (SSH bootstrap)"]
    end
    subgraph "External systems"
        VPNSRV["WireGuard VPN server"]
        GHCR["GHCR (cnlab-runtime OCI artifact)"]
        PORCHSRV["Nephio / Porch API"]
        EC2["AWS EC2 API"]
    end

    RC --> KUBEADM
    RC --> SSHPKG
    RC --> RUNTIME
    RC --> PORCHSRV
    NP --> ONPREM
    NP --> AWSPROV
    NP --> SSHPKG
    NP --> RUNTIME
    KUBEADM --> ARGOCD
    KUBEADM -.-> VPNSRV
    ONPREM -.-> VPNSRV
    AWSPROV --> EC2
    RUNTIME -.-> GHCR
```

### 2.4 Reconciliation / control-flow diagrams

**`RemoteCluster` phase state machine** (`internal/controller/remotecluster_controller.go`, phase
constants `phaseProvisioning="Provisioning"`, `phaseReady="Ready"`, `phaseFailed="Failed"`):

```mermaid
stateDiagram-v2
    [*] --> Provisioning: CR created (finalizer added, Requeue)
    Provisioning --> Provisioning: control-plane init goroutine\nstill running (poll every 30s)
    Provisioning --> Ready: kubeadm init/join succeeded,\ncreateClusterRepo + NodeProvisionNetConfig OK
    Provisioning --> Failed: SSH / kubeadm / config error
    Ready --> Ready: sync NodeProvisionNetConfig,\nrefresh token (every 23h),\nsync cnlab-runtime creds,\ncreate PackageVariants
    Failed --> Provisioning: retry (provisionRetryCount < 5)
    Failed --> [*]: terminal — provisionRetryCount >= 5\n(manual reset required)
    Ready --> [*]: DeletionTimestamp set → handleDelete
    Provisioning --> [*]: DeletionTimestamp set → handleDelete
    Failed --> [*]: DeletionTimestamp set → handleDelete
```

**`NodeProvision` phase state machine** (`api/ml/v1alpha1/nodeprovision_types.go:27-40` — the real
implementation has 14 named phases; the README's "Pending → Provisioning → Bootstrapping →
Joining → Ready → Failed" is a simplification of this):

```mermaid
stateDiagram-v2
    [*] --> Pending
    Pending --> Validating: OnPrem or AWS dispatch
    Validating --> ConfiguringVPN: AWS only
    ConfiguringVPN --> CreatingInstance: AWS only
    CreatingInstance --> WaitingForInstance: InstanceID recorded
    WaitingForInstance --> Bootstrapping: EC2 instance running\n(cloud-init executing)
    Validating --> Bootstrapping: OnPrem\n(background SSH goroutine)
    Bootstrapping --> RegisteringNode: node visible in\nkubectl get nodes
    RegisteringNode --> Joining
    Joining --> VerifyingHealth
    VerifyingHealth --> PrePullingImages: GPU node with\nimagePrepulls configured
    VerifyingHealth --> Ready: otherwise
    PrePullingImages --> Ready
    CreatingInstance --> Failed: stalled >10m
    WaitingForInstance --> Failed: stalled >10m
    Bootstrapping --> Failed: stalled >20m (OnPrem)
    RegisteringNode --> Failed: stalled >15m
    Failed --> Pending: retry (provisionRetryCount < 5,\nreleases stale VPN peer)
    Failed --> [*]: terminal — provisionRetryCount >= 5
    Ready --> Deleting: DeletionTimestamp set
    Deleting --> [*]
```

**End-to-end sequence** for building a two-node cluster and then scaling it with `NodeProvision`:

```mermaid
sequenceDiagram
    participant User
    participant MgmtAPI as Management cluster API
    participant RCR as RemoteClusterReconciler
    participant CP as Control-plane host (SSH)
    participant RemoteAPI as Remote cluster API
    participant NPR as NodeProvisionReconciler
    participant Worker as New worker (SSH/EC2)

    User->>MgmtAPI: apply RemoteCluster (nodeType=control-plane)
    RCR->>CP: SSH: kubeadm init, CRI-O, CNI, ArgoCD
    CP-->>RCR: join command
    RCR->>CP: SSH: create NodeProvisionNetConfig
    RCR->>MgmtAPI: status.phase=Ready
    User->>MgmtAPI: apply RemoteCluster (nodeType=worker)
    RCR->>CP: SSH: read join command (already Ready)
    RCR->>Worker: SSH: kubeadm join
    RCR->>MgmtAPI: status.phase=Ready
    User->>RemoteAPI: apply NodeProvision (provider=OnPrem)
    NPR->>RemoteAPI: read NodeProvisionNetConfig (join cmd, VPN)
    NPR->>Worker: SSH: kubeadm join (background goroutine)
    Worker-->>RemoteAPI: kubelet registers Node
    NPR->>RemoteAPI: label Node, status.phase=Ready
```

---

## 3. Prerequisites

### 3.1 Required software (build/dev)

| Tool | Version pinned in this repo | Where |
|---|---|---|
| Go | `1.24.5` | `go.mod:3` |
| `kustomize` | `v5.6.0` | `Makefile` |
| `controller-gen` | `v0.18.0` | `Makefile` |
| `golangci-lint` | `v2.3.0` | `Makefile` |
| `setup-envtest` (Kubebuilder test binaries) | pinned via `ENVTEST_K8S_VERSION` in `Makefile` | `Makefile`, `internal/controller/suite_test.go` |
| Docker (or compatible) | any recent version | `Dockerfile` (multi-stage build) |

The container image is built `FROM golang:1.26` (builder stage) and
`FROM gcr.io/distroless/static:nonroot` (final stage) — see [§4.5](#45-container-image).

### 3.2 Required cluster software/versions

- **Kubernetes** (both management and remote clusters): the module depends on
  `k8s.io/apimachinery` and `k8s.io/client-go` `v0.33.0` and `sigs.k8s.io/controller-runtime`
  `v0.21.0` — target a matching-ish Kubernetes 1.33+ API for the controller pod itself.
- **Remote/workload cluster Kubernetes version**: driven entirely by
  `spec.nodeInfo.softwareConfig.kubernetesVersion` / `NodeProvisionNetConfig.spec.softwareConfig.kubernetesVersion`
  (e.g. `v1.34.2` in the samples) — this is what gets installed by `kubeadm`/apt on each node, and
  is independent of the management cluster's own Kubernetes version.
- **Nephio/Porch**: required on the management cluster **only if** `RemoteCluster.spec.gitConfig.enable: "true"`
  — `RemoteClusterReconciler` creates `config.porch.kpt.dev/v1alpha1 PackageVariant`,
  `infra.nephio.org Repository`, and `Token` objects that Porch must reconcile.
- **CNI plugins bundle** (`v1.5.1` per the README's troubleshooting section) — installed by the
  controller itself during kubeadm init/join (for the `portmap` plugin needed alongside Flannel);
  see [§7.3](#73-common-errors--how-to-resolve-them) if this step is skipped/interrupted.

### 3.3 Required environment/configuration

- **SSH reachability**: the management cluster's controller pod must be able to reach every
  `RemoteCluster.spec.host` (or `spec.vpnConfig.ip` if set — the controller prefers the VPN IP
  over `Host` when present) on `spec.port` (default `22`). No SSH tunneling/bastion logic exists
  in `pkg/ssh` — it dials the host directly.
- **Passwordless `sudo`** for the SSH user on every node (verified explicitly for `OnPrem`
  `NodeProvision` via `sudo -n true` before provisioning starts —
  `internal/controller/ml/nodeprovision_controller.go:884-891`).
- **Ubuntu 22.04 (Jammy)** target OS — the kubeadm/CRI-O/apt-repo bootstrap logic
  (`pkg/kubeadm`) targets Jammy package repos and paths.
- **WireGuard VPN server**: reachable via SSH from wherever the relevant controller runs; see
  [`docs/wireguard-setup-bundle/WIREGUARD_SETUP.md`](wireguard-setup-bundle/WIREGUARD_SETUP.md)
  for setting one up. The VPN range is `NodeProvisionNetConfig.spec.vpnRange` /
  derived from `RemoteCluster.spec.vpnConfig.ip` via `VPNRangeToCIDR` (assumes a `/24`).
- **GPU nodes**: NVIDIA drivers are expected to already be present — the `NvidiaDriverVersion`
  field referenced in some sample YAML comments is **not** an actual field on any CRD today (it's
  commented out of both `api/v1/remotecluster_types.go:86-88` and
  `api/ml/v1alpha1/nodeprovisionnetconfig_types.go:79-81`); driver installation itself is not
  automated by these controllers.
- **AWS credentials** (for `NodeProvision` with `provider: AWS`): either static
  `awsAccessKeyId`/`awsSecretAccessKey` in the referenced Secret, or an IAM instance profile on
  the controller pod (a background `awsprovision.NewCredentialManager` Runnable refreshes
  STS/MFA-derived sessions — `cmd/main.go:192-196`).

### 3.4 Permissions / cluster prerequisites

The controller's `ClusterRole` (see `config/rbac/role.yaml` and the hand-written
`deploy/clusterrole.yaml`) needs, at minimum:

- Full CRUD + `status`/`finalizers` subresource access to `RemoteCluster`, `NodeProvision`,
  `NodeProvisionNetConfig`.
- `nodes` (get/list/watch/patch/update/delete) — for labeling/tainting/draining joined nodes.
- `secrets`, `configmaps` (including namespace-scoped rules for `kube-public` and `kube-system` —
  used to read `cluster-info` and mint bootstrap-token Secrets).
- `batch/jobs` — for the image pre-pull `Job`.
- `config.porch.kpt.dev` (`packagevariants`), `porch.kpt.dev` (`packagerevisions`),
  `infra.nephio.org` (`repositories`), `token.nephio.org` (`tokens`) — only exercised when
  `gitConfig.enable` is set.

A **security note worth flagging before you deploy this in a sensitive environment**: both
`pkg/ssh.Connect` and `pkg/ssh.ConnectWithPrivateKey` use
`HostKeyCallback: ssh.InsecureIgnoreHostKey()` — SSH host keys are **not verified**. Treat the
management-cluster-to-node and controller-to-VPN-server network paths as trusted (e.g. the
WireGuard tunnel itself) rather than relying on SSH host-key pinning.

---

## 4. Installation

The repository ships **two independent, non-overlapping** ways to install the controller —
pick one, don't mix them:

- **`config/` (kubebuilder-generated, kustomize-based)** — the standard path, driven by `make`.
- **`deploy/` (hand-written manifests)** — a flatter, pre-baked manifest set (its own
  `ClusterRole`/`Role`/`Deployment`/Secret templates) that also targets the
  `remote-cluster-provisioner-system` namespace, plus an unrelated `harbor-registry-configurator`
  DaemonSet for pointing every node's CRI-O at an insecure Harbor registry.

This guide documents the `config/`+`make` path as primary, since it's what the CRDs, RBAC, and
manager Deployment are generated from.

### 4.1 Build and push the controller image

```bash
# Build the manager binary locally (optional sanity check)
make build

# Build and push the controller image
make docker-build docker-push IMG=<registry>/remote-cluster-provisioner:<tag>
```

`docker-build` builds `Dockerfile`'s two-stage image (Go 1.26 builder using `garble` for an
obfuscated/stripped binary, final stage `gcr.io/distroless/static:nonroot`, non-root UID `65532`).

### 4.2 Install the CRDs

```bash
make install
```

This runs `kustomize build config/crd | kubectl apply -f -`, installing:

- `infra.dcn.ssu.ac.kr_remoteclusters.yaml`
- `ml.dcn.ssu.ac.kr_nodeprovisions.yaml`
- `ml.dcn.ssu.ac.kr_nodeprovisionnetconfigs.yaml`

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

### 4.3 Deploy the controller manager

```bash
make deploy IMG=<registry>/remote-cluster-provisioner:<tag>
```

This runs `cd config/manager && kustomize edit set image controller=${IMG}` and then
`kustomize build config/default | kubectl apply -f -`. It creates:

- Namespace **`remote-cluster-provisioner-system`**
- `ServiceAccount`, RBAC (`ClusterRole`/`ClusterRoleBinding`, leader-election `Role`/`RoleBinding`)
- `Deployment/remote-cluster-provisioner-controller-manager` (1 replica), args:
  `--leader-elect --health-probe-bind-address=:8083`
- A metrics `Service` (HTTPS on `:8443`, via `manager_metrics_patch.yaml`, applied by default)

> **Do not** just run `kubectl apply -k config/default` without first setting the image via
> `kustomize edit set image` (either by running `make deploy`, or manually) — the committed
> `config/manager/manager.yaml` ships with the placeholder image `controller:latest`, which will
> not exist in your registry.

Resource requests/limits from `config/manager/manager.yaml`:

| | requests | limits |
|---|---|---|
| CPU | 10m | 500m |
| Memory | 128Mi | 384Mi |

`terminationGracePeriodSeconds: 60` is set deliberately — background SSH goroutines
(control-plane init, on-prem provisioning) need time to observe context cancellation and close
their SSH sessions cleanly rather than being SIGKILLed mid-command.

### 4.4 Apply your cluster credentials and CRs

```bash
# SSH credentials + a RemoteCluster (control-plane example)
kubectl apply -f config/samples/infra_v1_remotecluster_gpu_worker.yaml

# On-prem node provisioning config (applied on the REMOTE cluster, once it exists)
kubectl apply -f config/samples/ml_v1alpha1_nodeprovision.yaml
```

(See [§6](#6-examples) for full walk-throughs and what to expect at each step.)

### 4.5 Container image

```dockerfile
# builder stage: golang:1.26, go mod download, garble-obfuscated static build
CGO_ENABLED=0 garble -literals -tiny build -trimpath -ldflags="-s -w" -o manager ./cmd/main.go

# final stage
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /workspace/manager .
USER 65532:65532
ENTRYPOINT ["/manager"]
```

The final image has no shell and no package manager (distroless) — if you need to `kubectl exec`
into the controller pod for debugging, you won't get a shell; rely on logs and `kubectl describe`
instead.

### 4.6 Verify the installation

```bash
# Controller pod is Running and both probes are passing
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

A healthy controller logs a leader-election acquisition message shortly after startup (structured
JSON in production mode, human-readable text if run with `--zap-devel`/local `make run`).

---

## 5. Usage

### 5.1 `RemoteCluster` — management cluster

Full field reference is in [§10.1](#101-remotecluster-infradcnssuackrv1). Minimal control-plane
example:

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
    enable: "true"                # omit/false to skip Porch/PackageVariant entirely
    gitServer: "http://192.168.3.99:31810"
    gitUsername: "nephio"
    upstreamPlatformRepo: "catalog-workloads-mlplatform"
    packageRevision: "v1.0.0"
```

Key fields to get right:

- **`nodeInfo.nodeType`** — `control-plane` runs `kubeadm init`; `worker` runs `kubeadm join`
  against the sibling `RemoteCluster` in the same namespace whose `spec.clusterName` matches and
  whose `nodeInfo.nodeType == control-plane` (`findControlPlane`,
  `internal/controller/remotecluster_controller.go:1826`). If no such CR exists yet, worker
  reconciliation just requeues every 30s (`controlPlaneRetryInterval`) until it does.
- **`auth`** — exactly one of `sshPrivateKeySecretRef` or `passwordSecretRef`. Key auto-detection:
  a secret value starting with `-----BEGIN` is treated as a private key; anything else as a
  password.
- **`vpnConfig.ip`** — if set, the controller SSHes to **this** IP instead of `spec.host` (VPN IP
  takes priority). This is how it keeps talking to nodes after they're joined to the VPN and the
  LAN IP might become unreachable (different subnet, firewall, etc.).
- **`gitConfig.enable`** — a **string** `"true"`/`"false"`, not a bool. Only when `"true"` does the
  controller create Porch `Repository`/`Token` objects and PackageVariants.

### 5.2 `NodeProvisionNetConfig` — passive config, remote cluster

Usually created/updated automatically (see [§8.1](#81-what-happens-on-createupdatedelete---remotecluster)),
but can be hand-applied for testing a remote cluster in isolation:

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvisionNetConfig
metadata:
  name: my-cluster-netconfig
spec:
  clusterName: my-cluster
  vpnRange: "10.9.0.0/24"
  vpnServerPublicConfig:
    publicIP: "13.215.206.108"
    sshPort: 22
    sshUsername: ubuntu
    vpnPort: 51820
    vpnSshCredentialsRef:
      name: vpn-server-secret
      namespace: default
      key: id_rsa
  softwareConfig:
    kubernetesVersion: "v1.34.2"
    cnlabRuntime:
      registry: "ghcr.io"
      repository: "vitu-mafeni/cnlab-runtime"
      version: "1.0.0-beta"
      orasVersion: "1.3.2"
      credentialsRef:
        name: cnlab-runtime-registry
        namespace: default
```

> The `config/samples/ml_v1alpha1_nodeprovision*.yaml` files (and the top-level README) also show
> `softwareConfig.nvidiaDriverVersion`, `nvidiaContainerToolkitVersion`, and
> `k8sDevicePluginVersion` under `softwareConfig`. **These are not real CRD fields** — they are
> commented out in `api/ml/v1alpha1/nodeprovisionnetconfig_types.go:79-81`, so the API server
> silently drops them if you include them. Don't rely on them; GPU driver/toolkit versions are not
> currently configurable through this CRD.

### 5.3 `NodeProvision` — remote cluster

**OnPrem** (SSH-based):

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: gpu-worker-01
  namespace: default
spec:
  provider: OnPrem
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

**AWS** (EC2 + cloud-init, minimal — most fields auto-resolve):

```yaml
apiVersion: ml.dcn.ssu.ac.kr/v1alpha1
kind: NodeProvision
metadata:
  name: aws-node-001
  namespace: default
spec:
  provider: AWS
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

- **`nodeLabel`** drives AWS auto-defaults (`DefaultInstanceTypeForLabel`) — `"cpu"` →
  `t3.xlarge`, `"gpu"` → `p3.2xlarge`. Set `spec.awsConfig.instanceType` (or the top-level
  `spec.instanceType`) to skip this lookup and use an exact instance type.
- **`hardwareType`** (separate from `nodeLabel`) controls which `imagePrepulls` entries
  (`NodeProvisionNetConfig.spec.softwareConfig.imagePrepulls`) get pre-pulled onto this node via
  the batch `Job` — only relevant for `gpu` nodes with `imagePrepulls` configured.
- **`role`** — samples only ever use `worker`. `NodeProvisionReconciler` only ever performs
  `kubeadm join`; it has no control-plane bootstrap path (that's `RemoteClusterReconciler`'s job).
- **`credentialsRef.key`** — if omitted, the controller tries, in order: `privateKey`, `id_rsa`,
  `ssh-privatekey`, `password`, `key`.

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
roughly 5–15 minutes while a background goroutine runs `kubeadm init` + CRI-O + CNI + ArgoCD
(`reconcileControlPlane` polls every 30s — `controlPlanePollInterval`). Watch it:

```bash
kubectl get remotecluster ml-cluster-cp -w
```

```
NAME             PHASE          MESSAGE
ml-cluster-cp    Provisioning   Provisioning in progress
ml-cluster-cp    Ready          Provisioned
```

Controller logs during this step (`kubectl logs -n remote-cluster-provisioner-system
deployment/remote-cluster-provisioner-controller-manager -f`) — an **illustrative transcript**
assembled from the verbatim message strings in the source (each is cited with a file:line in
[§7.2](#72-representative-example-logs-verbatim-from-source)), arranged in the order the code
emits them for a run with `vpnConfig` configured (as in the full samples under
`config/samples/`). Exact timestamps and the number of `requeueing` lines will vary with how long
`kubeadm init` actually takes on your hardware:

```
2026-09-08T10:15:03.412+0900   INFO    remotecluster   Starting provisioning node for cluster  {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "nodeType": "control-plane", "phase": ""}
2026-09-08T10:15:03.498+0900   INFO    remotecluster   Control plane init goroutine started    {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "startPhase": 0}
2026-09-08T10:15:33.501+0900   INFO    remotecluster   Control plane init in progress, requeueing      {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:16:03.512+0900   INFO    remotecluster   Control plane init in progress, requeueing      {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
                                        ⋮  (repeats every 30s — controlPlanePollInterval — while kubeadm init, CRI-O, CNI and ArgoCD are installed over SSH; typically 5-15 minutes)  ⋮
2026-09-08T10:23:41.220+0900   INFO    remotecluster   Control plane init completed    {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "joinCommand": true}
2026-09-08T10:23:42.005+0900   INFO    remotecluster   Kubeadm bootstrap token has never been explicitly refreshed; will refresh now  {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:23:44.115+0900   INFO    remotecluster   Refreshed kubeadm bootstrap token       {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:23:44.900+0900   INFO    remotecluster   Creating PackageVariants        {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:23:45.760+0900   INFO    remotecluster   Core PackageVariants created — requeueing to allow Porch sync before overlay step      {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "delay": "2m0s"}
2026-09-08T10:25:46.003+0900   INFO    remotecluster   Creating PackageVariants        {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:25:47.410+0900   INFO    remotecluster   PackageVariants created; cluster is fully ready        {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster"}
2026-09-08T10:25:48.020+0900   INFO    remotecluster   Kubeadm bootstrap token still valid     {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "refreshedAt": "2026-09-08T10:23:44+09:00", "nextRefreshIn": "22h58m0s"}
2026-09-08T10:25:48.021+0900   INFO    remotecluster   Cluster fully ready     {"cluster": "ml-cluster-cp", "clusterName": "ml-cluster", "nextTokenRefreshIn": "22h58m0s"}
```

> `Creating PackageVariants` and the bootstrap-token messages appear more than once above — that's
> expected, not a duplicate/retry bug: each state change (`annotationCoreVariantsCreated`, then
> `annotationPkgVariantsCreated`, then the token refresh timestamp) triggers its own follow-up
> reconcile, and every reconcile of a `Ready` cluster re-evaluates the token/PackageVariant checks
> from scratch.

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

**Expect:** the worker's reconcile finds the CP `RemoteCluster` via `findControlPlane`, confirms
it's `Ready` with a non-empty `status.joinCommand`, then SSHes into the worker and runs
`kubeadm join`. On success it labels the resulting Node
(`infra.dcn.ssu.ac.kr/worker=true`, `infra.dcn.ssu.ac.kr/hardware-type=gpu`) so any prepull
DaemonSets can schedule onto it.

Controller logs during this step — illustrative transcript, same caveats as Step 1. Note the gap:
unlike the control-plane path, `reconcileWorker` runs `kubeadm.JoinWorkerNode` **synchronously**
inside the reconcile call (bounded by the 30-minute `sshOperationTimeout`), so there is no
`requeueing`/progress-poll noise while the join is in progress — the log simply goes quiet for
several minutes between the first line and the last:

```
2026-09-08T10:31:02.104+0900   INFO    remotecluster   Synced VPN server config from control-plane     {"cluster": "ml-cluster-worker-01", "clusterName": "ml-cluster", "cp": "ml-cluster-cp"}
                                        ⋮  (no further log lines while kubeadm join runs over SSH — typically a few minutes)  ⋮
2026-09-08T10:34:18.760+0900   INFO    remotecluster   Worker node joined to cluster   {"cluster": "ml-cluster-worker-01", "clusterName": "ml-cluster"}
2026-09-08T10:34:19.902+0900   INFO    remotecluster   Labeled worker node for DaemonSet targeting     {"cluster": "ml-cluster-worker-01", "node": "ml-cluster-worker-01", "hardwareType": "gpu"}
```

**Step 3 — scale out with `NodeProvision`**, applied **on the remote cluster** (`ml-cluster`)
once it's up and its `kubeconfig` is available (via `NodeProvisionNetConfig.status.kubeconfig`,
refreshed by a systemd timer on the control plane — see [§8.1](#81-what-happens-on-createupdatedelete---remotecluster)):

```bash
kubectl --kubeconfig=ml-cluster.kubeconfig apply -f config/samples/ml_v1alpha1_nodeprovision.yaml
```

That sample file applies, in order: the `NodeProvision` CR (`provider: OnPrem`), its SSH Secret,
a `NodeProvisionNetConfig` (harmless to re-apply — it will already exist from Step 1/2), the
`cnlab-runtime` registry credentials Secret, and the VPN server SSH Secret.

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
the **remote cluster's** controller (`NodeProvisionReconciler`):

```
2026-09-08T11:02:10.301+0900   INFO    nodeprovision   Request received, starting provisioning         {"nodeprovision": "gpu-worker-01", "provider": "OnPrem"}
2026-09-08T11:02:10.940+0900   INFO    nodeprovision   Validation successful   {"nodeprovision": "gpu-worker-01"}
2026-09-08T11:02:11.205+0900   INFO    nodeprovision   On-prem bootstrap goroutine started     {"nodeprovision": "gpu-worker-01"}
                                        ⋮  (background goroutine: SSH kubeadm join, CRI-O, CDI setup — typically several minutes, bounded by the 20-minute onPremBootstrapStallTimeout)  ⋮
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

### 6.2 Example: rotating `cnlab-runtime` registry credentials

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

No restart needed. `RemoteClusterReconciler` detects the SHA-256 hash change of `username:token`
on its next `Ready`-phase reconcile (management cluster side, up to ~23h — or immediately on
controller restart via the `Start()` runnable) and re-pushes the secret + patches
`NodeProvisionNetConfig` over SSH. `NodeProvisionReconciler` detects the same hash change on the
remote cluster (every reconcile of a `Ready` `NodeProvision`) and runs `oras login` on that node.

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
# Follow the controller manager (same binary/deployment name on both
# management and remote clusters — namespace is always
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

Logs go through `controller-runtime`'s zap-backed logger. `cmd/main.go:86-88` hardcodes
`zap.Options{Development: true}` before flags are parsed, and neither `config/manager/manager.yaml`
nor `deploy/deployment.yaml` pass any `--zap-*` container args to override it — so **out of the box
this controller logs in zap's development console format** (tab-separated
`timestamp  LEVEL  logger-name  message  {json fields}`), not JSON, regardless of whether it's
running via `make run` or as the deployed container. If you want structured JSON for a log
pipeline, add `--zap-devel=false` (optionally with `--zap-encoder=json`) to the container's `args`
in `config/manager/manager.yaml`/`deploy/deployment.yaml`. A handful of low-level messages inside
`pkg/kubeadm` use the plain standard-library `log` package instead (e.g. `[phase %d/%d] Running %q`)
and will appear as unstructured lines interleaved with the structured ones.

### 7.2 Representative example logs (verbatim from source)

> Labeled **representative** — these are the exact strings emitted by the code
> (`internal/controller/remotecluster_controller.go`,
> `internal/controller/ml/nodeprovision_controller.go`), but the surrounding fields
> (`cluster=`, `attempt=`, timestamps) will vary per run.

**`RemoteCluster` — normal provisioning:**

```
INFO  Starting provisioning node for cluster            cluster=ml-cluster-cp clusterName=ml-cluster nodeType=control-plane
INFO  Control plane init goroutine started               startPhase=0
INFO  Control plane init in progress, requeueing
INFO  Control plane init completed                        joinCommand=true
INFO  Cluster fully ready                                  nextTokenRefreshIn=23h0m0s
```

**`RemoteCluster` — bootstrap token refresh:**

```
INFO  Kubeadm bootstrap token due for refresh
INFO  Refreshed kubeadm bootstrap token
INFO  Kubeadm bootstrap token still valid                 refreshedAt=... nextRefreshIn=21h0m0s
```

**`RemoteCluster` — retry / terminal failure:**

```
ERROR RemoteCluster provisioning failed — will retry       attempt=2 maxRetries=5
ERROR RemoteCluster provisioning reached retry limit — no further retries   attempts=5 maxRetries=5
```

**`RemoteCluster` — cnlab-runtime sync:**

```
INFO  cnlab-runtime credentials changed — syncing to remote cluster
ERROR cnlab-runtime credential sync reached retry limit — no further retries
```

**`NodeProvision` — normal on-prem provisioning:**

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

**`NodeProvision` — AWS provisioning:**

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

**`NodeProvision` — stall / retry:**

```
INFO  NodeProvision stalled without an InstanceID, failing for retry
INFO  On-prem bootstrap stalled with no progress, failing for retry
INFO  Node registration stalled, failing for retry
INFO  NodeProvision in terminal Failed state — retry limit reached, manual intervention required
INFO  Retrying NodeProvision after failure — releasing stale VPN IP and resetting phase
```

**`NodeProvision` — bootstrap token (local, no SSH):**

```
INFO  Bootstrap token missing or expired — refreshing via local Kubernetes API   tokenAge=never
INFO  Bootstrap token still valid                                                 age=2h0m expiresIn=21h0m
INFO  Refreshed bootstrap token
```

**Deletion:**

```
INFO  Deprovisioning RemoteCluster
INFO  Resetting node via SSH
INFO  Node reset complete
INFO  Removed WireGuard peer from VPN server
INFO  RemoteCluster cleanup complete
```

```
INFO  Deprovisioning node
INFO  Removed node from cluster
INFO  On-prem node reset complete
INFO  Removed VPN peer from server
INFO  Cleanup complete
```

### 7.3 Common errors & how to resolve them

| Symptom / log message | Likely cause | Resolution |
|---|---|---|
| `SSHConnectionFailed` in `status.message` | Host unreachable, wrong credentials, or WireGuard not up yet | Verify `spec.host`/`spec.vpnConfig.ip` is reachable from the controller pod's network; check the referenced Secret's key name matches `auth.*SecretRef.key` |
| `RemoteCluster provisioning reached retry limit — no further retries` | 5 consecutive provisioning failures (`maxProvisionRetries`) | Fix the underlying issue (see `status.message`), then reset: `kubectl patch remotecluster <name> --subresource=status --type=merge -p '{"status":{"provisionRetryCount":0}}'` |
| `NodeProvision in terminal Failed state` | 5 consecutive provisioning failures on the remote side | `kubectl patch nodeprovision <name> --subresource=status --type=merge -p '{"status":{"provisionRetryCount":0}}'` |
| `cnlab-runtime credential sync reached retry limit` | VPN link between management and remote cluster may be down, or credentials Secret is malformed | Check VPN connectivity to the remote cluster; reset via `kubectl patch remotecluster <name> --subresource=status --type=merge -p '{"status":{"cnlabSyncRetryCount":0}}'` |
| Node stuck in `Bootstrapping` | The provisioning goroutine is still running (kubeadm init/join + CRI-O install is legitimately 5–15 min), or it's genuinely hung | `kubectl get nodeprovision <name> -o jsonpath='{.status.message}'`; SSH in and check `sudo fuser /var/lib/dpkg/lock-frontend` and `tail -50 /var/log/node-provision.log` |
| AWS `oras pull` returns `unauthorized` | `NodeProvisionNetConfig.spec.softwareConfig.cnlabRuntime.credentialsRef` missing/empty | Apply the `cnlab-runtime-registry` Secret and patch the `NodeProvisionNetConfig` (or re-apply the sample, which includes it) |
| CNI plugins missing after kubeadm init | The CNI-plugins-bundle install step in `pkg/kubeadm` phase 9 was interrupted | Manually install: `wget` the `cni-plugins-linux-amd64-v1.5.1.tgz` release and extract to `/opt/cni/bin` (see README's troubleshooting section for the exact commands) |
| `PackageVariants` not appearing / stuck | Porch hasn't synced the new cluster `Repository` yet, or a stale `PackageVariant` exists from a previous attempt | `kubectl get repository.infra.nephio.org`, `kubectl get packagevariants`; delete stale variants named in the README (e.g. `gpu-operator-variant`, `harbor-variant`, …) to force re-creation |
| Dex service works after cluster is Ready, but login fails | Dex `Service` selector doesn't match the ArgoCD-deployed Dex pod's labels | `kubectl patch svc dex -n auth --type=json -p='[{"op":"replace","path":"/spec/selector","value":{"app":"dex"}}]'` |
| `NodeProvisionReconciler` never advances past `RegisteringNode` | Node isn't visible in `kubectl get nodes` — kubelet failed to register, or the VPN IP recorded in `status.ipAddress`/`vpnIp` doesn't match any Node's address | SSH into the node, check `systemctl status kubelet`, and check `wg show wg0` on both the node and VPN server for a matching peer |
| `AWS provider not yet implemented` for GCP/Azure | Only `AWS` and `OnPrem` are implemented providers | Use `AWS` or `OnPrem`; `GCP`/`Azure` return an explicit "not yet implemented" error |

### 7.4 Debugging / diagnostic commands

```bash
# Full status block for a RemoteCluster or NodeProvision
kubectl get remotecluster <name> -o yaml
kubectl get nodeprovision <name> -o yaml

# Condition history (setStatus APPENDS conditions — do not just look at the latest)
kubectl get remotecluster <name> -o jsonpath='{.status.conditions}' | jq

# Annotations the controller uses to resume/skip work across restarts
kubectl get remotecluster <name> -o jsonpath='{.metadata.annotations}' | jq

# Check what phase a stuck on-prem NodeProvision goroutine has reached
kubectl get nodeprovision <name> -o jsonpath='{.status.progress}{"\n"}{.status.message}'

# Inspect the image pre-pull Job (GPU nodes only)
kubectl get jobs -l job-name=<nodeprovision-name>-prepull
kubectl logs job/<nodeprovision-name>-prepull

# On the remote control-plane host directly (bypasses the operator entirely)
ssh ubuntu@<cp-ip> 'sudo tail -50 /var/log/node-provision.log'
ssh ubuntu@<cp-ip> 'sudo crictl info'
ssh ubuntu@<cp-ip> 'wg show wg0'
```

---

## 8. Controller Lifecycle / Reconciliation

### 8.1 What happens on Create/Update/Delete — `RemoteCluster`

**Create:**
1. Finalizer `infra.dcn.ssu.ac.kr/remotecluster-finalizer` is added (immediate `Requeue`, since
   `GenerationChangedPredicate` filters the metadata-only update this produces).
2. The referenced SSH credential Secret (and VPN SSH Secret, if configured) each get a protective
   finalizer, plus a controller-owned copy is kept (`<name>-controller-auth`), so provisioning
   can't be broken by someone deleting the user's Secret mid-flight.
3. Phase becomes `Provisioning`. For `control-plane`: a background goroutine runs the 13-phase
   `kubeadm.InitializeControlPlane` pipeline (cleanup → NFS server → sysctl → apt repos → CRI-O
   install/start → kubelet/kubeadm install → `kubeadm init` → post-init labeling → CNI (Flannel
   over `wg0`) → ArgoCD/CRD/cert-manager addons → NFS provisioner). For `worker`: the
   9-phase `kubeadm.JoinWorkerNode` pipeline runs synchronously within the reconcile call (bounded
   by a 30-minute SSH operation timeout), requiring the sibling control-plane `RemoteCluster` to
   already be `Ready`.
4. On success: `createClusterRepo` (Porch `Repository`/`Token`, only if `gitConfig.enable`), then
   `handleCreateUpdateNodeProvisionConfig(..., "create")` (creates `NodeProvisionNetConfig` on the
   remote cluster over SSH), then phase → `Ready`.
5. Still on the same or a following `Ready`-phase reconcile: a **local** copy of
   `NodeProvisionNetConfig` is created/kept in sync on the *management* cluster too (read by
   `NodeProvisionReconciler` there, if it's ever used to provision nodes for this logical cluster
   from the management side), the bootstrap token is refreshed if due, `cnlab-runtime` credentials
   are synced if present/rotated, and finally **`PackageVariants`** are created in two waves — core
   variants first, then (after a 2-minute wait for Porch to sync the repo) overlay + post-install
   variants.

**Update:** re-reconciles from whatever phase `status.phase` currently records. Annotations track
the last completed sub-phase index (`infra.dcn.ssu.ac.kr/last-completed-phase-cp` /
`-worker`) so a retry resumes from the failed phase instead of restarting `kubeadm init`/`join`
from scratch. A `Ready` cluster is re-reconciled periodically (`RequeueAfter` = time remaining
until the next 23h token refresh) purely to keep the token fresh and catch config drift (VPN/
software config/cnlab-runtime credentials) — this is also how a config change to the *parent*
control-plane `RemoteCluster` eventually reaches its `NodeProvisionNetConfig`.

**Delete** (`handleDelete`, triggered by `metadata.deletionTimestamp`):
1. No-op if the finalizer is already gone.
2. If it's a `worker`: drain + delete the corresponding Node from the control-plane side
   (`kubectl drain ... --ignore-daemonsets --delete-emptydir-data --force --timeout=120s`).
3. SSH into the node itself and run a full reset: `kubeadm reset --force`, stop/purge
   kubelet/kubeadm/kubectl/CRI-O/NVIDIA-container-toolkit, wipe `/etc/kubernetes`,
   `/var/lib/kubelet`, `/var/lib/etcd`, `/var/lib/crio`, then (in a detached background block, 3s
   delayed so the SSH session can exit first) tear down the WireGuard interface.
4. Remove the node's WireGuard peer from the VPN server (`wg set wg0 peer <key> remove`, plus
   editing `wg0.conf` so it doesn't reappear on VPN server restart).
5. Delete management-cluster-side Porch/Nephio objects (`Repository`/`Token`/`PackageVariant`)
   labeled for this cluster.
6. Remove the SSH-Secret and VPN-Secret finalizers (only now — after all SSH work is done).
7. Remove the `RemoteCluster` finalizer itself, allowing final deletion.

All of steps 2–5 are **best-effort** — errors are logged, not returned, so a genuinely
unreachable/already-gone node never blocks CR deletion. Only step 7's `Update` call is treated as
fatal to the reconcile.

### 8.2 What happens on Create/Update/Delete — `NodeProvision`

**Create:** finalizer `ml.dcn.ssu.ac.kr/nodeprovision-finalizer` added; phase progresses through
the state machine in [§2.4](#24-reconciliation--control-flow-diagrams). `OnPrem` provisioning runs
in a background goroutine (bounded by a 20-minute stall timeout, tracked in-process via
`sync.Map`s so a controller restart can detect an orphaned goroutine and restart provisioning
cleanly). `AWS` provisioning auto-resolves unset fields (`resolveAWSDefaults`) — instance type
from `nodeLabel`, latest Ubuntu 22.04 AMI, default VPC/subnet/security group, and an EC2 key pair
(persisted as `<name>-ssh-key` Secret) — persisting each resolved value with its own patch so the
reconcile's watch re-triggers cleanly rather than silently overwriting spec fields in memory only.

Once the node is visible in `kubectl get nodes` (matched by VPN IP), the controller adds a
**Node-level finalizer** (`ml.dcn.ssu.ac.kr/nodeprovision-node-finalizer`) — this is what prevents
`kubectl delete node` from bypassing the `NodeProvision` controller's own cleanup path.

**Update:** `Ready`-phase `NodeProvision`s are re-reconciled every cycle purely to run
`syncRuntimeCredentials` (checks the `cnlab-runtime` credentials hash and re-runs `oras login` on
drift). A `Failed` `NodeProvision` (below the retry cap) is automatically reset — `status.phase`,
`vpnIP`, and `ipAddress` are cleared, and the stale VPN peer (if any) is released — so the next
reconcile starts a clean attempt rather than resuming mid-way.

**Delete:**
1. Phase → `Deleting`.
2. The Kubernetes `Node` object's finalizer is removed and the Node is deleted; the controller
   waits (5s requeue) for confirmed deletion before touching cloud/SSH resources.
3. Provider-specific cleanup: **AWS** terminates the EC2 instance (with an auth-failure-aware
   retry that evicts a cached STS session and retries with static credentials only); **OnPrem**
   SSHes in and runs the same kind of full node-reset script used by `RemoteClusterReconciler`.
4. The node's WireGuard peer is removed from the VPN server.
5. The `<name>-ssh-key` Secret (AWS-generated key pair) is deleted, if present.
6. The `NodeProvision` finalizer is removed.

### 8.3 Retries, failures, status updates, recovery

Both controllers follow the same overall retry contract:

- Every provisioning failure increments a `*RetryCount` status field
  (`RemoteCluster.status.provisionRetryCount`, `RemoteCluster.status.cnlabSyncRetryCount`,
  `NodeProvision.status.provisionRetryCount`) via `client-go`'s `retry.RetryOnConflict` (so a
  concurrent status write elsewhere never silently loses the increment).
- After **5** consecutive failures (`maxProvisionRetries` / `maxCnlabSyncRetries`, both `= 5`), the
  resource is left in a **terminal** state — the controller stops requeueing and logs that manual
  intervention is required. It is deliberately not automatic, since 5 back-to-back failures against
  the same host usually indicates something a human needs to look at (wrong credentials, host
  down, disk full, etc.).
- Recovery is a manual `kubectl patch ... --subresource=status --type=merge -p '{"status":{"...RetryCount":0}}'`
  — see [§7.3](#73-common-errors--how-to-resolve-them) for the exact commands per resource.
- `RemoteCluster.status.conditions` **accumulates** a full history of `metav1.Condition` entries
  (one appended per `setStatus` call) rather than upserting a single "latest" condition per type —
  useful for a post-mortem of exactly what happened across every reconcile, at the cost of the
  list growing over the resource's lifetime.
- Sub-phase progress (which step inside the multi-phase kubeadm pipeline last completed) is
  persisted to CR **annotations**, not status — this lets a retry (or a controller restart mid-init)
  resume from the failed step instead of re-running the entire `kubeadm init`/`join`
  sequence from scratch.
- Two sync loops are **intentionally exempt** from the 5-strikes-and-terminal pattern:
  `syncNetConfigToRemote` (management → remote `NodeProvisionNetConfig` field sync) retries
  indefinitely every reconcile with no terminal state, because an unreachable remote cluster is
  considered a normal, temporary condition, not an error requiring a human — by design, per an
  explicit comment in the source contrasting it with the credential-sync path.

---

## 9. Operations

### 9.1 Health checks

```bash
kubectl get pods -n remote-cluster-provisioner-system
kubectl describe pod -n remote-cluster-provisioner-system \
  -l control-plane=controller-manager
```

Liveness/readiness are plain `healthz.Ping` checks (`cmd/main.go:215-222`) served on
`:8083` (`/healthz`, `/readyz`) — they only confirm the manager process is alive and its internal
caches have synced, **not** that any particular SSH/AWS/VPN operation is succeeding. For that, use
the CR-level status/conditions and logs described in [§7](#7-logs--troubleshooting).

### 9.2 Restart

```bash
kubectl rollout restart deployment/remote-cluster-provisioner-controller-manager \
  -n remote-cluster-provisioner-system
```

Because progress for long-running operations (kubeadm init phases, on-prem bootstrap goroutines)
is persisted to CR annotations/status rather than only held in memory, a restart resumes
in-flight work from the last completed phase rather than starting over — this is by design (see
[§8.3](#83-retries-failures-status-updates-recovery)). `terminationGracePeriodSeconds: 60` gives
in-flight SSH sessions time to close cleanly on shutdown.

### 9.3 Upgrade

```bash
make docker-build docker-push IMG=<registry>/remote-cluster-provisioner:<new-tag>
make deploy IMG=<registry>/remote-cluster-provisioner:<new-tag>
```

If the new version adds/changes CRD fields, re-run `make install` (or `make manifests` first if
you've edited `api/` types yourself) **before** `make deploy`, so the CRDs are compatible with the
new controller version.

### 9.4 Uninstall

```bash
# Remove the controller Deployment/RBAC/namespace
make undeploy

# Remove the CRDs (this will orphan any RemoteCluster/NodeProvision objects —
# their finalizers will never run, so nodes/VPN peers/PackageVariants they
# manage will NOT be cleaned up automatically)
make uninstall
```

**Before running `make uninstall` or deleting a namespace containing live `RemoteCluster`/
`NodeProvision` objects**, delete those CRs first (`kubectl delete remotecluster --all`,
`kubectl delete nodeprovision --all`) and wait for their finalizers to clear, so `handleDelete`
gets a chance to reset nodes and remove VPN peers. Deleting the CRDs out from under live CRs skips
all of that cleanup.

### 9.5 Inspecting controller status

```bash
# All RemoteClusters and their current phase across the fleet
kubectl get remotecluster -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,CLUSTER:.spec.clusterName

# All NodeProvisions on a remote cluster
kubectl get nodeprovision -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,PROVIDER:.spec.provider,IP:.status.ipAddress

# Leader-election lease (single-replica by default, but useful to confirm which pod is active)
kubectl get lease -n remote-cluster-provisioner-system
```

### 9.6 Monitoring / observability

- **Metrics**: a `/metrics` endpoint is exposed via the `controller-runtime` metrics server;
  `config/default/kustomization.yaml` applies `manager_metrics_patch.yaml` by default, which
  enables **HTTPS metrics on `:8443`** (backed by `--metrics-bind-address` / `--metrics-secure`
  flags in `cmd/main.go`).
- **Prometheus `ServiceMonitor`**: `config/prometheus/monitor.yaml` defines one (scrapes `/metrics`
  over HTTPS with bearer-token auth), but it is **commented out** of
  `config/default/kustomization.yaml` — uncomment the `- ../prometheus` line (and the
  cert-manager-related `replacements` block, if you also want mTLS on the metrics endpoint) to
  enable it.
- **No custom application metrics** (e.g. reconcile counters/histograms specific to this
  project) were found in the codebase beyond the standard `controller-runtime` workqueue/reconcile
  metrics that come for free with the manager.
- Day-to-day observability in practice is log- and status/condition-driven (see
  [§7](#7-logs--troubleshooting) and [§9.5](#95-inspecting-controller-status)), not
  dashboard-driven.

---

## 10. Reference

### 10.1 `RemoteCluster` (`infra.dcn.ssu.ac.kr/v1`)

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
| `nodeInfo.softwareConfig.imagePrepulls[]` | `{image, nodeTarget}` | `nodeTarget` enum `gpu`\|`all`, default `all` |
| `nodeInfo.softwareConfig.imagePullSecretRef` | `*SecretKeyReference` | optional |
| `nodeInfo.softwareConfig.cnlabRuntime` | `*CnlabRuntimeConfig` | optional; defaults: registry `ghcr.io`, repository `vitu-mafeni/cnlab-runtime`, version `1.0.0-beta`, orasVersion `1.3.2` |
| `nodeInfo.softwareConfig.platformVariables[]` | `{key, value}` | optional; passed to overlay `PackageVariant` setters |
| `auth.sshPrivateKeySecretRef` / `auth.passwordSecretRef` | `*SecretKeyReference` | exactly one; key content auto-detects key vs. password auth |
| `vpnConfig.ip` | string | this node's WireGuard IP; SSH target preferred over `host` when set |
| `vpnConfig.vpnServerPublicIP` / `vpnServerSSHPort` / `vpnServerSSHUsername` | string | VPN server SSH connection details |
| `vpnConfig.vpnSshCredentialsRef` | `VPNSSHCredentialsRef{name,namespace,key}` | Secret with the VPN server's SSH credential |
| `gitConfig.enable` | string | `"true"`/`"false"` — gates all Porch/Nephio object creation |
| `gitConfig.gitServer` / `gitUsername` / `upstreamPlatformRepo` / `packageRevision` | string | Porch `Repository`/`PackageVariant` upstream source |

**Status:**

| Field | Meaning |
|---|---|
| `phase` | `Provisioning` → `Ready` → `Failed` |
| `message` | Human-readable status, includes retry count on failure |
| `conditions[]` | Appended (not upserted) history of `metav1.Condition`, `Type` = the failure/success reason |
| `joinCommand` | Cached kubeadm join command from the control-plane, used by sibling worker `RemoteCluster`s |
| `provisionRetryCount` | Consecutive provisioning failures; resets to 0 on `Ready` |
| `cnlabSyncRetryCount` | Consecutive `cnlab-runtime` credential sync failures |

### 10.2 `NodeProvision` (`ml.dcn.ssu.ac.kr/v1alpha1`)

**Spec:**

| Field | Type | Notes |
|---|---|---|
| `provider` | string | `OnPrem` \| `AWS` (`GCP`/`Azure` are defined constants but return "not yet implemented") |
| `role` | string | samples only use `worker`; the controller has no control-plane bootstrap path |
| `hardwareType` | string | `gpu`/`cpu`(or empty) — filters `imagePrepulls` targeting |
| `nodeLabel` | string | drives AWS default instance-type resolution (`cpu`→`t3.xlarge`, `gpu`→`p3.2xlarge`) |
| `region` | string | AWS region |
| `instanceType` | string | overrides the `nodeLabel` lookup |
| `instanceId` | string | usually left empty; populated by the controller |
| `hostname` / `ipAddress` | string | OnPrem target; `ipAddress` takes priority if both set |
| `sshPort` | int | default `22` |
| `sshUsernameOverride` | string | |
| `credentialsRef` | `{name, namespace, key}` | `key` defaults to trying `privateKey`, `id_rsa`, `ssh-privatekey`, `password`, `key` in order |
| `awsConfig` | `*AWSConfig` | `vpcId`, `subnetId`, `securityGroupIds[]`, `ami`, `keyPairName`, `iamInstanceProfile`, `tags{}`, `rootVolumeSizeGB` — all auto-resolved if omitted |

**Status:**

| Field | Meaning |
|---|---|
| `phase` | One of 14 values — see [§2.4](#24-reconciliation--control-flow-diagrams) |
| `message` | Human-readable status |
| `startTime` / `completionTime` | Timestamps |
| `instanceId` / `hostname` / `ipAddress` | Identity |
| `publicIp` / `privateIp` | AWS-only |
| `vpnIp` | WireGuard IP allocated for this node |
| `progress` | 0–100 |
| `nodeName` | Kubernetes Node name once registered |
| `runtimeCredentialsHash` | SHA-256 of last-synced `cnlab-runtime` username+token |
| `provisionRetryCount` | Consecutive failures; resets to 0 on `Ready` |

### 10.3 `NodeProvisionNetConfig` (`ml.dcn.ssu.ac.kr/v1alpha1`)

**Spec:**

| Field | Type | Notes |
|---|---|---|
| `clusterName` | string | |
| `vpnRange` | `*string` | CIDR, e.g. `10.9.0.0/24` |
| `vpnServerPublicConfig.publicIP` | string | |
| `vpnServerPublicConfig.sshPort` / `sshUsername` | string | defaults `22` / `ubuntu` |
| `vpnServerPublicConfig.vpnPort` | string | default `51820` |
| `vpnServerPublicConfig.vpnSshCredentialsRef` | `{name,namespace,key}` | |
| `softwareConfig.kubernetesVersion` | string | |
| `softwareConfig.imagePrepulls[]` | `{image, nodeTarget}` | |
| `softwareConfig.imagePullSecretRef` | `*SecretKeyReference` | |
| `softwareConfig.cnlabRuntime` | `*CnlabRuntimeConfig` | same shape/defaults as `RemoteCluster`'s |

**Status:**

| Field | Meaning |
|---|---|
| `usedIPAddresses[]` | VPN IPs already allocated in this cluster |
| `clusterJoinCommand` | kubeadm join command for workers |
| `joinTokenRefreshedAt` | last local-API token refresh timestamp |
| `vpnPeers[]` | `{nodeName, publicKey, vpnIP}` |
| `kubeconfig` | base64 admin kubeconfig, refreshed by a systemd timer on the control-plane node |

> **Not real fields** (present in some sample YAML but commented out of the Go types, so silently
> dropped by the API server): `softwareConfig.nvidiaDriverVersion`,
> `softwareConfig.nvidiaContainerToolkitVersion`, `softwareConfig.k8sDevicePluginVersion`.

### 10.4 Finalizers, annotations, and condition/reason strings (`RemoteCluster`)

| Constant | String |
|---|---|
| `remoteClusterFinalizer` | `infra.dcn.ssu.ac.kr/remotecluster-finalizer` |
| `authSecretFinalizer` | `infra.dcn.ssu.ac.kr/remotecluster-ssh-auth` |
| `vpnSecretFinalizer` | `infra.dcn.ssu.ac.kr/remotecluster-vpn-ssh-auth` |
| `annotationPkgVariantsCreated` | `infra.dcn.ssu.ac.kr/package-variants-created` |
| `annotationWorkerJoined` | `infra.dcn.ssu.ac.kr/worker-joined` |
| `annotationLastCompletedPhaseCP` / `-Worker` | `infra.dcn.ssu.ac.kr/last-completed-phase-cp` / `-worker` |
| `annotationCPInitComplete` | `infra.dcn.ssu.ac.kr/cp-init-complete` |
| `annotationJoinCmdCache` | `infra.dcn.ssu.ac.kr/join-cmd-cache` |
| `annotationNodeProvisionCreated` | `infra.dcn.ssu.ac.kr/node-provision-created` |
| `annotationJoinTokenRefreshedAt` | `infra.dcn.ssu.ac.kr/join-token-refreshed-at` |
| `annotationCnlabCredentialsHash` | `infra.dcn.ssu.ac.kr/cnlab-credentials-hash` |
| `annotationNetConfigSyncHash` | `infra.dcn.ssu.ac.kr/netconfig-sync-hash` |
| `annotationCoreVariantsCreated` | `infra.dcn.ssu.ac.kr/core-variants-created` |
| `cnlabSyncConditionType` | `CnlabCredentialSyncFailed` |

`r.fail(ctx, cluster, "<Reason>", err)` reason strings: `SSHConnectionFailed`,
`NodeProvisionNetConfigUpdateFailed`, `UnknownNodeType`, `RuntimeConfigError`,
`ControlPlaneInitFailed`, `CorePackageVariantsFailed`, `OverlayPackageVariantsFailed`,
`ClusterRepoFailed`.

`NodeProvision` finalizers: `ml.dcn.ssu.ac.kr/nodeprovision-finalizer` (on the CR),
`ml.dcn.ssu.ac.kr/nodeprovision-node-finalizer` (on the Kubernetes `Node`).

### 10.5 CLI / `make` commands

| Command | Effect |
|---|---|
| `make manifests` | Regenerate CRDs/RBAC from `+kubebuilder` markers into `config/crd/bases` |
| `make generate` | Regenerate `DeepCopy` methods (`zz_generated.deepcopy.go`) |
| `make fmt` / `make vet` | `go fmt` / `go vet` |
| `make test` | envtest-based unit tests (excludes `e2e`) |
| `make test-e2e` | Kind-cluster-based e2e tests |
| `make lint` / `make lint-fix` | golangci-lint |
| `make build` | `go build -o bin/manager cmd/main.go` |
| `make run` | Run the manager locally, out-of-cluster |
| `make docker-build` / `docker-push` | Build/push the container image (`IMG` variable) |
| `make docker-buildx` | Multi-arch build/push |
| `make build-installer` | Generate a consolidated `dist/install.yaml` |
| `make install` / `make uninstall` | Apply/delete CRDs only |
| `make deploy IMG=...` / `make undeploy` | Apply/delete the full controller + RBAC + CRDs |

### 10.6 Relevant directories/files

| Path | Contents |
|---|---|
| `cmd/main.go` | Manager entrypoint, scheme registration, controller `SetupWithManager` calls, flags |
| `internal/controller/remotecluster_controller.go` | `RemoteClusterReconciler` (management cluster) |
| `internal/controller/ml/nodeprovision_controller.go` | `NodeProvisionReconciler` (remote cluster) |
| `internal/controller/ml/nodeprovisionnetconfig_controller.go` | `NodeProvisionNetConfigReconciler` (no-op scaffold) |
| `internal/controller/assets/` | Embedded CRD YAML used by the controller (`//go:embed`) |
| `api/v1/remotecluster_types.go` | `RemoteCluster` CRD Go types |
| `api/ml/v1alpha1/nodeprovision_types.go` | `NodeProvision` CRD Go types |
| `api/ml/v1alpha1/nodeprovisionnetconfig_types.go` | `NodeProvisionNetConfig` CRD Go types |
| `pkg/kubeadm/` | `InitializeControlPlane`, `JoinWorkerNode`, NVIDIA/CDI helpers |
| `pkg/ssh/` | SSH `Client`/`Connect`/`ConnectWithPrivateKey`/`Run` |
| `pkg/runtime/` | `cnlab-runtime` config + ORAS-based install steps |
| `pkg/argocd/` | `ConfigureArgoCD` |
| `provider/aws/` | EC2 provisioning + `CredentialManager` (STS/MFA) |
| `provider/onprem/` | `NewInClusterProvisioner` (SSH bootstrap for `NodeProvision`) |
| `config/crd/bases/` | Generated CRD manifests |
| `config/rbac/` | Generated RBAC manifests |
| `config/manager/` | Generated manager `Deployment`/`Namespace` |
| `config/default/` | Kustomize overlay tying CRD+RBAC+manager together (`make install`/`deploy` target) |
| `config/samples/` | Example CRs (see [§6](#6-examples)) |
| `config/prometheus/` | `ServiceMonitor` (not enabled by default) |
| `deploy/` | Hand-written alternative manifest set + `harbor-registry-configurator` DaemonSet |
| `Dockerfile` | Two-stage build (Go 1.26 builder + distroless final image) |
| `Makefile` | All `make` targets in [§10.5](#105-cli--make-commands) |
| `docs/wireguard-setup-bundle/WIREGUARD_SETUP.md` | WireGuard VPN server/client setup guide |
| `README.md` | Top-level project overview (source for several examples in this guide) |

### 10.7 Useful links

- Project README: [`../README.md`](../README.md)
- WireGuard setup guide: [`wireguard-setup-bundle/WIREGUARD_SETUP.md`](wireguard-setup-bundle/WIREGUARD_SETUP.md)
- Kubebuilder project book (background on the `config/` scaffold): https://book.kubebuilder.io/
- Nephio/Porch documentation (for `gitConfig.enable: "true"` deployments): https://docs.nephio.org/

---

*This guide reflects the state of the `harbor` branch at the time of writing. Controller behavior
is derived directly from the Go source; where sample manifests or the top-level README diverge
from the actual CRD schema or reconciler logic, this guide follows the source code and calls out
the discrepancy inline.*
