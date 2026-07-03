package kubeadm

import (
	"fmt"
	"log"
	"strings"
	"time"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	"dcn.ssu.ac.kr/infra/pkg/argocd"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

const (
	CrioAsset   = "https://github.com/vitu-mafeni/leehun-cri-o/releases/download/crio-1.35.0-restore-from-file/crio"
	CrioCommit  = "a0e6cb3d7f0ca8e9f31131daa17570082e716393"
	CriuAsset   = "https://github.com/vitu-mafeni/leehun-criu/releases/download/criu-4.2-device-restore-with-hook/criu"
	CriuGitID   = "eece9e851"
	RuncVersion = "v1.5.0"

	crioSock = "unix:///var/run/crio/crio.sock"
)

// crioReadyCheck polls crictl until CRI-O responds over gRPC or times out after 90 s.
// Socket existence alone is not sufficient — CRI-O can be mid-startup with a socket that
// refuses connections.
const crioReadyCheck = `for i in $(seq 1 30); do \
sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info >/dev/null 2>&1 \
  && echo "CRI-O ready" && break; \
echo "Waiting for CRI-O ($i/30)..."; sleep 3; \
done; \
sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info \
  || { sudo journalctl -xeu crio.service --no-pager -n 100 >&2; false; }`

// crioBuildSteps returns the ordered shell commands that build conmon and CRI-O from
// source, install them, and write all required runtime configuration.  The steps are
// identical for control-plane and worker nodes.
//
// Key fixes vs the previous version:
//   - sudo make install.systemd installs the source-built service file that points to
//     /usr/local/bin/crio, preventing the "two binaries fighting" problem where the apt
//     service file would start /usr/bin/crio instead of the custom binary.
//   - /usr/bin/crio is symlinked to /usr/local/bin/crio as a belt-and-suspenders fallback.
//   - conmon is symlinked to /usr/libexec/crio/conmon, which is CRI-O's hard-coded search
//     path when it cannot find conmon via PATH.
//   - crictl.yaml is written so crictl works without --runtime-endpoint.
//   - CRI-O drop-in uses only [crio.runtime] — [crio.image] has no listen key and was
//     silently breaking drop-in parsing.
//   - CNI directories are created before CRI-O starts so the CNI plugin probe succeeds.
func crioBuildSteps(clean string) []string {
	return []string{
		// ── Build deps ──────────────────────────────────────────────────────────────
		"sudo apt-get install -y build-essential libgpgme-dev gcc xmlto asciidoc " +
			"libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler " +
			"python3-protobuf uuid-dev libbsd-dev libnftables-dev libcap-dev libnl-3-dev " +
			"libnet1-dev libaio-dev libgnutls28-dev libdrm-dev --no-install-recommends",
		"sudo dpkg --configure -a",
		"sudo apt-get install -y git make pkg-config libassuan-dev libglib2.0-dev " +
			"libc6-dev libgpg-error-dev libseccomp-dev libsystemd-dev libselinux1-dev " +
			"libbtrfs-dev libudev-dev software-properties-common go-md2man runc crun",

		// ── Go 1.26.4 (golang-go from apt is ≤1.21; go.mod requires 1.26.4) ────────
		`GO_VER=1.26.4; \
curl -fsSL https://go.dev/dl/go${GO_VER}.linux-amd64.tar.gz -o /tmp/go.tar.gz && \
sudo rm -rf /usr/local/go && \
sudo tar -C /usr/local -xzf /tmp/go.tar.gz && \
rm -f /tmp/go.tar.gz && \
/usr/local/go/bin/go version`,

		// ── conmon from source ────────────────────────────────────────────────────────
		// CRI-O has a hard-coded search list that includes /usr/libexec/crio/conmon;
		// symlink the source-built binary there so CRI-O finds it without extra config.
		`sudo rm -rf /tmp/conmon && \
git clone https://github.com/containers/conmon /tmp/conmon && \
cd /tmp/conmon && \
PATH=/usr/local/go/bin:$PATH make && \
sudo make install && \
sudo mkdir -p /usr/libexec/crio && \
sudo ln -sf /usr/local/bin/conmon /usr/libexec/crio/conmon && \
rm -rf /tmp/conmon`,

		// ── CRI-O from source ─────────────────────────────────────────────────────────
		// make install        → /usr/local/bin/crio
		// make install.config → /etc/crio/ defaults
		// make install.systemd → /usr/local/lib/systemd/system/crio.service  (points to
		//                         /usr/local/bin/crio, not the apt binary at /usr/bin/crio)
		`sudo rm -rf /tmp/custom-crio && \
git clone https://github.com/vitu-mafeni/leehun-cri-o.git /tmp/custom-crio \
  -b 2026-02-03/support-restore-from-file && \
cd /tmp/custom-crio && \
PATH=/usr/local/go/bin:$PATH make && \
sudo make install && \
sudo make install.config && \
sudo make install.systemd 2>/dev/null || true && \
rm -rf /tmp/custom-crio`,

		// Belt-and-suspenders: if the apt service file (/lib/systemd/system/crio.service)
		// is used instead of the source-built one, ensure /usr/bin/crio also resolves to
		// the custom binary so systemd ExecStart=/usr/bin/crio runs the right thing.
		`sudo ln -sf /usr/local/bin/crio /usr/bin/crio`,
		`sudo ln -sf /usr/local/bin/crio-status /usr/bin/crio-status 2>/dev/null || true`,

		// ── crictl ────────────────────────────────────────────────────────────────────
		fmt.Sprintf(`curl -fsSL https://github.com/kubernetes-sigs/cri-tools/releases/download/v%s/crictl-v%s-linux-amd64.tar.gz \
  | sudo tar -C /usr/local/bin -xzf - crictl && \
sudo chmod 0755 /usr/local/bin/crictl && \
sudo ln -sf /usr/local/bin/crictl /usr/bin/crictl`, clean, clean),

		// ── CRI-O configuration ──────────────────────────────────────────────────────
		// crictl.yaml — without this crictl falls back to runtime detection which can
		// pick containerd or an empty socket.
		`printf 'runtime-endpoint: unix:///var/run/crio/crio.sock\n` +
			`image-endpoint: unix:///var/run/crio/crio.sock\n` +
			`timeout: 30\n` +
			`debug: false\n' | sudo tee /etc/crictl.yaml > /dev/null`,

		// Drop-in: socket path + conmon path.
		// Only [crio.runtime] has a listen key; [crio.image] does not — the old config
		// with [crio.image] listen = "..." was silently ignored or broke parsing.
		`sudo mkdir -p /etc/crio/crio.conf.d && \
printf '[crio.runtime]\nlisten = "/var/run/crio/crio.sock"\nconmon = "/usr/local/bin/conmon"\n' \
  | sudo tee /etc/crio/crio.conf.d/10-paths.conf > /dev/null`,

		// Container image pull policy.
		`sudo mkdir -p /etc/containers && \
printf '{"default":[{"type":"insecureAcceptAnything"}]}\n' \
  | sudo tee /etc/containers/policy.json > /dev/null`,

		// CNI directories must exist before CRI-O starts so the CNI plugin probe passes.
		`sudo mkdir -p /etc/cni/net.d /opt/cni/bin`,

		// CRIU configuration for checkpoint/restore support.
		// Remove any stale file first, then write fresh config.
		`sudo rm -f /etc/criu/runc.conf && \
sudo mkdir -p /etc/criu && \
printf 'tcp-close\nskip-in-flight\nlog-file /tmp/criu.log\nghost-limit 100M\nenable-external-masters\nexternal mnt[]\n' \
  | sudo tee /etc/criu/runc.conf > /dev/null`,

		// ── crun from source ─────────────────────────────────────────────────────────
		// The apt crun on Ubuntu 22.04 is ≈0.19 which predates OCI spec 1.0.2; CRI-O
		// 1.35 generates specs that old crun rejects with "unknown version specified".
		// Build the latest release from source to ensure compatibility.
		`sudo apt-get install -y autoconf automake libtool python3-dev libyajl-dev libjson-c-dev 2>/dev/null || true`,
		`sudo rm -rf /tmp/crun && \
git clone --depth=1 https://github.com/containers/crun /tmp/crun && \
cd /tmp/crun && \
./autogen.sh && \
./configure --disable-man-page && \
make -j$(nproc) && \
sudo make install && \
rm -rf /tmp/crun`,

		// CRI-O runtime drop-in: use crun as the default OCI runtime.
		// crun built from source installs to /usr/local/bin/crun.
		`sudo mkdir -p /etc/crio/crio.conf.d && \
printf '[crio]\n\n  [crio.runtime]\n    default_runtime = "crun"\n\n    [crio.runtime.runtimes]\n      [crio.runtime.runtimes.crun]\n        runtime_path = "/usr/local/bin/crun"\n        runtime_type = "oci"\n' \
  | sudo tee /etc/crio/crio.conf.d/999-runc.conf > /dev/null`,
	}
}

// ProvisionPhase groups a set of related shell commands under a human-readable name.
// InitializeControlPlane and JoinWorkerNode iterate over a []ProvisionPhase;
// passing startPhase > 0 skips already-completed phases so a failed run can
// resume from where it left off rather than restarting from scratch.
type ProvisionPhase struct {
	Name  string
	Steps []string
}

// runPhases executes each phase in order, skipping those with index < startPhase.
// After every successful phase it calls onPhaseComplete (if non-nil) with the
// completed phase index so the caller can persist progress.
func runPhases(client *sshhelper.Client, phases []ProvisionPhase, startPhase int, onPhaseComplete func(int)) error {
	for i, phase := range phases {
		if i < startPhase {
			log.Printf("[phase %d/%d] Skipping %q (already completed)", i, len(phases)-1, phase.Name)
			continue
		}
		log.Printf("[phase %d/%d] Running %q", i, len(phases)-1, phase.Name)
		for _, cmd := range phase.Steps {
			output, err := sshhelper.Run(client, cmd)
			if err != nil {
				return fmt.Errorf("phase %d (%s) command failed: %s\nOutput:\n%s", i, phase.Name, cmd, output)
			}
		}
		if onPhaseComplete != nil {
			onPhaseComplete(i)
		}
	}
	return nil
}

// cpPhase* constants name the control-plane provision phases.
// They are used by the controller to resume from the last completed phase on retry.
const (
	CPPhaseCleanup         = 0
	CPPhaseNFS             = 1
	CPPhaseSysSettings     = 2
	CPPhaseAPTRepos        = 3
	CPPhaseCRIOBuild       = 4
	CPPhaseCRIOStart       = 5
	CPPhaseK8sInstall      = 6
	CPPhaseKubeadmInit     = 7
	CPPhasePostInit        = 8
	CPPhaseCNI             = 9
	CPPhaseAddons          = 10
	CPPhaseArgoCDConfigure = 11
	CPPhaseNFSProvisioner  = 12
)

// WorkerPhase* constants name the worker-node provision phases.
const (
	WorkerPhaseCleanup     = 0
	WorkerPhaseSysSettings = 1
	WorkerPhaseAPTRepos    = 2
	WorkerPhaseCRIOBuild   = 3
	WorkerPhaseCRIOStart   = 4
	WorkerPhaseK8sInstall  = 5
	WorkerPhaseGPUCDI      = 6
	WorkerPhaseCRIORestart = 7
	WorkerPhaseJoin        = 8
)

func InitializeControlPlane(client *sshhelper.Client, cluster *infrav1.RemoteCluster, startPhase int, onPhaseComplete func(int)) (string, error) {
	log.Printf("Provisioning Kubernetes cluster with kubeadm on %s", cluster.Spec.Host)

	tunIP, err := GetTunIP(client)
	if err != nil {
		return "", fmt.Errorf("failed to get control plane wg0 IP: %w", err)
	}
	log.Printf("Control plane VPN IP: %s", tunIP)

	clean := strings.TrimPrefix(cluster.Spec.NodeInfo.SoftwareConfig.KubernetesVersion, "v")
	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid kubernetes version: %s", cluster.Spec.NodeInfo.SoftwareConfig.KubernetesVersion)
	}
	repoVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	kubeadmConfig := fmt.Sprintf(`
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: %s
  bindPort: 6443
nodeRegistration:
  criSocket: unix:///var/run/crio/crio.sock
  imagePullPolicy: IfNotPresent
  imagePullSerial: true
  taints:
    - effect: NoSchedule
      key: node-role.kubernetes.io/control-plane
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: "v%s"
clusterName: %s
networking:
  podSubnet: "10.244.0.0/16"
  serviceSubnet: "10.96.0.0/12"
  dnsDomain: cluster.local
controlPlaneEndpoint: ""
apiServer:
  extraArgs:
    - name: runtime-config
      value: "resource.k8s.io/v1beta1=true"
    - name: feature-gates
      value: "DynamicResourceAllocation=true,DRAConsumableCapacity=true"
controllerManager:
  extraArgs:
    - name: feature-gates
      value: "DynamicResourceAllocation=true,DRAConsumableCapacity=true"
scheduler:
  extraArgs:
    - name: feature-gates
      value: "DynamicResourceAllocation=true,DRAConsumableCapacity=true"
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
maxPods: 200
cgroupDriver: systemd
containerRuntimeEndpoint: unix:///var/run/crio/crio.sock
featureGates:
  DynamicResourceAllocation: true
  DRAConsumableCapacity: true
runtimeRequestTimeout: "15m"
---
apiVersion: kubeproxy.config.k8s.io/v1alpha1
kind: KubeProxyConfiguration
mode: ipvs
`, tunIP, clean, cluster.Spec.ClusterName)

	phases := []ProvisionPhase{
		// ── Phase 0: Cleanup ─────────────────────────────────────────────────────────
		// Only runs on a fresh start (startPhase == 0).  Skipped on retries so that
		// kubeadm reset does not undo work completed in earlier phases.
		{Name: "Cleanup", Steps: []string{
			"sudo systemctl stop kubelet 2>/dev/null || true",
			"sudo systemctl stop crio 2>/dev/null || true",
			"sudo kubeadm reset -f --cri-socket=unix:///var/run/crio/crio.sock 2>/dev/null || true",
			"sudo rm -rf /etc/cni/net.d 2>/dev/null || true",
		}},

		// ── Phase 1: NFS server ──────────────────────────────────────────────────────
		{Name: "NFS Server", Steps: []string{
			"sudo apt-get install -y nfs-kernel-server nfs-common",
			"sudo mkdir -p /srv/nfs/k8s",
			"sudo chown -R nobody:nogroup /srv/nfs/k8s",
			"sudo chmod 755 /srv/nfs/k8s",
			"echo '/srv/nfs/k8s *(rw,sync,no_subtree_check,no_root_squash)' | sudo tee /etc/exports",
			"sudo exportfs -ra",
			"sudo systemctl enable --now nfs-kernel-server",
		}},

		// ── Phase 2: System settings ─────────────────────────────────────────────────
		{Name: "System Settings", Steps: []string{
			"sudo swapoff -a",
			`sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab`,
			`echo -e "overlay\nbr_netfilter" | sudo tee /etc/modules-load.d/k8s.conf`,
			"sudo modprobe overlay",
			"sudo modprobe br_netfilter",
			`echo -e "net.bridge.bridge-nf-call-iptables=1\nnet.bridge.bridge-nf-call-ip6tables=1\nnet.ipv4.ip_forward=1" | sudo tee /etc/sysctl.d/k8s.conf`,
			"sudo sysctl --system",
		}},

		// ── Phase 3: APT repos ───────────────────────────────────────────────────────
		{Name: "APT Repos", Steps: []string{
			"sudo apt-get update",
			"sudo apt-get install -y ca-certificates software-properties-common curl gnupg apt-transport-https",
			"sudo install -m 0755 -d /etc/apt/keyrings",
			"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
			fmt.Sprintf(`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`, repoVersion),
			fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`, repoVersion),
			fmt.Sprintf(`sudo curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key | sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
			fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),
			"sudo apt-get update",
		}},

		// ── Phase 4: CRI-O build ─────────────────────────────────────────────────────
		{Name: "CRI-O Build", Steps: crioBuildSteps(clean)},

		// ── Phase 5: Start CRI-O ─────────────────────────────────────────────────────
		{Name: "CRI-O Start", Steps: []string{
			// Build crun from source unconditionally so a resume (skipping phase 4)
			// also gets a version compatible with CRI-O 1.35. apt crun on Ubuntu 22.04
			// is ≈0.19 which predates OCI spec 1.0.2 and fails with "unknown version".
			`sudo apt-get install -y autoconf automake libtool python3-dev libyajl-dev libjson-c-dev 2>/dev/null || true`,
			`sudo rm -rf /tmp/crun && \
git clone --depth=1 https://github.com/containers/crun /tmp/crun && \
cd /tmp/crun && \
./autogen.sh && \
./configure --disable-man-page && \
make -j$(nproc) && \
sudo make install && \
rm -rf /tmp/crun`,
			// Rewrite the runtime drop-in to use the source-built crun at /usr/local/bin/crun.
			`sudo mkdir -p /etc/crio/crio.conf.d && \
printf '[crio]\n\n  [crio.runtime]\n    default_runtime = "crun"\n\n    [crio.runtime.runtimes]\n      [crio.runtime.runtimes.crun]\n        runtime_path = "/usr/local/bin/crun"\n        runtime_type = "oci"\n' \
  | sudo tee /etc/crio/crio.conf.d/999-runc.conf > /dev/null`,
			// Stop any running CRI-O and kill stale conmon/crun child processes that
			// may have survived from the previous cluster. Leftover processes hold
			// open the socket or container storage, causing the new binary to fail
			// on start even after a clean wipe.
			"sudo systemctl stop crio 2>/dev/null || true",
			"sudo killall -9 crio conmon crun 2>/dev/null || true",
			// Remove the stale socket and run-time state directory so the new
			// binary starts with a clean socket path.
			"sudo rm -rf /run/crio /var/run/crio",
			// Wipe container storage (overlay mounts, image cache) with CRI-O
			// stopped so no mounts are held open.
			"sudo umount -l /var/lib/containers/storage/overlay/*/merged 2>/dev/null || true",
			"sudo crio wipe -f 2>/dev/null || true",
			"sudo rm -rf /var/lib/crio 2>/dev/null || true",
			"sudo systemctl daemon-reload",
			"sudo systemctl enable crio",
			// Use restart (not start) so it succeeds whether or not a stale unit
			// is already in failed/active state.
			`sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager -n 60 >&2; false; }`,
			crioReadyCheck,
		}},

		// ── Phase 6: Kubernetes components ───────────────────────────────────────────
		{Name: "K8s Install", Steps: []string{
			fmt.Sprintf("sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages", clean, clean, clean),
			"sudo apt-mark hold kubelet kubeadm kubectl",
			"sudo systemctl enable kubelet",
			"sudo systemctl stop kubelet 2>/dev/null || true",
			fmt.Sprintf(`printf 'KUBELET_EXTRA_ARGS=--node-ip=%s\n' | sudo tee /etc/default/kubelet > /dev/null`, tunIP),
			`sudo mkdir -p /etc/systemd/system/kubelet.service.d && \
printf '[Unit]\nAfter=crio.service\nRequires=crio.service\n' \
  | sudo tee /etc/systemd/system/kubelet.service.d/10-crio.conf > /dev/null`,
			"sudo systemctl daemon-reload",
		}},

		// ── Phase 7: kubeadm init ─────────────────────────────────────────────────────
		// kubeadm init is guarded by `test -f admin.conf` so it is safe to retry.
		{Name: "kubeadm Init", Steps: []string{
			`sudo mkdir -p /var/lib/kubelet /etc/containers`,
			fmt.Sprintf("cat <<'EOF' | sudo tee /tmp/kubeadm-config.yaml\n%s\nEOF", kubeadmConfig),
			`sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info \
  || { sudo systemctl restart crio && sleep 5 && \
       sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info \
       || { sudo journalctl -xeu crio.service --no-pager >&2; false; }; }`,
			`test -f /etc/kubernetes/admin.conf || ( \
sudo kubeadm init --config /tmp/kubeadm-config.yaml; RC=$?; \
if [ $RC -ne 0 ]; then \
  echo "=== crictl ps -a ===" >&2; \
  sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock ps -a >&2 2>&1 || true; \
  CIDS=$(sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock ps -a -q 2>/dev/null | head -6); \
  for CID in $CIDS; do \
    echo "=== container logs: $CID ===" >&2; \
    sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock logs --tail=50 "$CID" >&2 2>&1 || true; \
  done; \
  echo "=== kubelet journal ===" >&2; \
  sudo journalctl -xeu kubelet --no-pager -n 80 >&2 2>&1 || true; \
fi; \
exit $RC )`,
			"mkdir -p $HOME/.kube",
			"sudo cp -f /etc/kubernetes/admin.conf $HOME/.kube/config",
			"sudo chown $(id -u):$(id -g) $HOME/.kube/config",
		}},

		// ── Phase 8: Post-init ───────────────────────────────────────────────────────
		{Name: "Post-Init", Steps: []string{
			"kubectl taint nodes --all node-role.kubernetes.io/control-plane- || kubectl taint nodes --all node-role.kubernetes.io/master- || true",
			fmt.Sprintf("kubectl label nodes --all hardware-type=%s --overwrite", cluster.Spec.NodeInfo.HardwareType),
		}},

		// ── Phase 9: CNI ─────────────────────────────────────────────────────────────
		{Name: "CNI", Steps: []string{
			"kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml",
			`kubectl -n kube-flannel patch daemonset kube-flannel-ds --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--iface=wg0"}]'`,
			"kubectl rollout status daemonset kube-flannel-ds -n kube-flannel --timeout=120s",
			"kubectl wait --for=condition=Ready nodes --all --timeout=180s",
		}},

		// ── Phase 10: Addons ─────────────────────────────────────────────────────────
		{Name: "Addons", Steps: []string{
			"kubectl create namespace argocd || true",
			"kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml",
			"rm -rf /tmp/catalog",
			"git clone https://github.com/vitu-mafeni/catalog.git /tmp/catalog",
			"kubectl apply -f /tmp/catalog/nephio/optional/flux-helm-controllers",
			`cat <<EOF | kubectl apply -f -
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: nvidia
handler: nvidia
EOF`,
			"rm -rf /tmp/remote-cluster-provisioner",
			"git clone https://github.com/vitu-mafeni/remote-cluster-provisioner.git /tmp/remote-cluster-provisioner",
			"kubectl apply -f /tmp/remote-cluster-provisioner/config/crd/bases/",
			// Two-pass apply: pass 1 seeds namespaces and CRDs so that pass 2
			// does not hit "namespace not found" for resources applied before their
			// namespace YAML in alphabetical directory order.
			// Only sandbox/cert-manager is applied from the sandbox distro tree;
			// gitea, metallb-sandbox-config, network, and repository dirs are skipped.
			`kubectl apply -f /tmp/catalog/distros/sandbox/cert-manager/`,
			// Wait for the cert-manager webhook to be ready before pass 2 so that
			// ClusterIssuer creation does not fail with "connection refused".
			`kubectl -n cert-manager wait --for=condition=Available deployment/cert-manager-webhook --timeout=180s 2>/dev/null || true`,
			`sleep 5`,
			// Pass 2: real apply. Capture output, print it, then fail only on
			// actual server errors. Client-side "no matches for kind" errors for
			// GCP-specific types (ApplyReplacements, RootSync, etc.) are expected
			// and harmless — they do not produce "Error from server" lines.
			// `OUT=$(kubectl apply -Rf /tmp/catalog/distros/sandbox/cert-manager/ 2>&1); echo "$OUT"; ERRS=$(echo "$OUT" | grep "^Error from server" || true); [ -z "$ERRS" ]`,
		}},
	}

	if err := runPhases(client, phases, startPhase, onPhaseComplete); err != nil {
		return "", err
	}

	// ── Phase 11: ArgoCD configure ────────────────────────────────────────────────
	if startPhase <= CPPhaseArgoCDConfigure {
		if err := argocd.ConfigureArgoCD(client, cluster); err != nil {
			return "", fmt.Errorf("ArgoCD configuration failed: %w", err)
		}
		if onPhaseComplete != nil {
			onPhaseComplete(CPPhaseArgoCDConfigure)
		}
	}

	// ── Phase 12: NFS provisioner ─────────────────────────────────────────────────
	if startPhase <= CPPhaseNFSProvisioner {
		if err := installNFSProvisioner(client, tunIP); err != nil {
			return "", fmt.Errorf("NFS provisioner installation failed: %w", err)
		}
		if onPhaseComplete != nil {
			onPhaseComplete(CPPhaseNFSProvisioner)
		}
	}

	joinCmd, err := getJoinCommand(client)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve join command: %w", err)
	}

	log.Print("Control plane ready. Join command is available for worker nodes.")
	return joinCmd, nil
}

// installNFSProvisioner patches the NFS provisioner HelmRelease files in the catalog
// with the control-plane NFS server address and applies them to the cluster.
// Prerequisites: /tmp/catalog already cloned, FluxCD controllers deployed, kubectl configured.
//
// Both HelmRelease files in the provisioner directory are updated:
//   - nfs.server        → nfsServerIP (the control-plane WireGuard IP)
//   - nfs.path          → /srv/nfs/k8s
//   - storageClass.defaultClass → true  (replaces false, or inserts if absent)
func installNFSProvisioner(client *sshhelper.Client, nfsServerIP string) error {
	log.Printf("Installing NFS provisioner (server: %s)", nfsServerIP)

	nfsDir := "/tmp/catalog/workloads/ml-platform/nfs-provisioner"

	steps := []string{
		// HelmRelease resources target the storage namespace.
		"kubectl create namespace storage 2>/dev/null || true",

		// Patch every YAML file in the provisioner directory in-place.
		//
		// Escaping path through Go raw string → shell double-quote → sed:
		//   \\  in raw string = two literal backslashes
		//       shell double-quote collapses \\ → \
		//       sed receives single \  (used as sed command prefix, e.g. a\ or \| address)
		//   \\n in raw string = \\ + n
		//       shell collapses \\ → \, n stays → sed receives \n (newline in append text)
		fmt.Sprintf(`for f in %s/*.yaml %s/*.yml; do \
  [ -f "$f" ] || continue; \
  sed -i "s/server: .*/server: %s/" "$f"; \
  sed -i "s|path: .*|path: /srv/nfs/k8s|" "$f"; \
  sed -i "s/defaultClass: false/defaultClass: true/" "$f"; \
  grep -q "defaultClass" "$f" || \
    sed -i "\|path: /srv/nfs/k8s|a\\    storageClass:\\n      defaultClass: true" "$f"; \
done`, nfsDir, nfsDir, nfsServerIP),

		fmt.Sprintf("kubectl apply -f %s/", nfsDir),
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("nfs provisioner install failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	log.Printf("NFS provisioner installed successfully")
	return nil
}

func getJoinCommand(client *sshhelper.Client) (string, error) {
	output, err := sshhelper.Run(client, "sudo kubeadm token create --print-join-command --ttl 0")
	if err != nil {
		return "", fmt.Errorf("kubeadm token create failed: %w\nOutput: %s", err, output)
	}
	joinCmd := strings.TrimSpace(output)
	if joinCmd == "" {
		return "", fmt.Errorf("kubeadm returned an empty join command")
	}
	return joinCmd, nil
}

func JoinWorkerNode(client *sshhelper.Client, cpClient *sshhelper.Client, cluster *infrav1.RemoteCluster, joinCmd string, clusterParent *infrav1.RemoteCluster, startPhase int, onPhaseComplete func(int)) (error, string) {
	log.Printf("Joining worker node %s to cluster %s", cluster.Spec.Host, cluster.Spec.ClusterName)

	if joinCmd == "" {
		return fmt.Errorf("joinCmd must not be empty"), ""
	}
	if clusterParent.Spec.NodeInfo.HardwareType == "" {
		return fmt.Errorf("clusterParent.Spec.NodeInfo.HardwareType must not be empty"), ""
	}

	nodeIP, err := GetTunIP(client)
	if err != nil {
		return fmt.Errorf("failed to get worker wg0 IP: %w", err), ""
	}
	log.Printf("Worker VPN IP: %s", nodeIP)

	clean := strings.TrimPrefix(clusterParent.Spec.NodeInfo.SoftwareConfig.KubernetesVersion, "v")
	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid kubernetes version: %s", clusterParent.Spec.NodeInfo.SoftwareConfig.KubernetesVersion), ""
	}
	repoVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	// GPU CDI steps are non-empty only for GPU nodes; the phase always exists so
	// phase indices remain stable across node types.
	var gpuCDISteps []string
	if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		gpuCDISteps = []string{
			"sudo mkdir -p /etc/cdi /var/run/cdi /etc/crio/crio.conf.d",
			`test -f /etc/crio/crio.conf.d/99-cdi.conf || \
printf '[crio.runtime]\nenable_cdi = true\ncdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]\n' \
  | sudo tee /etc/crio/crio.conf.d/99-cdi.conf > /dev/null`,
		}
	}

	phases := []ProvisionPhase{
		// ── Phase 0: Cleanup ─────────────────────────────────────────────────────────
		// Only runs on a fresh start; skipped on retries so kubeadm reset does not
		// undo work completed in earlier phases.
		{Name: "Cleanup", Steps: []string{
			"sudo systemctl stop kubelet 2>/dev/null || true",
			"sudo systemctl stop crio 2>/dev/null || true",
			"sudo kubeadm reset -f --cri-socket=unix:///var/run/crio/crio.sock 2>/dev/null || true",
			"sudo rm -rf /etc/cni/net.d 2>/dev/null || true",
		}},

		// ── Phase 1: System settings ─────────────────────────────────────────────────
		{Name: "System Settings", Steps: []string{
			"sudo swapoff -a",
			`sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab`,
			`echo -e "overlay\nbr_netfilter" | sudo tee /etc/modules-load.d/k8s.conf`,
			"sudo modprobe overlay",
			"sudo modprobe br_netfilter",
			`echo -e "net.bridge.bridge-nf-call-iptables=1\nnet.bridge.bridge-nf-call-ip6tables=1\nnet.ipv4.ip_forward=1" | sudo tee /etc/sysctl.d/k8s.conf`,
			"sudo sysctl --system",
		}},

		// ── Phase 2: APT repos ───────────────────────────────────────────────────────
		{Name: "APT Repos", Steps: []string{
			"sudo apt-get update",
			"sudo apt-get install -y ca-certificates curl gnupg apt-transport-https",
			// nfs-common: kernel NFS client + mount.nfs so the node can mount NFS volumes.
			"sudo apt-get install -y nfs-common",
			"sudo install -m 0755 -d /etc/apt/keyrings",
			"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
			fmt.Sprintf(`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`, repoVersion),
			fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`, repoVersion),
			fmt.Sprintf(`sudo curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key | sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
			fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),
			"sudo apt-get update",
		}},

		// ── Phase 3: CRI-O build ─────────────────────────────────────────────────────
		{Name: "CRI-O Build", Steps: crioBuildSteps(clean)},

		// ── Phase 4: Start CRI-O ─────────────────────────────────────────────────────
		{Name: "CRI-O Start", Steps: []string{
			// Build crun from source (handles resume that skipped phase 3).
			`sudo apt-get install -y autoconf automake libtool python3-dev libyajl-dev libjson-c-dev 2>/dev/null || true`,
			`sudo rm -rf /tmp/crun && \
git clone --depth=1 https://github.com/containers/crun /tmp/crun && \
cd /tmp/crun && \
./autogen.sh && \
./configure --disable-man-page && \
make -j$(nproc) && \
sudo make install && \
rm -rf /tmp/crun`,
			`sudo mkdir -p /etc/crio/crio.conf.d && \
printf '[crio]\n\n  [crio.runtime]\n    default_runtime = "crun"\n\n    [crio.runtime.runtimes]\n      [crio.runtime.runtimes.crun]\n        runtime_path = "/usr/local/bin/crun"\n        runtime_type = "oci"\n' \
  | sudo tee /etc/crio/crio.conf.d/999-runc.conf > /dev/null`,
			"sudo systemctl stop crio 2>/dev/null || true",
			"sudo killall -9 crio conmon crun 2>/dev/null || true",
			"sudo rm -rf /run/crio /var/run/crio",
			"sudo umount -l /var/lib/containers/storage/overlay/*/merged 2>/dev/null || true",
			"sudo crio wipe -f 2>/dev/null || true",
			"sudo rm -rf /var/lib/crio 2>/dev/null || true",
			"sudo systemctl daemon-reload",
			"sudo systemctl enable crio",
			`sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager -n 60 >&2; false; }`,
			crioReadyCheck,
		}},

		// ── Phase 5: Kubernetes components ───────────────────────────────────────────
		{Name: "K8s Install", Steps: []string{
			"sudo apt-get update",
			fmt.Sprintf("sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages --allow-downgrades", clean, clean, clean),
			"sudo apt-mark hold kubelet kubeadm kubectl",
			"sudo systemctl enable kubelet",
			"sudo systemctl stop kubelet 2>/dev/null || true",
			fmt.Sprintf(`printf 'KUBELET_EXTRA_ARGS=--node-ip=%s\n' | sudo tee /etc/default/kubelet > /dev/null`, nodeIP),
			`sudo mkdir -p /etc/systemd/system/kubelet.service.d && \
printf '[Unit]\nAfter=crio.service\nRequires=crio.service\n' \
  | sudo tee /etc/systemd/system/kubelet.service.d/10-crio.conf > /dev/null`,
			"sudo systemctl daemon-reload",
			`sudo mkdir -p /var/lib/kubelet /etc/containers`,
		}},

		// ── Phase 6: GPU CDI config (empty for non-GPU nodes) ───────────────────────
		{Name: "GPU CDI", Steps: gpuCDISteps},

		// ── Phase 7: Final CRI-O restart (picks up CDI config) ───────────────────────
		{Name: "CRI-O Restart", Steps: []string{
			`sudo systemctl daemon-reload && \
sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; }`,
			crioReadyCheck,
		}},

		// ── Phase 8: kubeadm join ─────────────────────────────────────────────────────
		{Name: "kubeadm Join", Steps: []string{
			fmt.Sprintf("sudo %s --cri-socket=unix:///var/run/crio/crio.sock", joinCmd),
		}},
	}

	if err := runPhases(client, phases, startPhase, onPhaseComplete); err != nil {
		return err, ""
	}

	// Resolve the node name by matching the worker's VPN IP in the node address table.
	// kubelet takes a few seconds to register after kubeadm join; retry for up to 90 s.
	var rawNodeOutput string
	nodeName := ""
	for attempt := 0; attempt < 45; attempt++ {
		var queryErr error
		rawNodeOutput, queryErr = sshhelper.Run(cpClient,
			`kubectl get nodes -o json | jq -r '.items[] | .metadata.name as $n | .status.addresses[].address | [$n, .] | @tsv'`)
		if queryErr != nil {
			return fmt.Errorf("failed to list nodes: %w\nOutput:\n%s", queryErr, rawNodeOutput), ""
		}
		for _, line := range strings.Split(strings.TrimSpace(rawNodeOutput), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[1] == nodeIP {
				nodeName = fields[0]
				break
			}
		}
		if nodeName != "" {
			break
		}
		log.Printf("Worker node %s not yet visible in kubectl get nodes (attempt %d/45), retrying in 2s...", nodeIP, attempt+1)
		time.Sleep(2 * time.Second)
	}
	if nodeName == "" {
		return fmt.Errorf("failed to resolve node name for host %s vpn ip: %s — address table:\n%s", cluster.Spec.Host, nodeIP, rawNodeOutput), ""
	}

	labelAndTaintCmd := fmt.Sprintf(
		"kubectl label node %s hardware-type=%s gpu=on --overwrite && kubectl taint node %s hardware-type=gpu:PreferNoSchedule --overwrite",
		nodeName, cluster.Spec.NodeInfo.HardwareType, nodeName,
	)
	if output, err := sshhelper.Run(cpClient, labelAndTaintCmd); err != nil {
		return fmt.Errorf("failed to label/taint worker node %s: %w\nOutput:\n%s", nodeName, err, output), ""
	}

	log.Printf("Worker node %s successfully joined cluster %s", cluster.Spec.Host, cluster.Spec.ClusterName)
	return nil, nodeIP
}

// GetTunIP returns the IPv4 address of the wg0 interface on the remote host.
func GetTunIP(client *sshhelper.Client) (string, error) {
	output, err := sshhelper.Run(client, `ip -4 addr show wg0 | grep -oP '(?<=inet )\d+\.\d+\.\d+\.\d+'`)
	if err != nil {
		return "", fmt.Errorf("ip addr show wg0 failed: %w\nOutput: %s", err, output)
	}
	ip := strings.TrimSpace(output)
	if ip == "" {
		return "", fmt.Errorf("wg0 has no IPv4 address — is the VPN connected?")
	}
	return ip, nil
}

// InstallNvidiaContainerToolkit installs the NVIDIA container toolkit on a GPU node
// and configures CRI-O to use it. This should be called after JoinWorkerNode for
// nodes where cluster.Spec.NodeInfo.HardwareType == "gpu".
func InstallNvidiaContainerToolkit1(client *sshhelper.Client, cluster *infrav1.RemoteCluster, clusterParent *infrav1.RemoteCluster) error {
	if !strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		log.Printf("Skipping NVIDIA container toolkit install — node %s is not a GPU node", cluster.Spec.Host)
		return nil
	}

	log.Printf("Installing NVIDIA container toolkit on GPU node %s", cluster.Spec.Host)

	nvidiaToolkitVersion := clusterParent.Spec.NodeInfo.SoftwareConfig.NvidiaContainerToolkitVersion

	steps := []string{
		"sudo apt-get update",
		"sudo apt-get install -y --no-install-recommends ca-certificates curl gnupg2",
		"curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg",
		`curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list`,
		"sudo sed -i -e '/experimental/ s/^#//g' /etc/apt/sources.list.d/nvidia-container-toolkit.list",
		"sudo apt-get update",
		fmt.Sprintf(`sudo apt-get install --allow-downgrades -y \
nvidia-container-toolkit=%s \
nvidia-container-toolkit-base=%s \
libnvidia-container-tools=%s \
libnvidia-container1=%s`,
			nvidiaToolkitVersion, nvidiaToolkitVersion, nvidiaToolkitVersion, nvidiaToolkitVersion),
		"sudo nvidia-ctk runtime configure --runtime=crio",
		"sudo systemctl restart crio",
		"sudo nvidia-ctk --version",
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("nvidia toolkit install failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	log.Printf("NVIDIA container toolkit installed successfully on %s", cluster.Spec.Host)
	return nil
}

// InstallNvidiaDrivers11 installs the NVIDIA drivers on a GPU node.
// A reboot is typically required after driver installation for the drivers to take effect.
func InstallNvidiaDrivers11(client *sshhelper.Client, cluster *infrav1.RemoteCluster, clusterParent *infrav1.RemoteCluster) error {
	log.Printf("Installing NVIDIA drivers on GPU node %s", cluster.Spec.Host)

	nvidiaDriverVersion := clusterParent.Spec.NodeInfo.SoftwareConfig.NvidiaDriverVersion
	nvidiaToolkitVersion := clusterParent.Spec.NodeInfo.SoftwareConfig.NvidiaContainerToolkitVersion

	steps := []string{
		"sudo apt-get update",
		"sudo apt-get install -y --no-install-recommends ca-certificates curl gnupg2",
		"curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg",
		`curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list`,
		"sudo sed -i -e '/experimental/ s/^#//g' /etc/apt/sources.list.d/nvidia-container-toolkit.list",
		"sudo apt-get update",
		fmt.Sprintf(`sudo apt-get install --allow-downgrades -y \
nvidia-container-toolkit=%s \
nvidia-container-toolkit-base=%s \
libnvidia-container-tools=%s \
libnvidia-container1=%s`,
			nvidiaToolkitVersion, nvidiaToolkitVersion, nvidiaToolkitVersion, nvidiaToolkitVersion),
		"sudo nvidia-ctk runtime configure --runtime=crio",
		"sudo systemctl restart crio",
		"sudo ubuntu-drivers list --gpgpu || true",
		"sudo apt-get install -y linux-headers-$(uname -r) linux-headers-generic",
		fmt.Sprintf("sudo ubuntu-drivers install nvidia:%s", nvidiaDriverVersion),
		fmt.Sprintf("sudo ubuntu-drivers install --gpgpu nvidia:%s-server", nvidiaDriverVersion),
		fmt.Sprintf("sudo apt-get install -y nvidia-dkms-%s-server", nvidiaDriverVersion),
		fmt.Sprintf("sudo apt-get install -y nvidia-utils-%s-server", nvidiaDriverVersion),
		"nvidia-smi || true",
		"sudo nvidia-ctk --version",
		`sudo rm -rf /etc/crio/crio.conf.d`,
		`sudo mkdir -p /etc/crio/crio.conf.d`,
		// Restore paths drop-in — rm -rf wiped it along with the nvidia config.
		`printf '[crio.runtime]\nlisten = "/var/run/crio/crio.sock"\nconmon = "/usr/local/bin/conmon"\n' \
  | sudo tee /etc/crio/crio.conf.d/10-paths.conf > /dev/null`,
		// Restore crun as the default OCI runtime.
		`printf '[crio]\n\n  [crio.runtime]\n    default_runtime = "crun"\n\n    [crio.runtime.runtimes]\n      [crio.runtime.runtimes.crun]\n        runtime_path = "/usr/local/bin/crun"\n        runtime_type = "oci"\n' \
  | sudo tee /etc/crio/crio.conf.d/999-runc.conf > /dev/null`,
		// Recreate 99-nvidia.conf — rm -rf wiped the one nvidia-ctk wrote earlier.
		// Without this the nvidia runtime handler is absent from CRI-O's runtime map
		// and pods with runtimeClassName=nvidia fail with "failed to find runtime handler nvidia".
		// Guard: install the toolkit if nvidia-ctk is not in PATH (e.g. retry after partial failure).
		`command -v nvidia-ctk >/dev/null || sudo apt-get install -y nvidia-container-toolkit`,
		`sudo nvidia-ctk runtime configure --runtime=crio`,
		`sudo sed -i '/monitor_path/d' /etc/crio/crio.conf.d/99-nvidia.conf 2>/dev/null || true`,
		`sudo ln -sf /usr/libexec/crio/conmon /usr/local/bin/conmon 2>/dev/null || true`,
		"sudo systemctl restart crio",
		`sudo crictl info | python3 -m json.tool | grep '"DefaultRuntime"' || true`,
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("nvidia driver install failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	log.Printf("NVIDIA drivers installed on %s — a reboot is required for drivers to take effect", cluster.Spec.Host)
	return nil
}

// GenerateCDI generates the CDI spec for the NVIDIA GPUs on the node.
// Must be called after the node has rebooted post driver installation.
// func GenerateCDI(client *sshhelper.Client) error {
// 	steps := []string{
// 		"sudo mkdir -p /etc/cdi",
// 		"sudo nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml",
// 	}
// 	for _, cmd := range steps {
// 		output, err := sshhelper.Run(client, cmd)
// 		if err != nil {
// 			return fmt.Errorf("cdi generate failed: %s\nOutput:\n%s", cmd, output)
// 		}
// 	}
// 	return nil
// }
