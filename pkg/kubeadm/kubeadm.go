package kubeadm

import (
	"fmt"
	"log"
	"strings"
	"time"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1" // Add the correct import path for infrav1
	"dcn.ssu.ac.kr/infra/pkg/argocd"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

const (
	CrioAsset   = "https://github.com/vitu-mafeni/leehun-cri-o/releases/download/crio-1.35.0-restore-from-file/crio"
	CrioCommit  = "a0e6cb3d7f0ca8e9f31131daa17570082e716393"
	CriuAsset   = "https://github.com/vitu-mafeni/leehun-criu/releases/download/criu-4.2-device-restore-with-hook/criu"
	CriuGitID   = "eece9e851"
	RuncVersion = "v1.5.0"
)

func InitializeControlPlane(client *sshhelper.Client, cluster *infrav1.RemoteCluster) (string, error) {
	log.Printf("Provisioning Kubernetes cluster with kubeadm on %s", cluster.Spec.Host)

	// Resolve the control plane's VPN IP from wg0 first — needed for kubeadmConfig and kubelet args.
	tunIP, err := GetTunIP(client)
	if err != nil {
		return "", fmt.Errorf("failed to get control plane wg0 IP: %w", err)
	}
	log.Printf("Control plane VPN IP: %s", tunIP)

	clean := strings.TrimPrefix(cluster.Spec.NodeInfo.SoftwareConfig.KubernetesVersion, "v")

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

	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid kubernetes version: %s", cluster.Spec.NodeInfo.SoftwareConfig.KubernetesVersion)
	}

	repoVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	steps := []string{
		// Stop services; leave CRI-O storage intact so the custom binary can read its own metadata.
		// Wiping /var/lib/containers/storage causes "image not known" desync and
		// deleting /run/crio causes a directory-recreation race that produces "permission denied"
		// on the socket — kubeadm reset handles the Kubernetes-side state.
		"sudo systemctl stop kubelet 2>/dev/null || true",
		"sudo systemctl stop crio 2>/dev/null || true",
		"sudo kubeadm reset -f --cri-socket=unix:///var/run/crio/crio.sock 2>/dev/null || true",
		// "sudo rm -rf /etc/cni/net.d 2>/dev/null || true",

		"sudo apt-get install -y nfs-kernel-server nfs-common",
		"sudo mkdir -p /srv/nfs/k8s",
		"sudo chown -R nobody:nogroup /srv/nfs/k8s",
		"sudo chmod 755 /srv/nfs/k8s",
		"echo '/srv/nfs/k8s *(rw,sync,no_subtree_check,no_root_squash)' | sudo tee /etc/exports",
		"sudo exportfs -ra",
		"sudo systemctl enable nfs-kernel-server",
		"sudo systemctl restart nfs-kernel-server",
		"sudo swapoff -a",
		`sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab`,
		`echo -e "overlay\nbr_netfilter" | sudo tee /etc/modules-load.d/k8s.conf`,
		"sudo modprobe overlay",
		"sudo modprobe br_netfilter",
		`echo -e "net.bridge.bridge-nf-call-iptables=1\nnet.bridge.bridge-nf-call-ip6tables=1\nnet.ipv4.ip_forward=1" | sudo tee /etc/sysctl.d/k8s.conf`,
		"sudo sysctl --system",
		"sudo apt-get update",
		"sudo apt-get install -y ca-certificates software-properties-common curl gnupg apt-transport-https",
		"sudo install -m 0755 -d /etc/apt/keyrings",

		// add repositories
		"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
		"sudo mkdir -p /etc/apt/keyrings",
		fmt.Sprintf(`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`, repoVersion),

		fmt.Sprintf(`sudo curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key |     sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),

		// install dependencies packages and custom cri-o
		"sudo apt-get install -y build-essential libgpgme-dev pkg-config gcc xmlto build-essential asciidoc libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf pkg-config uuid-dev libbsd-dev libnftables-dev libcap-dev libnl-3-dev libnet1-dev libaio-dev libgnutls28-dev libdrm-dev --no-install-recommends",
		"sudo dpkg --configure -a",
		"sudo apt-get install -y     git     gcc     make     pkg-config     libassuan-dev     libglib2.0-dev     libc6-dev     libgpgme-dev     libgpg-error-dev     libseccomp-dev     libsystemd-dev     libselinux1-dev     libbtrfs-dev     libudev-dev     software-properties-common     go-md2man     runc     crun",

		// go.mod requires go 1.26.4 — golang-go from apt is too old (≤1.21).
		// Download the exact version from golang.org and install to /usr/local/go.
		`GO_VER=1.26.4; \
		curl -fsSL https://go.dev/dl/go${GO_VER}.linux-amd64.tar.gz -o /tmp/go.tar.gz && \
		sudo rm -rf /usr/local/go && \
		sudo tar -C /usr/local -xzf /tmp/go.tar.gz && \
		rm -f /tmp/go.tar.gz && \
		/usr/local/go/bin/go version`,

		"sudo rm -rf /tmp/conmon && git clone https://github.com/containers/conmon /tmp/conmon && cd /tmp/conmon && PATH=/usr/local/go/bin:$PATH make && sudo make install && cd - && rm -rf /tmp/conmon",

		// Each SSH step runs in its own shell — cd does not persist, so clone+build must
		// be one chained command executed inside the repo directory.
		// PATH must include /usr/local/go/bin so that make finds the correct Go version.
		`sudo rm -rf /tmp/custom-crio && \
		git clone https://github.com/vitu-mafeni/leehun-cri-o.git /tmp/custom-crio -b 2026-02-03/support-restore-from-file && \
		cd /tmp/custom-crio && \
		PATH=/usr/local/go/bin:$PATH make && \
		sudo make install && \
		sudo make install.config && \
		rm -rf /tmp/custom-crio`,

		`sudo mkdir -p /etc/crio/crio.conf.d && \
		echo '[crio.runtime]
		listen = "/var/run/crio/crio.sock"
		stream_address = "127.0.0.1"
		[crio.image]
		listen = "/var/run/crio/crio.sock"' | sudo tee /etc/crio/crio.conf.d/00-sock-path.conf`,

		fmt.Sprintf(`curl -fsSL https://github.com/kubernetes-sigs/cri-tools/releases/download/v%s/crictl-v%s-linux-amd64.tar.gz | sudo tar -C /usr/local/bin -xzf - crictl && \
sudo chmod 0755 /usr/local/bin/crictl && \
sudo ln -sf /usr/local/bin/crictl /usr/bin/crictl`, clean, clean),

		`sudo tee /etc/crictl.yaml >/dev/null <<EOF
		runtime-endpoint: unix:///var/run/crio/crio.sock
		image-endpoint: unix:///var/run/crio/crio.sock
		timeout: 10
		debug: false
		EOF`,

		"sudo systemctl daemon-reload",
		"sudo systemctl enable crio",
		"sudo systemctl start crio",
		"sudo systemctl status crio --no-pager || true",

		// repoVersion MUST be "1.35" so the packaged conmon/runc/config match the custom binary
		// "sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg",
		// fmt.Sprintf(`curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
		// | gpg --dearmor | sudo tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),

		// fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
		// https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
		// | sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),

		"sudo apt-get update",

		"runc --version || true",

		"sudo apt-get update",
		fmt.Sprintf("sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages", clean, clean, clean),
		"sudo apt-mark hold kubelet kubeadm kubectl",
		"sudo systemctl enable kubelet",
		"sudo systemctl daemon-reload",
		// Set kubelet node IP to VPN IP before init.
		fmt.Sprintf(`echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' | sudo tee /etc/default/kubelet`, tunIP),
		"sudo systemctl daemon-reload",
		`sudo mkdir /var/lib/kubelet/plugins/kubernetes.io/empty-dir -f || true && \
		sudo mkdir -p /etc/containers && \
		echo '{"default":[{"type":"insecureAcceptAnything"}]}' \
		| sudo tee /etc/containers/policy.json > /dev/null`,
		fmt.Sprintf("cat <<'EOF' | sudo tee /tmp/kubeadm-config.yaml\n%s\nEOF", kubeadmConfig),
		// apt-get install kubelet may have auto-started kubelet; stop it so kubeadm init controls the lifecycle.
		"sudo systemctl stop kubelet 2>/dev/null || true",
		// One final restart to pick up all config (node-ip, containers policy, etc.) before kubeadm init.
		// No restart after init — that would lose image metadata populated by kubeadm.
		`sudo systemctl daemon-reload && sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; }`,
		// Wait until CRI-O actually responds — socket existence is not enough.
		`for i in $(seq 1 30); do \
		sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info >/dev/null 2>&1 && echo "CRI-O ready" && break; \
		echo "Waiting for CRI-O ($i/30)..."; sleep 3; \
		done; \
		sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info || { sudo journalctl -xeu crio.service --no-pager -n 100 >&2; false; }`,
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
		"kubectl taint nodes --all node-role.kubernetes.io/control-plane- || kubectl taint nodes --all node-role.kubernetes.io/master- || true",
		fmt.Sprintf("kubectl label nodes --all hardware-type=%s --overwrite", cluster.Spec.NodeInfo.HardwareType),
		"kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml",
		"kubectl -n kube-flannel patch daemonset kube-flannel-ds --type=json -p='[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--iface=wg0\"}]'",
		"kubectl rollout status daemonset kube-flannel-ds -n kube-flannel --timeout=120s",
		"kubectl create namespace argocd || true",
		"kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml",

		// fmt.Sprintf("kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/%s/deployments/static/nvidia-device-plugin.yml ", cluster.Spec.NodeInfo.SoftwareConfig.K8sDevicePluginVersion),
		"rm /tmp/catalog/ -rf",
		"git clone https://github.com/vitu-mafeni/catalog.git /tmp/catalog",

		"kubectl apply -f /tmp/catalog/nephio/optional/flux-helm-controllers",
		"kubectl apply -f /tmp/catalog/workloads/ml-platform/longhorn/",

		`cat <<EOF | kubectl apply -f -
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: nvidia
handler: nvidia
EOF`,

		// temporary, will be removed after the controller is containerized.
		"rm -rf /tmp/remote-cluster-provisioner",
		"git clone -b r2-1 https://github.com/vitu-mafeni/remote-cluster-provisioner.git /tmp/remote-cluster-provisioner",
		"kubectl apply -f /tmp/remote-cluster-provisioner/config/crd/bases/",

		// "kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.36/deploy/local-path-storage.yaml",
		// "kubectl patch storageclass local-path -p '{\"metadata\": {\"annotations\":{\"storageclass.kubernetes.io/is-default-class\":\"true\"}}}'",

	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return "", fmt.Errorf("command failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	if err := argocd.ConfigureArgoCD(client, cluster); err != nil {
		return "", fmt.Errorf("ArgoCD configuration failed: %w", err)
	}

	// Generate a fresh join command valid for 24h.
	// Falls back to re-generating a token if the original init token has expired.
	joinCmd, err := getJoinCommand(client)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve join command: %w", err)
	}

	log.Print("Control plane ready. Join command is available for worker nodes to join the cluster.")
	return joinCmd, nil
}

// getJoinCommand generates a kubeadm join command for worker nodes.
// It creates a new bootstrap token (valid forever) and prints the full join string.
func getJoinCommand(client *sshhelper.Client) (string, error) {
	// Using --print-join-command gives us the full one-liner including
	// the CA cert hash, which is safer than parsing `kubeadm init` output.
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

// FOR WORKER NODES, we will run the join command in the controller and add the node to the cluster after provisioning, so no need to return join command for worker nodes here.
// JoinWorkerNode installs all prerequisites on the worker and joins it to the cluster.
// joinCmd is the full string returned by InitializeControlPlane (or getJoinCommand).
func JoinWorkerNode(client *sshhelper.Client, cpClient *sshhelper.Client, cluster *infrav1.RemoteCluster, joinCmd string, clusterParent *infrav1.RemoteCluster) (error, string) {
	log.Printf("Joining worker node %s to cluster %s", cluster.Spec.Host, cluster.Spec.ClusterName)

	if joinCmd == "" {
		return fmt.Errorf("joinCmd must not be empty"), ""
	}
	if clusterParent.Spec.NodeInfo.HardwareType == "" {
		return fmt.Errorf("clusterParent.Spec.NodeInfo.HardwareType must not be empty"), ""
	}

	// Resolve the worker's VPN IP from wg0.
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

	steps := []string{
		// Stop services and reset kubernetes state.
		// Do NOT wipe /var/lib/containers/storage, /var/lib/crio, or /run/crio:
		//   - storage wipe → "image not known" desync between kubelet and CRI-O
		//   - /run/crio wipe → directory-recreation race → "permission denied" on socket
		// kubeadm reset handles /etc/kubernetes cleanup so re-runs don't fail on "file already exists".
		"sudo systemctl stop kubelet 2>/dev/null || true",
		"sudo systemctl stop crio 2>/dev/null || true",
		"sudo kubeadm reset -f --cri-socket=unix:///var/run/crio/crio.sock 2>/dev/null || true",
		"sudo rm -rf /etc/cni/net.d 2>/dev/null || true",

		// =========================
		// Disable swap
		// =========================
		"sudo swapoff -a",
		`sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab`,

		// =========================
		// Kernel modules
		// =========================
		`echo -e "overlay\nbr_netfilter" | sudo tee /etc/modules-load.d/k8s.conf`,
		"sudo modprobe overlay",
		"sudo modprobe br_netfilter",

		// =========================
		// Sysctl configuration
		// =========================
		`echo -e "net.bridge.bridge-nf-call-iptables=1\nnet.bridge.bridge-nf-call-ip6tables=1\nnet.ipv4.ip_forward=1" | sudo tee /etc/sysctl.d/k8s.conf`,
		"sudo sysctl --system",

		// =========================
		// Base packages
		// =========================
		"sudo apt-get update",
		"sudo apt-get install -y ca-certificates curl gnupg apt-transport-https",

		// =========================
		// Install CRI-O
		// =========================
		// add repositories
		"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
		"sudo mkdir -p /etc/apt/keyrings",
		fmt.Sprintf(`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`, repoVersion),

		fmt.Sprintf(`sudo curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key |     sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),

		// install dependencies packages and custom cri-o
		"sudo apt-get install -y build-essential libgpgme-dev pkg-config gcc xmlto build-essential asciidoc libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf pkg-config uuid-dev libbsd-dev libnftables-dev libcap-dev libnl-3-dev libnet1-dev libaio-dev libgnutls28-dev libdrm-dev --no-install-recommends",
		"sudo dpkg --configure -a",
		"sudo apt-get install -y     git     gcc     make     pkg-config     libassuan-dev     libglib2.0-dev     libc6-dev     libgpgme-dev     libgpg-error-dev     libseccomp-dev     libsystemd-dev     libselinux1-dev     libbtrfs-dev     libudev-dev     software-properties-common     go-md2man     runc     crun",

		// go.mod requires go 1.26.4 — golang-go from apt is too old (≤1.21).
		// Download the exact version from golang.org and install to /usr/local/go.
		`GO_VER=1.26.4; \
		curl -fsSL https://go.dev/dl/go${GO_VER}.linux-amd64.tar.gz -o /tmp/go.tar.gz && \
		sudo rm -rf /usr/local/go && \
		sudo tar -C /usr/local -xzf /tmp/go.tar.gz && \
		rm -f /tmp/go.tar.gz && \
		/usr/local/go/bin/go version`,

		"sudo rm -rf /tmp/conmon && git clone https://github.com/containers/conmon /tmp/conmon && cd /tmp/conmon && PATH=/usr/local/go/bin:$PATH make && sudo make install && cd - && rm -rf /tmp/conmon",

		// Each SSH step runs in its own shell — cd does not persist, so clone+build must
		// be one chained command executed inside the repo directory.
		// PATH must include /usr/local/go/bin so that make finds the correct Go version.
		`sudo rm -rf /tmp/custom-crio && \
		git clone https://github.com/vitu-mafeni/leehun-cri-o.git /tmp/custom-crio -b 2026-02-03/support-restore-from-file && \
		cd /tmp/custom-crio && \
		PATH=/usr/local/go/bin:$PATH make && \
		sudo make install && \
		sudo make install.config && \
		rm -rf /tmp/custom-crio`,
		`sudo mkdir -p /etc/crio/crio.conf.d && \
		echo '[crio.runtime]
		listen = "/var/run/crio/crio.sock"
		stream_address = "127.0.0.1"
		[crio.image]
		listen = "/var/run/crio/crio.sock"' | sudo tee /etc/crio/crio.conf.d/00-sock-path.conf`,

		fmt.Sprintf(`curl -fsSL https://github.com/kubernetes-sigs/cri-tools/releases/download/v%s/crictl-v%s-linux-amd64.tar.gz | sudo tar -C /usr/local/bin -xzf - crictl && \
sudo chmod 0755 /usr/local/bin/crictl && \
sudo ln -sf /usr/local/bin/crictl /usr/bin/crictl`, clean, clean),

		`sudo tee /etc/crictl.yaml >/dev/null <<EOF
		runtime-endpoint: unix:///var/run/crio/crio.sock
		image-endpoint: unix:///var/run/crio/crio.sock
		timeout: 10
		debug: false
		EOF`,

		`sudo mkdir -p /etc/containers && \
		echo '{"default":[{"type":"insecureAcceptAnything"}]}' \
		| sudo tee /etc/containers/policy.json > /dev/null`,

		"sudo systemctl daemon-reload",
		"sudo systemctl enable crio",
		"sudo systemctl start crio",
		"sudo systemctl status crio --no-pager || true",

		"runc --version || true",

		"sudo apt-get update",
		"sudo mkdir /var/lib/kubelet/plugins/kubernetes.io/empty-dir -f || true",
		fmt.Sprintf("sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages --allow-downgrades", clean, clean, clean),
		"sudo apt-mark hold kubelet kubeadm kubectl",
		"sudo systemctl enable kubelet",
		"sudo systemctl stop kubelet",
		"sudo systemctl daemon-reload",

		// =========================
		// Join the cluster
		// =========================
		// Set kubelet node IP before joining so the node registers with the correct address.
		fmt.Sprintf(`echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' | sudo tee /etc/default/kubelet`, nodeIP),
		"sudo systemctl daemon-reload",
	}

	if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		steps = append(steps,
			"sudo mkdir -p /etc/cdi /var/run/cdi /etc/crio/crio.conf.d",
			`test -f /etc/crio/crio.conf.d/99-cdi.conf || echo '[crio.runtime]
enable_cdi = true
cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]' | sudo tee /etc/crio/crio.conf.d/99-cdi.conf`,
		)
	}

	steps = append(steps,
		// One final CRI-O restart to pick up all config (kubelet node-ip, CDI, etc.)
		// before kubeadm join. No restart after join — that would lose image metadata.
		`sudo systemctl daemon-reload && sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; }`,
		// Wait until CRI-O actually responds — socket existence is not enough.
		// Replace your crictl info verification loops with an explicit endpoint check:
		`for i in $(seq 1 30); do \
		sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info >/dev/null 2>&1 && echo "CRI-O ready" && break; \
		echo "Waiting for CRI-O ($i/30)..."; sleep 3; \
		done; \
		sudo crictl --runtime-endpoint unix:///var/run/crio/crio.sock info || { sudo journalctl -xeu crio.service --no-pager -n 100 >&2; false; }`,

		// Join the cluster — kubeadm pulls all images here; do not restart CRI-O after this.
		fmt.Sprintf("sudo %s --cri-socket=unix:///var/run/crio/crio.sock", joinCmd),
	)

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("command failed: %s\nOutput:\n%s", cmd, output), ""
		}
	}

	// Label the worker from the control plane, where kubectl is configured.
	// node whose InternalIP matches the worker's host address.
	// We print each node's name repeated alongside every address it has, then grep
	// for the target IP — this handles nodes with multiple addresses correctly.
	// After kubeadm join, kubelet takes a few seconds to register the node with the API server.
	// Retry the address lookup for up to 90 seconds before giving up.
	var rawNodeOutput string
	nodeName := ""
	for attempt := 0; attempt < 45; attempt++ {
		var queryErr error
		rawNodeOutput, queryErr = sshhelper.Run(cpClient, `kubectl get nodes -o json | jq -r '.items[] | .metadata.name as $n | .status.addresses[].address | [$n, .] | @tsv'`)
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

	// cluster.Spec.Host is the node ip as registered in the cluster.
	labelAndTaintCmd := fmt.Sprintf(
		"kubectl label node %s hardware-type=%s gpu=on --overwrite && kubectl taint node %s hardware-type=gpu:PreferNoSchedule",
		nodeName, cluster.Spec.NodeInfo.HardwareType, nodeName,
	)
	if output, err := sshhelper.Run(cpClient, labelAndTaintCmd); err != nil {
		return fmt.Errorf("failed to label/taint worker node %s: %w\nOutput:\n%s", nodeName, err, output), ""
	}

	log.Printf("Worker node %s successfully joined cluster %s", cluster.Spec.Host, cluster.Spec.ClusterName)
	return nil, nodeIP
}

// GetTunIP returns the IPv4 address of the wg0 interface on the remote host.
// It is used to register nodes with their VPN IP rather than their LAN IP.
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
		// =========================
		// Prerequisites
		// =========================
		"sudo apt-get update",
		"sudo apt-get install -y --no-install-recommends ca-certificates curl gnupg2",

		// =========================
		// NVIDIA repository + GPG key
		// =========================
		// Add GPG key
		"curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg",

		// Add repository
		`curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list`,

		// Enable experimental packages
		"sudo sed -i -e '/experimental/ s/^#//g' /etc/apt/sources.list.d/nvidia-container-toolkit.list",

		"sudo apt-get update",

		// =========================
		// Install toolkit (pinned version)
		// =========================
		fmt.Sprintf(`sudo apt-get install  --allow-downgrades -y \
nvidia-container-toolkit=%s \
nvidia-container-toolkit-base=%s \
libnvidia-container-tools=%s \
libnvidia-container1=%s`,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
		),

		// =========================
		// Configure CRI-O runtime
		// =========================
		"sudo nvidia-ctk runtime configure --runtime=crio",
		"sudo systemctl restart crio",

		// Sanity check
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

// InstallNvidiaDrivers installs the NVIDIA drivers on a GPU node.
// It should be called after InstallNvidiaContainerToolkit.
// A reboot is typically required after driver installation for the drivers to take effect.
func InstallNvidiaDrivers11(client *sshhelper.Client, cluster *infrav1.RemoteCluster, clusterParent *infrav1.RemoteCluster) error {
	// if !strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
	// 	log.Printf("Skipping NVIDIA driver install — node %s is not a GPU node", cluster.Spec.Host)
	// 	return nil
	// }

	log.Printf("Installing NVIDIA drivers on GPU node %s", cluster.Spec.Host)

	nvidiaDriverVersion := clusterParent.Spec.NodeInfo.SoftwareConfig.NvidiaDriverVersion
	nvidiaToolkitVersion := clusterParent.Spec.NodeInfo.SoftwareConfig.NvidiaContainerToolkitVersion

	steps := []string{
		// =========================
		// Prerequisites
		// =========================
		"sudo apt-get update",
		"sudo apt-get install -y --no-install-recommends ca-certificates curl gnupg2",

		// =========================
		// NVIDIA container toolkit repository + GPG key
		// =========================
		"curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg",
		`curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list`,
		"sudo sed -i -e '/experimental/ s/^#//g' /etc/apt/sources.list.d/nvidia-container-toolkit.list",
		"sudo apt-get update",

		// =========================
		// Install container toolkit (pinned version)
		// =========================
		fmt.Sprintf(`sudo apt-get install  --allow-downgrades -y \
nvidia-container-toolkit=%s \
nvidia-container-toolkit-base=%s \
libnvidia-container-tools=%s \
libnvidia-container1=%s`,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
		),

		// Configure CRI-O to use NVIDIA runtime
		"sudo nvidia-ctk runtime configure --runtime=crio",
		"sudo systemctl restart crio",

		// =========================
		// Install NVIDIA GPU drivers
		// =========================
		// List available GPU drivers (informational, non-fatal)
		"sudo ubuntu-drivers list --gpgpu || true",

		// Kernel headers are required for DKMS to build modules for the current kernel.
		"sudo apt-get install -y linux-headers-$(uname -r) linux-headers-generic",

		// Install display + compute driver
		fmt.Sprintf("sudo ubuntu-drivers install nvidia:%s", nvidiaDriverVersion),

		// Install server-grade GPU driver
		fmt.Sprintf("sudo ubuntu-drivers install --gpgpu nvidia:%s-server", nvidiaDriverVersion),

		// Install the DKMS package so modules are rebuilt if the kernel is ever updated.
		// ubuntu-drivers installs the no-dkms precompiled variant by default, which only
		// covers the kernel running at install time.
		fmt.Sprintf("sudo apt-get install -y nvidia-dkms-%s-server", nvidiaDriverVersion),

		// Install nvidia-utils for tools like nvidia-smi
		fmt.Sprintf("sudo apt-get install -y nvidia-utils-%s-server", nvidiaDriverVersion),

		// Sanity checks
		"nvidia-smi || true",
		"sudo nvidia-ctk --version",

		// =========================
		// Configure CRI-O NVIDIA runtime
		// Only write and restart if the config is missing or different.
		// =========================
		`sudo rm -rf /etc/crio/crio.conf.d`,
		`sudo mkdir -p /etc/crio/crio.conf.d`,

		// Write the expected config to a temp file for comparison
		`sudo tee /etc/crio/crio.conf.d/999-runc.conf > /dev/null <<'EOFCONF'
[crio]

  [crio.runtime]
    default_runtime = "runc"

    [crio.runtime.runtimes]

      [crio.runtime.runtimes.nvidia]
        runtime_path = "/usr/bin/nvidia-container-runtime"
        runtime_type = "oci"
	  [crio.runtime.runtimes.runc]
        runtime_path = "/usr/sbin/runc"
        runtime_type = "oci"
EOFCONF`,

		// // Apply config and restart CRI-O only if missing or different
		// `if [ ! -f /etc/crio/crio.conf.d/99-nvidia.conf ] || ! diff -q /tmp/99-nvidia.conf /etc/crio/crio.conf.d/99-nvidia.conf > /dev/null 2>&1; then sudo cp /tmp/99-nvidia.conf /etc/crio/crio.conf.d/99-nvidia.conf && sudo systemctl restart crio && echo "CRI-O restarted with updated NVIDIA runtime config"; else echo "CRI-O NVIDIA runtime config already up to date — skipping"; fi`,

		// // Verify NVIDIA is the default runtime
		// `sudo crictl info | python3 -m json.tool | grep '"DefaultRuntime"' || true`,
		// Remove monitor_path ONLY from 99-nvidia.conf — it is deprecated for
		// non-default runtimes in CRI-O >= 1.28. The base 10-crio.conf legitimately
		// uses monitor_path for crun/runc and must not be modified.
		`sudo sed -i '/monitor_path/d' /etc/crio/crio.conf.d/99-nvidia.conf`,
		// CRI-O requires conmon in $PATH for monitor fields translation.
		// It is installed at /usr/libexec/crio/conmon but not symlinked by default.
		`sudo ln -sf /usr/libexec/crio/conmon /usr/local/bin/conmon`,
		"sudo systemctl restart crio",

		// Verify NVIDIA is the default runtime
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
// Must be called after the node has rebooted post driver installation so that
// the NVIDIA kernel module is loaded and NVML can enumerate devices.
func GenerateCDI(client *sshhelper.Client) error {
	steps := []string{
		"sudo mkdir -p /etc/cdi",
		"sudo nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml",
	}
	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("cdi generate failed: %s\nOutput:\n%s", cmd, output)
		}
	}
	return nil
}
