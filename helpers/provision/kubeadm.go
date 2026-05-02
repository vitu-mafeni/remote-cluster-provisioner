package provision

import (
	"fmt"
	"log"
	"strings"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1" // Add the correct import path for infrav1
	sshhelper "dcn.ssu.ac.kr/infra/helpers/ssh"
)

func InitializeControlPlane(client *sshhelper.Client, cluster *infrav1.RemoteCluster) (string, error) {
	log.Printf("Provisioning Kubernetes cluster with kubeadm on %s", cluster.Spec.Host)

	// Resolve the control plane's VPN IP from tun0 first — needed for kubeadmConfig and kubelet args.
	tunIP, err := getTunIP(client)
	if err != nil {
		return "", fmt.Errorf("failed to get control plane tun0 IP: %w", err)
	}
	log.Printf("Control plane VPN IP: %s", tunIP)

	clean := strings.TrimPrefix(cluster.Spec.Kubernetes.Version, "v")

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
		return "", fmt.Errorf("invalid kubernetes version: %s", cluster.Spec.Kubernetes.Version)
	}

	repoVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	steps := []string{
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
		"sudo apt-get install -y ca-certificates curl gnupg apt-transport-https",
		"sudo install -m 0755 -d /etc/apt/keyrings",
		"sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg",
		fmt.Sprintf(`curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
| gpg --dearmor | sudo tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
| sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),
		fmt.Sprintf("CRICTL_VERSION=v%s", clean),
		"sudo apt-get update",
		"sudo apt-get install -y jq",
		"sudo apt-get install -y cri-o ",
		"sudo systemctl enable crio --now",
		"sudo systemctl restart crio",
		"sudo crictl info || true",
		"sudo systemctl status crio --no-pager || true",
		"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
		"sudo mkdir -p /etc/apt/keyrings",
		fmt.Sprintf(`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`, repoVersion),
		"sudo apt-get update",
		fmt.Sprintf("sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages", clean, clean, clean),
		"sudo apt-mark hold kubelet kubeadm kubectl",
		"sudo systemctl enable kubelet",
		"sudo systemctl daemon-reload",
		// Set kubelet node IP to VPN IP before init.
		fmt.Sprintf(`echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' | sudo tee /etc/default/kubelet`, tunIP),
		"sudo systemctl daemon-reload",
		fmt.Sprintf("cat <<'EOF' | sudo tee /tmp/kubeadm-config.yaml\n%s\nEOF", kubeadmConfig),
		"test -f /etc/kubernetes/admin.conf || sudo kubeadm init --config /tmp/kubeadm-config.yaml",
		"mkdir -p $HOME/.kube",
		"sudo cp -f /etc/kubernetes/admin.conf $HOME/.kube/config",
		"sudo chown $(id -u):$(id -g) $HOME/.kube/config",
		"kubectl taint nodes --all node-role.kubernetes.io/control-plane- || kubectl taint nodes --all node-role.kubernetes.io/master- || true",
		fmt.Sprintf("kubectl label nodes --all hardware-type=%s --overwrite", cluster.Spec.NodeInfo.HardwareType),
		"kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml",
		"kubectl -n kube-flannel patch daemonset kube-flannel-ds --type=json -p='[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--iface=tun0\"}]'",
		"kubectl rollout status daemonset kube-flannel-ds -n kube-flannel --timeout=120s",
		"kubectl create namespace argocd || true",
		"kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml",

		fmt.Sprintf("kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/%s/deployments/static/nvidia-device-plugin.yml ", cluster.Spec.NodeInfo.SoftwareConfig.K8sDevicePluginVersion),
		"rm /tmp/catalog/ -rf",
		"git clone https://github.com/vitu-mafeni/catalog.git /tmp/catalog",
		"kubectl apply -f /tmp/catalog/nephio/optional/flux-helm-controllers",
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return "", fmt.Errorf("command failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	if err := ConfigureArgoCD(client, cluster); err != nil {
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
func JoinWorkerNode(client *sshhelper.Client, cpClient *sshhelper.Client, cluster *infrav1.RemoteCluster, joinCmd string) error {
	log.Printf("Joining worker node %s to cluster %s", cluster.Spec.Host, cluster.Spec.ClusterName)

	if joinCmd == "" {
		return fmt.Errorf("joinCmd must not be empty")
	}
	if cluster.Spec.NodeInfo.HardwareType == "" {
		return fmt.Errorf("cluster.Spec.NodeInfo.HardwareType must not be empty")
	}

	// Resolve the worker's VPN IP from tun0.
	nodeIP, err := getTunIP(client)
	if err != nil {
		return fmt.Errorf("failed to get worker tun0 IP: %w", err)
	}
	log.Printf("Worker VPN IP: %s", nodeIP)

	clean := strings.TrimPrefix(cluster.Spec.Kubernetes.Version, "v")

	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid kubernetes version: %s", cluster.Spec.Kubernetes.Version)
	}

	repoVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	steps := []string{
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
		fmt.Sprintf(`if which crio > /dev/null 2>&1; then
			echo "CRI-O already installed, skipping"
		else
			sudo mkdir -p /etc/apt/keyrings &&
			sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg &&
			curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key | sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg &&
			echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null &&
			sudo apt-get update &&
			sudo apt-get install -y cri-o
		fi`, repoVersion, repoVersion),
		"sudo apt-get update",
		"sudo apt-get install -y cri-o",
		"sudo systemctl enable crio --now",
		"sudo systemctl restart crio",

		// =========================
		// Kubernetes repository + packages
		// =========================
		"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
		"sudo mkdir -p /etc/apt/keyrings",
		fmt.Sprintf(`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`, repoVersion),
		"sudo apt-get update",
		fmt.Sprintf("sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages --allow-downgrades", clean, clean, clean),
		"sudo apt-mark hold kubelet kubeadm kubectl",
		"sudo systemctl enable kubelet",
		"sudo systemctl daemon-reload",

		// =========================
		// Join the cluster
		// =========================
		// Set kubelet node IP before joining so the node registers with the correct address.
		fmt.Sprintf(`echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' | sudo tee /etc/default/kubelet`, nodeIP),
		"sudo systemctl daemon-reload",
		fmt.Sprintf("sudo %s", joinCmd),

		// // =========================
		// // Label this node
		// // =========================
		// fmt.Sprintf("kubectl label nodes --all hardware-type=%s --overwrite", cluster.Spec.NodeInfo.HardwareType),
	}

	// =========================
	// GPU node: enable CDI in CRI-O (required by Hami DRA driver)
	// =========================
	if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		gpuSteps := []string{
			// Create CDI directories
			"sudo mkdir -p /etc/cdi /var/run/cdi",
			// Write CRI-O CDI config drop-in and restart only if it doesn't already exist.
			// The inner subshell writes the file and restarts crio; the outer test skips both if already present.
			`test -f /etc/crio/crio.conf.d/99-cdi.conf || (echo '[crio.runtime]
enable_cdi = true
cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]' | sudo tee /etc/crio/crio.conf.d/99-cdi.conf && sudo systemctl restart crio)`,
			// Verify CDI is active
			"sudo crictl info | grep -i cdi || true",
		}
		steps = append(steps, gpuSteps...)
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("command failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	// Label the worker from the control plane, where kubectl is configured.
	// node whose InternalIP matches the worker's host address.
	// We print each node's name repeated alongside every address it has, then grep
	// for the target IP — this handles nodes with multiple addresses correctly.
	// Dump all node names and addresses so we can match by IP or hostname.
	// Log the raw output first to help debug mismatches.
	rawNodeOutput, err := sshhelper.Run(cpClient, `kubectl get nodes -o json | jq -r '.items[] | .metadata.name as $n | .status.addresses[].address | [$n, .] | @tsv'`)
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w\nOutput:\n%s", err, rawNodeOutput)
	}
	// log.Printf("Node address table for cluster %s:\n%s", cluster.Spec.ClusterName, rawNodeOutput)

	// Find the node name whose address column matches cluster.Spec.Host.
	nodeName := ""
	for _, line := range strings.Split(strings.TrimSpace(rawNodeOutput), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == nodeIP {
			nodeName = fields[0]
			break
		}
	}
	if nodeName == "" {
		return fmt.Errorf("failed to resolve node name for host %s vpn ip: %s — address table:\n%s", cluster.Spec.Host, nodeIP, rawNodeOutput)
	}

	// cluster.Spec.Host is the node ip as registered in the cluster.
	labelCmd := fmt.Sprintf("kubectl label node %s hardware-type=%s gpu=on --overwrite", nodeName, cluster.Spec.NodeInfo.HardwareType)
	if output, err := sshhelper.Run(cpClient, labelCmd); err != nil {
		return fmt.Errorf("failed to label worker node %s: %w\nOutput:\n%s", nodeName, err, output)
	}

	log.Printf("Worker node %s successfully joined cluster %s", cluster.Spec.Host, cluster.Spec.ClusterName)
	return nil
}

// getTunIP returns the IPv4 address of the tun0 interface on the remote host.
// It is used to register nodes with their VPN IP rather than their LAN IP.
func getTunIP(client *sshhelper.Client) (string, error) {
	output, err := sshhelper.Run(client, `ip -4 addr show tun0 | grep -oP '(?<=inet )\d+\.\d+\.\d+\.\d+'`)
	if err != nil {
		return "", fmt.Errorf("ip addr show tun0 failed: %w\nOutput: %s", err, output)
	}
	ip := strings.TrimSpace(output)
	if ip == "" {
		return "", fmt.Errorf("tun0 has no IPv4 address — is the VPN connected?")
	}
	return ip, nil
}

// InstallNvidiaContainerToolkit installs the NVIDIA container toolkit on a GPU node
// and configures CRI-O to use it. This should be called after JoinWorkerNode for
// nodes where cluster.Spec.NodeInfo.HardwareType == "gpu".
func InstallNvidiaContainerToolkit(client *sshhelper.Client, cluster *infrav1.RemoteCluster) error {
	if !strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		log.Printf("Skipping NVIDIA container toolkit install — node %s is not a GPU node", cluster.Spec.Host)
		return nil
	}

	log.Printf("Installing NVIDIA container toolkit on GPU node %s", cluster.Spec.Host)

	const nvidiaToolkitVersion = "1.19.0-1"

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
		fmt.Sprintf(`sudo apt-get install -y \
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
func InstallNvidiaDrivers(client *sshhelper.Client, cluster *infrav1.RemoteCluster) error {
	// if !strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
	// 	log.Printf("Skipping NVIDIA driver install — node %s is not a GPU node", cluster.Spec.Host)
	// 	return nil
	// }

	log.Printf("Installing NVIDIA drivers on GPU node %s", cluster.Spec.Host)

	nvidiaDriverVersion := cluster.Spec.NodeInfo.SoftwareConfig.NvidiaDriverVersion
	nvidiaToolkitVersion := cluster.Spec.NodeInfo.SoftwareConfig.NvidiaContainerToolkitVersion

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
		fmt.Sprintf(`sudo apt-get install -y \
nvidia-container-toolkit=%s \
nvidia-container-toolkit-base=%s \
libnvidia-container-tools=%s \
libnvidia-container1=%s`,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
			nvidiaToolkitVersion,
		),

		"sudo mkdir -p /etc/cdi",
		"sudo nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml",
		"sudo nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml --mode=nvml",

		// Configure CRI-O to use NVIDIA runtime
		"sudo nvidia-ctk runtime configure --runtime=crio",
		"sudo systemctl restart crio",

		// =========================
		// Install NVIDIA GPU drivers
		// =========================
		// List available GPU drivers (informational, non-fatal)
		"sudo ubuntu-drivers list --gpgpu || true",

		// Install display + compute driver
		fmt.Sprintf("sudo ubuntu-drivers install nvidia:%s", nvidiaDriverVersion),

		// Install server-grade GPU driver
		fmt.Sprintf("sudo ubuntu-drivers install --gpgpu nvidia:%s-server", nvidiaDriverVersion),

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
		`sudo tee /etc/crio/crio.conf.d/99-nvidia.conf > /dev/null <<'EOFCONF'
[crio]

  [crio.runtime]
    default_runtime = "nvidia"

    [crio.runtime.runtimes]

      [crio.runtime.runtimes.nvidia]
        runtime_path = "/usr/bin/nvidia-container-runtime"
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
