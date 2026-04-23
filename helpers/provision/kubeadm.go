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
      value: "DynamicResourceAllocation=true"
controllerManager:
  extraArgs:
    - name: feature-gates
      value: "DynamicResourceAllocation=true"
scheduler:
  extraArgs:
    - name: feature-gates
      value: "DynamicResourceAllocation=true"
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
maxPods: 200
cgroupDriver: systemd
containerRuntimeEndpoint: unix:///var/run/crio/crio.sock
featureGates:
  DynamicResourceAllocation: true
runtimeRequestTimeout: "15m"
---
apiVersion: kubeproxy.config.k8s.io/v1alpha1
kind: KubeProxyConfiguration
mode: ipvs
`, cluster.Spec.Host, clean, cluster.Spec.ClusterName)

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
		fmt.Sprintf("cat <<'EOF' | sudo tee /tmp/kubeadm-config.yaml\n%s\nEOF", kubeadmConfig),
		"test -f /etc/kubernetes/admin.conf || sudo kubeadm init --config /tmp/kubeadm-config.yaml",
		"mkdir -p $HOME/.kube",
		"sudo cp -f /etc/kubernetes/admin.conf $HOME/.kube/config",
		"sudo chown $(id -u):$(id -g) $HOME/.kube/config",
		"kubectl taint nodes --all node-role.kubernetes.io/control-plane- || kubectl taint nodes --all node-role.kubernetes.io/master- || true",
		fmt.Sprintf("kubectl label nodes --all hardware-type=%s --overwrite", cluster.Spec.NodeInfo.HardwareType),
		"kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml",
		"kubectl create namespace argocd || true",
		"kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml",
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

	log.Printf("Control plane ready. Join command: %s", joinCmd)
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
		"sudo install -m 0755 -d /etc/apt/keyrings",
		"sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg",
		fmt.Sprintf(`curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
| gpg --dearmor | sudo tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
| sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),
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
		fmt.Sprintf("sudo %s", joinCmd),

		// // =========================
		// // Label this node
		// // =========================
		// fmt.Sprintf("kubectl label nodes --all hardware-type=%s --overwrite", cluster.Spec.NodeInfo.HardwareType),
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
	log.Printf("Node address table for cluster %s:\n%s", cluster.Spec.ClusterName, rawNodeOutput)

	// Find the node name whose address column matches cluster.Spec.Host.
	nodeName := ""
	for _, line := range strings.Split(strings.TrimSpace(rawNodeOutput), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == cluster.Spec.Host {
			nodeName = fields[0]
			break
		}
	}
	if nodeName == "" {
		return fmt.Errorf("failed to resolve node name for host %s — address table:\n%s", cluster.Spec.Host, rawNodeOutput)
	}

	// cluster.Spec.Host is the node ip as registered in the cluster.
	labelCmd := fmt.Sprintf("kubectl label node %s hardware-type=%s --overwrite", nodeName, cluster.Spec.NodeInfo.HardwareType)
	if output, err := sshhelper.Run(cpClient, labelCmd); err != nil {
		return fmt.Errorf("failed to label worker node %s: %w\nOutput:\n%s", nodeName, err, output)
	}

	log.Printf("Worker node %s successfully joined cluster %s", cluster.Spec.Host, cluster.Spec.ClusterName)
	return nil
}
