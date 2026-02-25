package provision

import (
	"fmt"
	"strings"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1" // Add the correct import path for infrav1
	sshhelper "dcn.ssu.ac.kr/infra/helpers/ssh"
)

func SingleNode(client *sshhelper.Client, cluster *infrav1.RemoteCluster) error {

	clean := strings.TrimPrefix(cluster.Spec.Kubernetes.Version, "v")

	kubeadmConfig := fmt.Sprintf(`
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: "0.0.0.0"
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
cgroupDriver: systemd
containerRuntimeEndpoint: unix:///var/run/crio/crio.sock
featureGates:
  DynamicResourceAllocation: true
runtimeRequestTimeout: "15m"
---
apiVersion: kubeproxy.config.k8s.io/v1alpha1
kind: KubeProxyConfiguration
mode: ipvs
`, clean, cluster.Spec.ClusterName)

	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid kubernetes version: %s", cluster.Spec.Kubernetes.Version)
	}

	repoVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	steps := []string{

		// =========================
		// Idempotency check
		// =========================
		// "test -f /etc/kubernetes/admin.conf && echo 'already-initialized'",

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
		// fmt.Sprintf("CRIO_VERSION=v%s", repoVersion),
		"sudo install -m 0755 -d /etc/apt/keyrings",

		"sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg",

		fmt.Sprintf(`curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
| gpg --dearmor | sudo tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),

		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
| sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),

		"sudo apt-get update",
		"sudo apt-get install -y cri-o cri-tools",

		// Enable CRI-O
		"sudo systemctl enable crio",
		"sudo systemctl restart crio",

		// Optional sanity checks (very helpful)
		"sudo crictl info || true",
		"sudo systemctl status crio --no-pager || true",

		// =========================
		// Kubernetes repository
		// =========================
		"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
		"sudo mkdir -p /etc/apt/keyrings",

		fmt.Sprintf(`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`, repoVersion),

		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`, repoVersion),

		"sudo apt-get update",

		// Install specific version

		fmt.Sprintf("sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-*", clean, clean, clean),
		"sudo apt-mark hold kubelet kubeadm kubectl",
		"sudo systemctl enable kubelet",

		// =========================
		// Initialize cluster (single node)
		// =========================
		fmt.Sprintf("cat <<'EOF' | sudo tee /tmp/kubeadm-config.yaml\n%s\nEOF", kubeadmConfig),
		// "test -f /etc/kubernetes/admin.conf || sudo kubeadm init --pod-network-cidr=10.244.0.0/16",
		"test -f /etc/kubernetes/admin.conf || sudo kubeadm init --config /tmp/kubeadm-config.yaml",

		// =========================
		// Kubeconfig setup
		// =========================
		"mkdir -p $HOME/.kube",
		"sudo cp -f /etc/kubernetes/admin.conf $HOME/.kube/config",
		"sudo chown $(id -u):$(id -g) $HOME/.kube/config",
		// allow scheduling on control-plane node (for single-node cluster)
		"kubectl taint nodes --all node-role.kubernetes.io/control-plane- || kubectl taint nodes --all node-role.kubernetes.io/master- || true",

		// =========================
		// Install Flannel CNI
		// =========================
		"kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml",
		"kubectl create namespace argocd",
		"kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml",
		"git clone https://github.com/vitu-mafeni/catalog.git /tmp/catalog",
		"kubectl apply -f /tmp/catalog/nephio/optional/flux-helm-controllers",
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("command failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	if err := ConfigureArgoCD(client, cluster); err != nil {
		return err
	}

	return nil
}
