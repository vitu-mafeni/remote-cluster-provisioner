package provision

import (
	"fmt"

	sshhelper "dcn.ssu.ac.kr/infra/helpers/ssh"
)

func SingleNode(client *sshhelper.Client, version string) error {

	steps := []string{

		// Idempotency check
		"test -f /etc/kubernetes/admin.conf && echo 'already-initialized'",

		// Base packages
		"sudo apt-get update",
		"sudo apt-get install -y apt-transport-https ca-certificates curl",

		// Containerd
		"sudo apt-get install -y containerd",
		"sudo mkdir -p /etc/containerd",
		"sudo containerd config default | sudo tee /etc/containerd/config.toml",
		"sudo systemctl restart containerd",
		"sudo systemctl enable containerd",

		// Kubernetes packages
		"curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.30/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
		"echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.30/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list",

		"sudo apt-get update",
		"sudo apt-get install -y kubelet kubeadm kubectl",
		"sudo systemctl enable kubelet",

		// kubeadm init (skip if exists)
		"test -f /etc/kubernetes/admin.conf || sudo kubeadm init --pod-network-cidr=10.244.0.0/16",

		// kubeconfig setup
		"mkdir -p $HOME/.kube",
		"sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config",
		"sudo chown $(id -u):$(id -g) $HOME/.kube/config",

		// Install Flannel CNI
		"kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml",
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("cmd failed: %s\n%s", cmd, output)
		}
	}

	return nil
}
