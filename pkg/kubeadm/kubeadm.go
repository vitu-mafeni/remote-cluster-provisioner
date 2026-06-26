package kubeadm

import (
	"fmt"
	"log"
	"strings"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1" // Add the correct import path for infrav1
	"dcn.ssu.ac.kr/infra/pkg/argocd"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

const (
	CrioAsset  = "https://github.com/vitu-mafeni/leehun-cri-o/releases/download/crio-1.35.0-restore-from-file/crio"
	CrioCommit = "a0e6cb3d7f0ca8e9f31131daa17570082e716393"
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

		// custom cri-o

		// repoVersion MUST be "1.35" so the packaged conmon/runc/config match the custom binary
		"sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg",
		fmt.Sprintf(`curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
		| gpg --dearmor | sudo tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
		https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
		| sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),
		fmt.Sprintf("CRICTL_VERSION=v%s", clean),
		"sudo apt-get update",
		"sudo apt-get install -y jq criu crun conmon",
		// crun from apt (typically 0.17 on Ubuntu 22.04) rejects OCI spec 1.1.0 with
		// "unknown version specified". CRI-O 1.35 generates specs at 1.1.0, so we must
		// install crun >= 1.0 from GitHub and pin CRI-O to use that exact path.
		`CRUN_VER=$(curl -fsSL https://api.github.com/repos/containers/crun/releases/latest 2>/dev/null | jq -r .tag_name 2>/dev/null) && \
		{ [ -n "$CRUN_VER" ] && [ "$CRUN_VER" != "null" ]; } || CRUN_VER=1.17 && \
		sudo curl -fsSL "https://github.com/containers/crun/releases/download/${CRUN_VER}/crun-${CRUN_VER}-linux-amd64" \
		  -o /usr/local/bin/crun && \
		sudo chmod 0755 /usr/local/bin/crun && \
		sudo cp -f /usr/local/bin/crun /usr/bin/crun && \
		crun --version`,
		"sudo apt-get install -y cri-o",
		// Always point CRI-O at the GitHub-sourced crun so the apt version (which may
		// be on PATH before /usr/local/bin) is never used.
		`sudo mkdir -p /etc/crio/crio.conf.d && \
		printf '[crio.runtime.runtimes.crun]\nruntime_path = "/usr/local/bin/crun"\nruntime_type = "oci"\nruntime_root = "/run/crun"\n' \
		| sudo tee /etc/crio/crio.conf.d/10-crun.conf`,
		"sudo systemctl enable crio --now || { sudo journalctl -xeu crio.service --no-pager >&2; false; }",

		// --- swap in the custom restore-from-file binary, idempotent on commit ---
		fmt.Sprintf(`WANT=%s; \
		HAVE=$(crio version --json 2>/dev/null | jq -r .gitCommit); \
		if [ "$HAVE" = "$WANT" ]; then \
		echo "custom crio $WANT already installed, skipping"; \
		else \
		curl -fsSL %s -o /tmp/crio && \
		chmod 0755 /tmp/crio && \
		GOT=$(/tmp/crio version --json | jq -r .gitCommit) && \
		[ "$GOT" = "$WANT" ] && \
		sudo systemctl stop crio && \
		sudo install -m 0755 /tmp/crio /usr/bin/crio && \
		sudo systemctl daemon-reload && \
		sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; } && \
		rm -f /tmp/crio; \
		fi`, CrioCommit, CrioAsset),

		// the binary was built with PREFIX=/usr/local, so it looks for the CRIU device
		// restorer at /usr/local/libexec; the package ships it under /usr/libexec
		`test -f /usr/local/libexec/crio/criu-device-restorer.sh || \
		sudo install -D -m 0755 /usr/libexec/crio/criu-device-restorer.sh \
		/usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || \
		echo "WARN: criu-device-restorer.sh missing; restore-from-file may fail"`,

		"sudo crio version || true",
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
		`sudo mkdir -p /etc/containers && \
		echo '{"default":[{"type":"insecureAcceptAnything"}]}' \
		| sudo tee /etc/containers/policy.json > /dev/null`,
		fmt.Sprintf("cat <<'EOF' | sudo tee /tmp/kubeadm-config.yaml\n%s\nEOF", kubeadmConfig),
		// Always restart CRI-O here to pick up any config changes made above
		// (e.g. new crun path). A passive "is-active || start" misses the case where
		// CRI-O is active but using stale config from a previous failed provisioning run.
		`sudo systemctl daemon-reload && sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; }`,
		`for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		test -S /var/run/crio/crio.sock && echo "CRI-O socket ready" && break; \
		echo "Waiting for CRI-O socket ($i/20)..."; sleep 3; \
		done; \
		test -S /var/run/crio/crio.sock || { sudo journalctl -xeu crio.service --no-pager -n 100 >&2; false; }`,
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
		"kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml",
		"kubectl -n kube-flannel patch daemonset kube-flannel-ds --type=json -p='[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--iface=wg0\"}]'",
		"kubectl rollout status daemonset kube-flannel-ds -n kube-flannel --timeout=120s",
		"kubectl create namespace argocd || true",
		"kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml",

		fmt.Sprintf("kubectl create -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/%s/deployments/static/nvidia-device-plugin.yml ", cluster.Spec.NodeInfo.SoftwareConfig.K8sDevicePluginVersion),
		"rm /tmp/catalog/ -rf",
		"git clone https://github.com/vitu-mafeni/catalog.git /tmp/catalog",

		"kubectl apply -f /tmp/catalog/nephio/optional/flux-helm-controllers",
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

		"kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.36/deploy/local-path-storage.yaml",
		"kubectl patch storageclass local-path -p '{\"metadata\": {\"annotations\":{\"storageclass.kubernetes.io/is-default-class\":\"true\"}}}'",
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
		"sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg",
		fmt.Sprintf(`curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
		| gpg --dearmor | sudo tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
		fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
		https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
		| sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),
		"sudo apt-get update",
		"sudo apt-get install -y jq criu crun conmon",
		`CRUN_VER=$(curl -fsSL https://api.github.com/repos/containers/crun/releases/latest 2>/dev/null | jq -r .tag_name 2>/dev/null) && \
		{ [ -n "$CRUN_VER" ] && [ "$CRUN_VER" != "null" ]; } || CRUN_VER=1.17 && \
		sudo curl -fsSL "https://github.com/containers/crun/releases/download/${CRUN_VER}/crun-${CRUN_VER}-linux-amd64" \
		  -o /usr/local/bin/crun && \
		sudo chmod 0755 /usr/local/bin/crun && \
		crun --version`,
		"sudo apt-get install -y cri-o",
		// pin absolute crun path so CRI-O uses os.Stat instead of exec.LookPath
		`CRUN_BIN=$(command -v crun 2>/dev/null || echo /usr/local/bin/crun) && \
		echo "crun path: $CRUN_BIN" && \
		sudo mkdir -p /etc/crio/crio.conf.d && \
		printf '[crio.runtime.runtimes.crun]\nruntime_path = "%s"\nruntime_type = "oci"\nruntime_root = "/run/crun"\n' "$CRUN_BIN" \
		| sudo tee /etc/crio/crio.conf.d/10-crun.conf`,
		"sudo systemctl enable crio --now || { sudo journalctl -xeu crio.service --no-pager >&2; false; }",

		// swap in the custom restore-from-file binary, idempotent on commit
		fmt.Sprintf(`WANT=%s; \
		HAVE=$(crio version --json 2>/dev/null | jq -r .gitCommit); \
		if [ "$HAVE" = "$WANT" ]; then \
		echo "custom crio $WANT already installed, skipping"; \
		else \
		curl -fsSL %s -o /tmp/crio && \
		chmod 0755 /tmp/crio && \
		GOT=$(/tmp/crio version --json | jq -r .gitCommit) && \
		[ "$GOT" = "$WANT" ] && \
		sudo systemctl stop crio && \
		sudo install -m 0755 /tmp/crio /usr/bin/crio && \
		sudo systemctl daemon-reload && \
		sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; } && \
		rm -f /tmp/crio; \
		fi`, CrioCommit, CrioAsset),

		`test -f /usr/local/libexec/crio/criu-device-restorer.sh || \
		sudo install -D -m 0755 /usr/libexec/crio/criu-device-restorer.sh \
		/usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || \
		echo "WARN: criu-device-restorer.sh missing; restore-from-file may fail"`,

		"sudo crio version || true",
		"sudo crictl info || true",
		"sudo systemctl status crio --no-pager || true",

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
			return fmt.Errorf("command failed: %s\nOutput:\n%s", cmd, output), ""
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
		return fmt.Errorf("failed to list nodes: %w\nOutput:\n%s", err, rawNodeOutput), ""
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
func InstallNvidiaContainerToolkit(client *sshhelper.Client, cluster *infrav1.RemoteCluster, clusterParent *infrav1.RemoteCluster) error {
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
func InstallNvidiaDrivers(client *sshhelper.Client, cluster *infrav1.RemoteCluster, clusterParent *infrav1.RemoteCluster) error {
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
