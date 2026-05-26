package remotenodeprovision

/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	"dcn.ssu.ac.kr/infra/helpers/provision"
	sshhelper "dcn.ssu.ac.kr/infra/helpers/ssh"
	corev1 "k8s.io/api/core/v1"
)

func NewInClusterProvisioner(
	ctx context.Context,
	nodeProvision *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
	sshclient *sshhelper.Client,
	netNodeConfig *mlv1alpha1.NodeProvisionNetConfig,
) error {

	log.Printf(
		"Provisioning node %s",
		nodeProvision.Name,
	)

	// ============================================================
	// Allocate VPN IP
	// ============================================================

	vpnNodeIP, err := getNextAvailableIP(
		*netNodeConfig.Spec.VPNRange,
		netNodeConfig.Status.UsedIPAddresses,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to allocate VPN IP: %w",
			err,
		)
	}

	// ============================================================
	// Generate WireGuard keys
	// ============================================================

	privateKey, publicKey, err := generateWireGuardKeyPair()
	if err != nil {
		return fmt.Errorf(
			"failed generating wireguard keys: %w",
			err,
		)
	}

	// ============================================================
	// Load local wg template
	// ============================================================

	wgTemplate, err := loadLocalWGTemplate()
	if err != nil {
		return fmt.Errorf(
			"failed loading local wg template: %w",
			err,
		)
	}

	wgConfig := buildWGConfig(
		wgTemplate,
		privateKey,
		fmt.Sprintf("%s/24", vpnNodeIP),
	)

	// ============================================================
	// Register peer on VPN server
	// ============================================================

	addPeerCmd := fmt.Sprintf(
		"sudo wg set wg0 peer %s allowed-ips %s/32",
		publicKey,
		vpnNodeIP,
	)

	if output, err := sshhelper.Run(sshclient, addPeerCmd); err != nil {
		return fmt.Errorf(
			"failed adding wireguard peer: %w\nOutput:\n%s",
			err,
			output,
		)
	}

	// ============================================================
	// Kubernetes version parsing
	// ============================================================

	clean := strings.TrimPrefix(
		netNodeConfig.Spec.SoftwareConfig.KubernetesVersion,
		"v",
	)

	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return fmt.Errorf(
			"invalid kubernetes version: %s",
			netNodeConfig.Spec.SoftwareConfig.KubernetesVersion,
		)
	}

	repoVersion := fmt.Sprintf(
		"%s.%s",
		parts[0],
		parts[1],
	)

	// ============================================================
	// Provisioning steps
	// ============================================================

	steps := []string{

		// ========================================================
		// Disable swap
		// ========================================================

		"sudo swapoff -a",
		`sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab`,

		// ========================================================
		// Kernel modules
		// ========================================================

		`echo -e "overlay\nbr_netfilter" | sudo tee /etc/modules-load.d/k8s.conf`,
		"sudo modprobe overlay",
		"sudo modprobe br_netfilter",

		// ========================================================
		// Sysctl
		// ========================================================

		`echo -e "net.bridge.bridge-nf-call-iptables=1
net.bridge.bridge-nf-call-ip6tables=1
net.ipv4.ip_forward=1" | sudo tee /etc/sysctl.d/k8s.conf`,

		"sudo sysctl --system",

		// ========================================================
		// Base packages
		// ========================================================

		"sudo apt-get update",

		"sudo apt-get install -y ca-certificates curl gnupg apt-transport-https",

		"sudo apt-get install -y wireguard wireguard-tools",

		// ========================================================
		// WireGuard config
		// ========================================================

		"sudo mkdir -p /etc/wireguard",

		fmt.Sprintf(
			"cat <<'EOF' | sudo tee /etc/wireguard/wg0.conf\n%s\nEOF",
			wgConfig,
		),

		"sudo chmod 600 /etc/wireguard/wg0.conf",

		"sudo systemctl enable wg-quick@wg0",

		"sudo systemctl restart wg-quick@wg0",

		"sleep 5",

		// ========================================================
		// CRI-O
		// ========================================================

		fmt.Sprintf(`if which crio > /dev/null 2>&1; then
	echo "CRI-O already installed"
else
	sudo mkdir -p /etc/apt/keyrings &&
	sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg &&
	curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key | sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/cri-o-apt-keyring.gpg &&
	echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null &&
	sudo apt-get update &&
	sudo apt-get install -y cri-o
fi`, repoVersion, repoVersion),

		"sudo systemctl enable crio --now",

		"sudo systemctl restart crio",

		// ========================================================
		// Kubernetes packages
		// ========================================================

		"sudo rm -f /etc/apt/keyrings/kubernetes-apt-keyring.gpg",

		"sudo mkdir -p /etc/apt/keyrings",

		fmt.Sprintf(
			`curl -fsSL https://pkgs.k8s.io/core:/stable:/v%s/deb/Release.key | gpg --dearmor | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null`,
			repoVersion,
		),

		fmt.Sprintf(
			`echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v%s/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null`,
			repoVersion,
		),

		"sudo apt-get update",

		fmt.Sprintf(
			"sudo apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages --allow-downgrades",
			clean,
			clean,
			clean,
		),

		"sudo apt-mark hold kubelet kubeadm kubectl",

		"sudo systemctl enable kubelet",

		"sudo systemctl daemon-reload",
	}

	// ============================================================
	// Execute provisioning steps
	// ============================================================

	for _, cmd := range steps {

		output, err := sshhelper.Run(sshclient, cmd)
		if err != nil {
			return fmt.Errorf(
				"command failed: %s\nOutput:\n%s",
				cmd,
				output,
			)
		}
	}

	// ============================================================
	// Get actual wg0 IP AFTER tunnel starts
	// ============================================================

	nodeIP, err := provision.GetTunIP(sshclient)
	if err != nil {
		return fmt.Errorf(
			"failed getting wg0 IP: %w",
			err,
		)
	}

	log.Printf(
		"Node VPN IP: %s",
		nodeIP,
	)

	// ============================================================
	// Configure kubelet node-ip
	// ============================================================

	kubeletCmd := fmt.Sprintf(
		`echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' | sudo tee /etc/default/kubelet`,
		nodeIP,
	)

	if output, err := sshhelper.Run(sshclient, kubeletCmd); err != nil {
		return fmt.Errorf(
			"failed configuring kubelet node ip: %w\nOutput:\n%s",
			err,
			output,
		)
	}

	if output, err := sshhelper.Run(
		sshclient,
		"sudo systemctl daemon-reload",
	); err != nil {
		return fmt.Errorf(
			"failed daemon reload: %w\nOutput:\n%s",
			err,
			output,
		)
	}

	if output, err := sshhelper.Run(
		sshclient,
		"sudo systemctl restart kubelet",
	); err != nil {
		return fmt.Errorf(
			"failed restarting kubelet: %w\nOutput:\n%s",
			err,
			output,
		)
	}

	// ============================================================
	// Join cluster
	// ============================================================

	joinCmd := fmt.Sprintf(
		"sudo %s",
		netNodeConfig.Status.ClusterJoinCommand,
	)

	if output, err := sshhelper.Run(sshclient, joinCmd); err != nil {
		return fmt.Errorf(
			"failed joining cluster: %w\nOutput:\n%s",
			err,
			output,
		)
	}

	log.Printf(
		"Node %s successfully joined cluster",
		nodeProvision.Name,
	)

	return nil
}

func generateWireGuardKeyPair() (string, string, error) {

	// Generate private key
	privateCmd := exec.Command("wg", "genkey")

	var privateOut bytes.Buffer
	privateCmd.Stdout = &privateOut

	if err := privateCmd.Run(); err != nil {
		return "", "", fmt.Errorf(
			"failed generating private key: %w",
			err,
		)
	}

	privateKey := strings.TrimSpace(privateOut.String())

	// Generate public key
	publicCmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf(
			"echo '%s' | wg pubkey",
			privateKey,
		),
	)

	var publicOut bytes.Buffer
	publicCmd.Stdout = &publicOut

	if err := publicCmd.Run(); err != nil {
		return "", "", fmt.Errorf(
			"failed generating public key: %w",
			err,
		)
	}

	publicKey := strings.TrimSpace(publicOut.String())

	return privateKey, publicKey, nil
}

func loadLocalWGTemplate() (string, error) {

	content, err := os.ReadFile("/etc/wireguard/wg0.conf")
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func buildWGConfig(
	template string,
	privateKey string,
	address string,
) string {

	lines := strings.Split(template, "\n")

	for i, line := range lines {

		if strings.HasPrefix(
			strings.TrimSpace(line),
			"PrivateKey",
		) {
			lines[i] = fmt.Sprintf(
				"PrivateKey = %s",
				privateKey,
			)
		}

		if strings.HasPrefix(
			strings.TrimSpace(line),
			"Address",
		) {
			lines[i] = fmt.Sprintf(
				"Address = %s",
				address,
			)
		}
	}

	return strings.Join(lines, "\n")
}

// get next available IP in the VPN range by looking at currently used IPs
func getNextAvailableIP(
	vpnRange string,
	usedIPs []string,
) (string, error) {

	lastUsedIP := ""

	if len(usedIPs) > 0 {
		lastUsedIP = usedIPs[len(usedIPs)-1]
	}

	ip, ipNet, err := net.ParseCIDR(vpnRange)
	if err != nil {
		return "", fmt.Errorf(
			"invalid VPN range: %w",
			err,
		)
	}

	var nextIP net.IP

	if lastUsedIP != "" {

		nextIP = net.ParseIP(lastUsedIP)

		if nextIP == nil {
			return "", fmt.Errorf(
				"invalid last used IP: %s",
				lastUsedIP,
			)
		}

	} else {
		nextIP = ip
	}

	for {

		nextIP = incrementIP(nextIP)

		if !ipNet.Contains(nextIP) {
			return "", fmt.Errorf(
				"no available IPs in VPN range",
			)
		}

		nextIPStr := nextIP.String()

		if contains(usedIPs, nextIPStr) {
			continue
		}

		return nextIPStr, nil
	}
}

func contains(
	usedIPs []string,
	nextIPStr string,
) bool {

	for _, ip := range usedIPs {

		if ip == nextIPStr {
			return true
		}
	}

	return false
}

func incrementIP(nextIP net.IP) net.IP {

	ip := nextIP.To4()
	if ip == nil {
		return nextIP
	}

	for i := 3; i >= 0; i-- {

		ip[i]++

		if ip[i] != 0 {
			break
		}
	}

	return ip
}
