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
	"os/exec"
	"strings"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	"dcn.ssu.ac.kr/infra/helpers/provision"
	sshhelper "dcn.ssu.ac.kr/infra/helpers/ssh"
	corev1 "k8s.io/api/core/v1"
)

// NewInClusterProvisioner provisions an on-premises node by:
//  1. Allocating a VPN IP from the range tracked in netNodeConfig.
//  2. Generating a WireGuard keypair.
//  3. Registering the node as a peer on the VPN server via vpnServerClient.
//  4. Installing WireGuard, CRI-O, and Kubernetes packages on the node.
//  5. Joining the cluster.
//
// Returns the allocated VPN IP and the node's WireGuard public key so the
// caller can persist them in the NodeProvisionNetConfig status.
func NewInClusterProvisioner(
	ctx context.Context,
	nodeProvision *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
	sshclient *sshhelper.Client,
	vpnServerClient *sshhelper.Client,
	netNodeConfig *mlv1alpha1.NodeProvisionNetConfig,
) (vpnNodeIP string, publicKey string, err error) {

	log.Printf("Provisioning node %s", nodeProvision.Name)

	// ============================================================
	// Allocate VPN IP
	// ============================================================

	vpnNodeIP, err = getNextAvailableIP(
		*netNodeConfig.Spec.VPNRange,
		netNodeConfig.Status.UsedIPAddresses,
	)
	if err != nil {
		return "", "", fmt.Errorf("failed to allocate VPN IP: %w", err)
	}

	// ============================================================
	// Generate WireGuard keys
	// ============================================================

	privateKey, publicKey, err := generateWireGuardKeyPair()
	if err != nil {
		return "", "", fmt.Errorf("failed generating wireguard keys: %w", err)
	}

	// ============================================================
	// Build WireGuard client config from server info
	// ============================================================

	wgConfig, err := buildClientWGConfig(
		vpnServerClient,
		vpnNodeIP,
		*netNodeConfig.Spec.VPNRange,
		netNodeConfig.Spec.VPNServerPublicConfig.PublicIP,
		netNodeConfig.Spec.VPNServerPublicConfig.VPNPort,
		privateKey,
	)
	if err != nil {
		return "", "", fmt.Errorf("failed building wireguard client config: %w", err)
	}

	// ============================================================
	// Register peer on VPN server (idempotent)
	// The peer must be registered before the client interface comes
	// up so that the server is ready to accept the handshake.
	// ============================================================

	if err := registerVPNPeer(vpnServerClient, publicKey, vpnNodeIP); err != nil {
		return "", "", fmt.Errorf("failed registering wireguard peer on VPN server: %w", err)
	}

	// ============================================================
	// Kubernetes version parsing
	// ============================================================

	clean := strings.TrimPrefix(netNodeConfig.Spec.SoftwareConfig.KubernetesVersion, "v")

	parts := strings.Split(clean, ".")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid kubernetes version: %s", netNodeConfig.Spec.SoftwareConfig.KubernetesVersion)
	}

	repoVersion := fmt.Sprintf("%s.%s", parts[0], parts[1])

	// ============================================================
	// Provisioning steps on the node
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
		// WireGuard client config
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
			clean, clean, clean,
		),
		"sudo apt-mark hold kubelet kubeadm kubectl",
		"sudo systemctl enable kubelet",
		"sudo systemctl daemon-reload",
	}

	for _, cmd := range steps {
		output, err := sshhelper.Run(sshclient, cmd)
		if err != nil {
			return "", "", fmt.Errorf("command failed: %s\nOutput:\n%s", cmd, output)
		}
	}

	// ============================================================
	// Get actual wg0 IP AFTER tunnel starts
	// ============================================================

	nodeIP, err := provision.GetTunIP(sshclient)
	if err != nil {
		return "", "", fmt.Errorf("failed getting wg0 IP: %w", err)
	}

	log.Printf("Node VPN IP: %s", nodeIP)

	// ============================================================
	// Verify connectivity from VPN server to new peer
	// ============================================================

	verifyVPNConnectivity(vpnServerClient, nodeIP)

	// ============================================================
	// Configure kubelet node-ip
	// ============================================================

	kubeletCmd := fmt.Sprintf(
		`echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' | sudo tee /etc/default/kubelet`,
		nodeIP,
	)
	if output, err := sshhelper.Run(sshclient, kubeletCmd); err != nil {
		return "", "", fmt.Errorf("failed configuring kubelet node ip: %w\nOutput:\n%s", err, output)
	}
	if output, err := sshhelper.Run(sshclient, "sudo systemctl daemon-reload"); err != nil {
		return "", "", fmt.Errorf("failed daemon reload: %w\nOutput:\n%s", err, output)
	}
	if output, err := sshhelper.Run(sshclient, "sudo systemctl restart kubelet"); err != nil {
		return "", "", fmt.Errorf("failed restarting kubelet: %w\nOutput:\n%s", err, output)
	}

	// ============================================================
	// Join cluster
	// ============================================================

	joinCmd := fmt.Sprintf("sudo %s", netNodeConfig.Status.ClusterJoinCommand)
	if output, err := sshhelper.Run(sshclient, joinCmd); err != nil {
		return "", "", fmt.Errorf("failed joining cluster: %w\nOutput:\n%s", err, output)
	}

	log.Printf("Node %s successfully joined cluster", nodeProvision.Name)

	return nodeIP, publicKey, nil
}

// registerVPNPeer adds (or updates) the WireGuard peer on the VPN server.
// The operation is idempotent: if the public key already appears in the
// running config the wg set command still succeeds (it is an upsert).
// The peer block is also appended to /etc/wireguard/wg0.conf for persistence
// across reboots, but only if it is not already present.
// RegisterVPNPeer is the exported form used by cloud-provider provisioners.
func RegisterVPNPeer(vpnServerClient *sshhelper.Client, publicKey, vpnNodeIP string) error {
	return registerVPNPeer(vpnServerClient, publicKey, vpnNodeIP)
}

func registerVPNPeer(vpnServerClient *sshhelper.Client, publicKey, vpnNodeIP string) error {
	// Update running WireGuard config (idempotent upsert).
	addCmd := fmt.Sprintf(
		"sudo wg set wg0 peer %s allowed-ips %s/32 persistent-keepalive 25",
		publicKey, vpnNodeIP,
	)
	if output, err := sshhelper.Run(vpnServerClient, addCmd); err != nil {
		return fmt.Errorf("wg set peer: %w\nOutput:\n%s", err, output)
	}

	// Persist peer to config file so it survives a server reboot.
	// Uses grep to avoid duplicating the block on repeated reconciles.
	persistCmd := fmt.Sprintf(`
if ! sudo grep -qF '%s' /etc/wireguard/wg0.conf 2>/dev/null; then
  printf '\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\nPersistentKeepalive = 25\n' | sudo tee -a /etc/wireguard/wg0.conf > /dev/null
fi`, publicKey, publicKey, vpnNodeIP)

	if output, err := sshhelper.Run(vpnServerClient, persistCmd); err != nil {
		return fmt.Errorf("persisting peer to wg0.conf: %w\nOutput:\n%s", err, output)
	}

	log.Printf("Registered VPN peer: publicKey=%s vpnIP=%s", publicKey, vpnNodeIP)
	return nil
}

// verifyVPNConnectivity pings the new peer from the VPN server.
// A failure is logged as a warning rather than returned as an error because
// the node's WireGuard interface may still be initialising.
func verifyVPNConnectivity(vpnServerClient *sshhelper.Client, vpnNodeIP string) {
	pingCmd := fmt.Sprintf("ping -c 3 -W 5 %s", vpnNodeIP)
	if output, err := sshhelper.Run(vpnServerClient, pingCmd); err != nil {
		log.Printf("Warning: VPN connectivity check to %s failed (node may still be initialising): %v\nOutput: %s", vpnNodeIP, err, output)
	} else {
		log.Printf("VPN connectivity to %s verified", vpnNodeIP)
	}
}

// GenerateWireGuardKeyPair is the exported form used by cloud-provider provisioners.
func GenerateWireGuardKeyPair() (string, string, error) { return generateWireGuardKeyPair() }

func generateWireGuardKeyPair() (string, string, error) {

	privateCmd := exec.Command("wg", "genkey")

	var privateOut bytes.Buffer
	privateCmd.Stdout = &privateOut

	if err := privateCmd.Run(); err != nil {
		return "", "", fmt.Errorf("failed generating private key: %w", err)
	}

	privateKey := strings.TrimSpace(privateOut.String())

	publicCmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf("echo '%s' | wg pubkey", privateKey),
	)

	var publicOut bytes.Buffer
	publicCmd.Stdout = &publicOut

	if err := publicCmd.Run(); err != nil {
		return "", "", fmt.Errorf("failed generating public key: %w", err)
	}

	publicKey := strings.TrimSpace(publicOut.String())

	return privateKey, publicKey, nil
}

// BuildClientWGConfig is the exported form used by cloud-provider provisioners.
func BuildClientWGConfig(vpnServerClient *sshhelper.Client, vpnNodeIP, vpnRange, serverPublicIP string, vpnPort int, privateKey string) (string, error) {
	return buildClientWGConfig(vpnServerClient, vpnNodeIP, vpnRange, serverPublicIP, vpnPort, privateKey)
}

// buildClientWGConfig fetches the VPN server's WireGuard public key and actual
// listen port via SSH, then constructs a complete client wg0.conf.
// vpnPort is used only as a fallback when the server's listen-port cannot be read.
func buildClientWGConfig(
	vpnServerClient *sshhelper.Client,
	vpnNodeIP, vpnRange, serverPublicIP string,
	vpnPort int,
	privateKey string,
) (string, error) {
	pubKeyOut, err := sshhelper.Run(vpnServerClient, "sudo wg show wg0 public-key")
	if err != nil {
		return "", fmt.Errorf("reading VPN server public key: %w", err)
	}
	serverPublicKey := strings.TrimSpace(pubKeyOut)
	if serverPublicKey == "" {
		return "", fmt.Errorf("empty public key returned from VPN server")
	}

	// Read the actual listen port from the running interface so the client
	// endpoint is always correct regardless of what vpnPort is set to.
	portOut, portErr := sshhelper.Run(vpnServerClient, "sudo wg show wg0 listen-port")
	if portErr == nil {
		if p := strings.TrimSpace(portOut); p != "" && p != "(none)" {
			vpnPort = 0
			fmt.Sscanf(p, "%d", &vpnPort)
		}
	}
	if vpnPort == 0 {
		vpnPort = 51820
	}

	log.Printf("Building WireGuard client config: server=%s port=%d", serverPublicIP, vpnPort)

	cfg := fmt.Sprintf(`[Interface]
PrivateKey = %s
Address = %s/24

[Peer]
PublicKey = %s
Endpoint = %s:%d
AllowedIPs = %s
PersistentKeepalive = 25
`, privateKey, vpnNodeIP, serverPublicKey, serverPublicIP, vpnPort, vpnRange)

	return cfg, nil
}

// GetNextAvailableIP is the exported form used by cloud-provider provisioners.
func GetNextAvailableIP(vpnRange string, usedIPs []string) (string, error) {
	return getNextAvailableIP(vpnRange, usedIPs)
}

// getNextAvailableIP returns the next IP in vpnRange that is not in usedIPs.
func getNextAvailableIP(vpnRange string, usedIPs []string) (string, error) {
	lastUsedIP := ""
	if len(usedIPs) > 0 {
		lastUsedIP = usedIPs[len(usedIPs)-1]
	}

	ip, ipNet, err := net.ParseCIDR(vpnRange)
	if err != nil {
		return "", fmt.Errorf("invalid VPN range: %w", err)
	}

	var nextIP net.IP
	if lastUsedIP != "" {
		nextIP = net.ParseIP(lastUsedIP)
		if nextIP == nil {
			return "", fmt.Errorf("invalid last used IP: %s", lastUsedIP)
		}
	} else {
		nextIP = ip
	}

	for {
		nextIP = incrementIP(nextIP)
		if !ipNet.Contains(nextIP) {
			return "", fmt.Errorf("no available IPs in VPN range")
		}
		nextIPStr := nextIP.String()
		if !contains(usedIPs, nextIPStr) {
			return nextIPStr, nil
		}
	}
}

func contains(usedIPs []string, nextIPStr string) bool {
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
