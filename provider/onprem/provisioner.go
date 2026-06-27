package onprem

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
	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
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
	reportStep func(string),
) (vpnNodeIP string, publicKey string, err error) {
	if reportStep == nil {
		reportStep = func(string) {}
	}

	log.Printf("Provisioning node %s", nodeProvision.Name)

	// ============================================================
	// Allocate VPN IP — cross-checked against both CR state and
	// the live WireGuard peer list on the VPN server so that a
	// drift between the two sources never causes an IP collision.
	// ============================================================

	vpnNodeIP, err = allocateVPNIP(vpnServerClient,
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
	// Check if wg0 is already up — if so, skip the WireGuard group
	// entirely and use the existing interface IP.
	// ============================================================

	existingWg0IP, _ := sshhelper.Run(sshclient,
		`ip -4 addr show wg0 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1 | head -1`)
	existingWg0IP = strings.TrimSpace(existingWg0IP)

	// ============================================================
	// Provisioning steps on the node — grouped by phase so that
	// reportStep gives the operator visible progress.
	// ============================================================

	type stepGroup struct {
		label string
		cmds  []string
	}

	wgGroup := stepGroup{
		label: "installing WireGuard and configuring VPN tunnel",
		cmds: []string{
			"sudo apt-get install -y wireguard wireguard-tools",
			"sudo mkdir -p /etc/wireguard",
			fmt.Sprintf(`
CURRENT_IP=$(sudo wg show wg0 allowed-ips 2>/dev/null | awk '{print $2}' | cut -d/ -f1 | head -1)
if [ "$CURRENT_IP" = "%s" ]; then
  echo "wg0 already running with correct IP %s, skipping tunnel setup"
else
  sudo systemctl stop wg-quick@wg0 2>/dev/null || true
  sudo wg-quick down wg0 2>/dev/null || true
  sudo ip link delete wg0 2>/dev/null || true
  cat <<'WGEOF' | sudo tee /etc/wireguard/wg0.conf
%s
WGEOF
  sudo chmod 600 /etc/wireguard/wg0.conf
  sudo systemctl enable wg-quick@wg0
  sudo systemctl start wg-quick@wg0
  sleep 5
fi`, vpnNodeIP, vpnNodeIP, wgConfig),
		},
	}
	if existingWg0IP != "" {
		log.Printf("[%s] wg0 already up with IP %s — skipping WireGuard installation and reconfiguration", nodeProvision.Name, existingWg0IP)
		reportStep(fmt.Sprintf("wg0 already configured (%s) — skipping WireGuard setup", existingWg0IP))
		wgGroup = stepGroup{} // skip entirely
	}

	groups := []stepGroup{
		{
			label: "disabling swap and configuring kernel modules",
			cmds: []string{
				"sudo swapoff -a",
				`sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab`,
				`echo -e "overlay\nbr_netfilter" | sudo tee /etc/modules-load.d/k8s.conf`,
				"sudo modprobe overlay",
				"sudo modprobe br_netfilter",
				`echo -e "net.bridge.bridge-nf-call-iptables=1
net.bridge.bridge-nf-call-ip6tables=1
net.ipv4.ip_forward=1" | sudo tee /etc/sysctl.d/k8s.conf`,
				"sudo sysctl --system",
			},
		},
		{
			label: "installing base packages (apt-get update, curl, gnupg)",
			cmds: []string{
				"sudo apt-get update",
				"sudo apt-get install -y ca-certificates curl gnupg apt-transport-https",
			},
		},
		wgGroup,
		{
			label: fmt.Sprintf("installing CRI-O v%s", repoVersion),
			cmds: []string{
				"sudo rm -f /etc/apt/keyrings/cri-o-apt-keyring.gpg",
				fmt.Sprintf(`curl -fsSL https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/Release.key \
		| gpg --dearmor | sudo tee /etc/apt/keyrings/cri-o-apt-keyring.gpg > /dev/null`, repoVersion),
				fmt.Sprintf(`echo "deb [signed-by=/etc/apt/keyrings/cri-o-apt-keyring.gpg] \
		https://download.opensuse.org/repositories/isv:/cri-o:/stable:/v%s/deb/ /" \
		| sudo tee /etc/apt/sources.list.d/cri-o.list > /dev/null`, repoVersion),
				"sudo apt-get update",
				"sudo apt-get install -y jq criu crun conmon",
				// Install crun >= 1.0 from GitHub; the apt version (0.17 on Ubuntu 22.04)
				// rejects OCI spec 1.1.0 generated by CRI-O 1.35 with "unknown version specified".
				// Also overwrite /usr/bin/crun so that PATH-based lookups don't pick up the old one.
				`CRUN_VER=$(curl -fsSL https://api.github.com/repos/containers/crun/releases/latest 2>/dev/null | jq -r .tag_name 2>/dev/null) && \
{ [ -n "$CRUN_VER" ] && [ "$CRUN_VER" != "null" ]; } || CRUN_VER=1.17 && \
sudo curl -fsSL "https://github.com/containers/crun/releases/download/${CRUN_VER}/crun-${CRUN_VER}-linux-amd64" \
  -o /usr/local/bin/crun && \
sudo chmod 0755 /usr/local/bin/crun && \
sudo cp -f /usr/local/bin/crun /usr/bin/crun && \
crun --version`,
				"sudo apt-get install -y cri-o",
				// Hardcode /usr/local/bin/crun — do NOT use command -v crun because that may
				// resolve to /usr/bin/crun (old apt crun) if /usr/bin precedes /usr/local/bin in PATH.
				`sudo mkdir -p /etc/crio/crio.conf.d && \
printf '[crio.runtime.runtimes.crun]\nruntime_path = "/usr/local/bin/crun"\nruntime_type = "oci"\nruntime_root = "/run/crun"\n' \
| sudo tee /etc/crio/crio.conf.d/10-crun.conf`,
				"sudo systemctl enable crio --now || { sudo journalctl -xeu crio.service --no-pager >&2; false; }",

				// Ensure criu runtime dependencies are installed (libnl, libcap, libbsd, libgnutls)
				"sudo apt-get install -y libcap2 libnl-3-200 libbsd0 libgnutls30",

				// Install custom criu (device-restore-with-hook), idempotent on GitID
				fmt.Sprintf(`WANT="%s"; \
CRIU_BIN=$(command -v criu || echo /usr/sbin/criu); \
HAVE=$(criu --version 2>&1 | awk '/GitID:/{print $2}'); \
if [ "$HAVE" = "$WANT" ]; then \
  echo "custom criu $WANT already at $CRIU_BIN, skipping"; \
else \
  curl -fsSL %s -o /tmp/criu && \
  chmod 0755 /tmp/criu && \
  GOT=$(/tmp/criu --version 2>&1 | awk '/GitID:/{print $2}') && \
  [ "$GOT" = "$WANT" ] && \
  sudo install -m 0755 /tmp/criu "$CRIU_BIN" && \
  rm -f /tmp/criu && \
  echo "installed custom criu $WANT at $CRIU_BIN"; \
fi`, kubeadm.CriuGitID, kubeadm.CriuAsset),
				"criu --version || true",
				// Grant CAP_CHECKPOINT_RESTORE capability so criu can run
				"sudo setcap cap_checkpoint_restore+eip /usr/sbin/criu || true",
				"criu check 2>&1 | head -1 || true",

				// Install latest runc, idempotent on version
				fmt.Sprintf(`WANT=%[1]s; \
RUNC_BIN=$(command -v runc || echo /usr/local/sbin/runc); \
HAVE=$(runc --version 2>/dev/null | awk '/^runc version/{print "v"$3}'); \
if [ "$HAVE" = "$WANT" ]; then \
  echo "runc $WANT already installed at $RUNC_BIN, skipping"; \
else \
  curl -fsSL https://github.com/opencontainers/runc/releases/download/%[1]s/runc.amd64 -o /tmp/runc && \
  curl -fsSL https://github.com/opencontainers/runc/releases/download/%[1]s/runc.sha256sum -o /tmp/runc.sha256sum && \
  WSHA=$(awk '/ runc\.amd64$/{print $1}' /tmp/runc.sha256sum) && \
  GSHA=$(sha256sum /tmp/runc | awk '{print $1}') && \
  [ -n "$WSHA" ] && [ "$WSHA" = "$GSHA" ] && \
  sudo install -m 0755 /tmp/runc "$RUNC_BIN" && \
  rm -f /tmp/runc /tmp/runc.sha256sum && \
  echo "installed runc $WANT at $RUNC_BIN"; \
fi`, kubeadm.RuncVersion),
				"runc --version || true",

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
fi`, kubeadm.CrioCommit, kubeadm.CrioAsset),
				`test -f /usr/local/libexec/crio/criu-device-restorer.sh || \
sudo install -D -m 0755 /usr/libexec/crio/criu-device-restorer.sh \
/usr/local/libexec/crio/criu-device-restorer.sh 2>/dev/null || \
echo "WARN: criu-device-restorer.sh missing; restore-from-file may fail"`,
				"sudo crio version || true",
				"sudo crictl info || true",
				"sudo systemctl status crio --no-pager || true",
			},
		},
		{
			label: fmt.Sprintf("installing Kubernetes packages (kubelet/kubeadm/kubectl v%s)", clean),
			cmds: []string{
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
			},
		},
	}

	for _, g := range groups {
		if g.label == "" {
			continue // empty group (e.g. wg0 already up — WireGuard step skipped)
		}
		reportStep(g.label)
		log.Printf("[%s] %s", nodeProvision.Name, g.label)
		for _, cmd := range g.cmds {
			output, err := sshhelper.Run(sshclient, cmd)
			if err != nil {
				return "", "", fmt.Errorf("command failed (%s): %s\nOutput:\n%s", g.label, cmd, output)
			}
		}
	}

	// ============================================================
	// Get actual wg0 IP AFTER tunnel starts
	// ============================================================

	reportStep("reading VPN tunnel IP from node")
	nodeIP, err := kubeadm.GetTunIP(sshclient)
	if err != nil {
		return "", "", fmt.Errorf("failed getting wg0 IP: %w", err)
	}
	log.Printf("[%s] Node VPN IP: %s", nodeProvision.Name, nodeIP)

	// ============================================================
	// Verify connectivity from VPN server to new peer
	// ============================================================

	reportStep(fmt.Sprintf("verifying VPN connectivity to %s", nodeIP))
	verifyVPNConnectivity(vpnServerClient, nodeIP)

	// ============================================================
	// Configure kubelet node-ip (env file only — no restart yet)
	//
	// Write KUBELET_EXTRA_ARGS before kubeadm join so that when kubeadm
	// starts kubelet as part of the join process it picks up --node-ip
	// from /etc/default/kubelet automatically.  Restarting kubelet here
	// would be counterproductive: it would start without a cluster config
	// and generate spurious connection errors before kubeadm even runs.
	// ============================================================

	reportStep(fmt.Sprintf("writing kubelet node-ip config (%s)", nodeIP))
	kubeletEnvCmd := fmt.Sprintf(
		`echo 'KUBELET_EXTRA_ARGS=--node-ip=%s' | sudo tee /etc/default/kubelet`,
		nodeIP,
	)
	if output, err := sshhelper.Run(sshclient, kubeletEnvCmd); err != nil {
		return "", "", fmt.Errorf("failed writing kubelet node-ip env: %w\nOutput:\n%s", err, output)
	}
	if output, err := sshhelper.Run(sshclient, "sudo systemctl daemon-reload"); err != nil {
		return "", "", fmt.Errorf("failed daemon reload: %w\nOutput:\n%s", err, output)
	}

	// ============================================================
	// Join cluster
	//
	// kubeadm join stops any running kubelet, writes its config files
	// (/var/lib/kubelet/kubeadm-flags.env, /var/lib/kubelet/config.yaml),
	// then starts kubelet — at which point /etc/default/kubelet is sourced
	// so our KUBELET_EXTRA_ARGS=--node-ip takes effect for the first real
	// kubelet registration.
	//
	// A 10-minute timeout prevents an indefinite hang if container image
	// pulls stall or the API server becomes temporarily unreachable.
	// ============================================================

	// Always restart CRI-O here to pick up any config changes made above
	// (e.g. new crun path). A passive "is-active || start" misses the case where
	// CRI-O is active but using stale config from a previous failed provisioning run.
	reportStep("restarting CRI-O and waiting for socket readiness")
	if output, err := sshhelper.Run(sshclient, `sudo systemctl daemon-reload && sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; }`); err != nil {
		return "", "", fmt.Errorf("failed to restart CRI-O: %w\nOutput:\n%s", err, output)
	}

	// Wait for CRI-O socket to be ready before join (up to 60s)
	if output, err := sshhelper.Run(sshclient, `for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
test -S /var/run/crio/crio.sock && echo "CRI-O socket ready" && break; \
echo "Waiting for CRI-O socket ($i/20)..."; sleep 3; \
done; \
test -S /var/run/crio/crio.sock || { sudo journalctl -xeu crio.service --no-pager -n 100 >&2; false; }`); err != nil {
		return "", "", fmt.Errorf("CRI-O socket not ready: %w\nOutput:\n%s", err, output)
	}

	reportStep("running kubeadm join (may take several minutes — pulling images and bootstrapping TLS)")
	// Append --cri-socket to use CRI-O instead of defaulting to containerd
	joinCmd := fmt.Sprintf("sudo timeout 600 %s --cri-socket=unix:///var/run/crio/crio.sock", netNodeConfig.Status.ClusterJoinCommand)
	if output, err := sshhelper.Run(sshclient, joinCmd); err != nil {
		return "", "", fmt.Errorf("failed joining cluster: %w\nOutput:\n%s", err, output)
	}
	log.Printf("[%s] kubeadm join completed", nodeProvision.Name)

	// ============================================================
	// Post-join: restart kubelet to ensure --node-ip is active
	//
	// kubeadm may have amended /var/lib/kubelet/kubeadm-flags.env.
	// A single daemon-reload + restart ensures kubelet re-reads both
	// env files and registers the node with the correct VPN IP address.
	// ============================================================

	reportStep("restarting kubelet to apply node-ip after join")
	if output, err := sshhelper.Run(sshclient, "sudo systemctl daemon-reload && sudo systemctl restart kubelet"); err != nil {
		return "", "", fmt.Errorf("failed restarting kubelet after join: %w\nOutput:\n%s", err, output)
	}

	log.Printf("[%s] successfully joined cluster", nodeProvision.Name)

	return nodeIP, publicKey, nil
}

// ReadVPNServerPeers is the exported form of readVPNServerPeers, used by the
// controller during cleanup to look up a peer's public key by IP when the CR
// status no longer holds it (e.g. partial provisioning failure).
func ReadVPNServerPeers(vpnServerClient *sshhelper.Client) (map[string]string, error) {
	return readVPNServerPeers(vpnServerClient)
}

// readVPNServerPeers SSHes to the VPN server and parses "wg show wg0 dump"
// into a map of plainIP → publicKey for every registered peer.
// The first line of the dump is the interface line and is skipped.
// A parse error on an individual line is silently skipped (best-effort).
func readVPNServerPeers(vpnServerClient *sshhelper.Client) (map[string]string, error) {
	out, err := sshhelper.Run(vpnServerClient, "sudo wg show wg0 dump 2>/dev/null || true")
	if err != nil {
		return nil, fmt.Errorf("reading VPN server peers: %w", err)
	}
	peers := make(map[string]string) // plainIP → pubkey
	for i, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if i == 0 || line == "" {
			continue // skip interface header line and blank lines
		}
		// Fields: pubkey preshared-key endpoint allowed-ips last-handshake rx tx keepalive
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		pubkey := fields[0]
		// allowed-ips may be a comma-separated list of CIDRs; iterate all.
		for _, cidr := range strings.Split(fields[3], ",") {
			cidr = strings.TrimSpace(cidr)
			if cidr == "" || cidr == "(none)" {
				continue
			}
			ip, _, parseErr := net.ParseCIDR(cidr)
			if parseErr != nil {
				ip = net.ParseIP(cidr)
			}
			if ip != nil {
				peers[ip.String()] = pubkey
			}
		}
	}
	return peers, nil
}

// allocateVPNIP picks the next free IP in vpnRange that is not used according
// to EITHER crUsedIPs (NodeProvisionNetConfig status) OR the live WireGuard
// peer table on the VPN server.  Using both sources prevents collisions when
// the CR and the server have drifted — e.g. after a controller crash, a manual
// wg peer add, or a partial cleanup.
func allocateVPNIP(vpnServerClient *sshhelper.Client, vpnRange string, crUsedIPs []string) (string, error) {
	serverPeers, err := readVPNServerPeers(vpnServerClient)
	if err != nil {
		// Non-fatal: fall back to CR-only allocation and log the warning.
		log.Printf("Warning: could not read live VPN peer list; falling back to CR state only: %v", err)
		serverPeers = map[string]string{}
	}

	// Build the union of all known-used IPs from both sources.
	usedSet := make(map[string]bool, len(crUsedIPs)+len(serverPeers))
	for _, ip := range crUsedIPs {
		usedSet[ip] = true
	}
	for ip := range serverPeers {
		usedSet[ip] = true
	}
	allUsed := make([]string, 0, len(usedSet))
	for ip := range usedSet {
		allUsed = append(allUsed, ip)
	}

	chosen, err := getNextAvailableIP(vpnRange, allUsed)
	if err != nil {
		return "", err
	}

	// Final sanity-check: if there's a TOCTOU race and the server already has
	// this IP, surface a clear error so the caller can retry.
	if ownerKey, conflict := serverPeers[chosen]; conflict {
		return "", fmt.Errorf(
			"allocated IP %s is already registered on VPN server by peer %s (possible race — will retry)",
			chosen, ownerKey,
		)
	}
	return chosen, nil
}

// AllocateVPNIP is the exported form of allocateVPNIP used by cloud-provider provisioners.
func AllocateVPNIP(vpnServerClient *sshhelper.Client, vpnRange string, crUsedIPs []string) (string, error) {
	return allocateVPNIP(vpnServerClient, vpnRange, crUsedIPs)
}

// RegisterVPNPeer is the exported form used by cloud-provider provisioners.
func RegisterVPNPeer(vpnServerClient *sshhelper.Client, publicKey, vpnNodeIP string) error {
	return registerVPNPeer(vpnServerClient, publicKey, vpnNodeIP)
}

// registerVPNPeer adds the WireGuard peer to the VPN server's running config
// and persists it to /etc/wireguard/wg0.conf.
//
// Conflict detection (before any write):
//   - If the server already has our exact (publicKey, vpnNodeIP) pair → skip
//     the wg set call (already registered, idempotent).
//   - If the server has our vpnNodeIP assigned to a DIFFERENT public key →
//     return an error so the caller can allocate a different IP rather than
//     silently creating a routing conflict.
//
// Conf-file persistence is IP-aware:
//   - Any stale peer block that claims vpnNodeIP with a different key is
//     removed before the new block is appended.
//   - The append is skipped if our public key is already present.
func registerVPNPeer(vpnServerClient *sshhelper.Client, publicKey, vpnNodeIP string) error {
	// ── 1. Read live server state ──────────────────────────────────────────
	serverPeers, readErr := readVPNServerPeers(vpnServerClient)
	if readErr != nil {
		log.Printf("Warning: could not verify peer conflicts before registration: %v", readErr)
		serverPeers = map[string]string{}
	}

	// ── 2. Conflict / idempotency check ────────────────────────────────────
	if existingKey, taken := serverPeers[vpnNodeIP]; taken {
		if existingKey == publicKey {
			log.Printf("Peer %s already registered with IP %s (idempotent) — skipping wg set", publicKey, vpnNodeIP)
			// Still sync the conf file below in case it was missed previously.
		} else {
			return fmt.Errorf(
				"IP %s is already assigned to a different peer (%s) on the VPN server; "+
					"allocate a new IP instead of overwriting an active peer",
				vpnNodeIP, existingKey,
			)
		}
	}

	// ── 3. Update running WireGuard config (upsert by public key) ──────────
	addCmd := fmt.Sprintf(
		"sudo wg set wg0 peer %s allowed-ips %s/32 persistent-keepalive 25",
		publicKey, vpnNodeIP,
	)
	if output, err := sshhelper.Run(vpnServerClient, addCmd); err != nil {
		return fmt.Errorf("wg set peer: %w\nOutput:\n%s", err, output)
	}

	// ── 4. Persist to /etc/wireguard/wg0.conf (IP-aware, idempotent) ───────
	//
	// Step A: Remove any peer block that contains AllowedIPs = <vpnNodeIP>/32
	//         but does NOT contain PublicKey = <publicKey>.  This cleans up
	//         stale entries from a previous node that used the same IP.
	//
	// The awk script buffers each [Peer] block and decides whether to emit or
	// discard it after reading the blank line that terminates the block.
	//
	// Step B: Append our peer block only if our public key is absent.
	persistCmd := fmt.Sprintf(`
set -e
WG_CONF=/etc/wireguard/wg0.conf

# Step A — strip any peer block that claims our IP with a different key.
if sudo test -f "$WG_CONF"; then
  sudo awk -v target_ip="%s/32" -v our_key="%s" '
    /^\[Peer\]/ {
      in_peer=1; buf=$0"\n"; has_ip=0; has_key=0; next
    }
    in_peer {
      buf=buf $0 "\n"
      if ($0 ~ "AllowedIPs" && index($0, target_ip)) has_ip=1
      if ($0 ~ "PublicKey"  && index($0, our_key))   has_key=1
      if (/^[[:space:]]*$/ || /^\[/) {
        # End of block — emit unless it is the stale conflicting entry
        if (!(has_ip && !has_key)) printf "%%s", buf
        if (/^\[/) { in_peer=0; buf=$0"\n"; has_ip=0; has_key=0 } else { in_peer=0; buf="" }
        next
      }
      next
    }
    { print }
    END { if (in_peer && !(has_ip && !has_key)) printf "%%s", buf }
  ' "$WG_CONF" | sudo tee "${WG_CONF}.tmp" > /dev/null
  sudo mv "${WG_CONF}.tmp" "$WG_CONF"
fi

# Step B — append our peer block if not already present.
if ! sudo grep -qF 'PublicKey = %s' "$WG_CONF" 2>/dev/null; then
  printf '\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\nPersistentKeepalive = 25\n' \
    | sudo tee -a "$WG_CONF" > /dev/null
fi`,
		vpnNodeIP, publicKey, // awk -v args (Step A)
		publicKey,            // grep check (Step B)
		publicKey, vpnNodeIP, // printf args (Step B)
	)

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
