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
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	"dcn.ssu.ac.kr/infra/pkg/kubeadm"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	"golang.org/x/crypto/curve25519"
	corev1 "k8s.io/api/core/v1"
)

// NewInClusterProvisioner provisions an on-premises node by:
//  1. Allocating a VPN IP from the range tracked in netNodeConfig (or, when
//     the node already has a working wg0, adopting its existing IP + key).
//  2. Generating a WireGuard keypair.
//  3. Registering the node as a peer on the VPN server via vpnServerClient.
//  4. Installing WireGuard, CRI-O, and Kubernetes packages on the node.
//  5. Joining the cluster.
//
// Returns the allocated VPN IP and the node's WireGuard public key so the
// caller can persist them in the NodeProvisionNetConfig status. Once the peer
// has been registered on the VPN server, EVERY error return still carries the
// allocated vpnNodeIP and publicKey so the caller can record/release the peer.
//
// When nodeProvision.Spec.DisableVPN is set, steps 1-3 and the WireGuard part
// of 4 are skipped, vpnServerClient may be nil, the node's spec.ipAddress is
// used as the node IP, and the returned public key is empty.
//
// ctx is honoured: every remote command runs through sshhelper.RunCtx and
// ctx.Err() is checked between steps.
func NewInClusterProvisioner(
	ctx context.Context,
	nodeProvision *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
	sshclient *sshhelper.Client,
	vpnServerClient *sshhelper.Client,
	netNodeConfig *mlv1alpha1.NodeProvisionNetConfig,
	reportStep func(string),
	runtimeCfg pkgruntime.Config,
) (vpnNodeIP string, publicKey string, err error) {
	if reportStep == nil {
		reportStep = func(string) {}
	}

	log.Printf("Provisioning node %s", nodeProvision.Name)

	noVPN := nodeProvision.Spec.DisableVPN
	registered := false // true once the peer exists on the VPN server

	// Resolve (and validate) the insecure-registry list before any side effect
	// such as VPN peer registration.
	insecureHosts, err := InsecureRegistryHosts(netNodeConfig.Spec.SoftwareConfig)
	if err != nil {
		return "", "", fmt.Errorf("NodeProvisionNetConfig softwareConfig.%w", err)
	}

	// fail returns the error together with the allocated VPN identity when a
	// peer has already been registered, so the caller can persist / release it.
	fail := func(e error) (string, string, error) {
		if registered {
			return vpnNodeIP, publicKey, e
		}
		return "", "", e
	}
	run := func(c *sshhelper.Client, cmd string) (string, error) {
		return sshhelper.RunCtx(ctx, c, cmd)
	}

	// Secrets scrubbed from any error text derived from remote output.
	var secrets []string
	secrets = append(secrets, runtimeCfg.Token)

	var wgConfig string
	adoptedWG := false // node already had a working wg0; its identity was registered as-is
	if noVPN {
		// No tunnel: the node is addressed directly by its own (public) IP.
		vpnNodeIP = strings.TrimSpace(nodeProvision.Spec.IPAddress)
		if net.ParseIP(vpnNodeIP) == nil {
			return "", "", fmt.Errorf("spec.disableVPN requires spec.ipAddress to be an IP address, got %q", nodeProvision.Spec.IPAddress)
		}
		log.Printf("Provisioning node %s without VPN (node IP %s)", nodeProvision.Name, vpnNodeIP)
	} else {
		if netNodeConfig.Spec.VPNRange == nil || *netNodeConfig.Spec.VPNRange == "" {
			return "", "", fmt.Errorf("NodeProvisionNetConfig has no vpnRange configured")
		}
		if vpnServerClient == nil {
			return "", "", fmt.Errorf("VPN server connection is required unless spec.disableVPN is set")
		}
		vpnRange := *netNodeConfig.Spec.VPNRange

		// ============================================================
		// Probe the node's wg0 FIRST. If a tunnel is already up we must
		// register THAT identity (IP + public key) on the VPN server:
		// generating a fresh pair here while skipping the node-side
		// configuration would leave the node on its old key and make
		// later peer cleanup remove the wrong peer.
		// ============================================================
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		existingIP, existingKey := probeNodeWG(ctx, sshclient)
		if existingIP != "" && existingKey != "" && !nodeHasRecentHandshake(ctx, sshclient) {
			// A wg0 that never (or no longer) handshakes with the VPN server
			// (server rebuilt or re-keyed) would make the join time out later:
			// do not adopt it, reconfigure the tunnel with a fresh identity.
			log.Printf("[%s] existing wg0 (IP %s) has no recent handshake with the VPN server — not adopting it, reconfiguring the tunnel", nodeProvision.Name, existingIP)
			existingIP, existingKey = "", ""
		}
		if err := ctx.Err(); err != nil {
			return "", "", err
		}

		// Serialize allocate + register across every provisioner in this process.
		unlock, lerr := LockVPNAllocationCtx(ctx)
		if lerr != nil {
			return "", "", fmt.Errorf("waiting for the VPN allocation lock: %w", lerr)
		}
		func() {
			defer unlock()

			if existingIP != "" && existingKey != "" {
				if verr := validateExistingWG(ctx, vpnServerClient, vpnRange, existingIP, existingKey); verr != nil {
					log.Printf("[%s] existing wg0 (IP %s) cannot be adopted: %v — reconfiguring the tunnel with a fresh identity", nodeProvision.Name, existingIP, verr)
				} else if rerr := registerVPNPeer(ctx, vpnServerClient, existingKey, existingIP); rerr != nil {
					err = fmt.Errorf("failed registering existing wireguard peer on VPN server: %w", rerr)
					return
				} else {
					vpnNodeIP, publicKey = existingIP, existingKey
					registered, adoptedWG = true, true
					log.Printf("[%s] wg0 already up with IP %s — adopted its identity (key %s)", nodeProvision.Name, existingIP, existingKey)
					reportStep(fmt.Sprintf("wg0 already configured (%s) — skipping WireGuard setup", existingIP))
					return
				}
			}

			// ============================================================
			// Allocate VPN IP — cross-checked against both CR state and
			// the live WireGuard peer list on the VPN server so that a
			// drift between the two sources never causes an IP collision.
			// ============================================================
			ip, aerr := allocateVPNIP(ctx, vpnServerClient, vpnRange, netNodeConfig.Status.UsedIPAddresses)
			if aerr != nil {
				err = fmt.Errorf("failed to allocate VPN IP: %w", aerr)
				return
			}

			privateKey, pub, kerr := generateWireGuardKeyPair()
			if kerr != nil {
				err = fmt.Errorf("failed generating wireguard keys: %w", kerr)
				return
			}
			secrets = append(secrets, privateKey)

			cfg, cerr := buildClientWGConfig(
				ctx,
				vpnServerClient,
				ip,
				vpnRange,
				netNodeConfig.Spec.VPNServerPublicConfig.PublicIP,
				parsePort(netNodeConfig.Spec.VPNServerPublicConfig.VPNPort, 51820),
				privateKey,
			)
			if cerr != nil {
				err = fmt.Errorf("failed building wireguard client config: %w", cerr)
				return
			}

			// Register the peer before the client interface comes up so the
			// server is ready to accept the handshake.
			if rerr := registerVPNPeer(ctx, vpnServerClient, pub, ip); rerr != nil {
				err = fmt.Errorf("failed registering wireguard peer on VPN server: %w", rerr)
				return
			}
			vpnNodeIP, publicKey, wgConfig = ip, pub, cfg
			registered = true
		}()
		if err != nil {
			return fail(err)
		}
	}

	// ============================================================
	// Kubernetes version parsing
	// ============================================================

	clean, repoVersion, verr := kubeadm.ParseKubernetesVersion(netNodeConfig.Spec.SoftwareConfig.KubernetesVersion)
	if verr != nil {
		return fail(verr)
	}
	// The join command is embedded in a shell script run on the node.
	if jerr := kubeadm.ValidateJoinCommand(netNodeConfig.Status.ClusterJoinCommand); jerr != nil {
		return fail(jerr)
	}

	// A node that is already HEALTHILY joined to this cluster (resume after a
	// controller restart or an SSH drop) must not be wiped and re-installed:
	// only the tunnel (if it needed a fresh identity) is set up and the run is
	// reported as done.
	apiEndpoint := kubeadm.JoinEndpoint(netNodeConfig.Status.ClusterJoinCommand)
	alreadyJoined := nodeAlreadyJoined(ctx, sshclient, apiEndpoint)
	if alreadyJoined {
		log.Printf("[%s] node is already healthily joined to the cluster — skipping cleanup, install and join", nodeProvision.Name)
		reportStep("node already joined to the cluster — skipping install and join")
	}

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
			"sudo DEBIAN_FRONTEND=noninteractive apt-get install -y wireguard wireguard-tools",
			"sudo mkdir -p /etc/wireguard",
			// The config holds the node's private key: create the file 0600 BEFORE
			// writing to it, and do not echo it back (tee > /dev/null).
			fmt.Sprintf(`
sudo systemctl stop wg-quick@wg0 2>/dev/null || true
sudo wg-quick down wg0 2>/dev/null || true
sudo ip link delete wg0 2>/dev/null || true
sudo install -m 0600 -o root -g root /dev/null /etc/wireguard/wg0.conf
cat <<'WGEOF' | sudo tee /etc/wireguard/wg0.conf > /dev/null
%s
WGEOF
sudo systemctl enable wg-quick@wg0
sudo systemctl start wg-quick@wg0 || true
WG_OK=0
for i in $(seq 1 15); do
  if systemctl is-active --quiet wg-quick@wg0 && ip link show wg0 >/dev/null 2>&1; then WG_OK=1; break; fi
  sleep 1
done
if [ "$WG_OK" != "1" ]; then
  echo "wg-quick@wg0 failed to start" >&2
  sudo systemctl status wg-quick@wg0 --no-pager -l >&2 || true
  sudo journalctl -u wg-quick@wg0 --no-pager -n 30 >&2 || true
  exit 1
fi
sleep 2`, wgConfig),
		},
	}
	if noVPN {
		wgGroup = stepGroup{} // no tunnel to set up
	} else if adoptedWG {
		wgGroup = stepGroup{} // node's existing tunnel is kept as-is
	}

	groups := []stepGroup{
		{
			label: "cleaning up previous provisioning state",
			cmds: []string{
				// Reset kubeadm state first (before CRI-O is stopped so containers
				// are cleaned up) when kubeadm exists; a re-run must not just wipe
				// container storage under a still-configured kubelet. A node that
				// is healthily joined to THIS cluster is never reset.
				kubeadmResetCmd(apiEndpoint),
				"sudo systemctl stop kubelet crio 2>/dev/null || true",
				"sudo rm -rf /etc/cni/net.d 2>/dev/null || true",
				// kubeadm reset does not remove CNI-created virtual interfaces; stale
				// ones make the next flanneld fail with "address already in use".
				"sudo ip link delete flannel.1 2>/dev/null || true",
				"sudo ip link delete cni0 2>/dev/null || true",
				"sudo ip link delete kube-ipvs0 2>/dev/null || true",
				`awk '$2~/^\/var\/lib\/containers|^\/run\/containers/{print $2}' /proc/mounts | sort -r | xargs -r sudo umount -l 2>/dev/null || true`,
				"sudo umount -l /var/lib/crio 2>/dev/null || true",
				"sudo rm -rf /var/lib/crio /run/crio /run/containers 2>/dev/null || true",
				"sudo rm -rf /var/lib/containers /etc/containers 2>/dev/null || true",
				"sudo rm -rf /var/log/crio /etc/crio /etc/cdi 2>/dev/null || true",
				"sudo rm -rf /var/lib/kubelet/kubeadm-flags.env 2>/dev/null || true",
				// Remove CRI-O and runtime binaries so reinstall always uses a known-good path.
				"sudo rm -f /usr/bin/crio /usr/local/bin/crio 2>/dev/null || true",
				"sudo rm -f /usr/local/bin/crun /usr/bin/crun 2>/dev/null || true",
				"sudo rm -f /usr/sbin/runc /usr/local/sbin/runc 2>/dev/null || true",
				"sudo rm -f /usr/sbin/criu /usr/local/bin/crictl /usr/bin/crictl 2>/dev/null || true",
			},
		},
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
				"sudo DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl gnupg apt-transport-https",
			},
		},
		wgGroup,
		{
			label: "installing cnlab-runtime (CRI-O + CRIU + runc + crictl via OCI artifact)",
			// The registries.conf.d drop-ins are written right after the install and
			// before CRI-O is enabled/started below, which is when CRI-O reads them.
			cmds: withInsecureRegistries(pkgruntime.InstallSteps(runtimeCfg), insecureHosts),
		},
		{
			label: "stopping CRI-O and wiping stale container storage before service enable",
			cmds: []string{
				"sudo systemctl stop crio 2>/dev/null || true",
				`awk '$2~/^\/var\/lib\/containers|^\/run\/containers/{print $2}' /proc/mounts | sort -r | xargs -r sudo umount -l 2>/dev/null || true`,
				"sudo rm -rf /var/lib/crio /run/crio /var/lib/containers /run/containers 2>/dev/null || true",
				"sudo systemctl daemon-reload",
				"sudo systemctl enable crio",
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
					"sudo DEBIAN_FRONTEND=noninteractive apt-get install -y kubelet=%s-* kubeadm=%s-* kubectl=%s-* --allow-change-held-packages --allow-downgrades",
					clean, clean, clean,
				),
				"sudo apt-mark hold kubelet kubeadm kubectl",
				"sudo systemctl enable kubelet",
				"sudo systemctl daemon-reload",
			},
		},
	}

	// runStep runs one remote command under pipefail; errors carry the step
	// label and scrubbed output only — never the command text.
	runStep := func(label string, idx, total int, cmd string) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		output, rerr := run(sshclient, "set -o pipefail\n"+cmd)
		if rerr != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			return sshhelper.StepError(fmt.Sprintf("%s (step %d/%d)", label, idx, total), rerr, output, secrets...)
		}
		return nil
	}

	if noVPN {
		// kubelet refuses to start with a --node-ip that is not bound to a local
		// interface (e.g. a public IP that is 1:1-NATed by the provider). Check it
		// before the long install steps so a wrong spec.ipAddress fails at once
		// with an actionable message instead of after many minutes of work.
		reportStep(fmt.Sprintf("verifying %s is assigned to a local interface", vpnNodeIP))
		if err := kubeadm.VerifyLocalIP(sshclient, vpnNodeIP); err != nil {
			return fail(fmt.Errorf("spec.ipAddress: %w", err))
		}
	}

	for _, g := range groups {
		if g.label == "" {
			continue // empty group (e.g. wg0 already up — WireGuard step skipped)
		}
		if alreadyJoined && g.label != wgGroup.label {
			continue // healthy joined node: leave everything but the tunnel alone
		}
		reportStep(g.label)
		log.Printf("[%s] %s", nodeProvision.Name, g.label)
		for i, cmd := range g.cmds {
			if rerr := runStep(g.label, i+1, len(g.cmds), cmd); rerr != nil {
				return fail(rerr)
			}
		}
	}

	var nodeIP string
	if noVPN {
		// Already verified to be bound locally before any provisioning step.
		nodeIP = vpnNodeIP
	} else {
		// ============================================================
		// Get actual wg0 IP AFTER tunnel starts
		// ============================================================

		reportStep("reading VPN tunnel IP from node")
		nodeIP, err = kubeadm.GetTunIP(sshclient)
		if err != nil {
			return fail(fmt.Errorf("failed getting wg0 IP: %w", err))
		}
		log.Printf("[%s] Node VPN IP: %s", nodeProvision.Name, nodeIP)
		if nodeIP != vpnNodeIP {
			return fail(fmt.Errorf("wg0 on the node has IP %s but %s was registered on the VPN server", nodeIP, vpnNodeIP))
		}

		// ============================================================
		// Verify connectivity from VPN server to new peer
		// ============================================================

		reportStep(fmt.Sprintf("verifying VPN connectivity to %s", nodeIP))
		verifyVPNConnectivity(vpnServerClient, nodeIP)
	}

	if alreadyJoined {
		log.Printf("[%s] node already joined; tunnel verified", nodeProvision.Name)
		return nodeIP, publicKey, nil
	}

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
	if rerr := runStep("writing kubelet node-ip env", 1, 1, kubeletEnvCmd); rerr != nil {
		return fail(rerr)
	}
	if rerr := runStep("systemd daemon-reload", 1, 1, "sudo systemctl daemon-reload"); rerr != nil {
		return fail(rerr)
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
	if rerr := runStep("restarting CRI-O", 1, 1, `sudo systemctl daemon-reload && sudo systemctl restart crio || { sudo journalctl -xeu crio.service --no-pager >&2; false; }`); rerr != nil {
		return fail(rerr)
	}

	// Wait for CRI-O socket to be ready before join (up to 60s)
	if rerr := runStep("waiting for the CRI-O socket", 1, 1, `for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
test -S /var/run/crio/crio.sock && echo "CRI-O socket ready" && break; \
echo "Waiting for CRI-O socket ($i/20)..."; sleep 3; \
done; \
test -S /var/run/crio/crio.sock || { sudo journalctl -xeu crio.service --no-pager -n 100 >&2; false; }`); rerr != nil {
		return fail(rerr)
	}

	// CRI-O recovery: when swapping binaries (especially custom CRI-O), the image cache
	// can become inconsistent, causing containers to fail with "image not found" errors.
	// Force a clean reset: stop services, unmount overlay storage, clear cache, restart.
	reportStep("checking CRI-O image cache consistency")
	if rerr := runStep("CRI-O image cache recovery", 1, 1, `if ! sudo systemctl is-active crio >/dev/null 2>&1 || ! test -S /var/run/crio/crio.sock; then \
  echo "CRI-O not responding, force-resetting image cache"; \
  sudo systemctl stop kubelet crio 2>/dev/null || true; \
  awk '$2~/^\/var\/lib\/containers|^\/run\/containers/{print $2}' /proc/mounts | sort -r | xargs -r sudo umount -l 2>/dev/null || true; \
  sudo umount -l /var/lib/crio 2>/dev/null || true; \
  sudo rm -rf /var/lib/crio /run/crio /var/lib/containers /run/containers 2>/dev/null || true; \
  sudo systemctl restart crio; \
  sleep 5; \
  for i in 1 2 3 4 5; do \
    test -S /var/run/crio/crio.sock && break; \
    sleep 3; \
  done; \
  sudo systemctl restart kubelet; \
  sleep 10; \
fi`); rerr != nil {
		return fail(rerr)
	}

	reportStep("running kubeadm join (may take several minutes — pulling images and bootstrapping TLS)")
	// Append --cri-socket to use CRI-O instead of defaulting to containerd
	joinCmd := fmt.Sprintf("sudo timeout 600 %s --cri-socket=unix:///var/run/crio/crio.sock", strings.TrimSpace(netNodeConfig.Status.ClusterJoinCommand))
	if rerr := runStep("kubeadm join", 1, 1, joinCmd); rerr != nil {
		return fail(rerr)
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
	if rerr := runStep("restarting kubelet after join", 1, 1, "sudo systemctl daemon-reload && sudo systemctl restart kubelet"); rerr != nil {
		return fail(rerr)
	}

	log.Printf("[%s] successfully joined cluster", nodeProvision.Name)

	return nodeIP, publicKey, nil
}

// kubeadmResetCmd returns the cleanup step that resets kubeadm state when
// kubeadm is installed, unless the node is healthily joined to the cluster whose
// API server is apiEndpoint (see kubeadm.NodeAlreadyJoinedFunc).
func kubeadmResetCmd(apiEndpoint string) string {
	return kubeadm.NodeAlreadyJoinedFunc + `
if command -v kubeadm >/dev/null 2>&1; then
  if node_already_joined ` + sshhelper.ShellQuote(apiEndpoint) + `; then
    echo "node is healthily joined to the cluster; not resetting kubeadm state"
  else
    sudo kubeadm reset --force --cri-socket=unix:///var/run/crio/crio.sock >/dev/null 2>&1 || true
  fi
fi`
}

// nodeAlreadyJoined reports whether the node is healthily joined to the cluster
// at apiEndpoint (kubelet.conf points at it, kubelet is active and the API
// answers /healthz). Any error means "not joined".
func nodeAlreadyJoined(ctx context.Context, sshclient *sshhelper.Client, apiEndpoint string) bool {
	_, err := sshhelper.RunCtx(ctx, sshclient, kubeadm.NodeAlreadyJoinedFunc+"\nnode_already_joined "+sshhelper.ShellQuote(apiEndpoint))
	return err == nil
}

// vpnCmdTimeout bounds each command run on the VPN server.
const vpnCmdTimeout = 2 * time.Minute

// vpnAllocSem serializes "allocate a VPN IP" through "register the peer" across
// every provisioner in this process (on-prem and cloud): without it two
// concurrent provisions read the same peer table and pick the same IP. It is a
// channel semaphore (not a sync.Mutex) so that waiting for it can be abandoned
// when the caller's context ends.
var vpnAllocSem = make(chan struct{}, 1)

// LockVPNAllocationCtx takes the process-wide VPN allocation lock, giving up
// with ctx.Err() as soon as ctx is done while waiting, and returns the function
// that releases it (idempotent). Hold it from AllocateVPNIP until
// RegisterVPNPeer has finished (or use AllocateAndRegisterVPNPeer, which does so
// itself).
func LockVPNAllocationCtx(ctx context.Context) (unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case vpnAllocSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() { once.Do(func() { <-vpnAllocSem }) }, nil
}

// LockVPNAllocation is LockVPNAllocationCtx without cancellation: it blocks
// until the lock is free. Prefer the Ctx variant.
func LockVPNAllocation() (unlock func()) {
	unlock, _ = LockVPNAllocationCtx(context.Background())
	return unlock
}

// vpnRun runs one command on the VPN server with a per-command timeout. The
// returned output merges stderr into stdout: use it for diagnostics only.
func vpnRun(ctx context.Context, c *sshhelper.Client, cmd string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, vpnCmdTimeout)
	defer cancel()
	return sshhelper.RunCtx(cctx, c, cmd)
}

// vpnRunStdout is vpnRun for output that is PARSED: only stdout is returned, so
// noise such as "sudo: unable to resolve host <name>" cannot corrupt keys,
// ports or peer tables.
func vpnRunStdout(ctx context.Context, c *sshhelper.Client, cmd string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, vpnCmdTimeout)
	defer cancel()
	return sshhelper.RunStdoutCtx(cctx, c, cmd)
}

var (
	wgKeyRE       = regexp.MustCompile(`^[A-Za-z0-9+/]{43}=$`)
	hostOrIPRE    = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.:-]*[A-Za-z0-9])?$`)
	ipv4AddrLineR = regexp.MustCompile(`inet (\d+\.\d+\.\d+\.\d+)/(\d+)`)
)

// VPNPeer is the identity produced by AllocateAndRegisterVPNPeer.
type VPNPeer struct {
	IP        string
	PublicKey string
	// WGConfig is the complete client wg0.conf (contains the private key; never log it).
	WGConfig string
}

// AllocateAndRegisterVPNPeer allocates a free VPN IP, generates a keypair,
// builds the client wg0.conf and registers the peer on the VPN server, all
// under the process-wide allocation lock. On failure nothing stays registered
// (a half-registered peer is removed best-effort).
func AllocateAndRegisterVPNPeer(ctx context.Context, vpnServerClient *sshhelper.Client, vpnRange string, crUsedIPs []string, serverPublicIP string, vpnPort int) (*VPNPeer, error) {
	unlock, err := LockVPNAllocationCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("waiting for the VPN allocation lock: %w", err)
	}
	defer unlock()

	ip, err := allocateVPNIP(ctx, vpnServerClient, vpnRange, crUsedIPs)
	if err != nil {
		return nil, fmt.Errorf("allocating VPN IP: %w", err)
	}
	priv, pub, err := generateWireGuardKeyPair()
	if err != nil {
		return nil, fmt.Errorf("generating WireGuard keypair: %w", err)
	}
	cfg, err := buildClientWGConfig(ctx, vpnServerClient, ip, vpnRange, serverPublicIP, vpnPort, priv)
	if err != nil {
		return nil, fmt.Errorf("building WireGuard client config: %w", err)
	}
	// registerVPNPeer removes the peer again when it fails after having added it.
	if err := registerVPNPeer(ctx, vpnServerClient, pub, ip); err != nil {
		return nil, fmt.Errorf("registering VPN peer: %w", err)
	}
	return &VPNPeer{IP: ip, PublicKey: pub, WGConfig: cfg}, nil
}

// ReadVPNServerPeers is the exported form of readVPNServerPeers, used by the
// controller during cleanup to look up a peer's public key by IP when the CR
// status no longer holds it (e.g. partial provisioning failure).
func ReadVPNServerPeers(vpnServerClient *sshhelper.Client) (map[string]string, error) {
	return readVPNServerPeers(context.Background(), vpnServerClient)
}

// ReadVPNServerPeersCtx is ReadVPNServerPeers honouring ctx.
func ReadVPNServerPeersCtx(ctx context.Context, vpnServerClient *sshhelper.Client) (map[string]string, error) {
	return readVPNServerPeers(ctx, vpnServerClient)
}

// readVPNServerPeers SSHes to the VPN server and parses "wg show wg0 dump"
// into a map of plainIP → publicKey for every registered peer.
// The first line of the dump is the interface line and is skipped.
// A parse error on an individual line is silently skipped (best-effort).
func readVPNServerPeers(ctx context.Context, vpnServerClient *sshhelper.Client) (map[string]string, error) {
	out, err := vpnRunStdout(ctx, vpnServerClient, "sudo wg show wg0 dump 2>/dev/null || true")
	if err != nil {
		return nil, fmt.Errorf("reading VPN server peers: %w", err)
	}
	return parseWGDump(out), nil
}

// parseWGDump parses "wg show wg0 dump" output into plainIP → publicKey. The
// first line is the interface line; lines that are not peer lines are skipped.
func parseWGDump(out string) map[string]string {
	peers := make(map[string]string) // plainIP → pubkey
	for i, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if i == 0 || line == "" {
			continue // skip interface header line and blank lines
		}
		// Fields: pubkey preshared-key endpoint allowed-ips last-handshake rx tx keepalive
		fields := strings.Fields(line)
		if len(fields) < 4 || !wgKeyRE.MatchString(fields[0]) {
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
	return peers
}

// readVPNServerAddresses returns the IPv4 addresses assigned to the VPN
// server's own wg0 interface (these must never be handed to a node).
func readVPNServerAddresses(ctx context.Context, vpnServerClient *sshhelper.Client) ([]string, error) {
	out, err := vpnRunStdout(ctx, vpnServerClient, "ip -4 -o addr show dev wg0 2>/dev/null || true")
	if err != nil {
		return nil, fmt.Errorf("reading VPN server wg0 address: %w", err)
	}
	var addrs []string
	for _, m := range ipv4AddrLineR.FindAllStringSubmatch(out, -1) {
		addrs = append(addrs, m[1])
	}
	return addrs, nil
}

// allocateVPNIP picks the next free IP in vpnRange that is not used according
// to EITHER crUsedIPs (NodeProvisionNetConfig status) OR the live WireGuard
// peer table on the VPN server, and is never the network address, the
// broadcast address or the VPN server's own wg0 address. Using both sources
// prevents collisions when the CR and the server have drifted — e.g. after a
// controller crash, a manual wg peer add, or a partial cleanup.
//
// Callers must hold LockVPNAllocation from this call until the peer is
// registered (AllocateAndRegisterVPNPeer does so).
func allocateVPNIP(ctx context.Context, vpnServerClient *sshhelper.Client, vpnRange string, crUsedIPs []string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	serverPeers, err := readVPNServerPeers(ctx, vpnServerClient)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return "", cerr
		}
		// Non-fatal: fall back to CR-only allocation and log the warning.
		log.Printf("Warning: could not read live VPN peer list; falling back to CR state only: %v", err)
		serverPeers = map[string]string{}
	}

	// Build the union of all known-used IPs from both sources.
	usedSet := make(map[string]bool, len(crUsedIPs)+len(serverPeers)+2)
	for _, ip := range crUsedIPs {
		usedSet[ip] = true
	}
	for ip := range serverPeers {
		usedSet[ip] = true
	}

	// The VPN server's own tunnel address. Prefer reading it from the server;
	// if that fails, fall back to the host part of the configured range when it
	// is written in "server address/prefix" form (e.g. 10.8.0.1/24).
	serverAddrs, addrErr := readVPNServerAddresses(ctx, vpnServerClient)
	if addrErr != nil {
		log.Printf("Warning: %v; assuming the range's own address is the server's", addrErr)
	}
	for _, a := range serverAddrs {
		usedSet[a] = true
	}
	if ip, ipNet, perr := net.ParseCIDR(vpnRange); perr == nil {
		if !ip.Equal(ipNet.IP) {
			usedSet[ip.String()] = true
		}
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

// AllocateVPNIP is the exported form of allocateVPNIP used by cloud-provider
// provisioners. The caller must hold LockVPNAllocation until the peer is
// registered; prefer AllocateAndRegisterVPNPeer.
func AllocateVPNIP(vpnServerClient *sshhelper.Client, vpnRange string, crUsedIPs []string) (string, error) {
	return allocateVPNIP(context.Background(), vpnServerClient, vpnRange, crUsedIPs)
}

// RegisterVPNPeer is the exported form used by cloud-provider provisioners.
// The caller must hold LockVPNAllocation (see AllocateVPNIP).
func RegisterVPNPeer(vpnServerClient *sshhelper.Client, publicKey, vpnNodeIP string) error {
	return registerVPNPeer(context.Background(), vpnServerClient, publicKey, vpnNodeIP)
}

// probeNodeWG reports the IPv4 address and public key of the node's existing
// wg0 interface ("" for both when there is none or it cannot be read). Only
// stdout is parsed, so "sudo: unable to resolve host" noise cannot hide a key.
func probeNodeWG(ctx context.Context, sshclient *sshhelper.Client) (ip, pubKey string) {
	out, err := sshhelper.RunStdoutCtx(ctx, sshclient,
		`ip -4 addr show wg0 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1 | head -1`)
	if err != nil {
		return "", ""
	}
	ip = strings.TrimSpace(out)
	if net.ParseIP(ip).To4() == nil {
		return "", ""
	}
	keyOut, err := sshhelper.RunStdoutCtx(ctx, sshclient, `sudo wg show wg0 public-key 2>/dev/null`)
	if err != nil {
		return "", ""
	}
	pubKey = strings.TrimSpace(keyOut)
	if !wgKeyRE.MatchString(pubKey) {
		return "", ""
	}
	return ip, pubKey
}

// wgHandshakeMaxAge is how old the node's latest WireGuard handshake may be for
// its existing wg0 to count as a working tunnel (persistent keepalive keeps a
// live tunnel's handshake well under this; WireGuard itself rejects sessions
// after 3 minutes).
const wgHandshakeMaxAge = 3 * time.Minute

// nodeHasRecentHandshake reports whether the node's wg0 completed a handshake
// with the VPN server within wgHandshakeMaxAge. A tunnel whose server was
// rebuilt or re-keyed never handshakes, and adopting it would only produce a
// join timeout later; such a node must be reconfigured with a fresh identity.
// The age is computed against the NODE's clock (date +%s is read in the same
// command), so clock skew between the controller and the node is irrelevant.
func nodeHasRecentHandshake(ctx context.Context, sshclient *sshhelper.Client) bool {
	out, err := sshhelper.RunStdoutCtx(ctx, sshclient, `date +%s; sudo wg show wg0 latest-handshakes 2>/dev/null || true`)
	if err != nil {
		return false
	}
	return hasRecentHandshake(out, wgHandshakeMaxAge)
}

// hasRecentHandshake parses "<now epoch>\n<peerkey>\t<handshake epoch>..." (the
// first line is the reference time) and reports whether any peer has a
// non-zero handshake younger than maxAge.
func hasRecentHandshake(out string, maxAge time.Duration) bool {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return false
	}
	now, err := strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64)
	if err != nil || now <= 0 {
		return false
	}
	for _, line := range lines[1:] {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		ts, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil || ts <= 0 {
			continue // 0 = never
		}
		if age := now - ts; age >= 0 && time.Duration(age)*time.Second < maxAge {
			return true
		}
	}
	return false
}

// validateExistingWG decides whether the node's existing wg0 identity can be
// adopted: its IP must be a usable host address inside vpnRange, must not be
// the VPN server's own address, and must not be held by a different key.
func validateExistingWG(ctx context.Context, vpnServerClient *sshhelper.Client, vpnRange, ip, pubKey string) error {
	if err := ipUsableInRange(vpnRange, ip); err != nil {
		return err
	}
	if addrs, err := readVPNServerAddresses(ctx, vpnServerClient); err == nil {
		for _, a := range addrs {
			if a == ip {
				return fmt.Errorf("IP %s is the VPN server's own address", ip)
			}
		}
	}
	peers, err := readVPNServerPeers(ctx, vpnServerClient)
	if err != nil {
		return ctx.Err() // cannot tell; registerVPNPeer re-checks
	}
	if holder, taken := peers[ip]; taken && holder != pubKey {
		return fmt.Errorf("IP %s is already assigned to a different peer (%s) on the VPN server", ip, holder)
	}
	return ctx.Err()
}

// ipUsableInRange reports an error unless ip is a host address (not the network
// or broadcast address) inside vpnRange.
func ipUsableInRange(vpnRange, ip string) error {
	_, ipNet, err := net.ParseCIDR(vpnRange)
	if err != nil {
		return fmt.Errorf("invalid VPN range: %w", err)
	}
	parsed := net.ParseIP(ip).To4()
	if parsed == nil || !ipNet.Contains(parsed) {
		return fmt.Errorf("IP %s is not inside the VPN range %s", ip, vpnRange)
	}
	if ones, bits := ipNet.Mask.Size(); bits == 32 && ones < 31 {
		if parsed.Equal(ipNet.IP) || parsed.Equal(broadcastIP(ipNet)) {
			return fmt.Errorf("IP %s is the network or broadcast address of %s", ip, vpnRange)
		}
	}
	return nil
}

// wgConfAwkPeerRewrite is the awk program shared by persistPeerScript and
// removePeerScript. It walks wg0.conf as a sequence of sections and treats each
// [Peer] block as: the comment/blank lines immediately PRECEDING its header (its
// label), the header, and its body up to the last non-comment line. Comment and
// blank lines are buffered in `hold` and attached to the FOLLOWING [Peer] block
// (or printed before the next non-peer header / at EOF), so dropping a peer never
// drops or shifts a neighbour's label. A block is kept unless `drop()` says so.
//
// The program expects awk variables target_ip and our_key (either may be empty
// for the removal script) and the awk function drop_block(has_ip, has_key).
const wgConfAwkPeerRewrite = `
function flush() {
  if (n > 0 && !drop_block(has_ip, has_key)) printf "%s", buf
  buf = ""; n = 0; has_ip = 0; has_key = 0; in_peer = 0
}
/^[[:space:]]*(#|$)/ { hold = hold $0 "\n"; next }
/^[[:space:]]*\[Peer\][[:space:]]*$/ { flush(); buf = hold $0 "\n"; hold = ""; n = 1; in_peer = 1; next }
/^[[:space:]]*\[/ { flush(); printf "%s", hold; hold = ""; print; next }
in_peer {
  buf = buf hold $0 "\n"; hold = ""
  if ($0 ~ /^[[:space:]]*AllowedIPs[[:space:]]*=/) {
    v = $0; sub(/^[^=]*=/, "", v); m = split(v, a, ",")
    for (i = 1; i <= m; i++) { gsub(/[[:space:]]/, "", a[i]); if (target_ip != "" && a[i] == target_ip) has_ip = 1 }
  }
  if ($0 ~ /^[[:space:]]*PublicKey[[:space:]]*=/) {
    v = $0; sub(/^[^=]*=/, "", v); gsub(/[[:space:]]/, "", v); if (v == our_key) has_key = 1
  }
  next
}
{ printf "%s", hold; hold = ""; print }
END { flush(); printf "%s", hold }
`

// wgConfOutsideFunc defines outside_of FILE for bash: it prints everything in
// FILE that is NOT part of a [Peer] block (the [Interface] section and any other
// section, with their comments), using the same block boundaries as
// wgConfAwkPeerRewrite. The rewrite scripts require the output for the old and
// the new file to be byte-identical, i.e. only peer blocks may change. Unlike a
// "PrivateKey line still present" check this also works for servers that load
// their key through PostUp/PreUp or a key file.
const wgConfOutsideFunc = `outside_of() {
  awk '
    /^[[:space:]]*(#|$)/ { hold = hold $0 "\n"; next }
    /^[[:space:]]*\[Peer\][[:space:]]*$/ { hold = ""; in_peer = 1; next }
    /^[[:space:]]*\[/ { printf "%s", hold; hold = ""; in_peer = 0; print; next }
    in_peer { hold = ""; next }
    { printf "%s", hold; hold = ""; print }
    END { printf "%s", hold }
  ' "$1"
}`

// wgConfSanityChecks runs after the rewritten config is in $TMP. It refuses to
// replace $WG_CONF unless the result is non-empty, still has an [Interface]
// section, did not gain peers and differs from the original ONLY inside [Peer]
// blocks.
const wgConfSanityChecks = `[ -s "$TMP" ] || { echo "refusing to replace $WG_CONF with an empty file" >&2; exit 1; }
grep -q '^[[:space:]]*\[Interface\]' "$TMP" || { echo "rewritten config lost [Interface]" >&2; exit 1; }
cmp -s <(outside_of "$WG_CONF") <(outside_of "$TMP") || { echo "rewritten config changed something outside the [Peer] blocks" >&2; exit 1; }
BEFORE=$(grep -c '^[[:space:]]*\[Peer\]' "$WG_CONF" || true)
AFTER=$(grep -c '^[[:space:]]*\[Peer\]' "$TMP" || true)
[ "$AFTER" -le "$BEFORE" ] || { echo "rewritten config gained peers" >&2; exit 1; }`

// persistPeerScript rewrites /etc/wireguard/wg0.conf on the VPN server. It runs
// as root (via sudo ... flock) with TARGET_IP and OUR_KEY in the environment.
//
//   - flock serialises concurrent editors (this controller's cleanup uses the
//     same lock file).
//   - The new content is built in a 0600 temp file in the same directory and
//     only moved into place when it passes wgConfSanityChecks (non-empty, still
//     has [Interface], peer count did not grow, nothing outside the [Peer]
//     blocks changed), so a failed/truncated awk never replaces the live
//     config. The result keeps root:root 0600 — wg0.conf contains the server's
//     PrivateKey.
//   - The awk rewrite treats a [Peer] block as running until the next section
//     header (so blocks without blank-line separators keep their header),
//     keeps each block's leading comments with it, and drops blocks that claim
//     our IP under another key or our key under another IP.
//
// WG_CONF may be overridden in the environment (tests only).
const persistPeerScript = `set -euo pipefail
WG_CONF="${WG_CONF:-/etc/wireguard/wg0.conf}"
` + wgConfOutsideFunc + `
[ -f "$WG_CONF" ] || install -m 0600 -o root -g root /dev/null "$WG_CONF"
if [ -s "$WG_CONF" ]; then
  TMP=$(mktemp "${WG_CONF}.XXXXXX")
  trap 'rm -f "$TMP"' EXIT
  awk -v target_ip="${TARGET_IP}/32" -v our_key="$OUR_KEY" '
    function drop_block(has_ip, has_key) { return (has_ip && !has_key) || (has_key && !has_ip) }
` + wgConfAwkPeerRewrite + `  ' "$WG_CONF" > "$TMP"
` + wgConfSanityChecks + `
  chown root:root "$TMP"
  chmod 0600 "$TMP"
  mv -f "$TMP" "$WG_CONF"
  trap - EXIT
fi
chmod 0600 "$WG_CONF"
if ! awk -v k="$OUR_KEY" '/^[[:space:]]*PublicKey[[:space:]]*=/ { v = $0; sub(/^[^=]*=/, "", v); gsub(/[[:space:]]/, "", v); if (v == k) f = 1 } END { exit !f }' "$WG_CONF"; then
  printf '\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\nPersistentKeepalive = 25\n' "$OUR_KEY" "$TARGET_IP" >> "$WG_CONF"
fi`

// removePeerScript drops the [Peer] block whose PublicKey is OUR_KEY from
// /etc/wireguard/wg0.conf on the VPN server (together with the comment lines
// that label it). Same guarantees as persistPeerScript: serialised by the same
// flock (taken by the caller), built in a 0600 temp file and only moved into
// place when it passes wgConfSanityChecks, root:root 0600 afterwards.
// WG_CONF may be overridden in the environment (tests only).
const removePeerScript = `set -euo pipefail
WG_CONF="${WG_CONF:-/etc/wireguard/wg0.conf}"
[ -f "$WG_CONF" ] || exit 0
` + wgConfOutsideFunc + `
TMP=$(mktemp "${WG_CONF}.XXXXXX")
trap 'rm -f "$TMP"' EXIT
awk -v our_key="$OUR_KEY" '
  function drop_block(has_ip, has_key) { return has_key }
` + wgConfAwkPeerRewrite + `' "$WG_CONF" > "$TMP"
` + wgConfSanityChecks + `
chown root:root "$TMP" 2>/dev/null || true
chmod 0600 "$TMP"
mv -f "$TMP" "$WG_CONF"
trap - EXIT`

// wgPublicKeyRE matches a WireGuard public key (32 bytes, base64).
var wgPublicKeyRE = regexp.MustCompile(`^[A-Za-z0-9+/]{43}=$`)

// RemoveVPNPeerFromServerConf removes the peer with the given public key from
// the VPN server's persisted /etc/wireguard/wg0.conf, under the same lock as
// peer registration so concurrent add/remove operations cannot lose updates.
// It does not touch the running interface (use UnregisterVPNPeer for both).
func RemoveVPNPeerFromServerConf(vpnServerClient *sshhelper.Client, publicKey string) error {
	return removeVPNPeerFromServerConf(context.Background(), vpnServerClient, publicKey)
}

func removeVPNPeerFromServerConf(ctx context.Context, vpnServerClient *sshhelper.Client, publicKey string) error {
	if !wgPublicKeyRE.MatchString(publicKey) {
		return fmt.Errorf("refusing to remove peer: %q is not a WireGuard public key", publicKey)
	}
	cmd := fmt.Sprintf("sudo env OUR_KEY=%s flock -w 60 /run/lock/wg0-conf.lock bash -s <<'WG_REMOVE_EOF'\n%s\nWG_REMOVE_EOF",
		sshhelper.ShellQuote(publicKey), removePeerScript)
	if output, err := vpnRun(ctx, vpnServerClient, "set -o pipefail\n"+cmd); err != nil {
		return sshhelper.StepError("removing peer from wg0.conf", err, output)
	}
	return nil
}

// UnregisterVPNPeer releases a VPN peer: it removes it from the VPN server's
// running interface (`wg set wg0 peer <key> remove`) AND from the persisted
// wg0.conf. It is idempotent — calling it for a peer that is already gone (or
// twice) succeeds — so it is safe on every failure/cleanup path. Both steps are
// always attempted; the returned error joins whatever failed. It uses its own
// bounded background context (cleanup must still run when the caller's context
// is already cancelled).
func UnregisterVPNPeer(vpnServerClient *sshhelper.Client, publicKey string) error {
	if !wgPublicKeyRE.MatchString(publicKey) {
		return fmt.Errorf("refusing to unregister peer: %q is not a WireGuard public key", publicKey)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*vpnCmdTimeout)
	defer cancel()
	var errs []error
	if out, err := vpnRun(ctx, vpnServerClient, "sudo wg set wg0 peer "+sshhelper.ShellQuote(publicKey)+" remove"); err != nil {
		errs = append(errs, sshhelper.StepError("wg set peer remove", err, out))
	}
	if err := removeVPNPeerFromServerConf(ctx, vpnServerClient, publicKey); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// registerVPNPeer adds the WireGuard peer to the VPN server's running config
// and persists it to /etc/wireguard/wg0.conf.
//
// Conflict detection (before any write):
//   - If the server already has our exact (publicKey, vpnNodeIP) pair → skip
//     the conflict error (already registered, idempotent).
//   - If the server has our vpnNodeIP assigned to a DIFFERENT public key →
//     return an error so the caller can allocate a different IP rather than
//     silently creating a routing conflict.
//
// The caller must hold LockVPNAllocation.
func registerVPNPeer(ctx context.Context, vpnServerClient *sshhelper.Client, publicKey, vpnNodeIP string) error {
	if !wgKeyRE.MatchString(publicKey) {
		return fmt.Errorf("refusing to register malformed WireGuard public key")
	}
	if net.ParseIP(vpnNodeIP).To4() == nil {
		return fmt.Errorf("refusing to register non-IPv4 VPN address %q", vpnNodeIP)
	}

	// ── 1. Read live server state ──────────────────────────────────────────
	serverPeers, readErr := readVPNServerPeers(ctx, vpnServerClient)
	if readErr != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		log.Printf("Warning: could not verify peer conflicts before registration: %v", readErr)
		serverPeers = map[string]string{}
	}
	// A peer that is already on the server before this call is not ours to
	// remove if registration fails; when the table could not be read we cannot
	// tell, so it is treated as pre-existing too.
	preexisting := readErr != nil
	for _, k := range serverPeers {
		if k == publicKey {
			preexisting = true
		}
	}

	// ── 2. Conflict / idempotency check ────────────────────────────────────
	if existingKey, taken := serverPeers[vpnNodeIP]; taken {
		if existingKey == publicKey {
			log.Printf("Peer %s already registered with IP %s (idempotent)", publicKey, vpnNodeIP)
			// Still sync the conf file below in case it was missed previously.
		} else {
			return fmt.Errorf(
				"IP %s is already assigned to a different peer (%s) on the VPN server; "+
					"allocate a new IP instead of overwriting an active peer",
				vpnNodeIP, existingKey,
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// From here on the server may have been modified. A failure must not leave
	// a live, unrecorded peer behind (its allowed-ips would keep a VPN address
	// occupied and routable): release it unless it existed before this call.
	release := func() {
		if preexisting {
			return
		}
		if err := UnregisterVPNPeer(vpnServerClient, publicKey); err != nil {
			log.Printf("Warning: could not release half-registered VPN peer %s: %v", publicKey, err)
		}
	}

	// ── 3. Update running WireGuard config (upsert by public key) ──────────
	addCmd := fmt.Sprintf(
		"sudo wg set wg0 peer %s allowed-ips %s/32 persistent-keepalive 25",
		sshhelper.ShellQuote(publicKey), sshhelper.ShellQuote(vpnNodeIP),
	)
	if output, err := vpnRun(ctx, vpnServerClient, addCmd); err != nil {
		release() // the command may have run before the connection/ctx failed
		return sshhelper.StepError("wg set peer", err, output)
	}

	// ── 4. Persist to /etc/wireguard/wg0.conf (IP-aware, idempotent) ───────
	persistCmd := fmt.Sprintf("sudo env TARGET_IP=%s OUR_KEY=%s flock -w 60 /run/lock/wg0-conf.lock bash -s <<'WG_PERSIST_EOF'\n%s\nWG_PERSIST_EOF",
		sshhelper.ShellQuote(vpnNodeIP), sshhelper.ShellQuote(publicKey), persistPeerScript)
	if output, err := vpnRun(ctx, vpnServerClient, "set -o pipefail\n"+persistCmd); err != nil {
		release()
		return sshhelper.StepError("persisting peer to wg0.conf", err, output)
	}

	log.Printf("Registered VPN peer: publicKey=%s vpnIP=%s", publicKey, vpnNodeIP)
	return nil
}

// verifyVPNConnectivity pings the new peer from the VPN server.
// A failure is logged as a warning rather than returned as an error because
// the node's WireGuard interface may still be initialising.
func verifyVPNConnectivity(vpnServerClient *sshhelper.Client, vpnNodeIP string) {
	pingCmd := fmt.Sprintf("ping -c 3 -W 5 %s", sshhelper.ShellQuote(vpnNodeIP))
	if output, err := vpnRun(context.Background(), vpnServerClient, pingCmd); err != nil {
		log.Printf("Warning: VPN connectivity check to %s failed (node may still be initialising): %v\nOutput: %s", vpnNodeIP, err, sshhelper.Tail(output, 500))
	} else {
		log.Printf("VPN connectivity to %s verified", vpnNodeIP)
	}
}

// GenerateWireGuardKeyPair is the exported form used by cloud-provider provisioners.
func GenerateWireGuardKeyPair() (string, string, error) { return generateWireGuardKeyPair() }

func generateWireGuardKeyPair() (string, string, error) {
	// WireGuard keys are Curve25519 keys. Generate entirely in Go so the
	// controller pod does not need the `wg` binary installed.
	var privRaw [32]byte
	if _, err := rand.Read(privRaw[:]); err != nil {
		return "", "", fmt.Errorf("failed generating private key: %w", err)
	}
	// Curve25519 clamping (RFC 7748 §5)
	privRaw[0] &= 248
	privRaw[31] &= 127
	privRaw[31] |= 64

	pubRaw, err := curve25519.X25519(privRaw[:], curve25519.Basepoint)
	if err != nil {
		return "", "", fmt.Errorf("failed deriving public key: %w", err)
	}

	privateKey := base64.StdEncoding.EncodeToString(privRaw[:])
	publicKey := base64.StdEncoding.EncodeToString(pubRaw)

	return privateKey, publicKey, nil
}

// BuildClientWGConfig is the exported form used by cloud-provider provisioners.
func BuildClientWGConfig(vpnServerClient *sshhelper.Client, vpnNodeIP, vpnRange, serverPublicIP string, vpnPort int, privateKey string) (string, error) {
	return buildClientWGConfig(context.Background(), vpnServerClient, vpnNodeIP, vpnRange, serverPublicIP, vpnPort, privateKey)
}

// buildClientWGConfig fetches the VPN server's WireGuard public key and actual
// listen port via SSH, then constructs a complete client wg0.conf. The client
// address uses the prefix length of vpnRange. vpnPort is used only as a
// fallback when the server's listen-port cannot be read.
func buildClientWGConfig(
	ctx context.Context,
	vpnServerClient *sshhelper.Client,
	vpnNodeIP, vpnRange, serverPublicIP string,
	vpnPort int,
	privateKey string,
) (string, error) {
	_, rangeNet, err := net.ParseCIDR(vpnRange)
	if err != nil {
		return "", fmt.Errorf("invalid VPN range %q: %w", vpnRange, err)
	}
	prefixLen, _ := rangeNet.Mask.Size()
	if net.ParseIP(vpnNodeIP).To4() == nil {
		return "", fmt.Errorf("invalid VPN node IP %q", vpnNodeIP)
	}
	serverPublicIP = strings.TrimSpace(serverPublicIP)
	if !hostOrIPRE.MatchString(serverPublicIP) {
		return "", fmt.Errorf("invalid VPN server public address %q", serverPublicIP)
	}

	pubKeyOut, err := vpnRunStdout(ctx, vpnServerClient, "sudo wg show wg0 public-key")
	if err != nil {
		return "", fmt.Errorf("reading VPN server public key: %w", err)
	}
	serverPublicKey := strings.TrimSpace(pubKeyOut)
	if !wgKeyRE.MatchString(serverPublicKey) {
		return "", fmt.Errorf("VPN server returned an invalid public key")
	}

	// Read the actual listen port from the running interface so the client
	// endpoint is always correct regardless of what vpnPort is set to.
	portOut, portErr := vpnRunStdout(ctx, vpnServerClient, "sudo wg show wg0 listen-port")
	if portErr != nil {
		if err := ctx.Err(); err != nil {
			return "", err
		}
	} else if p, ok := parseListenPort(portOut); ok {
		vpnPort = p
	}
	if vpnPort == 0 {
		vpnPort = 51820
	}

	log.Printf("Building WireGuard client config: server=%s port=%d", serverPublicIP, vpnPort)

	return renderClientWGConfig(privateKey, vpnNodeIP, prefixLen, serverPublicKey, serverPublicIP, vpnPort, rangeNet.String()), nil
}

// parseListenPort parses the stdout of `wg show <if> listen-port`. "(none)",
// empty or out-of-range values report ok=false (keep the configured port).
func parseListenPort(out string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil || n < 1 || n > 65535 {
		return 0, false
	}
	return n, true
}

// renderClientWGConfig formats a client wg0.conf. The address carries the VPN
// range's prefix length (not a hardcoded /24).
func renderClientWGConfig(privateKey, vpnNodeIP string, prefixLen int, serverPublicKey, serverPublicIP string, vpnPort int, allowedIPs string) string {
	return fmt.Sprintf(`[Interface]
PrivateKey = %s
Address = %s/%d

[Peer]
PublicKey = %s
Endpoint = %s
AllowedIPs = %s
PersistentKeepalive = 25
`, privateKey, vpnNodeIP, prefixLen, serverPublicKey, net.JoinHostPort(serverPublicIP, strconv.Itoa(vpnPort)), allowedIPs)
}

// GetNextAvailableIP is the exported form used by cloud-provider provisioners.
func GetNextAvailableIP(vpnRange string, usedIPs []string) (string, error) {
	return getNextAvailableIP(vpnRange, usedIPs)
}

// getNextAvailableIP returns the lowest usable host IP in vpnRange that is not
// in usedIPs. It always scans from the start of the range so that released IPs
// are reused rather than the range being exhausted prematurely. The network
// and broadcast addresses are never returned.
func getNextAvailableIP(vpnRange string, usedIPs []string) (string, error) {
	_, ipNet, err := net.ParseCIDR(vpnRange)
	if err != nil {
		return "", fmt.Errorf("invalid VPN range: %w", err)
	}
	if ipNet.IP.To4() == nil {
		return "", fmt.Errorf("invalid VPN range %s: only IPv4 ranges are supported", vpnRange)
	}

	// Build a lookup set for O(1) membership test.
	used := make(map[string]struct{}, len(usedIPs))
	for _, u := range usedIPs {
		used[u] = struct{}{}
	}

	ones, bits := ipNet.Mask.Size()
	broadcast := broadcastIP(ipNet)
	next := incrementIP(cloneIP(ipNet.IP.To4())) // first host: network address + 1
	for ipNet.Contains(next) {
		if ones < 31 && bits == 32 && next.Equal(broadcast) {
			break // never hand out the broadcast address
		}
		if _, taken := used[next.String()]; !taken {
			return next.String(), nil
		}
		next = incrementIP(next)
	}
	return "", fmt.Errorf("no available IPs in VPN range %s", vpnRange)
}

// broadcastIP returns the last address of an IPv4 network.
func broadcastIP(n *net.IPNet) net.IP {
	ip := n.IP.To4()
	b := make(net.IP, 4)
	for i := 0; i < 4; i++ {
		b[i] = ip[i] | ^n.Mask[i]
	}
	return b
}

func cloneIP(ip net.IP) net.IP { return append(net.IP(nil), ip...) }

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

// parsePort converts a string port value to int, returning defaultPort when
// the string is empty or unparseable. Accepts port fields stored as strings in YAML.
func parsePort(s string, defaultPort int) int {
	if s == "" {
		return defaultPort
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return defaultPort
	}
	return n
}

// InsecureRegistryHosts returns the merged, de-duplicated, sorted registry
// hosts CRI-O must treat as insecure for nodes of this config: the explicit
// softwareConfig.insecureRegistries plus the registry host of every qualified
// imagePrepulls image (kept for backward compatibility). It is the single
// derivation used by the on-prem NodeProvision flow and the AWS/GCP bootstrap
// scripts; the kubeadm RemoteCluster paths use the same pkgruntime helper. An
// invalid explicit entry is an error.
func InsecureRegistryHosts(sw mlv1alpha1.SoftwareConfig) ([]string, error) {
	images := make([]string, 0, len(sw.ImagePrepulls))
	for _, ip := range sw.ImagePrepulls {
		images = append(images, ip.Image)
	}
	return pkgruntime.InsecureRegistryHosts(sw.InsecureRegistries, images)
}

// withInsecureRegistries appends the insecure-registry drop-in step to the
// runtime install steps; a no-op for an empty host list.
func withInsecureRegistries(steps []string, hosts []string) []string {
	if s := pkgruntime.InsecureRegistriesStep(hosts); s != "" {
		return append(steps, s)
	}
	return steps
}
