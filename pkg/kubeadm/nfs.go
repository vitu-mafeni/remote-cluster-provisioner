package kubeadm

import (
	"net"
	"regexp"
	"strings"
)

const (
	// CheckpointNFSParentExport is the control plane's NFS export (see the NFS
	// Server phase of InitializeControlPlane); every sub-directory is reachable
	// through it.
	CheckpointNFSParentExport = "/srv/nfs/k8s"
	// CheckpointNFSExport is the shared checkpoint directory on the control plane
	// that every worker mounts.
	CheckpointNFSExport = CheckpointNFSParentExport + "/checkpoint-storage"
	// CheckpointMountPath is where workers mount CheckpointNFSExport.
	CheckpointMountPath = "/shared-pvc/checkpoint-storage"
)

var nfsHostnameRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)

// NFSServerFromJoinCommand returns the control-plane host (IP or DNS name) of a
// `kubeadm join <host>:<port> ...` command — the address the node reaches the
// control plane on (the VPN IP, or the private IP without a VPN), which is also
// where the control plane's NFS server listens. It returns "" when the command has
// no plain host:port endpoint or the host is not a valid IP/hostname, so nothing
// unsafe is ever interpolated into a script.
func NFSServerFromJoinCommand(joinCmd string) string {
	ep := JoinEndpoint(joinCmd)
	if ep == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(ep)
	if err != nil {
		return ""
	}
	if net.ParseIP(host) != nil || nfsHostnameRE.MatchString(host) {
		return host
	}
	return ""
}

// CheckpointStorageMountSnippet returns bash that mounts the control plane's shared
// checkpoint directory on a worker:
//
//	apt-get install -y nfs-common
//	mkdir -p /shared-pvc/checkpoint-storage
//	mount -t nfs <server>:/srv/nfs/k8s/checkpoint-storage /shared-pvc/checkpoint-storage
//
// with the robustness a provisioning script needs:
//   - nfs-common is installed only when mount.nfs is missing;
//   - it is a no-op when the path is already mounted (retries, reboots);
//   - it is persisted in /etc/fstab (nofail, _netdev, x-systemd.automount — the
//     tunnel or network may not be up when the node boots, and the GCE startup script
//     exits early on reboot) replacing any earlier entry for the same mount point,
//     other lines untouched;
//   - when the control plane has no checkpoint-storage directory yet (it was
//     provisioned before this existed) the worker creates it through the parent
//     export, which the control plane exports with no_root_squash;
//   - the mount is retried, each attempt time-bounded;
//   - it NEVER fails the caller: a problem is reported as a WARNING, because the node
//     joining the cluster matters more than optional shared storage.
//
// sudo ("" or "sudo") prefixes the privileged commands. CNLAB_NFS_MOUNT and
// CNLAB_FSTAB override the mount point and fstab path (tests only). An empty or
// invalid server yields "" (nothing to do).
func CheckpointStorageMountSnippet(server, sudo string) string {
	if server == "" || !(net.ParseIP(server) != nil || nfsHostnameRE.MatchString(server)) {
		return ""
	}
	src := server
	if ip := net.ParseIP(server); ip != nil && strings.Contains(server, ":") {
		src = "[" + server + "]" // IPv6 literal
	}
	if sudo != "" {
		sudo += " "
	}
	return `NFS_SRC_HOST='` + src + `'
NFS_PARENT='` + CheckpointNFSParentExport + `'
NFS_EXPORT='` + CheckpointNFSExport + `'
NFS_MNT="${CNLAB_NFS_MOUNT:-` + CheckpointMountPath + `}"
NFS_FSTAB="${CNLAB_FSTAB:-/etc/fstab}"
nfs_create_export_dir() {
  # An older control plane has no checkpoint-storage directory: create it through
  # the parent export (exported rw with no_root_squash).
  local t
  t="$(mktemp -d)" || return 1
  if ` + sudo + `timeout 60 mount -t nfs "${NFS_SRC_HOST}:${NFS_PARENT}" "$t"; then
    ` + sudo + `mkdir -p "$t/${NFS_EXPORT#"${NFS_PARENT}"/}"
    ` + sudo + `umount "$t" || true
  fi
  rmdir "$t" 2>/dev/null || true
}
nfs_mount_checkpoint_storage() {
  local i
  if ! command -v mount.nfs >/dev/null 2>&1; then
    ` + sudo + `env DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=5 update -qq || true
    ` + sudo + `env DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 -o Acquire::Retries=5 install -y nfs-common || return 1
  fi
  ` + sudo + `mkdir -p "$NFS_MNT" || return 1
  # Persist across reboots: replace this mount point's entry, leave other lines alone.
  ` + sudo + `touch "$NFS_FSTAB"
  ` + sudo + `sed -i "\|[[:space:]]${NFS_MNT}[[:space:]]|d" "$NFS_FSTAB"
  printf '%s\n' "${NFS_SRC_HOST}:${NFS_EXPORT} ${NFS_MNT} nfs defaults,_netdev,nofail,x-systemd.automount,x-systemd.mount-timeout=30 0 0" | ` + sudo + `tee -a "$NFS_FSTAB" >/dev/null
  for i in 1 2 3 4 5; do
    if mountpoint -q "$NFS_MNT" 2>/dev/null; then
      echo "Shared checkpoint storage already mounted at ${NFS_MNT}"
      return 0
    fi
    if ` + sudo + `timeout 60 mount -t nfs "${NFS_SRC_HOST}:${NFS_EXPORT}" "$NFS_MNT"; then
      echo "Mounted ${NFS_SRC_HOST}:${NFS_EXPORT} at ${NFS_MNT}"
      return 0
    fi
    nfs_create_export_dir || true
    sleep 5
  done
  return 1
}
echo "Mounting shared checkpoint storage from the control plane"
nfs_mount_checkpoint_storage || echo "WARNING: could not mount ${NFS_SRC_HOST}:${NFS_EXPORT} at ${NFS_MNT} (continuing: it is retried from /etc/fstab at boot); check that the control plane exports it and allows this node"
`
}
