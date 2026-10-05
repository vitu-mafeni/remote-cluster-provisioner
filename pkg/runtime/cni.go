package runtime

// CNIPluginsVersion is the containernetworking/plugins release installed into
// /opt/cni/bin on every node.
const CNIPluginsVersion = "v1.5.1"

// CNIPluginsInstallScript returns a shell snippet that installs the standard
// CNI plugins bundle into /opt/cni/bin, working both as root (cloud-init) and
// as an unprivileged SSH user (it uses sudo only when not root).
//
// Flannel's cni-conf.json chains its own "flannel" plugin with the standard
// "portmap" plugin, which flannel does not ship. Without portmap CRI-O rejects
// the whole conflist ("failed to find plugin portmap in path [/opt/cni/bin/]")
// and the node's kubelet reports NetworkPluginNotReady forever, even though
// the conflist file is on disk.
//
// The snippet is safe to run any number of times and never fails because
// something already exists:
//   - it does nothing when the plugins are already present;
//   - it downloads into a fresh mktemp directory instead of a fixed /tmp path.
//     A leftover /tmp file owned by another user makes the next write fail
//     with EACCES under fs.protected_regular=2 (the Ubuntu default), even for
//     root, which a fixed path would trip on every retry after a run as a
//     different user;
//   - tar overwrites existing files, and the temp directory is always removed.
//
// A real failure (download or extract) still fails the step.
func CNIPluginsInstallScript() string {
	return `CNI_SUDO=""; [ "$(id -u)" = 0 ] || CNI_SUDO="sudo"
CNI_BIN="${CNI_BIN:-/opt/cni/bin}"
CNI_MISSING=0
for CNI_P in portmap bridge host-local loopback; do
  [ -x "$CNI_BIN/$CNI_P" ] || CNI_MISSING=1
done
if [ "$CNI_MISSING" = 1 ]; then
  CNI_ARCH=$(dpkg --print-architecture 2>/dev/null || uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
  CNI_TMP=$(mktemp -d)
  if curl -fsSL --retry 3 --retry-delay 2 "https://github.com/containernetworking/plugins/releases/download/` + CNIPluginsVersion + `/cni-plugins-linux-${CNI_ARCH}-` + CNIPluginsVersion + `.tgz" -o "$CNI_TMP/cni-plugins.tgz" \
    && $CNI_SUDO mkdir -p "$CNI_BIN" \
    && $CNI_SUDO tar -xzf "$CNI_TMP/cni-plugins.tgz" -C "$CNI_BIN" --no-same-owner; then
    CNI_RC=0
  else
    CNI_RC=1
  fi
  rm -rf "$CNI_TMP"
  [ "$CNI_RC" = 0 ] || { echo "ERROR: installing CNI plugins ` + CNIPluginsVersion + ` failed" >&2; false; }
fi`
}
