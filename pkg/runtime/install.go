package runtime

import (
	"fmt"
	"strings"
)

// InstallSteps returns an ordered list of shell commands for SSH-based
// provisioners (each executed via sshhelper.Run in its own SSH session).
//
// Security: cfg.Token is embedded in the second step's command string.
// SSH exec does not write to bash history; the token is not logged by the
// caller because runPhases only logs phase names, not individual commands.
func InstallSteps(cfg Config) []string {
	cfg.ApplyDefaults()
	return []string{
		installOrasCmd(cfg),
		installRuntimeCmd(cfg),
		configureDropInsCmd(),
	}
}

// InstallScript returns a single bash block for embedding in a cloud-init
// template. It references $CNLAB_REGISTRY_USER and $CNLAB_REGISTRY_TOKEN
// which must be exported by the caller before this block runs.
func InstallScript(cfg Config) string {
	cfg.ApplyDefaults()
	return fmt.Sprintf(`
# -----------------------------------------------------------------------------
# cnlab-runtime OCI artifact install
# -----------------------------------------------------------------------------
report "Installing cnlab-runtime %[2]s via ORAS"
CNLAB_REF='%[1]s'
CNLAB_VERSION='%[2]s'
CNLAB_REGISTRY='%[3]s'
CNLAB_ORAS_VER='%[4]s'
CNLAB_ARCH=$(dpkg --print-architecture 2>/dev/null || uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
case "$CNLAB_ARCH" in
  amd64|arm64) ;;
  *) echo "[cnlab-runtime] unsupported architecture: $CNLAB_ARCH" >&2; exit 1 ;;
esac

# Install ORAS (idempotent)
CNLAB_ORAS_INSTALLED=$(oras version 2>/dev/null | awk '/Version:/{print $2}' | head -1 || true)
if [ "$CNLAB_ORAS_INSTALLED" != "$CNLAB_ORAS_VER" ]; then
  curl -fsSL "https://github.com/oras-project/oras/releases/download/v${CNLAB_ORAS_VER}/oras_${CNLAB_ORAS_VER}_linux_${CNLAB_ARCH}.tar.gz" -o /tmp/oras.tar.gz
  sudo mkdir -p /tmp/oras-install
  sudo tar -xzf /tmp/oras.tar.gz -C /tmp/oras-install
  sudo install -m 0755 /tmp/oras-install/oras /usr/local/bin/oras
  sudo rm -rf /tmp/oras.tar.gz /tmp/oras-install
  report "ORAS $CNLAB_ORAS_VER installed"
fi
oras version

# Idempotency: skip if already at the requested version AND crio binary is present.
# Checking only the version is insufficient: dpkg --remove keeps config files
# (including /etc/cnlab/runtime-release.yaml) but removes binaries, so the
# version check can pass while /usr/local/bin/crio is gone.
CNLAB_HAVE=$(cnlab-runtime version 2>/dev/null | head -1 || true)
if echo "$CNLAB_HAVE" | grep -qF "$CNLAB_VERSION" && command -v crio >/dev/null 2>&1; then
  report "cnlab-runtime $CNLAB_VERSION already installed, skipping"
else
  # cloud-init runs without a user environment; oras needs $HOME for its config store.
  export HOME="${HOME:-/root}"
  # Login only when credentials are provided; omit for public registries.
  if [ -n "${CNLAB_REGISTRY_TOKEN:-}" ]; then
    printf '%%s' "$CNLAB_REGISTRY_TOKEN" | oras login "$CNLAB_REGISTRY" \
      --username "$CNLAB_REGISTRY_USER" --password-stdin
  fi
  rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
  mkdir -p /tmp/cnlab-runtime
  oras pull "$CNLAB_REF" -o /tmp/cnlab-runtime
  if [ -n "${CNLAB_REGISTRY_TOKEN:-}" ]; then
    oras logout "$CNLAB_REGISTRY" 2>/dev/null || true
  fi
  (cd /tmp/cnlab-runtime/artifact && sha256sum -c SHA256SUMS) || {
    echo "[cnlab-runtime] checksum verification FAILED" >&2
    rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
    exit 1
  }
  CNLAB_DEB=$(ls /tmp/cnlab-runtime/artifact/cnlab-runtime_*_${CNLAB_ARCH}.deb 2>/dev/null | head -1)
  if [ -z "$CNLAB_DEB" ]; then
    echo "[cnlab-runtime] no .deb found in pulled artifact" >&2
    ls /tmp/cnlab-runtime/artifact/ >&2 || true
    rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
    exit 1
  fi
  echo "[cnlab-runtime] installing $CNLAB_DEB"
  DEBIAN_FRONTEND=noninteractive dpkg -i --force-overwrite "$CNLAB_DEB" || true
  DEBIAN_FRONTEND=noninteractive apt-get install -f -y
  rm -f /var/cache/apt/archives/cnlab-runtime_*.deb
  cnlab-runtime version
  rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
  report "cnlab-runtime $CNLAB_VERSION installed"
fi
`, cfg.ImageRef(), cfg.Version, cfg.Registry, cfg.OrasVersion)
}

// installOrasCmd installs or upgrades ORAS to cfg.OrasVersion.
func installOrasCmd(cfg Config) string {
	return fmt.Sprintf(`set -euo pipefail
ORAS_VER='%s'
INSTALLED=$(oras version 2>/dev/null | awk '/Version:/{print $2}' | head -1 || true)
if [ "$INSTALLED" = "$ORAS_VER" ]; then
  echo "[cnlab-runtime] ORAS $ORAS_VER already installed"
else
  ARCH=$(dpkg --print-architecture 2>/dev/null || uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
  curl -fsSL "https://github.com/oras-project/oras/releases/download/v${ORAS_VER}/oras_${ORAS_VER}_linux_${ARCH}.tar.gz" -o /tmp/oras.tar.gz
  sudo mkdir -p /tmp/oras-install
  sudo tar -xzf /tmp/oras.tar.gz -C /tmp/oras-install
  sudo install -m 0755 /tmp/oras-install/oras /usr/local/bin/oras
  sudo rm -rf /tmp/oras.tar.gz /tmp/oras-install
  echo "[cnlab-runtime] ORAS $ORAS_VER installed"
fi
oras version`, cfg.OrasVersion)
}

// installRuntimeCmd pulls the cnlab-runtime OCI artifact and installs the deb.
// When cfg.Token is non-empty it is embedded in the command string for oras login
// (acceptable; not logged by runPhases). When empty the pull is attempted without
// authentication, suitable for public registries.
func installRuntimeCmd(cfg Config) string {
	var loginBlock, logoutBlock string
	if cfg.Token != "" {
		// Use %q (Go double-quoted string with escape sequences) to produce a
		// shell-safe string: double-quoted with backslash escaping of $, `, \, "
		// so that a token or username containing single quotes, dollar signs, or
		// backticks cannot break out of the string context and execute code.
		loginBlock = fmt.Sprintf(
			"sudo install -m 0600 /dev/null /tmp/.cnlab-reg\n"+
				"printf '%%s' %s | sudo tee /tmp/.cnlab-reg >/dev/null\n"+
				"CNLAB_TOKEN=$(sudo cat /tmp/.cnlab-reg)\n"+
				"sudo rm -f /tmp/.cnlab-reg\n"+
				"printf '%%s' \"$CNLAB_TOKEN\" | oras login \"$REGISTRY\" --username %s --password-stdin\n"+
				"unset CNLAB_TOKEN",
			shellQuote(cfg.Token), shellQuote(cfg.Username))
		logoutBlock = `oras logout "$REGISTRY" 2>/dev/null || true`
	}

	return fmt.Sprintf(`set -euo pipefail
REF='%[1]s'
VERSION='%[2]s'
REGISTRY='%[3]s'
ARCH=$(dpkg --print-architecture 2>/dev/null || uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
case "$ARCH" in
  amd64|arm64) ;;
  *) echo "[cnlab-runtime] unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
HAVE=$(cnlab-runtime version 2>/dev/null | head -1 || true)
if echo "$HAVE" | grep -qF "$VERSION"; then
  echo "[cnlab-runtime] version $VERSION already installed, skipping"
  exit 0
fi
%[4]s
rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
mkdir -p /tmp/cnlab-runtime
oras pull "$REF" -o /tmp/cnlab-runtime
%[5]s
(cd /tmp/cnlab-runtime/artifact && sha256sum -c SHA256SUMS) || {
  echo "[cnlab-runtime] checksum verification FAILED" >&2
  rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
  exit 1
}
DEB=$(ls /tmp/cnlab-runtime/artifact/cnlab-runtime_*_${ARCH}.deb 2>/dev/null | head -1)
if [ -z "$DEB" ]; then
  echo "[cnlab-runtime] no .deb found in pulled artifact" >&2
  ls /tmp/cnlab-runtime/artifact/ >&2 || true
  rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
  exit 1
fi
echo "[cnlab-runtime] installing $DEB"
sudo DEBIAN_FRONTEND=noninteractive dpkg -i --force-overwrite "$DEB" || true
sudo DEBIAN_FRONTEND=noninteractive apt-get install -f -y
sudo rm -f /var/cache/apt/archives/cnlab-runtime_*.deb
cnlab-runtime version
rm -rf /tmp/cnlab-runtime/artifact /tmp/cnlab-runtime
echo "[cnlab-runtime] version $VERSION installed"`,
		cfg.ImageRef(), cfg.Version, cfg.Registry, loginBlock, logoutBlock)
}

// configureDropInsCmd checks whether each CRI-O / CRIU config file already
// exists (installed by cnlab-runtime deb) and only writes the fallback if it
// does not. This preserves any config the deb provides while still covering
// nodes where the deb omits a file.
func configureDropInsCmd() string {
	return `set -euo pipefail
sudo mkdir -p /etc/crio/crio.conf.d /etc/containers /etc/cni/net.d /opt/cni/bin /etc/criu

# crictl endpoint config
if [ ! -f /etc/crictl.yaml ]; then
  printf 'runtime-endpoint: unix:///var/run/crio/crio.sock\nimage-endpoint: unix:///var/run/crio/crio.sock\ntimeout: 30\ndebug: false\n' \
    | sudo tee /etc/crictl.yaml > /dev/null
fi

# CRI-O socket + conmon path
if [ ! -f /etc/crio/crio.conf.d/10-paths.conf ]; then
  printf '[crio.runtime]\nlisten = "/var/run/crio/crio.sock"\nconmon = "/usr/local/bin/conmon"\n' \
    | sudo tee /etc/crio/crio.conf.d/10-paths.conf > /dev/null
fi

# Container image pull policy
if [ ! -f /etc/containers/policy.json ]; then
  printf '{"default":[{"type":"insecureAcceptAnything"}]}\n' \
    | sudo tee /etc/containers/policy.json > /dev/null
fi

# Unqualified image registry
if [ ! -f /etc/containers/registries.conf ]; then
  sudo tee /etc/containers/registries.conf >/dev/null <<'REGEOF'
unqualified-search-registries = ["docker.io"]

[[registry]]
location = "docker.io"
REGEOF
fi

# Default OCI runtime drop-in
if [ ! -f /etc/crio/crio.conf.d/999-runc.conf ]; then
  sudo tee /etc/crio/crio.conf.d/999-runc.conf >/dev/null <<'RUNCEOF'
[crio]

  [crio.runtime]
    default_runtime = "runc"

    [crio.runtime.runtimes]
      [crio.runtime.runtimes.runc]
        runtime_path = "/usr/bin/runc"
        runtime_type = "oci"

      [crio.runtime.runtimes.nvidia]
        runtime_path = "/usr/bin/nvidia-container-runtime"
        runtime_type = "oci"
RUNCEOF
fi

# NVIDIA handler override (GPU Operator installs the actual binary at runtime)
if [ ! -f /etc/crio/crio.conf.d/9999-nvidia.conf ]; then
  sudo tee /etc/crio/crio.conf.d/9999-nvidia.conf >/dev/null <<'NVEOF'
[crio.runtime]
  [crio.runtime.runtimes]
    [crio.runtime.runtimes.nvidia]
      runtime_path = "/usr/local/nvidia/toolkit/nvidia-container-runtime"
      runtime_type = "oci"
    [crio.runtime.runtimes.nvidia-cdi]
      runtime_path = "/usr/local/nvidia/toolkit/nvidia-container-runtime.cdi"
      runtime_type = "oci"
NVEOF
fi

# CRIU runtime configuration
if [ ! -f /etc/criu/runc.conf ]; then
  printf 'tcp-close\nskip-in-flight\nlog-file /tmp/criu.log\nghost-limit 100M\nenable-external-masters\nexternal mnt[]\nirmap-scan-path /home/jovyan\nirmap-scan-path /usr\nirmap-scan-path /opt/conda\nirmap-scan-path /opt/remote-dev\n' \
    | sudo tee /etc/criu/runc.conf > /dev/null
fi
if [ ! -f /etc/criu/default.conf ]; then
  sudo cp -f /etc/criu/runc.conf /etc/criu/default.conf
fi`
}

// shellQuote wraps s in double quotes with backslash-escaping of characters
// that are special inside a double-quoted bash string: \, $, `, and ".
// This prevents token or username values that contain single quotes or other
// shell metacharacters from escaping the string context and executing code.
func shellQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `$`, `\$`)
	s = strings.ReplaceAll(s, "`", "\\`")
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}
