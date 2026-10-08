package runtime

import (
	"fmt"
	"regexp"
	"strings"

	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

var (
	registryRE   = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[0-9]{1,5})?$`)
	repositoryRE = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*(/[a-z0-9]+([._-][a-z0-9]+)*)*$`)
	versionTagRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._+-]{0,127}$`)
	osSuffixRE   = regexp.MustCompile(`-ubuntu[0-9]+$`)
	orasVersion  = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.]+)?$`)
)

// Validate checks (after defaults are applied) that every value interpolated
// into install scripts has a strict, injection-free shape. The scripts also
// shell-quote every value, so this is defence in depth that additionally turns
// typos into a clear error instead of a broken pull.
func (c Config) Validate() error {
	switch c.OSVariant {
	case "", OSVariantAuto:
	default:
		return fmt.Errorf("runtime osVariant %q is not supported (use %q or leave empty)", c.OSVariant, OSVariantAuto)
	}
	if c.OSVariant == OSVariantAuto {
		// The default version predates per-OS variants, so it has no -ubuntuNN tag.
		if c.Version == "" {
			return fmt.Errorf("runtime osVariant %q requires an explicit base version (e.g. 1.0.2)", OSVariantAuto)
		}
		if osSuffixRE.MatchString(c.Version) {
			return fmt.Errorf("runtime version %q already has an OS suffix; with osVariant %q give the base version only", c.Version, OSVariantAuto)
		}
	}
	c.ApplyDefaults()
	if !registryRE.MatchString(c.Registry) {
		return fmt.Errorf("runtime registry %q is not a valid registry host[:port]", c.Registry)
	}
	if !repositoryRE.MatchString(c.Repository) {
		return fmt.Errorf("runtime repository %q is not a valid OCI repository name", c.Repository)
	}
	if !versionTagRE.MatchString(c.Version) {
		return fmt.Errorf("runtime version %q is not a valid OCI tag", c.Version)
	}
	if !orasVersion.MatchString(c.OrasVersion) {
		return fmt.Errorf("ORAS version %q is not a valid version", c.OrasVersion)
	}
	return nil
}

// InstallSteps returns an ordered list of shell commands for SSH-based
// provisioners (each executed via sshhelper.Run in its own SSH session).
//
// Security: cfg.Token is embedded (shell-quoted) in the second step's command
// string. SSH exec does not write to bash history, and callers must never
// include a step's command text in errors or logs (see sshhelper.StepError,
// which reports only the step label and scrubbed output).
func InstallSteps(cfg Config) []string {
	cfg.ApplyDefaults()
	return []string{
		installOrasCmd(cfg),
		installRuntimeCmd(cfg),
		configureDropInsCmd(),
		CNIPluginsInstallScript(),
	}
}

// orasInstallSnippet returns bash that installs ORAS (idempotent) with the
// release tarball verified against the release's published sha256 checksums
// file. It fails closed: a missing/mismatching checksum aborts the install.
// Expects ORAS_VER and ARCH to be set. Commands use sudo (a no-op wrapper on
// cloud-init, which runs as root).
const orasInstallSnippet = `INSTALLED=$(oras version 2>/dev/null | awk '/Version:/{print $2}' | head -1 || true)
if [ "$INSTALLED" = "$ORAS_VER" ]; then
  echo "[cnlab-runtime] ORAS $ORAS_VER already installed"
else
  ORAS_TMP=$(mktemp -d)
  ORAS_TARBALL="oras_${ORAS_VER}_linux_${ARCH}.tar.gz"
  ORAS_BASE="https://github.com/oras-project/oras/releases/download/v${ORAS_VER}"
  curl -fsSL "${ORAS_BASE}/${ORAS_TARBALL}" -o "${ORAS_TMP}/${ORAS_TARBALL}"
  curl -fsSL "${ORAS_BASE}/oras_${ORAS_VER}_checksums.txt" -o "${ORAS_TMP}/checksums.txt"
  ( cd "$ORAS_TMP" && grep -F "  ${ORAS_TARBALL}" checksums.txt > expected.sha256 && [ -s expected.sha256 ] && sha256sum -c expected.sha256 ) || {
    echo "[cnlab-runtime] ORAS tarball checksum verification FAILED" >&2
    rm -rf "$ORAS_TMP"
    exit 1
  }
  mkdir -p "${ORAS_TMP}/x"
  tar -xzf "${ORAS_TMP}/${ORAS_TARBALL}" -C "${ORAS_TMP}/x"
  sudo install -m 0755 "${ORAS_TMP}/x/oras" /usr/local/bin/oras
  rm -rf "$ORAS_TMP"
  echo "[cnlab-runtime] ORAS $ORAS_VER installed"
fi
oras version`

// InstallScript returns a single bash block for embedding in a cloud-init
// template. It references $CNLAB_REGISTRY_USER and $CNLAB_REGISTRY_TOKEN
// which must be exported by the caller before this block runs.
func InstallScript(cfg Config) string {
	cfg.ApplyDefaults()
	cnlabInstall := fmt.Sprintf(`
# -----------------------------------------------------------------------------
# cnlab-runtime OCI artifact install
# -----------------------------------------------------------------------------
CNLAB_REF=%[1]s
CNLAB_VERSION=%[2]s
CNLAB_REGISTRY=%[3]s
CNLAB_ORAS_VER=%[4]s
%[6]s
report "Installing cnlab-runtime ${CNLAB_VERSION} via ORAS"
CNLAB_ARCH=$(dpkg --print-architecture 2>/dev/null || uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
case "$CNLAB_ARCH" in
  amd64|arm64) ;;
  *) echo "[cnlab-runtime] unsupported architecture: $CNLAB_ARCH" >&2; exit 1 ;;
esac

# Install ORAS (idempotent, checksum-verified)
ORAS_VER="$CNLAB_ORAS_VER"
ARCH="$CNLAB_ARCH"
%[5]s
report "ORAS $CNLAB_ORAS_VER ready"

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
  # Registry credentials live only in a private (0700) temp directory that is
  # removed on every exit path (success, failure, signal) so the token never
  # lingers in ~/.docker/config.json. The auth FILE is a path inside it that
  # does not exist yet: oras creates it on login, and would reject an existing
  # empty file ("invalid config format").
  CNLAB_AUTH_DIR=$(mktemp -d)
  trap 'rm -rf "$CNLAB_AUTH_DIR"' EXIT
  CNLAB_AUTH_ARGS=(--registry-config "$CNLAB_AUTH_DIR/config.json")
  # Login only when credentials are provided; omit for public registries.
  if [ -n "${CNLAB_REGISTRY_TOKEN:-}" ]; then
    printf '%%s' "$CNLAB_REGISTRY_TOKEN" | oras login "$CNLAB_REGISTRY" "${CNLAB_AUTH_ARGS[@]}" \
      --username "$CNLAB_REGISTRY_USER" --password-stdin
  fi
  CNLAB_WORK="${TMPDIR:-/tmp}/cnlab-runtime"
  rm -rf "$CNLAB_WORK"
  mkdir -p "$CNLAB_WORK"
  oras pull "${CNLAB_AUTH_ARGS[@]}" "$CNLAB_REF" -o "$CNLAB_WORK"
  rm -rf "$CNLAB_AUTH_DIR"
  trap - EXIT
  (cd "$CNLAB_WORK/artifact" && sha256sum -c SHA256SUMS) || {
    echo "[cnlab-runtime] checksum verification FAILED" >&2
    rm -rf "$CNLAB_WORK"
    exit 1
  }
  # "|| true": with pipefail a no-match ls would abort the script (set -e) before
  # the friendly error below could be printed.
  CNLAB_DEB=$(ls "$CNLAB_WORK"/artifact/cnlab-runtime_*_${CNLAB_ARCH}.deb 2>/dev/null | head -1 || true)
  if [ -z "$CNLAB_DEB" ]; then
    echo "[cnlab-runtime] no .deb found in pulled artifact" >&2
    ls "$CNLAB_WORK/artifact/" >&2 || true
    rm -rf "$CNLAB_WORK"
    exit 1
  fi
  echo "[cnlab-runtime] installing $CNLAB_DEB"
  DEBIAN_FRONTEND=noninteractive dpkg -i --force-overwrite "$CNLAB_DEB" || true
  DEBIAN_FRONTEND=noninteractive apt-get install -f -y
  rm -f /var/cache/apt/archives/cnlab-runtime_*.deb
  cnlab-runtime version
  rm -rf "$CNLAB_WORK"
  report "cnlab-runtime $CNLAB_VERSION installed"
fi
`, sshhelper.ShellQuote(cfg.ImageRef()), sshhelper.ShellQuote(cfg.Version),
		sshhelper.ShellQuote(cfg.Registry), sshhelper.ShellQuote(cfg.OrasVersion), orasInstallSnippet,
		osVariantSnippet(cfg, "CNLAB_VERSION", "CNLAB_REF"))
	return cnlabInstall + `
# -----------------------------------------------------------------------------
# Standard CNI plugins (portmap is required by flannel's conflist)
# -----------------------------------------------------------------------------
` + CNIPluginsInstallScript() + "\n"
}

// installOrasCmd installs or upgrades ORAS to cfg.OrasVersion.
func installOrasCmd(cfg Config) string {
	return fmt.Sprintf(`set -euo pipefail
ORAS_VER=%s
ARCH=$(dpkg --print-architecture 2>/dev/null || uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
%s`, sshhelper.ShellQuote(cfg.OrasVersion), orasInstallSnippet)
}

// installRuntimeCmd pulls the cnlab-runtime OCI artifact and installs the deb.
// When cfg.Token is non-empty it is embedded (shell-quoted) in the command
// string for oras login; callers must never surface the command text (see
// InstallSteps). When empty the pull is attempted without authentication,
// suitable for public registries. Credentials are kept in a private temp auth
// directory removed on every exit path, never in ~/.docker/config.json.
func installRuntimeCmd(cfg Config) string {
	var loginBlock string
	if cfg.Token != "" {
		loginBlock = fmt.Sprintf(
			"CNLAB_TOKEN=%s\n"+
				"printf '%%s' \"$CNLAB_TOKEN\" | oras login \"$REGISTRY\" \"${AUTH_ARGS[@]}\" --username %s --password-stdin\n"+
				"unset CNLAB_TOKEN",
			sshhelper.ShellQuote(cfg.Token), sshhelper.ShellQuote(cfg.Username))
	}

	return fmt.Sprintf(`set -euo pipefail
REF=%[1]s
VERSION=%[2]s
REGISTRY=%[3]s
%[5]s
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
# Private 0700 dir removed on every exit path; the auth FILE is a path inside it
# that does not exist yet (oras creates it on login and rejects an existing
# empty file with "invalid config format").
AUTH_DIR=$(mktemp -d)
trap 'rm -rf "$AUTH_DIR"' EXIT
AUTH_ARGS=(--registry-config "$AUTH_DIR/config.json")
%[4]s
WORK="${TMPDIR:-/tmp}/cnlab-runtime"
rm -rf "$WORK"
mkdir -p "$WORK"
oras pull "${AUTH_ARGS[@]}" "$REF" -o "$WORK"
rm -rf "$AUTH_DIR"
(cd "$WORK/artifact" && sha256sum -c SHA256SUMS) || {
  echo "[cnlab-runtime] checksum verification FAILED" >&2
  rm -rf "$WORK"
  exit 1
}
# "|| true": under set -e + pipefail a no-match ls would abort before the message.
DEB=$(ls "$WORK"/artifact/cnlab-runtime_*_${ARCH}.deb 2>/dev/null | head -1 || true)
if [ -z "$DEB" ]; then
  echo "[cnlab-runtime] no .deb found in pulled artifact" >&2
  ls "$WORK/artifact/" >&2 || true
  rm -rf "$WORK"
  exit 1
fi
echo "[cnlab-runtime] installing $DEB"
sudo DEBIAN_FRONTEND=noninteractive dpkg -i --force-overwrite "$DEB" || true
sudo DEBIAN_FRONTEND=noninteractive apt-get install -f -y
sudo rm -f /var/cache/apt/archives/cnlab-runtime_*.deb
cnlab-runtime version
rm -rf "$WORK"
echo "[cnlab-runtime] version $VERSION installed"`,
		sshhelper.ShellQuote(cfg.ImageRef()), sshhelper.ShellQuote(cfg.Version),
		sshhelper.ShellQuote(cfg.Registry), loginBlock, osVariantSnippet(cfg, "VERSION", "REF"))
}

// osVariantSnippet returns bash that resolves the per-OS artifact variant on the
// node when cfg.OSVariant is OSVariantAuto (empty otherwise, so scripts for
// explicit versions are unchanged). It appends -ubuntu20 / -ubuntu22 to
// versionVar and rewrites the tag of refVar to match, so everything after it
// (the "already installed" check and the oras pull) uses the resolved tag.
//
// Ubuntu 20.x/21.x get the ubuntu20 build and 22.x and newer the ubuntu22
// build (the 22.04 build is the one for 22.04 and up). Anything else fails
// before touching the registry. The os-release path can be overridden through
// CNLAB_OS_RELEASE_FILE, which the tests use.
func osVariantSnippet(cfg Config, versionVar, refVar string) string {
	if cfg.OSVariant != OSVariantAuto {
		return ""
	}
	return strings.NewReplacer("@VERSION@", versionVar, "@REF@", refVar).Replace(`# Pick the runtime variant that matches this node's OS (osVariant: auto).
CNLAB_OSR="${CNLAB_OS_RELEASE_FILE:-/etc/os-release}"
CNLAB_OS_ID=$(. "$CNLAB_OSR" 2>/dev/null && printf '%s' "${ID:-}" || true)
CNLAB_OS_VID=$(. "$CNLAB_OSR" 2>/dev/null && printf '%s' "${VERSION_ID:-}" || true)
CNLAB_OS_MAJOR="${CNLAB_OS_VID%%.*}"
if [ "$CNLAB_OS_ID" != "ubuntu" ] || ! [[ "$CNLAB_OS_MAJOR" =~ ^[0-9]+$ ]]; then
  echo "[cnlab-runtime] osVariant auto needs Ubuntu; found ID='${CNLAB_OS_ID}' VERSION_ID='${CNLAB_OS_VID}'" >&2
  exit 1
elif [ "$CNLAB_OS_MAJOR" -ge 22 ]; then
  CNLAB_OS_TAG=ubuntu22
elif [ "$CNLAB_OS_MAJOR" -ge 20 ]; then
  CNLAB_OS_TAG=ubuntu20
else
  echo "[cnlab-runtime] Ubuntu ${CNLAB_OS_VID} is not supported (Ubuntu 20.04 or newer required)" >&2
  exit 1
fi
@VERSION@="${@VERSION@}-${CNLAB_OS_TAG}"
@REF@="${@REF@%:*}:${@VERSION@}"
echo "[cnlab-runtime] node is Ubuntu ${CNLAB_OS_VID}: using runtime ${@VERSION@}"`)
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
  printf 'tcp-close\nskip-in-flight\nlog-file /tmp/criu.log\nghost-limit 100M\nenable-external-masters\nexternal mnt[]\nirmap-scan-path /home/jovyan\nirmap-scan-path /usr\nirmap-scan-path /opt/conda\nirmap-scan-path /opt/remote-dev\nallow-uprobes\n' \
    | sudo tee /etc/criu/runc.conf > /dev/null
fi
# Pre-existing runc.conf (older provisioning): make sure allow-uprobes is set.
grep -qx 'allow-uprobes' /etc/criu/runc.conf || echo 'allow-uprobes' | sudo tee -a /etc/criu/runc.conf > /dev/null
if [ ! -f /etc/criu/default.conf ]; then
  sudo cp -f /etc/criu/runc.conf /etc/criu/default.conf
fi
grep -qx 'allow-uprobes' /etc/criu/default.conf || echo 'allow-uprobes' | sudo tee -a /etc/criu/default.conf > /dev/null`
}
