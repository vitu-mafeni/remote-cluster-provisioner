package runtime

import (
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"

	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

// Insecure-registry support, shared by every node bootstrap path (on-prem
// control-plane and worker kubeadm steps, the on-prem NodeProvision SSH flow,
// the AWS cloud-init script and the GCP startup script).
//
// CRI-O (containers/image) reads /etc/containers/registries.conf.d only when
// the daemon starts, so the drop-ins MUST be written before CRI-O's (re)start
// that precedes the first pull.

const (
	// InsecureRegistriesDir is the containers/image drop-in directory.
	InsecureRegistriesDir = "/etc/containers/registries.conf.d"

	// MaxInsecureRegistries bounds spec.softwareConfig.insecureRegistries (it is
	// also the CRD maxItems) so a script can't grow without limit.
	MaxInsecureRegistries = 32

	// maxRegistryHostLen is the DNS name limit (253) plus ":65535".
	maxRegistryHostLen = 259
)

// InsecureRegistryPattern is the host[:port] shape accepted for an insecure
// registry. It mirrors the kubebuilder Pattern marker on the API field and is
// deliberately strict: no scheme, path, whitespace, quote or shell
// metacharacter can match, so the value is safe in a file name, a TOML string
// and a shell script.
const InsecureRegistryPattern = `^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[0-9]{1,5})?$`

var insecureRegistryRE = regexp.MustCompile(InsecureRegistryPattern)

// ValidateInsecureRegistry checks one host[:port] entry.
func ValidateInsecureRegistry(h string) error {
	if h == "" {
		return fmt.Errorf("must not be empty")
	}
	if len(h) > maxRegistryHostLen {
		return fmt.Errorf("%q is too long", h[:40]+"...")
	}
	if !insecureRegistryRE.MatchString(h) {
		return fmt.Errorf("%q is not a valid registry host[:port]: use a bare host or host:port without a scheme (http://), path, spaces, quotes or other special characters", h)
	}
	if i := strings.LastIndex(h, ":"); i >= 0 {
		port, err := strconv.Atoi(h[i+1:])
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("%q has an invalid port (must be 1-65535)", h)
		}
	}
	return nil
}

// ValidateInsecureRegistries validates a whole spec.softwareConfig.insecureRegistries
// list, returning an error that names the offending entry.
func ValidateInsecureRegistries(hosts []string) error {
	if len(hosts) > MaxInsecureRegistries {
		return fmt.Errorf("insecureRegistries has %d entries; at most %d are allowed", len(hosts), MaxInsecureRegistries)
	}
	for i, h := range hosts {
		if err := ValidateInsecureRegistry(h); err != nil {
			return fmt.Errorf("insecureRegistries[%d]: %w", i, err)
		}
	}
	return nil
}

// InsecureRegistryHosts returns the merged, de-duplicated, sorted list of
// registry hosts that must be marked insecure (plain HTTP / untrusted TLS):
//
//   - every explicit entry (spec.softwareConfig.insecureRegistries), which is
//     validated and rejected with an error when malformed, plus
//   - the registry host of every qualified image reference in images (the
//     imagePrepulls images). This derivation is the original behaviour and is
//     kept for backward compatibility; an image whose host is not a valid
//     host[:port] is skipped with a log line, not an error.
//
// An empty result means "write nothing".
func InsecureRegistryHosts(explicit []string, images []string) ([]string, error) {
	if err := ValidateInsecureRegistries(explicit); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(explicit)+len(images))
	var hosts []string
	add := func(h string) {
		if !seen[h] {
			seen[h] = true
			hosts = append(hosts, h)
		}
	}
	for _, h := range explicit {
		add(h)
	}
	for _, img := range images {
		host, _, found := strings.Cut(img, "/")
		if !found || !strings.ContainsAny(host, ".:") {
			continue
		}
		if ValidateInsecureRegistry(host) != nil {
			log.Printf("Skipping insecure-registry drop-in for %q: not a valid host[:port]", host)
			continue
		}
		add(host)
	}
	sort.Strings(hosts)
	return hosts, nil
}

// InsecureRegistryDropIn returns the drop-in path and TOML content marking one
// registry insecure. host must already be validated; used for a single host so
// the on-disk format is defined in exactly one place.
func InsecureRegistryDropIn(host string) (path, content string) {
	return insecureDropInPath(host, ""), insecureDropInContent(host)
}

func insecureDropInFileName(host string) string {
	return "50-insecure-" + strings.NewReplacer(".", "-", ":", "-").Replace(host) + ".conf"
}

func insecureDropInPath(host, suffix string) string {
	name := insecureDropInFileName(host)
	if suffix != "" {
		name = strings.TrimSuffix(name, ".conf") + suffix + ".conf"
	}
	return InsecureRegistriesDir + "/" + name
}

func insecureDropInContent(host string) string {
	return "[[registry]]\nlocation = \"" + host + "\"\ninsecure = true\n"
}

// InsecureRegistriesScript renders the shell that writes one drop-in per host.
// Returns "" when hosts is empty. sudo selects `sudo mkdir`/`sudo tee` (SSH
// steps run as an unprivileged user) versus plain commands (cloud-init and GCE
// startup scripts run as root; the bootstrap script has no sudo guarantee).
// Hosts are re-validated here as defence in depth: an invalid host is skipped
// rather than embedded in a script. File names and contents are shell-quoted /
// quoted-heredoc.
func InsecureRegistriesScript(hosts []string, sudo bool) string {
	var valid []string
	for _, h := range hosts {
		if ValidateInsecureRegistry(h) != nil {
			log.Printf("Skipping insecure-registry drop-in for %q: not a valid host[:port]", h)
			continue
		}
		valid = append(valid, h)
	}
	if len(valid) == 0 {
		return ""
	}
	pre := ""
	if sudo {
		pre = "sudo "
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%smkdir -p %s\n", pre, sshhelper.ShellQuote(InsecureRegistriesDir))
	used := map[string]bool{}
	for _, h := range valid {
		// "a.b" and "a-b" map to the same file name; keep both by suffixing.
		suffix := ""
		for n := 2; used[insecureDropInPath(h, suffix)]; n++ {
			suffix = "-" + strconv.Itoa(n)
		}
		path := insecureDropInPath(h, suffix)
		used[path] = true
		fmt.Fprintf(&b, "cat <<'CNLAB_REG_EOF' | %stee %s > /dev/null\n%sCNLAB_REG_EOF\n",
			pre, sshhelper.ShellQuote(path), insecureDropInContent(h))
	}
	return b.String()
}

// InsecureRegistriesStep is InsecureRegistriesScript(hosts, true): the
// self-contained SSH step form used by the on-prem paths.
func InsecureRegistriesStep(hosts []string) string {
	return InsecureRegistriesScript(hosts, true)
}
