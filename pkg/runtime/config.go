package runtime

const (
	DefaultRegistry    = "ghcr.io"
	DefaultRepository  = "vitu-mafeni/cnlab-runtime"
	DefaultVersion     = "1.0.0-beta"
	DefaultOrasVersion = "1.3.2"

	// OSVariantAuto makes the install script pick the artifact variant from the
	// node's own /etc/os-release: "<Version>-ubuntu20" on Ubuntu 20.x/21.x and
	// "<Version>-ubuntu22" on Ubuntu 22.x and newer. Version is then the base
	// version (e.g. "1.0.2") without an OS suffix.
	OSVariantAuto = "auto"
)

// Config holds the resolved runtime artifact configuration with credentials
// already extracted from the Kubernetes Secret. The Token field must never
// be logged.
type Config struct {
	Registry    string
	Repository  string
	Version     string
	OrasVersion string
	// OSVariant is "" (Version is the exact tag) or OSVariantAuto.
	OSVariant string
	Username  string
	Token     string // never log this value
}

// ApplyDefaults fills zero-value fields with the package defaults.
func (c *Config) ApplyDefaults() {
	if c.Registry == "" {
		c.Registry = DefaultRegistry
	}
	if c.Repository == "" {
		c.Repository = DefaultRepository
	}
	if c.Version == "" {
		c.Version = DefaultVersion
	}
	if c.OrasVersion == "" {
		c.OrasVersion = DefaultOrasVersion
	}
}

// ImageRef returns the fully-qualified OCI reference "registry/repository:version".
// With OSVariantAuto the tag is only known on the node, so this is the base
// reference; the install script appends the OS suffix.
func (c *Config) ImageRef() string {
	return c.Registry + "/" + c.Repository + ":" + c.Version
}
