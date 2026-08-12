package runtime

const (
	DefaultRegistry    = "ghcr.io"
	DefaultRepository  = "vitu-mafeni/cnlab-runtime"
	DefaultVersion     = "1.0.0-beta"
	DefaultOrasVersion = "1.3.2"
)

// Config holds the resolved runtime artifact configuration with credentials
// already extracted from the Kubernetes Secret. The Token field must never
// be logged.
type Config struct {
	Registry    string
	Repository  string
	Version     string
	OrasVersion string
	Username    string
	Token       string // never log this value
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
func (c *Config) ImageRef() string {
	return c.Registry + "/" + c.Repository + ":" + c.Version
}
