/*
Copyright 2026.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NodeProvisionNetConfigSpec defines the desired state of NodeProvisionNetConfig
type NodeProvisionNetConfigSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// DisableVPN marks the whole cluster as running without a VPN. Nodes
	// provisioned against this config inherit it (NodeProvision.spec.disableVPN
	// is set to true for them), and vpnRange/vpnServerPublicConfig are unused.
	// Serialized even when false so a sync can switch the mode back off.
	// +optional
	DisableVPN bool `json:"disableVPN"`

	// foo is an example field of NodeProvisionNetConfig. Edit nodeprovisionnetconfig_types.go to remove/update
	// +optional
	VPNRange              *string         `json:"vpnRange,omitempty"`
	VPNServerPublicConfig VPNServerConfig `json:"vpnServerPublicConfig,omitempty"`
	// ClusterName identifies the cluster this config belongs to (synced from
	// RemoteCluster.spec.clusterName). A NodeProvision selects its config by
	// setting NodeProvision.spec.clusterName to this value; it is required to be
	// unique among the NodeProvisionNetConfigs of a namespace when a namespace
	// holds more than one cluster.
	ClusterName    string         `json:"clusterName,omitempty"`
	SoftwareConfig SoftwareConfig `json:"softwareConfig,omitempty"`
}

type VPNServerConfig struct {
	PublicIP    string `json:"publicIP,omitempty"`
	SSHPort     string `json:"sshPort,omitempty"`
	SSHUsername string `json:"sshUsername,omitempty"`
	// VPNPort is the UDP port WireGuard listens on (default 51820).
	VPNPort string `json:"vpnPort,omitempty"`

	VPNSSHCredentialsRef VPNSSHCredentialsRef `json:"vpnSshCredentialsRef,omitempty"`
}

type VPNSSHCredentialsRef struct {
	Name      string `json:"name,omitempty"`
	NameSpace string `json:"namespace,omitempty"`
	// Key is the data key within the secret that holds the credential.
	Key string `json:"key,omitempty"`
}

// VPNPeerStatus records one registered WireGuard peer.
type VPNPeerStatus struct {
	NodeName  string `json:"nodeName,omitempty"`
	PublicKey string `json:"publicKey,omitempty"`
	VPNIP     string `json:"vpnIP,omitempty"`
}

// ImagePrepull describes a single image to pre-pull and which node types should pull it.
type ImagePrepull struct {
	// Image is the fully-qualified container image reference.
	Image string `json:"image"`
	// NodeTarget controls which worker nodes pull this image.
	// "gpu"  — GPU workers only.
	// "all"  — every worker node (CPU and GPU).
	// +kubebuilder:validation:Enum=gpu;all
	// +kubebuilder:default=all
	NodeTarget string `json:"nodeTarget,omitempty"`
}

type SoftwareConfig struct {
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`
	// NvidiaDriverVersion           string `json:"nvidiaDriverVersion,omitempty"`
	// NvidiaContainerToolkitVersion string `json:"nvidiaContainerToolkitVersion,omitempty"`
	// K8sDevicePluginVersion        string `json:"k8sDevicePluginVersion,omitempty"`

	ImagePrepulls []ImagePrepull `json:"imagePrepulls,omitempty"`

	// InsecureRegistries lists container registries (host or host:port, e.g.
	// "harbor.example.com:30002") that CRI-O must reach over plain HTTP or with
	// an untrusted/self-signed certificate. Each is written to a
	// /etc/containers/registries.conf.d drop-in with insecure = true on every
	// node provisioned by this operator (on-prem, AWS, GCP) BEFORE CRI-O starts.
	// Use it for registries that are not listed in imagePrepulls but are pulled
	// from by other workloads.
	//
	// Backward compatibility: the registry host of every fully-qualified image
	// in imagePrepulls is ALSO marked insecure, exactly as before; the effective
	// list is the de-duplicated, sorted union of both. Entries must be a bare
	// host or host:port: no scheme, path, spaces, quotes or other special
	// characters (invalid entries are rejected).
	//
	// CRI-O reads registries.conf.d only at start, so nodes that are already
	// provisioned need a one-time manual CRI-O restart after the drop-in is
	// written (see docs/controllers-user-guide.md).
	// +optional
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=259
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[0-9]{1,5})?$`
	InsecureRegistries []string `json:"insecureRegistries,omitempty"`

	// ImagePullSecretRef optionally references a Secret containing registry
	// credentials used when pre-pulling private images listed in ImagePrepulls.
	// The Secret must have "username" and "password" keys.
	// +optional
	ImagePullSecretRef *SecretKeyReference `json:"imagePullSecretRef,omitempty"`

	// CnlabRuntime configures the prebuilt cnlab-runtime OCI artifact installation.
	// All fields are optional; defaults apply when omitted.
	// +optional
	CnlabRuntime *CnlabRuntimeConfig `json:"cnlabRuntime,omitempty"`
}

// CnlabRuntimeConfig defines how to obtain the prebuilt cnlab-runtime OCI artifact.
type CnlabRuntimeConfig struct {
	// Registry is the OCI registry host. Default: "ghcr.io"
	// +optional
	Registry string `json:"registry,omitempty"`
	// Repository is the image repository path. Default: "vitu-mafeni/cnlab-runtime"
	// +optional
	Repository string `json:"repository,omitempty"`
	// Version is the artifact version tag. Default: "1.0.0-beta"
	// +optional
	Version string `json:"version,omitempty"`
	// OrasVersion is the ORAS CLI version to install. Default: "1.3.2"
	// +optional
	OrasVersion string `json:"orasVersion,omitempty"`
	// OSVariant selects how the artifact tag is chosen. Empty (default): Version
	// is used exactly as given. "auto": Version is the base version (e.g.
	// "1.0.2") and each node picks its own build from /etc/os-release, installing
	// "<Version>-ubuntu20" on Ubuntu 20.x/21.x and "<Version>-ubuntu22" on
	// Ubuntu 22.x-25.x and "<Version>-ubuntu26" on Ubuntu 26.x and newer. Use it
	// for clusters that mix supported OS releases; it requires an explicit
	// Version without an OS suffix.
	// +kubebuilder:validation:Enum=auto
	// +optional
	OSVariant string `json:"osVariant,omitempty"`
	// CredentialsRef references a Secret with "username" and "token" keys
	// for authenticating to the OCI registry. Follows the same pattern as
	// VPNSSHCredentialsRef — set Name to enable secret lookup.
	// +optional
	CredentialsRef VPNSSHCredentialsRef `json:"credentialsRef,omitempty"`
}

// SecretKeyReference identifies a Kubernetes Secret by name and an optional key.
type SecretKeyReference struct {
	Name string `json:"name"`
	// Key is the data key within the secret. When omitted, the controller uses
	// well-known key names (username / password).
	// +optional
	Key string `json:"key,omitempty"`
}

// NodeProvisionNetConfigStatus defines the observed state of NodeProvisionNetConfig.
type NodeProvisionNetConfigStatus struct {
	// +optional
	UsedIPAddresses []string `json:"usedIPAddresses,omitempty"`
	// ClusterJoinCommand is the kubeadm join command for worker nodes.
	ClusterJoinCommand string `json:"clusterJoinCommand,omitempty"`
	// JoinTokenRefreshedAt records when ClusterJoinCommand was last updated with
	// a fresh kubeadm bootstrap token.  The NodeProvision controller uses this to
	// ensure it never launches an EC2 instance with a token that is close to its
	// 24-hour expiry.
	// +optional
	JoinTokenRefreshedAt *metav1.Time `json:"joinTokenRefreshedAt,omitempty"`
	// VPNPeers tracks every WireGuard peer registered on the VPN server.
	// +optional
	VPNPeers []VPNPeerStatus `json:"vpnPeers,omitempty"`
	// Kubeconfig is the base64-encoded admin kubeconfig for this cluster.
	// Populated after control-plane init and refreshed locally by the
	// kubeconfig-refresh systemd timer on the control-plane node so that
	// the remote cluster can stay self-sufficient without the management cluster.
	// +optional
	Kubeconfig string `json:"kubeconfig,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// NodeProvisionNetConfig is the Schema for the nodeprovisionnetconfigs API
type NodeProvisionNetConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of NodeProvisionNetConfig
	// +required
	Spec NodeProvisionNetConfigSpec `json:"spec"`

	// status defines the observed state of NodeProvisionNetConfig
	// +optional
	Status NodeProvisionNetConfigStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// NodeProvisionNetConfigList contains a list of NodeProvisionNetConfig
type NodeProvisionNetConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []NodeProvisionNetConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NodeProvisionNetConfig{}, &NodeProvisionNetConfigList{})
}
