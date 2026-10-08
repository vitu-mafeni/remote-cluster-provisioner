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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RemoteClusterSpec defines the desired state of RemoteCluster
type RemoteClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// foo is an example field of RemoteCluster. Edit remotecluster_types.go to remove/update
	// +optional
	ClusterName string    `json:"clusterName"` // this has to be unique for each cluster, and will be used as the cluster name when provisioning, and also will be used as the parent cluster
	Host        string    `json:"host"`
	VPNConfig   VPNConfig `json:"vpnConfig,omitempty"`
	Port        string    `json:"port"`
	User        string    `json:"user"`
	NodeInfo    NodeInfo  `json:"nodeInfo,omitempty"`

	Auth      RemoteClusterAuth `json:"auth"`
	GitConfig GitConfig         `json:"gitConfig,omitempty"`

	// DisableVPN runs the cluster without WireGuard. Use it when spec.host is
	// an IP that is directly reachable by the controller and by the other
	// nodes of the cluster. spec.host must be an IP address bound to a local
	// interface on the node; it is used as the kubelet node IP (and, for a
	// control-plane, the API server advertise address) in place of the wg0
	// address.
	//
	// The mode belongs to the whole cluster and is decided by the
	// control-plane: it publishes it in the cluster's NodeProvisionNetConfig,
	// workers (RemoteCluster and NodeProvision) inherit it, and a node that
	// disables the VPN under a VPN control-plane is failed. With it set, no VPN
	// server is contacted, no VPN range/credentials are published or copied, no
	// peer is removed, flannel is not pinned to wg0, WireGuard is not installed
	// or torn down, and spec.vpnConfig is ignored.
	// +optional
	DisableVPN bool `json:"disableVPN,omitempty"`
}

type VPNConfig struct {
	IP                   string               `json:"ip,omitempty"`
	VPNServerPublicIP    string               `json:"vpnServerPublicIP,omitempty"`
	VPNServerSSHPort     string               `json:"vpnServerSSHPort,omitempty"`
	VPNServerSSHUsername string               `json:"vpnServerSSHUsername,omitempty"`
	VPNSSHCredentialsRef VPNSSHCredentialsRef `json:"vpnSshCredentialsRef,omitempty"`
}

type VPNSSHCredentialsRef struct {
	Name      string `json:"name,omitempty"`
	NameSpace string `json:"namespace,omitempty"`
	Key       string `json:"key,omitempty"`
}

type NodeInfo struct {
	NodeType       string         `json:"nodeType"`     // control-plane or worker
	HardwareType   string         `json:"hardwareType"` // cpu or gpu
	SoftwareConfig SoftwareConfig `json:"softwareConfig,omitempty"`
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

// PlatformVariable is a single key/value pair injected into each PackageVariant's
// spec.packageContext.data, overriding the package's committed defaults.
type PlatformVariable struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SoftwareConfig struct {
	// NvidiaDriverVersion           string   `json:"nvidiaDriverVersion,omitempty"`
	// NvidiaContainerToolkitVersion string   `json:"nvidiaContainerToolkitVersion,omitempty"`
	// K8sDevicePluginVersion        string   `json:"k8sDevicePluginVersion,omitempty"`
	KubernetesVersion string         `json:"kubernetesVersion,omitempty"` // e.g., "v1.34.2"
	ImagePrepulls     []ImagePrepull `json:"imagePrepulls,omitempty"`

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

	// PlatformVariables holds cluster-specific key/value pairs injected into
	// every PackageVariant's spec.packageContext.data.  Each key overrides the
	// corresponding default in the package's committed package-context.yaml.
	// +optional
	PlatformVariables []PlatformVariable `json:"platformVariables,omitempty"`
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
	// Ubuntu 22.x and newer. Use it for clusters that mix OS releases; it
	// requires an explicit Version without an OS suffix.
	// +kubebuilder:validation:Enum=auto
	// +optional
	OSVariant string `json:"osVariant,omitempty"`
	// CredentialsRef references a Secret with "username" and "token" keys
	// for authenticating to the OCI registry. Follows the same pattern as
	// VPNSSHCredentialsRef — set Name to enable secret lookup.
	// +optional
	CredentialsRef VPNSSHCredentialsRef `json:"credentialsRef,omitempty"`
}

type GitConfig struct {
	Enable      string `json:"enable,omitempty"` // "true" or "false"
	GitServer   string `json:"gitServer"`        // e.g., "https://github.com"
	GitUsername string `json:"gitUsername"`      // e.g., "nephio"
	// UpstreamPlatformRepo is the name of the git repository in the management cluster that serves as the source of truth for platform configuration.
	UpstreamPlatformRepo string `json:"upstreamPlatformRepo"` // e.g., "catalog-workloads-mlplatform"
	PackageRevision      string `json:"packageRevision"`      // e.g., branch/tag/commit like "main" or "v1.0.0"
}

type RemoteClusterAuth struct {
	// PasswordSecretRef holds the secret reference for password-based SSH authentication.
	// +optional
	PasswordSecretRef *SecretKeyReference `json:"passwordSecretRef,omitempty"`

	// SSHPrivateKeySecretRef holds the secret reference for private-key based SSH authentication.
	// The secret value should be the raw private key data.
	// +optional
	SSHPrivateKeySecretRef *SecretKeyReference `json:"sshPrivateKeySecretRef,omitempty"`
}

type SecretKeyReference struct {
	Name string `json:"name"`
	// +optional
	Key string `json:"key,omitempty"`
}

// RemoteClusterStatus defines the observed state of RemoteCluster.
type RemoteClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions is an ordered list of provisioning progress entries.
	// Each entry records one step (success or failure) as it happens, building
	// a full audit trail rather than overwriting prior state.
	// +optional
	Conditions  []metav1.Condition `json:"conditions,omitempty"`
	Phase       string             `json:"phase,omitempty"`
	Message     string             `json:"message,omitempty"`
	JoinCommand string             `json:"joinCommand,omitempty"`

	// CnlabSyncRetryCount is the number of consecutive failures pushing
	// cnlab-runtime credentials to the remote cluster.  When it reaches
	// maxCnlabSyncRetries the controller stops retrying and surfaces an error
	// condition; the VPN link to the remote cluster may be down.
	// +optional
	CnlabSyncRetryCount int `json:"cnlabSyncRetryCount,omitempty"`

	// ProvisionRetryCount is the number of consecutive provisioning failures.
	// When it reaches maxProvisionRetries the controller stops retrying and
	// leaves the resource in Failed phase; manual intervention is required.
	// +optional
	ProvisionRetryCount int `json:"provisionRetryCount,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// RemoteCluster is the Schema for the remoteclusters API
type RemoteCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of RemoteCluster
	// +required
	Spec RemoteClusterSpec `json:"spec"`

	// status defines the observed state of RemoteCluster
	// +optional
	Status RemoteClusterStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// RemoteClusterList contains a list of RemoteCluster
type RemoteClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RemoteCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RemoteCluster{}, &RemoteClusterList{})
}
