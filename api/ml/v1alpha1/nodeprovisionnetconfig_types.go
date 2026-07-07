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

	// foo is an example field of NodeProvisionNetConfig. Edit nodeprovisionnetconfig_types.go to remove/update
	// +optional
	VPNRange              *string         `json:"vpnRange,omitempty"`
	VPNServerPublicConfig VPNServerConfig `json:"vpnServerPublicConfig,omitempty"`
	ClusterName           string          `json:"clusterName,omitempty"`
	SoftwareConfig        SoftwareConfig  `json:"softwareConfig,omitempty"`
}

type VPNServerConfig struct {
	PublicIP    string `json:"publicIP,omitempty"`
	SSHPort     int    `json:"sshPort,omitempty"`
	SSHUsername string `json:"sshUsername,omitempty"`
	// VPNPort is the UDP port WireGuard listens on (default 51820).
	VPNPort int `json:"vpnPort,omitempty"`

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

type SoftwareConfig struct {
	KubernetesVersion             string `json:"kubernetesVersion,omitempty"`
	NvidiaDriverVersion           string `json:"nvidiaDriverVersion,omitempty"`
	NvidiaContainerToolkitVersion string `json:"nvidiaContainerToolkitVersion,omitempty"`
	K8sDevicePluginVersion        string `json:"k8sDevicePluginVersion,omitempty"`

	ImagePrepulls []string `json:"imagePrepulls,omitempty"`

	// ImagePullSecretRef optionally references a Secret containing registry
	// credentials used when pre-pulling private images listed in ImagePrepulls.
	// The Secret must have "username" and "password" keys.
	// +optional
	ImagePullSecretRef *SecretKeyReference `json:"imagePullSecretRef,omitempty"`
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
