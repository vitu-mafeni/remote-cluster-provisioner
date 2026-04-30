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
	ClusterName string   `json:"clusterName"` // this has to be unique for each cluster, and will be used as the cluster name when provisioning, and also will be used as the parent cluster
	Host        string   `json:"host"`
	Port        int      `json:"port"`
	User        string   `json:"user"`
	NodeInfo    NodeInfo `json:"nodeInfo,omitempty"`

	Auth       RemoteClusterAuth       `json:"auth"`
	Kubernetes RemoteClusterKubernetes `json:"kubernetes"`
	GitConfig  GitConfig               `json:"gitConfig,omitempty"`
}

type NodeInfo struct {
	NodeType       string         `json:"nodeType"`     // control-plane or worker
	HardwareType   string         `json:"hardwareType"` // cpu or gpu
	SoftwareConfig SoftwareConfig `json:"softwareConfig,omitempty"`
}

type SoftwareConfig struct {
	NvidiaDriverVersion           string `json:"nvidiaDriverVersion,omitempty"`
	NvidiaContainerToolkitVersion string `json:"nvidiaContainerToolkitVersion,omitempty"`
	K8sDevicePluginVersion        string `json:"k8sDevicePluginVersion,omitempty"`
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
	PasswordSecretRef SecretKeyReference `json:"passwordSecretRef"`
}

type RemoteClusterKubernetes struct {
	Version string `json:"version"`
}

type SecretKeyReference struct {
	Name string `json:"name"`
	Key  string `json:"key"`
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
