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
	VPNRange       *string        `json:"vpnRange,omitempty"`
	ClusterName    string         `json:"clusterName,omitempty"`
	SoftwareConfig SoftwareConfig `json:"softwareConfig,omitempty"`
}

type SoftwareConfig struct {
	KubernetesVersion             string `json:"kubernetesVersion,omitempty"`
	NvidiaDriverVersion           string `json:"nvidiaDriverVersion,omitempty"`
	NvidiaContainerToolkitVersion string `json:"nvidiaContainerToolkitVersion,omitempty"`
	K8sDevicePluginVersion        string `json:"k8sDevicePluginVersion,omitempty"`
}

// NodeProvisionNetConfigStatus defines the observed state of NodeProvisionNetConfig.
type NodeProvisionNetConfigStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the NodeProvisionNetConfig resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	UsedIPAddresses    []string `json:"usedIPAddresses,omitempty"`
	ClusterJoinCommand string   `json:"clusterJoinCommand,omitempty"`
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
