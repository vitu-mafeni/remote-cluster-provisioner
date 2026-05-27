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

type NodeProvisionPhase string
type CloudProvider string

const (
	NodeProvisionPhasePending      NodeProvisionPhase = "Pending"
	NodeProvisionPhaseProvisioning NodeProvisionPhase = "Provisioning"
	NodeProvisionPhaseJoining      NodeProvisionPhase = "Joining"
	NodeProvisionPhaseReady        NodeProvisionPhase = "Ready"
	NodeProvisionPhaseFailed       NodeProvisionPhase = "Failed"

	CloudProviderAWS    CloudProvider = "AWS"
	CloudProviderGCP    CloudProvider = "GCP"
	CloudProviderAzure  CloudProvider = "Azure"
	CloudProviderOnPrem CloudProvider = "OnPrem"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NodeProvisionSpec defines the desired state of NodeProvision
// CredentialsRef references the Secret containing provider credentials.
type CredentialsRef struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	// Key is the data key within the secret that holds the credential.
	// When omitted the controller tries well-known names: privateKey, id_rsa,
	// ssh-privatekey, password, key.
	Key string `json:"key,omitempty"`
}

// NodeProvisionSpec defines the desired state of NodeProvision.
type NodeProvisionSpec struct {
	Provider CloudProvider `json:"provider,omitempty"`

	Role string `json:"role,omitempty"`

	NodeLabel string `json:"nodeLabel,omitempty"`

	Region string `json:"region,omitempty"`

	InstanceType string `json:"instanceType,omitempty"`

	InstanceID string `json:"instanceId,omitempty"`

	Hostname string `json:"hostname,omitempty"`

	IPAddress string `json:"ipAddress,omitempty"`
	SSHPort   int    `json:"sshPort,omitempty"`

	SSHUsernameOverride string `json:"sshUsernameOverride,omitempty"`

	CredentialsRef CredentialsRef `json:"credentialsRef,omitempty"`
}

// NodeProvisionStatus defines the observed state of NodeProvision.

type NodeProvisionStatus struct {
	// Current lifecycle phase.
	Phase NodeProvisionPhase `json:"phase,omitempty"`

	// Human-readable status message.
	Message string `json:"message,omitempty"`

	// Timestamp when provisioning started.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Timestamp when provisioning completed.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Provider-generated instance ID.
	InstanceID string `json:"instanceId,omitempty"`

	// Assigned hostname.
	Hostname string `json:"hostname,omitempty"`

	// Assigned node IP.
	IPAddress string `json:"ipAddress,omitempty"`

	// Kubernetes node name after join.
	NodeName string `json:"nodeName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// NodeProvision is the Schema for the nodeprovisions API
type NodeProvision struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of NodeProvision
	// +required
	Spec NodeProvisionSpec `json:"spec"`

	// status defines the observed state of NodeProvision
	// +optional
	Status NodeProvisionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// NodeProvisionList contains a list of NodeProvision
type NodeProvisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []NodeProvision `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NodeProvision{}, &NodeProvisionList{})
}
