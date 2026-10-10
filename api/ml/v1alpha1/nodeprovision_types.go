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
	NodeProvisionPhasePending            NodeProvisionPhase = "Pending"
	NodeProvisionPhaseValidating         NodeProvisionPhase = "Validating"
	NodeProvisionPhaseCreatingInstance   NodeProvisionPhase = "CreatingInstance"
	NodeProvisionPhaseWaitingForInstance NodeProvisionPhase = "WaitingForInstance"
	NodeProvisionPhaseConfiguringVPN     NodeProvisionPhase = "ConfiguringVPN"
	NodeProvisionPhaseProvisioning       NodeProvisionPhase = "Provisioning"
	NodeProvisionPhaseBootstrapping      NodeProvisionPhase = "Bootstrapping"
	NodeProvisionPhaseRegisteringNode    NodeProvisionPhase = "RegisteringNode"
	NodeProvisionPhaseJoining            NodeProvisionPhase = "Joining"
	NodeProvisionPhaseVerifyingHealth    NodeProvisionPhase = "VerifyingHealth"
	NodeProvisionPhaseReady              NodeProvisionPhase = "Ready"
	NodeProvisionPhaseFailed             NodeProvisionPhase = "Failed"
	NodeProvisionPhaseDeleting           NodeProvisionPhase = "Deleting"
	NodeProvisionPhasePrePullingImages   NodeProvisionPhase = "PrePullingImages"

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

// AWSConfig holds AWS-specific parameters for EC2 node provisioning.
type AWSConfig struct {
	// VPC ID where the instance will be launched, e.g. "vpc-0123456789abcdef0".
	// When subnetId is also set it must be that subnet's VPC. When only vpcId is
	// set, a subnet of this VPC is chosen. When neither is set the region's
	// default VPC is used (and created if the region has none).
	// +optional
	VPCID string `json:"vpcId,omitempty"`
	// Subnet ID for the instance's primary network interface. Determines the VPC
	// when vpcId is unset. Must be in spec.region.
	// +optional
	SubnetID string `json:"subnetId,omitempty"`
	// Security group IDs to attach to the instance; they must all be in the
	// instance's VPC. When vpcId and subnetId are unset, the VPC is taken from
	// these groups. When empty, the default security group of the resolved VPC is
	// used: if the controller chose the whole network (nothing set here, vpcId
	// or subnetId) it opens SSH, and WireGuard when the VPN is enabled, on that
	// group; for a VPC or subnet you named it leaves the group's rules untouched.
	// +optional
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
	// DisablePublicIP launches the instance without a public IPv4 address, for
	// private subnets. The node then needs an egress path (NAT gateway, transit
	// gateway, ...) for package/image downloads and, with a VPN, to reach the VPN
	// server. Default false: the instance gets a public IP.
	// +optional
	DisablePublicIP bool `json:"disablePublicIp,omitempty"`
	// SkipControlPlaneVPCCheck turns off the no-VPN guard that fails a node whose
	// VPC does not contain the control plane's (private) API endpoint. Without a
	// VPN every node must be able to route to the control plane; set this only
	// when it is reachable by VPC peering or a transit gateway.
	// +optional
	SkipControlPlaneVPCCheck bool `json:"skipControlPlaneVpcCheck,omitempty"`
	// AMI ID to use for the instance.
	AMI string `json:"ami,omitempty"`
	// EC2 key pair name for SSH access (optional when using cloud-init only).
	// +optional
	KeyPairName string `json:"keyPairName,omitempty"`
	// IAM instance profile name or ARN for the instance.
	// +optional
	IAMInstanceProfile string `json:"iamInstanceProfile,omitempty"`
	// Additional tags to apply to created AWS resources.
	// +optional
	Tags map[string]string `json:"tags,omitempty"`
	// RootVolumeSizeGB overrides the root EBS volume size in GB.
	// Defaults to 50 GB when unset. The Ubuntu 22.04 AMI default (8 GB) is
	// too small for a Kubernetes node running CRI-O and container images.
	// +optional
	RootVolumeSizeGB int32 `json:"rootVolumeSizeGB,omitempty"`
}

// GCPAccelerator attaches GPUs to an N1 instance. A2/A3/G2 machine types include
// their GPUs, so leave this unset for them.
type GCPAccelerator struct {
	// Type is the accelerator type name, e.g. "nvidia-tesla-t4", "nvidia-tesla-v100".
	// +kubebuilder:validation:MinLength=1
	Type string `json:"type"`
	// Count is the number of GPUs to attach (default 1).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	Count int32 `json:"count,omitempty"`
}

// GCPConfig holds Google Compute Engine parameters for GCP node provisioning.
//
// The machine type is spec.instanceType (e.g. "e2-standard-4", "n1-standard-8",
// "g2-standard-8"). The location is spec.region and/or gcpConfig.zone: a zone
// ("us-central1-a") implies its region and, if spec.region is also set, must
// belong to it; with only a region the controller picks a zone of that region
// that offers the machine type (and accelerator) and records it in this field.
type GCPConfig struct {
	// ProjectID is the GCP project to create the instance in. Defaults to the
	// project_id of the service-account key in spec.credentialsRef.
	// +optional
	ProjectID string `json:"projectId,omitempty"`
	// Zone is the compute zone, e.g. "us-central1-a" (see the type comment).
	// +optional
	Zone string `json:"zone,omitempty"`
	// Network is the VPC network name or "projects/<host>/global/networks/<n>"
	// (Shared VPC). When unset it is taken from subnetwork if that is set,
	// otherwise "default". If both are set the subnetwork must belong to it.
	// +optional
	Network string `json:"network,omitempty"`
	// Subnetwork is a subnetwork name (in the network's project), or
	// "regions/<r>/subnetworks/<n>", "projects/<p>/regions/<r>/subnetworks/<n>"
	// or a full URL. It must be in the instance's region and PRIVATE. Required for custom-mode networks; optional for auto-mode ones.
	// +optional
	Subnetwork string `json:"subnetwork,omitempty"`
	// SourceImage is the boot image: a full image URL
	// ("projects/<p>/global/images/<name>") or a "projects/<p>/global/images/family/<f>"
	// reference. When empty the latest image of imageFamily/imageProject is
	// resolved and recorded here (default: latest Ubuntu 22.04 LTS, x86_64).
	// +optional
	SourceImage string `json:"sourceImage,omitempty"`
	// ImageFamily is the image family used when sourceImage is empty.
	// Defaults to "ubuntu-2204-lts".
	// +optional
	ImageFamily string `json:"imageFamily,omitempty"`
	// ImageProject is the project that publishes imageFamily.
	// Defaults to "ubuntu-os-cloud".
	// +optional
	ImageProject string `json:"imageProject,omitempty"`
	// BootDiskSizeGB is the boot disk size in GB (default 50).
	// +kubebuilder:validation:Minimum=10
	// +optional
	BootDiskSizeGB int32 `json:"bootDiskSizeGB,omitempty"`
	// BootDiskType is the persistent disk type, e.g. "pd-balanced" (default),
	// "pd-ssd", "pd-standard" or a hyperdisk type required by newer machine series.
	// +optional
	BootDiskType string `json:"bootDiskType,omitempty"`
	// Labels are extra GCE labels for the instance. Controller labels cannot be overridden.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// NetworkTags are extra network tags for the instance (firewall targeting).
	// +optional
	NetworkTags []string `json:"networkTags,omitempty"`
	// ServiceAccountEmail attaches this service account to the instance. When
	// empty no service account is attached (least privilege; the node does not need one).
	// +optional
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`
	// ServiceAccountScopes are the OAuth scopes of the attached service account.
	// Defaults to cloud-platform (access is then governed by IAM).
	// +optional
	ServiceAccountScopes []string `json:"serviceAccountScopes,omitempty"`
	// Accelerator attaches GPUs to an N1 instance (see GCPAccelerator). GPU
	// instances always use onHostMaintenance=TERMINATE.
	// +optional
	Accelerator *GCPAccelerator `json:"accelerator,omitempty"`
	// Spot runs the instance as a Spot VM (can be preempted at any time).
	// +optional
	Spot bool `json:"spot,omitempty"`
	// DisableExternalIP creates the instance without an external IP address. The
	// node then needs Cloud NAT (or another egress path) for package downloads
	// and, with a VPN, to reach the VPN server.
	// +optional
	DisableExternalIP bool `json:"disableExternalIP,omitempty"`
	// FirewallSourceRanges are the CIDRs allowed to reach the node through the
	// per-node firewall rules (SSH, and kubelet/flannel without a VPN). Defaults
	// to the RFC1918 private ranges: the controller and the control plane reach
	// the node's internal address. With a VPN SSH travels inside the tunnel, so
	// the SSH rule is created only when this is set.
	// +optional
	FirewallSourceRanges []string `json:"firewallSourceRanges,omitempty"`
}

// NodeProvisionSpec defines the desired state of NodeProvision.
type NodeProvisionSpec struct {
	Provider CloudProvider `json:"provider,omitempty"`

	// ClusterName names the cluster this node joins: the NodeProvisionNetConfig
	// in the namespace whose spec.clusterName equals this value supplies the VPN
	// range, VPN mode, image pre-pulls, registry secret and join command.
	// Provisioning fails with a clear error when none or several match.
	//
	// Optional only for backward compatibility: when empty and the namespace
	// holds exactly one NodeProvisionNetConfig, that one is used; when the
	// namespace holds several, provisioning fails and asks for this field
	// (the controller never guesses). Set it whenever a namespace serves more
	// than one cluster.
	// +optional
	ClusterName string `json:"clusterName,omitempty"`

	// HardwareType classifies the node for image pre-pull targeting.
	// "gpu" — node has GPUs; pulls images with nodeTarget "gpu" and "all".
	// "cpu" (or empty) — pulls only images with nodeTarget "all".
	// +optional
	HardwareType string `json:"hardwareType,omitempty"`

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

	// DisableVPN provisions the node without a WireGuard tunnel: no VPN server
	// connection, IP allocation, peer, WireGuard package/config or security
	// group rule is made. Use it when the node is directly reachable by the
	// controller and the control plane, and the control plane's API endpoint
	// (the address in the kubeadm join command) is reachable from the node.
	//
	// The mode belongs to the whole cluster and is taken from the
	// NodeProvisionNetConfig (spec.disableVPN, synced from the control-plane
	// RemoteCluster): a node in a VPN-less cluster inherits true, and setting
	// true against a VPN cluster fails the provision. Setting it here is only
	// needed for a hand-authored NodeProvisionNetConfig.
	//
	// The node's own address is used as the kubelet node IP:
	//   - OnPrem: spec.ipAddress, which must be an IP bound to a local interface.
	//   - AWS: the instance's private IP (a public EC2 IP is NATed and cannot be
	//     a kubelet node IP), so the control plane must be able to route to it,
	//     e.g. same or peered VPC.
	//   - GCP: the instance's internal (VPC) IP, for the same reason; the
	//     per-node firewall rules admit the control plane / controller ranges
	//     (gcpConfig.firewallSourceRanges).
	// +optional
	DisableVPN bool `json:"disableVPN,omitempty"`

	// AWSConfig holds provider-specific parameters for AWS EC2 provisioning.
	// +optional
	AWSConfig *AWSConfig `json:"awsConfig,omitempty"`

	// GCPConfig holds provider-specific parameters for Google Compute Engine
	// provisioning (provider: GCP).
	// +optional
	GCPConfig *GCPConfig `json:"gcpConfig,omitempty"`
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

	// Provider-generated instance ID (e.g. i-xxxxxxxxxxxxxxxxx for AWS; the
	// instance name for GCP, which is unique within the zone).
	InstanceID string `json:"instanceId,omitempty"`

	// Assigned hostname.
	Hostname string `json:"hostname,omitempty"`

	// IPAddress is the VPN (WireGuard) IP assigned to the node.
	IPAddress string `json:"ipAddress,omitempty"`

	// PublicIP is the cloud-provider public IP of the instance.
	// +optional
	PublicIP string `json:"publicIp,omitempty"`

	// PrivateIP is the cloud-provider private IP of the instance.
	// +optional
	PrivateIP string `json:"privateIp,omitempty"`

	// VpnIP is the WireGuard VPN IP allocated for this node.
	// +optional
	VpnIP string `json:"vpnIp,omitempty"`

	// Progress is a 0–100 percentage of provisioning completion.
	// +optional
	Progress int `json:"progress,omitempty"`

	// LastUpdated is the timestamp of the most recent status update.
	// +optional
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`

	// Kubernetes node name after join.
	NodeName string `json:"nodeName,omitempty"`

	// RuntimeCredentialsHash is the SHA-256 hex digest of the registry
	// username+token most recently synced to this node via oras login.
	// The controller re-runs the login whenever this hash differs from the
	// current secret, ensuring the node stays authorised to pull runtime updates.
	// +optional
	RuntimeCredentialsHash string `json:"runtimeCredentialsHash,omitempty"`

	// ProvisionRetryCount is the number of consecutive provisioning failures.
	// When it reaches maxProvisionRetries the controller stops retrying and
	// leaves the resource in Failed phase; manual intervention is required.
	// +optional
	ProvisionRetryCount int `json:"provisionRetryCount,omitempty"`
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
