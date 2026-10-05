package aws

import (
	"strings"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// IsGPUNode is the single rule that decides whether a NodeProvision is a GPU
// node, shared by the controller (labels, taints, image pre-pull) and the AWS
// cloud-init (GPU node setup): spec.hardwareType decides when set ("gpu" is a
// GPU node, anything else is not); when it is empty a spec.nodeLabel containing
// "gpu" decides.
func IsGPUNode(np *mlv1alpha1.NodeProvision) bool {
	hw := strings.TrimSpace(np.Spec.HardwareType)
	if hw != "" {
		return strings.EqualFold(hw, "gpu")
	}
	return strings.Contains(strings.ToLower(np.Spec.NodeLabel), "gpu")
}
