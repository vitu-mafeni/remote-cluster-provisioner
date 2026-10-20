package ml

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
)

// Sentinel errors of the NodeProvisionNetConfig selection (see selectNetConfig).
// Callers test them with errors.Is: "not found" is usually transient (the
// NetConfig is synced from the RemoteCluster and may not exist yet), while
// "ambiguous" is a spec error only the user can fix.
var (
	errNetConfigNotFound  = errors.New("no NodeProvisionNetConfig")
	errNetConfigAmbiguous = errors.New("ambiguous NodeProvisionNetConfig")
)

// selectNetConfig is THE rule for which NodeProvisionNetConfig a NodeProvision
// uses, shared by every reader of the config (requireNetConfig, the VPN-mode
// reconcile, VPN peer cleanup and, through requireNetConfig, the AWS/GCP/on-prem
// provisioning, pre-pull and registry-credential code). items must be the
// NetConfigs of np's namespace.
//
//   - np.spec.clusterName set: the single NetConfig whose spec.clusterName
//     equals it; none or several matching is an error.
//   - np.spec.clusterName empty: backward compatible with the pre-clusterName
//     behaviour when the namespace holds exactly one NetConfig (it is used,
//     whatever its clusterName); with several the controller refuses to guess
//     and returns an errNetConfigAmbiguous asking for spec.clusterName.
func selectNetConfig(items []mlv1alpha1.NodeProvisionNetConfig, np *mlv1alpha1.NodeProvision) (*mlv1alpha1.NodeProvisionNetConfig, error) {
	if want := np.Spec.ClusterName; want != "" {
		var match []int
		for i := range items {
			if items[i].Spec.ClusterName == want {
				match = append(match, i)
			}
		}
		switch len(match) {
		case 1:
			return &items[match[0]], nil
		case 0:
			return nil, fmt.Errorf("%w with spec.clusterName %q in namespace %q (existing: %s)",
				errNetConfigNotFound, want, np.Namespace, describeNetConfigs(items))
		default:
			sub := make([]mlv1alpha1.NodeProvisionNetConfig, 0, len(match))
			for _, i := range match {
				sub = append(sub, items[i])
			}
			return nil, fmt.Errorf("%w: %d NodeProvisionNetConfigs in namespace %q have spec.clusterName %q (%s); exactly one is allowed",
				errNetConfigAmbiguous, len(match), np.Namespace, want, describeNetConfigs(sub))
		}
	}
	switch len(items) {
	case 0:
		return nil, errNetConfigNotFound
	case 1:
		return &items[0], nil
	default:
		return nil, fmt.Errorf("%w: namespace %q has %d NodeProvisionNetConfigs (%s) and NodeProvision %q does not set spec.clusterName; "+
			"set spec.clusterName to the cluster this node joins (the RemoteCluster spec.clusterName)",
			errNetConfigAmbiguous, np.Namespace, len(items), describeNetConfigs(items), np.Name)
	}
}

// describeNetConfigs renders "name (clusterName=x), ..." in a stable order for
// error messages.
func describeNetConfigs(items []mlv1alpha1.NodeProvisionNetConfig) string {
	parts := make([]string, 0, len(items))
	for i := range items {
		parts = append(parts, fmt.Sprintf("%s (clusterName=%q)", items[i].Name, items[i].Spec.ClusterName))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

// netConfigReader is the reader for NetConfigs: the uncached API reader when
// configured (every field of the object is load-bearing for provisioning and
// must not suffer informer lag), else the manager client.
func (r *NodeProvisionReconciler) netConfigReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// netConfigFor lists the NodeProvisionNetConfigs of np's namespace and applies
// selectNetConfig. The returned error wraps errNetConfigNotFound or
// errNetConfigAmbiguous for the selection outcomes; any other error is a
// failed list.
func (r *NodeProvisionReconciler) netConfigFor(ctx context.Context, np *mlv1alpha1.NodeProvision) (*mlv1alpha1.NodeProvisionNetConfig, error) {
	list := &mlv1alpha1.NodeProvisionNetConfigList{}
	if err := r.netConfigReader().List(ctx, list, client.InNamespace(np.Namespace)); err != nil {
		return nil, fmt.Errorf("listing NodeProvisionNetConfigs: %w", err)
	}
	return selectNetConfig(list.Items, np)
}
