package provision

import (
	"fmt"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	sshhelper "dcn.ssu.ac.kr/infra/helpers/ssh"
)

func ConfigureArgoCD(client *sshhelper.Client, cluster *infrav1.RemoteCluster) error {

	appProjectYAML := fmt.Sprintf(`
apiVersion: argoproj.io/v1alpha1
kind: AppProject
metadata:
  name: default
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  description: Project for %s
  sourceRepos:
    - '*'
  destinations:
    - namespace: '*'
      server: '*'
  clusterResourceWhitelist:
    - group: '*'
      kind: '*'
  namespaceResourceWhitelist:
    - group: '*'
      kind: '*'
`, cluster.Spec.ClusterName)

	applicationYAML := fmt.Sprintf(`
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: %s
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  source:
    repoURL: %s/%s/%s.git
    targetRevision: HEAD
    path: .
    directory:
      recurse: true
  destination:
    server: https://kubernetes.default.svc
    namespace: default
  syncPolicy:
    automated:
      prune: false
      selfHeal: false
      allowEmpty: true
    syncOptions:
      - CreateNamespace=true
  ignoreDifferences:
    - group: fn.kpt.dev
      kind: ApplyReplacements
    - group: fn.kpt.dev
      kind: StarlarkRun
`,
		cluster.Spec.ClusterName,
		cluster.Spec.GitConfig.GitServer,
		cluster.Spec.GitConfig.GitUsername,
		cluster.Spec.ClusterName,
	)

	commands := []string{
		// Apply AppProject
		fmt.Sprintf("cat <<EOF | kubectl apply -f -\n%s\nEOF", appProjectYAML),

		// Apply Application
		fmt.Sprintf("cat <<EOF | kubectl apply -f -\n%s\nEOF", applicationYAML),
	}
	fmt.Printf("Applying ArgoCD resources for cluster %s:\n", cluster.Spec.ClusterName)
	for _, cmd := range commands {
		output, err := sshhelper.Run(client, cmd)
		if err != nil {
			return fmt.Errorf("failed applying ArgoCD resource\ncmd: %s\noutput: %s", cmd, output)
		}
	}

	return nil
}
