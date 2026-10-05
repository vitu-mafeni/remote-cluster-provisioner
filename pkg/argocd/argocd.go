package argocd

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

// clusterNameRE is a DNS-1123 subdomain: the cluster name becomes the Argo CD
// Application's metadata.name and is interpolated into manifests.
var clusterNameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)

// yamlStr renders s as a YAML scalar. A JSON string is valid YAML (double
// quoted, escaped), so no value can break out of its scalar or inject keys.
func yamlStr(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// applyManifestCmd returns a shell command that applies manifest from a
// heredoc. The delimiter is quoted (no shell expansion of the body) and the
// body only contains JSON-quoted scalars, which cannot contain a newline and
// therefore cannot terminate the heredoc early.
func applyManifestCmd(manifest string) string {
	return "kubectl apply -f - <<'ARGOCD_MANIFEST_EOF'\n" + manifest + "\nARGOCD_MANIFEST_EOF"
}

// ConfigureArgoCD applies the AppProject/Application for the cluster and sets
// the Argo CD admin password. It never includes remote command text in returned
// errors.
func ConfigureArgoCD(client *sshhelper.Client, cluster *infrav1.RemoteCluster) error {
	clusterName := cluster.Spec.ClusterName
	if !clusterNameRE.MatchString(clusterName) {
		return fmt.Errorf("spec.clusterName %q is not a valid Kubernetes resource name (lowercase alphanumerics, '-' and '.')", clusterName)
	}

	appProjectYAML := fmt.Sprintf(`apiVersion: argoproj.io/v1alpha1
kind: AppProject
metadata:
  name: default
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  description: %s
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
  roles:
    - name: deny-overrides
      description: Prevent live parameter overrides — all changes must go through git
      policies:
        - p, proj:default:deny-overrides, applications, override, default/*, deny
      groups:
        - '*'`, yamlStr("Project for "+clusterName))

	repoURL := fmt.Sprintf("%s/%s/%s.git",
		cluster.Spec.GitConfig.GitServer, cluster.Spec.GitConfig.GitUsername, clusterName)

	applicationYAML := fmt.Sprintf(`apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: %s
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  source:
    repoURL: %s
    targetRevision: HEAD
    path: .
    directory:
      recurse: true
  destination:
    server: https://kubernetes.default.svc
    namespace: default
  revisionHistoryLimit: 10
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
      allowEmpty: true
    syncOptions:
      - CreateNamespace=true
      - SkipDryRunOnMissingResource=true
      - ApplyOutOfSyncOnly=true
      - PruneLast=true
      - RespectIgnoreDifferences=true
    retry:
      limit: -1
      backoff:
        duration: 30s
        maxDuration: 5m
        factor: 2
  ignoreDifferences:
    - group: fn.kpt.dev
      kind: ApplyReplacements
    - group: fn.kpt.dev
      kind: StarlarkRun`, yamlStr(clusterName), yamlStr(repoURL))

	steps := []struct{ label, cmd string }{
		// Patch argocd-cm: set reconciliation interval so ArgoCD detects drift
		// every 60 s and immediately applies changes (automated sync below).
		{"patch argocd-cm reconciliation interval", `kubectl patch configmap argocd-cm -n argocd --type merge \
  -p '{"data":{"timeout.reconciliation":"60s"}}'`},

		// Expose ArgoCD server HTTPS port as NodePort 32210 (unchanged behaviour;
		// restrict reachability with the node's firewall / security group).
		{"expose argocd-server as NodePort", `kubectl patch svc argocd-server -n argocd --type json \
  -p '[{"op":"replace","path":"/spec/type","value":"NodePort"},{"op":"add","path":"/spec/ports/1/nodePort","value":32210}]'`},

		// Set admin password to admin123 (bcrypt-hashed on the node at runtime).
		{"set argocd admin password", `python3 -c 'import bcrypt' 2>/dev/null || apt-get install -y -q python3-bcrypt 2>/dev/null
ARGOCD_PASS=$(python3 -c 'import bcrypt; print(bcrypt.hashpw(b"admin123", bcrypt.gensalt(10)).decode())')
kubectl -n argocd patch secret argocd-secret \
  -p "{\"stringData\":{\"admin.password\":\"${ARGOCD_PASS}\",\"admin.passwordMtime\":\"$(date +%FT%T%Z)\"}}"`},

		{"apply argocd AppProject", "set -o pipefail\n" + applyManifestCmd(appProjectYAML)},
		{"apply argocd Application", "set -o pipefail\n" + applyManifestCmd(applicationYAML)},
	}
	log.Printf("Applying ArgoCD resources for cluster %s", clusterName)
	for i, st := range steps {
		output, err := sshhelper.Run(client, st.cmd)
		if err != nil {
			return sshhelper.StepError(fmt.Sprintf("argocd step %d/%d (%s)", i+1, len(steps), st.label), err, output)
		}
	}

	return nil
}
