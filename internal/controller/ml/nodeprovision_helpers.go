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

package ml

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

// ────────────────────────────────────────────────────────────────────────────
// Secret redaction for status messages / logs
// ────────────────────────────────────────────────────────────────────────────

// statusMessageMaxLen bounds how much of an error is copied into
// Status.Message (and the log line that mirrors it).
const statusMessageMaxLen = 1000

var (
	// Full PEM blocks (kept non-greedy so several blocks are all redacted).
	rePEMBlock = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*KEY-----.*?-----END [A-Z0-9 ]*KEY-----`)
	// A PEM block that lost its END marker (e.g. truncated output): redact the rest.
	rePEMOpen = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*KEY-----.*`)
	// Well-known token formats (GitHub, Docker Hub, AWS access key ids).
	reTokenFormats = regexp.MustCompile(
		`gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|dckr_pat_[A-Za-z0-9_\-]{10,}|A[KS]IA[0-9A-Z]{16}`)
	// crictl/oras style "--creds user:pass" (not among the shared flag names).
	reCredsFlag = regexp.MustCompile(`(?i)(--creds(?:\s+|=))("[^"]*"|'[^']*'|\S+)`)
	// "token <opaque value>" with no separator; the length floor keeps ordinary
	// prose ("token authentication") intact.
	reTokenWord = regexp.MustCompile(`(?i)(\btoken\s+)[A-Za-z0-9._~+/=-]{16,}`)
	// HTTP bearer tokens. The value must look like a credential (contain a
	// digit or a token punctuation character, or be a long opaque word) so that
	// prose such as "bearer authentication" is left alone.
	reBearer = regexp.MustCompile(
		`(?i)(\bbearer\s+)(?:[A-Za-z0-9._~+/=-]*[0-9._~+/=-][A-Za-z0-9._~+/=-]*|[A-Za-z]{20,})`)
	// user:password@ in a URL.
	reURLUserinfo = regexp.MustCompile(`(?i)(\b[a-z][a-z0-9+.-]*://[^\s/:@]+:)[^\s/@]+@`)
)

const redacted = "[REDACTED]"

// redactSecrets masks credential-looking substrings so an error can be shown
// in Status.Message or logged without leaking tokens, private keys or
// passwords. It is deliberately conservative: it may over-redact, never
// under-redact the formats it knows about.
//
// The general key/value, flag, JSON, Authorization and kubeadm-token rules live
// in sshhelper.RedactSecrets (shared with the SSH step errors); this adds the
// formats specific to this controller: PEM blocks (also unterminated), GitHub/
// Docker/AWS token shapes, --creds, bare "token <value>", short bearer tokens and URL
// userinfo.
func redactSecrets(s string) string {
	if s == "" {
		return s
	}
	s = rePEMBlock.ReplaceAllString(s, redacted)
	s = rePEMOpen.ReplaceAllString(s, redacted)
	s = reTokenFormats.ReplaceAllString(s, redacted)
	s = reCredsFlag.ReplaceAllString(s, "${1}"+redacted)
	s = reTokenWord.ReplaceAllString(s, "${1}"+redacted)
	s = reBearer.ReplaceAllString(s, "${1}"+redacted)
	s = reURLUserinfo.ReplaceAllString(s, "${1}"+redacted+"@")
	return sshhelper.RedactSecrets(s)
}

// truncateMessage shortens s to at most max bytes on a rune boundary,
// appending a marker when anything was cut.
func truncateMessage(s string, max int) string {
	if len(s) <= max {
		return s
	}
	const marker = "... (truncated)"
	cut := max - len(marker)
	if cut < 0 {
		cut = 0
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + marker
}

// sanitizeStatusMessage redacts secrets and cloud account identifiers (AWS ARNs,
// account and request IDs from raw SDK errors) and bounds the length.
func sanitizeStatusMessage(s string) string {
	return truncateMessage(awsprovision.RedactIdentifiers(redactSecrets(s)), statusMessageMaxLen)
}

// ────────────────────────────────────────────────────────────────────────────
// Shell helpers
// ────────────────────────────────────────────────────────────────────────────

// shellQuote returns s as a single POSIX-shell word (single-quoted, with
// embedded single quotes escaped).
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// wgPublicKeyRe matches a WireGuard key: 32 bytes of standard base64.
var wgPublicKeyRe = regexp.MustCompile(`^[A-Za-z0-9+/]{43}=$`)

// validWireGuardKey reports whether k is a syntactically valid WireGuard key.
func validWireGuardKey(k string) bool { return wgPublicKeyRe.MatchString(k) }

// ────────────────────────────────────────────────────────────────────────────
// GPU classification
// ────────────────────────────────────────────────────────────────────────────

// isGPUNode decides whether a NodeProvision is a GPU node (labelling, taints,
// CDI setup, image pre-pull filtering). It delegates to awsprovision.IsGPUNode,
// the one rule shared with the AWS cloud-init, so an instance is configured for
// a GPU exactly when the controller treats the node as one.
func isGPUNode(np *mlv1alpha1.NodeProvision) bool {
	return awsprovision.IsGPUNode(np)
}

// ────────────────────────────────────────────────────────────────────────────
// Credential secret namespace
// ────────────────────────────────────────────────────────────────────────────

// credentialsNamespace resolves the namespace of spec.credentialsRef. The
// secret must live in the NodeProvision's own namespace: an empty namespace
// defaults to it and any other value is rejected, so a user who can create a
// NodeProvision cannot make the controller read (and copy) a Secret from a
// namespace they have no access to.
func credentialsNamespace(np *mlv1alpha1.NodeProvision) (string, error) {
	ns := np.Spec.CredentialsRef.Namespace
	if ns == "" || ns == np.Namespace {
		return np.Namespace, nil
	}
	return "", fmt.Errorf(
		"spec.credentialsRef.namespace %q is not allowed: the credentials Secret must be in the NodeProvision's namespace %q (leave namespace empty)",
		ns, np.Namespace)
}

// ────────────────────────────────────────────────────────────────────────────
// On-prem background job tracking
// ────────────────────────────────────────────────────────────────────────────

// onPremJob is the in-memory handle of one background on-prem bootstrap. It is
// keyed by "<namespace>/<name>" but also records the UID of the NodeProvision
// it was started for, so a CR deleted and recreated under the same name never
// consumes (or is blocked by) the previous incarnation's job.
type onPremJob struct {
	uid    types.UID
	ch     <-chan onPremJobResult
	cancel context.CancelFunc
	// result caches the outcome once it has been received from ch, so a
	// reconcile that fails after receiving it can be retried without losing it.
	result *onPremJobResult
}

func onPremKey(np *mlv1alpha1.NodeProvision) string { return np.Namespace + "/" + np.Name }

// loadOnPremJob returns the in-flight job for np. An entry that belongs to a
// different UID is stale: it is cancelled, removed and reported as absent.
func (r *NodeProvisionReconciler) loadOnPremJob(np *mlv1alpha1.NodeProvision) (*onPremJob, bool) {
	key := onPremKey(np)
	v, ok := r.onPremJobs.Load(key)
	if !ok {
		return nil, false
	}
	job, ok := v.(*onPremJob)
	if !ok || job == nil {
		r.onPremJobs.Delete(key)
		return nil, false
	}
	if job.uid != np.UID {
		job.cancel()
		r.onPremJobs.Delete(key)
		r.onPremProgress.Delete(key)
		return nil, false
	}
	return job, true
}

// dropOnPremJob cancels the job's context (a no-op once it has finished) and
// forgets it.
func (r *NodeProvisionReconciler) dropOnPremJob(np *mlv1alpha1.NodeProvision, job *onPremJob) {
	if job != nil && job.cancel != nil {
		job.cancel()
	}
	key := onPremKey(np)
	r.onPremJobs.Delete(key)
	r.onPremProgress.Delete(key)
}

// ────────────────────────────────────────────────────────────────────────────
// Deletion grace
// ────────────────────────────────────────────────────────────────────────────

const (
	// deletionGiveUpAfter bounds how long deletion may be blocked by a cleanup
	// step that keeps failing (unreachable VPN server, unusable AWS
	// credentials). Past it the step is abandoned loudly so the CR can go.
	deletionGiveUpAfter = 10 * time.Minute
	// onPremJobStopWait bounds how long deletion waits for a cancelled
	// bootstrap goroutine to report what it allocated.
	onPremJobStopWait = 2 * time.Minute
)

// deletionElapsed returns how long the NodeProvision has been terminating.
func deletionElapsed(np *mlv1alpha1.NodeProvision) time.Duration {
	if np.DeletionTimestamp == nil {
		return 0
	}
	return time.Since(np.DeletionTimestamp.Time)
}

// ────────────────────────────────────────────────────────────────────────────
// Node lookup
// ────────────────────────────────────────────────────────────────────────────

// nodeInternalAddress returns the node's InternalIP (or the first address).
func nodeInternalAddress(n *corev1.Node) string {
	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return a.Address
		}
	}
	if len(n.Status.Addresses) > 0 {
		return n.Status.Addresses[0].Address
	}
	return ""
}

// findRegisteredNode returns the cluster Node that already belongs to this
// NodeProvision, or nil. A node stamped with this NodeProvision's UID always
// matches; an unlabelled node matches only on the IP this controller
// previously recorded in status.ipAddress (a node owned by anything else is
// never adopted).
func (r *NodeProvisionReconciler) findRegisteredNode(ctx context.Context, np *mlv1alpha1.NodeProvision) *corev1.Node {
	if np.UID == "" {
		return nil
	}
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return nil
	}
	var byIP *corev1.Node
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Labels[nodeProvisionUIDLabel] == string(np.UID) {
			return n
		}
		if byIP == nil && np.Status.IPAddress != "" && n.Labels[nodeProvisionUIDLabel] == "" {
			for _, a := range n.Status.Addresses {
				if a.Address == np.Status.IPAddress {
					byIP = n
					break
				}
			}
		}
	}
	return byIP
}

// resolveNodeName returns status.nodeName, or — when it was never persisted —
// the name of the Node stamped with this NodeProvision's UID ("" when none).
func (r *NodeProvisionReconciler) resolveNodeName(ctx context.Context, np *mlv1alpha1.NodeProvision) (string, error) {
	if np.Status.NodeName != "" {
		return np.Status.NodeName, nil
	}
	if np.UID == "" {
		return "", nil
	}
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabels{nodeProvisionUIDLabel: string(np.UID)}); err != nil {
		return "", fmt.Errorf("listing nodes owned by NodeProvision %s: %w", np.Name, err)
	}
	if len(nodes.Items) > 0 {
		return nodes.Items[0].Name, nil
	}
	return "", nil
}
