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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	infrav1 "dcn.ssu.ac.kr/infra/api/v1"
	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

const (
	// annotationProvisionedVPNMode records the VPN mode the node was actually
	// provisioned with ("vpn" or "novpn"). spec.disableVPN is mutable, so every
	// decision that concerns something already configured on the node (SSH
	// address, WireGuard teardown, peer removal) must use this value when it is
	// present and fall back to the spec only when it is absent.
	annotationProvisionedVPNMode = "infra.dcn.ssu.ac.kr/provisioned-vpn-mode"

	// annotationWorkerFinalizePending holds the worker's node IP between the
	// moment the join succeeded and the moment the worker's Ready status and the
	// control-plane's usedIPAddresses entry have both been recorded, so a failure
	// in between is retried instead of being lost. The value "none" means there is
	// no IP to record.
	annotationWorkerFinalizePending = "infra.dcn.ssu.ac.kr/worker-finalize-pending"

	// prepullNamespace is the namespace of the image pre-pull DaemonSets on the
	// remote cluster; the registry secret they read must live there.
	prepullNamespace = "default"

	vpnModeVPN   = "vpn"
	vpnModeNoVPN = "novpn"

	// conditionVPNModeMismatch / conditionVPNModeChangeIgnored are surfaced when
	// a node's VPN mode disagrees with its control-plane or with what it was
	// provisioned with.
	conditionVPNModeMismatch      = "VPNModeMismatch"
	conditionVPNModeChangeIgnored = "VPNModeChangeIgnored"

	// maxConditionMessage bounds condition messages (SSH output can be huge).
	maxConditionMessage = 4096

	// maxConditions is a defensive cap on the (upserted by type) condition list.
	maxConditions = 40
)

// effectiveVPNDisabled reports whether the VPN is disabled for a node: the
// recorded provisioned mode wins, the spec is only used when nothing has been
// provisioned yet.
func effectiveVPNDisabled(c *infrav1.RemoteCluster) bool {
	switch c.Annotations[annotationProvisionedVPNMode] {
	case vpnModeNoVPN:
		return true
	case vpnModeVPN:
		return false
	}
	return c.Spec.DisableVPN
}

func vpnModeString(disabled bool) string {
	if disabled {
		return vpnModeNoVPN
	}
	return vpnModeVPN
}

// withEffectiveVPNMode returns a deep copy of c whose spec.disableVPN is the
// effective (recorded, else spec) mode. Used for code that reads
// Spec.DisableVPN directly (pkg/kubeadm) and for delete-time cleanup. The copy
// must never be written back to the API server.
func withEffectiveVPNMode(c *infrav1.RemoteCluster) *infrav1.RemoteCluster {
	cp := c.DeepCopy()
	cp.Spec.DisableVPN = effectiveVPNDisabled(c)
	return cp
}

// vpnCredRefNamespace returns the namespace of a VPN credentials reference,
// defaulting to the RemoteCluster's own namespace like getVPNSecret does.
func vpnCredRefNamespace(cluster *infrav1.RemoteCluster, ref infrav1.VPNSSHCredentialsRef) string {
	if ref.NameSpace != "" {
		return ref.NameSpace
	}
	return cluster.Namespace
}

// shQuote quotes s as a single POSIX shell word.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// yamlQuote renders s as a YAML scalar. A JSON string is a valid YAML
// double-quoted scalar, so newlines/quotes/colons in s cannot break out.
func yamlQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(b)
}

// secretApplyCmd returns a shell command that applies a Secret on the remote
// cluster via `kubectl apply`. Names, namespace, type and keys are emitted as
// quoted YAML scalars; values are base64.
func secretApplyCmd(name, namespace string, typ corev1.SecretType, data map[string][]byte) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: v1\nkind: Secret\nmetadata:\n  name: %s\n  namespace: %s\n", yamlQuote(name), yamlQuote(namespace))
	if typ != "" {
		fmt.Fprintf(&b, "type: %s\n", yamlQuote(string(typ)))
	}
	b.WriteString("data:\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "  %s: %s\n", yamlQuote(k), base64.StdEncoding.EncodeToString(data[k]))
	}
	return fmt.Sprintf("cat <<'EOF' | kubectl apply -f -\n%s\nEOF", b.String())
}

// jsonString renders s as a JSON string literal (for hand-built JSON).
func jsonString(s string) string { return yamlQuote(s) }

var k8sNodeNameRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

// validNodeName reports whether s can be a Kubernetes node name.
func validNodeName(s string) bool {
	return len(s) > 0 && len(s) <= 253 && k8sNodeNameRe.MatchString(s)
}

// parseNodeNameByIP finds the node whose INTERNAL-IP is ip in the output of
// `kubectl get nodes -o wide --no-headers`. Returns "" when there is no match.
func parseNodeNameByIP(out, ip string) string {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return ""
	}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		// NAME STATUS ROLES AGE VERSION INTERNAL-IP EXTERNAL-IP ...
		if len(f) >= 6 && f[5] == ip && validNodeName(f[0]) {
			return f[0]
		}
	}
	return ""
}

// truncateMessage bounds a condition message. It cuts on a rune boundary so a
// multi-byte character is never split into invalid UTF-8 (which the API server
// would reject).
func truncateMessage(s string) string {
	if len(s) <= maxConditionMessage {
		return s
	}
	cut := maxConditionMessage
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// upsertCondition sets a condition by Type (never appends a duplicate) and
// keeps the list bounded. LastTransitionTime only moves when Status changes.
func upsertCondition(cluster *infrav1.RemoteCluster, cond metav1.Condition) {
	cond.Message = truncateMessage(cond.Message)
	if cond.Reason == "" {
		cond.Reason = cond.Type
	}
	cond.ObservedGeneration = cluster.Generation
	cluster.Status.Conditions = dedupeConditions(cluster.Status.Conditions)
	apimeta.SetStatusCondition(&cluster.Status.Conditions, cond)
	if n := len(cluster.Status.Conditions); n > maxConditions {
		cluster.Status.Conditions = cluster.Status.Conditions[n-maxConditions:]
	}
}

// dedupeConditions keeps only the last entry per Type, preserving order. It
// compacts the history that older controller versions appended on every step.
func dedupeConditions(in []metav1.Condition) []metav1.Condition {
	if len(in) < 2 {
		return in
	}
	last := make(map[string]int, len(in))
	for i, c := range in {
		last[c.Type] = i
	}
	if len(last) == len(in) {
		return in
	}
	out := make([]metav1.Condition, 0, len(last))
	for i, c := range in {
		if last[c.Type] == i {
			out = append(out, c)
		}
	}
	return out
}

// mutateStatus re-fetches the RemoteCluster, applies fn to the fresh copy and
// writes its status, retrying on conflicts. It never touches spec/metadata and
// leaves the phase alone unless fn changes it.
func (r *RemoteClusterReconciler) mutateStatus(ctx context.Context, cluster *infrav1.RemoteCluster, fn func(fresh *infrav1.RemoteCluster) (changed bool)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &infrav1.RemoteCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
			return err
		}
		if !fn(fresh) {
			return nil
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		// Keep the caller's copy current (resourceVersion included).
		fresh.DeepCopyInto(cluster)
		return nil
	})
}

// setCondition upserts one condition without changing the phase or the retry
// counter. It is a no-op when the condition already has the same content.
func (r *RemoteClusterReconciler) setCondition(ctx context.Context, cluster *infrav1.RemoteCluster, condType string, status metav1.ConditionStatus, reason, message string) {
	err := r.mutateStatus(ctx, cluster, func(fresh *infrav1.RemoteCluster) bool {
		message := truncateMessage(message)
		if ex := apimeta.FindStatusCondition(fresh.Status.Conditions, condType); ex != nil &&
			ex.Status == status && ex.Reason == reason && ex.Message == message {
			return false
		}
		upsertCondition(fresh, metav1.Condition{Type: condType, Status: status, Reason: reason, Message: message})
		return true
	})
	if err != nil {
		logf.FromContext(ctx).Error(err, "failed to persist condition (non-fatal)", "type", condType)
	}
}

// clearConditions removes the given condition types; it only writes when at
// least one of them was present.
func (r *RemoteClusterReconciler) clearConditions(ctx context.Context, cluster *infrav1.RemoteCluster, types ...string) {
	present := false
	for _, t := range types {
		if apimeta.FindStatusCondition(cluster.Status.Conditions, t) != nil {
			present = true
			break
		}
	}
	if !present {
		return
	}
	err := r.mutateStatus(ctx, cluster, func(fresh *infrav1.RemoteCluster) bool {
		changed := false
		for _, t := range types {
			if apimeta.RemoveStatusCondition(&fresh.Status.Conditions, t) {
				changed = true
			}
		}
		return changed
	})
	if err != nil {
		logf.FromContext(ctx).Error(err, "failed to clear conditions (non-fatal)", "types", types)
		return
	}
	for _, t := range types {
		apimeta.RemoveStatusCondition(&cluster.Status.Conditions, t)
	}
}

// controlPlaneJob is one in-flight control-plane init goroutine.
type controlPlaneJob struct {
	ch  <-chan controlPlaneJobResult
	uid string
	// cancel aborts the goroutine's work (it closes the SSH connection it owns).
	cancel func()
	// cancelled is set by cancel so late progress callbacks are dropped.
	cancelled atomic.Bool
}

// controlPlaneCancelAckTimeout bounds how long cancelControlPlaneJob waits for
// the init goroutine to notice its closed connection and return. A variable so
// tests can shorten it.
var controlPlaneCancelAckTimeout = 10 * time.Second

// cancelControlPlaneJob aborts and forgets the in-flight init for key, if any,
// and drops its recorded progress. Closing the SSH connection does not by itself
// stop the remote `kubeadm init`, and the goroutine may still be touching the
// host, so it waits (bounded) until the goroutine has returned. It reports
// whether the goroutine acknowledged the cancellation in time (true when there
// was nothing to cancel).
func (r *RemoteClusterReconciler) cancelControlPlaneJob(key string) bool {
	acked := true
	if v, ok := r.controlPlaneJobs.LoadAndDelete(key); ok {
		if job, ok := v.(*controlPlaneJob); ok {
			job.cancelled.Store(true)
			if job.cancel != nil {
				job.cancel()
			}
			if job.ch != nil {
				timer := time.NewTimer(controlPlaneCancelAckTimeout)
				select {
				case <-job.ch:
				case <-timer.C:
					acked = false
					logf.Log.WithName("remotecluster").Info(
						"control-plane init goroutine did not stop in time after cancel", "job", key,
						"waited", controlPlaneCancelAckTimeout.String())
				}
				timer.Stop()
			}
		}
	}
	r.controlPlaneProgress.Delete(key)
	return acked
}

// secretReferenced reports whether any RemoteCluster other than self (and not
// being deleted) still references the secret ns/name according to refs.
func (r *RemoteClusterReconciler) secretReferenced(
	ctx context.Context,
	self *infrav1.RemoteCluster,
	ns, name string,
	refs func(other *infrav1.RemoteCluster) (refNS, refName string),
) (bool, error) {
	var list infrav1.RemoteClusterList
	if err := r.List(ctx, &list); err != nil {
		return true, fmt.Errorf("listing RemoteClusters: %w", err)
	}
	for i := range list.Items {
		o := &list.Items[i]
		if o.Namespace == self.Namespace && o.Name == self.Name {
			continue
		}
		if !o.DeletionTimestamp.IsZero() {
			continue
		}
		if rns, rname := refs(o); rname == name && rns == ns {
			return true, nil
		}
	}
	return false, nil
}

// authSecretName returns the name of the user-managed SSH secret of a node.
func authSecretName(c *infrav1.RemoteCluster) string {
	if c.Spec.Auth.SSHPrivateKeySecretRef != nil {
		return c.Spec.Auth.SSHPrivateKeySecretRef.Name
	}
	if c.Spec.Auth.PasswordSecretRef != nil {
		return c.Spec.Auth.PasswordSecretRef.Name
	}
	return ""
}

// softFailInterval is how long to wait before retrying a step that failed on a
// cluster that is already up (post-Ready / post-join).
const softFailInterval = time.Minute

// softFail records a failure of a step that runs on an already working cluster
// (control-plane past kubeadm init, or a joined worker) as a condition and asks
// for a delayed retry. Unlike fail() it neither demotes the phase to Failed nor
// consumes the provisioning retry budget, so a transient Porch/SSH error can
// never take a working cluster out of Ready or leave it terminally Failed.
func (r *RemoteClusterReconciler) softFail(ctx context.Context, cluster *infrav1.RemoteCluster, reason string, cause error) (ctrl.Result, error) {
	logf.FromContext(ctx).Error(cause, "step failed on a running cluster — will retry",
		"cluster", cluster.Name, "reason", reason, "retryIn", softFailInterval.String())
	r.setCondition(ctx, cluster, reason, metav1.ConditionFalse, reason, fmt.Sprintf("%v (will retry)", cause))
	return ctrl.Result{RequeueAfter: softFailInterval}, nil
}

// failNoCount marks the RemoteCluster Failed with a clear message and retries
// after a minute without consuming the provisioning retry budget. It is meant
// for conditions the user must fix in the spec (e.g. a VPN mode conflict), where
// burning the 5 attempts would turn a fixable state into a terminal one.
func (r *RemoteClusterReconciler) failNoCount(ctx context.Context, cluster *infrav1.RemoteCluster, reason string, cause error) (ctrl.Result, error) {
	logf.FromContext(ctx).Error(cause, "RemoteCluster cannot proceed — will re-check", "cluster", cluster.Name, "reason", reason)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &infrav1.RemoteCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
			return err
		}
		return r.setStatus(ctx, fresh, phaseFailed, reason, cause.Error(), true)
	})
	if err != nil {
		logf.FromContext(ctx).Error(err, "failed to persist status", "reason", reason)
	}
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

// recoverControlPlane moves a control-plane that has already finished kubeadm
// init (Status.JoinCommand set) back to Ready. Without it such a cluster, once
// demoted to Failed/Provisioning by a later step, would loop through
// reconcileProvisioning forever: the init step returns early on JoinCommand and
// nothing would ever flip the phase back, so PackageVariants, token refresh and
// sync would stop. The Ready branch then re-runs its idempotent steps.
func (r *RemoteClusterReconciler) recoverControlPlane(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) {
	logf.FromContext(ctx).Info("Control plane already initialised — returning to Ready",
		"cluster", cluster.Name, "previousPhase", cluster.Status.Phase)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return err
		}
		if cluster.Status.JoinCommand == "" {
			return nil
		}
		return r.setStatus(ctx, cluster, phaseReady, "Recovered",
			"Control plane already initialised; resuming steady-state reconciliation", false)
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("returning control plane to Ready: %w", err)
	}
	// Status-only changes are filtered by GenerationChangedPredicate: requeue explicitly.
	return ctrl.Result{Requeue: true}, nil
}

// recordProvisionedVPNMode stamps annotationProvisionedVPNMode (once) with the
// mode the node is about to be provisioned with. Call it only after the SSH
// connection to the host succeeded and immediately before the first remote step
// that changes the host: a node that was never reached must stay free to have
// its spec.disableVPN corrected.
func (r *RemoteClusterReconciler) recordProvisionedVPNMode(ctx context.Context, cluster *infrav1.RemoteCluster) error {
	if cluster.Annotations[annotationProvisionedVPNMode] != "" {
		return nil
	}
	mode := vpnModeString(cluster.Spec.DisableVPN)
	if err := r.patchAnnotation(ctx, cluster, annotationProvisionedVPNMode, mode); err != nil {
		return fmt.Errorf("recording provisioned VPN mode: %w", err)
	}
	ensureAnnotations(cluster)[annotationProvisionedVPNMode] = mode
	return nil
}

// provisioningUntouched reports whether, as far as the RemoteCluster records,
// no provisioning phase has completed on the host yet: the node is not Ready,
// no kubeadm init/join result and no phase progress is stored, and no
// control-plane init goroutine is running for it.
func (r *RemoteClusterReconciler) provisioningUntouched(cluster *infrav1.RemoteCluster) bool {
	if cluster.Status.Phase == phaseReady || cluster.Status.JoinCommand != "" {
		return false
	}
	for _, k := range []string{
		annotationLastCompletedPhaseCP, annotationLastCompletedPhaseWorker,
		annotationWorkerJoined, annotationWorkerFinalizePending, annotationCPInitComplete,
	} {
		if cluster.Annotations[k] != "" {
			return false
		}
	}
	if _, running := r.controlPlaneJobs.Load(cluster.Namespace + "/" + cluster.Name); running {
		return false
	}
	return true
}

// releaseVPNModeIfUntouched removes annotationProvisionedVPNMode when the
// provisioning attempt failed before any phase completed (per a fresh read of
// the object), so the user can still correct spec.disableVPN. The phase-progress
// annotations are left alone. It is best effort: a failure only keeps the record.
func (r *RemoteClusterReconciler) releaseVPNModeIfUntouched(ctx context.Context, cluster *infrav1.RemoteCluster) {
	fresh := &infrav1.RemoteCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
		return
	}
	if fresh.Annotations[annotationProvisionedVPNMode] == "" || !r.provisioningUntouched(fresh) {
		return
	}
	if err := r.patchAnnotations(ctx, cluster, map[string]string{annotationProvisionedVPNMode: ""}); err != nil {
		logf.FromContext(ctx).Error(err, "releasing provisioned VPN mode after an early failure (non-fatal)")
		return
	}
	delete(cluster.Annotations, annotationProvisionedVPNMode)
	logf.FromContext(ctx).Info("Provisioning failed before any phase completed — VPN mode record released",
		"cluster", cluster.Name)
}

// finalizeWorkerJoin records what must happen after a successful join: the
// worker's Ready status, the control-plane's usedIPAddresses entry and the node
// labels the image pre-pull DaemonSets select on. It is idempotent (every step is
// safe to repeat) and driven by annotationWorkerFinalizePending, which is only
// cleared once every step succeeded, so it can simply be retried after any
// failure.
func (r *RemoteClusterReconciler) finalizeWorkerJoin(
	ctx context.Context,
	cluster, clusterParent *infrav1.RemoteCluster,
	sshClientCP *sshhelper.Client,
) error {
	log := logf.FromContext(ctx).WithValues("cluster", cluster.Name)

	if cluster.Status.Phase != phaseReady {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
				return err
			}
			return r.setStatus(ctx, cluster, phaseReady, "WorkerJoined", "Worker node joined to cluster", false)
		}); err != nil {
			return fmt.Errorf("updating worker status to Ready: %w", err)
		}
	}

	pending := cluster.Annotations[annotationWorkerFinalizePending]
	if pending != "" && pending != "none" {
		if _, err := r.handleCreateUpdateNodeProvisionConfig(ctx, cluster, clusterParent, sshClientCP, pending, "update"); err != nil {
			return fmt.Errorf("updating NodeProvisionNetConfig with used IP: %w", err)
		}
	}

	// Label the node so the prepull DaemonSets (already deployed on the
	// control-plane) schedule on it. Part of the retried finalization: without the
	// labels the pre-pull pods never start on this worker.
	if err := r.labelWorkerNode(ctx, cluster, sshClientCP); err != nil {
		return fmt.Errorf("labelling worker node: %w", err)
	}

	if pending != "" {
		if err := r.patchAnnotations(ctx, cluster, map[string]string{annotationWorkerFinalizePending: ""}); err != nil {
			return fmt.Errorf("clearing worker finalize marker: %w", err)
		}
		delete(cluster.Annotations, annotationWorkerFinalizePending)
	}
	if err := r.ensureLocalNodeProvisionNetConfig(ctx, clusterParent, clusterParent); err != nil {
		log.Error(err, "syncing local NodeProvisionNetConfig (non-fatal)")
	}
	r.clearConditions(ctx, cluster, "WorkerFinalizeFailed")
	return nil
}

// labelWorkerNode sets infra.dcn.ssu.ac.kr/worker and
// infra.dcn.ssu.ac.kr/hardware-type on the worker's Kubernetes node (kubectl
// --overwrite, so it is idempotent). The node is identified by the worker's OS
// hostname, or by its IP on the control-plane when the worker is unreachable;
// it is never guessed from spec.clusterName, which is shared by every node.
func (r *RemoteClusterReconciler) labelWorkerNode(ctx context.Context, cluster *infrav1.RemoteCluster, sshClientCP *sshhelper.Client) error {
	log := logf.FromContext(ctx).WithValues("cluster", cluster.Name)

	hwLabel := "cpu"
	if strings.EqualFold(cluster.Spec.NodeInfo.HardwareType, "gpu") {
		hwLabel = "gpu"
	}

	nodeName := ""
	if workerSSH, err := r.getSSHClient(ctx, cluster); err == nil {
		if out, hErr := sshhelper.RunStdoutCtx(ctx, workerSSH, "hostname"); hErr == nil {
			if h := strings.ToLower(strings.TrimSpace(out)); validNodeName(h) {
				nodeName = h
			}
		}
		_ = workerSSH.Conn.Close()
	}
	if nodeName == "" {
		nodeName = r.nodeNameByIP(ctx, sshClientCP, cluster)
	}
	if nodeName == "" {
		return fmt.Errorf("could not resolve the worker's node name (worker unreachable and its IP is not a known node)")
	}

	labelCmd := fmt.Sprintf(
		"kubectl label node %s infra.dcn.ssu.ac.kr/worker=true infra.dcn.ssu.ac.kr/hardware-type=%s --overwrite",
		shQuote(nodeName), shQuote(hwLabel),
	)
	if out, err := sshhelper.RunCtx(ctx, sshClientCP, labelCmd); err != nil {
		return fmt.Errorf("kubectl label node %s: %w: %s", nodeName, err, strings.TrimSpace(out))
	}
	log.Info("Labeled worker node for DaemonSet targeting", "node", nodeName, "hardwareType", hwLabel)
	return nil
}

// retryWorkerFinalize re-runs finalizeWorkerJoin for a worker that already
// joined but whose finalization did not complete: its phase may be Ready with a
// pending marker, or Provisioning/Failed after a later step failed. Only the
// control-plane connection is needed; the worker is never re-provisioned.
func (r *RemoteClusterReconciler) retryWorkerFinalize(ctx context.Context, cluster *infrav1.RemoteCluster) (ctrl.Result, error) {
	clusterParent, err := r.findControlPlane(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing RemoteClusters: %w", err)
	}
	if clusterParent == nil || clusterParent.Status.Phase != phaseReady || clusterParent.Status.JoinCommand == "" {
		return ctrl.Result{RequeueAfter: controlPlaneRetryInterval}, nil
	}
	sshCtx, cancel := context.WithTimeout(ctx, sshOperationTimeout)
	defer cancel()
	sshClientCP, err := r.getSSHClient(sshCtx, clusterParent)
	if err != nil {
		return r.softFail(ctx, cluster, "WorkerFinalizeFailed", fmt.Errorf("connecting to control-plane via SSH: %w", err))
	}
	defer func() { _ = sshClientCP.Conn.Close() }()
	if err := r.finalizeWorkerJoin(sshCtx, cluster, clusterParent, sshClientCP); err != nil {
		return r.softFail(ctx, cluster, "WorkerFinalizeFailed", err)
	}
	return ctrl.Result{}, nil
}

// nodeNameByIP resolves the Kubernetes node name of a worker from the control
// plane by matching its INTERNAL-IP. Returns "" when it cannot be resolved.
func (r *RemoteClusterReconciler) nodeNameByIP(ctx context.Context, cpClient *sshhelper.Client, cluster *infrav1.RemoteCluster) string {
	// Stdout only: stderr noise (e.g. "sudo: unable to resolve host") must not
	// end up in the parsed table.
	out, err := sshhelper.RunStdoutCtx(ctx, cpClient, "kubectl get nodes -o wide --no-headers 2>/dev/null")
	if err != nil {
		return ""
	}
	var candidates []string
	if !effectiveVPNDisabled(cluster) && cluster.Spec.VPNConfig.IP != "" {
		candidates = append(candidates, cluster.Spec.VPNConfig.IP)
	}
	candidates = append(candidates, cluster.Spec.Host)
	for _, ip := range candidates {
		if n := parseNodeNameByIP(out, ip); n != "" {
			return n
		}
	}
	return ""
}

// tokenRequeueAfter returns how long until the kubeadm bootstrap token of a
// control-plane is due for renewal, for use as the RequeueAfter of a Ready
// reconcile: status/annotation-only writes do not wake the controller
// (GenerationChangedPredicate), so without an explicit requeue the renewal would
// never run. It is never zero or negative: an overdue or never-refreshed token
// (its refresh just failed) is retried after softFailInterval.
func tokenRequeueAfter(cluster *infrav1.RemoteCluster) time.Duration {
	ts, ok := cluster.Annotations[annotationJoinTokenRefreshedAt]
	if !ok {
		return softFailInterval
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return softFailInterval
	}
	remaining := tokenRefreshInterval - time.Since(t)
	if remaining < softFailInterval {
		return softFailInterval
	}
	if remaining > tokenRefreshInterval {
		return tokenRefreshInterval // clock skew: never wait longer than one interval
	}
	return remaining
}

// maxPackageVariantNameLen keeps PackageVariant names safe as label values too.
const maxPackageVariantNameLen = 63

// perClusterPackageVariantName returns "<base>-<clusterName>" for a variant whose
// base name is taken by another cluster. A clusterName that is not a valid name
// fragment, or a result over the length limit, is sanitised/truncated and made
// unique with a short stable hash of the raw clusterName.
func perClusterPackageVariantName(base, clusterName string) string {
	sanitized := strings.Trim(strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		}
		return '-'
	}, clusterName), "-")
	if sanitized == clusterName && len(base)+1+len(sanitized) <= maxPackageVariantNameLen {
		return base + "-" + sanitized
	}

	sum := sha256.Sum256([]byte(clusterName))
	suffix := fmt.Sprintf("-%x", sum[:4])
	if room := maxPackageVariantNameLen - len(base) - len(suffix) - 1; room > 0 {
		if len(sanitized) > room {
			sanitized = strings.TrimRight(sanitized[:room], "-")
		}
		if sanitized != "" {
			return base + "-" + sanitized + suffix
		}
	}
	if max := maxPackageVariantNameLen - len(suffix); len(base) > max {
		base = strings.TrimRight(base[:max], "-")
	}
	return base + suffix
}
