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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
)

// ────────────────────────────────────────────────────────────────────────────
// Failed → retry
// ────────────────────────────────────────────────────────────────────────────

// reconcileFailed handles a NodeProvision in the Failed phase: once the retry
// budget is not exhausted and the requeueFailed backoff (measured from the
// failure time) has elapsed, it tears down whatever the failed attempt left
// behind and resets the status so provisioning starts over.
//
// The reset must never leave a live resource that the next attempt cannot
// see: an AWS instance is terminated (and its Node removed) before InstanceID
// is cleared, and the VPN peer is released before VpnIP is cleared. If any of
// those steps fails the status is left untouched and the step is retried.
func (r *NodeProvisionReconciler) reconcileFailed(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Terminal: retry limit already reached — do not auto-retry. Patching
	// .status.provisionRetryCount below the limit re-enables retries. What the
	// failed attempts left running is released once (see teardownTerminal).
	if np.Status.ProvisionRetryCount >= maxProvisionRetries {
		log.Info("NodeProvision in terminal Failed state — retry limit reached, manual intervention required",
			"attempts", np.Status.ProvisionRetryCount,
			"maxRetries", maxProvisionRetries,
			"message", np.Status.Message)
		return r.teardownTerminal(ctx, np)
	}

	// Honour the backoff. Status changes now wake the reconciler (so a manual
	// reset of provisionRetryCount is noticed), which means this handler also
	// runs immediately after failNodeProvision wrote the Failed phase.
	if np.Status.LastUpdated != nil {
		if remaining := requeueFailed - time.Since(np.Status.LastUpdated.Time); remaining > 0 {
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
	}

	// A CR can fail while its bootstrap goroutine is still registered. Stop it
	// (recording what it allocated) before the reset: the reset lands in phase
	// "" and only Bootstrapping consumes a job, so a leftover job would block
	// re-provisioning forever.
	if res, wait, err := r.stopJobForFailed(ctx, np); wait || err != nil {
		return res, err
	}

	// On-prem: if the node the failed attempt bootstrapped is already registered
	// in the cluster, re-running the SSH bootstrap would only disturb a working
	// node (and releasing its VPN peer would cut it off). Resume at Joining.
	var resumeNode *corev1.Node
	if np.Spec.Provider == mlv1alpha1.CloudProviderOnPrem {
		resumeNode = r.findRegisteredNode(ctx, np)
	}

	if resumeNode == nil {
		if np.Spec.Provider == mlv1alpha1.CloudProviderAWS {
			if res, wait := r.teardownAWSForRetry(ctx, np); wait {
				return res, nil
			}
		}
		if np.Spec.Provider == mlv1alpha1.CloudProviderGCP {
			if res, wait := r.teardownGCPForRetry(ctx, np); wait {
				return res, nil
			}
		}
		if err := r.cleanupVPNPeer(ctx, np); err != nil {
			// Do not clear VpnIP while the peer may still exist: it is the
			// handle cleanupVPNPeer uses to find and release it.
			log.Error(err, "releasing VPN peer before retry failed — will retry the release before re-provisioning")
			r.noteRetryBlocked(ctx, np, err)
			return ctrl.Result{RequeueAfter: requeueFailed}, nil
		}
	}

	// Re-fetch to get the latest ResourceVersion before writing — a parallel
	// reconcile may have already reset the phase, in which case we do nothing.
	fresh := &mlv1alpha1.NodeProvision{}
	if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if fresh.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		return ctrl.Result{}, nil
	}
	if fresh.Status.ProvisionRetryCount >= maxProvisionRetries {
		log.Info("NodeProvision reached terminal state on re-fetch — not resetting",
			"attempts", fresh.Status.ProvisionRetryCount)
		return ctrl.Result{}, nil
	}

	now := metav1.Now()
	fresh.Status.LastUpdated = &now
	if resumeNode != nil {
		applyAdoptedNode(fresh, resumeNode)
		fresh.Status.Phase = mlv1alpha1.NodeProvisionPhaseJoining
		fresh.Status.Message = fmt.Sprintf(
			"Retrying after failure (attempt %d/%d): node %s is already registered, resuming at join",
			fresh.Status.ProvisionRetryCount, maxProvisionRetries, resumeNode.Name)
		log.Info("Retrying NodeProvision after failure — node already registered, skipping bootstrap",
			"node", resumeNode.Name, "attempt", fresh.Status.ProvisionRetryCount)
	} else {
		clearProvisionedIdentity(&fresh.Status)
		fresh.Status.Phase = ""
		fresh.Status.Message = fmt.Sprintf("Retrying after failure (attempt %d/%d)", fresh.Status.ProvisionRetryCount, maxProvisionRetries)
		log.Info("Retrying NodeProvision after failure — released stale resources and reset phase",
			"attempt", fresh.Status.ProvisionRetryCount,
			"maxRetries", maxProvisionRetries)
	}
	if err := r.Status().Update(ctx, fresh); err != nil {
		if apierrors.IsConflict(err) {
			// Someone else wrote the CR in between; redo the (idempotent)
			// reset against the current version.
			log.Info("Phase reset conflicted with another write, retrying", "err", err.Error())
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("resetting failed NodeProvision for retry: %w", err)
	}
	if err := r.clearPrepullRetryCount(ctx, fresh); err != nil {
		log.Error(err, "failed to reset pre-pull retry count (continuing)")
	}
	if resumeNode != nil {
		return ctrl.Result{RequeueAfter: requeueJoining}, nil
	}
	return ctrl.Result{RequeueAfter: requeueFailed}, nil
}

// terminalTeardownMarker in Status.Message records that the resources of a
// terminally failed NodeProvision were released, which makes the teardown
// one-time: failNodeProvisionWith rewrites the message on the next failure, so
// a re-armed (provisionRetryCount reset) resource that fails again is torn down
// again.
const terminalTeardownMarker = "[resources released]"

// teardownTerminal releases what a terminally failed NodeProvision still holds
// — the EC2 instance, the joined Node and the VPN peer — once, and records it
// in Status.Message. Every step is idempotent, so it is safe to rerun after an
// interruption, and it re-checks the retry count before persisting so it never
// fights a user who reset provisionRetryCount meanwhile (the normal retry path
// then finds a clean status).
//
// An on-prem node that has already joined the cluster is left alone: it is a
// working machine, and releasing its VPN peer would cut it off.
func (r *NodeProvisionReconciler) teardownTerminal(ctx context.Context, np *mlv1alpha1.NodeProvision) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if strings.Contains(np.Status.Message, terminalTeardownMarker) {
		return ctrl.Result{}, nil
	}
	if res, wait, err := r.stopJobForFailed(ctx, np); wait || err != nil {
		return res, err
	}

	var released []string
	switch np.Spec.Provider {
	case mlv1alpha1.CloudProviderAWS:
		instance := np.Status.InstanceID
		if res, wait := r.releaseAWSInstance(ctx, np, true); wait {
			return res, nil
		}
		if instance != "" {
			released = append(released, "instance "+instance+" terminated")
		}
		if np.Status.NodeName != "" {
			released = append(released, "node "+np.Status.NodeName+" removed")
		}
	case mlv1alpha1.CloudProviderGCP:
		var res ctrl.Result
		var wait bool
		if released, res, wait = r.teardownTerminalGCP(ctx, np); wait {
			return res, nil
		}
	case mlv1alpha1.CloudProviderOnPrem:
		if node := r.findRegisteredNode(ctx, np); node != nil {
			log.Info("Terminal failure but the node is registered in the cluster — leaving it and its VPN peer in place",
				"node", node.Name)
			return ctrl.Result{}, nil
		}
	}

	hadPeer := np.Status.VpnIP != ""
	if err := r.cleanupVPNPeer(ctx, np); err != nil {
		log.Error(err, "releasing VPN peer of terminally failed NodeProvision — will retry")
		r.noteRetryBlocked(ctx, np, err)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	if hadPeer {
		released = append(released, "VPN peer released")
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return err
		}
		if fresh.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed ||
			fresh.Status.ProvisionRetryCount < maxProvisionRetries {
			return nil // re-armed while tearing down: the retry path takes over
		}
		clearProvisionedIdentity(&fresh.Status)
		summary := terminalTeardownMarker
		if len(released) > 0 {
			summary += " " + strings.Join(released, ", ")
		}
		fresh.Status.Message = truncateMessage(fresh.Status.Message, statusMessageMaxLen-len(summary)-3) + " — " + summary
		return r.Status().Update(ctx, fresh)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("recording terminal teardown: %w", err)
	}
	log.Info("Released resources of terminally failed NodeProvision", "released", released)
	return ctrl.Result{}, nil
}

// clearProvisionedIdentity forgets everything that identified the resources of
// a torn-down attempt, so a later attempt never reuses a stale InstanceID/VPN IP.
func clearProvisionedIdentity(st *mlv1alpha1.NodeProvisionStatus) {
	st.VpnIP = ""
	st.IPAddress = ""
	st.InstanceID = ""
	st.PublicIP = ""
	st.PrivateIP = ""
	st.NodeName = ""
}

// stopJobForFailed cancels and forgets np's on-prem bootstrap job, if any,
// before a Failed CR is reset or torn down. wait is true when the goroutine has
// not reported yet and the caller must requeue.
func (r *NodeProvisionReconciler) stopJobForFailed(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, wait bool, err error) {
	if np.Spec.Provider != mlv1alpha1.CloudProviderOnPrem {
		return ctrl.Result{}, false, nil
	}
	// Give a cancelled goroutine a bounded time to report what it allocated,
	// measured from the failure; afterwards it is forgotten.
	allowPending := np.Status.LastUpdated != nil && time.Since(np.Status.LastUpdated.Time) < onPremJobStopWait
	pending, err := r.stopOnPremJob(ctx, np, failedJobStopBlock, allowPending)
	if err != nil {
		return ctrl.Result{}, true, fmt.Errorf("stopping on-prem bootstrap of failed NodeProvision: %w", err)
	}
	if pending {
		return ctrl.Result{RequeueAfter: 3 * time.Second}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// failedJobStopBlock is how long stopJobForFailed waits for a just-cancelled
// bootstrap goroutine to report (a variable so tests need not wait it out).
var failedJobStopBlock = 5 * time.Second

const (
	// retryBlockedPrefix starts a Status.Message that reports a cleanup
	// failure blocking the retry; lastFailureSep introduces the original failure
	// message that is preserved after it.
	retryBlockedPrefix = "Retry blocked: could not release the VPN peer: "
	lastFailureSep     = " | last failure: "
)

// retryBlockedMessage builds the status message for a blocked retry. It is a
// pure function of (current message, err) and idempotent — feeding its own
// output back with the same error returns it unchanged — so an unchanged
// error never causes another status write.
func retryBlockedMessage(current string, err error) string {
	head := retryBlockedPrefix + truncateMessage(redactSecrets(err.Error()), 400)
	orig := current
	if strings.HasPrefix(current, retryBlockedPrefix) {
		orig = ""
		if i := strings.Index(current, lastFailureSep); i >= 0 {
			orig = current[i+len(lastFailureSep):]
		}
	}
	msg := head
	if orig != "" {
		msg += lastFailureSep + truncateMessage(orig, 300)
	}
	return sanitizeStatusMessage(msg)
}

// noteRetryBlocked surfaces a cleanup failure that blocks a retry in
// Status.Message (bounded and redacted), once per distinct message. LastUpdated
// is left alone: it anchors the retry backoff.
func (r *NodeProvisionReconciler) noteRetryBlocked(ctx context.Context, np *mlv1alpha1.NodeProvision, cause error) {
	log := logf.FromContext(ctx)
	fresh := &mlv1alpha1.NodeProvision{}
	if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
		return
	}
	if fresh.Status.Phase != mlv1alpha1.NodeProvisionPhaseFailed {
		return
	}
	msg := retryBlockedMessage(fresh.Status.Message, cause)
	if fresh.Status.Message == msg {
		return
	}
	fresh.Status.Message = msg
	if err := r.Status().Update(ctx, fresh); err != nil {
		log.Error(err, "recording blocked-retry reason in status (non-fatal)")
	}
}

// applyAdoptedNode points np's status at an already-registered Node so the
// join step can take over instead of bootstrapping again.
func applyAdoptedNode(np *mlv1alpha1.NodeProvision, node *corev1.Node) {
	np.Status.NodeName = node.Name
	if np.Status.IPAddress == "" {
		np.Status.IPAddress = nodeInternalAddress(node)
	}
	if !np.Spec.DisableVPN && np.Status.VpnIP == "" {
		// The kubelet advertises the tunnel address on VPN clusters.
		np.Status.VpnIP = np.Status.IPAddress
	}
}

// teardownAWSForRetry removes what a failed AWS attempt left behind before its
// status is reset: the joined Node (its finalizer would otherwise pin it
// forever) and the EC2 instance. wait is true when the caller must return res
// and try again later without resetting status.
//
// When no InstanceID was recorded a launched instance may still exist (the
// launch succeeded, the status write did not). Its VPN peer must not be
// released before deciding what to do with it, so it is looked up first: it is
// adopted when possible and terminated only when adoption is impossible.
func (r *NodeProvisionReconciler) teardownAWSForRetry(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, wait bool) {
	if np.Status.InstanceID == "" {
		if res, handled := r.resolveUnrecordedInstance(ctx, np); handled {
			return res, true
		}
	}
	return r.releaseAWSInstance(ctx, np, false)
}

// resolveUnrecordedInstance handles a tagged EC2 instance that status does not
// know about while a failed AWS attempt is being retried. handled is true when
// the caller must return res (adopted, or waiting); false means there is
// nothing left (none found, or it was terminated) and teardown may continue.
func (r *NodeProvisionReconciler) resolveUnrecordedInstance(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, handled bool) {
	log := logf.FromContext(ctx)

	secret, err := r.awsCredentialSecret(ctx, np)
	if err != nil {
		// Without credentials nothing can be looked up (and no new attempt could
		// launch anything either); do not block the retry on it.
		log.Info("No AWS credentials to look for an unrecorded instance — continuing", "err", err.Error())
		return ctrl.Result{}, false
	}
	id, err := r.aws().FindInstanceID(ctx, np, r.teardownAWSCreds(ctx, np, secret))
	if err != nil {
		log.Error(err, "looking for an unrecorded EC2 instance before retrying — will retry")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}
	if id == "" {
		return ctrl.Result{}, false
	}

	var netConfig *mlv1alpha1.NodeProvisionNetConfig
	if !np.Spec.DisableVPN {
		if netConfig, err = r.requireNetConfig(ctx, np); err != nil {
			log.Error(err, "reading NodeProvisionNetConfig to recover the instance's VPN peer — will retry")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
	}
	vpnIP, ok := recordedPeerFor(np, netConfig)
	if !ok {
		log.Info("Found an unrecorded EC2 instance but no recorded VPN peer; terminating it so a clean attempt can run",
			"instanceId", id)
		if tErr := r.terminateAWSInstance(ctx, np, id); tErr != nil {
			log.Error(tErr, "terminating unadoptable EC2 instance — will retry", "instanceId", id)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
		return ctrl.Result{}, false
	}
	log.Info("Adopting the EC2 instance of the failed attempt instead of replacing it", "instanceId", id)
	if pErr := r.persistInstanceID(ctx, np, id, vpnIP); pErr != nil {
		log.Error(pErr, "persisting adopted InstanceID — will retry", "instanceId", id)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}
	return ctrl.Result{RequeueAfter: requeueShort}, true
}

// releaseAWSInstance removes the joined Node, then terminates the instance
// recorded in status. With sweepUnrecorded it also terminates a tagged instance
// that status never recorded (terminal teardown, where adoption is pointless).
func (r *NodeProvisionReconciler) releaseAWSInstance(ctx context.Context, np *mlv1alpha1.NodeProvision, sweepUnrecorded bool) (res ctrl.Result, wait bool) {
	log := logf.FromContext(ctx)

	nodeName, err := r.resolveNodeName(ctx, np)
	if err != nil {
		log.Error(err, "resolving node before teardown — will retry")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}
	if nodeName != "" && !r.removeK8sNode(ctx, np, nodeName) {
		log.Info("Waiting for stale node to be removed before continuing", "node", nodeName)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, true
	}

	if id := np.Status.InstanceID; id != "" {
		if err := r.terminateAWSInstance(ctx, np, id); err != nil {
			log.Error(err, "terminating EC2 instance of failed attempt — will retry",
				"instanceId", id)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
		log.Info("Terminated EC2 instance of failed attempt", "instanceId", id)
	} else if sweepUnrecorded && r.terminateOrphanedInstance(ctx, np) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}
	return ctrl.Result{}, false
}

// recordedPeerFor returns the VPN IP recorded for np's peer by node name (or by
// the VPN IP already in status) in nc. Without a VPN there is nothing to
// recover and ok is true with an empty IP; with a VPN and no recorded peer ok
// is false.
func recordedPeerFor(np *mlv1alpha1.NodeProvision, nc *mlv1alpha1.NodeProvisionNetConfig) (vpnIP string, ok bool) {
	if np.Spec.DisableVPN {
		return "", true
	}
	if nc == nil {
		return "", false
	}
	for _, p := range nc.Status.VPNPeers {
		if p.NodeName == np.Name && p.VPNIP != "" {
			return p.VPNIP, true
		}
	}
	if np.Status.VpnIP != "" {
		for _, p := range nc.Status.VPNPeers {
			if p.VPNIP == np.Status.VpnIP {
				return p.VPNIP, true
			}
		}
	}
	return "", false
}

// ────────────────────────────────────────────────────────────────────────────
// Kubernetes Node removal
// ────────────────────────────────────────────────────────────────────────────

// removeK8sNode strips the management finalizer from the Node and deletes it.
// It returns true once the Node is confirmed gone (or is not this
// NodeProvision's to remove); false while removal is still in progress.
func (r *NodeProvisionReconciler) removeK8sNode(ctx context.Context, np *mlv1alpha1.NodeProvision, nodeName string) bool {
	log := logf.FromContext(ctx)

	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Node confirmed deleted from cluster", "node", nodeName)
			return true
		}
		log.Error(err, "looking up node for removal", "node", nodeName)
		return false
	}
	// Never remove a Node stamped for a different NodeProvision (e.g. the name
	// was reused by a recreated CR or a re-registered machine).
	if owner := node.Labels[nodeProvisionUIDLabel]; owner != "" && np.UID != "" && owner != string(np.UID) {
		log.Info("Node belongs to a different NodeProvision — leaving it alone", "node", nodeName)
		return true
	}

	// Strip our management finalizer first.  This is necessary because if
	// the node already has a DeletionTimestamp (e.g. from a previous
	// controller run that called Delete before crashing, or from a manual
	// `kubectl delete node`), removing the last finalizer causes the API
	// server to GC the node immediately — so the subsequent Delete call
	// would hit "not found".  We handle that below with IgnoreNotFound.
	if controllerutil.ContainsFinalizer(node, nodeProvisionNodeFinalizer) {
		patch := client.MergeFrom(node.DeepCopy())
		controllerutil.RemoveFinalizer(node, nodeProvisionNodeFinalizer)
		if err := r.Patch(ctx, node, patch); err != nil {
			log.Error(err, "removing node finalizer (continuing)", "node", nodeName)
		} else {
			log.Info("Removed management finalizer from node", "node", nodeName)
		}
	}
	// If DeletionTimestamp is already set the API server will delete the
	// node as soon as all finalizers are gone (handled above).
	if node.DeletionTimestamp.IsZero() {
		if err := r.Delete(ctx, node); client.IgnoreNotFound(err) != nil {
			log.Error(err, "deleting node from cluster", "node", nodeName)
		} else if err == nil {
			log.Info("Removed node from cluster", "node", nodeName)
		}
	} else {
		log.Info("Node already terminating — finalizer removal will complete deletion", "node", nodeName)
	}
	// Confirmation comes from the next lookup returning NotFound.
	return false
}

// ────────────────────────────────────────────────────────────────────────────
// AWS helpers
// ────────────────────────────────────────────────────────────────────────────

// awsCredentialSecret returns the Secret to use for teardown-time AWS calls:
// the user-supplied one, falling back to the controller-owned copy in case the
// user deleted theirs first.
func (r *NodeProvisionReconciler) awsCredentialSecret(ctx context.Context, np *mlv1alpha1.NodeProvision) (*corev1.Secret, error) {
	log := logf.FromContext(ctx)
	secret, err := r.getSecret(ctx, np)
	if err == nil {
		return secret, nil
	}
	if !apierrors.IsNotFound(err) {
		log.Error(err, "getting credentials for AWS teardown, trying controller copy")
	} else {
		log.Info("User credentials secret not found, falling back to controller copy",
			"userSecret", np.Spec.CredentialsRef.Name)
	}
	secret, cErr := r.getControllerCredsSecret(ctx, np)
	if cErr != nil {
		return nil, fmt.Errorf("no AWS credentials available; both user and controller-copy secrets unavailable: %w", cErr)
	}
	return secret, nil
}

// teardownAWSCreds resolves AWS credentials for secret, falling back to the
// static keys when the credential manager cannot produce a session.
func (r *NodeProvisionReconciler) teardownAWSCreds(ctx context.Context, np *mlv1alpha1.NodeProvision, secret *corev1.Secret) awsprovision.AWSCredentials {
	creds, err := r.resolveAWSCreds(ctx, np.Spec.Region, secret)
	if err != nil {
		logf.FromContext(ctx).Error(err, "resolving AWS credentials — using static fallback")
		return awsprovision.ResolveAWSCredentials(secret)
	}
	return creds
}

// terminateAWSInstance terminates the EC2 instance, handling expired STS
// sessions: on an auth failure the CredMgr cache entry is evicted and the call
// is retried once with static credentials (session token stripped). A nil
// return means the instance is terminated or already gone.
func (r *NodeProvisionReconciler) terminateAWSInstance(ctx context.Context, np *mlv1alpha1.NodeProvision, instanceID string) error {
	secret, err := r.awsCredentialSecret(ctx, np)
	if err != nil {
		return fmt.Errorf("terminating EC2 instance %s: %w", instanceID, err)
	}
	creds := r.teardownAWSCreds(ctx, np, secret)
	err = r.aws().TerminateInstance(ctx, np, creds, instanceID)
	if err == nil {
		return nil
	}
	if !awsprovision.IsAWSAuthFailure(err) {
		return fmt.Errorf("terminating EC2 instance %s: %w", instanceID, err)
	}
	// Cached STS session is likely expired. Evict the CredMgr cache so the next
	// reconcile forces a fresh credential resolution, then retry once now with
	// static-only credentials in case the IAM key itself is still valid.
	if r.CredMgr != nil {
		r.CredMgr.Evict(secret.Namespace, secret.Name)
	}
	staticCreds := awsprovision.StaticCredsNoSession(awsprovision.ResolveAWSCredentials(secret))
	if retryErr := r.aws().TerminateInstance(ctx, np, staticCreds, instanceID); retryErr != nil {
		return fmt.Errorf("terminating EC2 instance %s (after static-credential retry): %w", instanceID, retryErr)
	}
	return nil
}

// persistInstanceID records an EC2 instance (and its VPN address) on the
// NodeProvision status with a bounded retry against conflicts.
func (r *NodeProvisionReconciler) persistInstanceID(ctx context.Context, np *mlv1alpha1.NodeProvision, instanceID, vpnIP string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return err
		}
		now := metav1.Now()
		fresh.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
		fresh.Status.Message = "Waiting for instance to become running"
		fresh.Status.InstanceID = instanceID
		if vpnIP != "" {
			fresh.Status.VpnIP = vpnIP
			fresh.Status.IPAddress = vpnIP
		}
		fresh.Status.Progress = 30
		fresh.Status.LastUpdated = &now
		if fresh.Status.StartTime == nil {
			fresh.Status.StartTime = &now
		}
		return r.Status().Update(ctx, fresh)
	})
}

// adoptExistingInstance looks for an EC2 instance already launched for this
// NodeProvision (tagged with its UID) — left behind by a crash between
// RunInstances and the status write — and adopts it instead of launching a
// second one. handled is true when the caller must return res/err.
func (r *NodeProvisionReconciler) adoptExistingInstance(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	creds awsprovision.AWSCredentials,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
) (res ctrl.Result, handled bool, err error) {
	id, findErr := r.aws().FindInstanceID(ctx, np, creds)
	if findErr != nil {
		res, err = r.failNodeProvision(ctx, np, fmt.Sprintf("looking up existing EC2 instance: %v", findErr))
		return res, true, err
	}
	if id == "" {
		return ctrl.Result{}, false, nil
	}
	res, err = r.adoptInstance(ctx, np, id, netConfig)
	return res, true, err
}

// recoverLaunchedInstance is the crash-recovery step of the CreatingInstance /
// ConfiguringVPN phases (no InstanceID yet): if an instance tagged with this
// NodeProvision's UID exists it is adopted. handled is false when there is
// nothing to adopt (or it cannot be looked up), so the caller falls through to
// its stall handling.
func (r *NodeProvisionReconciler) recoverLaunchedInstance(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, handled bool, err error) {
	if np.Spec.Provider == mlv1alpha1.CloudProviderGCP {
		return r.recoverLaunchedGCPInstance(ctx, np)
	}
	if np.Spec.Provider != mlv1alpha1.CloudProviderAWS {
		return ctrl.Result{}, false, nil
	}
	log := logf.FromContext(ctx)
	secret, sErr := r.awsCredentialSecret(ctx, np)
	if sErr != nil {
		log.Info("No AWS credentials to look for a launched instance", "err", sErr.Error())
		return ctrl.Result{}, false, nil
	}
	id, fErr := r.aws().FindInstanceID(ctx, np, r.teardownAWSCreds(ctx, np, secret))
	if fErr != nil {
		log.Error(fErr, "looking for an already-launched EC2 instance (will retry)")
		return ctrl.Result{}, false, nil
	}
	if id == "" {
		return ctrl.Result{}, false, nil
	}
	var netConfig *mlv1alpha1.NodeProvisionNetConfig
	if !np.Spec.DisableVPN {
		if netConfig, err = r.requireNetConfig(ctx, np); err != nil {
			log.Error(err, "reading NodeProvisionNetConfig to recover the instance's VPN peer (will retry)")
			return ctrl.Result{RequeueAfter: requeueShort}, true, nil
		}
	}
	res, err = r.adoptInstance(ctx, np, id, netConfig)
	return res, true, err
}

// adoptInstance takes over EC2 instance id, which was launched for np. In VPN
// mode the tunnel identity baked into its cloud-init is the peer recorded on
// the NetConfig under this node's name; that peer is preserved and its address
// persisted. If none is recorded the instance can never join, so it is
// terminated and the attempt fails for a clean retry. Without a VPN only the
// InstanceID is persisted.
func (r *NodeProvisionReconciler) adoptInstance(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	id string,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	vpnIP, ok := recordedPeerFor(np, netConfig)
	if !ok {
		log.Info("Found an EC2 instance for this NodeProvision but no recorded VPN peer; terminating it so a clean attempt can run",
			"instanceId", id)
		if tErr := r.terminateAWSInstance(ctx, np, id); tErr != nil {
			return r.failNodeProvision(ctx, np, fmt.Sprintf(
				"existing EC2 instance %s has no recorded VPN peer and could not be terminated: %v", id, tErr))
		}
		return r.failNodeProvision(ctx, np, fmt.Sprintf(
			"terminated orphaned EC2 instance %s that had no recorded VPN peer; retrying", id))
	}

	log.Info("Adopting EC2 instance found by NodeProvision tag instead of launching another", "instanceId", id)
	if pErr := r.persistInstanceID(ctx, np, id, vpnIP); pErr != nil {
		log.Error(pErr, "persisting adopted InstanceID — will retry", "instanceId", id)
	}
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// terminateOrphanedInstance is used on deletion when no InstanceID was ever
// persisted: it terminates an instance tagged for this NodeProvision, if any.
// requeue is true when deletion must wait and retry.
func (r *NodeProvisionReconciler) terminateOrphanedInstance(ctx context.Context, np *mlv1alpha1.NodeProvision) (requeue bool) {
	log := logf.FromContext(ctx)
	if np.Spec.Region == "" || np.UID == "" {
		return false
	}
	secret, err := r.awsCredentialSecret(ctx, np)
	if err != nil {
		log.Info("No AWS credentials available to look for an orphaned instance — skipping", "err", err.Error())
		return false
	}
	id, err := r.aws().FindInstanceID(ctx, np, r.teardownAWSCreds(ctx, np, secret))
	if err != nil {
		if deletionElapsed(np) > deletionGiveUpAfter {
			log.Error(err, "giving up looking for an orphaned EC2 instance after the deletion grace period; it may need manual cleanup")
			return false
		}
		log.Error(err, "looking for an orphaned EC2 instance — will retry")
		return true
	}
	if id == "" {
		return false
	}
	log.Info("Found orphaned EC2 instance for this NodeProvision — terminating", "instanceId", id)
	if err := r.terminateAWSInstance(ctx, np, id); err != nil {
		if deletionElapsed(np) > deletionGiveUpAfter {
			log.Error(err, "giving up terminating orphaned EC2 instance after the deletion grace period; it needs manual cleanup",
				"instanceId", id)
			return false
		}
		log.Error(err, "terminating orphaned EC2 instance — will retry", "instanceId", id)
		return true
	}
	return false
}

// ────────────────────────────────────────────────────────────────────────────
// On-prem background job lifecycle
// ────────────────────────────────────────────────────────────────────────────

// setStatusVPN persists a VPN allocation on the NodeProvision status (retrying
// on conflict) and mirrors it into np.
func (r *NodeProvisionReconciler) setStatusVPN(ctx context.Context, np *mlv1alpha1.NodeProvision, vpnIP string) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return err
		}
		if fresh.Status.VpnIP == vpnIP && fresh.Status.IPAddress == vpnIP {
			return nil
		}
		fresh.Status.VpnIP = vpnIP
		fresh.Status.IPAddress = vpnIP
		return r.Status().Update(ctx, fresh)
	})
	if err != nil {
		return err
	}
	np.Status.VpnIP = vpnIP
	np.Status.IPAddress = vpnIP
	return nil
}

// recordAllocationInNetConfig best-effort writes an allocated VPN IP/peer to the
// NetConfig so cleanupVPNPeer can find the peer by node name.
func (r *NodeProvisionReconciler) recordAllocationInNetConfig(ctx context.Context, np *mlv1alpha1.NodeProvision, vpnIP, publicKey string) {
	if vpnIP == "" || publicKey == "" || np.Spec.DisableVPN {
		return
	}
	nc, err := r.requireNetConfig(ctx, np)
	if err != nil {
		logf.FromContext(ctx).Info("NetConfig unavailable while recording VPN allocation; peer will be found via the VPN server", "err", err.Error())
		return
	}
	if err := r.updateNetConfigStatus(ctx, nc, vpnIP, publicKey, np.Name); err != nil {
		logf.FromContext(ctx).Error(err, "recording VPN allocation in NetConfig (non-fatal)")
	}
}

// stopOnPremJob cancels np's background bootstrap (if any), waits up to block
// for it to report what it allocated, persists that allocation so the VPN peer
// can be released, and forgets the job. pending is true when the goroutine has
// not reported yet and the caller still wants to wait (allowPending); the job
// stays registered in that case.
func (r *NodeProvisionReconciler) stopOnPremJob(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	block time.Duration,
	allowPending bool,
) (pending bool, err error) {
	job, ok := r.loadOnPremJob(np)
	if !ok {
		return false, nil
	}
	job.cancel()
	if job.result == nil && block > 0 {
		t := time.NewTimer(block)
		select {
		case res := <-job.ch:
			job.result = &res
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
	}
	if job.result == nil {
		select { // non-blocking look
		case res := <-job.ch:
			job.result = &res
		default:
		}
	}
	if job.result == nil {
		if allowPending {
			return true, nil
		}
		logf.FromContext(ctx).Info("Cancelled on-prem bootstrap did not report in time; forgetting it")
		r.dropOnPremJob(np, job)
		return false, nil
	}
	if res := job.result; res.vpnIP != "" && !np.Spec.DisableVPN {
		r.recordAllocationInNetConfig(ctx, np, res.vpnIP, res.publicKey)
		if err := r.setStatusVPN(ctx, np, res.vpnIP); err != nil {
			return false, fmt.Errorf("persisting VPN allocation of cancelled bootstrap: %w", err)
		}
	}
	r.dropOnPremJob(np, job)
	return false, nil
}
