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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	gcpprovision "dcn.ssu.ac.kr/infra/provider/gcp"
)

// This file is the GCP (Google Compute Engine) provisioning path of the
// NodeProvision controller. It mirrors the AWS path (reconcileAWSProvisioning
// and friends in nodeprovision_controller.go / nodeprovision_lifecycle.go)
// step for step and shares everything provider-neutral with it: the phase
// machine, the stall timeouts, failNodeProvision, VPN peer bookkeeping
// (requireNetConfig, updateNetConfigStatus, cleanupVPNPeer), persistInstanceID,
// reconcileJoining and getSSHClientByProvider.
//
// Status.InstanceID holds the GCE instance NAME (unique per zone). The
// instance, its boot disk and (via the controller-owned UID label) its
// ownership are all derived from the deterministic name, so a reconcile retried
// after a crash adopts the instance instead of creating another.

// ────────────────────────────────────────────────────────────────────────────
// Test seam
// ────────────────────────────────────────────────────────────────────────────

// gcpAPI groups the GCP calls the controller makes (and the VPN-server dial of
// the launch path). A nil field means the real implementation.
type gcpAPI struct {
	ResolveDefaults func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds gcpprovision.Credentials) (*gcpprovision.Resolved, error)
	FindInstance    func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds gcpprovision.Credentials) (string, error)
	// ProvisionInstance inserts the instance; sshPublicKey is the authorized_keys
	// public half of the key stored in the <name>-ssh-key Secret.
	ProvisionInstance func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds gcpprovision.Credentials, sshPublicKey []byte,
		vpn *ssh.Client, nc *mlv1alpha1.NodeProvisionNetConfig, rt pkgruntime.Config) (*gcpprovision.ProvisionResult, error)
	WaitForInstanceRunning func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds gcpprovision.Credentials, instanceName string) (internalIP, externalIP string, err error)
	// DeleteInstance deletes the instance (when instanceName != "") and the
	// node's firewall rules; idempotent.
	DeleteInstance func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds gcpprovision.Credentials, instanceName string) error
	// DialVPNServer opens the SSH connection to the VPN server (VPN mode only).
	DialVPNServer func(ctx context.Context, nc *mlv1alpha1.NodeProvisionNetConfig) (*ssh.Client, error)
}

// gcp returns the GCP calls with defaults filled in.
func (r *NodeProvisionReconciler) gcp() gcpAPI {
	g := r.gcpOverrides
	if g.ResolveDefaults == nil {
		g.ResolveDefaults = func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds gcpprovision.Credentials) (*gcpprovision.Resolved, error) {
			return gcpprovision.ResolveDefaults(ctx, creds, np)
		}
	}
	if g.FindInstance == nil {
		g.FindInstance = gcpprovision.FindInstance
	}
	if g.ProvisionInstance == nil {
		g.ProvisionInstance = gcpprovision.ProvisionInstance
	}
	if g.WaitForInstanceRunning == nil {
		g.WaitForInstanceRunning = gcpprovision.WaitForInstanceRunning
	}
	if g.DeleteInstance == nil {
		g.DeleteInstance = gcpprovision.DeleteInstance
	}
	if g.DialVPNServer == nil {
		g.DialVPNServer = r.getVPNServerSSHClient
	}
	return g
}

// ────────────────────────────────────────────────────────────────────────────
// Credentials
// ────────────────────────────────────────────────────────────────────────────

// resolveGCPCreds parses the service-account key in secret (spec.credentialsRef.key,
// default credentials.json / serviceAccountKey). There is nothing to cache or
// refresh: the Google client libraries mint and renew OAuth2 tokens from the key.
func resolveGCPCreds(np *mlv1alpha1.NodeProvision, secret *corev1.Secret) (gcpprovision.Credentials, error) {
	return gcpprovision.ResolveCredentials(secret, np.Spec.CredentialsRef.Key)
}

// gcpCredentialSecret returns the Secret to use for teardown-time GCP calls:
// the user-supplied one, falling back to the controller-owned copy in case the
// user deleted theirs first.
func (r *NodeProvisionReconciler) gcpCredentialSecret(ctx context.Context, np *mlv1alpha1.NodeProvision) (*corev1.Secret, error) {
	log := logf.FromContext(ctx)
	secret, err := r.getSecret(ctx, np)
	if err == nil {
		return secret, nil
	}
	if !apierrors.IsNotFound(err) {
		log.Error(err, "getting credentials for GCP teardown, trying controller copy")
	} else {
		log.Info("User credentials secret not found, falling back to controller copy",
			"userSecret", np.Spec.CredentialsRef.Name)
	}
	secret, cErr := r.getControllerCredsSecret(ctx, np)
	if cErr != nil {
		return nil, fmt.Errorf("no GCP credentials available; both user and controller-copy secrets unavailable: %w", cErr)
	}
	return secret, nil
}

// teardownGCPCreds resolves the credentials used for teardown-time calls.
func (r *NodeProvisionReconciler) teardownGCPCreds(ctx context.Context, np *mlv1alpha1.NodeProvision) (gcpprovision.Credentials, error) {
	secret, err := r.gcpCredentialSecret(ctx, np)
	if err != nil {
		return gcpprovision.Credentials{}, err
	}
	creds, err := resolveGCPCreds(np, secret)
	if err != nil {
		return gcpprovision.Credentials{}, fmt.Errorf("resolving GCP credentials: %w", err)
	}
	return creds, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Provisioning
// ────────────────────────────────────────────────────────────────────────────

// reconcileGCPProvisioning validates, resolves defaults, registers the VPN peer
// (VPN mode) and launches the instance. Phases mirror the AWS path:
// Validating -> ConfiguringVPN (VPN mode) -> CreatingInstance -> (persist
// InstanceID) WaitingForInstance -> Bootstrapping -> Joining ...
func (r *NodeProvisionReconciler) reconcileGCPProvisioning(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	name := np.Name

	// ── Idempotency guard ────────────────────────────────────────────────────
	// An InstanceID in status means the instance exists: never insert again.
	if np.Status.InstanceID != "" {
		log.Info("GCE instance already exists, skipping creation", "instance", np.Status.InstanceID)
		if np.Status.Phase != mlv1alpha1.NodeProvisionPhaseWaitingForInstance {
			now := metav1.Now()
			np.Status.Phase = mlv1alpha1.NodeProvisionPhaseWaitingForInstance
			np.Status.LastUpdated = &now
			if err := r.Status().Update(ctx, np); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	creds, err := resolveGCPCreds(np, secret)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving GCP credentials failed: %v", err))
	}

	// ── Resolve defaults (machine type, project, zone, network, image) ──────
	// A patch changes the ResourceVersion; returning lets the spec-change watch
	// event trigger a fresh reconcile on the up-to-date object.
	patched, err := r.resolveGCPDefaults(ctx, np, creds)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving GCP defaults: %v", err))
	}
	if patched {
		return ctrl.Result{}, nil
	}

	// ── Validate ────────────────────────────────────────────────────────────
	if err := gcpprovision.ValidateGCPConfig(np.Spec); err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("GCP validation failed: %v", err))
	}
	log.Info("GCP validation successful")

	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseValidating, "Validating GCP configuration", 5)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
	}

	// ── Fetch NodeProvisionNetConfig ────────────────────────────────────────
	netConfig, err := r.requireNetConfig(ctx, np)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// ── Adopt an instance a previous attempt already launched ───────────────
	// The insert may have succeeded without the InstanceID reaching status. The
	// instance has a deterministic name and this NodeProvision's UID label, so
	// look it up before allocating a second VPN peer and a second instance.
	if res, handled, aErr := r.adoptExistingGCPInstance(ctx, np, creds, netConfig); handled {
		return res, aErr
	}

	// ── SSH key (private half in <name>-ssh-key, public half into metadata) ─
	sshPub, err := r.ensureGCPSSHKey(ctx, np)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("preparing SSH key: %v", err))
	}

	// ── Connect to VPN server (VPN mode only; never in VPN-less mode) ────────
	var vpnServerClient *ssh.Client
	if !np.Spec.DisableVPN {
		r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseConfiguringVPN, "Configuring VPN client", 15)
		if sErr := r.Status().Update(ctx, np); sErr != nil {
			if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
				return ctrl.Result{}, err
			}
		}
		vpnServerClient, err = r.gcp().DialVPNServer(ctx, netConfig)
		if err != nil {
			return r.failNodeProvision(ctx, np, fmt.Sprintf("connecting to VPN server: %v", err))
		}
		defer vpnServerClient.Conn.Close() //nolint:errcheck
	}

	// ── Launch the GCE instance with the startup script ─────────────────────
	r.setPhaseStatus(np, mlv1alpha1.NodeProvisionPhaseCreatingInstance, "Creating GCE instance", 25)
	if sErr := r.Status().Update(ctx, np); sErr != nil {
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, np); err != nil {
			return ctrl.Result{}, err
		}
	}
	log.Info("Creating GCE instance")

	runtimeCfg, err := r.resolveCnlabRuntimeConfig(ctx, netConfig.Spec.SoftwareConfig, netConfig.Namespace)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving cnlab-runtime config: %v", err))
	}

	result, err := r.gcp().ProvisionInstance(ctx, np, creds, sshPub, vpnServerClient, netConfig, runtimeCfg)
	if errors.Is(err, gcpprovision.ErrInstanceAlreadyLaunched) {
		// The insert reports that an instance of this name exists and the peer
		// registered for this attempt was released. Not a failure: the next
		// reconcile (phase CreatingInstance/ConfiguringVPN) adopts it by name+UID.
		log.Info("A GCE instance was already launched for this NodeProvision; requeueing to adopt it")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	// Always persist the VPN allocation immediately — even when the launch failed
	// — so cleanupVPNPeer can find and release the peer on the next retry instead
	// of leaving an orphan and allocating yet another IP.
	var recordVPN func(*mlv1alpha1.NodeProvisionStatus)
	if result != nil && result.VpnIP != "" {
		if uErr := r.updateNetConfigStatus(ctx, netConfig, result.VpnIP, result.PublicKey, name); uErr != nil {
			log.Error(uErr, "persisting VPN allocation to NetConfig (non-fatal)")
		}
		vpnIP := result.VpnIP
		recordVPN = func(st *mlv1alpha1.NodeProvisionStatus) {
			st.VpnIP = vpnIP
			st.IPAddress = vpnIP
		}
	}
	if err != nil {
		return r.failNodeProvisionWith(ctx, np, fmt.Sprintf("GCE provisioning failed: %v", err), recordVPN)
	}
	if result.Adopted {
		log.Info("ProvisionInstance adopted an existing GCE instance", "instance", result.InstanceID)
		return r.adoptGCPInstance(ctx, np, result.InstanceID, netConfig)
	}
	log.Info("GCE instance created", "instance", result.InstanceID)

	// Persist the InstanceID with a bounded retry against conflicts/transient API
	// errors. The instance already exists, so falling back to failNodeProvision
	// here would leave the phase stuck without an InstanceID. If every retry fails
	// it is still recoverable: it carries this NodeProvision's UID label, so the
	// CreatingInstance/ConfiguringVPN branch (recoverLaunchedInstance) adopts it,
	// and deletion removes it by name.
	if persistErr := r.persistInstanceID(ctx, np, result.InstanceID, result.VpnIP); persistErr != nil {
		log.Error(persistErr, "failed to persist InstanceID after retries — will retry on next reconcile",
			"instance", result.InstanceID)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	log.Info("GCE instance created, waiting for it to become running", "instance", result.InstanceID)
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// gcpDefaultsComplete reports whether every defaultable field is already set, in
// which case no Compute API call is needed (the common case on every reconcile
// after the first).
func gcpDefaultsComplete(np *mlv1alpha1.NodeProvision) bool {
	c := np.Spec.GCPConfig
	return c != nil && np.Spec.InstanceType != "" && np.Spec.Region != "" &&
		c.ProjectID != "" && c.Zone != "" && (c.SourceImage != "" || c.SourceSnapshot != "") && c.Network != ""
}

// resolveGCPDefaults fills in missing spec fields (project, machine type, zone,
// region, network, boot image) and persists them with a Patch so they are visible
// to operators and stay fixed across retries; fields set by the user are never
// overwritten. It returns (true, nil) when it patched so the caller can return
// and let the watch event trigger a fresh reconcile.
func (r *NodeProvisionReconciler) resolveGCPDefaults(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	creds gcpprovision.Credentials,
) (patched bool, err error) {
	log := logf.FromContext(ctx)
	if gcpDefaultsComplete(np) {
		return false, nil
	}
	res, err := r.gcp().ResolveDefaults(ctx, np, creds)
	if err != nil {
		return false, err
	}
	if res == nil {
		return false, fmt.Errorf("resolving GCP defaults returned nothing")
	}
	if np.Spec.GCPConfig != nil && reflect.DeepEqual(*np.Spec.GCPConfig, res.Config) &&
		np.Spec.Region == res.Region && np.Spec.InstanceType == res.InstanceType {
		return false, nil
	}
	base := np.DeepCopy()
	np.Spec.Region = res.Region
	np.Spec.InstanceType = res.InstanceType
	cfg := res.Config
	np.Spec.GCPConfig = &cfg
	if err := r.Patch(ctx, np, client.MergeFrom(base)); err != nil {
		return false, fmt.Errorf("patching NodeProvision spec with resolved defaults: %w", err)
	}
	log.Info("Patched NodeProvision spec with resolved GCP defaults",
		"project", cfg.ProjectID, "zone", cfg.Zone, "machineType", res.InstanceType,
		"network", cfg.Network, "image", cfg.SourceImage, "snapshot", cfg.SourceSnapshot)
	return true, nil
}

// ensureGCPSSHKey makes sure the private key Secret "<name>-ssh-key" exists
// (generating a fresh RSA key when it does not, with the same layout the AWS path
// uses so getSSHClientByProvider serves both) and returns the matching public key.
// Idempotent: an existing Secret is reused, never overwritten.
func (r *NodeProvisionReconciler) ensureGCPSSHKey(ctx context.Context, np *mlv1alpha1.NodeProvision) ([]byte, error) {
	log := logf.FromContext(ctx)
	secretName := gcpprovision.SSHKeySecretName(np.Name)
	key := types.NamespacedName{Name: secretName, Namespace: np.Namespace}

	existing := &corev1.Secret{}
	err := r.Get(ctx, key, existing)
	switch {
	case err == nil:
		priv := strings.TrimSpace(string(existing.Data["ssh-privatekey"]))
		if priv == "" {
			return nil, fmt.Errorf("SSH key secret %q has no ssh-privatekey", secretName)
		}
		pub, perr := gcpprovision.PublicKeyFromPEM(priv)
		if perr != nil {
			return nil, fmt.Errorf("parsing private key from secret %q: %w", secretName, perr)
		}
		return pub, nil
	case !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("looking up SSH key secret %q: %w", secretName, err)
	}

	privPEM, pub, err := gcpprovision.GenerateSSHKeyPair()
	if err != nil {
		return nil, err
	}
	trueVal := true
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: np.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         mlv1alpha1.GroupVersion.String(),
				Kind:               "NodeProvision",
				Name:               np.Name,
				UID:                np.UID,
				Controller:         &trueVal,
				BlockOwnerDeletion: &trueVal,
			}},
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "node-provision-controller",
				"ml.dcn.ssu.ac.kr/node":        np.Name,
			},
		},
		Type: corev1.SecretTypeSSHAuth,
		Data: map[string][]byte{"ssh-privatekey": []byte(privPEM)},
	}
	if err := r.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("creating SSH key secret %q: %w", secretName, err)
		}
		// Lost a race with another reconcile: use the key that won.
		if gErr := r.apiReader().Get(ctx, key, existing); gErr != nil {
			return nil, fmt.Errorf("re-reading SSH key secret %q: %w", secretName, gErr)
		}
		return gcpprovision.PublicKeyFromPEM(strings.TrimSpace(string(existing.Data["ssh-privatekey"])))
	}
	log.Info("SSH key secret created", "secret", secretName)
	return pub, nil
}

// ────────────────────────────────────────────────────────────────────────────
// WaitingForInstance
// ────────────────────────────────────────────────────────────────────────────

// reconcileGCPWaitingForInstance polls the instance until RUNNING, then moves to
// Bootstrapping (reconcileJoining takes over from there).
func (r *NodeProvisionReconciler) reconcileGCPWaitingForInstance(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	secret *corev1.Secret,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	creds, err := resolveGCPCreds(np, secret)
	if err != nil {
		return r.failNodeProvision(ctx, np, fmt.Sprintf("resolving GCP credentials failed: %v", err))
	}

	internalIP, externalIP, err := r.gcp().WaitForInstanceRunning(ctx, np, creds, np.Status.InstanceID)
	if err != nil {
		if gcpprovision.IsTransient(err) {
			// A throttled/unavailable API is not a reason to fail the node; the
			// waitingForInstanceStallTimeout still bounds how long this may go on.
			log.Info("Transient GCP API error while polling the instance, requeueing", "err", err.Error())
			return ctrl.Result{RequeueAfter: requeueShort}, nil
		}
		return r.failNodeProvision(ctx, np, fmt.Sprintf("polling instance state: %v", err))
	}
	if internalIP == "" {
		log.Info("Instance not yet running, requeueing", "instance", np.Status.InstanceID)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &mlv1alpha1.NodeProvision{}
		if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, fresh); err != nil {
			return err
		}
		now := metav1.Now()
		fresh.Status.PrivateIP = internalIP
		fresh.Status.PublicIP = externalIP
		if fresh.Spec.DisableVPN {
			// Without a tunnel the kubelet node IP is the instance's internal IP
			// (matches the IP the startup script reads from the metadata server).
			fresh.Status.IPAddress = internalIP
		}
		fresh.Status.Phase = mlv1alpha1.NodeProvisionPhaseBootstrapping
		fresh.Status.Message = "Instance running; startup-script bootstrap in progress"
		fresh.Status.Progress = 50
		fresh.Status.LastUpdated = &now
		if uErr := r.Status().Update(ctx, fresh); uErr != nil {
			return uErr
		}
		*np = *fresh
		return nil
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("updating NodeProvision status: %w", err)
	}
	log.Info("GCE instance running", "internalIP", internalIP, "externalIP", externalIP)
	return ctrl.Result{RequeueAfter: requeueJoining}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Adoption / crash recovery (GCP counterparts of the AWS helpers in
// nodeprovision_lifecycle.go)
// ────────────────────────────────────────────────────────────────────────────

// adoptExistingGCPInstance looks for an instance already launched for this
// NodeProvision and adopts it instead of launching a second one. handled is
// true when the caller must return res/err.
func (r *NodeProvisionReconciler) adoptExistingGCPInstance(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	creds gcpprovision.Credentials,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
) (res ctrl.Result, handled bool, err error) {
	id, findErr := r.gcp().FindInstance(ctx, np, creds)
	if findErr != nil {
		res, err = r.failNodeProvision(ctx, np, fmt.Sprintf("looking up existing GCE instance: %v", findErr))
		return res, true, err
	}
	if id == "" {
		return ctrl.Result{}, false, nil
	}
	res, err = r.adoptGCPInstance(ctx, np, id, netConfig)
	return res, true, err
}

// recoverLaunchedGCPInstance is the crash-recovery step of the CreatingInstance /
// ConfiguringVPN phases (no InstanceID yet). handled is false when there is
// nothing to adopt (or it cannot be looked up) so the caller falls through to its
// stall handling.
func (r *NodeProvisionReconciler) recoverLaunchedGCPInstance(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, handled bool, err error) {
	log := logf.FromContext(ctx)
	creds, cErr := r.teardownGCPCreds(ctx, np)
	if cErr != nil {
		log.Info("No usable GCP credentials to look for a launched instance", "err", cErr.Error())
		return ctrl.Result{}, false, nil
	}
	id, fErr := r.gcp().FindInstance(ctx, np, creds)
	if fErr != nil {
		log.Error(fErr, "looking for an already-launched GCE instance (will retry)")
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
	res, err = r.adoptGCPInstance(ctx, np, id, netConfig)
	return res, true, err
}

// adoptGCPInstance takes over instance id, launched for np. In VPN mode the
// tunnel identity baked into its startup script is the peer recorded on the
// NetConfig under this node's name; that peer is preserved and its address
// persisted. If none is recorded the instance can never join, so it is deleted
// and the attempt fails for a clean retry. Without a VPN only the InstanceID is
// persisted.
func (r *NodeProvisionReconciler) adoptGCPInstance(
	ctx context.Context,
	np *mlv1alpha1.NodeProvision,
	id string,
	netConfig *mlv1alpha1.NodeProvisionNetConfig,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	vpnIP, ok := recordedPeerFor(np, netConfig)
	if !ok {
		log.Info("Found a GCE instance for this NodeProvision but no recorded VPN peer; deleting it so a clean attempt can run",
			"instance", id)
		if tErr := r.deleteGCPInstance(ctx, np, id); tErr != nil {
			return r.failNodeProvision(ctx, np, fmt.Sprintf(
				"existing GCE instance %s has no recorded VPN peer and could not be deleted: %v", id, tErr))
		}
		return r.failNodeProvision(ctx, np, fmt.Sprintf(
			"deleted orphaned GCE instance %s that had no recorded VPN peer; retrying", id))
	}

	log.Info("Adopting GCE instance found by name and NodeProvision UID instead of launching another", "instance", id)
	if pErr := r.persistInstanceID(ctx, np, id, vpnIP); pErr != nil {
		log.Error(pErr, "persisting adopted InstanceID — will retry", "instance", id)
	}
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Teardown
// ────────────────────────────────────────────────────────────────────────────

// deleteGCPInstance deletes the instance (when id != "") and the node's firewall
// rules. A nil return means they are gone.
func (r *NodeProvisionReconciler) deleteGCPInstance(ctx context.Context, np *mlv1alpha1.NodeProvision, id string) error {
	creds, err := r.teardownGCPCreds(ctx, np)
	if err != nil {
		return fmt.Errorf("deleting GCE instance %s: %w", id, err)
	}
	if err := r.gcp().DeleteInstance(ctx, np, creds, id); err != nil {
		return fmt.Errorf("deleting GCE instance %s: %w", id, err)
	}
	return nil
}

// teardownGCPForRetry removes what a failed GCP attempt left behind before its
// status is reset: the joined Node (its finalizer would otherwise pin it
// forever) and the instance. wait is true when the caller must return res and
// try again later without resetting status.
//
// When no InstanceID was recorded a launched instance may still exist (the
// insert succeeded, the status write did not): it is looked up before its VPN
// peer is released, adopted when possible, deleted only when adoption is
// impossible.
func (r *NodeProvisionReconciler) teardownGCPForRetry(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, wait bool) {
	if np.Status.InstanceID == "" {
		if res, handled := r.resolveUnrecordedGCPInstance(ctx, np); handled {
			return res, true
		}
	}
	return r.releaseGCPInstance(ctx, np, false)
}

// resolveUnrecordedGCPInstance handles an instance that status does not know
// about while a failed GCP attempt is being retried (see teardownGCPForRetry).
func (r *NodeProvisionReconciler) resolveUnrecordedGCPInstance(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, handled bool) {
	log := logf.FromContext(ctx)

	creds, err := r.teardownGCPCreds(ctx, np)
	if err != nil {
		// Without credentials nothing can be looked up (and no new attempt could
		// launch anything either); do not block the retry on it.
		log.Info("No usable GCP credentials to look for an unrecorded instance — continuing", "err", err.Error())
		return ctrl.Result{}, false
	}
	id, err := r.gcp().FindInstance(ctx, np, creds)
	if err != nil {
		log.Error(err, "looking for an unrecorded GCE instance before retrying — will retry")
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
		log.Info("Found an unrecorded GCE instance but no recorded VPN peer; deleting it so a clean attempt can run",
			"instance", id)
		if tErr := r.deleteGCPInstance(ctx, np, id); tErr != nil {
			log.Error(tErr, "deleting unadoptable GCE instance — will retry", "instance", id)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
		return ctrl.Result{}, false
	}
	log.Info("Adopting the GCE instance of the failed attempt instead of replacing it", "instance", id)
	if pErr := r.persistInstanceID(ctx, np, id, vpnIP); pErr != nil {
		log.Error(pErr, "persisting adopted InstanceID — will retry", "instance", id)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}
	return ctrl.Result{RequeueAfter: requeueShort}, true
}

// releaseGCPInstance removes the joined Node, then deletes the instance recorded
// in status together with the node's firewall rules. With sweepUnrecorded it
// also deletes a UID-labelled instance that status never recorded (terminal
// teardown, where adoption is pointless).
func (r *NodeProvisionReconciler) releaseGCPInstance(ctx context.Context, np *mlv1alpha1.NodeProvision, sweepUnrecorded bool) (res ctrl.Result, wait bool) {
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
		if err := r.deleteGCPInstance(ctx, np, id); err != nil {
			log.Error(err, "deleting GCE instance of failed attempt — will retry", "instance", id)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
		log.Info("Deleted GCE instance of failed attempt", "instance", id)
	} else if sweepUnrecorded {
		if r.terminateOrphanedGCPInstance(ctx, np) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
	} else if gcpLocationResolved(np) {
		// No instance to delete, but the failed attempt may have left firewall rules.
		creds, cErr := r.teardownGCPCreds(ctx, np)
		if cErr != nil {
			// Same stance as the AWS teardown: without credentials nothing can be
			// cleaned up, and the retry must not be blocked on it.
			log.Info("No usable GCP credentials to remove firewall rules of the failed attempt — continuing", "err", cErr.Error())
		} else if err := r.gcp().DeleteInstance(ctx, np, creds, ""); err != nil {
			log.Error(err, "deleting firewall rules of failed attempt — will retry")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
	}
	return ctrl.Result{}, false
}

// gcpLocationResolved reports whether anything could have been created: the
// project and zone are only recorded once defaults resolution succeeded.
func gcpLocationResolved(np *mlv1alpha1.NodeProvision) bool {
	c := np.Spec.GCPConfig
	return c != nil && c.Zone != "" && c.ProjectID != ""
}

// terminateOrphanedGCPInstance is used on deletion/terminal teardown when no
// InstanceID was ever persisted: it deletes the instance carrying this
// NodeProvision's name and UID label, if any, plus the node's firewall rules.
// requeue is true when deletion must wait and retry; after deletionGiveUpAfter
// the step is abandoned loudly so the CR can go.
func (r *NodeProvisionReconciler) terminateOrphanedGCPInstance(ctx context.Context, np *mlv1alpha1.NodeProvision) (requeue bool) {
	log := logf.FromContext(ctx)
	if !gcpLocationResolved(np) || np.UID == "" {
		return false
	}
	creds, err := r.teardownGCPCreds(ctx, np)
	if err != nil {
		log.Info("No usable GCP credentials available to look for an orphaned instance — skipping", "err", err.Error())
		return false
	}
	giveUp := func(what string, err error) bool {
		if deletionElapsed(np) > deletionGiveUpAfter {
			log.Error(err, "giving up "+what+" after the deletion grace period; it may need manual cleanup")
			return false
		}
		log.Error(err, what+" — will retry")
		return true
	}
	id, err := r.gcp().FindInstance(ctx, np, creds)
	if err != nil {
		return giveUp("looking for an orphaned GCE instance", err)
	}
	if id != "" {
		log.Info("Found orphaned GCE instance for this NodeProvision — deleting", "instance", id)
	}
	// id may be "": the call then only removes the node's firewall rules.
	if err := r.gcp().DeleteInstance(ctx, np, creds, id); err != nil {
		return giveUp("deleting the orphaned GCE instance / firewall rules", err)
	}
	return false
}

// teardownTerminalGCP is the GCP case of teardownTerminal: it releases the joined
// Node and the instance (including a never-recorded one). It returns the strings
// to record in the terminal-teardown summary, or wait=true when the caller must
// return res.
func (r *NodeProvisionReconciler) teardownTerminalGCP(ctx context.Context, np *mlv1alpha1.NodeProvision) (released []string, res ctrl.Result, wait bool) {
	instance := np.Status.InstanceID
	if res, wait := r.releaseGCPInstance(ctx, np, true); wait {
		return nil, res, true
	}
	if instance != "" {
		released = append(released, "instance "+instance+" deleted")
	}
	if np.Status.NodeName != "" {
		released = append(released, "node "+np.Status.NodeName+" removed")
	}
	return released, ctrl.Result{}, false
}

// cleanupGCPOnDelete is the GCP case of handleDelete: it deletes the instance and
// the node's firewall rules, and must complete before the secrets and the
// finalizer are removed — otherwise the instance would be orphaned with no retry
// path. wait is true when the caller must requeue (res) and keep the finalizer.
func (r *NodeProvisionReconciler) cleanupGCPOnDelete(ctx context.Context, np *mlv1alpha1.NodeProvision) (res ctrl.Result, wait bool) {
	log := logf.FromContext(ctx)
	if id := np.Status.InstanceID; id != "" {
		if err := r.deleteGCPInstance(ctx, np, id); err != nil {
			log.Error(err, "GCE instance deletion failed — requeuing with delay", "instance", id)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true
		}
		log.Info("GCE instance deleted", "instance", id)
		// Clear InstanceID so a duplicate or requeued reconcile skips the delete.
		np.Status.InstanceID = ""
		if statusErr := r.Status().Update(ctx, np); statusErr != nil {
			log.Error(statusErr, "clearing InstanceID from status after deletion (non-fatal)")
		}
		return ctrl.Result{}, false
	}
	// No InstanceID was ever persisted, but an instance may have been launched
	// anyway (crash between the insert and the status write).
	if r.terminateOrphanedGCPInstance(ctx, np) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}
	return ctrl.Result{}, false
}
