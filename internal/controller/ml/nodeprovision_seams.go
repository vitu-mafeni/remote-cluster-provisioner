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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mlv1alpha1 "dcn.ssu.ac.kr/infra/api/ml/v1alpha1"
	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
	"dcn.ssu.ac.kr/infra/pkg/ssh"
	awsprovision "dcn.ssu.ac.kr/infra/provider/aws"
	remotenodeprovision "dcn.ssu.ac.kr/infra/provider/onprem"
)

// ────────────────────────────────────────────────────────────────────────────
// Replaceable external calls (seams)
//
// Everything the controller does against AWS, the VPN server or a physical
// node goes through one of the hooks below. Production leaves them nil and gets
// the real implementation; unit tests replace them so no network is needed.
// ────────────────────────────────────────────────────────────────────────────

// awsAPI groups the AWS calls the controller makes. A nil field means the real
// awsprovision function.
type awsAPI struct {
	FindInstanceID    func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds awsprovision.AWSCredentials) (string, error)
	TerminateInstance func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds awsprovision.AWSCredentials, instanceID string) error
	ProvisionEC2Node  func(ctx context.Context, np *mlv1alpha1.NodeProvision, creds awsprovision.AWSCredentials,
		vpn *ssh.Client, nc *mlv1alpha1.NodeProvisionNetConfig, rt pkgruntime.Config) (*awsprovision.ProvisionResult, error)
}

// aws returns the AWS calls with defaults filled in.
func (r *NodeProvisionReconciler) aws() awsAPI {
	a := r.awsOverrides
	if a.FindInstanceID == nil {
		a.FindInstanceID = awsprovision.FindInstanceIDByNodeProvision
	}
	if a.TerminateInstance == nil {
		a.TerminateInstance = awsprovision.TerminateInstance
	}
	if a.ProvisionEC2Node == nil {
		a.ProvisionEC2Node = awsprovision.ProvisionEC2Node
	}
	return a
}

// vpnServer is the slice of the WireGuard server the peer cleanup needs.
type vpnServer interface {
	// ReadPeers returns the live peers as VPN IP -> public key.
	ReadPeers() (map[string]string, error)
	// RemovePeer removes the peer from the running config and from wg0.conf.
	// publicKey has already been validated as a WireGuard key.
	RemovePeer(publicKey string) error
	Close() error
}

// sshVPNServer is the real vpnServer, driven over SSH.
type sshVPNServer struct{ client *ssh.Client }

func (s sshVPNServer) ReadPeers() (map[string]string, error) {
	return remotenodeprovision.ReadVPNServerPeers(s.client)
}

func (s sshVPNServer) RemovePeer(publicKey string) error {
	var firstErr error
	removeCmd := fmt.Sprintf("sudo wg set wg0 peer %s remove", ssh.ShellQuote(publicKey))
	if _, err := ssh.Run(s.client, removeCmd); err != nil {
		firstErr = fmt.Errorf("removing WireGuard peer from running config: %w", err)
	}
	if err := remotenodeprovision.RemoveVPNPeerFromServerConf(s.client, publicKey); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("removing peer block from wg0.conf on VPN server: %w", err)
	}
	return firstErr
}

func (s sshVPNServer) Close() error { return s.client.Conn.Close() }

// dialVPN connects to the VPN server described by nc.
func (r *NodeProvisionReconciler) dialVPN(ctx context.Context, nc *mlv1alpha1.NodeProvisionNetConfig) (vpnServer, error) {
	if r.dialVPNServer != nil {
		return r.dialVPNServer(ctx, nc)
	}
	c, err := r.getVPNServerSSHClient(ctx, nc)
	if err != nil {
		return nil, err
	}
	return sshVPNServer{client: c}, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Node reset on deletion: run once per deletion
// ────────────────────────────────────────────────────────────────────────────

// nodeResetDoneAnnotation marks a terminating NodeProvision whose node-side
// reset script has already run. Deletion is retried (e.g. while the VPN peer
// cannot be released) and the reset must not be replayed on every retry.
const nodeResetDoneAnnotation = "ml.dcn.ssu.ac.kr/node-reset-done"

// nodeResetDone reports whether the reset already ran for np: the annotation
// survives a controller restart, the in-memory set covers a failed annotation
// write.
func (r *NodeProvisionReconciler) nodeResetDone(np *mlv1alpha1.NodeProvision) bool {
	if np.Annotations[nodeResetDoneAnnotation] != "" {
		return true
	}
	_, ok := r.nodeResetUIDs.Load(np.UID)
	return ok
}

// markNodeResetDone records that the reset ran (best effort on the annotation).
func (r *NodeProvisionReconciler) markNodeResetDone(ctx context.Context, np *mlv1alpha1.NodeProvision) {
	r.nodeResetUIDs.Store(np.UID, struct{}{})
	base := np.DeepCopy()
	if np.Annotations == nil {
		np.Annotations = map[string]string{}
	}
	np.Annotations[nodeResetDoneAnnotation] = time.Now().UTC().Format(time.RFC3339)
	if err := r.Patch(ctx, np, client.MergeFrom(base)); err != nil {
		logf.FromContext(ctx).Error(err, "recording node-reset-done annotation (in-memory marker still prevents a rerun)")
		np.Annotations = base.Annotations
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Manager context
// ────────────────────────────────────────────────────────────────────────────

// setManagerContext stores the manager's root context (called from Start).
func (r *NodeProvisionReconciler) setManagerContext(ctx context.Context) {
	r.mgrMu.Lock()
	r.mgrCtx = ctx
	r.mgrMu.Unlock()
}

// baseContext is the parent of background goroutines: the manager's context
// once Start has run (so they stop on shutdown), context.Background before.
func (r *NodeProvisionReconciler) baseContext() context.Context {
	r.mgrMu.RLock()
	defer r.mgrMu.RUnlock()
	if r.mgrCtx == nil {
		return context.Background()
	}
	return r.mgrCtx
}

// apiReader is the uncached reader (or the cached client when none is set).
func (r *NodeProvisionReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
