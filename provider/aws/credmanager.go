package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CredentialManager maintains a live in-memory cache of resolved AWS
// credentials for each credential secret and proactively refreshes MFA-backed
// (or any STS-issued) sessions before they expire.
//
// Register it with the controller manager so the background loop runs:
//
//	mgr.Add(credMgr)
//
// Reconcilers call Get instead of resolving credentials inline; the background
// goroutine ensures the cached session is always fresh even between reconciles.
type CredentialManager struct {
	client   client.Client
	mu       sync.RWMutex
	cache    map[credKey]*credEntry
	interval time.Duration // how often the background loop scans for near-expiry entries
	grace    time.Duration // refresh when this close to expiry
	log      logr.Logger
}

type credKey struct{ namespace, name, region string }

type credEntry struct {
	creds  AWSCredentials
	expiry time.Time // zero → static credentials (never expire)
}

func (e *credEntry) needsRefresh(grace time.Duration) bool {
	return !e.expiry.IsZero() && time.Now().Add(grace).After(e.expiry)
}

// NewCredentialManager returns a CredentialManager ready to be added to a
// controller-runtime Manager.
func NewCredentialManager(c client.Client, log logr.Logger) *CredentialManager {
	return &CredentialManager{
		client:   c,
		cache:    make(map[credKey]*credEntry),
		interval: 2 * time.Minute,
		grace:    mfaSessionGrace, // 5 minutes — same window as the MFA session grace
		log:      log,
	}
}

// Get returns AWS credentials for the given secret.
//
// On a cache hit with a session that is not near expiry the cached credentials
// are returned immediately (no API calls).  On a cache miss or when the
// session is within the grace window the secret is fetched from Kubernetes,
// fresh credentials are resolved (generating a TOTP code and calling
// sts:GetSessionToken / sts:AssumeRole as needed), the new session token is
// written back into the secret, and the cache is updated.
func (m *CredentialManager) Get(ctx context.Context, namespace, name, region string) (AWSCredentials, error) {
	key := credKey{namespace, name, region}

	m.mu.RLock()
	entry, cached := m.cache[key]
	m.mu.RUnlock()

	if cached && !entry.needsRefresh(m.grace) {
		return entry.creds, nil
	}
	return m.refresh(ctx, key)
}

// Evict removes the cache entry for the given secret.  Call it when the
// associated resource (NodeProvision, etc.) is deleted to prevent the
// background loop from repeatedly attempting to refresh a deleted secret.
func (m *CredentialManager) Evict(namespace, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.cache {
		if k.namespace == namespace && k.name == name {
			delete(m.cache, k)
		}
	}
}

// Start implements controller-runtime's Runnable.  It runs the background
// refresh loop until ctx is cancelled.
func (m *CredentialManager) Start(ctx context.Context) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m.refreshExpiring(ctx)
		}
	}
}

// NeedLeaderElection implements controller-runtime's LeaderElectionRunnable.
// Returns true so the refresher only runs on the elected leader, matching the
// reconciler's own leader-election behaviour.
func (m *CredentialManager) NeedLeaderElection() bool { return true }

// refresh runs the full credential resolution pipeline for the given secret:
//
//  1. Fetch the current secret from Kubernetes.
//  2. If roleArn is present: check the persisted role session in the secret
//     first — if it is still valid, skip both GetSessionToken and AssumeRole.
//  3. Resolve base credentials via ResolveAWSCredentialsWithMFA (handles
//     static key pair, pre-obtained session token, and MFA-backed
//     GetSessionToken with auto-generated TOTP codes).
//  4. If roleArn is present: call sts:AssumeRole with the base credentials;
//     persist the role session back into the secret.
//     If roleArn is absent: persist any new GetSessionToken session.
//  5. Update the in-memory cache with the final credentials and expiry.
//
// Callers must not hold m.mu.
func (m *CredentialManager) refresh(ctx context.Context, key credKey) (AWSCredentials, error) {
	secret := &corev1.Secret{}
	if err := m.client.Get(ctx, client.ObjectKey{
		Namespace: key.namespace,
		Name:      key.name,
	}, secret); err != nil {
		return AWSCredentials{}, fmt.Errorf("fetching credential secret %s/%s: %w", key.namespace, key.name, err)
	}

	roleArn := secretVal(secret, "roleArn")

	// ── Fast path: reuse persisted role session from the secret ─────────────
	// This avoids both a GetSessionToken and an AssumeRole call on restarts
	// or when the in-memory cache was evicted.
	if roleArn != "" {
		if creds, exp, ok := CachedRoleSession(secret, m.grace); ok {
			m.mu.Lock()
			m.cache[key] = &credEntry{creds: creds, expiry: exp}
			m.mu.Unlock()
			return creds, nil
		}
	}

	// ── Step 1: resolve base credentials ────────────────────────────────────
	// Handles: static key pair / pre-obtained session token / MFA GetSessionToken.
	baseCreds, mfaSession, err := ResolveAWSCredentialsWithMFA(ctx, key.region, secret)
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("resolving base AWS credentials from %s/%s: %w", key.namespace, key.name, err)
	}

	// ── Step 2a (no AssumeRole): cache base / MFA session ───────────────────
	if roleArn == "" {
		entry := &credEntry{creds: baseCreds}
		if mfaSession != nil {
			entry.expiry = mfaSession.Expiry
			m.patchSecret(ctx, secret, map[string]string{
				secretKeySessionToken:  mfaSession.Credentials.SessionToken,
				secretKeySessionExpiry: mfaSession.Expiry.UTC().Format(time.RFC3339),
			}, "MFA/GetSessionToken session refreshed", key)
		}
		m.mu.Lock()
		m.cache[key] = entry
		m.mu.Unlock()
		return baseCreds, nil
	}

	// ── Step 2b: AssumeRole with base credentials ────────────────────────────
	sessionName := secretVal(secret, "roleSessionName")
	externalID := secretVal(secret, "externalId")
	durSec := parseRoleDuration(secretVal(secret, "roleDurationSeconds"))

	roleSession, err := AssumeRole(ctx, key.region, baseCreds, roleArn, sessionName, externalID, durSec)
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("assuming role %s: %w", roleArn, err)
	}

	// Persist role session so restarts skip the full chain until it expires.
	m.patchSecret(ctx, secret, map[string]string{
		secretKeyRoleAccessKeyID:     roleSession.Credentials.AccessKeyID,
		secretKeyRoleSecretAccessKey: roleSession.Credentials.SecretAccessKey,
		secretKeyRoleSessionToken:    roleSession.Credentials.SessionToken,
		secretKeyRoleSessionExpiry:   roleSession.Expiry.UTC().Format(time.RFC3339),
	}, "AssumeRole session refreshed", key)

	m.mu.Lock()
	m.cache[key] = &credEntry{creds: roleSession.Credentials, expiry: roleSession.Expiry}
	m.mu.Unlock()
	return roleSession.Credentials, nil
}

// patchSecret writes the given key→value pairs into secret.Data and patches
// the secret in Kubernetes.  Errors are non-fatal (logged only).
func (m *CredentialManager) patchSecret(ctx context.Context, secret *corev1.Secret, data map[string]string, logMsg string, key credKey) {
	patch := secret.DeepCopy()
	if patch.Data == nil {
		patch.Data = map[string][]byte{}
	}
	for k, v := range data {
		patch.Data[k] = []byte(v)
	}
	if err := m.client.Patch(ctx, patch, client.MergeFrom(secret)); err != nil {
		m.log.Error(err, "persisting AWS session to secret (non-fatal)",
			"secret", key.name, "namespace", key.namespace)
	} else {
		m.log.Info(logMsg, "secret", key.name, "namespace", key.namespace)
	}
}

// parseRoleDuration converts a string seconds value to int32 for AssumeRole.
// Returns roleDefaultDuration (1 h) when the value is absent or out of range.
func parseRoleDuration(s string) int32 {
	if s == "" {
		return 0 // let AssumeRole apply its own default
	}
	var d int64
	if _, err := fmt.Sscan(s, &d); err != nil || d < 900 || d > 43200 {
		return 0
	}
	return int32(d)
}

// refreshExpiring scans the cache for entries that are near expiry and
// refreshes them.  Called by the background loop.
func (m *CredentialManager) refreshExpiring(ctx context.Context) {
	m.mu.RLock()
	var toRefresh []credKey
	for k, e := range m.cache {
		if e.needsRefresh(m.grace) {
			toRefresh = append(toRefresh, k)
		}
	}
	m.mu.RUnlock()

	for _, key := range toRefresh {
		if _, err := m.refresh(ctx, key); err != nil {
			m.log.Error(err, "background AWS credential refresh failed",
				"secret", key.name, "namespace", key.namespace)
			// If the secret was deleted, evict the stale entry so we stop
			// retrying on every tick.
			var apiErr *apierrors.StatusError
			if errors.As(err, &apiErr) && apierrors.IsNotFound(apiErr) {
				m.mu.Lock()
				delete(m.cache, key)
				m.mu.Unlock()
			}
		}
	}
}
