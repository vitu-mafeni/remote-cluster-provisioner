package aws

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// gatedClient wraps a fake client: Get records how many calls are in flight at
// once and (for the first call) blocks until released, so a test can interleave
// Evict and further refreshes with a refresh that is in progress.
type gatedClient struct {
	client.Client
	active, maxActive, gets atomic.Int32
	entered                 chan struct{}
	release                 chan struct{}
	once                    sync.Once
}

func (g *gatedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	n := g.active.Add(1)
	for {
		m := g.maxActive.Load()
		if n <= m || g.maxActive.CompareAndSwap(m, n) {
			break
		}
	}
	g.gets.Add(1)
	defer g.active.Add(-1)
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return g.Client.Get(ctx, key, obj, opts...)
}

func staticSecret(data map[string]string) *corev1.Secret {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "creds"}, Data: map[string][]byte{}}
	for k, v := range data {
		s.Data[k] = []byte(v)
	}
	return s
}

// Evict used to delete the per-key mutex even while a refresh held it, so the
// next refresh created a fresh mutex and ran CONCURRENTLY with the first (a TOTP
// code reused inside its 30 s window is rejected by AWS).
func TestEvictDuringRefreshDoesNotAllowConcurrentRefresh(t *testing.T) {
	secret := staticSecret(map[string]string{"awsAccessKeyId": "AKIAEXAMPLE", "awsSecretAccessKey": "s3cr3t-value"})
	g := &gatedClient{
		Client:  fake.NewClientBuilder().WithObjects(secret).Build(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	m := NewCredentialManager(g, testLogger())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = m.Get(context.Background(), "ns", "creds", "us-east-1") }()
	<-g.entered // refresh #1 holds the key lock and is blocked in client.Get

	m.Evict("ns", "creds")
	go func() { defer wg.Done(); _, _ = m.Get(context.Background(), "ns", "creds", "us-east-1") }()
	time.Sleep(200 * time.Millisecond) // give refresh #2 the chance to (wrongly) start
	close(g.release)
	wg.Wait()

	if got := g.maxActive.Load(); got != 1 {
		t.Fatalf("refreshes of the same key ran concurrently (max %d in flight)", got)
	}
	// The waiter re-checks the cache after taking the lock and reuses the winner's result.
	if got := g.gets.Load(); got != 1 {
		t.Errorf("secret fetched %d times, want 1 (second caller should reuse the first refresh)", got)
	}
}

func TestConcurrentGetsRefreshOnce(t *testing.T) {
	secret := staticSecret(map[string]string{"awsAccessKeyId": "AKIAEXAMPLE", "awsSecretAccessKey": "s3cr3t-value"})
	g := &gatedClient{
		Client:  fake.NewClientBuilder().WithObjects(secret).Build(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	m := NewCredentialManager(g, testLogger())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = m.Get(context.Background(), "ns", "creds", "r") }()
	}
	<-g.entered
	time.Sleep(100 * time.Millisecond)
	close(g.release)
	wg.Wait()
	if g.gets.Load() != 1 || g.maxActive.Load() != 1 {
		t.Errorf("gets=%d maxActive=%d, want a single serialized refresh", g.gets.Load(), g.maxActive.Load())
	}
}

func mfaSecret(token string, exp time.Time) *corev1.Secret {
	return staticSecret(map[string]string{
		"awsAccessKeyId":     "AKIAEXAMPLE",
		"awsSecretAccessKey": "s3cr3t-value",
		"mfaSerialNumber":    "arn:aws:iam::123456789012:mfa/dev",
		"mfaTotpSecret":      "JBSWY3DPEHPK3PXP",
		"awsSessionToken":    token,
		"awsSessionExpiry":   exp.UTC().Format(time.RFC3339),
	})
}

// A cached MFA session read from the secret must be cached with its REAL expiry,
// not a synthetic now+15min that outlives it.
func TestCachedMFASessionHonoursRealExpiry(t *testing.T) {
	exp := time.Now().Add(7 * time.Minute) // outside the 5 min grace, so it is reused as-is
	m := NewCredentialManager(fake.NewClientBuilder().WithObjects(mfaSecret("SESSIONTOKEN123", exp)).Build(), testLogger())

	creds, err := m.Get(context.Background(), "ns", "creds", "r")
	if err != nil || creds.SessionToken != "SESSIONTOKEN123" {
		t.Fatalf("creds=%+v err=%v", creds, err)
	}
	m.mu.RLock()
	entry := m.cache[credKey{"ns", "creds", "r"}]
	m.mu.RUnlock()
	if entry == nil {
		t.Fatal("nothing cached")
	}
	if d := entry.expiry.Sub(exp.Truncate(time.Second)); d < -time.Second || d > time.Second {
		t.Errorf("cached expiry %v does not match the session's real expiry %v", entry.expiry, exp)
	}
	// Two minutes later the session is within grace of its real expiry and must be refreshed,
	// whereas the old synthetic TTL would still have served it.
	if !entry.needsRefresh(m.grace + 3*time.Minute) {
		t.Error("entry must need a refresh once within grace of the real expiry")
	}
}

func TestSessionTokenWithoutKnownExpiryGetsSyntheticTTL(t *testing.T) {
	s := staticSecret(map[string]string{"awsAccessKeyId": "AKIAEXAMPLE", "awsSecretAccessKey": "s3cr3t-value", "awsSessionToken": "TOKEN-NO-EXPIRY"})
	m := NewCredentialManager(fake.NewClientBuilder().WithObjects(s).Build(), testLogger())
	if _, err := m.Get(context.Background(), "ns", "creds", "r"); err != nil {
		t.Fatal(err)
	}
	m.mu.RLock()
	e := m.cache[credKey{"ns", "creds", "r"}]
	m.mu.RUnlock()
	if e.expiry.IsZero() || time.Until(e.expiry) > unknownExpiryTTL+m.grace+time.Minute {
		t.Errorf("unknown-expiry session must get the synthetic TTL, got %v", e.expiry)
	}
	// An expiry recorded for a DIFFERENT token must not be applied to this one.
	s2 := mfaSecret("OTHER", time.Now().Add(time.Hour))
	if _, ok := secretSessionExpiry(s2, "SOMETHING-ELSE"); ok {
		t.Error("expiry of another token must be ignored")
	}
}
