package aws

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	corev1 "k8s.io/api/core/v1"
)

// MFA-backed STS session authentication
//
// AWS IAM supports two credential modes:
//
//  1. Static credentials — long-lived access key + secret, set directly in
//     the secret as awsAccessKeyId and awsSecretAccessKey.
//
//  2. MFA session — the same long-lived key pair is combined with a one-time
//     code from an already-registered MFA device to call sts:GetSessionToken,
//     which returns short-lived temporary credentials.  The controller caches
//     the resulting session token and automatically refreshes it before it
//     expires by generating a fresh TOTP code from the device's shared secret.
//
// Secret keys used for MFA session mode:
//
//	awsAccessKeyId      — IAM user long-lived access key ID
//	awsSecretAccessKey  — IAM user long-lived secret access key
//	mfaSerialNumber     — ARN of the registered MFA device,
//	                      e.g. arn:aws:iam::123456789012:mfa/mydevice
//	mfaTotpSecret       — Base32-encoded TOTP shared secret from the
//	                      authenticator app (enables automatic token refresh)
//	mfaDurationSeconds  — Optional session lifetime in seconds (900–129600,
//	                      default 43200 = 12 h)
//
// Keys written back by the controller after each successful sts:GetSessionToken:
//
//	awsSessionToken     — temporary session token
//	awsSessionExpiry    — RFC3339 expiry timestamp

const (
	secretKeySessionToken  = "awsSessionToken"
	secretKeySessionExpiry = "awsSessionExpiry"

	// Sessions expiring within this window are refreshed proactively.
	mfaSessionGrace = 5 * time.Minute
	// Default sts:GetSessionToken duration when mfaDurationSeconds is absent.
	mfaDefaultDuration = int32(43200) // 12 hours
)

// MFASession holds the temporary credentials returned by sts:GetSessionToken
// and the time at which they expire.
type MFASession struct {
	Credentials AWSCredentials
	Expiry      time.Time
}

// IsExpired reports whether the session has expired or will expire within the
// proactive-refresh grace window.
func (s MFASession) IsExpired() bool {
	return s.Expiry.IsZero() || time.Now().Add(mfaSessionGrace).After(s.Expiry)
}

// IsMFAConfigured reports whether the secret is set up for MFA session mode
// (both mfaSerialNumber and mfaTotpSecret must be present).
func IsMFAConfigured(secret *corev1.Secret) bool {
	return secretVal(secret, "mfaSerialNumber") != "" &&
		secretVal(secret, "mfaTotpSecret") != ""
}

// ResolveAWSCredentialsWithMFA returns ready-to-use AWS credentials, selecting
// the right mode based on what is present in the secret:
//
//   - Static mode: mfaSerialNumber or mfaTotpSecret absent → returns the
//     long-lived key pair (or any awsSessionToken already in the secret)
//     via the existing ResolveAWSCredentials path.
//
//   - MFA session mode: both mfaSerialNumber and mfaTotpSecret present →
//     checks the cached session token first; if still valid it is returned
//     directly (nil *MFASession).  If expired (or absent) a new TOTP code is
//     generated from mfaTotpSecret and sts:GetSessionToken is called.
//
// A non-nil *MFASession signals that fresh credentials were just obtained.
// The caller MUST write MFASession.Credentials.SessionToken and
// MFASession.Expiry back into the secret (keys awsSessionToken +
// awsSessionExpiry) so the next reconcile reuses the session without another
// STS round-trip.
func ResolveAWSCredentialsWithMFA(ctx context.Context, region string, secret *corev1.Secret) (AWSCredentials, *MFASession, error) {
	if !IsMFAConfigured(secret) {
		return ResolveAWSCredentials(secret), nil, nil
	}

	keyID     := secretVal(secret, "awsAccessKeyId")
	secretKey := secretVal(secret, "awsSecretAccessKey")
	serial    := secretVal(secret, "mfaSerialNumber")
	totpSeed  := secretVal(secret, "mfaTotpSecret")

	// ── Reuse cached session if still valid ─────────────────────────────────
	if token := secretVal(secret, secretKeySessionToken); token != "" {
		if rawExp := secretVal(secret, secretKeySessionExpiry); rawExp != "" {
			if exp, err := time.Parse(time.RFC3339, rawExp); err == nil {
				sess := MFASession{
					Credentials: AWSCredentials{
						AccessKeyID:     keyID,
						SecretAccessKey: secretKey,
						SessionToken:    token,
					},
					Expiry: exp,
				}
				if !sess.IsExpired() {
					return sess.Credentials, nil, nil
				}
			}
		}
	}

	// ── Obtain a fresh session via sts:GetSessionToken ───────────────────────
	tokenCode, err := generateTOTPCode(totpSeed, time.Now())
	if err != nil {
		return AWSCredentials{}, nil, fmt.Errorf("generating TOTP code for MFA refresh: %w", err)
	}

	duration := mfaDefaultDuration
	if v := secretVal(secret, "mfaDurationSeconds"); v != "" {
		if d, err := strconv.ParseInt(v, 10, 32); err == nil && d >= 900 && d <= 129600 {
			duration = int32(d)
		}
	}

	sess, err := getSTSSessionToken(ctx, region, keyID, secretKey, serial, tokenCode, duration)
	if err != nil {
		return AWSCredentials{}, nil, err
	}
	return sess.Credentials, &sess, nil
}

// getSTSSessionToken exchanges long-lived IAM credentials and a one-time MFA
// code for a set of temporary STS credentials via sts:GetSessionToken.
func getSTSSessionToken(
	ctx context.Context,
	region, keyID, secretKey, serialNumber, tokenCode string,
	durationSec int32,
) (MFASession, error) {
	sdkCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(keyID, secretKey, ""),
		),
	)
	if err != nil {
		return MFASession{}, fmt.Errorf("loading AWS config for STS: %w", err)
	}

	out, err := sts.NewFromConfig(sdkCfg).GetSessionToken(ctx, &sts.GetSessionTokenInput{
		DurationSeconds: awssdk.Int32(durationSec),
		SerialNumber:    awssdk.String(serialNumber),
		TokenCode:       awssdk.String(tokenCode),
	})
	if err != nil {
		return MFASession{}, fmt.Errorf("sts:GetSessionToken: %w", err)
	}
	if out.Credentials == nil {
		return MFASession{}, fmt.Errorf("sts:GetSessionToken returned empty credentials")
	}

	c := out.Credentials
	return MFASession{
		Credentials: AWSCredentials{
			AccessKeyID:     awssdk.ToString(c.AccessKeyId),
			SecretAccessKey: awssdk.ToString(c.SecretAccessKey),
			SessionToken:    awssdk.ToString(c.SessionToken),
		},
		Expiry: awssdk.ToTime(c.Expiration),
	}, nil
}

// generateTOTPCode computes an RFC 6238 TOTP code (6 digits, 30-second period)
// from a base32-encoded shared secret.  No external dependencies required.
func generateTOTPCode(seed string, t time.Time) (string, error) {
	seed = strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(seed), " ", ""))
	if pad := len(seed) % 8; pad != 0 {
		seed += strings.Repeat("=", 8-pad)
	}
	key, err := base32.StdEncoding.DecodeString(seed)
	if err != nil {
		return "", fmt.Errorf("invalid mfaTotpSecret (expected base32): %w", err)
	}

	// Counter = number of 30-second intervals since Unix epoch.
	counter := make([]byte, 8)
	binary.BigEndian.PutUint64(counter, uint64(math.Floor(float64(t.Unix())/30)))

	mac := hmac.New(sha1.New, key)
	mac.Write(counter)
	h := mac.Sum(nil)

	// Dynamic truncation per RFC 4226 §5.3.
	offset := h[len(h)-1] & 0x0f
	code := (uint32(h[offset]&0x7f)<<24 |
		uint32(h[offset+1])<<16 |
		uint32(h[offset+2])<<8 |
		uint32(h[offset+3])) % 1_000_000

	return fmt.Sprintf("%06d", code), nil
}

// secretVal reads a secret Data key and trims surrounding whitespace.
func secretVal(secret *corev1.Secret, key string) string {
	return strings.TrimSpace(string(secret.Data[key]))
}
