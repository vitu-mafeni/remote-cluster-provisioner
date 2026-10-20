package gcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
)

// ErrInstanceAlreadyLaunched is returned by ProvisionInstance when the instance
// insert reports that an instance with this NodeProvision's deterministic name
// already exists (a launch whose response was lost, or a concurrent reconcile).
// Any VPN peer registered for the rejected attempt has been released. The caller
// should requeue WITHOUT counting a failure: the next reconcile finds and adopts
// the instance by name and UID label.
var ErrInstanceAlreadyLaunched = errors.New("a GCE instance for this NodeProvision was already launched; adopt it instead of launching another")

// ErrInstanceNameTaken is returned when an instance with the deterministic name
// exists but was not launched for this NodeProvision (its UID label differs).
// It is never adopted or deleted; the operator must resolve the name clash.
var ErrInstanceNameTaken = errors.New("a GCE instance with this name already exists in the zone but belongs to something else")

// Kind classifies a GCP API error.
type Kind int

// Error kinds, see Classify.
const (
	KindOther Kind = iota
	KindNotFound
	KindAlreadyExists
	KindQuota
	KindStockout
	KindPermissionDenied
	KindAuth
	KindTransient
)

func (k Kind) String() string {
	switch k {
	case KindNotFound:
		return "NotFound"
	case KindAlreadyExists:
		return "AlreadyExists"
	case KindQuota:
		return "QuotaExceeded"
	case KindStockout:
		return "ResourceExhausted"
	case KindPermissionDenied:
		return "PermissionDenied"
	case KindAuth:
		return "AuthFailure"
	case KindTransient:
		return "Transient"
	default:
		return "Other"
	}
}

// httpCode returns the HTTP status carried by err (0 when none). The REST
// client wraps *googleapi.Error in an apierror.APIError that unwraps to it;
// long-running-operation failures are surfaced the same way with the
// operation's HTTP status.
func httpCode(err error) int {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code
	}
	return 0
}

func errText(err error) string {
	var sb strings.Builder
	sb.WriteString(strings.ToLower(err.Error()))
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		for _, e := range ge.Errors {
			sb.WriteString(" ")
			sb.WriteString(strings.ToLower(e.Reason))
			sb.WriteString(" ")
			sb.WriteString(strings.ToLower(e.Message))
		}
	}
	return sb.String()
}

// Classify maps err to a Kind. The checks are ordered by specificity: quota and
// stockout errors arrive as 403/429/503 with a distinguishing reason, so they
// are tested before the generic status codes.
func Classify(err error) Kind {
	if err == nil {
		return KindOther
	}
	text := errText(err)
	code := httpCode(err)

	switch {
	case strings.Contains(text, "quota_exceeded"), strings.Contains(text, "quotaexceeded"),
		strings.Contains(text, "quota '") && strings.Contains(text, "exceeded"),
		strings.Contains(text, "exceeded quota"), strings.Contains(text, "limit exceeded for resource"):
		return KindQuota
	case strings.Contains(text, "zone_resource_pool_exhausted"), strings.Contains(text, "resource pool exhausted"),
		strings.Contains(text, "does not have enough resources"), strings.Contains(text, "stockout"),
		strings.Contains(text, "resource_pool_exhausted"):
		return KindStockout
	}

	if strings.Contains(text, "ratelimitexceeded") || strings.Contains(text, "userratelimitexceeded") {
		return KindTransient
	}

	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		return KindAuth
	}
	if strings.Contains(text, "invalid_grant") || strings.Contains(text, "invalid jwt") ||
		strings.Contains(text, "could not find default credentials") {
		return KindAuth
	}

	switch code {
	case 404:
		return KindNotFound
	case 409:
		return KindAlreadyExists
	case 401:
		return KindAuth
	case 403:
		return KindPermissionDenied
	case 429, 500, 502, 503, 504:
		return KindTransient
	}
	if code == 0 {
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne) ||
			strings.Contains(text, "connection reset") || strings.Contains(text, "connection refused") ||
			strings.Contains(text, "unexpected eof") || strings.Contains(text, "tls handshake timeout") {
			return KindTransient
		}
	}
	return KindOther
}

// IsNotFound reports whether err means the resource does not exist.
func IsNotFound(err error) bool { return Classify(err) == KindNotFound }

// IsAlreadyExists reports whether err is a 409 conflict (resource exists).
func IsAlreadyExists(err error) bool { return Classify(err) == KindAlreadyExists }

// IsQuotaExceeded reports whether err is a project quota error.
func IsQuotaExceeded(err error) bool { return Classify(err) == KindQuota }

// IsStockout reports whether the zone has no capacity for the requested shape.
func IsStockout(err error) bool { return Classify(err) == KindStockout }

// IsPermissionDenied reports whether the service account lacks a permission or
// API (a quota 403 is reported as quota, not permission).
func IsPermissionDenied(err error) bool { return Classify(err) == KindPermissionDenied }

// IsAuthFailure reports whether the credentials were rejected (bad/expired/
// revoked key, disabled service account).
func IsAuthFailure(err error) bool { return Classify(err) == KindAuth }

// IsTransient reports whether retrying the same call later may succeed
// (429, 5xx, connection errors, timeouts).
func IsTransient(err error) bool { return Classify(err) == KindTransient }

// Describe annotates err with actionable advice for operator-facing messages
// (Status.Message). The original error text is kept.
func Describe(err error) string {
	if err == nil {
		return ""
	}
	switch Classify(err) {
	case KindQuota:
		return fmt.Sprintf("GCP quota exceeded (raise the quota or pick another region/machine type): %v", err)
	case KindStockout:
		return fmt.Sprintf("the zone has no capacity for this machine type/GPU right now (try another zone): %v", err)
	case KindPermissionDenied:
		return fmt.Sprintf("permission denied (the service account needs roles/compute.instanceAdmin.v1 and roles/compute.securityAdmin or equivalent, and the Compute Engine API must be enabled): %v", err)
	case KindAuth:
		return fmt.Sprintf("GCP rejected the service-account credentials (key deleted, disabled or malformed): %v", err)
	}
	return err.Error()
}

// apiError is an operator-facing message that keeps the underlying API error
// reachable (errors.As / Classify still see the *googleapi.Error).
type apiError struct {
	msg   string
	cause error
}

func (e *apiError) Error() string { return e.msg }
func (e *apiError) Unwrap() error { return e.cause }

// apiErrorf formats "<what>: <Describe(cause)>" and keeps cause unwrappable.
func apiErrorf(cause error, format string, args ...any) error {
	return &apiError{msg: fmt.Sprintf(format, args...) + ": " + Describe(cause), cause: cause}
}
