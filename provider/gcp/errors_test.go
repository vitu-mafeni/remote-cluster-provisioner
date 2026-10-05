package gcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Kind
	}{
		{"nil", nil, KindOther},
		{"not found", gerr(404, "notFound", "The resource 'projects/p/zones/z/instances/i' was not found"), KindNotFound},
		{"already exists", gerr(409, "alreadyExists", "The resource 'projects/p/zones/z/instances/i' already exists"), KindAlreadyExists},
		{"quota via reason", gerr(403, "quotaExceeded", "Quota 'CPUS' exceeded. Limit: 24.0 in region us-central1."), KindQuota},
		{"quota via message only", gerr(403, "", "Quota 'NVIDIA_T4_GPUS' exceeded. Limit: 0.0"), KindQuota},
		{"quota from a failed operation", gerr(403, "QUOTA_EXCEEDED", "QUOTA_EXCEEDED: Quota 'SSD_TOTAL_GB' exceeded"), KindQuota},
		{"stockout from a failed operation", gerr(503, "ZONE_RESOURCE_POOL_EXHAUSTED", "ZONE_RESOURCE_POOL_EXHAUSTED: The zone 'projects/p/zones/z' does not have enough resources available"), KindStockout},
		{"stockout without code", fmt.Errorf("does not have enough resources available to fulfill the request"), KindStockout},
		{"permission denied", gerr(403, "forbidden", "Required 'compute.instances.create' permission for 'projects/p/zones/z/instances/i'"), KindPermissionDenied},
		{"api not enabled", gerr(403, "accessNotConfigured", "Compute Engine API has not been used in project 1 before or it is disabled"), KindPermissionDenied},
		{"unauthenticated", gerr(401, "authError", "Invalid Credentials"), KindAuth},
		{"oauth retrieve error", &oauth2.RetrieveError{ErrorCode: "invalid_grant", ErrorDescription: "account not found"}, KindAuth},
		{"invalid_grant text", errors.New(`oauth2: cannot fetch token: 400 Bad Request: {"error":"invalid_grant"}`), KindAuth},
		{"rate limited 429", gerr(429, "rateLimitExceeded", "Too Many Requests"), KindTransient},
		{"rate limit as 403", gerr(403, "rateLimitExceeded", "Rate Limit Exceeded"), KindTransient},
		{"backend 503", gerr(503, "backendError", "The service is currently unavailable"), KindTransient},
		{"500", gerr(500, "internalError", "boom"), KindTransient},
		{"network timeout", timeoutErr{}, KindTransient},
		{"deadline", context.DeadlineExceeded, KindTransient},
		{"connection reset", errors.New("read tcp: connection reset by peer"), KindTransient},
		{"bad request", gerr(400, "invalid", "Invalid value for field 'resource.machineType'"), KindOther},
		{"plain error", errors.New("boom"), KindOther},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("%s: Classify = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestClassifyThroughWrappers(t *testing.T) {
	inner := gerr(404, "notFound", "nope")
	for name, err := range map[string]error{
		"fmt.Errorf %w": fmt.Errorf("describing instance i: %w", inner),
		"apiErrorf":     apiErrorf(inner, "looking up %s", "x"),
		"double wrap":   fmt.Errorf("outer: %w", apiErrorf(inner, "inner")),
	} {
		if !IsNotFound(err) {
			t.Errorf("%s: wrapped NotFound must still classify as NotFound", name)
		}
	}
	if !IsQuotaExceeded(apiErrorf(gerr(403, "quotaExceeded", "Quota 'CPUS' exceeded"), "launch")) {
		t.Error("wrapped quota error must classify as quota")
	}
}

func TestPredicates(t *testing.T) {
	if !IsAlreadyExists(gerr(409, "alreadyExists", "x")) || IsAlreadyExists(gerr(404, "", "x")) {
		t.Error("IsAlreadyExists")
	}
	if !IsPermissionDenied(gerr(403, "forbidden", "denied")) || IsPermissionDenied(gerr(403, "quotaExceeded", "Quota 'CPUS' exceeded")) {
		t.Error("a quota 403 is quota, not permission denied")
	}
	if !IsStockout(gerr(503, "", "ZONE_RESOURCE_POOL_EXHAUSTED")) {
		t.Error("IsStockout")
	}
	if !IsAuthFailure(gerr(401, "", "x")) {
		t.Error("IsAuthFailure")
	}
	if !IsTransient(gerr(503, "", "x")) || IsTransient(gerr(404, "", "x")) {
		t.Error("IsTransient")
	}
}

func TestDescribeAddsAdviceAndKeepsOriginal(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{gerr(403, "quotaExceeded", "Quota 'CPUS' exceeded"), "quota"},
		{gerr(403, "forbidden", "Required 'compute.instances.create' permission"), "roles/compute.instanceAdmin.v1"},
		{gerr(503, "", "ZONE_RESOURCE_POOL_EXHAUSTED"), "another zone"},
		{gerr(401, "", "bad"), "credentials"},
	} {
		d := Describe(tc.err)
		if !strings.Contains(d, tc.want) || !strings.Contains(d, tc.err.Error()) {
			t.Errorf("Describe(%v) = %q, want advice %q and the original text", tc.err, d, tc.want)
		}
	}
	if Describe(nil) != "" || Describe(errors.New("x")) != "x" {
		t.Error("Describe of nil/unclassified errors")
	}
}
