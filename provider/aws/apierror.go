package aws

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/aws/smithy-go"
)

// AWS SDK errors carry identifiers that must not reach operator-visible text
// (NodeProvision status, events): IAM ARNs and account IDs ("User: arn:aws:iam::
// 123456789012:user/x is not authorized ..."), request IDs and the opaque
// encoded authorization failure blob.
var (
	reARN = regexp.MustCompile(`arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:[0-9]{0,12}:[^\s,;"')\]]+`)
	// "RequestID: <id>", "request id: <id>", "Request ID <id>".
	reRequestID = regexp.MustCompile(`(?i)(\brequest[ _-]?id[:=]?\s+)[A-Za-z0-9-]+`)
	// "Encoded authorization failure message: <blob>".
	reEncodedAuth = regexp.MustCompile(`(?i)(encoded authorization failure message:?\s+)\S+`)
	// "account 123456789012", "account id: 123456789012", "account-id=123456789012".
	reAccountID = regexp.MustCompile(`(?i)(\baccount(?:[ _-]?id)?[:= ]+)[0-9]{12}\b`)
	// AWS access key IDs.
	reAccessKeyID = regexp.MustCompile(`\bA[KS]IA[0-9A-Z]{16}\b`)
)

const redactedID = "[REDACTED]"

// RedactIdentifiers masks AWS account/credential identifiers in s so it can be
// shown to operators or stored in a status message.
func RedactIdentifiers(s string) string {
	if s == "" {
		return s
	}
	s = reARN.ReplaceAllString(s, "[REDACTED-ARN]")
	s = reEncodedAuth.ReplaceAllString(s, "${1}"+redactedID)
	s = reRequestID.ReplaceAllString(s, "${1}"+redactedID)
	s = reAccountID.ReplaceAllString(s, "${1}"+redactedID)
	s = reAccessKeyID.ReplaceAllString(s, redactedID)
	return s
}

// describeAPIError renders err for operators: for an AWS API error just
// "<Code>: <Message>" (no operation name, endpoint or request ID), otherwise the
// redacted error text.
func describeAPIError(err error) string {
	if err == nil {
		return ""
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if msg := apiErr.ErrorMessage(); msg != "" {
			return RedactIdentifiers(apiErr.ErrorCode() + ": " + msg)
		}
		return RedactIdentifiers(apiErr.ErrorCode())
	}
	return RedactIdentifiers(err.Error())
}

// sanitizedError is an operator-safe message that keeps the original error
// reachable for errors.Is/As (so callers can still classify it) without ever
// printing it.
type sanitizedError struct {
	msg   string
	cause error
}

func (e *sanitizedError) Error() string { return e.msg }
func (e *sanitizedError) Unwrap() error { return e.cause }

// apiErrorf formats "<what>: <sanitized cause>" and keeps cause unwrappable.
func apiErrorf(cause error, format string, args ...any) error {
	return &sanitizedError{msg: fmt.Sprintf(format, args...) + ": " + describeAPIError(cause), cause: cause}
}
