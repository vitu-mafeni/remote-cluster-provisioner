package ssh

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// maxErrorOutput is how much of a failing step's output is embedded in a
// returned error (and therefore, potentially, in CR status messages / logs).
const maxErrorOutput = 2048

const redacted = "[REDACTED]"

// ShellQuote returns s quoted for safe use as a single word in a POSIX shell
// command (single quotes, with embedded single quotes escaped). The result is
// safe in every shell context except inside another quoted string.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// secretKeyWords are the (lower-case) words that mark a credential. They are
// matched case-insensitively. "pwd" is deliberately absent: it is a very common
// non-secret (working directory) key.
const secretKeyWords = `password|passwd|passphrase|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|preshared[_-]?key|credentials?`

// secretValue matches one value: a double-quoted string (with escapes), a
// single-quoted string, or a run of non-space characters. Quoted values may
// contain spaces and are redacted completely.
const secretValue = `(?:"(?:[^"\\]|\\.)*"|'[^']*'|\S+)`

// secretPatterns match well-known credential shapes that may appear in remote
// command output even when the caller did not list them explicitly. Patterns
// with a capture group keep group 1 (the key and separator) and replace the
// rest.
var secretPatterns = []*regexp.Regexp{
	// Command-line flags: --password x, --registry-token=x, --token 'a b'. The
	// flag's LAST dash-separated word must be the keyword, so --token-ttl and
	// --discovery-token-ca-cert-hash are not touched, and --password-stdin
	// (which takes no value) is left alone.
	regexp.MustCompile(`(?i)(--?(?:[A-Za-z0-9]+-)*(?:` + secretKeyWords + `)(?:[ \t]+|=))` + secretValue),
	// key=value / key: value / "key": "value" / export KEY="value", where the
	// key ends in a keyword (PGPASSWORD, MY_TOKEN, awsSecretAccessKey,
	// PrivateKey = ...). Only horizontal whitespace around the separator, so a
	// key with an empty value never swallows the next line.
	regexp.MustCompile(`(?i)([A-Za-z0-9_.-]*(?:` + secretKeyWords + `)["']?[ \t]*[:=][ \t]*)` + secretValue),
	// HTTP credentials.
	regexp.MustCompile(`(?i)(authorization["']?[ \t]*[:=][ \t]*)(?:(?:bearer|basic|token)[ \t]+)?` + secretValue),
	regexp.MustCompile(`(?i)(\bbearer[ \t]+)[A-Za-z0-9._~+/=-]{20,}`),
	// kubeadm bootstrap token (id.secret) anywhere in the text.
	regexp.MustCompile(`\b[a-z0-9]{6}\.[a-z0-9]{16}\b`),
	// Well-known token shapes: AWS access key ids, GitHub tokens.
	regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{30,}\b`),
	regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----`),
}

// RedactSecrets removes each non-trivial value in secrets from s, together
// with well-known credential patterns: key/value pairs whose key names a
// password/token/secret/private key (shell assignments, YAML, JSON, CLI flags,
// WireGuard PrivateKey lines), Authorization/Bearer credentials, kubeadm
// bootstrap tokens and PEM private keys. Quoted values are redacted whole.
func RedactSecrets(s string, secrets ...string) string {
	for _, sec := range secrets {
		sec = strings.TrimSpace(sec)
		if len(sec) < 4 {
			continue // too short to be meaningfully secret; avoids mangling output
		}
		s = strings.ReplaceAll(s, sec, redacted)
	}
	for _, re := range secretPatterns {
		s = redactMatches(re, s)
	}
	return s
}

// Tail returns at most the last n bytes of s (cut on a rune boundary),
// prefixed with a truncation marker when something was dropped.
func Tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := len(s) - n
	for cut < len(s) && !utf8.RuneStart(s[cut]) {
		cut++
	}
	return "...(truncated)...\n" + s[cut:]
}

// StepError builds the error returned when a remote step fails. It names the
// step (label) and includes the last ~2KB of the scrubbed step OUTPUT. The
// command text itself is deliberately never included: commands routinely embed
// registry tokens and WireGuard private keys, and errors end up in CR status
// messages and controller logs. Pass known secret values so they are scrubbed
// from the output as well.
func StepError(label string, err error, output string, secrets ...string) error {
	out := strings.TrimSpace(Tail(RedactSecrets(output, secrets...), maxErrorOutput))
	if err == nil {
		return fmt.Errorf("%s failed\nOutput:\n%s", label, out)
	}
	return fmt.Errorf("%s failed: %w\nOutput:\n%s", label, err, RedactSecrets(out, secrets...))
}

// redactMatches replaces, for every match of re, the value part (everything
// after capture group 1, or the whole match when re has no group) with
// [REDACTED], keeping the quote style of quoted values so JSON stays readable.
// A value that is already redacted is left alone, which makes redaction
// idempotent (StepError redacts twice).
func redactMatches(re *regexp.Regexp, s string) string {
	idx := re.FindAllStringSubmatchIndex(s, -1)
	if len(idx) == 0 {
		return s
	}
	var b strings.Builder
	last := 0
	for _, m := range idx {
		start, end := m[0], m[1]
		valStart := start
		if re.NumSubexp() > 0 {
			valStart = m[3] // end of group 1
		}
		b.WriteString(s[last:valStart])
		val := s[valStart:end]
		switch {
		case strings.HasPrefix(val, redacted):
			b.WriteString(val)
		case strings.HasPrefix(val, `"`):
			b.WriteString(`"` + redacted + `"`)
		case strings.HasPrefix(val, `'`):
			b.WriteString(`'` + redacted + `'`)
		default:
			b.WriteString(redacted)
		}
		last = end
	}
	b.WriteString(s[last:])
	return b.String()
}
