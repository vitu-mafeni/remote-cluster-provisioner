package ssh

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestShellQuoteRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	for _, in := range []string{
		"plain", "with space", "it's", `a"b`, "$(touch /tmp/pwned)", "`id`", "a;b|c&d", "line1\nline2", "", `\`, "'; rm -rf / #",
	} {
		out, err := exec.Command("bash", "-c", "printf %s "+ShellQuote(in)).Output()
		if err != nil {
			t.Fatalf("bash failed for %q: %v", in, err)
		}
		if string(out) != in {
			t.Errorf("round trip: got %q, want %q", out, in)
		}
	}
}

func TestRedactSecrets(t *testing.T) {
	in := "PrivateKey = abcdefSECRETkey123=\nfoo ghp_TOKENVALUE bar\nkubeadm join x --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:00"
	out := RedactSecrets(in, "ghp_TOKENVALUE")
	for _, leak := range []string{"abcdefSECRETkey123", "ghp_TOKENVALUE", "abcdef.0123456789abcdef"} {
		if strings.Contains(out, leak) {
			t.Errorf("secret %q leaked: %s", leak, out)
		}
	}
	if !strings.Contains(out, "foo [REDACTED] bar") {
		t.Errorf("explicit secret not redacted: %s", out)
	}
}

func TestStepErrorOmitsCommandAndTruncates(t *testing.T) {
	output := strings.Repeat("x", 10000) + "TAIL-MARKER ghp_SECRET1234"
	err := StepError("phase 3 (CRI-O Install) step 2/3", errors.New("Process exited with status 1"), output, "ghp_SECRET1234")
	msg := err.Error()
	if !strings.Contains(msg, "phase 3 (CRI-O Install) step 2/3") || !strings.Contains(msg, "TAIL-MARKER") {
		t.Errorf("error lacks label or output tail: %s", msg)
	}
	if strings.Contains(msg, "ghp_SECRET1234") {
		t.Errorf("secret leaked: %s", msg)
	}
	if len(msg) > 3000 {
		t.Errorf("error not truncated (%d bytes)", len(msg))
	}
	if errors.Unwrap(err) == nil {
		t.Errorf("underlying error not wrapped")
	}
}

func TestTailRuneBoundary(t *testing.T) {
	s := strings.Repeat("é", 50) // 2 bytes each
	got := Tail(s, 11)
	if !strings.HasSuffix(got, "é") || strings.ContainsRune(got, '�') {
		t.Errorf("Tail cut inside a rune: %q", got)
	}
}

func TestHostKeyCallbackFailsClosedOnBadKnownHostsFile(t *testing.T) {
	t.Setenv(KnownHostsEnv, "/nonexistent/known_hosts")
	cb := hostKeyCallback()
	if err := cb("host:22", nil, nil); err == nil {
		t.Fatal("expected verification failure when known_hosts file cannot be loaded")
	}
}

func TestRedactSecretsForms(t *testing.T) {
	tests := []struct {
		name, in string
		leaks    []string // must not survive
		keep     []string // must survive (over-match guard)
	}{
		{"double-quoted assignment", `password="hunter2x"`, []string{"hunter2x"}, []string{"password="}},
		{"export uppercase", `export PASSWORD="hunter2x"`, []string{"hunter2x"}, []string{"export PASSWORD"}},
		{"json", `{"password":"hunter2x","user":"bob"}`, []string{"hunter2x"}, []string{`"user":"bob"`}},
		{"json with spaces", `{"password" : "hunter2x"}`, []string{"hunter2x"}, nil},
		{"yaml", "password: hunter2x\nuser: bob", []string{"hunter2x"}, []string{"user: bob"}},
		{"yaml empty value does not eat next line", "password:\nuser: bob", nil, []string{"user: bob"}},
		{"PGPASSWORD", `PGPASSWORD=abc123 psql`, []string{"abc123"}, []string{"psql"}},
		{"suffix token var", `MY_TOKEN=abc123`, []string{"abc123"}, nil},
		{"bare kubeadm token", `join with token abcdef.0123456789abcdef now`, []string{"abcdef.0123456789abcdef"}, []string{"join with token", "now"}},
		{"kubeadm flag", `--token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:0123`, []string{"abcdef.0123456789abcdef"}, []string{"--discovery-token-ca-cert-hash sha256:0123"}},
		{"quoted value with spaces", `--password 'a b' --next`, []string{"a b", "b'"}, []string{"--next"}},
		{"double-quoted value with spaces and escape", `--password "a \"b\" c" --next`, []string{"a ", "c\""}, []string{"--next"}},
		{"flag with equals", `--registry-token=s3cr3tvalue`, []string{"s3cr3tvalue"}, nil},
		{"wireguard keys", "PrivateKey = AAAAsecretBBBB=\nPresharedKey=CCCCsecretDDDD=", []string{"AAAAsecret", "CCCCsecret"}, nil},
		{"authorization header", "Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123", []string{"abcdefghijklmnopqrstuvwxyz0123"}, nil},
		{"bearer token", "sent bearer abcdefghijklmnopqrstuvwxyz0123", []string{"abcdefghijklmnopqrstuvwxyz0123"}, nil},
		{"aws key id", "key AKIAABCDEFGHIJKLMNOP used", []string{"AKIAABCDEFGHIJKLMNOP"}, []string{"used"}},
		{"pem", "-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----", []string{"MIIabc"}, nil},
		// Over-match guards.
		{"pwd is not a secret", `pwd=/home/u`, nil, []string{"pwd=/home/u"}},
		{"bearer authentication prose", `unsupported bearer authentication method`, nil, []string{"bearer authentication method"}},
		{"password-stdin takes no value", `oras login --password-stdin ghcr.io`, nil, []string{"--password-stdin ghcr.io"}},
		{"token-ttl untouched", `kubeadm token create --ttl 24h --token-ttl 24h`, nil, []string{"--token-ttl 24h"}},
		{"sudo prompt", `[sudo] password for ubuntu: `, nil, []string{"password for ubuntu"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := RedactSecrets(tt.in)
			for _, l := range tt.leaks {
				if strings.Contains(out, l) {
					t.Errorf("%q leaked in %q", l, out)
				}
			}
			for _, k := range tt.keep {
				if !strings.Contains(out, k) {
					t.Errorf("%q was over-redacted: %q", k, out)
				}
			}
			if len(tt.leaks) > 0 && !strings.Contains(out, "[REDACTED]") {
				t.Errorf("no redaction marker in %q", out)
			}
			if again := RedactSecrets(out); again != out {
				t.Errorf("not idempotent: %q -> %q", out, again)
			}
		})
	}
}

func TestRedactSecretsExplicitValuesAndShortOnesIgnored(t *testing.T) {
	if got := RedactSecrets("abc hunter2secret abc", "hunter2secret", "abc"); got != "abc [REDACTED] abc" {
		t.Errorf("got %q", got)
	}
}
