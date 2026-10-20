package gcp

import (
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// DefaultCredentialKeys are the Secret data keys tried (in order) when
// spec.credentialsRef.key is empty.
var DefaultCredentialKeys = []string{"credentials.json", "serviceAccountKey"}

// Credentials is a parsed Google service-account key. The raw JSON contains a
// private key: it is never logged, and String/GoString redact it so an
// accidental %v or %+v cannot leak it.
//
// There is no token cache or background refresher (unlike the AWS credential
// manager, which exists for MFA/STS sessions): the Google client libraries
// exchange the key for short-lived OAuth2 access tokens themselves and refresh
// them transparently for the lifetime of each client.
type Credentials struct {
	JSON        []byte
	ProjectID   string // project_id of the key (the default project)
	ClientEmail string
}

// String implements fmt.Stringer without revealing the key.
func (c Credentials) String() string {
	return fmt.Sprintf("gcp.Credentials{serviceAccount: %q, project: %q}", c.ClientEmail, c.ProjectID)
}

// GoString implements fmt.GoStringer without revealing the key.
func (c Credentials) GoString() string { return c.String() }

// ResolveCredentials extracts and validates the service-account key stored in
// secret. key is spec.credentialsRef.key; when empty the DefaultCredentialKeys
// are tried. Only keys of "type": "service_account" are accepted: the Google
// libraries warn that credential configurations from external sources must be
// validated, and other types (external_account, authorized_user, ...) can make
// the controller read files or call arbitrary endpoints.
//
// Error messages never contain the key material.
func ResolveCredentials(secret *corev1.Secret, key string) (Credentials, error) {
	if secret == nil {
		return Credentials{}, fmt.Errorf("no credentials secret")
	}
	var raw []byte
	if key != "" {
		v, ok := secret.Data[key]
		if !ok {
			return Credentials{}, fmt.Errorf("key %q not found in secret %q", key, secret.Name)
		}
		raw = v
	} else {
		for _, k := range DefaultCredentialKeys {
			if v, ok := secret.Data[k]; ok {
				raw = v
				break
			}
		}
		if raw == nil {
			return Credentials{}, fmt.Errorf("no service-account key found in secret %q (tried: %s)",
				secret.Name, strings.Join(DefaultCredentialKeys, ", "))
		}
	}
	return ParseServiceAccountKey(raw)
}

// ParseServiceAccountKey validates a service-account key JSON document.
func ParseServiceAccountKey(raw []byte) (Credentials, error) {
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return Credentials{}, fmt.Errorf("service-account key is empty")
	}
	var k struct {
		Type        string `json:"type"`
		ProjectID   string `json:"project_id"`
		PrivateKey  string `json:"private_key"`
		ClientEmail string `json:"client_email"`
		TokenURI    string `json:"token_uri"`
	}
	if err := json.Unmarshal(raw, &k); err != nil {
		// Do not wrap err: a syntax error can quote a fragment of the document.
		return Credentials{}, fmt.Errorf("service-account key is not valid JSON")
	}
	if k.Type != "service_account" {
		return Credentials{}, fmt.Errorf(`service-account key has "type": %q; only "service_account" keys are supported`, k.Type)
	}
	if k.ClientEmail == "" || !strings.Contains(k.ClientEmail, "@") {
		return Credentials{}, fmt.Errorf(`service-account key has no valid "client_email"`)
	}
	if !strings.Contains(k.PrivateKey, "PRIVATE KEY") {
		return Credentials{}, fmt.Errorf(`service-account key has no "private_key"`)
	}
	if k.TokenURI != "" && !strings.HasPrefix(k.TokenURI, "https://") {
		return Credentials{}, fmt.Errorf(`service-account key has a non-https "token_uri"`)
	}
	return Credentials{JSON: raw, ProjectID: k.ProjectID, ClientEmail: k.ClientEmail}, nil
}
