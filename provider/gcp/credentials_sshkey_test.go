package gcp

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pkgruntime "dcn.ssu.ac.kr/infra/pkg/runtime"
)

func pkgruntimeConfig() pkgruntime.Config { return pkgruntime.Config{} }

var (
	keyOnce sync.Once
	keyPriv string
	keyPub  []byte
	keyErr  error
)

// testKey returns one shared 4096-bit key pair (generation takes a moment).
func testKey() (string, []byte, error) {
	keyOnce.Do(func() { keyPriv, keyPub, keyErr = GenerateSSHKeyPair() })
	return keyPriv, keyPub, keyErr
}

const saKeyJSON = `{
  "type": "service_account",
  "project_id": "my-project",
  "private_key_id": "abc",
  "private_key": "-----BEGIN PRIVATE KEY-----\nTOPSECRETMATERIAL\n-----END PRIVATE KEY-----\n",
  "client_email": "node-provisioner@my-project.iam.gserviceaccount.com",
  "token_uri": "https://oauth2.googleapis.com/token"
}`

func secretWith(data map[string]string) *corev1.Secret {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "gcp-creds", Namespace: "ns"}, Data: map[string][]byte{}}
	for k, v := range data {
		s.Data[k] = []byte(v)
	}
	return s
}

func TestResolveCredentials_DefaultKeysAndExplicitKey(t *testing.T) {
	for _, key := range DefaultCredentialKeys {
		c, err := ResolveCredentials(secretWith(map[string]string{key: saKeyJSON}), "")
		if err != nil {
			t.Fatalf("key %s: %v", key, err)
		}
		if c.ProjectID != "my-project" || c.ClientEmail != "node-provisioner@my-project.iam.gserviceaccount.com" || len(c.JSON) == 0 {
			t.Errorf("key %s: parsed %+v", key, c)
		}
	}
	// credentials.json wins over serviceAccountKey when both exist.
	other := strings.Replace(saKeyJSON, "my-project", "other-project", 1)
	c, err := ResolveCredentials(secretWith(map[string]string{"credentials.json": saKeyJSON, "serviceAccountKey": other}), "")
	if err != nil || c.ProjectID != "my-project" {
		t.Errorf("credentials.json must take precedence, got %+v err=%v", c, err)
	}
	// An explicit key is used exclusively.
	c, err = ResolveCredentials(secretWith(map[string]string{"credentials.json": saKeyJSON, "mine": other}), "mine")
	if err != nil || c.ProjectID != "other-project" {
		t.Errorf("explicit key: got %+v err=%v", c, err)
	}
	if _, err := ResolveCredentials(secretWith(map[string]string{"credentials.json": saKeyJSON}), "missing"); err == nil || !strings.Contains(err.Error(), `"missing"`) {
		t.Errorf("explicit missing key must be a clear error, got %v", err)
	}
	if _, err := ResolveCredentials(secretWith(map[string]string{"unrelated": "x"}), ""); err == nil || !strings.Contains(err.Error(), "credentials.json") {
		t.Errorf("no key must list the tried names, got %v", err)
	}
	if _, err := ResolveCredentials(nil, ""); err == nil {
		t.Error("nil secret must be an error")
	}
}

func TestParseServiceAccountKey_RejectsUnsafeOrBroken(t *testing.T) {
	cases := map[string]string{
		"empty":              "",
		"not json":           "this is not json TOPSECRETMATERIAL",
		"external account":   `{"type":"external_account","audience":"x","credential_source":{"file":"/etc/passwd"}}`,
		"authorized user":    `{"type":"authorized_user","client_id":"x","client_secret":"y","refresh_token":"z"}`,
		"no email":           `{"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----x"}`,
		"no private key":     `{"type":"service_account","client_email":"a@b.iam.gserviceaccount.com"}`,
		"insecure token_uri": `{"type":"service_account","client_email":"a@b.c","private_key":"-----BEGIN PRIVATE KEY-----x","token_uri":"http://evil.example/token"}`,
	}
	for name, doc := range cases {
		_, err := ParseServiceAccountKey([]byte(doc))
		if err == nil {
			t.Errorf("%s: expected an error", name)
			continue
		}
		if strings.Contains(err.Error(), "TOPSECRETMATERIAL") {
			t.Errorf("%s: error leaks key material: %v", name, err)
		}
	}
}

func TestCredentialsNeverPrintTheKey(t *testing.T) {
	c, err := ParseServiceAccountKey([]byte(saKeyJSON))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), c.String()} {
		if strings.Contains(s, "TOPSECRETMATERIAL") || strings.Contains(s, "PRIVATE KEY") {
			t.Errorf("credentials print the key: %q", s)
		}
	}
}

func TestSSHKeyRoundTripAndMetadata(t *testing.T) {
	priv, pub, err := testKey()
	if err != nil {
		t.Fatal(err)
	}
	derived, err := PublicKeyFromPEM(priv)
	if err != nil {
		t.Fatal(err)
	}
	if string(derived) != string(pub) {
		t.Error("the public key derived from the stored private key must equal the generated one (retries reuse the Secret)")
	}
	if _, err := PublicKeyFromPEM("not pem"); err == nil {
		t.Error("garbage PEM must be an error")
	}

	md, err := SSHKeysMetadata("ubuntu", pub)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(md, "ubuntu:ssh-rsa ") || !strings.HasSuffix(md, " ubuntu") || strings.Contains(md, "\n") {
		t.Errorf("ssh-keys metadata must be a single <user>:<type> <key> <user> line, got %q", md)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(md, "ubuntu:"), " ubuntu")
	if _, _, _, _, err := ssh.ParseAuthorizedKey([]byte(body)); err != nil {
		t.Errorf("embedded key must parse: %v", err)
	}
	for _, bad := range []string{"", "a:b", "x y", "x\ny"} {
		if _, err := SSHKeysMetadata(bad, pub); err == nil {
			t.Errorf("user %q must be rejected", bad)
		}
	}
	if _, err := SSHKeysMetadata("ubuntu", []byte("garbage")); err == nil {
		t.Error("invalid public key must be rejected")
	}
	if SSHUser("") != "ubuntu" || SSHUser("ops") != "ops" {
		t.Error("SSHUser must default to ubuntu and honour the override")
	}
	if SSHKeySecretName("n1") != "n1-ssh-key" {
		t.Error("the SSH key Secret name must match the AWS convention")
	}
}
