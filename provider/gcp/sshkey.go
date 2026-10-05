package gcp

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"

	"golang.org/x/crypto/ssh"
)

// SSHKeySecretName is the Secret holding the node's SSH private key
// ("ssh-privatekey", PKCS#1 PEM) — the same name and format the AWS path uses,
// so the controller's getSSHClientByProvider serves both.
func SSHKeySecretName(npName string) string { return npName + "-ssh-key" }

// GenerateSSHKeyPair creates a 4096-bit RSA key and returns the PKCS#1 PEM
// private key and the OpenSSH authorized_keys public key.
func GenerateSSHKeyPair() (privPEM string, pubAuthorizedKey []byte, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return "", nil, fmt.Errorf("generating RSA key: %w", err)
	}
	privPEM = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}))
	pub, err := ssh.NewPublicKey(&priv.PublicKey)
	if err != nil {
		return "", nil, fmt.Errorf("deriving SSH public key: %w", err)
	}
	return privPEM, ssh.MarshalAuthorizedKey(pub), nil
}

// PublicKeyFromPEM derives the authorized_keys public key from a PKCS#1 PEM
// private key (so a retry reuses the key stored in the Secret).
func PublicKeyFromPEM(privPEM string) ([]byte, error) {
	block, _ := pem.Decode([]byte(privPEM))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing PKCS1 private key: %w", err)
	}
	pub, err := ssh.NewPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("deriving SSH public key: %w", err)
	}
	return ssh.MarshalAuthorizedKey(pub), nil
}

// SSHUser returns the Linux user the controller logs in as and whose
// authorized_keys the guest agent populates from the ssh-keys metadata.
func SSHUser(sshUsernameOverride string) string {
	if sshUsernameOverride != "" {
		return sshUsernameOverride
	}
	return DefaultSSHUser
}

// SSHKeysMetadata renders the value of the instance "ssh-keys" metadata key:
// "<user>:<key type> <base64> <user>". The guest agent creates the user (with
// sudo) when it does not exist, which is what lets the "ubuntu" user work on
// images that do not predefine it.
func SSHKeysMetadata(user string, authorizedKey []byte) (string, error) {
	pub, _, _, _, err := ssh.ParseAuthorizedKey(authorizedKey)
	if err != nil {
		return "", fmt.Errorf("invalid SSH public key: %w", err)
	}
	if user == "" || strings.ContainsAny(user, ": \t\r\n") {
		return "", fmt.Errorf("invalid SSH user %q", user)
	}
	line := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
	return user + ":" + line + " " + user, nil
}
