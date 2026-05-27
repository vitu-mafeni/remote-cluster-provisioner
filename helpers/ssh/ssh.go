package ssh

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"strings"

	cryptossh "golang.org/x/crypto/ssh"
)

func Connect(host string, port int, user, password string) (*Client, error) {
	log.Printf("Connecting to %s:%d with user %s", host, port, user)

	config := &cryptossh.ClientConfig{
		User: user,
		Auth: []cryptossh.AuthMethod{
			cryptossh.Password(password),
		},
		HostKeyCallback: cryptossh.InsecureIgnoreHostKey(),
	}

	conn, err := cryptossh.Dial("tcp", fmt.Sprintf("%s:%d", host, port), config)
	if err != nil {
		return nil, err
	}

	return &Client{Conn: conn}, nil
}

func ConnectWithPrivateKey(host string, port int, user, privateKey string) (*Client, error) {
	// Normalize line endings and surrounding whitespace that can creep in
	// when keys are stored in Kubernetes secrets or pasted into YAML files.
	privateKey = strings.TrimSpace(privateKey)
	privateKey = strings.ReplaceAll(privateKey, "\r\n", "\n")
	privateKey = strings.ReplaceAll(privateKey, "\r", "\n")

	signer, err := parsePrivateKey([]byte(privateKey))
	if err != nil {
		return nil, fmt.Errorf("ssh: %w", err)
	}

	log.Printf("Connecting to %s:%d with user %s via private key", host, port, user)

	config := &cryptossh.ClientConfig{
		User: user,
		Auth: []cryptossh.AuthMethod{
			cryptossh.PublicKeys(signer),
		},
		HostKeyCallback: cryptossh.InsecureIgnoreHostKey(),
	}

	conn, err := cryptossh.Dial("tcp", fmt.Sprintf("%s:%d", host, port), config)
	if err != nil {
		return nil, err
	}

	return &Client{Conn: conn}, nil
}

// parsePrivateKey attempts to parse a PEM-encoded private key using multiple
// formats in order.  This handles the case where the PEM header says
// "OPENSSH PRIVATE KEY" but the binary payload was produced by a different
// (non-OpenSSH) tool, as well as PKCS#1, PKCS#8, and EC keys.
func parsePrivateKey(pemBytes []byte) (cryptossh.Signer, error) {
	// Primary path: handles OpenSSH native, RSA PKCS#1, EC, and PKCS#8 keys.
	signer, err := cryptossh.ParsePrivateKey(pemBytes)
	if err == nil {
		return signer, nil
	}
	primaryErr := err

	// Fallback: decode the PEM block and try x509 parsers directly.
	// This covers keys that carry an "OPENSSH PRIVATE KEY" header but store
	// PKCS#8 or PKCS#1 binary content (produced by some non-standard tools).
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in private key: %w", primaryErr)
	}

	type tryParser func([]byte) (cryptossh.Signer, error)
	parsers := []tryParser{
		func(b []byte) (cryptossh.Signer, error) {
			key, e := x509.ParsePKCS8PrivateKey(b)
			if e != nil {
				return nil, e
			}
			return cryptossh.NewSignerFromKey(key)
		},
		func(b []byte) (cryptossh.Signer, error) {
			key, e := x509.ParsePKCS1PrivateKey(b)
			if e != nil {
				return nil, e
			}
			return cryptossh.NewSignerFromKey(key)
		},
		func(b []byte) (cryptossh.Signer, error) {
			key, e := x509.ParseECPrivateKey(b)
			if e != nil {
				return nil, e
			}
			return cryptossh.NewSignerFromKey(key)
		},
		// PKCS#8 sometimes wraps ed25519 — handle via the parsed interface{}
		func(b []byte) (cryptossh.Signer, error) {
			key, e := x509.ParsePKCS8PrivateKey(b)
			if e != nil {
				return nil, e
			}
			switch k := key.(type) {
			case *rsa.PrivateKey, *ecdsa.PrivateKey, ed25519.PrivateKey:
				return cryptossh.NewSignerFromKey(k)
			}
			return nil, fmt.Errorf("unsupported key type from PKCS#8")
		},
	}

	for _, p := range parsers {
		if s, e := p(block.Bytes); e == nil {
			return s, nil
		}
	}

	return nil, fmt.Errorf("unable to parse private key (tried OpenSSH, PKCS#8, PKCS#1, EC formats): %w", primaryErr)
}
