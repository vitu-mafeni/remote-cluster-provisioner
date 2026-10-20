package ssh

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	cryptossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

const (
	// dialTimeout bounds TCP connect plus the SSH handshake.
	dialTimeout = 30 * time.Second
	// keepAliveInterval is how often a keepalive request is sent on an idle
	// connection; keepAliveMaxFailures consecutive failures close it so that
	// in-flight Run calls fail instead of hanging on a dead peer.
	keepAliveInterval    = 30 * time.Second
	keepAliveMaxFailures = 3
	keepAliveReplyWait   = 15 * time.Second

	// KnownHostsEnv names an environment variable holding the path of an
	// OpenSSH known_hosts file. When set, host keys are verified against it.
	KnownHostsEnv = "SSH_KNOWN_HOSTS_FILE"
)

var insecureHostKeyWarning sync.Once

// hostKeyCallback returns the host key verifier: strict known_hosts checking
// when SSH_KNOWN_HOSTS_FILE is set (failing closed if the file is unusable),
// otherwise the legacy accept-anything behaviour with a one-time warning.
// There is intentionally no trust-on-first-use cache: nodes are routinely
// reinstalled and get fresh host keys.
func hostKeyCallback() cryptossh.HostKeyCallback {
	if path := strings.TrimSpace(os.Getenv(KnownHostsEnv)); path != "" {
		cb, err := knownhosts.New(path)
		if err != nil {
			return func(string, net.Addr, cryptossh.PublicKey) error {
				return fmt.Errorf("ssh: cannot load %s=%q: %w", KnownHostsEnv, path, err)
			}
		}
		return cb
	}
	insecureHostKeyWarning.Do(func() {
		log.Printf("WARNING: SSH host keys are NOT verified (%s is not set); connections are open to man-in-the-middle attacks. Set %s to an OpenSSH known_hosts file to enable verification.", KnownHostsEnv, KnownHostsEnv)
	})
	return cryptossh.InsecureIgnoreHostKey()
}

var (
	probeKeyOnce sync.Once
	probeKey     cryptossh.PublicKey
)

// hostKeyAlgorithmsByType maps a known_hosts key type to the host key algorithms
// that authenticate that key (RSA keys can be presented with three signature
// algorithms; every type may also arrive as an OpenSSH certificate).
var hostKeyAlgorithmsByType = map[string][]string{
	cryptossh.KeyAlgoED25519:    {cryptossh.KeyAlgoED25519, cryptossh.CertAlgoED25519v01},
	cryptossh.KeyAlgoECDSA256:   {cryptossh.KeyAlgoECDSA256, cryptossh.CertAlgoECDSA256v01},
	cryptossh.KeyAlgoECDSA384:   {cryptossh.KeyAlgoECDSA384, cryptossh.CertAlgoECDSA384v01},
	cryptossh.KeyAlgoECDSA521:   {cryptossh.KeyAlgoECDSA521, cryptossh.CertAlgoECDSA521v01},
	cryptossh.KeyAlgoRSA:        {cryptossh.KeyAlgoRSASHA512, cryptossh.KeyAlgoRSASHA256, cryptossh.KeyAlgoRSA, cryptossh.CertAlgoRSASHA512v01, cryptossh.CertAlgoRSASHA256v01, cryptossh.CertAlgoRSAv01},
	cryptossh.KeyAlgoSKED25519:  {cryptossh.KeyAlgoSKED25519, cryptossh.CertAlgoSKED25519v01},
	cryptossh.KeyAlgoSKECDSA256: {cryptossh.KeyAlgoSKECDSA256, cryptossh.CertAlgoSKECDSA256v01},
}

// hostKeyTypePreference orders the known_hosts key types when several are present.
var hostKeyTypePreference = []string{
	cryptossh.KeyAlgoED25519, cryptossh.KeyAlgoECDSA256, cryptossh.KeyAlgoECDSA384, cryptossh.KeyAlgoECDSA521,
	cryptossh.KeyAlgoSKED25519, cryptossh.KeyAlgoSKECDSA256, cryptossh.KeyAlgoRSA,
}

// knownHostKeyAlgorithms returns the host key algorithms the client should
// offer for addr ("host:port"): only those matching key types that known_hosts
// lists for that host. Without this restriction the SSH handshake picks the
// client's most preferred algorithm (e.g. ecdsa) and, when known_hosts holds
// only another type (e.g. ed25519), verification fails with a "key mismatch"
// although the host IS listed. It returns nil (leave the defaults) when cb is
// not a known_hosts callback or the host has no direct entry; verification
// stays fail-closed either way because cb still checks the presented key.
func knownHostKeyAlgorithms(cb cryptossh.HostKeyCallback, addr string) []string {
	if cb == nil {
		return nil
	}
	probeKeyOnce.Do(func() {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return
		}
		probeKey, _ = cryptossh.NewPublicKey(pub)
	})
	if probeKey == nil {
		return nil
	}
	// A random key never matches a real entry, so a listed host yields a
	// *KeyError carrying every key known for it.
	err := cb(addr, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 22}, probeKey)
	var ke *knownhosts.KeyError
	if !errors.As(err, &ke) || len(ke.Want) == 0 {
		return nil
	}
	have := map[string]bool{}
	for _, k := range ke.Want {
		have[k.Key.Type()] = true
	}
	var algos []string
	for _, typ := range hostKeyTypePreference {
		if have[typ] {
			algos = append(algos, hostKeyAlgorithmsByType[typ]...)
		}
	}
	return algos
}

// dial connects with a bounded TCP connect + handshake time and starts the
// keepalive loop.
func dial(host string, port int, config *cryptossh.ClientConfig) (*Client, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	if algos := knownHostKeyAlgorithms(config.HostKeyCallback, addr); len(algos) > 0 {
		config.HostKeyAlgorithms = algos
	}
	nc, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	// Bound the handshake too (NewClientConn has no timeout of its own).
	_ = nc.SetDeadline(time.Now().Add(dialTimeout))
	sconn, chans, reqs, err := cryptossh.NewClientConn(nc, addr, config)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	_ = nc.SetDeadline(time.Time{})
	conn := cryptossh.NewClient(sconn, chans, reqs)
	go keepAlive(conn)
	return &Client{Conn: conn}, nil
}

// keepAlive pings the server periodically and closes the connection when it
// stops answering. It exits when the connection is closed.
func keepAlive(conn *cryptossh.Client) {
	closed := make(chan struct{})
	go func() {
		_ = conn.Wait()
		close(closed)
	}()
	t := time.NewTicker(keepAliveInterval)
	defer t.Stop()
	failures := 0
	for {
		select {
		case <-closed:
			return
		case <-t.C:
			reply := make(chan error, 1)
			go func() {
				_, _, err := conn.SendRequest("keepalive@openssh.com", true, nil)
				reply <- err
			}()
			var err error
			select {
			case err = <-reply:
			case <-time.After(keepAliveReplyWait):
				err = fmt.Errorf("keepalive timed out")
			case <-closed:
				return
			}
			if err != nil {
				failures++
				if failures >= keepAliveMaxFailures {
					_ = conn.Close()
					return
				}
			} else {
				failures = 0
			}
		}
	}
}

func Connect(host string, port int, user, password string) (*Client, error) {
	log.Printf("Connecting to %s:%d with user %s", host, port, user)

	config := &cryptossh.ClientConfig{
		User: user,
		Auth: []cryptossh.AuthMethod{
			cryptossh.Password(password),
		},
		HostKeyCallback: hostKeyCallback(),
		Timeout:         dialTimeout,
	}

	return dial(host, port, config)
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
		HostKeyCallback: hostKeyCallback(),
		Timeout:         dialTimeout,
	}

	return dial(host, port, config)
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
