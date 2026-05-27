package ssh

import (
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

	signer, err := cryptossh.ParsePrivateKey([]byte(privateKey))
	if err != nil {
		return nil, err
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
