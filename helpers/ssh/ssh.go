package ssh

import (
	"fmt"

	cryptossh "golang.org/x/crypto/ssh"
)

func Connect(host string, port int, user, password string) (*Client, error) {
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
