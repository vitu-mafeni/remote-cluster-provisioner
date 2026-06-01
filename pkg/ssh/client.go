package ssh

import cryptossh "golang.org/x/crypto/ssh"

type Client struct {
	Conn *cryptossh.Client
}
