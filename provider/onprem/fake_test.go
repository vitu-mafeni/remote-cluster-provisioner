package onprem

import (
	cryptossh "golang.org/x/crypto/ssh"

	sshhelper "dcn.ssu.ac.kr/infra/pkg/ssh"
)

func sshClient(c *cryptossh.Client) *sshhelper.Client { return &sshhelper.Client{Conn: c} }
