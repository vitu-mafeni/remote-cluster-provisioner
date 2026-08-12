package ssh

import "strings"

// Run executes cmd on the remote host under bash.
// The script is piped to "bash -s" via stdin so that bash-specific features
// (set -euo pipefail, process substitution, etc.) work regardless of the
// remote user's login shell or sshd's default exec shell (/bin/sh on Ubuntu).
func Run(client *Client, cmd string) (string, error) {
	session, err := client.Conn.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	session.Stdin = strings.NewReader(cmd)
	output, err := session.CombinedOutput("bash -s")
	return string(output), err
}
