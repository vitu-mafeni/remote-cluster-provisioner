package ssh

// func Run(client *Client, cmd string) (string, error) {

// 	session, err := client.Conn.NewSession()
// 	if err != nil {
// 		return "", err
// 	}
// 	defer session.Close()

// 	var out bytes.Buffer
// 	session.Stdout = &out
// 	session.Stderr = &out

//		err = session.Run(cmd)
//		return out.String(), err
//	}
func Run(client *Client, cmd string) (string, error) {
	session, err := client.Conn.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	output, err := session.CombinedOutput(cmd)
	return string(output), err
}
