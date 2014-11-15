package mysql_replication_listener

import (
	"bufio"
	"fmt"
	"net"
)

type (
	connection struct {
		conn      net.Conn
		header    *pkgHeader
		handshake *pkgHandshake
	}
)

func newConnection() *connection {
	return &connection{
		conn:      nil,
		header:    newPkgHeader(),
		handshake: newHandshake(),
	}
}

func (c *connection) connectAndAuth(host string, port int, username, password string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	c.conn = conn

	err = c.init()
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) init() error {
	//receive handshake
	reader := newProtoReader(bufio.NewReader(c.conn))
	err := c.header.read(reader)

	if err != nil {
		return err
	}

	err = c.handshake.readServer(reader, c.header.length)

	if err != nil {
		return err
	}

	return nil
}
