package mysql_replication_listener

import (
	"bufio"
	"fmt"
	"net"
)

type (
	connection struct {
		conn           net.Conn
		handshake      *pkgHandshake
		reader         *protoReader
		writer         *protoWriter
		masterPosition uint64
		fileName       string
	}
)

func newConnection() *connection {
	return &connection{
		conn:      nil,
		handshake: newHandshake(),
	}
}

func (c *connection) connectAndAuth(host string, port int, username, password string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	c.conn = conn

	c.reader = newProtoReader(bufio.NewReader(c.conn))
	c.writer = newProtoWriter(bufio.NewWriter(c.conn))

	err = c.init(username, password)
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) init(username, password string) (err error) {
	//receive handshake
	//get handshake data and parse
	err = c.handshake.readServer(c.reader)

	if err != nil {
		return
	}

	//prepare and buff handshake auth response
	c.handshake.writeServer(c.writer, byte(1), username, password)
	err = c.writer.Flush()

	if err != nil {
		return
	}

	return ok_packet(c.reader)
}

func (c *connection) getMasterPosition() (uint64, error) {

	return 0, nil
}

func (c *connection) query(command string) (*resultSet, error) {
	return query(c.writer, c.reader, command)
}
