package mysql_replication_listener

import (
	"bufio"
	"fmt"
	"net"
)

type (
	connection struct {
		conn       net.Conn
		packReader *packReader
		packWriter *packWriter

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

func (c *connection) connectAndAuth(host string, port int, username, password string, server_id uint32) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return err
	}
	c.conn = conn

	c.packReader = newPackReader(conn)
	c.packWriter = newPackWriter(conn)
	c.reader = newProtoReader(bufio.NewReader(c.conn))
	c.writer = newProtoWriter(bufio.NewWriter(c.conn))

	err = c.init(username, password)
	if err != nil {
		return err
	}

	err = c.registerSlave(server_id)
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) init(username, password string) (err error) {
	pack, err := c.packReader.readNextPack()
	if err != nil {
		return err
	}
	//receive handshake
	//get handshake data and parse
	err = c.handshake.readServer(pack)

	if err != nil {
		return
	}

	//prepare and buff handshake auth response
	pack = c.handshake.writeServer(username, password)
	err = c.packWriter.flush(pack)

	if err != nil {
		return
	}

	pack, err := c.packReader.readNextPack()
	if err != nil {
		return err
	}

	return pack.isError()
}

func (c *connection) registerSlave(server_id uint32) (err error) {
	//register slave
	//command
	c.writer.writeTheeByteUInt32(uint32(18))
	c.writer.WriteByte(0)
	c.writer.WriteByte(byte(_COM_REGISTER_SLAVE))
	//server_id
	c.writer.writeUInt32(server_id)
	//host
	c.writer.writeStringLength("")
	//user
	c.writer.writeStringLength("")
	//password
	c.writer.writeStringLength("")
	c.writer.writeUInt16(uint16(0))
	c.writer.writeUInt32(uint32(0))
	c.writer.writeUInt32(uint32(0))
	err = c.writer.Flush()
	if err != nil {
		return err
	}
	return ok_packet(c.reader)
}

func (c *connection) binlogDump(position uint32, server_id uint32, filename string) (err error) {
	//register slave
	packLength := 1 + 4 + 2 + 4 + len(filename)
	c.writer.writeTheeByteUInt32(uint32(packLength))
	c.writer.Writer.WriteByte(byte(0))
	//command
	c.writer.WriteByte(byte(_COM_BINLOG_DUMP))
	//position
	c.writer.writeUInt32(position)
	//flags
	c.writer.writeUInt16(uint16(0))
	//position
	c.writer.writeUInt32(server_id)
	//file name
	c.writer.Write([]byte(filename))
	err = c.writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (c *connection) getMasterStatus() (pos string, filename string, err error) {
	rs, err := c.query("SHOW MASTER STATUS")
	if err != nil {
		return
	}

	err = rs.nextRow()
	if err != nil {
		return
	}

	filenameByteAr, _, err := rs.buff.readLenString()
	filename = string(filenameByteAr)
	if err != nil {
		return
	}

	posAr, _, err := rs.buff.readLenString()
	pos = string(posAr)
	if err != nil {
		return
	}

	rs.nextRow()
	rs = nil
	return
}

func (c *connection) query(command string) (*resultSet, error) {
	return query(c.writer, c.reader, command)
}
