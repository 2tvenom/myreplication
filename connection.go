package mysql_replication_listener

import (
	"fmt"
	"net"
	"strconv"
)

type (
	connection struct {
		conn       net.Conn
		packReader *packReader
		packWriter *packWriter

		masterPosition uint64
		fileName       string
	}
)

func newConnection() *connection {
	return &connection{
		conn:      nil,
	}
}

func (c *connection) ConnectAndAuth(host string, port int, username, password string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return err
	}
	c.conn = conn

	c.packReader = newPackReader(conn)
	c.packWriter = newPackWriter(conn)

	err = c.init(username, password)
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
	handshake := &pkgHandshake{}

	err = handshake.readServer(pack)

	if err != nil {
		return
	}

	//prepare and buff handshake auth response
	pack = handshake.writeServer(username, password)
	pack.setSequence(byte(1))
	err = c.packWriter.flush(pack)

	if err != nil {
		return
	}

	pack, err = c.packReader.readNextPack()
	if err != nil {
		return err
	}

	return pack.isError()
}

func (c *connection) getMasterStatus() (pos uint32, filename string, err error) {
	rs, err := c.query("SHOW MASTER STATUS")
	if err != nil {
		return
	}

	pack, err := rs.nextRow()
	if err != nil {
		return
	}

	_fileName, _ := pack.readStringLength()
	_pos, _ := pack.readStringLength()

	filename = string(_fileName)
	pos64, err := strconv.ParseUint(string(_pos), 10, 32)

	if err != nil {
		return
	}

	pos = uint32(pos64)

	rs.nextRow()
	rs = nil
	return
}

func (c *connection) query(command string) (*resultSet, error) {
	q := &query{}
	pack := q.writeServer(command)
	err := c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	rs := &resultSet{}
	rs.setReader(c.packReader)
	err = rs.init()

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (c *connection) startBinlogDump(position uint32, fileName string, serverId uint32) (err error) {
	register := &registerSlave{}
	pack := register.writeServer(serverId)
	err = c.packWriter.flush(pack)
	if err != nil {
		return err
	}

	pack, err = c.packReader.readNextPack()
	if err != nil {
		return err
	}

	err = pack.isError()

	if err != nil {
		return err
	}

	startBinLog := &binlogDump{}
	pack = startBinLog.writeServer(position, fileName, serverId)
	err = c.packWriter.flush(pack)
	if err != nil {
		return err
	}

	return nil
}
