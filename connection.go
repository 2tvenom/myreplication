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

		currentDb string

		masterPosition uint64
		fileName       string
	}
)

const (
	_DEFAULT_DB = "information_schema"
)

func NewConnection() *connection {
	return &connection{
		conn: nil,
	}
}

func (c *connection) Connection() net.Conn {
	return c.conn
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

func (c *connection) GetMasterStatus() (pos uint32, filename string, err error) {
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

func (c *connection) ChecksumCompatibility() (ok bool, err error) {
	err = c.initDb(_DEFAULT_DB)
	if err != nil {
		return
	}
	rs, err := c.query("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")

	if err != nil {
		return
	}

	pack, err := rs.nextRow()
	if err != nil {
		if err == EOF_ERR {
			return false, nil
		}
		return
	}

	pack.readStringLength()
	_type, _ := pack.readStringLength()
	rs.nextRow()

	if len(_type) == 0 {
		return
	}
	ok = true
	_, err = c.query("set @master_binlog_checksum = @@global.binlog_checksum")
	return
}

func (c *connection) initDb(schemaName string) error {
	q := &initDb{}
	pack := q.writeServer(schemaName)
	err := c.packWriter.flush(pack)
	if err != nil {
		return err
	}

	pack, err = c.packReader.readNextPack()
	if err != nil {
		return err
	}

	return pack.isError()
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

func (c *connection) connectDb(db string) error {
	q := &connectDb{}
	pack := q.writeServer(db)
	err := c.packWriter.flush(pack)
	if err != nil {
		return err
	}

	pack, err = c.packReader.readNextPack()

	if err != nil {
		return err
	}

	return pack.isError()
}

func (c *connection) fieldList(db, table string) (*resultSet, error) {
	if c.currentDb != db {
		err := c.connectDb(db)
		if err != nil {
			return nil, nil
		}
	}

	q := &fieldList{}
	pack := q.writeServer(table)
	err := c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	rs := &resultSet{}
	rs.setReader(c.packReader)
	err = rs.initFieldList()

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (c *connection) StartBinlogDump(position uint32, fileName string, serverId uint32) (el *eventLog, err error) {
	ok, err := c.ChecksumCompatibility()
	if err != nil {
		return
	}

	register := &registerSlave{}
	pack := register.writeServer(serverId)
	err = c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	pack, err = c.packReader.readNextPack()

	if err != nil {
		return nil, err
	}

	err = pack.isError()

	if err != nil {
		return nil, err
	}

	startBinLog := &binlogDump{}
	pack = startBinLog.writeServer(position, fileName, serverId)
	err = c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	var additionalLength int

	if ok {
		additionalLength = 4
	}

	el = newEventLog(c, additionalLength)

	return el, nil
}
