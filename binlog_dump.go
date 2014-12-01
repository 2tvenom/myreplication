package mysql_replication_listener

type (
	binlogDump struct {
	}
)

func (bd *binlogDump) writeServer(position uint32, fileName string, serverId uint32) *pack {
	//command
	pack := newPack()

	pack.WriteByte(byte(_COM_BINLOG_DUMP))
	//position
	pack.writeUInt32(position)
	//flags
	pack.writeUInt16(uint16(0))
	//server id
	pack.writeUInt32(serverId)
	//filename
	pack.Write([]byte(fileName))

	return pack
}
