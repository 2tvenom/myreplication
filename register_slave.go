package myreplication

type (
	registerSlave struct {
	}
)

func (rs *registerSlave) writeServer(server_id uint32) *pack {
	//register slave
	//command

	pack := newPack()
	pack.WriteByte(byte(_COM_REGISTER_SLAVE))
	pack.writeUInt32(server_id)
	//host
	pack.writeStringLength("")
	//user
	pack.writeStringLength("")
	//password
	pack.writeStringLength("")
	//slaves mysql port
	pack.writeUInt16(uint16(0))
	//replication rank
	pack.writeUInt32(uint32(0))
	//master id
	pack.writeUInt32(uint32(0))
	return pack
}
