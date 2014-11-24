package mysql_replication_listener

func registerSlave(writer *protoWriter, reader *protoReader, server_id uint32) (err error) {
	//register slave
	//command
	writer.writeTheeByteUInt32(uint32(18))
	writer.WriteByte(0)
	writer.WriteByte(byte(_COM_REGISTER_SLAVE))
	//server_id
	writer.writeUInt32(server_id)
	//host
	writer.writeStringLength("")
	//user
	writer.writeStringLength("")
	//password
	writer.writeStringLength("")
	writer.writeUInt16(uint16(0))
	writer.writeUInt32(uint32(0))
	writer.writeUInt32(uint32(0))
	err = writer.Flush()
	if err != nil {
		return err
	}
	return ok_packet(reader)
}
