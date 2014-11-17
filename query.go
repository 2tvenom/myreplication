package mysql_replication_listener

func query(writer *protoWriter, reader *protoReader, queryCommand string) (*resultSet, error) {
	queryLength := uint32(len(queryCommand)) + 1
	err := writer.writeTheeByteUInt32(queryLength)
	if err != nil {
		return nil, err
	}

	err = writer.Writer.WriteByte(0)
	if err != nil {
		return nil, err
	}

	err = writer.Writer.WriteByte(_COM_QUERY)
	if err != nil {
		return nil, err
	}

	_, err = writer.Writer.Write([]byte(queryCommand))
	if err != nil {
		return nil, err
	}

	err = writer.Writer.Flush()

	if err != nil {
		return nil, err
	}

	rs := &resultSet{reader: reader}
	err = rs.init()
	if err != nil {
		return nil, err
	}
	return rs, err
}
