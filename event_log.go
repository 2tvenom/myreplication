package mysql_replication_listener

import "fmt"

type (
	eventLogParser struct {
		reader *protoReader
	}

	eventLogHeader struct {
		timestamp uint32
		eventType byte
		serverId  uint32
		eventSize uint32
		logPos    uint32
		flags     uint16
	}

	eventLogRotateEvent struct {
		*eventLogHeader
		position       uint64
		binlogFileName string
	}

	eventLogFormatDescriptionEvent struct {
		*eventLogHeader
		binlogVersion          uint16
		mysqlServerVersion     string
		timestamp              uint32
		eventHeaderLength      byte
		eventTypeHeaderLengths []byte
	}

	eventLogFormatUpdateEventV2 struct {
		*eventLogHeader
		tableId         uint32
		flags           uint16
		extraDataLength uint16
		extraData       []byte
		columnsCount    uint64
	}

	eventLogQueryEvent struct {
		*eventLogHeader
		slaveProxyId     uint32
		executionTime    uint32
		schemaLength     byte
		errorCode        uint16
		statusVarsLength uint16
		statusVars       []byte
		schema           []byte
		query            []byte
	}
)

func (re *eventLogQueryEvent) readEvent(reader *protoReader) {
	re.slaveProxyId, _ = reader.readUint32()
	re.executionTime, _ = reader.readUint32()
	re.schemaLength, _ = reader.Reader.ReadByte()
	re.errorCode, _ = reader.readUint16()
	re.statusVarsLength, _ = reader.readUint16()

	re.statusVars = make([]byte, re.statusVarsLength)
	reader.Reader.Read(re.statusVars)

	re.schema = make([]byte, re.schemaLength)
	reader.Reader.Read(re.schema)

	reader.Reader.ReadByte()

	queryLength := re.eventLogHeader.eventSize - (19 + 4 + 4 + 1 + 2 + 2 + uint32(re.statusVarsLength) + uint32(re.schemaLength) + 1)
	re.query = make([]byte, queryLength)
	reader.Reader.Read(re.query)
}

func (re *eventLogFormatUpdateEventV2) readEvent(reader *protoReader) {
	buff := make([]byte, re.eventSize-19)
	reader.Reader.Read(buff)
	fmt.Printf("%v\n", "fff")
	fmt.Printf("% x\n", buff)
	re.tableId, _ = reader.readThreeBytesUint32()
	re.flags, _ = reader.readUint16()
	re.extraDataLength, _ = reader.readUint16()
	if re.extraDataLength-2 > 0 {
		re.extraData = make([]byte, re.extraDataLength-2)
		reader.Reader.Read(re.extraData)
	} else {
		re.extraData = []byte{}
	}
	re.columnsCount, _, _ = reader.readIntOrNil()
}

func (re *eventLogRotateEvent) readEvent(reader *protoReader) {
	re.position, _ = reader.readUint64()
	buff := make([]byte, re.eventSize-(8+19))
	reader.Reader.Read(buff)
	re.binlogFileName = string(buff)
}

func (re *eventLogFormatDescriptionEvent) readEvent(reader *protoReader) {
	re.binlogVersion, _ = reader.readUint16()
	buff := make([]byte, 50)
	reader.Reader.Read(buff)
	re.mysqlServerVersion = string(buff)
	re.timestamp, _ = reader.readUint32()
	re.eventHeaderLength, _ = reader.Reader.ReadByte()
	buff = make([]byte, re.eventSize-(57+19))
	reader.Reader.Read(buff)
}

func newEventLogParser(reader *protoReader) *eventLogParser {
	return &eventLogParser{
		reader,
	}
}

func (ev *eventLogParser) read() (interface{}, error) {
	ev.reader.readThreeBytesUint32()
	ev.reader.Reader.ReadByte()

	header := &eventLogHeader{}
	header.read(ev.reader)

	switch header.eventType {
	case _ROTATE_EVENT:
		event := &eventLogRotateEvent{
			eventLogHeader: header,
		}
		event.readEvent(ev.reader)
		return event, nil
	case _FORMAT_DESCRIPTION_EVENT:
		event := &eventLogFormatDescriptionEvent{
			eventLogHeader: header,
		}
		event.readEvent(ev.reader)
		return event, nil
	case _UPDATE_ROWS_EVENTv2:
		event := &eventLogFormatUpdateEventV2{
			eventLogHeader: header,
		}
		event.readEvent(ev.reader)
		return event, nil
	case _QUERY_EVENT:
		event := &eventLogQueryEvent{
			eventLogHeader: header,
		}
		event.readEvent(ev.reader)
		return event, nil
	default:
		println(header.eventType)
		println("Unknown event")
	}

	return nil, nil
}

func (ev *eventLogHeader) read(reader *protoReader) error {
	reader.Reader.ReadByte()
	ev.timestamp, _ = reader.readUint32()
	ev.eventType, _ = reader.Reader.ReadByte()
	ev.serverId, _ = reader.readUint32()
	ev.eventSize, _ = reader.readUint32()
	ev.logPos, _ = reader.readUint32()
	ev.flags, _ = reader.readUint16()
	return nil
}
