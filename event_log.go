package mysql_replication_listener

import (
	"fmt"
)

type (
	eventLog struct {
		mysqlConnection *connection
		binlogVersion uint16

		lastRotatePosition uint64
		lastRotateFileName []byte

		headerQueryEventLength byte
		headerDeleteRowsEventV1Length byte
		headerUpdateRowsEventV1Length byte
		headerWriteRowsEventV1Length byte
	}

	eventLogHeader struct {
		Timestamp uint32
		EventType byte
		ServerId  uint32
		EventSize uint32
		NextPosition    uint32
		Flags     uint16
	}

	logRotateEvent struct {
		*eventLogHeader
		position       uint64
		binlogFileName []byte
	}

	formatDescriptionEvent struct {
		*eventLogHeader
		binlogVersion uint16
		mysqlServerVersion []byte
		createTimestamp uint32
		eventTypeHeaderLengths []byte
	}

	startEventV3Event struct {
		*eventLogHeader
		binlogVersion uint16
		mysqlServerVersion []byte
		createTimestamp uint32
	}

	QueryEvent struct {
		*eventLogHeader
		SlaveProxyId uint32
		ExecutionTime uint32
		ErrorCode uint16
		StatusVars []byte
		Schema string
		Query string
	}

)

func (event *QueryEvent) read(pack *pack, binlogVersion uint16) {
	pack.readUint32(&event.SlaveProxyId)
	pack.readUint32(&event.ExecutionTime)

	schemaLength, _ := pack.ReadByte()

	pack.readUint16(&event.ErrorCode)

	if binlogVersion >= 4 {
		var statusVarsLength uint16
		pack.readUint16(&statusVarsLength)
		event.StatusVars = pack.Next(int(statusVarsLength))
	}

	event.Schema = string(pack.Next(int(schemaLength)))

	splitter, _ := pack.ReadByte()

	if splitter != 0 {
		panic("Incorrect binlog QueryEvent structure")
	}

	event.Query = string(pack.Bytes())
}

func (event *logRotateEvent) read(pack *pack) {
	pack.readUint64(&event.position)
	event.binlogFileName = pack.Bytes()
}

func (event *formatDescriptionEvent) read(pack *pack) {
	pack.readUint16(&event.binlogVersion)
	event.mysqlServerVersion = make([]byte, 50)
	pack.Read(event.mysqlServerVersion)
	pack.readUint32(&event.createTimestamp)
	length, _ := pack.ReadByte()
	event.eventTypeHeaderLengths = make([]byte, length)
	pack.Read(event.eventTypeHeaderLengths)
}

func (event *startEventV3Event) read(pack *pack) {
	pack.readUint16(&event.binlogVersion)
	event.mysqlServerVersion = make([]byte, 50)
	pack.Read(event.mysqlServerVersion)

	pack.readUint32(&event.createTimestamp)
}

func (eh *eventLogHeader) read(pack *pack){
	pack.ReadByte()
	pack.readUint32(&eh.Timestamp)
	eh.EventType, _ = pack.ReadByte()
	pack.readUint32(&eh.ServerId)
	pack.readUint32(&eh.EventSize)
	pack.readUint32(&eh.NextPosition)
	pack.readUint16(&eh.Flags)
}

func newEventLog(mysqlConnection *connection) *eventLog{
	return &eventLog{
		mysqlConnection: mysqlConnection,
	}
}

func (ev *eventLog) start() {
	for {
		event, err := ev.readEvent()
		if err != nil {
			println(err.Error())
		}

		switch e := event.(type) {
		case *startEventV3Event:
			ev.binlogVersion = e.binlogVersion
		case *formatDescriptionEvent:
			ev.binlogVersion = e.binlogVersion
//			fmt.Printf("% x\n", e.eventTypeHeaderLengths)
			ev.headerQueryEventLength = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_QUERY_POSITION]
//			ev.headerDeleteRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_DELETEV1_POSITION]
//			ev.headerUpdateRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_UPDATEV1_POSITION]
//			ev.headerWriteRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_WRITEV1_POSITION]
		case *logRotateEvent:
			ev.lastRotatePosition = e.position
			ev.lastRotateFileName = e.binlogFileName
		case *QueryEvent:
			println(e.Query)
			//redirect to chan
		}
	}
}

func (ev *eventLog) readEvent() (interface {}, error) {
	pack, err := ev.mysqlConnection.packReader.readNextPack()

	if err != nil {
		return nil, err
	}

	header := &eventLogHeader{}
	header.read(pack)

	switch header.EventType{
	case _START_EVENT_V3:
		event := &startEventV3Event{}
		event.eventLogHeader = header
		event.read(pack)
		return event, nil
	case _FORMAT_DESCRIPTION_EVENT:
		event := &formatDescriptionEvent{}
		event.eventLogHeader = header
		event.read(pack)
		return event, nil
	case _ROTATE_EVENT:
		event := &logRotateEvent{}
		event.eventLogHeader = header
		event.read(pack)
		return event, nil
	case _QUERY_EVENT:
		event := &QueryEvent{}
		event.eventLogHeader = header
		event.read(pack, ev.binlogVersion)
		return event, nil
	default:
		println("Unknown event")
		println(fmt.Sprintf("% x\n", pack.buff))

	}

	return nil, nil
}


//type (
//	eventLogParser struct {
//		reader *protoReader
//	}
//

//

//
//	eventLogFormatDescriptionEvent struct {
//		*eventLogHeader
//		binlogVersion          uint16
//		mysqlServerVersion     string
//		timestamp              uint32
//		eventHeaderLength      byte
//		eventTypeHeaderLengths []byte
//	}
//
//	eventLogFormatUpdateEventV2 struct {
//		*eventLogHeader
//		tableId         uint32
//		flags           uint16
//		extraDataLength uint16
//		extraData       []byte
//		columnsCount    uint64
//	}
//
//	eventLogQueryEvent struct {
//		*eventLogHeader
//		slaveProxyId     uint32
//		executionTime    uint32
//		schemaLength     byte
//		errorCode        uint16
//		statusVarsLength uint16
//		statusVars       []byte
//		schema           []byte
//		query            []byte
//	}
//)
//
//func (re *eventLogQueryEvent) readEvent(reader *protoReader) {
//	re.slaveProxyId, _ = reader.readUint32()
//	re.executionTime, _ = reader.readUint32()
//	re.schemaLength, _ = reader.Reader.ReadByte()
//	re.errorCode, _ = reader.readUint16()
//	re.statusVarsLength, _ = reader.readUint16()
//
//	re.statusVars = make([]byte, re.statusVarsLength)
//	reader.Reader.Read(re.statusVars)
//
//	re.schema = make([]byte, re.schemaLength)
//	reader.Reader.Read(re.schema)
//
//	reader.Reader.ReadByte()
//
//	queryLength := re.eventLogHeader.eventSize - (19 + 4 + 4 + 1 + 2 + 2 + uint32(re.statusVarsLength) + uint32(re.schemaLength) + 1)
//	re.query = make([]byte, queryLength)
//	reader.Reader.Read(re.query)
//}
//
//func (re *eventLogFormatUpdateEventV2) readEvent(reader *protoReader) {
//	buff := make([]byte, re.eventSize-19)
//	reader.Reader.Read(buff)
//	fmt.Printf("%v\n", "fff")
//	fmt.Printf("% x\n", buff)
//	re.tableId, _ = reader.readThreeBytesUint32()
//	re.flags, _ = reader.readUint16()
//	re.extraDataLength, _ = reader.readUint16()
//	if re.extraDataLength-2 > 0 {
//		re.extraData = make([]byte, re.extraDataLength-2)
//		reader.Reader.Read(re.extraData)
//	} else {
//		re.extraData = []byte{}
//	}
//	re.columnsCount, _, _ = reader.readIntOrNil()
//}
//
//func (re *eventLogRotateEvent) readEvent(reader *protoReader) {
//	re.position, _ = reader.readUint64()
//	buff := make([]byte, re.eventSize-(8+19))
//	reader.Reader.Read(buff)
//	re.binlogFileName = string(buff)
//}
//
//func (re *eventLogFormatDescriptionEvent) readEvent(reader *protoReader) {
//	re.binlogVersion, _ = reader.readUint16()
//	buff := make([]byte, 50)
//	reader.Reader.Read(buff)
//	re.mysqlServerVersion = string(buff)
//	re.timestamp, _ = reader.readUint32()
//	re.eventHeaderLength, _ = reader.Reader.ReadByte()
//	buff = make([]byte, re.eventSize-(57+19))
//	reader.Reader.Read(buff)
//}
//
//func newEventLogParser(reader *protoReader) *eventLogParser {
//	return &eventLogParser{
//		reader,
//	}
//}
//
//func (ev *eventLogParser) read() (interface{}, error) {
//	ev.reader.readThreeBytesUint32()
//	ev.reader.Reader.ReadByte()
//
//	header := &eventLogHeader{}
//	header.read(ev.reader)
//
//	switch header.eventType {
//	case _ROTATE_EVENT:
//		event := &eventLogRotateEvent{
//			eventLogHeader: header,
//		}
//		event.readEvent(ev.reader)
//		return event, nil
//	case _FORMAT_DESCRIPTION_EVENT:
//		event := &eventLogFormatDescriptionEvent{
//			eventLogHeader: header,
//		}
//		event.readEvent(ev.reader)
//		return event, nil
//	case _UPDATE_ROWS_EVENTv2:
//		event := &eventLogFormatUpdateEventV2{
//			eventLogHeader: header,
//		}
//		event.readEvent(ev.reader)
//		return event, nil
//	case _QUERY_EVENT:
//		event := &eventLogQueryEvent{
//			eventLogHeader: header,
//		}
//		event.readEvent(ev.reader)
//		return event, nil
//	default:
//		println(header.eventType)
//		println("Unknown event")
//	}
//
//	return nil, nil
//}
//
//func (ev *eventLogHeader) read(reader *protoReader) error {
//	reader.Reader.ReadByte()
//	ev.timestamp, _ = reader.readUint32()
//	ev.eventType, _ = reader.Reader.ReadByte()
//	ev.serverId, _ = reader.readUint32()
//	ev.eventSize, _ = reader.readUint32()
//	ev.logPos, _ = reader.readUint32()
//	ev.flags, _ = reader.readUint16()
//	return nil
//}
