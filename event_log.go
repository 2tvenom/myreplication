package mysql_replication_listener

import (
	"fmt"
)

type (
	eventLog struct {
		mysqlConnection *connection
		binlogVersion   uint16

		lastRotatePosition uint64
		lastRotateFileName []byte

		headerQueryEventLength        byte
		headerDeleteRowsEventV1Length byte
		headerUpdateRowsEventV1Length byte
		headerWriteRowsEventV1Length  byte
	}

	eventLogHeader struct {
		Timestamp    uint32
		EventType    byte
		ServerId     uint32
		EventSize    uint32
		NextPosition uint32
		Flags        uint16
	}

	logRotateEvent struct {
		*eventLogHeader
		position       uint64
		binlogFileName []byte
	}

	formatDescriptionEvent struct {
		*eventLogHeader
		binlogVersion          uint16
		mysqlServerVersion     []byte
		createTimestamp        uint32
		eventTypeHeaderLengths []byte
	}

	startEventV3Event struct {
		*eventLogHeader
		binlogVersion      uint16
		mysqlServerVersion []byte
		createTimestamp    uint32
	}

	QueryEvent struct {
		*eventLogHeader
		SlaveProxyId  uint32
		ExecutionTime uint32
		ErrorCode     uint16
		StatusVars    []byte
		Schema        string
		Query         string
		binLogVersion uint16
	}

	XidEvent struct {
		*eventLogHeader
		TransactionId uint64
	}

	IntVarEvent struct {
		*eventLogHeader
		Type  byte
		Value uint64
	}

	BeginLoadQueryEvent struct {
		*eventLogHeader
		FileId    uint32
		BlockData string
	}

	AppendBlockEvent BeginLoadQueryEvent

	ExecuteLoadQueryEvent struct {
		*eventLogHeader
		SlaveProxyId     uint32
		ExecutionTime    uint32
		ErrorCode        uint16
		StatusVars       []byte
		Schema           string
		FileId           uint32
		StartPos         uint32
		EndPos           uint32
		DupHandlingFlags byte
		Query            string
	}

	binLogEvent interface {
		read(*pack)
	}
)

func (event *ExecuteLoadQueryEvent) read(pack *pack) {
	pack.readUint32(&event.SlaveProxyId)
	pack.readUint32(&event.ExecutionTime)

	schemaLength, _ := pack.ReadByte()

	pack.readUint16(&event.ErrorCode)

	var statusVarsLength uint16
	pack.readUint16(&statusVarsLength)

	pack.readUint32(&event.FileId)
	pack.readUint32(&event.StartPos)
	pack.readUint32(&event.EndPos)
	event.DupHandlingFlags, _ = pack.ReadByte()

	event.StatusVars = pack.Next(int(statusVarsLength))
	event.Schema = string(pack.Next(int(schemaLength)))

	splitter, _ := pack.ReadByte()

	if splitter != 0 {
		panic("Incorrect binlog EXECUTE_LOAD_QUERY_EVENT structure")
	}

	event.Query = string(pack.Bytes())
}

func (event *BeginLoadQueryEvent) read(pack *pack) {
	pack.readUint32(&event.FileId)
	event.BlockData = string(pack.Bytes())
}

func (event *IntVarEvent) read(pack *pack) {
	event.Type, _ = pack.ReadByte()
	pack.readUint64(&event.Value)
}

func (event *XidEvent) read(pack *pack) {
	pack.readUint64(&event.TransactionId)
}

func (event *QueryEvent) read(pack *pack) {
	pack.readUint32(&event.SlaveProxyId)
	pack.readUint32(&event.ExecutionTime)

	schemaLength, _ := pack.ReadByte()

	pack.readUint16(&event.ErrorCode)

	if event.binLogVersion >= 4 {
		var statusVarsLength uint16
		pack.readUint16(&statusVarsLength)
		event.StatusVars = pack.Next(int(statusVarsLength))
	}

	event.Schema = string(pack.Next(int(schemaLength)))

	splitter, _ := pack.ReadByte()

	if splitter != 0 {
		panic("Incorrect binlog QUERY_EVENT structure")
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

func (eh *eventLogHeader) read(pack *pack) {
	pack.ReadByte()
	pack.readUint32(&eh.Timestamp)
	eh.EventType, _ = pack.ReadByte()
	pack.readUint32(&eh.ServerId)
	pack.readUint32(&eh.EventSize)
	pack.readUint32(&eh.NextPosition)
	pack.readUint16(&eh.Flags)
}

func newEventLog(mysqlConnection *connection) *eventLog {
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
		case *XidEvent:
			println(e.TransactionId)
			//redirect to chan
		case *IntVarEvent:
			println(e.Type)
			println(e.Value)
			//redirect to chan
		case *BeginLoadQueryEvent:
			println(e.BlockData)
			//redirect to chan
		case *AppendBlockEvent:
			println(e.BlockData)
			//redirect to chan
		case *ExecuteLoadQueryEvent:
			println(e.Query)
			//redirect to chan
		}
	}
}

func (ev *eventLog) readEvent() (interface{}, error) {
	pack, err := ev.mysqlConnection.packReader.readNextPack()

	if err != nil {
		return nil, err
	}

	header := &eventLogHeader{}
	header.read(pack)

	var event binLogEvent

	switch header.EventType {
	case _START_EVENT_V3:
		event = &startEventV3Event{
			eventLogHeader: header,
		}
	case _FORMAT_DESCRIPTION_EVENT:
		event = &formatDescriptionEvent{
			eventLogHeader: header,
		}
	case _ROTATE_EVENT:
		event = &logRotateEvent{
			eventLogHeader: header,
		}
	case _QUERY_EVENT:
		event = &QueryEvent{
			eventLogHeader: header,
			binLogVersion:  ev.binlogVersion,
		}
	case _XID_EVENT:
		event = &XidEvent{
			eventLogHeader: header,
		}
	case _INTVAR_EVENT:
		event = &IntVarEvent{
			eventLogHeader: header,
		}
	case _BEGIN_LOAD_QUERY_EVENT:
		event = &BeginLoadQueryEvent{
			eventLogHeader: header,
		}
	case _APPEND_BLOCK_EVENT:
		event = &AppendBlockEvent{
			eventLogHeader: header,
		}
	case _EXECUTE_LOAD_QUERY_EVENT:
		event = &ExecuteLoadQueryEvent{
			eventLogHeader: header,
		}
	default:
		println("Unknown event")
		println(fmt.Sprintf("% x\n", pack.buff))
		return nil, nil
	}

	event.read(pack)
	return event, nil
}
