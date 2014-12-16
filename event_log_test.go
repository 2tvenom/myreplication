package mysql_replication_listener

import (
	"bytes"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

type (
	rowEventTestCase struct {
		tableMapEventBuff []byte
		rowsEventBuff     []byte
		expectedValues    [][]*RowsEventValue
		expectedNewValues [][]*RowsEventValue
	}
)

func TestBinlogRotateEvent(t *testing.T) {
	mockHandshake := []byte{
		//length
		0x2C, 0x00, 0x00,
		0x01,
		0x00,
		0x00, 0x00, 0x00, 0x00,
		0x04,
		0x01, 0x00, 0x00, 0x00,
		0x2b, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x20, 0x00,
		0x3f, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x6d, 0x79, 0x73, 0x71, 0x6c, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x33,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	logRotate := &logRotateEvent{}
	logRotate.eventLogHeader = header
	logRotate.read(pack)

	var expectedTimeStamp uint32 = 0

	if logRotate.Timestamp != expectedTimeStamp {
		t.Fatal(
			"Incorrect timestamp",
			"expected", expectedTimeStamp,
			"got", logRotate.Timestamp,
		)
	}

	if logRotate.EventType != _ROTATE_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _ROTATE_EVENT,
			"got", logRotate.EventType,
		)
	}

	var expectedServerId uint32 = 1

	if logRotate.ServerId != expectedServerId {
		t.Fatal(
			"Incorrect server id",
			"expected", expectedServerId,
			"got", logRotate.ServerId,
		)
	}

	var expectedEvenSize uint32 = 43

	if logRotate.EventSize != expectedEvenSize {
		t.Fatal(
			"Incorrect event size",
			"expected", expectedEvenSize,
			"got", logRotate.EventSize,
		)
	}

	var expectedNextPosition uint32 = 0

	if logRotate.NextPosition != expectedNextPosition {
		t.Fatal(
			"Incorrect next position",
			"expected", expectedNextPosition,
			"got", logRotate.NextPosition,
		)
	}

	var expectedFlags uint16 = 32

	if logRotate.Flags != expectedFlags {
		t.Fatal(
			"Incorrect flags",
			"expected", expectedFlags,
			"got", logRotate.Flags,
		)
	}

	var expectedPosition uint64 = 575

	if logRotate.position != expectedPosition {
		t.Fatal(
			"Incorrect position",
			"expected", expectedPosition,
			"got", logRotate.position,
		)
	}

	expectedBinlogFileName := "mysql-bin.000003"

	if string(logRotate.binlogFileName) != expectedBinlogFileName {
		t.Fatal(
			"Incorrect file name",
			"expected", expectedBinlogFileName,
			"got", string(logRotate.binlogFileName),
		)
	}
}

func TestFormatDescriptionEvent(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0x68, 0x00, 0x00,
		0x01,
		//event header
		0x00,
		0x67, 0x6f, 0x66, 0x54,
		0x0f,
		0x01, 0x00, 0x00, 0x00,
		0x67, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
		//event
		0x04, 0x00,
		0x35, 0x2e, 0x35, 0x2e, 0x33, 0x38, 0x2d, 0x30, 0x75, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x30, 0x2e, 0x31, 0x34,
		0x2e, 0x30, 0x34, 0x2e, 0x31, 0x2d, 0x6c, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x13,
		0x38, 0x0d, 0x00, 0x08, 0x00, 0x12, 0x00, 0x04, 0x04, 0x04, 0x04, 0x12, 0x00, 0x00, 0x54, 0x00, 0x04, 0x1a,
		0x08, 0x00, 0x00, 0x00, 0x08, 0x08, 0x08, 0x02, 0x00,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	formatDesc := &formatDescriptionEvent{}
	formatDesc.eventLogHeader = header
	formatDesc.read(pack)

	if formatDesc.EventType != _FORMAT_DESCRIPTION_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _FORMAT_DESCRIPTION_EVENT,
			"got", formatDesc.EventType,
		)
	}

	var expectedBinlogVersion uint16 = 4

	if formatDesc.binlogVersion != expectedBinlogVersion {
		t.Fatal(
			"Incorrect binlog version",
			"expected", expectedBinlogVersion,
			"got", formatDesc.binlogVersion,
		)
	}

	expectedMysqlServerVersion := "5.5.38-0ubuntu0.14.04.1-log"
	resultVersion := strings.TrimRight(string(formatDesc.mysqlServerVersion), "\x00")
	if resultVersion != expectedMysqlServerVersion {
		t.Fatal(
			"Incorrect mysql version",
			"expected", expectedMysqlServerVersion,
			"got", resultVersion,
		)
	}

}

func TestStartEventV3Event(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0x4C, 0x00, 0x00,
		0x01,
		//event header
		0x00,
		0x67, 0x6f, 0x66, 0x54,
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x67, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
		//event
		0x03, 0x00,
		0x35, 0x2e, 0x35, 0x2e, 0x33, 0x38, 0x2d, 0x30, 0x75, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x30, 0x2e, 0x31, 0x34,
		0x2e, 0x30, 0x34, 0x2e, 0x31, 0x2d, 0x6c, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	formatDesc := &startEventV3Event{}
	formatDesc.eventLogHeader = header
	formatDesc.read(pack)

	if formatDesc.EventType != _START_EVENT_V3 {
		t.Fatal(
			"Incorrect event type",
			"expected", _FORMAT_DESCRIPTION_EVENT,
			"got", formatDesc.EventType,
		)
	}

	var expectedBinlogVersion uint16 = 3

	if formatDesc.binlogVersion != expectedBinlogVersion {
		t.Fatal(
			"Incorrect binlog version",
			"expected", expectedBinlogVersion,
			"got", formatDesc.binlogVersion,
		)
	}

	expectedMysqlServerVersion := "5.5.38-0ubuntu0.14.04.1-log"
	resultVersion := strings.TrimRight(string(formatDesc.mysqlServerVersion), "\x00")
	if resultVersion != expectedMysqlServerVersion {
		t.Fatal(
			"Incorrect mysql version",
			"expected", expectedMysqlServerVersion,
			"got", resultVersion,
		)
	}

}

func TestQueryEvent(t *testing.T) {
	mockHandshake := []byte{
		0x45, 0x00, 0x00, 0x01, 0x00, 0xeb, 0x26, 0x7e, 0x54, 0x02, 0x01, 0x00, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00,
		0xcf, 0xec, 0x01, 0x00, 0x08, 0x00, 0xa5, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73,
		0x74, 0x64, 0x04, 0x21, 0x00, 0x21, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x42, 0x45, 0x47, 0x49,
		0x4e, 0x6D, 0x00, 0x00, 0x02, 0x00, 0xeb, 0x26, 0x7e, 0x54, 0x02, 0x01, 0x00, 0x00, 0x00, 0x6c, 0x00, 0x00,
		0x00, 0x3b, 0xed, 0x01, 0x00, 0x00, 0x00, 0xa5, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
		0x1a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03,
		0x73, 0x74, 0x64, 0x04, 0x21, 0x00, 0x21, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x75, 0x70, 0x64,
		0x61, 0x74, 0x65, 0x20, 0x74, 0x65, 0x73, 0x74, 0x2e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31, 0x20, 0x73,
		0x65, 0x74, 0x20, 0x64, 0x61, 0x74, 0x61, 0x3d, 0x27, 0x66, 0x66, 0x31, 0x27, 0x20, 0x77, 0x68, 0x65, 0x72,
		0x65, 0x20, 0x69, 0x64, 0x3d, 0x31,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))

	type (
		testCase struct {
			query         string
			schema        string
			errorCode     uint16
			executionTime uint32
		}
	)

	cases := []*testCase{
		&testCase{"BEGIN", "test", 0, 0},
		&testCase{"update test.table01 set data='ff1' where id=1", "test", 0, 0},
	}

	for i, test := range cases {
		t.Log("Test Query event pack", i)
		pack, _ := packReader.readNextPack()

		header := &eventLogHeader{}
		header.readHead(pack)

		query := &QueryEvent{
			eventLogHeader: header,
			binLogVersion:  4,
		}
		query.read(pack)

		if query.GetQuery() != test.query {
			t.Fatal(
				"Incorrect query",
				"expected", test.query,
				"got", query.GetQuery(),
			)
		}

		if query.GetSchema() != test.schema {
			t.Fatal(
				"Incorrect schema",
				"expected", test.schema,
				"got", query.GetSchema(),
			)
		}

		if query.GetErrorCode() != test.errorCode {
			t.Fatal(
				"Incorrect error code",
				"expected", test.errorCode,
				"got", query.GetErrorCode(),
			)
		}

		if query.GetExecutionTime() != test.executionTime {
			t.Fatal(
				"Incorrect execution time",
				"expected", test.executionTime,
				"got", query.GetExecutionTime(),
			)
		}
	}
}

func TestXidEvent(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0x1C, 0x00, 0x00,
		0x01,
		//event header
		0x00, 0x96, 0x34, 0x7e, 0x54, 0x10, 0x01, 0x00, 0x00, 0x00, 0x1b, 0x00, 0x00, 0x00, 0xec, 0xee, 0x01, 0x00,
		0x00, 0x00, 0x08, 0x75, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	xid := &XidEvent{}
	xid.eventLogHeader = header
	xid.read(pack)

	if xid.EventType != _XID_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _XID_EVENT,
			"got", xid.EventType,
		)
	}

	var expectedTransactionId uint64 = 29960

	if xid.TransactionId != expectedTransactionId {
		t.Fatal(
			"Incorrect transaction id",
			"expected", expectedTransactionId,
			"got", xid.TransactionId,
		)
	}

}

func TestIntVarEvent(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0x1D, 0x00, 0x00,
		0x01,
		//event header
		0x00, 0x65, 0x6f, 0x7f, 0x54, 0x05, 0x01, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x6a, 0xf2, 0x01, 0x00,
		0x00, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	intVar := &IntVarEvent{}
	intVar.eventLogHeader = header
	intVar.read(pack)

	if intVar.EventType != _INTVAR_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _INTVAR_EVENT,
			"got", intVar.EventType,
		)
	}

	var expectedValue uint64 = 1

	if intVar.GetValue() != expectedValue {
		t.Fatal(
			"Incorrect value",
			"expected", expectedValue,
			"got", intVar.GetValue(),
		)
	}

	if intVar.GetType() != INSERT_ID_EVENT {
		t.Fatal(
			"Incorrect type",
			"expected", INSERT_ID_EVENT,
			"got", intVar.GetType(),
		)
	}
}

func TestBeginLoadQueryEvent(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0x28, 0x00, 0x00,
		0x01,
		//event header
		0x00, 0xce, 0x7c, 0x7f, 0x54, 0x11, 0x01, 0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x00, 0xa3, 0xf7, 0x01, 0x00,
		0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x35, 0x2c, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0a, 0x36, 0x2c, 0x77, 0x6f,
		0x72, 0x6c, 0x64, 0x0a,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	beginEvent := &BeginLoadQueryEvent{}
	beginEvent.eventLogHeader = header
	beginEvent.read(pack)

	if beginEvent.EventType != _BEGIN_LOAD_QUERY_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _BEGIN_LOAD_QUERY_EVENT,
			"got", beginEvent.EventType,
		)
	}

	var expectedFileId uint32 = 1

	if beginEvent.fileId != expectedFileId {
		t.Fatal(
			"Incorrect file id",
			"expected", expectedFileId,
			"got", beginEvent.fileId,
		)
	}

	expectedBlockData := "5,hello\n6,world\n"

	if beginEvent.GetData() != expectedBlockData {
		t.Fatal(
			"Incorrect block data",
			"expected", expectedBlockData,
			"got", beginEvent.GetData(),
		)
	}
}

func TestExecuteLoadQueryEvent(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0xD9, 0x00, 0x00,
		0x01,
		//event header
		0x00, 0xce, 0x7c, 0x7f, 0x54, 0x12, 0x01, 0x00, 0x00, 0x00, 0xd8, 0x00, 0x00, 0x00, 0x7b, 0xf8, 0x01, 0x00,
		0x00, 0x00, 0xd8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x09, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x21, 0x00, 0x21, 0x00, 0x08, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x00, 0x4c, 0x4f, 0x41, 0x44, 0x20, 0x44, 0x41, 0x54, 0x41, 0x20, 0x49, 0x4e, 0x46,
		0x49, 0x4c, 0x45, 0x20, 0x27, 0x64, 0x61, 0x74, 0x61, 0x27, 0x20, 0x49, 0x4e, 0x54, 0x4f, 0x20, 0x54, 0x41,
		0x42, 0x4c, 0x45, 0x20, 0x60, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31, 0x60, 0x20, 0x46, 0x49, 0x45, 0x4c,
		0x44, 0x53, 0x20, 0x54, 0x45, 0x52, 0x4d, 0x49, 0x4e, 0x41, 0x54, 0x45, 0x44, 0x20, 0x42, 0x59, 0x20, 0x27,
		0x2c, 0x27, 0x20, 0x45, 0x4e, 0x43, 0x4c, 0x4f, 0x53, 0x45, 0x44, 0x20, 0x42, 0x59, 0x20, 0x27, 0x27, 0x20,
		0x45, 0x53, 0x43, 0x41, 0x50, 0x45, 0x44, 0x20, 0x42, 0x59, 0x20, 0x27, 0x5c, 0x5c, 0x27, 0x20, 0x4c, 0x49,
		0x4e, 0x45, 0x53, 0x20, 0x54, 0x45, 0x52, 0x4d, 0x49, 0x4e, 0x41, 0x54, 0x45, 0x44, 0x20, 0x42, 0x59, 0x20,
		0x27, 0x5c, 0x6e, 0x27, 0x20, 0x28, 0x60, 0x69, 0x64, 0x60, 0x2c, 0x20, 0x60, 0x64, 0x61, 0x74, 0x61, 0x60,
		0x29,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	executeLoad := &ExecuteLoadQueryEvent{}
	executeLoad.eventLogHeader = header
	executeLoad.read(pack)

	if executeLoad.EventType != _EXECUTE_LOAD_QUERY_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _EXECUTE_LOAD_QUERY_EVENT,
			"got", executeLoad.EventType,
		)
	}

	expectedSchema := "test"

	if executeLoad.GetSchema() != expectedSchema {
		t.Fatal(
			"Incorrect schema",
			"expected", expectedSchema,
			"got", executeLoad.GetSchema(),
		)
	}

	var expectedErrorCode uint16 = 0

	if executeLoad.GetErrorCode() != expectedErrorCode {
		t.Fatal(
			"Incorrect error code",
			"expected", expectedErrorCode,
			"got", executeLoad.GetErrorCode(),
		)
	}

	var expectedExecutionTime uint32 = 0

	if executeLoad.GetExecutionTime() != expectedExecutionTime {
		t.Fatal(
			"Incorrect execution time",
			"expected", expectedExecutionTime,
			"got", executeLoad.GetExecutionTime(),
		)
	}

	expectedQuery := "LOAD DATA INFILE 'data' INTO TABLE `table01` FIELDS TERMINATED BY ',' ENCLOSED BY '' "
	expectedQuery += "ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' (`id`, `data`)"

	if executeLoad.GetQuery() != expectedQuery {
		t.Fatal(
			"Incorrect query",
			"expected", expectedQuery,
			"got", executeLoad.GetQuery(),
		)
	}
}

func TestUserVarEvent(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0x2F, 0x00, 0x00,
		0x01,
		//event header
		0x00, 0x47, 0xc0, 0x80, 0x54, 0x0e, 0x01, 0x00, 0x00, 0x00, 0x2e, 0x00, 0x00, 0x00, 0x4c, 0xfd, 0x01, 0x00,
		0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x76, 0x61, 0x72, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x00, 0x00, 0x21, 0x00,
		0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x73, 0x73, 0x73, 0x73, 0x73,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	userVar := &UserVarEvent{}
	userVar.eventLogHeader = header
	userVar.read(pack)

	if userVar.EventType != _USER_VAR_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _USER_VAR_EVENT,
			"got", userVar.EventType,
		)
	}

	expectedName := "var_name"

	if userVar.GetName() != expectedName {
		t.Fatal(
			"Incorrect variable name",
			"expected", expectedName,
			"got", userVar.GetName(),
		)
	}

	expectedIsNil := false

	if userVar.IsNil() != expectedIsNil {
		t.Fatal(
			"Incorrect is_null",
			"expected", expectedIsNil,
			"got", userVar.IsNil(),
		)
	}

	expectedValue := "sssss"

	if userVar.GetValue() != expectedValue {
		t.Fatal(
			"Incorrect value",
			"expected", expectedValue,
			"got", userVar.GetValue(),
		)
	}
}

func TestRandEvent(t *testing.T) {
	mockHandshake := []byte{
		//pack header
		0x24, 0x00, 0x00,
		0x01,
		//event header
		0x00, 0xc6, 0xce, 0x80, 0x54, 0x0d, 0x01, 0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x9b, 0x02, 0x00, 0x00,
		0x00, 0x00, 0xbf, 0xfa, 0x4e, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x76, 0x1c, 0x04, 0x3c, 0x00, 0x00, 0x00, 0x00,
	}

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	rand := &RandEvent{}
	rand.eventLogHeader = header
	rand.read(pack)

	if rand.EventType != _RAND_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _RAND_EVENT,
			"got", rand.EventType,
		)
	}

	var expectedSeed1 uint64 = 508492479
	var expectedSeed2 uint64 = 1006902390

	if rand.GetSeed1() != expectedSeed1 {
		t.Fatal(
			"Incorrect seed1",
			"expected", expectedSeed1,
			"got", rand.GetSeed1(),
		)
	}

	if rand.GetSeed2() != expectedSeed2 {
		t.Fatal(
			"Incorrect seed2",
			"expected", expectedSeed2,
			"got", rand.GetSeed2(),
		)
	}
}

func getTableMapEvent(mockHandshake []byte) *TableMapEvent {
	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	header := &eventLogHeader{}
	header.readHead(pack)

	table := &TableMapEvent{}
	table.eventLogHeader = header
	table.read(pack)

	return table
}

func TestTableMapEvent(t *testing.T) {

	mockHandshake := []byte{
		//pack header
		0x49, 0x00, 0x00,
		0x01,
		//event header
		0x00,
		0x5d, 0xff, 0x86, 0x54,
		0x13,
		0x01, 0x00, 0x00, 0x00,
		0x48, 0x00, 0x00, 0x00,
		0x34, 0x06, 0x00, 0x00,
		0x00, 0x00,
		//body
		//table id
		0x2c, 0x00, 0x00, 0x00, 0x00, 0x00,
		//flags
		0x01, 0x00,
		//schema length
		0x04,
		//schema name "test"
		0x74, 0x65, 0x73, 0x74,
		//filler
		0x00,
		//table name length
		0x05,
		//table name "types"
		0x74, 0x79, 0x70, 0x65, 0x73,
		//filler
		0x00,
		//column count
		0x13,
		//column count def
		0x03, 0x01, 0x01, 0x02, 0x02, 0x09, 0x09, 0x03, 0x03, 0x03, 0x03, 0x08, 0x08, 0xf6, 0xf6, 0x05, 0x05, 0x04, 0x04,
		//meta info length
		0x08,
		//meta info
		0x0a, 0x00, 0x0a, 0x00, 0x08, 0x08, 0x04, 0x04,
		//bit mask
		0x6e, 0xfb, 0x07,
	}

	table := getTableMapEvent(mockHandshake)

	if table.EventType != _TABLE_MAP_EVENT {
		t.Fatal(
			"Incorrect event type",
			"expected", _TABLE_MAP_EVENT,
			"got", table.EventType,
		)
	}

	expectedSchema := "test"
	if table.SchemaName != expectedSchema {
		t.Fatal(
			"Incorrect schema name",
			"expected", expectedSchema,
			"got", table.SchemaName,
		)
	}

	expectedTable := "types"
	if table.TableName != expectedTable {
		t.Fatal(
			"Incorrect table name",
			"expected", expectedTable,
			"got", table.TableName,
		)
	}

	expectedColumnCount := 19
	if len(table.Columns) != expectedColumnCount {
		t.Fatal(
			"Incorrect column count",
			"expected", expectedColumnCount,
			"got", len(table.Columns),
		)
	}

	type (
		TableMapEventTest struct {
			expectedType     byte
			expectedIsNull   bool
			expectedMetaInfo []byte
		}
	)

	testsCases := []*TableMapEventTest{
		&TableMapEventTest{MYSQL_TYPE_LONG, false, []byte{}},                // `id` int(11) NOT NULL AUTO_INCREMENT,
		&TableMapEventTest{MYSQL_TYPE_TINY, true, []byte{}},                 // `i1` tinyint(4) DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_TINY, true, []byte{}},                 // `i2` tinyint(3) unsigned DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_SHORT, true, []byte{}},                // `i3` smallint(6) DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_SHORT, false, []byte{}},               // `i4` smallint(5) unsigned NOT NULL,
		&TableMapEventTest{MYSQL_TYPE_INT24, true, []byte{}},                // `i5` mediumint(9) DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_INT24, true, []byte{}},                // `i6` mediumint(8) unsigned DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_LONG, false, []byte{}},                // `i7` int(11) NOT NULL,
		&TableMapEventTest{MYSQL_TYPE_LONG, true, []byte{}},                 // `i8` int(10) unsigned DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_LONG, true, []byte{}},                 // `i9` int(11) DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_LONG, false, []byte{}},                // `1i0` int(10) unsigned NOT NULL,
		&TableMapEventTest{MYSQL_TYPE_LONGLONG, true, []byte{}},             // `1i1` bigint(20) DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_LONGLONG, true, []byte{}},             // `1i2` bigint(20) unsigned DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_NEWDECIMAL, true, []byte{0x0a, 0x00}}, // `1i3` decimal(10,0) DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_NEWDECIMAL, true, []byte{0x0a, 0x00}}, // `1i4` decimal(10,0) unsigned DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_DOUBLE, true, []byte{0x08}},           // `1i5` double DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_DOUBLE, true, []byte{0x08}},           // `1i6` double unsigned DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_FLOAT, true, []byte{0x04}},            // `1i7` float DEFAULT NULL,
		&TableMapEventTest{MYSQL_TYPE_FLOAT, true, []byte{0x04}},            // `1i8` float unsigned DEFAULT NULL,
	}

	for i, testCase := range testsCases {
		testColumn := table.Columns[i]

		if testColumn.Type != testCase.expectedType {
			t.Fatal(
				"Incorrect columnt type with index", i,
				"expected", testCase.expectedType,
				"got", testColumn.Type,
			)
		}

		if testColumn.Nullable != testCase.expectedIsNull {
			t.Fatal(
				"Incorrect null flag with index", i,
				"expected", testCase.expectedIsNull,
				"got", testColumn.Nullable,
			)
		}

		if !reflect.DeepEqual(testColumn.MetaInfo, testCase.expectedMetaInfo) {
			t.Fatal(
				"Incorrect meta info",
				"expected", testCase.expectedMetaInfo,
				"got", testColumn.MetaInfo,
			)
		}
	}
}

func TestWriteRowsEventV1(t *testing.T) {

	rat12, _ := new(big.Rat).SetString("12")
	rat13, _ := new(big.Rat).SetString("13")
	rat29, _ := new(big.Rat).SetString("29")
	rat30, _ := new(big.Rat).SetString("30")

	testCases := []*rowEventTestCase{
		&rowEventTestCase{
			tableMapEventBuff: []byte{
				0x34, 0x00, 0x00, 0x01, 0x00, 0xaa, 0x74, 0x89, 0x54, 0x13, 0x01, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00,
				0x00, 0xb1, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74,
				0x65, 0x73, 0x74, 0x00, 0x07, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31, 0x00, 0x03, 0x03, 0x0f, 0x05,
				0x03, 0x2d, 0x00, 0x08, 0x04,
			},
			rowsEventBuff: []byte{
				0x2E, 0x00, 0x00,
				0x01,
				0x00,
				0xaa, 0x74, 0x89, 0x54,
				0x17,
				0x01, 0x00, 0x00, 0x00,
				0x2d, 0x00, 0x00, 0x00,
				0xde, 0x0d, 0x00, 0x00,
				0x00, 0x00,
				0x2b, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x01, 0x00,
				0x03,
				0xff,
				0xf8,
				0x0c, 0x00, 0x00, 0x00, 0x02, 0x68, 0x69, 0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
			},
			expectedValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(12), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 3.14, MYSQL_TYPE_DOUBLE},
				},
			},
		},

		&rowEventTestCase{
			tableMapEventBuff: []byte{
				0x34, 0x00, 0x00, 0x01, 0x00, 0x83, 0xa8, 0x89, 0x54, 0x13, 0x01, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00,
				0x00, 0x70, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74,
				0x65, 0x73, 0x74, 0x00, 0x07, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31, 0x00, 0x03, 0x03, 0x0f, 0x05,
				0x03, 0x2d, 0x00, 0x08, 0x04,
			},
			rowsEventBuff: []byte{
				0x26, 0x00, 0x00,
				0x01,
				0x00, 0x83, 0xa8, 0x89, 0x54, 0x17, 0x01, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x00, 0x95, 0x0e, 0x00,
				0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0xff, 0xfc, 0x0d, 0x00, 0x00,
				0x00, 0x02, 0x68, 0x69,
			},
			expectedValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(13), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, true, nil, MYSQL_TYPE_DOUBLE},
				},
			},
		},
		&rowEventTestCase{
			tableMapEventBuff: []byte{
				0x34, 0x00, 0x00, 0x01, 0x00, 0x83, 0xa8, 0x89, 0x54, 0x13, 0x01, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00,
				0x00, 0x70, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74,
				0x65, 0x73, 0x74, 0x00, 0x07, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31, 0x00, 0x03, 0x03, 0x0f, 0x05,
				0x03, 0x2d, 0x00, 0x08, 0x04,
			},
			rowsEventBuff: []byte{
				0x39, 0x00, 0x00,
				0x01,
				0x00,
				0x73, 0xa9, 0x89, 0x54,
				0x17,
				0x01, 0x00, 0x00, 0x00,
				0x38, 0x00, 0x00, 0x00,
				0x5f, 0x0f, 0x00, 0x00,
				0x00, 0x00,
				0x2b, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x01, 0x00,
				0x03,
				0xff,
				//row 1
				0xfc,
				0x0e, 0x00, 0x00, 0x00, //14
				0x02, 0x68, 0x69, //hi
				//row 2
				0xf8,
				0x0f, 0x00, 0x00, 0x00, //15
				0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, //hello
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, //22
			},
			expectedValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(14), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, true, nil, MYSQL_TYPE_DOUBLE},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(15), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hello", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 22.0, MYSQL_TYPE_DOUBLE},
				},
			},
		},

		&rowEventTestCase{
			tableMapEventBuff: []byte{
				0x49, 0x00, 0x00,
				0x01,
				0x00,
				0xf3, 0xc1, 0x89, 0x54,
				0x13,
				0x01, 0x00, 0x00, 0x00,
				0x48, 0x00, 0x00, 0x00,
				0x06, 0x10, 0x00, 0x00,
				0x00, 0x00,
				0x2d, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x01, 0x00,
				0x04,
				0x74, 0x65, 0x73, 0x74,
				0x00,
				0x05,
				0x74, 0x79, 0x70, 0x65, 0x73,
				0x00,
				0x13,
				0x03, 0x01, 0x01, 0x02, 0x02, 0x09, 0x09, 0x03, 0x03, 0x03, 0x03, 0x08, 0x08, 0xf6, 0xf6, 0x05, 0x05,
				0x04, 0x04,
				0x08,
				0x0a, 0x00,
				0x0a, 0x00,
				0x08,
				0x08,
				0x04,
				0x04,
				0x6e, 0xfb, 0x07,
			},
			rowsEventBuff: []byte{
				0xB0, 0x00, 0x00,
				0x01,
				0x00,
				0xf3, 0xc1, 0x89, 0x54,
				0x17,
				0x01, 0x00, 0x00, 0x00,
				0xaf, 0x00, 0x00, 0x00,
				0xb5, 0x10, 0x00, 0x00,
				0x00, 0x00,
				0x2d, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x01, 0x00,
				//count columns
				0x13,
				//binary present
				0xff, 0xff, 0xff,
				//null bitmap
				0x02, 0x88, 0xf8,
				//values
				//row 1
				0x02, 0x00, 0x00, 0x00,
				0x01,
				0x02, 0x00,
				0x03, 0x00,
				0x04, 0x00, 0x00,
				0x05, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				0x07, 0x00, 0x00, 0x00,
				0x08, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x80, 0x00, 0x00, 0x00, 0x0c, // new decimal stupid format without manual
				0x80, 0x00, 0x00, 0x00, 0x0d,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2e, 0x40,
				0x00, 0x00, 0x80, 0x41, 0x00, 0x00, 0x88, 0x41,
				//null bitmap
				0x02, 0x80, 0xf8,
				//row 2
				0x03, 0x00, 0x00, 0x00,
				0x12,
				0x13, 0x00,
				0x14, 0x00,
				0x15, 0x00, 0x00,
				0x16, 0x00, 0x00,
				0x17, 0x00, 0x00, 0x00,
				0x18, 0x00, 0x00, 0x00,
				0x19, 0x00, 0x00, 0x00,
				0x1a, 0x00, 0x00, 0x00,
				0x1b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x1c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x80, 0x00, 0x00, 0x00, 0x1d,
				0x80, 0x00, 0x00, 0x00, 0x1e,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0x40,
				0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x04, 0x42,
			},

			expectedValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(2), MYSQL_TYPE_LONG},
					&RowsEventValue{1, true, nil, MYSQL_TYPE_TINY},
					&RowsEventValue{2, false, byte(1), MYSQL_TYPE_TINY},
					&RowsEventValue{3, false, uint16(2), MYSQL_TYPE_SHORT},
					&RowsEventValue{4, false, uint16(3), MYSQL_TYPE_SHORT},
					&RowsEventValue{5, false, uint32(4), MYSQL_TYPE_INT24},
					&RowsEventValue{6, false, uint32(5), MYSQL_TYPE_INT24},
					&RowsEventValue{7, false, uint32(6), MYSQL_TYPE_LONG},
					&RowsEventValue{8, false, uint32(7), MYSQL_TYPE_LONG},
					&RowsEventValue{9, false, uint32(8), MYSQL_TYPE_LONG},
					&RowsEventValue{10, false, uint32(9), MYSQL_TYPE_LONG},
					&RowsEventValue{11, true, nil, MYSQL_TYPE_LONGLONG},
					&RowsEventValue{12, false, uint64(11), MYSQL_TYPE_LONGLONG},
					&RowsEventValue{13, false, rat12, MYSQL_TYPE_NEWDECIMAL},
					&RowsEventValue{14, false, rat13, MYSQL_TYPE_NEWDECIMAL},
					&RowsEventValue{15, true, nil, MYSQL_TYPE_DOUBLE},
					&RowsEventValue{16, false, 15.0, MYSQL_TYPE_DOUBLE},
					&RowsEventValue{17, false, float32(16), MYSQL_TYPE_FLOAT},
					&RowsEventValue{18, false, float32(17), MYSQL_TYPE_FLOAT},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(3), MYSQL_TYPE_LONG},
					&RowsEventValue{1, true, nil, MYSQL_TYPE_TINY},
					&RowsEventValue{2, false, byte(18), MYSQL_TYPE_TINY},
					&RowsEventValue{3, false, uint16(19), MYSQL_TYPE_SHORT},
					&RowsEventValue{4, false, uint16(20), MYSQL_TYPE_SHORT},
					&RowsEventValue{5, false, uint32(21), MYSQL_TYPE_INT24},
					&RowsEventValue{6, false, uint32(22), MYSQL_TYPE_INT24},
					&RowsEventValue{7, false, uint32(23), MYSQL_TYPE_LONG},
					&RowsEventValue{8, false, uint32(24), MYSQL_TYPE_LONG},
					&RowsEventValue{9, false, uint32(25), MYSQL_TYPE_LONG},
					&RowsEventValue{10, false, uint32(26), MYSQL_TYPE_LONG},
					&RowsEventValue{11, false, uint64(27), MYSQL_TYPE_LONGLONG},
					&RowsEventValue{12, false, uint64(28), MYSQL_TYPE_LONGLONG},
					&RowsEventValue{13, false, rat29, MYSQL_TYPE_NEWDECIMAL},
					&RowsEventValue{14, false, rat30, MYSQL_TYPE_NEWDECIMAL},
					&RowsEventValue{15, true, nil, MYSQL_TYPE_DOUBLE},
					&RowsEventValue{16, false, 31.0, MYSQL_TYPE_DOUBLE},
					&RowsEventValue{17, false, float32(32), MYSQL_TYPE_FLOAT},
					&RowsEventValue{18, false, float32(33), MYSQL_TYPE_FLOAT},
				},
			},
		},
	}

	for i, testCase := range testCases {
		packReader := newPackReader(bytes.NewBuffer(testCase.rowsEventBuff))
		pack, _ := packReader.readNextPack()

		header := &eventLogHeader{}
		header.readHead(pack)

		write := &rowsEvent{}
		write.eventLogHeader = header
		write.version = byte(1)
		write.postHeaderLength = byte(8)
		write.tableMapEvent = getTableMapEvent(testCase.tableMapEventBuff)
		write.read(pack)

		if write.EventType != _WRITE_ROWS_EVENTv1 {
			t.Fatal(
				"Incorrect event type",
				"expected", _WRITE_ROWS_EVENTv1,
				"got", write.EventType,
			)
		}

		if len(write.values) != len(testCase.expectedValues) {
			t.Fatal(
				"Incorrect values quantity at test", i,
				"expected", len(testCase.expectedValues),
				"got", len(write.values),
			)
		}

		for k, expectedValueRow := range testCase.expectedValues {

			for j, expectedValue := range expectedValueRow {
				resultValue := write.values[k][j]

				if expectedValue.GetType() != resultValue.GetType() {
					t.Fatal(
						"Incorrect type at test", i, "row", k, "value id", j,
						"expected", expectedValue.GetType(),
						"got", resultValue.GetType(),
					)
				}

				if expectedValue.GetColumnId() != resultValue.GetColumnId() {
					t.Fatal(
						"Incorrect column id at test", i, "row", k, "value id", j,
						"expected", expectedValue.GetColumnId(),
						"got", resultValue.GetColumnId(),
					)
				}

				if expectedValue.IsNil() != resultValue.IsNil() {
					t.Fatal(
						"Incorrect null value at test", i, "row", k, "value id", j,
						"expected", expectedValue.IsNil(),
						"got", resultValue.IsNil(),
					)
				}

				if !reflect.DeepEqual(expectedValue.GetValue(), resultValue.GetValue()) {
					t.Fatal(
						"Incorrect value at test", i, "row", k, "value id", j,
						"expected", expectedValue.GetValue(),
						"got", resultValue.GetValue(),
					)
				}
			}
		}
	}
}

func TestDeleteRowsEventV1(t *testing.T) {
	rat333 := new(big.Rat).SetInt64(333)
	testCases := []*rowEventTestCase{
		&rowEventTestCase{
			tableMapEventBuff: []byte{
				0x2E, 0x00, 0x00, 0x01,
				0x00, 0x73, 0xe6, 0x8a, 0x54, 0x13, 0x01, 0x00, 0x00, 0x00, 0x2d, 0x00, 0x00, 0x00, 0x81, 0x18, 0x00,
				0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00,
				0x03, 0x64, 0x65, 0x63, 0x00, 0x02, 0x03, 0xf6, 0x02, 0x0a, 0x00, 0x02,
			},
			rowsEventBuff: []byte{
				0x28, 0x00, 0x00, 0x01,
				0x00, 0x73, 0xe6, 0x8a, 0x54, 0x19, 0x01, 0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x00, 0xa8, 0x18, 0x00,
				0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0xff, 0xfc, 0x06, 0x00, 0x00,
				0x00, 0x80, 0x00, 0x00, 0x01, 0x4d,
			},
			expectedValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(6), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, rat333, MYSQL_TYPE_NEWDECIMAL},
				},
			},
		},
		&rowEventTestCase{
			tableMapEventBuff: []byte{
				0x34, 0x00, 0x00, 0x01,
				0x00, 0xd2, 0xe9, 0x8a, 0x54, 0x13, 0x01, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x3a, 0x19, 0x00,
				0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00,
				0x07, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31, 0x00, 0x03, 0x03, 0x0f, 0x05, 0x03, 0x2d, 0x00, 0x08,
				0x04,
			},
			rowsEventBuff: []byte{
				0x41, 0x00, 0x00, 0x01,
				0x00, 0xd2, 0xe9, 0x8a, 0x54, 0x19, 0x01, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x7a, 0x19, 0x00,
				0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0xff, 0xfc, 0x0d, 0x00, 0x00,
				0x00, 0x02, 0x68, 0x69, 0xfc, 0x0e, 0x00, 0x00, 0x00, 0x02, 0x68, 0x69, 0xf8, 0x0f, 0x00, 0x00, 0x00,
				0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40,
			},
			expectedValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(13), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, true, nil, MYSQL_TYPE_DOUBLE},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(14), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, true, nil, MYSQL_TYPE_DOUBLE},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(15), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hello", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 22.0, MYSQL_TYPE_DOUBLE},
				},
			},
		},
	}

	for i, testCase := range testCases {
		packReader := newPackReader(bytes.NewBuffer(testCase.rowsEventBuff))
		pack, _ := packReader.readNextPack()

		header := &eventLogHeader{}
		header.readHead(pack)

		delete := &rowsEvent{}
		delete.eventLogHeader = header
		delete.version = byte(1)
		delete.postHeaderLength = byte(8)
		delete.tableMapEvent = getTableMapEvent(testCase.tableMapEventBuff)
		delete.read(pack)

		if delete.EventType != _DELETE_ROWS_EVENTv1 {
			t.Fatal(
				"Incorrect event type",
				"expected", _DELETE_ROWS_EVENTv1,
				"got", delete.EventType,
			)
		}

		if len(delete.values) != len(testCase.expectedValues) {
			t.Fatal(
				"Incorrect values quantity at test", i,
				"expected", len(testCase.expectedValues),
				"got", len(delete.values),
			)
		}

		for k, expectedValueRow := range testCase.expectedValues {

			for j, expectedValue := range expectedValueRow {
				resultValue := delete.values[k][j]

				if expectedValue.GetType() != resultValue.GetType() {
					t.Fatal(
						"Incorrect type at test", i, "row", k, "value id", j,
						"expected", expectedValue.GetType(),
						"got", resultValue.GetType(),
					)
				}

				if expectedValue.GetColumnId() != resultValue.GetColumnId() {
					t.Fatal(
						"Incorrect column id at test", i, "row", k, "value id", j,
						"expected", expectedValue.GetColumnId(),
						"got", resultValue.GetColumnId(),
					)
				}

				if expectedValue.IsNil() != resultValue.IsNil() {
					t.Fatal(
						"Incorrect null value at test", i, "row", k, "value id", j,
						"expected", expectedValue.IsNil(),
						"got", resultValue.IsNil(),
					)
				}

				if !reflect.DeepEqual(expectedValue.GetValue(), resultValue.GetValue()) {
					t.Fatal(
						"Incorrect value at test", i, "row", k, "value id", j,
						"expected", expectedValue.GetValue(),
						"got", resultValue.GetValue(),
					)
				}
			}
		}
	}
}

func TestUpdateRowsEventV1(t *testing.T) {
	testCases := []*rowEventTestCase{
		&rowEventTestCase{
			tableMapEventBuff: []byte{
				0x34, 0x00, 0x00, 0x01,
				0x00, 0x76, 0xeb, 0x8a, 0x54, 0x13, 0x01, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x0c, 0x1a, 0x00,
				0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00,
				0x07, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31, 0x00, 0x03, 0x03, 0x0f, 0x05, 0x03, 0x2d, 0x00, 0x08,
				0x04,
			},
			rowsEventBuff: []byte{
				0x88, 0x00, 0x00, 0x01,
				0x00,
				0x76, 0xeb, 0x8a, 0x54,
				0x18,
				0x01, 0x00, 0x00, 0x00,
				0x87, 0x00, 0x00, 0x00,
				0x93, 0x1a, 0x00, 0x00,
				0x00, 0x00,
				0x2b, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x01, 0x00,
				//count rows
				0x03,
				// preset bitmap 1
				0xff,
				// preset bitmap 2 (for update)
				0xff,
				//row 1
				0xf8,
				//10
				0x0a, 0x00, 0x00, 0x00,
				// "hi"
				0x02, 0x68, 0x69,
				//3.14
				0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
				// null bitmap 2
				0xf8,
				//10
				0x0a, 0x00, 0x00, 0x00,
				// "hello"
				0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
				//3.14
				0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
				//row 2
				//null bitmap 1
				0xf8,
				// 11
				0x0b, 0x00, 0x00, 0x00,
				// "hi"
				0x02, 0x68, 0x69,
				//3.14
				0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
				// null bitmap 2
				0xf8,
				//11
				0x0b, 0x00, 0x00, 0x00,
				// "hello"
				0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
				//3.14
				0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
				// row 3
				// null bitmap 1
				0xf8,
				//12
				0x0c, 0x00, 0x00, 0x00,
				// "hi"
				0x02, 0x68, 0x69,
				//3.14
				0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
				//null bitmap 2
				0xf8,
				//12
				0x0c, 0x00, 0x00, 0x00,
				// "hello"
				0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
				//3.14
				0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,
			},
			expectedValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(10), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 3.14, MYSQL_TYPE_DOUBLE},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(11), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 3.14, MYSQL_TYPE_DOUBLE},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(12), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hi", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 3.14, MYSQL_TYPE_DOUBLE},
				},
			},
			expectedNewValues: [][]*RowsEventValue{
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(10), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hello", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 3.14, MYSQL_TYPE_DOUBLE},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(11), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hello", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 3.14, MYSQL_TYPE_DOUBLE},
				},
				[]*RowsEventValue{
					&RowsEventValue{0, false, uint32(12), MYSQL_TYPE_LONG},
					&RowsEventValue{1, false, "hello", MYSQL_TYPE_VARCHAR},
					&RowsEventValue{2, false, 3.14, MYSQL_TYPE_DOUBLE},
				},
			},
		},
	}

	for i, testCase := range testCases {
		packReader := newPackReader(bytes.NewBuffer(testCase.rowsEventBuff))
		pack, _ := packReader.readNextPack()

		header := &eventLogHeader{}
		header.readHead(pack)

		update := &rowsEvent{}
		update.eventLogHeader = header
		update.version = byte(1)
		update.postHeaderLength = byte(8)
		update.tableMapEvent = getTableMapEvent(testCase.tableMapEventBuff)
		update.read(pack)

		if update.EventType != _UPDATE_ROWS_EVENTv1 {
			t.Fatal(
				"Incorrect event type",
				"expected", _UPDATE_ROWS_EVENTv1,
				"got", update.EventType,
			)
		}

		if len(update.values) != len(testCase.expectedValues) {
			t.Fatal(
				"Incorrect values quantity at test", i,
				"expected", len(testCase.expectedValues),
				"got", len(update.values),
			)
		}

		if len(update.newValues) != len(testCase.expectedNewValues) {
			t.Fatal(
				"Incorrect values quantity at test", i,
				"expected", len(testCase.expectedNewValues),
				"got", len(update.newValues),
			)
		}

		//test old values
		for k, expectedValueRow := range testCase.expectedValues {
			for j, expectedValue := range expectedValueRow {
				resultValue := update.values[k][j]

				if expectedValue.GetType() != resultValue.GetType() {
					t.Fatal(
						"Incorrect type at test old values ", i, "row", k, "value id", j,
						"expected", expectedValue.GetType(),
						"got", resultValue.GetType(),
					)
				}

				if expectedValue.GetColumnId() != resultValue.GetColumnId() {
					t.Fatal(
						"Incorrect column id at test old values ", i, "row", k, "value id", j,
						"expected", expectedValue.GetColumnId(),
						"got", resultValue.GetColumnId(),
					)
				}

				if expectedValue.IsNil() != resultValue.IsNil() {
					t.Fatal(
						"Incorrect null value at test old values ", i, "row", k, "value id", j,
						"expected", expectedValue.IsNil(),
						"got", resultValue.IsNil(),
					)
				}

				if !reflect.DeepEqual(expectedValue.GetValue(), resultValue.GetValue()) {
					t.Fatal(
						"Incorrect value at test old values ", i, "row", k, "value id", j,
						"expected", expectedValue.GetValue(),
						"got", resultValue.GetValue(),
					)
				}
			}
		}

		//test new values
		for k, expectedValueRow := range testCase.expectedNewValues {
			for j, expectedValue := range expectedValueRow {
				resultValue := update.newValues[k][j]

				if expectedValue.GetType() != resultValue.GetType() {
					t.Fatal(
						"Incorrect type at test new values ", i, "row", k, "value id", j,
						"expected", expectedValue.GetType(),
						"got", resultValue.GetType(),
					)
				}

				if expectedValue.GetColumnId() != resultValue.GetColumnId() {
					t.Fatal(
						"Incorrect column id at test new values ", i, "row", k, "value id", j,
						"expected", expectedValue.GetColumnId(),
						"got", resultValue.GetColumnId(),
					)
				}

				if expectedValue.IsNil() != resultValue.IsNil() {
					t.Fatal(
						"Incorrect null value at test new values ", i, "row", k, "value id", j,
						"expected", expectedValue.IsNil(),
						"got", resultValue.IsNil(),
					)
				}

				if !reflect.DeepEqual(expectedValue.GetValue(), resultValue.GetValue()) {
					t.Fatal(
						"Incorrect value at test new values ", i, "row", k, "value id", j,
						"expected", expectedValue.GetValue(),
						"got", resultValue.GetValue(),
					)
				}
			}
		}
	}
}
