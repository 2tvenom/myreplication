package mysql_replication_listener

import (
	"bytes"
	"testing"
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
	header.read(pack)

	logRotate := &LogRotateEvent{}
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

	if logRotate.Position != expectedPosition {
		t.Fatal(
			"Incorrect position",
			"expected", expectedPosition,
			"got", logRotate.Position,
		)
	}

	expectedBinlogFileName := "mysql-bin.000003"

	if logRotate.BinlogFileName != expectedBinlogFileName {
		t.Fatal(
			"Incorrect file name",
			"expected", expectedBinlogFileName,
			"got", logRotate.BinlogFileName,
		)
	}
}
