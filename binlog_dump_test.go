package myreplication

import (
	"reflect"
	"testing"
)

func TestStartBinLog(t *testing.T) {
	rs := binlogDump{}

	binlogFileName := "binlog.000"

	pack := rs.writeServer(uint32(10), binlogFileName, uint32(5))

	result := pack.packBytes()

	expectedLength := []byte{0x15, 0x00, 0x00}

	offset := 0

	if !reflect.DeepEqual(expectedLength, result[offset:offset+3]) {
		t.Fatal(
			"Incorrect query length",
			"expected", expectedLength,
			"got", result[offset:offset+3],
		)
	}

	offset += 3

	expectedSequence := byte(0)

	if expectedSequence != result[offset : offset+1][0] {
		t.Fatal(
			"Incorrect sequence",
			"expected", expectedSequence,
			"got", result[offset : offset+1][0],
		)
	}

	offset++

	if _COM_BINLOG_DUMP != result[offset : offset+1][0] {
		t.Fatal(
			"Incorrect command",
			"expected", _COM_QUERY,
			"got", result[offset : offset+1][0],
		)
	}

	offset++

	expectedBinlogPosition := []byte{0x0A, 0x00, 0x00, 0x00}
	if !reflect.DeepEqual(expectedBinlogPosition, result[offset:offset+4]) {
		t.Fatal(
			"Incorrect binlog position",
			"expected", expectedBinlogPosition,
			"got", result[offset:offset+4],
		)
	}
	offset += 4

	expectedFlags := []byte{0x00, 0x00}
	if !reflect.DeepEqual(expectedFlags, result[offset:offset+2]) {
		t.Fatal(
			"Incorrect flags",
			"expected", expectedFlags,
			"got", result[offset:offset+2],
		)
	}
	offset += 2

	expectedServerId := []byte{0x05, 0x00, 0x00, 0x00}
	if !reflect.DeepEqual(expectedServerId, result[offset:offset+4]) {
		t.Fatal(
			"Incorrect server id",
			"expected", expectedServerId,
			"got", result[offset:offset+4],
		)
	}
	offset += 4

	expectedBinlogFileName := []byte(binlogFileName)
	if !reflect.DeepEqual(expectedBinlogFileName, result[offset:]) {
		t.Fatal(
			"Incorrect binlog file name",
			"expected", binlogFileName,
			"got", string(result[offset:]),
		)
	}
}
