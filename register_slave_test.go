package mysql_replication_listener

import (
	"reflect"
	"testing"
)

func TestRegisterSlave(t *testing.T) {
	rs := registerSlave{}
	pack := rs.writeServer(uint32(10))

	result := pack.packBytes()

	expectedLength := []byte{0x12, 0x00, 0x00}

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

	if _COM_REGISTER_SLAVE != result[offset : offset+1][0] {
		t.Fatal(
			"Incorrect command",
			"expected", _COM_QUERY,
			"got", result[offset : offset+1][0],
		)
	}

	offset++

	expectedServerId := []byte{0x0A, 0x00, 0x00, 0x00}
	if !reflect.DeepEqual(expectedServerId, result[offset:offset+4]) {
		t.Fatal(
			"Incorrect servert id",
			"expected", expectedLength,
			"got", result[offset:offset+4],
		)
	}
	offset += 4

	expectedStrLength := byte(0)

	if expectedStrLength != result[offset : offset+1][0] {
		t.Fatal(
			"Expected hostname length",
			"expected", expectedStrLength,
			"got", result[offset : offset+1][0],
		)
	}
	offset++

	if expectedStrLength != result[offset : offset+1][0] {
		t.Fatal(
			"Expected username length",
			"expected", expectedStrLength,
			"got", result[offset : offset+1][0],
		)
	}
	offset++

	if expectedStrLength != result[offset : offset+1][0] {
		t.Fatal(
			"Expected password length",
			"expected", expectedStrLength,
			"got", result[offset : offset+1][0],
		)
	}
	offset++

	expectedPort := []byte{0x00, 0x00}

	if !reflect.DeepEqual(expectedPort, result[offset:offset+2]) {
		t.Fatal(
			"Expected port",
			"expected", expectedPort,
			"got", result[offset:offset+2],
		)
	}
	offset += 2

	expectedReplicationRank := []byte{0x00, 0x00, 0x00, 0x00}

	if !reflect.DeepEqual(expectedReplicationRank, result[offset:offset+4]) {
		t.Fatal(
			"Expected replication rank",
			"expected", expectedReplicationRank,
			"got", result[offset:offset+4],
		)
	}
	offset += 4

	expectedMasterId := []byte{0x00, 0x00, 0x00, 0x00}

	if !reflect.DeepEqual(expectedMasterId, result[offset:offset+4]) {
		t.Fatal(
			"Expected master id",
			"expected", expectedMasterId,
			"got", result[offset:offset+4],
		)
	}
	offset += 4

}
