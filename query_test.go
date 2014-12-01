package mysql_replication_listener

import (
	"reflect"
	"testing"
)

func TestSendQuery(t *testing.T) {
	command := "select @@version_comment limit 1"
	q := query{}
	pack := q.writeServer(command)
	result := pack.packBytes()

	expectedLength := []byte{0x21, 0x00, 0x00}

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

	if _COM_QUERY != result[offset : offset+1][0] {
		t.Fatal(
			"Incorrect command",
			"expected", _COM_QUERY,
			"got", result[offset : offset+1][0],
		)
	}

	offset++

	if command != string(result[offset:offset+len(command)]) {
		t.Fatal(
			"Incorrect commnad",
			"expected", command,
			"got", string(result[offset:offset+len(command)]),
		)
	}

	offset += len(command)
}
