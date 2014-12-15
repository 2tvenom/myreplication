package mysql_replication_listener

import (
	"reflect"
	"testing"
)

func TestSendFieldList(t *testing.T) {
	table := "test01"
	q := fieldList{}
	pack := q.writeServer(table)
	result := pack.packBytes()

	expectedLength := []byte{0x08, 0x00, 0x00}

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

	if _COM_FIELD_LIST != result[offset : offset+1][0] {
		t.Fatal(
			"Incorrect command",
			"expected", _COM_QUERY,
			"got", result[offset : offset+1][0],
		)
	}

	offset++

	if table != string(result[offset:offset+len(table)]) {
		t.Fatal(
			"Incorrect table name",
			"expected", table,
			"got", string(result[offset:offset+len(table)]),
		)
	}

	offset += len(table)
}
