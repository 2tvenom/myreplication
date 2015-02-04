package myreplication

import (
	"reflect"
	"testing"
)

func TestConnectDb(t *testing.T) {
	db := "test"
	q := connectDb{}
	pack := q.writeServer(db)
	result := pack.packBytes()

	expectedLength := []byte{0x05, 0x00, 0x00}

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

	if _COM_INIT_DB != result[offset : offset+1][0] {
		t.Fatal(
			"Incorrect command",
			"expected", _COM_INIT_DB,
			"got", result[offset : offset+1][0],
		)
	}

	offset++

	if db != string(result[offset:offset+len(db)]) {
		t.Fatal(
			"Incorrect db",
			"expected", db,
			"got", string(result[offset:offset+len(db)]),
		)
	}

	offset += len(db)
}
