package mysql_replication_listener

//
//import (
//	"testing"
//	"reflect"
//)
//
//func TestRegisterSlave(t *testing.T) {
//	mockWriter := make([]byte, 0, 100)
//	writer := getProtoWriter(mockWriter)
//	mockReader := []byte{
//		0x07, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
//	}
//	reader := getProtoReader(mockReader)
//
//	err := registerSlave(writer, reader, uint32(10))
//
//	if err != nil {
//		t.Fatal("Query got error", err)
//	}
//
//	expectedLength := []byte{0x12, 0x00, 0x00}
//
//	offset := 0
//
//	if !reflect.DeepEqual(expectedLength, mockWriter[offset:offset+3]) {
//		t.Fatal(
//			"Incorrect query length",
//			"expected", expectedLength,
//			"got", mockWriter[offset:offset+3],
//		)
//	}
//
//	offset += 3
//
//	expectedSequence := byte(0)
//
//	if expectedSequence != mockWriter[offset : offset+1][0] {
//		t.Fatal(
//			"Incorrect sequence",
//			"expected", expectedSequence,
//			"got", mockWriter[offset : offset+1][0],
//		)
//	}
//
//	offset++
//
//	if _COM_REGISTER_SLAVE != mockWriter[offset : offset+1][0] {
//		t.Fatal(
//			"Incorrect command",
//			"expected", _COM_QUERY,
//			"got", mockWriter[offset : offset+1][0],
//		)
//	}
//
//	offset++
//
//	expectedServerId := []byte{0x0A, 0x00, 0x00, 0x00}
//	if !reflect.DeepEqual(expectedServerId, mockWriter[offset:offset+4]) {
//		t.Fatal(
//			"Incorrect servert id",
//			"expected", expectedLength,
//			"got", mockWriter[offset:offset+4],
//		)
//	}
//	offset+=4
//
//	expectedStrLength := byte(0)
//
//	if expectedStrLength != mockWriter[offset : offset+1][0] {
//		t.Fatal(
//			"Expected hostname length",
//			"expected", expectedStrLength,
//			"got", mockWriter[offset : offset+1][0],
//		)
//	}
//	offset++
//
//
//	if expectedStrLength != mockWriter[offset : offset+1][0] {
//		t.Fatal(
//			"Expected username length",
//			"expected", expectedStrLength,
//			"got", mockWriter[offset : offset+1][0],
//		)
//	}
//	offset++
//
//
//	if expectedStrLength != mockWriter[offset : offset+1][0] {
//		t.Fatal(
//			"Expected password length",
//			"expected", expectedStrLength,
//			"got", mockWriter[offset : offset+1][0],
//		)
//	}
//	offset++
//
//	expectedPort := []byte{0x00, 0x00}
//
//	if !reflect.DeepEqual(expectedPort, mockWriter[offset:offset+2]) {
//		t.Fatal(
//			"Expected port",
//			"expected", expectedPort,
//			"got", mockWriter[offset : offset+2],
//		)
//	}
//	offset+=2
//
//	expectedReplicationRank := []byte{0x00, 0x00, 0x00, 0x00}
//
//	if !reflect.DeepEqual(expectedReplicationRank, mockWriter[offset:offset+4]) {
//		t.Fatal(
//			"Expected replication rank",
//			"expected", expectedReplicationRank,
//			"got", mockWriter[offset : offset+4],
//		)
//	}
//	offset+=4
//
//	expectedMasterId := []byte{0x00, 0x00, 0x00, 0x00}
//
//	if !reflect.DeepEqual(expectedMasterId, mockWriter[offset:offset+4]) {
//		t.Fatal(
//			"Expected master id",
//			"expected", expectedMasterId,
//			"got", mockWriter[offset : offset+4],
//		)
//	}
//	offset+=4
//
//}
