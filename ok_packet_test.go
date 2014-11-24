package mysql_replication_listener

//
//import (
//	"testing"
//)
//
//func TestOkPacketOk(t *testing.T) {
//	mockHandshake := []byte{
//		//length
//		0x07, 0x00, 0x00,
//		//sequence id
//		0x02,
//		//code
//		0x00,
//		0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
//	}
//	reader := getProtoReader(mockHandshake)
//
//	err := ok_packet(reader)
//
//	if err != nil {
//		t.Fatal("Ok packet return err", err)
//	}
//}
//
//func TestOkPacketError(t *testing.T) {
//	mockHandshake := []byte{
//		//length
//		0x17, 0x00, 0x00,
//		//sequence
//		0x01,
//		//err code
//		0xff,
//		//error id
//		0x48, 0x04,
//		//error text
//		0x23, 0x48, 0x59, 0x30, 0x30, 0x30, 0x4e, 0x6f, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x20, 0x75, 0x73,
//		0x65, 0x64,
//	}
//	reader := getProtoReader(mockHandshake)
//
//	err := ok_packet(reader)
//	if err == nil {
//		t.Fatal("Ok packet must return error")
//	}
//
//	errorText := "#HY000No tables used"
//
//	if err.Error() != errorText {
//		t.Fatal(
//			"Incorrect err packet text",
//			"expected", errorText,
//			"got", err.Error(),
//		)
//	}
//}
