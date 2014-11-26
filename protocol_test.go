package mysql_replication_listener

import (
	//	"reflect"
	"reflect"
	"testing"
)

func getPackReadHelper() {

}

func TestReadUInt16(t *testing.T) {
	mockData := []byte{0x1D, 0x86}
	var expected uint16 = 34333
	var result uint16
	err := readUint16(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestWriteUInt16(t *testing.T) {
	var mockData uint16 = 34333
	expected := []byte{0x1D, 0x86}
	result := make([]byte, 2)

	writeUInt16(result, mockData)

	if !reflect.DeepEqual(expected, result) {
		t.Error("Expected", expected, "got", result)
	}
}

func TestReadThreeByteUInt32(t *testing.T) {
	mockData := []byte{0x76, 0x8A, 0x34}
	var expected uint32 = 3443318
	var result uint32
	err := readThreeBytesUint32(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestWriteThreeByteUInt32(t *testing.T) {
	var mockData uint32 = 3443318
	expected := []byte{0x76, 0x8A, 0x34}
	result := make([]byte, 3)

	writeThreeByteUInt32(result, mockData)

	if !reflect.DeepEqual(expected, result) {
		t.Error("Expected", expected, "got", result)
	}
}

func TestReadUInt32(t *testing.T) {
	mockData := []byte{0xD6, 0x00, 0x77, 0x14}
	var expected uint32 = 343343318
	var result uint32
	err := readUint32(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestWriteUInt32(t *testing.T) {
	var mockData uint32 = 343343318
	expected := []byte{0xD6, 0x00, 0x77, 0x14}
	result := make([]byte, 4)

	writeUInt32(result, mockData)

	if !reflect.DeepEqual(expected, result) {
		t.Error("Expected", expected, "got", result)
	}
}

func TestReadUint64(t *testing.T) {
	mockData := []byte{0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20}
	var expected uint64 = 2332321241244333252
	var result uint64

	err := readUint64(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if result != expected {
		t.Error("Expected", expected, "got", result)
	}
}

func TestWriteUint64(t *testing.T) {
	var mockData uint64 = 2332321241244333252
	expected := []byte{0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20}
	result := make([]byte, 8)

	writeUInt64(result, mockData)

	if !reflect.DeepEqual(expected, result) {
		t.Error("Expected", expected, "got", result)
	}
}

func TestWriteLengthInt(t *testing.T) {
	expected := []byte{0x0A}
	result := writeLengthInt(uint64(10))

	if !reflect.DeepEqual(result, expected) {
		t.Error("Expected", expected, "got", result)
	}

	expected = []byte{0xFC, 0x1D, 0x86}
	result = writeLengthInt(uint64(34333))

	if !reflect.DeepEqual(result, expected) {
		t.Error("Expected", expected, "got", result)
	}

	expected = []byte{0xFD, 0x76, 0x8A, 0x34}
	result = writeLengthInt(uint64(3443318))

	if !reflect.DeepEqual(result, expected) {
		t.Error("Expected", expected, "got", result)
	}

	expected = []byte{0xFE, 0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20}
	result = writeLengthInt(uint64(2332321241244333252))

	if !reflect.DeepEqual(result, expected) {
		t.Error("Expected", expected, "got", result)
	}
}

//func TestThreeByteUInt32(t *testing.T) {
//	mockData := []byte{95, 0, 0}
//	reader := getProtoReader(mockData)
//	var expected uint32 = 95
//
//	result, err := reader.readThreeBytesUint32()
//
//	if err != nil {
//		t.Error("Got error", err)
//	}
//
//	if expected != result {
//		t.Error("Expected", expected, "got", result)
//	}
//}

//func TestUint64(t *testing.T) {
//	mockData := []byte{0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20}
//	reader := getProtoReader(mockData)
//
//	result, err := reader.readUint64()
//
//	if err != nil {
//		t.Error("Got error", err)
//	}
//
//	var expected uint64 = 2332321241244333252
//
//	if result != expected {
//		t.Error("Expected", expected, "got", result)
//	}
//}
//
//func TestUInt32(t *testing.T) {
//	mockData := []byte{0xD3, 0x87, 0x2C, 0x4A}
//	reader := getProtoReader(mockData)
//	var expected uint32 = 1244432339
//
//	result, err := reader.readUint32()
//
//	if err != nil {
//		t.Error("Got error", err)
//	}
//
//	if expected != result {
//		t.Error("Expected", expected, "got", result)
//	}
//}
//
//func TestUInt16(t *testing.T) {
//	mockData := []byte{0x9C, 0x30}
//	reader := getProtoReader(mockData)
//	var expected uint16 = 12444
//
//	result, err := reader.readUint16()
//
//	if err != nil {
//		t.Error("Got error", err)
//	}
//
//	if expected != result {
//		t.Error("Expected", expected, "got", result)
//	}
//}
//
//func TestNilString(t *testing.T) {
//	mockData := []byte{53, 46, 53, 46, 51, 56, 45, 48, 117, 98, 117, 110, 116, 117, 48, 46, 49, 52, 46, 48, 52, 46, 49, 45, 108, 111, 103, 0}
//	reader := getProtoReader(mockData)
//
//	expected := []byte("5.5.38-0ubuntu0.14.04.1-log")
//
//	result, err := reader.readNilString()
//
//	if err != nil {
//		t.Error("Got error", err)
//	}
//
//	if !reflect.DeepEqual(expected, result) {
//		t.Error("Expected", string(expected), "got", string(result))
//	}
//}
//
//func TestLenIntByte(t *testing.T) {
//	mockData := []byte{0xFA}
//	reader := getProtoReader(mockData)
//
//	result, null, byteLength := reader.readIntOrNil()
//
//	if null {
//		t.Error("Got null int")
//	}
//
//	byteLengthExpected := byte(1)
//	if byteLengthExpected != byteLength {
//		t.Error(
//			"Incorrect length",
//			"expected", byteLengthExpected,
//			"got", byteLength,
//		)
//	}
//
//	if result != 250 {
//		t.Error("Expected", 250, "got", result)
//	}
//}
//
//func TestLenUint16(t *testing.T) {
//	mockData := []byte{0xFC, 0xFB, 0x00}
//	reader := getProtoReader(mockData)
//
//	result, null, byteLength := reader.readIntOrNil()
//
//	byteLengthExpected := byte(3)
//	if byteLengthExpected != byteLength {
//		t.Error(
//			"Incorrect length",
//			"expected", byteLengthExpected,
//			"got", byteLength,
//		)
//	}
//
//	if null {
//		t.Error("Got null int")
//	}
//
//	if result != 251 {
//		t.Error("Expected", 251, "got", result)
//	}
//}
//
//func TestLenUint24(t *testing.T) {
//	mockData := []byte{0xFD, 0xC4, 0x1E, 0x42}
//	reader := getProtoReader(mockData)
//
//	result, null, byteLength := reader.readIntOrNil()
//
//	byteLengthExpected := byte(4)
//	if byteLengthExpected != byteLength {
//		t.Error(
//			"Incorrect length",
//			"expected", byteLengthExpected,
//			"got", byteLength,
//		)
//	}
//
//	if null {
//		t.Error("Got null int")
//	}
//
//	if result != 4333252 {
//		t.Error("Expected", 4333252, "got", result)
//	}
//}
//
//func TestLenUint64(t *testing.T) {
//	mockData := []byte{0xFE, 0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20}
//	reader := getProtoReader(mockData)
//
//	result, null, byteLength := reader.readIntOrNil()
//
//	byteLengthExpected := byte(9)
//	if byteLengthExpected != byteLength {
//		t.Error(
//			"Incorrect length",
//			"expected", byteLengthExpected,
//			"got", byteLength,
//		)
//	}
//
//	if null {
//		t.Error("Got null int")
//	}
//
//	if result != 2332321241244333252 {
//		t.Error("Expected", 2332321241244333252, "got", result)
//	}
//}
//
//func TestLenNil(t *testing.T) {
//	mockData := []byte{0xFB}
//	reader := getProtoReader(mockData)
//
//	result, null, byteLength := reader.readIntOrNil()
//
//	byteLengthExpected := byte(1)
//	if byteLengthExpected != byteLength {
//		t.Error(
//			"Incorrect length",
//			"expected", byteLengthExpected,
//			"got", byteLength,
//		)
//	}
//
//	if !null {
//		t.Error("Got not null")
//	}
//
//	if result != 0 {
//		t.Error("Expected", 0, "got", result)
//	}
//}
//
//func TestLenString(t *testing.T) {
//	mockData := []byte{0x03, 0x64, 0x65, 0x66}
//	reader := getProtoReader(mockData)
//
//	result, byteLength, err := reader.readLenString()
//
//	if err != nil {
//		t.Error("Got error", err)
//	}
//
//	if byteLength != uint64(4) {
//		t.Error(
//			"Incorrect lenth",
//			"expected", 4,
//			"got", byteLength,
//		)
//	}
//
//	expected := "def"
//
//	if string(result) != expected {
//		t.Error("Expected", expected, "got", string(result))
//	}
//}
