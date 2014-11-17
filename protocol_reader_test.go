package mysql_replication_listener

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func getProtoReader(mockData []byte) *protoReader {
	return newProtoReader(bufio.NewReader(bytes.NewReader(mockData)))
}

func TestThreeByteUInt32(t *testing.T) {
	mockData := []byte{95, 0, 0}
	reader := getProtoReader(mockData)
	var expected uint32 = 95

	result, err := reader.readThreeBytesUint32()

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestUint64(t *testing.T) {
	mockData := []byte{0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20}
	reader := getProtoReader(mockData)

	result, err := reader.readUint64()

	if err != nil {
		t.Error("Got error", err)
	}

	var expected uint64 = 2332321241244333252

	if result != expected {
		t.Error("Expected", expected, "got", result)
	}
}

func TestUInt32(t *testing.T) {
	mockData := []byte{0xD3, 0x87, 0x2C, 0x4A}
	reader := getProtoReader(mockData)
	var expected uint32 = 1244432339

	result, err := reader.readUint32()

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestUInt16(t *testing.T) {
	mockData := []byte{0x9C, 0x30}
	reader := getProtoReader(mockData)
	var expected uint16 = 12444

	result, err := reader.readUint16()

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestNilString(t *testing.T) {
	mockData := []byte{53, 46, 53, 46, 51, 56, 45, 48, 117, 98, 117, 110, 116, 117, 48, 46, 49, 52, 46, 48, 52, 46, 49, 45, 108, 111, 103, 0}
	reader := getProtoReader(mockData)

	expected := []byte("5.5.38-0ubuntu0.14.04.1-log")

	result, err := reader.readNilString()

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, result) {
		t.Error("Expected", string(expected), "got", string(result))
	}
}

func TestLenIntByte(t *testing.T) {
	mockData := []byte{0xFA}
	reader := getProtoReader(mockData)

	result, null, byteLength := reader.readIntOrNil()

	if null {
		t.Error("Got null int")
	}

	byteLengthExpected := byte(1)
	if byteLengthExpected != byteLength {
		t.Error(
			"Incorrect length",
			"expected", byteLengthExpected,
			"got", byteLength,
		)
	}

	if result != 250 {
		t.Error("Expected", 250, "got", result)
	}
}

func TestLenUint16(t *testing.T) {
	mockData := []byte{0xFC, 0xFB, 0x00}
	reader := getProtoReader(mockData)

	result, null, byteLength := reader.readIntOrNil()

	byteLengthExpected := byte(3)
	if byteLengthExpected != byteLength {
		t.Error(
			"Incorrect length",
			"expected", byteLengthExpected,
			"got", byteLength,
		)
	}

	if null {
		t.Error("Got null int")
	}

	if result != 251 {
		t.Error("Expected", 251, "got", result)
	}
}

func TestLenUint24(t *testing.T) {
	mockData := []byte{0xFD, 0xC4, 0x1E, 0x42}
	reader := getProtoReader(mockData)

	result, null, byteLength := reader.readIntOrNil()

	byteLengthExpected := byte(4)
	if byteLengthExpected != byteLength {
		t.Error(
			"Incorrect length",
			"expected", byteLengthExpected,
			"got", byteLength,
		)
	}

	if null {
		t.Error("Got null int")
	}

	if result != 4333252 {
		t.Error("Expected", 4333252, "got", result)
	}
}

func TestLenUint64(t *testing.T) {
	mockData := []byte{0xFE, 0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20}
	reader := getProtoReader(mockData)

	result, null, byteLength := reader.readIntOrNil()

	byteLengthExpected := byte(9)
	if byteLengthExpected != byteLength {
		t.Error(
			"Incorrect length",
			"expected", byteLengthExpected,
			"got", byteLength,
		)
	}

	if null {
		t.Error("Got null int")
	}

	if result != 2332321241244333252 {
		t.Error("Expected", 2332321241244333252, "got", result)
	}
}

func TestLenNil(t *testing.T) {
	mockData := []byte{0xFB}
	reader := getProtoReader(mockData)

	result, null, byteLength := reader.readIntOrNil()

	byteLengthExpected := byte(1)
	if byteLengthExpected != byteLength {
		t.Error(
			"Incorrect length",
			"expected", byteLengthExpected,
			"got", byteLength,
		)
	}

	if !null {
		t.Error("Got not null")
	}

	if result != 0 {
		t.Error("Expected", 0, "got", result)
	}
}

func TestLenString(t *testing.T) {
	mockData := []byte{0x03, 0x64, 0x65, 0x66}
	reader := getProtoReader(mockData)

	result, byteLength, err := reader.readLenString()

	if err != nil {
		t.Error("Got error", err)
	}

	if byteLength != uint64(4) {
		t.Error(
			"Incorrect lenth",
			"expected", 4,
			"got", byteLength,
		)
	}

	expected := "def"

	if string(result) != expected {
		t.Error("Expected", expected, "got", string(result))
	}
}
