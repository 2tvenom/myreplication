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
