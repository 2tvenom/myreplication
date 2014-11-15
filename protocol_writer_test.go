package mysql_replication_listener

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func getProtoWriter(mockData []byte) *protoWriter {
	return newProtoWriter(bufio.NewWriter(bytes.NewBuffer(mockData)))
}

func TestWriteUInt32(t *testing.T) {
	buff := make([]byte, 0, 4)
	writer := getProtoWriter(buff)
	expected := []byte{0x0D, 0x70, 0x16, 0x42}
	var testData uint32 = 1108766733

	err := writer.WriteUInt32(testData)
	if err != nil {
		t.Error("Got error", err)
	}

	writer.Flush()

	if !reflect.DeepEqual(expected, buff[0:4]) {
		t.Error("Expected", expected, "got", buff[0:4])
	}
}

func TestWriteStringNil(t *testing.T) {
	buff := make([]byte, 0, 4)
	writer := getProtoWriter(buff)
	expected := []byte{0x0D, 0x70, 0x16, 0x42}
	var testData uint32 = 1108766733

	err := writer.WriteUInt32(testData)
	if err != nil {
		t.Error("Got error", err)
	}

	writer.Flush()

	if !reflect.DeepEqual(expected, buff[0:4]) {
		t.Error("Expected", expected, "got", buff[0:4])
	}
}
