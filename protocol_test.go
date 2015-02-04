package myreplication

import (
	"reflect"
	"testing"
)

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

func TestReadSixByteUint64(t *testing.T) {
	mockData := []byte{0x8F, 0x7F, 0xE8, 0x44, 0x9A, 0x27}
	var expected uint64 = 43543534534543
	var result uint64

	err := readSixByteUint64(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if result != expected {
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

func TestReadUInt16Revert(t *testing.T) {
	mockData := []byte{0x15, 0xA8}
	var expected uint16 = 5544
	var result uint16
	err := readUint16Revert(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestReadThreeBytesUint32Revert(t *testing.T) {
	mockData := []byte{0x32, 0xE5, 0x78}
	var expected uint32 = 3335544
	var result uint32
	err := readThreeBytesUint32Revert(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestReadUint32Revert(t *testing.T) {
	mockData := []byte{0x13, 0xE1, 0xA3, 0x0C}
	var expected uint32 = 333554444
	var result uint32
	err := readUint32Revert(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestReadSixByteUint64Revert(t *testing.T) {
	mockData := []byte{0x1E, 0x56, 0x2B, 0x6A, 0xDD, 0x1C}
	var expected uint64 = 33355444444444
	var result uint64
	err := readSixByteUint64Revert(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}

func TestReadUint64Revert(t *testing.T) {
	mockData := []byte{0x2E, 0x4A, 0x3C, 0x00, 0x66, 0xD9, 0x1B, 0x1C}
	var expected uint64 = 3335544446444444444
	var result uint64
	err := readUint64Revert(mockData, &result)

	if err != nil {
		t.Error("Got error", err)
	}

	if expected != result {
		t.Error("Expected", expected, "got", result)
	}
}
