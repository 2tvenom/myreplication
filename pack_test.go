package mysql_replication_listener

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestReadPackTotal(t *testing.T) {
	mockBuff := []byte{0x03, 0x00, 0x00, 0x0a, 0x01, 0x02, 0x03, 0x03, 0x00, 0x00, 0x0b, 0x04, 0x05, 0x06}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, err := reader.readNextPack()

	if err != nil {
		t.Error("Got error", err)
	}

	var expectedLength uint32 = 3

	if pack.length != expectedLength {
		t.Error(
			"incorrect length",
			"expected", expectedLength,
			"got", pack.length,
		)
	}

	var expectedSequence byte = 10

	if pack.sequence != expectedSequence {
		t.Error(
			"incorrect sequence",
			"expected", expectedSequence,
			"got", pack.sequence,
		)
	}

	expectedBuff := []byte{0x01, 0x02, 0x03}

	if !reflect.DeepEqual(expectedBuff, pack.buff) {
		t.Error(
			"incorrect buff",
			"expected", expectedBuff,
			"got", pack.buff,
		)
	}

	pack, err = reader.readNextPack()

	if err != nil {
		t.Error("Got error", err)
	}

	if pack.length != expectedLength {
		t.Error(
			"incorrect length",
			"expected", expectedLength,
			"got", pack.length,
		)
	}

	expectedSequence = 11

	if pack.sequence != expectedSequence {
		t.Error(
			"incorrect sequence",
			"expected", expectedSequence,
			"got", pack.sequence,
		)
	}

	expectedBuff = []byte{0x04, 0x05, 0x06}

	if !reflect.DeepEqual(expectedBuff, pack.buff) {
		t.Error(
			"incorrect buff",
			"expected", expectedBuff,
			"got", pack.buff,
		)
	}
}

func TestReadPackByte(t *testing.T) {
	mockBuff := []byte{
		0x01, 0x00, 0x00,
		0x0a,
		0x10,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	var expected byte = 16
	var result byte

	err := pack.readByte(&result)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result != expected {
		t.Error(
			"Incorrect result",
			"expected", expected,
			"got", result,
		)
	}
}

func TestReadUint16(t *testing.T) {
	mockBuff := []byte{
		0x02, 0x00, 0x00,
		0x0a,
		0x1D, 0x86,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	var expected uint16 = 34333
	var result uint16

	err := pack.readUint16(&result)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result != expected {
		t.Error(
			"Incorrect result",
			"expected", expected,
			"got", result,
		)
	}
}

func TestReadThreeByteUint32(t *testing.T) {
	mockBuff := []byte{
		0x03, 0x00, 0x00,
		0x0a,
		0x76, 0x8A, 0x34,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	var expected uint32 = 3443318
	var result uint32

	err := pack.readThreeByteUint32(&result)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result != expected {
		t.Error(
			"Incorrect result",
			"expected", expected,
			"got", result,
		)
	}
}

func TestReadUint32(t *testing.T) {
	mockBuff := []byte{
		0x04, 0x00, 0x00,
		0x0a,
		0xD6, 0x00, 0x77, 0x14,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	var expected uint32 = 343343318
	var result uint32

	err := pack.readUint32(&result)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result != expected {
		t.Error(
			"Incorrect result",
			"expected", expected,
			"got", result,
		)
	}
}

func TestPackReadUint64(t *testing.T) {
	mockBuff := []byte{
		0x08, 0x00, 0x00,
		0x0a,
		0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20,
	}

	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	var expected uint64 = 2332321241244333252
	var result uint64

	err := pack.readUint64(&result)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result != expected {
		t.Error(
			"Incorrect result",
			"expected", expected,
			"got", result,
		)
	}
}

func TestPackReadSixByteUint64(t *testing.T) {
	mockBuff := []byte{
		0x08, 0x00, 0x00,
		0x0a,
		0x8F, 0x7F, 0xE8, 0x44, 0x9A, 0x27,
	}

	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	var expected uint64 = 43543534534543
	var result uint64

	err := pack.readSixByteUint64(&result)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result != expected {
		t.Error(
			"Incorrect result",
			"expected", expected,
			"got", result,
		)
	}
}

func TestReadIntLengthOrNil(t *testing.T) {
	mockBuff := []byte{
		//pack 0, nil length encoded integer
		0x01, 0x00, 0x00,
		0x00,
		0xFB,
		//pack 1, 29 integer
		0x01, 0x00, 0x00,
		0x01,
		0x1D,
		//pack 2, 251 integer
		0x03, 0x00, 0x00,
		0x02,
		0xFC, 0xFB, 0x00,
		//pack 3, 3443318 integer
		0x04, 0x00, 0x00,
		0x03,
		0xFD, 0x76, 0x8A, 0x34,
		//pack 4, 2332321241244333252 integer
		0x09, 0x00, 0x00,
		0x03,
		0xFE, 0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	var (
		null   bool
		result uint64
	)

	type (
		expectedPair struct {
			null   bool
			result uint64
		}
	)

	testsCollection := []*expectedPair{
		&expectedPair{true, 0},
		&expectedPair{false, 29},
		&expectedPair{false, 251},
		&expectedPair{false, 3443318},
		&expectedPair{false, 2332321241244333252},
	}

	for i, test := range testsCollection {

		t.Log("length int. Test pack", i, "with nil:", test.null, "result:", test.result)

		null = false
		result = 0

		pack, err := reader.readNextPack()

		if err != nil {
			t.Fatal(
				"Error read pack:", err,
			)
		}

		err = pack.readIntLengthOrNil(&result, &null)

		if err != nil {
			t.Error(
				"Got error", err,
			)
		}

		if null != test.null {
			t.Error(
				"Incorrect nil",
				"expected", test.null,
				"got", null,
			)
		}

		if result != test.result {
			t.Error(
				"Incorrect result",
				"expected", test.result,
				"got", result,
			)
		}
	}
}

func TestReadNilString(t *testing.T) {
	mockBuff := []byte{
		0x20,
		0x00, 0x00, 0x0a,
		0x35, 0x2e, 0x35, 0x2e, 0x33, 0x38, 0x2d, 0x30, 0x75, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x30, 0x2e, 0x31, 0x34,
		0x2e, 0x30, 0x34, 0x2e, 0x31, 0x2d, 0x6c, 0x6f, 0x67, 0x00,
		//garbage byte
		0x35, 0x2e, 0x35, 0x2e,
	}

	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	expected := []byte("5.5.38-0ubuntu0.14.04.1-log")

	result, err := pack.readNilString()

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, result) {
		t.Error("Expected", string(expected), "got", string(result))
	}
}

func TestReadStringLength(t *testing.T) {
	mockBuff := []byte{
		0x1D,
		0x00, 0x00, 0x0a,
		0x1B, 0x35, 0x2e, 0x35, 0x2e, 0x33, 0x38, 0x2d, 0x30, 0x75, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x30, 0x2e, 0x31, 0x34,
		0x2e, 0x30, 0x34, 0x2e, 0x31, 0x2d, 0x6c, 0x6f, 0x67,
		//garbage byte
		0xFF,
	}

	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	expected := []byte("5.5.38-0ubuntu0.14.04.1-log")

	result, err := pack.readStringLength()

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, result) {
		t.Error("Expected", string(expected), "got", string(result))
	}
}

func TestReadTotal(t *testing.T) {
	mockBuff := []byte{
		0x14, 0x00, 0x00,
		0x0a,
		0x10,
		0x1D, 0x86,
		0x76, 0x8A, 0x34,
		0xD6, 0x00, 0x77, 0x14,
		0xC4, 0x74, 0x77, 0xCE, 0xCF, 0x11, 0x5E, 0x20,
	}

	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	var expectedByte byte = 16
	var expected16 uint16 = 34333
	var expectedtb32 uint32 = 3443318
	var expected32 uint32 = 343343318
	var expected64 uint64 = 2332321241244333252

	var resultByte byte
	var result16 uint16
	var resulttb32 uint32
	var result32 uint32
	var result64 uint64

	err := pack.readByte(&resultByte)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if resultByte != expectedByte {
		t.Error(
			"Incorrect result",
			"expected", expectedByte,
			"got", resultByte,
		)
	}

	err = pack.readUint16(&result16)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result16 != expected16 {
		t.Error(
			"Incorrect result",
			"expected", expected16,
			"got", result16,
		)
	}

	err = pack.readThreeByteUint32(&resulttb32)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if resulttb32 != expectedtb32 {
		t.Error(
			"Incorrect result",
			"expected", expectedtb32,
			"got", resulttb32,
		)
	}

	err = pack.readUint32(&result32)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result32 != expected32 {
		t.Error(
			"Incorrect result",
			"expected", expected32,
			"got", result32,
		)
	}

	err = pack.readUint64(&result64)
	if err != nil {
		t.Error(
			"Got error", err,
		)
	}

	if result64 != expected64 {
		t.Error(
			"Incorrect result",
			"expected", expected64,
			"got", result64,
		)
	}
}

func TestWritePackUint16(t *testing.T) {
	pack := newPack()

	expected := []byte{
		0x00, 0x00, 0x00,
		0x00,
		0x30, 0x82,
	}

	var data uint16 = 33328

	err := pack.writeUInt16(data)

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, pack.Bytes()) {
		t.Error("Expected", expected, "got", pack.Bytes())
	}
}

func TestWritePackThreeByteUint32(t *testing.T) {
	pack := newPack()

	expected := []byte{
		0x00, 0x00, 0x00,
		0x00,
		0x76, 0x8A, 0x34,
	}

	var data uint32 = 3443318

	err := pack.writeThreeByteUInt32(data)

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, pack.Bytes()) {
		t.Error("Expected", expected, "got", pack.Bytes())
	}
}

func TestWritePackUint32(t *testing.T) {
	pack := newPack()

	expected := []byte{
		0x00, 0x00, 0x00,
		0x00,
		0xD6, 0x00, 0x77, 0x14,
	}

	var data uint32 = 343343318

	err := pack.writeUInt32(data)

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, pack.Bytes()) {
		t.Error("Expected", expected, "got", pack.Bytes())
	}
}

func TestWriteNilString(t *testing.T) {
	pack := newPack()

	expected := []byte{
		0x00, 0x00, 0x00,
		0x00,
		0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00,
	}

	err := pack.writeStringNil("hello")

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, pack.Bytes()) {
		t.Error("Expected", expected, "got", pack.Bytes())
	}
}

func TestWriteStringLength(t *testing.T) {
	pack := newPack()

	expected := []byte{
		0x00, 0x00, 0x00,
		0x00,
		0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F,
	}

	err := pack.writeStringLength("hello")

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, pack.Bytes()) {
		t.Error("Expected", expected, "got", pack.Bytes())
	}
}

func TestPackWithLength(t *testing.T) {
	pack := newPack()
	pack.setSequence(byte(10))

	expected := []byte{
		0x06, 0x00, 0x00,
		0x0A,
		0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00,
	}

	err := pack.writeStringNil("hello")

	if err != nil {
		t.Error("Got error", err)
	}

	if !reflect.DeepEqual(expected, pack.packBytes()) {
		t.Error("Expected", expected, "got", pack.Bytes())
	}
}

func TestPackFlush(t *testing.T) {
	mockBuff := bytes.NewBuffer([]byte{})
	packWriter := newPackWriter(mockBuff)

	pack := newPack()
	pack.setSequence(byte(10))
	pack.writeStringNil("hello")

	err := packWriter.flush(pack)

	if err != nil {
		t.Error("Got error", err)
	}

	expected := []byte{
		0x06, 0x00, 0x00,
		0x0A,
		0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00,
	}

	if !reflect.DeepEqual(expected, mockBuff.Bytes()) {
		t.Error("Expected", expected, "got", pack.Bytes())
	}
}

func TestOkPacket(t *testing.T) {
	mockBuff := []byte{
		//length
		0x07, 0x00, 0x00,
		//sequence id
		0x02,
		//code
		0x00,
		0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	if pack.isError() != nil {
		t.Error(
			"Got error", pack.isError(),
		)
	}
}

func TestOkPacketError(t *testing.T) {
	mockBuff := []byte{
		//length
		0x17, 0x00, 0x00,
		//sequence
		0x01,
		//err code
		0xff,
		//error id
		0x48, 0x04,
		//error text
		0x23, 0x48, 0x59, 0x30, 0x30, 0x30, 0x4e, 0x6f, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x20, 0x75, 0x73,
		0x65, 0x64,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	errorText := "#HY000No tables used"

	err := pack.isError()

	if err == nil || err.Error() != errorText {
		t.Error(
			"incorrect err packet",
			"expected", errorText,
			"got", err.Error(),
		)
	}
}

func TestEOFPacket(t *testing.T) {
	mockBuff := []byte{
		//length
		0x05, 0x00, 0x00,
		//sequence
		0x01,
		//EOF
		0xFE,
		//warning
		0x00, 0x00,
		//status
		0x02, 0x00,
	}
	reader := newPackReader(bytes.NewBuffer(mockBuff))

	pack, _ := reader.readNextPack()

	if !pack.isEOF() {
		t.Error("packet is not EOF")
	}
}

func TestReadDateTime(t *testing.T) {

	type dateTimeTestCase struct {
		buff         []byte
		expectedTime time.Time
	}

	testCases := []*dateTimeTestCase{
		&dateTimeTestCase{
			buff: []byte{
				0x10, 0x00, 0x00, 0x01, 0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x02, 0x00,
			},
			expectedTime: time.Date(2010, 10, 17, 19, 27, 30, 1, time.Local),
		},
		&dateTimeTestCase{
			buff: []byte{
				0x09, 0x00, 0x00, 0x01, 0x04, 0xda, 0x07, 0x0a, 0x11, 0x00, 0x00, 0x02, 0x00,
			},
			expectedTime: time.Date(2010, 10, 17, 0, 0, 0, 0, time.Local),
		},
		&dateTimeTestCase{
			buff: []byte{
				0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00,
			},
			expectedTime: time.Date(1, 1, 1, 2, 30, 0, 0, time.Local),
		},
		&dateTimeTestCase{
			buff: []byte{
				0x0C, 0x00, 0x00, 0x01,
				0x07, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e,
				0x00, 0x00, 0x02, 0x00,
			},
			expectedTime: time.Date(2010, 10, 17, 19, 27, 30, 0, time.Local),
		},
	}

	for i, testCase := range testCases {
		reader := newPackReader(bytes.NewBuffer(testCase.buff))
		pack, _ := reader.readNextPack()

		result := pack.readDateTime()

		if !testCase.expectedTime.Equal(result) {
			t.Fatal(
				"incorrect date time at test", i,
				"expected", testCase.expectedTime,
				"got", result,
			)
		}
	}
}

func TestReadTime(t *testing.T) {

	type timeTestCase struct {
		buff         []byte
		expectedTime time.Duration
	}

	testCases := []*timeTestCase{
		&timeTestCase{
			buff: []byte{
				0x11, 0x00, 0x00, 0x01,
				0x0c, 0x01, 0x78, 0x00, 0x00, 0x00, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x02, 0x00,
			},
			expectedTime: -time.Duration(
				120*24*time.Hour + 19*time.Hour + 27*time.Minute + 30*time.Second + time.Microsecond,
			),
		},
		&timeTestCase{
			buff: []byte{
				0x11, 0x00, 0x00, 0x01,
				0x0c, 0x00, 0x78, 0x00, 0x00, 0x00, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x02, 0x00,
			},
			expectedTime: time.Duration(
				120*24*time.Hour + 19*time.Hour + 27*time.Minute + 30*time.Second + time.Microsecond,
			),
		},
		&timeTestCase{
			buff: []byte{
				0x0D, 0x00, 0x00, 0x01,
				0x08, 0x00, 0x78, 0x00, 0x00, 0x00, 0x13, 0x1b, 0x1e,
				0x00, 0x00, 0x02, 0x00,
			},
			expectedTime: time.Duration(
				120*24*time.Hour + 19*time.Hour + 27*time.Minute + 30*time.Second,
			),
		},
	}

	for i, testCase := range testCases {
		reader := newPackReader(bytes.NewBuffer(testCase.buff))
		pack, _ := reader.readNextPack()

		result := pack.readTime()

		if result != testCase.expectedTime {
			t.Fatal(
				"incorrect time at test", i,
				"expected", testCase.expectedTime,
				"got", result,
			)
		}
	}
}

func TestNewDecimal(t *testing.T) {
	type decimalTestCase struct {
		buff              []byte
		expectedDecimal   float64
		precission, scale int
	}

	testCases := []*decimalTestCase{
		&decimalTestCase{
			buff: []byte{
				0x07, 0x00, 0x00, 0x01,
				0x9e, 0x61, 0x42,
				0x00, 0x00, 0x02, 0x00,
			},
			precission:      6,
			scale:           2,
			expectedDecimal: 7777.66,
		},
		&decimalTestCase{
			buff: []byte{
				0x07, 0x00, 0x00, 0x01,
				0x84, 0xd2, 0x38,
				0x00, 0x00, 0x02, 0x00,
			},
			precission:      6,
			scale:           2,
			expectedDecimal: 1234.56,
		},
		&decimalTestCase{
			buff: []byte{
				0x09, 0x00, 0x00, 0x01,
				0x80, 0x00, 0x00, 0x01, 0x4d,
				0x00, 0x00, 0x02, 0x00,
			},
			precission:      10,
			scale:           0,
			expectedDecimal: 333,
		},
		&decimalTestCase{
			buff: []byte{
				0x09, 0x00, 0x00, 0x01,
				0x7f, 0xff, 0xff, 0xfe, 0xb2,
				0x00, 0x00, 0x02, 0x00,
			},
			precission:      10,
			scale:           0,
			expectedDecimal: -333,
		},
	}

	for i, testCase := range testCases {
		reader := newPackReader(bytes.NewBuffer(testCase.buff))
		pack, _ := reader.readNextPack()

		result, _ := pack.readNewDecimal(testCase.precission, testCase.scale).Float64()

		if result != testCase.expectedDecimal {
			t.Fatal(
				"incorrect decimal at test", i,
				"expected", testCase.expectedDecimal,
				"got", result,
			)
		}
	}
}

func TestDecimalBinarySize(t *testing.T) {

	type decimalSizeTestCase struct {
		expectedSize, precission, scale int
	}

	testCases := []*decimalSizeTestCase{
		&decimalSizeTestCase{
			precission:   6,
			scale:        2,
			expectedSize: 3,
		},
		&decimalSizeTestCase{
			precission:   10,
			scale:        0,
			expectedSize: 5,
		},
	}

	for i, testCase := range testCases {
		size := getDecimalBinarySize(testCase.precission, testCase.scale)

		if size != testCase.expectedSize {
			t.Fatal(
				"incorrect decimal binary size at test", i,
				"expected", testCase.expectedSize,
				"got", size,
			)
		}
	}
}
