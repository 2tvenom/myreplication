package mysql_replication_listener

import (
	"bufio"
	"errors"
)

type (
	protoReader struct {
		*bufio.Reader
	}
)

func newProtoReader(b *bufio.Reader) *protoReader {
	return &protoReader{
		b,
	}
}

func readUint16(buff []byte, dest *uint16) error {
	if len(buff) != 2 {
		return errors.New("incorrect source byte array length")
	}

	*dest = uint16(buff[0] & 0xFF)
	*dest += uint16(buff[1]&0xFF) << 8

	return nil
}

func readThreeBytesUint32(buff []byte, dest *uint32) error {
	if len(buff) != 3 {
		return errors.New("incorrect source byte array length")
	}

	*dest = 0
	for i := 0; i < 3; i++ {
		*dest += uint32(buff[i]&0xFF) << uint(i*8)
	}

	return nil
}

func readUint32(buff []byte, dest *uint32) error {
	if len(buff) != 4 {
		return errors.New("incorrect source byte array length")
	}

	*dest = 0
	for i := 0; i < 4; i++ {
		*dest += uint32(buff[i]&0xFF) << uint(i*8)
	}

	return nil
}

func readSixByteUint64(buff []byte, dest *uint64) error {
	if len(buff) != 6 {
		return errors.New("incorrect source byte array length")
	}

	*dest = 0
	for i := 0; i < 6; i++ {
		*dest += uint64(buff[i]&0xFF) << uint(i*8)
	}

	return nil
}

func readUint64(buff []byte, dest *uint64) error {
	if len(buff) != 8 {
		return errors.New("incorrect source byte array length")
	}

	*dest = 0
	for i := 0; i < 8; i++ {
		*dest += uint64(buff[i]&0xFF) << uint(i*8)
	}

	return nil
}

func writeUInt16(buff []byte, data uint16) {
	for i := 0; i < 2; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
}

func writeThreeByteUInt32(buff []byte, data uint32) {
	for i := 0; i < 3; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
}

func writeUInt32(buff []byte, data uint32) {
	for i := 0; i < 4; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
}

func writeUInt64(buff []byte, data uint64) {
	for i := 0; i < 8; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
}

func writeLengthInt(i uint64) []byte {
	var buff []byte
	switch {
	case i <= 250:
		buff = []byte{byte(i)}
	case i <= 0xffff:
		buff = make([]byte, 3)
		buff[0] = byte(0xFC)
		writeUInt16(buff[1:3], uint16(i))
	case i <= 0xffffff:
		buff = make([]byte, 4)
		buff[0] = byte(0xFD)
		writeThreeByteUInt32(buff[1:4], uint32(i))
	default:
		buff = make([]byte, 9)
		buff[0] = byte(0xFE)
		writeUInt64(buff[1:9], i)
	}
	return buff
}
