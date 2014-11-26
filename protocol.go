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

	*dest = uint32(buff[0] & 0xFF)
	*dest += uint32(buff[1]&0xFF) << 8
	*dest += uint32(buff[2]&0xFF) << 16

	return nil
}

func readUint32(buff []byte, dest *uint32) error {
	if len(buff) != 4 {
		return errors.New("incorrect source byte array length")
	}

	*dest = uint32(buff[0] & 0xFF)
	*dest += uint32(buff[1]&0xFF) << 8
	*dest += uint32(buff[2]&0xFF) << 16
	*dest += uint32(buff[3]&0xFF) << 24

	return nil
}

func readUint64(buff []byte, dest *uint64) error {
	if len(buff) != 8 {
		return errors.New("incorrect source byte array length")
	}

	*dest = uint64(buff[0] & 0xFF)
	*dest += uint64(buff[1]&0xFF) << 8
	*dest += uint64(buff[2]&0xFF) << 16
	*dest += uint64(buff[3]&0xFF) << 24
	*dest += uint64(buff[4]&0xFF) << 32
	*dest += uint64(buff[5]&0xFF) << 40
	*dest += uint64(buff[6]&0xFF) << 48
	*dest += uint64(buff[7]&0xFF) << 56

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

//#############################################

func (pr *protoReader) readThreeBytesUint32() (uint32, error) {
	var result uint32

	buff := make([]byte, 3)

	_, err := pr.Read(buff)
	if err != nil {
		return 0, err
	}

	result = uint32(buff[0] & 0xFF)
	result += uint32(buff[1]&0xFF) << 8
	result += uint32(buff[2]&0xFF) << 16

	return result, nil
}

func (pr *protoReader) readUint32() (uint32, error) {
	var result uint32

	buff := make([]byte, 4)

	_, err := pr.Read(buff)
	if err != nil {
		return 0, err
	}

	result = uint32(buff[0] & 0xFF)
	result += uint32(buff[1]&0xFF) << 8
	result += uint32(buff[2]&0xFF) << 16
	result += uint32(buff[3]&0xFF) << 24

	return result, nil
}

func (pr *protoReader) readUint64() (uint64, error) {
	var result uint64

	buff := make([]byte, 8)

	_, err := pr.Read(buff)
	if err != nil {
		return 0, err
	}

	result = uint64(buff[0] & 0xFF)
	result += uint64(buff[1]&0xFF) << 8
	result += uint64(buff[2]&0xFF) << 16
	result += uint64(buff[3]&0xFF) << 24
	result += uint64(buff[4]&0xFF) << 32
	result += uint64(buff[5]&0xFF) << 40
	result += uint64(buff[6]&0xFF) << 48
	result += uint64(buff[7]&0xFF) << 56

	return result, nil
}

func (pr *protoReader) readUint16() (uint16, error) {
	b1, err := pr.ReadByte()
	if err != nil {
		return 0, err
	}

	b2, err := pr.ReadByte()
	if err != nil {
		return 0, err
	}

	return uint16(b1&0xFF) + (uint16(b2&0xFF) << 8), nil
}

func (pr *protoReader) readNilString() ([]byte, error) {
	buff, err := pr.ReadBytes(byte(0))

	if err != nil {
		return []byte{}, err
	}

	return buff[0 : len(buff)-1], nil
}

func (pr *protoReader) readLenString() ([]byte, uint64, error) {
	l, null, byteLength := pr.readIntOrNil()
	if null {
		panic("Incorrect packet data")
	}

	if l == 0 {
		return []byte{}, uint64(byteLength), nil
	}
	str := make([]byte, l)
	_, err := pr.Reader.Read(str)
	return str, l + uint64(byteLength), err
}

func (pr *protoReader) readIntOrNil() (value uint64, null bool, byteLength byte) {
	lb, _ := pr.Reader.ReadByte()
	byteLength = 1
	switch lb {
	case 0xFB:
		null = true
		byteLength = 1
	case 0xFC:
		val, _ := pr.readUint16()
		value = uint64(val)
		byteLength = 3
	case 0xFD:
		val, _ := pr.readThreeBytesUint32()
		value = uint64(val)
		byteLength = 4
	case 0xFE:
		value, _ = pr.readUint64()
		byteLength = 9
	default:
		value = uint64(lb)
	}
	return
}
