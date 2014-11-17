package mysql_replication_listener

import (
	"bufio"
)

type (
	protoReader struct {
		*bufio.Reader
	}
)

func newProtoReader(bufio *bufio.Reader) *protoReader {
	return &protoReader{bufio}
}

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
	lb, _ := pr.ReadByte()
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
