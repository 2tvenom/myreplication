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
