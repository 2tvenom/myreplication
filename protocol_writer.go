package mysql_replication_listener

import (
	"bufio"
)

type (
	protoWriter struct {
		*bufio.Writer
	}
)

func newProtoWriter(bufio *bufio.Writer) *protoWriter {
	return &protoWriter{bufio}
}

func (pw *protoWriter) writeUInt64(data uint64) error {
	buff := make([]byte, 8, 8)

	writeUInt64(buff, data)

	_, err := pw.Write(buff)
	return err
}

func (pw *protoWriter) writeUInt32(data uint32) error {
	buff := make([]byte, 4, 4)

	for i := 0; i < 4; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
	_, err := pw.Write(buff)
	return err
}

func (pw *protoWriter) writeUInt16(data uint16) error {
	buff := make([]byte, 2, 2)

	for i := 0; i < 2; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
	_, err := pw.Write(buff)
	return err
}

func (pw *protoWriter) writeTheeByteUInt32(data uint32) error {
	buff := make([]byte, 3, 3)

	for i := 0; i < 3; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
	_, err := pw.Write(buff)

	return err
}

func (pw *protoWriter) writeStringNil(data string) error {
	_, err := pw.Write([]byte(data))
	if err != nil {
		return err
	}

	err = pw.WriteByte(byte(0))
	return err
}

func (pw *protoWriter) writeStringLength(data string) error {
	pw.writeIntLength(uint64(len(data)))

	_, err := pw.Write([]byte(data))
	if err != nil {
		return err
	}

	return err
}

func (pw *protoWriter) writeIntLength(i uint64) {
	switch {
	case i <= 250:
		pw.WriteByte(byte(i))
	case i <= 0xffff:
		pw.WriteByte(byte(0xFC))
		pw.writeUInt16(uint16(i))
	case i <= 0xffffff:
		pw.WriteByte(byte(0xFD))
		pw.writeTheeByteUInt32(uint32(i))
	default:
		pw.WriteByte(byte(0xFE))
		pw.writeUInt64(i)
	}
}
