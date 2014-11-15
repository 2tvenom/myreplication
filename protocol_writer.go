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

func (pw *protoWriter) WriteUInt32(data uint32) error {
	buff := make([]byte, 4, 4)

	for i := 0; i < 4; i++ {
		buff[i] = byte(data >> uint(i*8))
	}
	_, err := pw.Write(buff)
	return err
}

func (pw *protoWriter) WriteStringNil(data string) error {
	_, err := pw.Write([]byte(data))
	if err != nil {
		return err
	}
	err = pw.WriteByte(byte(0))
	return err
}
