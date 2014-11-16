package mysql_replication_listener

import "fmt"

type (
	errPacket struct {
		code        uint16
		description []byte
	}
)

func (e *errPacket) Error() string {
	return fmt.Sprintf("%s", string(e.description))
}

func ok_packet(r *protoReader) (err error) {
	length, err := r.readThreeBytesUint32()

	if err != nil {
		return
	}
	//sequence id
	_, err = r.Reader.ReadByte()
	if err != nil {
		return
	}

	code, err := r.Reader.ReadByte()
	if err != nil {
		return
	}
	length -= 1

	if code == 0x00 {
		devNull := make([]byte, int(length))
		r.Reader.Read(devNull)
		devNull = nil
		return nil
	} else if code == 0xFF {
		errPack := &errPacket{}
		errPack.code, err = r.readUint16()
		length -= 2
		if err != nil {
			return
		}

		errPack.description = make([]byte, length)
		_, err = r.Reader.Read(errPack.description)
		if err != nil {
			return
		}

		return errPack
	} else {
		panic("Incorrect ok/err packet")
	}
	return
}
