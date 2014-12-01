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

