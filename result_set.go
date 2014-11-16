package mysql_replication_listener

import "fmt"

type (
	resultSet struct {
		reader *protoReader
	}
)

func (rs *resultSet) read() error {
	buff := make([]byte, 300)
	rs.reader.Reader.Read(buff)

	fmt.Printf("% x \n", buff)

	return nil
}
