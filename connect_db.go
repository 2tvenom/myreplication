package mysql_replication_listener

type (
	connectDb struct {
	}
)

func (q *connectDb) writeServer(db string) *pack {
	pack := newPack()
	pack.WriteByte(_COM_INIT_DB)
	pack.Write([]byte(db))
	return pack
}
