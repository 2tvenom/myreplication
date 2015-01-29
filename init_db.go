package mysql_replication_listener

type (
	initDb struct {
	}
)

func (q *initDb) writeServer(schemaName string) *pack {
	pack := newPack()
	pack.WriteByte(_COM_INIT_DB)
	pack.Write([]byte(schemaName))
	return pack
}
