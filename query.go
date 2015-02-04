package myreplication

type (
	query struct {
	}
)

func (q *query) writeServer(queryCommand string) *pack {
	pack := newPack()
	pack.WriteByte(_COM_QUERY)
	pack.Write([]byte(queryCommand))
	return pack
}
