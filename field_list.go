package myreplication

type (
	fieldList struct {
	}
)

func (q *fieldList) writeServer(table string) *pack {
	pack := newPack()
	pack.WriteByte(_COM_FIELD_LIST)
	pack.writeStringNil(table)
	return pack
}
