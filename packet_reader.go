package mysql_replication_listener

type (
	pkgHeader struct {
		sequence byte
		length   uint32
	}
)

func newPkgHeader() *pkgHeader {
	return &pkgHeader{0, 0}
}

func (p *pkgHeader) read(r *protoReader) error {
	length, err := r.readThreeBytesUint32()

	if err != nil {
		return err
	}

	p.length = length

	seq, err := r.ReadByte()

	if err != nil {
		return err
	}

	if seq != p.sequence {
		panic("Incorrect sequence")
	}
	p.sequence++
	return nil
}

func (p *pkgHeader) resetSequence() {
	p.sequence = 0
}

func (p *pkgHeader) write(r *protoWriter, length uint32) error {
	//	r.WriteUInt32()
	return nil
}
