package mysql_replication_listener

import (
	"errors"
)

type (
	resultSet struct {
		reader      *packReader
		columns     []*columnSet
		finish      bool
		sequenceId  byte
		lastWarning uint16
		lastStatus  uint16
	}

	columnSet struct {
		catalog       []byte
		schema        []byte
		table         []byte
		org_table     []byte
		name          []byte
		org_name      []byte
		character_set uint16
		column_length uint32
		column_type   byte
		flags         uint16
		decimals      byte
	}
)

var (
	EOF_ERR = errors.New("EOF")
)

func (rs *resultSet) setReader(reader *packReader) {
	rs.reader = reader
}

func (rs *resultSet) init() error {
	pack, err := rs.reader.readNextPack()
	if err != nil {
		return err
	}

	queryErr := pack.isError()

	if queryErr != nil {
		return queryErr
	}

	var (
		columnCount uint64
		null        bool
	)

	err = pack.readIntLengthOrNil(&columnCount, &null)

	if err != nil {
		return err
	}

	if null {
		panic("Column count got panic")
	}

	rs.columns = make([]*columnSet, columnCount)

	sequenceId := pack.getSequence() + 1

	var i uint64

	for i = 0; i < columnCount; i++ {
		columnPack, err := rs.reader.readNextPack()
		if err != nil {
			return err
		}

		if columnPack.getSequence() != sequenceId {
			panic("Incorrect sequence")
		}
		sequenceId++

		rs.columns[i] = packToColumnPack(columnPack)
	}

	pack, err = rs.reader.readNextPack()

	if err != nil {
		return err
	}

	if sequenceId != pack.getSequence() {
		panic("Incorrect sequence")
	}

	rs.finish = false
	rs.sequenceId = pack.getSequence()
	rs.sequenceId++
	eof, _ := pack.ReadByte()

	if eof != _MYSQL_EOF {
		panic("Incorrect EOF packet")
	}
	pack.readUint16(&rs.lastWarning)
	pack.readUint16(&rs.lastStatus)

	return nil
}

func (rs *resultSet) initFieldList() error {
	rs.columns = []*columnSet{}

	for {
		columnPack, err := rs.reader.readNextPack()

		if err != nil {
			return err
		}

		err = columnPack.isError()

		if err != nil {
			return err
		}

		if columnPack.isEOF() {
			columnPack.readUint16(&rs.lastWarning)
			columnPack.readUint16(&rs.lastStatus)
			break
		}

		columnDef := packToColumnPack(columnPack)
		rs.columns = append(rs.columns, columnDef)
	}

	rs.finish = true

	return nil
}

func packToColumnPack(columnPack *pack) *columnSet {
	cs := &columnSet{}
	cs.catalog, _ = columnPack.readStringLength()
	cs.schema, _ = columnPack.readStringLength()
	cs.table, _ = columnPack.readStringLength()
	cs.org_table, _ = columnPack.readStringLength()
	cs.name, _ = columnPack.readStringLength()
	cs.org_name, _ = columnPack.readStringLength()
	//filler
	filler, _ := columnPack.ReadByte()
	if filler != 0x0c {
		panic("incorrect filler")
	}
	columnPack.readUint16(&cs.character_set)
	columnPack.readUint32(&cs.column_length)
	cs.column_type, _ = columnPack.ReadByte()
	columnPack.readUint16(&cs.flags)
	cs.decimals, _ = columnPack.ReadByte()
	return cs
}

func (rs *resultSet) nextRow() (*pack, error) {
	if rs.finish {
		return nil, EOF_ERR
	}

	pack, err := rs.reader.readNextPack()

	if err != nil {
		return nil, err
	}

	if pack.getSequence() != rs.sequenceId {
		panic("Incorrect seuence")
	}
	rs.sequenceId++

	if pack.isEOF() {
		rs.finish = true
		return nil, EOF_ERR
	}

	return pack, nil
}
