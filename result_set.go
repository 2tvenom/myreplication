package mysql_replication_listener

import "fmt"

import (
)

type (
	resultSet struct {
		reader  *protoReader
		columns []*columnSet
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

func (rs *resultSet) init() error {
	_, err := rs.reader.readThreeBytesUint32()
	if err != nil {
		return err
	}

	sequenceId, err := rs.reader.Reader.ReadByte()
	if err != nil {
		return err
	}
	sequenceId++

	columnCount, null, _ := rs.reader.readIntOrNil()
	if null {
		panic("Column count got panic")
	}

	rs.columns = make([]*columnSet, columnCount)

	var i uint64
	for i = 0; i < columnCount; i++ {
		length, err := rs.reader.readThreeBytesUint32()
		if err != nil {
			return err
		}
		sc, err := rs.reader.Reader.ReadByte()

		if err != nil {
			return err
		}
		if sc != sequenceId {
			panic("Incorrect sequence")
		}
		sequenceId++

		cs := &columnSet{}
		var strlength uint64
		cs.catalog, strlength, err = rs.reader.readLenString()
		if err != nil {
			return err
		}
		length -= uint32(strlength)
		cs.schema, strlength, err = rs.reader.readLenString()
		if err != nil {
			return err
		}
		length -= uint32(strlength)
		cs.table, strlength, err = rs.reader.readLenString()
		if err != nil {
			return err
		}
		length -= uint32(strlength)
		cs.org_table, strlength, err = rs.reader.readLenString()
		if err != nil {
			return err
		}
		length -= uint32(strlength)
		cs.name, strlength, err = rs.reader.readLenString()
		if err != nil {
			return err
		}

		length -= uint32(strlength)

		cs.org_name, strlength, err = rs.reader.readLenString()
		if err != nil {
			return err
		}
		length -= uint32(strlength)

		_, err = rs.reader.Reader.ReadByte()
		if err != nil {
			return err
		}
		length--
		cs.character_set, err = rs.reader.readUint16()
		if err != nil {
			return err
		}
		length -= 2
		cs.column_length, err = rs.reader.readUint32()
		if err != nil {
			return err
		}
		length -= 4
		cs.column_type, err = rs.reader.Reader.ReadByte()
		if err != nil {
			return err
		}
		length--
		cs.flags, err = rs.reader.readUint16()
		if err != nil {
			return err
		}
		length -= 2
		cs.decimals, err = rs.reader.Reader.ReadByte()
		if err != nil {
			return err
		}
		length--
		devNullFiller := make([]byte, 2)
		_, err = rs.reader.Reader.Read(devNullFiller)
		if err != nil {
			return err
		}
		devNullFiller = nil
		length -= 2
		if length != 0 {
			panic("Incorrect length")
		}
		rs.columns[i] = cs
	}

	return nil
}

func (rs *resultSet) read() error {
	buff := make([]byte, 300)
	rs.reader.Reader.Read(buff)

	fmt.Printf("% x \n", buff)

	return nil
}
