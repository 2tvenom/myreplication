package myreplication

import (
	//	"reflect"
	"bytes"
	"reflect"
	"testing"
)

func TestReceiveQueryDataResultSetOneRowOneColumn(t *testing.T) {
	mockBuff := []byte{
		//length
		0x01, 0x00, 0x00,
		//sequence
		0x01,
		//columns count
		0x01,
		//length
		0x27, 0x00, 0x00,
		//sequence
		0x02,
		//catalog
		0x03, 0x64, 0x65, 0x66,
		//schema
		0x00,
		//table
		0x00,
		//orig_table
		0x00,
		//name
		0x11, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74,
		//orig name
		0x00,
		//filler
		0x0c,
		//char set
		0x08, 0x00,
		//column length
		0x1c, 0x00, 0x00, 0x00,
		//column type
		0xfd,
		//flags
		0x00, 0x00,
		//decimals
		0x1f,
		//filler
		0x00, 0x00,
		//length
		0x05, 0x00, 0x00,
		//sequence id
		0x03,
		//EOF
		0xfe,
		//warnings
		0x00, 0x00,
		//status flags
		0x02, 0x00,
		//length
		0x1d, 0x00, 0x00,
		//sequence id
		0x04,
		//data
		0x1c, 0x4d, 0x79, 0x53, 0x51, 0x4c, 0x20, 0x43, 0x6f, 0x6d, 0x6d, 0x75, 0x6e, 0x69, 0x74, 0x79, 0x20, 0x53,
		0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x28, 0x47, 0x50, 0x4c, 0x29,
		//length
		0x05, 0x00, 0x00,
		//sequence id
		0x05,
		//EOF
		0xfe,
		//warnings
		0x00, 0x00,
		//status flag
		0x02, 0x00,
	}

	rs := resultSet{}
	rs.reader = newPackReader(bytes.NewBuffer(mockBuff))
	err := rs.init()

	if err != nil {
		t.Error("ResultSet init error:", err)
	}

	if len(rs.columns) != 1 {
		t.Fatal(
			"Incorrect columns count",
			"expected", 1,
			"got", len(rs.columns),
		)
	}
	type (
		checkPair struct {
			columnName    string
			expectedData  string
			columnPointer *[]byte
		}
	)
	checkDataMap := []*checkPair{
		&checkPair{"catalog", "def", &(rs.columns[0].catalog)},
		&checkPair{"schema", "", &(rs.columns[0].schema)},
		&checkPair{"table", "", &(rs.columns[0].table)},
		&checkPair{"org_table", "", &(rs.columns[0].org_table)},
		&checkPair{"name", "@@version_comment", &(rs.columns[0].name)},
		&checkPair{"org_name", "", &(rs.columns[0].org_name)},
	}

	for _, pair := range checkDataMap {
		if string(*(pair.columnPointer)) != pair.expectedData {
			t.Fatal(
				"Incorrect column info", pair.columnName,
				"expected", pair.expectedData,
				"got", string(*pair.columnPointer),
			)
		}
	}

	pack, err := rs.nextRow()

	if err != nil {
		t.Fatal(
			"Got incorrect error", err,
		)
	}

	expectedString := "MySQL Community Server (GPL)"
	str, err := pack.readStringLength()

	if err != nil {
		t.Fatal(
			"Got incorrect error read string", err,
		)
	}

	if string(str) != expectedString {
		t.Fatal(
			"Got incorrect data",
			"expected", expectedString,
			"got", string(str),
		)
	}

	pack, err = rs.nextRow()

	if err != EOF_ERR {
		t.Fatal(
			"Got incorrect next row", err,
		)
	}
}

func TestReceiveQueryDataResultSetOneRowManyColumns(t *testing.T) {
	mockBuff := []byte{
		0x01, 0x00, 0x00,
		0x01,
		0x02,
		//column 1
		0x27, 0x00, 0x00,
		0x02,
		0x03, 0x64, 0x65, 0x66,
		0x00,
		0x00,
		0x00,
		0x11, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74,
		0x00,
		0x0c,
		0x08, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0xfd,
		0x00, 0x00,
		0x1f,
		0x00, 0x00,
		//column 2
		0x1f, 0x00, 0x00,
		0x03,
		0x03, 0x64, 0x65, 0x66,
		0x00,
		0x00,
		0x00,
		0x09, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
		0x00,
		0x0c,
		0x08, 0x00,
		0x1b, 0x00, 0x00, 0x00,
		0xfd,
		0x00, 0x00,
		0x1f,
		0x00, 0x00,
		0x05, 0x00, 0x00,
		0x04,
		0xfe,
		0x00, 0x00,
		0x02, 0x00,
		0x25, 0x00, 0x00,
		0x05,
		0x08, 0x28, 0x55, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x29,
		0x1b, 0x35, 0x2e, 0x35, 0x2e, 0x33, 0x38, 0x2d, 0x30, 0x75, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x30, 0x2e, 0x31,
		0x34, 0x2e, 0x30, 0x34, 0x2e, 0x31, 0x2d, 0x6c, 0x6f, 0x67,
		0x05, 0x00, 0x00,
		0x06,
		0xfe,
		0x00, 0x00,
		0x02, 0x00,
	}
	rs := resultSet{}
	rs.reader = newPackReader(bytes.NewBuffer(mockBuff))
	err := rs.init()

	if err != nil {
		t.Fatal(
			"Query got error",
			err,
		)
	}

	if len(rs.columns) != 2 {
		t.Fatal(
			"Incorrect columns count",
			"expected", 1,
			"got", len(rs.columns),
		)
	}
	type (
		checkPair struct {
			columnName    string
			expectedData  string
			columnPointer *[]byte
		}
	)
	checkDataMap := []*checkPair{
		&checkPair{"catalog", "def", &(rs.columns[0].catalog)},
		&checkPair{"schema", "", &(rs.columns[0].schema)},
		&checkPair{"table", "", &(rs.columns[0].table)},
		&checkPair{"org_table", "", &(rs.columns[0].org_table)},
		&checkPair{"name", "@@version_comment", &(rs.columns[0].name)},
		&checkPair{"org_name", "", &(rs.columns[0].org_name)},
		&checkPair{"catalog", "def", &(rs.columns[1].catalog)},
		&checkPair{"schema", "", &(rs.columns[1].schema)},
		&checkPair{"table", "", &(rs.columns[1].table)},
		&checkPair{"org_table", "", &(rs.columns[1].org_table)},
		&checkPair{"name", "@@version", &(rs.columns[1].name)},
		&checkPair{"org_name", "", &(rs.columns[1].org_name)},
	}

	for _, pair := range checkDataMap {
		if string(*(pair.columnPointer)) != pair.expectedData {
			t.Fatal(
				"Incorrect column info", pair.columnName,
				"expected", pair.expectedData,
				"got", string(*pair.columnPointer),
			)
		}
	}
	pack, err := rs.nextRow()

	if err != nil {
		t.Fatal(
			"Got incorrect error", err,
		)
	}

	expectedString := "(Ubuntu)"
	str, err := pack.readStringLength()

	if err != nil {
		t.Fatal(
			"Got incorrect error read string", err,
		)
	}

	if string(str) != expectedString {
		t.Fatal(
			"Got incorrect data",
			"expected", expectedString,
			"got", string(str),
		)
	}

	expectedString = "5.5.38-0ubuntu0.14.04.1-log"
	str, err = pack.readStringLength()

	if err != nil {
		t.Fatal(
			"Got incorrect error read string", err,
		)
	}

	if string(str) != expectedString {
		t.Fatal(
			"Got incorrect data",
			"expected", expectedString,
			"got", string(str),
		)
	}

	pack, err = rs.nextRow()

	if err != EOF_ERR {
		t.Fatal(
			"Got incorrect next row", err,
		)
	}
}

func TestFieldList(t *testing.T) {
	mockBuff := []byte{
		//column 1
		0x2e, 0x00, 0x00,
		0x01,
		//catalog "def"
		0x03,
		0x64, 0x65, 0x66,
		//schema "test"
		0x04,
		0x74, 0x65, 0x73, 0x74,
		//table "table01"
		0x07,
		0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31,
		//orig_table "table01"
		0x07,
		0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31,
		//name "id"
		0x02,
		0x69, 0x64,
		//orig_name "id"
		0x02,
		0x69, 0x64,
		//filler
		0x0c,
		//char set
		0x3f, 0x00,
		//column length
		0x0b, 0x00, 0x00, 0x00,
		//column_type LONG
		0x03,
		//flags
		0x03, 0x42,
		//decimals
		0x00,
		//filler
		0x00, 0x00,
		//wtf???
		0x01, 0x30,

		//column 2
		0x31, 0x00, 0x00,
		0x02,
		//catalog "def"
		0x03,
		0x64, 0x65, 0x66,
		//schema "test"
		0x04,
		0x74, 0x65, 0x73, 0x74,
		//table "table01"
		0x07,
		0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31,
		//orig_table "table01"
		0x07,
		0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31,
		//name "data"
		0x04,
		0x64, 0x61, 0x74, 0x61,
		//orig_name "data"
		0x04,
		0x64, 0x61, 0x74, 0x61,
		//filler
		0x0c,
		//char set
		0x08, 0x00,
		//column length
		0x2d, 0x00, 0x00, 0x00,
		//column type "VAR_STRING"
		0xfd,
		//flags
		0x01, 0x10,
		//decimals
		0x00,
		//filler
		0x00, 0x00,
		//wtf?
		0x00,
		//column 3
		0x2d, 0x00, 0x00,
		0x03,
		//catalog "def"
		0x03,
		0x64, 0x65, 0x66,
		//schema "test"
		0x04,
		0x74, 0x65, 0x73, 0x74,
		//table "table01"
		0x07,
		0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31,
		//orig_table "table01"
		0x07,
		0x74, 0x61, 0x62, 0x6c, 0x65, 0x30, 0x31,
		//name "dl"
		0x02,
		0x64, 0x6c,
		//orig_name "dl"
		0x02,
		0x64, 0x6c,
		//filler
		0x0c,
		//char set
		0x3f, 0x00,
		//column length
		0x16, 0x00, 0x00, 0x00,
		//column type DOUBLE
		0x05,
		//flags
		0x00, 0x00,
		//decimals
		0x1f,
		//filler
		0x00, 0x00,
		//wtf???
		0xfb,
		//EOF packet
		0x05, 0x00, 0x00,
		0x04,
		0xfe, 0x00, 0x00,
		0x02, 0x00,
	}
	rs := resultSet{}
	rs.reader = newPackReader(bytes.NewBuffer(mockBuff))
	err := rs.initFieldList()

	if err != nil {
		t.Fatal(
			"Query got error",
			err,
		)
	}

	if len(rs.columns) != 3 {
		t.Fatal(
			"Incorrect columns count",
			"expected", 3,
			"got", len(rs.columns),
		)
	}
	type (
		checkPair struct {
			columnName    string
			expectedData  interface{}
			columnPointer interface{}
		}
	)
	checkDataMap := []*checkPair{
		&checkPair{"schema", "test", rs.columns[0].schema},
		&checkPair{"table", "table01", rs.columns[0].table},
		&checkPair{"org_table", "table01", rs.columns[0].org_table},
		&checkPair{"name", "id", rs.columns[0].name},
		&checkPair{"org_name", "id", rs.columns[0].org_name},
		&checkPair{"column_type", MYSQL_TYPE_LONG, rs.columns[0].column_type},

		&checkPair{"schema", "test", rs.columns[1].schema},
		&checkPair{"table", "table01", rs.columns[1].table},
		&checkPair{"org_table", "table01", rs.columns[1].org_table},
		&checkPair{"name", "data", rs.columns[1].name},
		&checkPair{"org_name", "data", rs.columns[1].org_name},
		&checkPair{"column_type", MYSQL_TYPE_VAR_STRING, rs.columns[1].column_type},

		&checkPair{"schema", "test", rs.columns[2].schema},
		&checkPair{"table", "table01", rs.columns[2].table},
		&checkPair{"org_table", "table01", rs.columns[2].org_table},
		&checkPair{"name", "dl", rs.columns[2].name},
		&checkPair{"org_name", "dl", rs.columns[2].org_name},
		&checkPair{"column_type", MYSQL_TYPE_DOUBLE, rs.columns[2].column_type},
	}

	for _, pair := range checkDataMap {
		if reflect.DeepEqual(pair.columnPointer, pair.expectedData) {
			t.Fatal(
				"Incorrect column info", pair.columnName,
				"expected", pair.expectedData,
				"got", pair.columnPointer,
			)
		}
	}
}
