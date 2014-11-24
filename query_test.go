package mysql_replication_listener

//
//import (
//	"reflect"
//	"testing"
//)
//
//func TestSendQuery(t *testing.T) {
//	mockWriter := make([]byte, 0, 100)
//	writer := getProtoWriter(mockWriter)
//	mockReader := []byte{
//		0x01, 0x00, 0x00, 0x01, 0x01, 0x27, 0x00, 0x00, 0x02, 0x03, 0x64, 0x65, 0x66,
//		0x00, 0x00, 0x00, 0x11, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
//		0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x00, 0x0c, 0x08, 0x00, 0x1c,
//		0x00, 0x00, 0x00, 0xfd, 0x00, 0x00, 0x1f, 0x00, 0x00, 0x05, 0x00, 0x00, 0x03,
//		0xfe, 0x00, 0x00, 0x02, 0x00, 0x1d, 0x00, 0x00, 0x04, 0x1c, 0x4d, 0x79, 0x53,
//		0x51, 0x4c, 0x20, 0x43, 0x6f, 0x6d, 0x6d, 0x75, 0x6e, 0x69, 0x74, 0x79, 0x20,
//		0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x28, 0x47, 0x50, 0x4c, 0x29, 0x05,
//		0x00, 0x00, 0x05, 0xfe, 0x00, 0x00, 0x02, 0x00,
//	}
//	reader := getProtoReader(mockReader)
//
//	command := "select @@version_comment limit 1"
//	_, err := query(writer, reader, command)
//
//	if err != nil {
//		t.Fatal("Query got error", err)
//	}
//
//	expectedLength := []byte{0x21, 0x00, 0x00}
//
//	offset := 0
//
//	if !reflect.DeepEqual(expectedLength, mockWriter[offset:offset+3]) {
//		t.Fatal(
//			"Incorrect query length",
//			"expected", expectedLength,
//			"got", mockWriter[offset:offset+3],
//		)
//	}
//
//	offset += 3
//
//	expectedSequence := byte(0)
//
//	if expectedSequence != mockWriter[offset : offset+1][0] {
//		t.Fatal(
//			"Incorrect sequence",
//			"expected", expectedSequence,
//			"got", mockWriter[offset : offset+1][0],
//		)
//	}
//
//	offset++
//
//	if _COM_QUERY != mockWriter[offset : offset+1][0] {
//		t.Fatal(
//			"Incorrect command",
//			"expected", _COM_QUERY,
//			"got", mockWriter[offset : offset+1][0],
//		)
//	}
//
//	offset++
//
//	if command != string(mockWriter[offset:offset+len(command)]) {
//		t.Fatal(
//			"Incorrect commnad",
//			"expected", command,
//			"got", string(mockWriter[offset:offset+len(command)]),
//		)
//	}
//
//	offset += len(command)
//}
//
//func TestReceiveQueryDataResultSetOneRowOneColumn(t *testing.T) {
//	mockWriter := make([]byte, 0, 100)
//	writer := getProtoWriter(mockWriter)
//	mockReader := []byte{
//		//length
//		0x01, 0x00, 0x00,
//		//sequence
//		0x01,
//		//columns count
//		0x01,
//		//length
//		0x27, 0x00, 0x00,
//		//sequence
//		0x02,
//		//catalog
//		0x03, 0x64, 0x65, 0x66,
//		//schema
//		0x00,
//		//table
//		0x00,
//		//orig_table
//		0x00,
//		//name
//		0x11, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74,
//		//orig name
//		0x00,
//		//filler
//		0x0c,
//		//char set
//		0x08, 0x00,
//		//column length
//		0x1c, 0x00, 0x00, 0x00,
//		//column type
//		0xfd,
//		//flags
//		0x00, 0x00,
//		//decimals
//		0x1f,
//		//filler
//		0x00, 0x00,
//		//length
//		0x05, 0x00, 0x00,
//		//sequence id
//		0x03,
//		//EOF
//		0xfe,
//		//warnings
//		0x00, 0x00,
//		//status flags
//		0x02, 0x00,
//		//length
//		0x1d, 0x00, 0x00,
//		//sequence id
//		0x04,
//		//data
//		0x1c, 0x4d, 0x79, 0x53, 0x51, 0x4c, 0x20, 0x43, 0x6f, 0x6d, 0x6d, 0x75, 0x6e, 0x69, 0x74, 0x79, 0x20, 0x53,
//		0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x28, 0x47, 0x50, 0x4c, 0x29,
//		//length
//		0x05, 0x00, 0x00,
//		//sequence id
//		0x05,
//		//EOF
//		0xfe,
//		//warnings
//		0x00, 0x00,
//		//status flag
//		0x02, 0x00,
//	}
//	reader := getProtoReader(mockReader)
//
//	command := "SELECT @@version_comment"
//	rs, err := query(writer, reader, command)
//
//	if err != nil {
//		t.Fatal(
//			"Query got error",
//			err,
//		)
//	}
//
//	if len(rs.columns) != 1 {
//		t.Fatal(
//			"Incorrect columns count",
//			"expected", 1,
//			"got", len(rs.columns),
//		)
//	}
//	type (
//		checkPair struct {
//			columnName    string
//			expectedData  string
//			columnPointer *[]byte
//		}
//	)
//	checkDataMap := []*checkPair{
//		&checkPair{"catalog", "def", &(rs.columns[0].catalog)},
//		&checkPair{"schema", "", &(rs.columns[0].schema)},
//		&checkPair{"table", "", &(rs.columns[0].table)},
//		&checkPair{"org_table", "", &(rs.columns[0].org_table)},
//		&checkPair{"name", "@@version_comment", &(rs.columns[0].name)},
//		&checkPair{"org_name", "", &(rs.columns[0].org_name)},
//	}
//
//	for _, pair := range checkDataMap {
//		if string(*(pair.columnPointer)) != pair.expectedData {
//			t.Fatal(
//				"Incorrect column info", pair.columnName,
//				"expected", pair.expectedData,
//				"got", string(*pair.columnPointer),
//			)
//		}
//	}
//
//	stat := rs.nextRow()
//
//	if stat != nil {
//		t.Fatal(
//			"Got incorrect error", stat,
//		)
//	}
//
//	expectedString := "MySQL Community Server (GPL)"
//	str, _, err := rs.buff.readLenString()
//
//	if err != nil {
//		t.Fatal(
//			"Got incorrect error read string", stat,
//		)
//	}
//
//	if string(str) != expectedString {
//		t.Fatal(
//			"Got incorrect data",
//			"expected", expectedString,
//			"got", string(str),
//		)
//	}
//
//	stat = rs.nextRow()
//
//	if stat != EOF_ERR {
//		t.Fatal(
//			"Got incorrect next row", stat,
//		)
//	}
//}
//
//func TestReceiveQueryDataResultSetOneRowManyColumns(t *testing.T) {
//	mockWriter := make([]byte, 0, 100)
//	writer := getProtoWriter(mockWriter)
//	mockReader := []byte{
//		0x01, 0x00, 0x00,
//		0x01,
//		0x02,
//		//column 1
//		0x27, 0x00, 0x00,
//		0x02,
//		0x03, 0x64, 0x65, 0x66,
//		0x00,
//		0x00,
//		0x00,
//		0x11, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74,
//		0x00,
//		0x0c,
//		0x08, 0x00,
//		0x08, 0x00, 0x00, 0x00,
//		0xfd,
//		0x00, 0x00,
//		0x1f,
//		0x00, 0x00,
//		//column 2
//		0x1f, 0x00, 0x00,
//		0x03,
//		0x03, 0x64, 0x65, 0x66,
//		0x00,
//		0x00,
//		0x00,
//		0x09, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
//		0x00,
//		0x0c,
//		0x08, 0x00,
//		0x1b, 0x00, 0x00, 0x00,
//		0xfd,
//		0x00, 0x00,
//		0x1f,
//		0x00, 0x00,
//		0x05, 0x00, 0x00,
//		0x04,
//		0xfe,
//		0x00, 0x00,
//		0x02, 0x00,
//		0x25, 0x00, 0x00,
//		0x05,
//		0x08, 0x28, 0x55, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x29,
//		0x1b, 0x35, 0x2e, 0x35, 0x2e, 0x33, 0x38, 0x2d, 0x30, 0x75, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x30, 0x2e, 0x31,
//		0x34, 0x2e, 0x30, 0x34, 0x2e, 0x31, 0x2d, 0x6c, 0x6f, 0x67,
//		0x05, 0x00, 0x00,
//		0x06,
//		0xfe,
//		0x00, 0x00,
//		0x02, 0x00,
//	}
//	reader := getProtoReader(mockReader)
//
//	command := "SELECT @@version_comment, @@version"
//	rs, err := query(writer, reader, command)
//
//	if err != nil {
//		t.Fatal(
//			"Query got error",
//			err,
//		)
//	}
//
//	if len(rs.columns) != 2 {
//		t.Fatal(
//			"Incorrect columns count",
//			"expected", 1,
//			"got", len(rs.columns),
//		)
//	}
//	type (
//		checkPair struct {
//			columnName    string
//			expectedData  string
//			columnPointer *[]byte
//		}
//	)
//	checkDataMap := []*checkPair{
//		&checkPair{"catalog", "def", &(rs.columns[0].catalog)},
//		&checkPair{"schema", "", &(rs.columns[0].schema)},
//		&checkPair{"table", "", &(rs.columns[0].table)},
//		&checkPair{"org_table", "", &(rs.columns[0].org_table)},
//		&checkPair{"name", "@@version_comment", &(rs.columns[0].name)},
//		&checkPair{"org_name", "", &(rs.columns[0].org_name)},
//		&checkPair{"catalog", "def", &(rs.columns[1].catalog)},
//		&checkPair{"schema", "", &(rs.columns[1].schema)},
//		&checkPair{"table", "", &(rs.columns[1].table)},
//		&checkPair{"org_table", "", &(rs.columns[1].org_table)},
//		&checkPair{"name", "@@version", &(rs.columns[1].name)},
//		&checkPair{"org_name", "", &(rs.columns[1].org_name)},
//	}
//
//	for _, pair := range checkDataMap {
//		if string(*(pair.columnPointer)) != pair.expectedData {
//			t.Fatal(
//				"Incorrect column info", pair.columnName,
//				"expected", pair.expectedData,
//				"got", string(*pair.columnPointer),
//			)
//		}
//	}
//	stat := rs.nextRow()
//
//	if stat != nil {
//		t.Fatal(
//			"Got incorrect error", stat,
//		)
//	}
//
//	expectedString := "(Ubuntu)"
//	str, _, err := rs.buff.readLenString()
//
//	if err != nil {
//		t.Fatal(
//			"Got incorrect error read string", stat,
//		)
//	}
//
//	if string(str) != expectedString {
//		t.Fatal(
//			"Got incorrect data",
//			"expected", expectedString,
//			"got", string(str),
//		)
//	}
//
//	expectedString = "5.5.38-0ubuntu0.14.04.1-log"
//	str, _, err = rs.buff.readLenString()
//
//	if err != nil {
//		t.Fatal(
//			"Got incorrect error read string", stat,
//		)
//	}
//
//	if string(str) != expectedString {
//		t.Fatal(
//			"Got incorrect data",
//			"expected", expectedString,
//			"got", string(str),
//		)
//	}
//
//	stat = rs.nextRow()
//
//	if stat != EOF_ERR {
//		t.Fatal(
//			"Got incorrect next row", stat,
//		)
//	}
//}
