package tests

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"myreplication"
	"os"
	"reflect"
	"testing"
)

const (
	REPLICATION_USERNAME = "admin"
	REPLICATION_PASSWORD = "admin"
	ROOT_USERNAME        = "root"
	ROOT_PASSWORD        = "admin"
	DATABASE             = "test"
	HOST                 = "localhost"
	PORT                 = 3307
)

type (
	columnTest struct {
		columnId    int
		columnType  byte
		columnValue interface{}
		isNil       bool
	}
)

func TestRowReplication(t *testing.T) {
	newConnection := myreplication.NewConnection()
	serverId := uint32(2)
	err := newConnection.ConnectAndAuth(HOST, PORT, REPLICATION_USERNAME, REPLICATION_PASSWORD)

	if err != nil {
		t.Fatal("Client not connected and not autentificate to master server with error:", err.Error())
	}
	pos, filename, err := newConnection.GetMasterStatus()

	if err != nil {
		t.Fatal("Master status fail: ", err.Error())
	}

	el, err := newConnection.StartBinlogDump(pos, filename, serverId)

	if err != nil {
		t.Fatal("Cant start bin log: ", err.Error())
	}
	events := el.GetEventChan()

	go func() {
		con, err := sql.Open("mysql", fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s",
			REPLICATION_USERNAME,
			REPLICATION_PASSWORD,
			HOST,
			PORT,
			DATABASE,
		))
		defer con.Close()
		if err != nil {
			t.Fatal(err.Error())
		}

		_, err = con.Exec("TRUNCATE new_table")
		if err != nil {
			t.Fatal(err.Error())
		}
		query := (<-events).(*myreplication.QueryEvent).GetQuery()
		expectedQuery := "TRUNCATE new_table"

		if expectedQuery != query {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect query", "expected", expectedQuery, "got", query)
		}

		maxId := 1
		expectedTable := "new_table"
		expectedSchema := "test"

		t.Log("Write test")

		con.Exec("INSERT INTO new_table(text_field, num_field) values(?,?)", "Hello!", 10)

		expectedQuery = "BEGIN"

		query = (<-events).(*myreplication.QueryEvent).GetQuery()

		if expectedQuery != query {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect query", "expected", expectedQuery, "got", query)
		}

		writeQuery := (<-events).(*myreplication.WriteEvent)

		if expectedTable != writeQuery.GetTable() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect table name", "expected", expectedTable, "got", writeQuery.GetTable())
		}

		if expectedSchema != writeQuery.GetSchema() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect schema", "expected", expectedSchema, "got", writeQuery.GetTable())
		}

		rows := writeQuery.GetRows()

		expectedRowsCount := 1
		if expectedRowsCount != len(rows) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect rows count", "expected", expectedRowsCount, "got", len(rows))
		}

		columns := rows[0]
		expectedColumnsCount := 3

		if expectedColumnsCount != len(columns) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect columns count", "expected", expectedColumnsCount, "got", len(columns))
		}

		tests := []*columnTest{
			&columnTest{0, myreplication.MYSQL_TYPE_LONG, uint32(maxId), false},
			&columnTest{1, myreplication.MYSQL_TYPE_VARCHAR, "Hello!", false},
			&columnTest{2, myreplication.MYSQL_TYPE_LONG, uint32(10), false},
		}

		for i, column := range columns {
			if tests[i].columnId != column.GetColumnId() {
				newConnection.Connection().Close()
				t.Fatal(
					"Write event. Got incorrect column id at column",
					i, "expected", tests[i].columnId, "got", column.GetColumnId(),
				)
			}

			if tests[i].columnType != column.GetType() {
				newConnection.Connection().Close()
				t.Fatal(
					"Write event. Got incorrect column type at column",
					i, "expected", tests[i].columnType, "got", column.GetType(),
				)
			}

			if tests[i].isNil != column.IsNil() {
				newConnection.Connection().Close()
				t.Fatal(
					"Write event. Got column nil value incorrect",
					i, "expected", tests[i].isNil, "got", column.IsNil(),
				)
			}

			if column.IsNil() {
				continue
			}

			if !reflect.DeepEqual(tests[i].columnValue, column.GetValue()) {
				newConnection.Connection().Close()
				t.Fatal(
					"Write event. Got incorrect column value at column",
					i, "expected", tests[i].columnValue, "got", column.GetValue(),
				)
			}
		}

		t.Log("Update test")
		_, err = con.Exec("UPDATE new_table SET text_field = ? WHERE id = ?", "World!", maxId)

		if err != nil {
			newConnection.Connection().Close()
			t.Fatal(err.Error())
		}

		expectedQuery = "BEGIN"

		query = (<-events).(*myreplication.QueryEvent).GetQuery()

		if expectedQuery != query {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect query", "expected", expectedQuery, "got", query)
		}

		updateEvent := (<-events).(*myreplication.UpdateEvent)

		if expectedTable != updateEvent.GetTable() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect table name", "expected", expectedTable, "got", writeQuery.GetTable())
		}

		if expectedSchema != updateEvent.GetSchema() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect schema", "expected", expectedSchema, "got", writeQuery.GetTable())
		}

		t.Log("Update test: check old row version")

		rows = updateEvent.GetRows()
		expectedRowsCount = 1
		if expectedRowsCount != len(rows) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect rows count", "expected", expectedRowsCount, "got", len(rows))
		}

		columns = rows[0]
		expectedColumnsCount = 3

		if expectedColumnsCount != len(columns) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect columns count", "expected", expectedColumnsCount, "got", len(columns))
		}

		for i, column := range columns {
			if tests[i].columnId != column.GetColumnId() {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got incorrect column id at column",
					i, "expected", tests[i].columnId, "got", column.GetColumnId(),
				)
			}

			if tests[i].columnType != column.GetType() {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got incorrect column type at column",
					i, "expected", tests[i].columnType, "got", column.GetType(),
				)
			}

			if tests[i].isNil != column.IsNil() {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got column nil value incorrect",
					i, "expected", tests[i].isNil, "got", column.IsNil(),
				)
			}

			if column.IsNil() {
				continue
			}

			if !reflect.DeepEqual(tests[i].columnValue, column.GetValue()) {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got incorrect column value at column",
					i, "expected", tests[i].columnValue, "got", column.GetValue(),
				)
			}
		}

		t.Log("Update test: check new row version")

		rows = updateEvent.GetNewRows()
		expectedRowsCount = 1
		if expectedRowsCount != len(rows) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect rows count", "expected", expectedRowsCount, "got", len(rows))
		}

		columns = rows[0]
		expectedColumnsCount = 3

		if expectedColumnsCount != len(columns) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect columns count", "expected", expectedColumnsCount, "got", len(columns))
		}

		tests = []*columnTest{
			&columnTest{0, myreplication.MYSQL_TYPE_LONG, uint32(maxId), false},
			&columnTest{1, myreplication.MYSQL_TYPE_VARCHAR, "World!", false},
			&columnTest{2, myreplication.MYSQL_TYPE_LONG, uint32(10), false},
		}

		for i, column := range columns {
			if tests[i].columnId != column.GetColumnId() {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got incorrect column id at column",
					i, "expected", tests[i].columnId, "got", column.GetColumnId(),
				)
			}

			if tests[i].columnType != column.GetType() {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got incorrect column type at column",
					i, "expected", tests[i].columnType, "got", column.GetType(),
				)
			}

			if tests[i].isNil != column.IsNil() {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got column nil value incorrect",
					i, "expected", tests[i].isNil, "got", column.IsNil(),
				)
			}

			if column.IsNil() {
				continue
			}

			if !reflect.DeepEqual(tests[i].columnValue, column.GetValue()) {
				newConnection.Connection().Close()
				t.Fatal(
					"Update event. Got incorrect column value at column",
					i, "expected", tests[i].columnValue, "got", column.GetValue(),
				)
			}
		}

		t.Log("Delete test")

		_, err = con.Exec("DELETE FROM new_table WHERE id = ?", maxId)
		if err != nil {
			newConnection.Connection().Close()
			t.Fatal(err.Error())
		}

		expectedQuery = "BEGIN"

		query = (<-events).(*myreplication.QueryEvent).GetQuery()
		if expectedQuery != query {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect query", "expected", expectedQuery, "got", query)
		}

		deleteEvent := (<-events).(*myreplication.DeleteEvent)

		if expectedTable != deleteEvent.GetTable() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect table name", "expected", expectedTable, "got", writeQuery.GetTable())
		}

		if expectedSchema != deleteEvent.GetSchema() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect schema", "expected", expectedSchema, "got", writeQuery.GetTable())
		}

		rows = deleteEvent.GetRows()
		expectedRowsCount = 1
		if expectedRowsCount != len(rows) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect rows count", "expected", expectedRowsCount, "got", len(rows))
		}

		columns = rows[0]
		expectedColumnsCount = 3

		if expectedColumnsCount != len(columns) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect columns count", "expected", expectedColumnsCount, "got", len(columns))
		}

		for i, column := range columns {
			if tests[i].columnId != column.GetColumnId() {
				newConnection.Connection().Close()
				t.Fatal(
					"Delete event. Got incorrect column id at column",
					i, "expected", tests[i].columnId, "got", column.GetColumnId(),
				)
			}

			if tests[i].columnType != column.GetType() {
				newConnection.Connection().Close()
				t.Fatal(
					"Delete event. Got incorrect column type at column",
					i, "expected", tests[i].columnType, "got", column.GetType(),
				)
			}

			if tests[i].isNil != column.IsNil() {
				newConnection.Connection().Close()
				t.Fatal(
					"Delete event. Got column nil value incorrect",
					i, "expected", tests[i].isNil, "got", column.IsNil(),
				)
			}

			if column.IsNil() {
				continue
			}

			if !reflect.DeepEqual(tests[i].columnValue, column.GetValue()) {
				newConnection.Connection().Close()
				t.Fatal(
					"Delete event. Got incorrect column value at column",
					i, "expected", tests[i].columnValue, "got", column.GetValue(),
				)
			}
		}

		os.Exit(0)
	}()

	err = el.Start()

	if err != nil {
		t.Fatal("Start error", err)
	}
}
