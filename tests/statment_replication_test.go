package tests

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"mysql_replication_listener"
	"os"
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

func TestStatementReplication(t *testing.T) {
	newConnection := mysql_replication_listener.NewConnection()
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
			t.Fatal(err)
		}

		r, err := con.Query("SELECT max(id) FROM new_table")
		var maxId uint64

		if r.Next() {
			r.Scan(&maxId)
		}

		con.Exec("INSERT INTO new_table(text_field, num_field) values(?,?)", "Hello!", 10)

		expectedQuery := "BEGIN"

		if expectedQuery != (<-events).(*mysql_replication_listener.QueryEvent).GetQuery() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect query")
		}

		if (<-events).(*mysql_replication_listener.IntVarEvent).GetValue() != (maxId + 1) {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect IntEvent")
		}

		expectedQuery = "INSERT INTO new_table(text_field, num_field) values('Hello!',10)"

		if expectedQuery != (<-events).(*mysql_replication_listener.QueryEvent).GetQuery() {
			newConnection.Connection().Close()
			t.Fatal("Got incorrect query")
		}

		os.Exit(0)
	}()

	err = el.Start()

	if err != nil {
		t.Fatal("Start error", err)
	}
}
