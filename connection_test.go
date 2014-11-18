package mysql_replication_listener

import (
	"testing"
)

var (
	host     = "localhost"
	port     = 3306
	username = "root"
	password = ""
)

func TestConnectionAndAuth(t *testing.T) {
	newConnection := newConnection()
	err := newConnection.connectAndAuth(host, port, username, password, uint32(2))

	if err != nil {
		t.Fatal("Client not connected and not autentificate to master server with error", err)
	}
	pos, filename, err := newConnection.getMasterStatus()

	if err != nil {
		t.Fatal("Master status fail", err)
	}

	println(filename)
	println(pos)

	//	rs, err := newConnection.query("SELECT @@version_comment, @@version")
	//
	//	if err != nil {
	//		t.Fatal("Query error", err)
	//	}

}
