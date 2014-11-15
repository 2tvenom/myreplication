package mysql_replication_listener

import (
	"testing"
)

var (
	host     = "localhost"
	port     = 3306
	username = "repl"
	password = "slavepass"
)

func TestConnectionAndAutentificate(t *testing.T) {
	//	newConnection := newConnection()
	//	err := newConnection.connectAndAuth(host, port, username, password)
	//
	//	if err != nil {
	//		t.Fatal("Client not connected and not autentificate to master server with error", err)
	//	}

}
