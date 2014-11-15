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
	newConnection := newConnection()
	err := newConnection.connectAndAuth(host, port, username, password)

	if err != nil {
		t.Fatal("Client not connected and not autentificate to master server with error", err)
	}

	if newConnection.header.length != 95 {
		t.Fatal("Init header not have 95 lenghs", err)
	}

	if newConnection.handshake.protocol_version != 10 {
		t.Fatal("Mysql protocol is not 10", err)
	}

	serverVersion := "5.5.38-0ubuntu0.14.04.1-log"

	if newConnection.handshake.server_version != serverVersion {
		t.Fatal("Mysql version is not", serverVersion, "got", newConnection.handshake.server_version)
	}

	var statusFlagExpected uint16 = 2

	if newConnection.handshake.status_flags != statusFlagExpected {
		t.Fatal("Incorrect status flag", statusFlagExpected, "got", newConnection.handshake.status_flags)
	}
}
