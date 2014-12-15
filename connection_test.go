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
	//	serverId := uint32(2)
	err := newConnection.ConnectAndAuth(host, port, username, password)

	if err != nil {
		t.Fatal("Client not connected and not autentificate to master server with error", err)
	}
	pos, filename, err := newConnection.getMasterStatus()

	if err != nil {
		t.Fatal("Master status fail", err)
	}

	t.Log("Filename", filename)
	t.Log("Position", pos)

	el, err := newConnection.startBinlogDump(pos, filename, uint32(2))

	if err != nil {
		t.Fatal("Cant start bin log", err)
	}

	println("--", el.binlogVersion, "--")
//		el.start()
}
