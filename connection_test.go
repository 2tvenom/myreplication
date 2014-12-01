package mysql_replication_listener


import (
	"testing"
	"fmt"
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

	err = newConnection.startBinlogDump(pos, filename, uint32(2))

	if err != nil {
		t.Fatal("Cant start bin log", err)
	}

	p, _ := newConnection.packReader.readNextPack()
	fmt.Printf("% x\n", p.buff)
	p, _ = newConnection.packReader.readNextPack()
	fmt.Printf("% x\n", p.buff)


//	parser := newEventLogParser(newConnection.reader)
//	for i := 0; i < 5; i++ {
//		event, _ := parser.read()
//
//		switch e := event.(type) {
//		case *eventLogRotateEvent:
//			println("rotate")
//		case *eventLogFormatDescriptionEvent:
//			println("description")
//		case *eventLogFormatUpdateEventV2:
//			println(e.columnsCount)
//			println(e.tableId)
//		case *eventLogQueryEvent:
//			println(string(e.query))
//		}
//	}

	//	rs, err := newConnection.query("SELECT @@version_comment, @@version")
	//
	//	if err != nil {
	//		t.Fatal("Query error", err)
	//	}

}
