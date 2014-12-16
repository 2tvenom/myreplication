package main

import (
	"fmt"
	"mysql_replication_listener"
)

var (
	host     = "localhost"
	port     = 3306
	username = "root"
	password = ""
)

func main() {
	newConnection := mysql_replication_listener.NewConnection()
	serverId := uint32(2)
	err := newConnection.ConnectAndAuth(host, port, username, password)

	if err != nil {
		panic("Client not connected and not autentificate to master server with error:" + err.Error())
	}
	pos, filename, err := newConnection.GetMasterStatus()

	if err != nil {
		panic("Master status fail: " + err.Error())
	}

	el, err := newConnection.StartBinlogDump(pos, filename, serverId)

	if err != nil {
		panic("Cant start bin log: " + err.Error())
	}

	go func () {
		events := el.GetEventChan()
		for {
			event := <- events

			switch e := event.(type) {
			case *mysql_replication_listener.QueryEvent:
				println(e.GetQuery())
			case *mysql_replication_listener.WriteEvent:
				println("Write", e.GetTable())
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			case *mysql_replication_listener.DeleteEvent:
				println("Delete", e.GetTable())
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			case *mysql_replication_listener.UpdateEvent:
				println("Update", e.GetTable())
				for i, row := range e.GetNewRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			default:
			}
		}
	}()
	err 1= el.Start()
	println(err.Error())
}
