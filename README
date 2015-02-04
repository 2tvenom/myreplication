# Go MySql binary log replication listener

Pure Go Implementation of MySQL replication protocol. This allow you to receive event like insert, update, delete with their datas and raw SQL queries. This code has been developed and maintained by Ven at January 2015.

## Installation

```bash
go get github.com/2tvenom/myreplication
```

## Test

The project is test with:

 - Go 1.3.3 
 - MySQL 5.5, 5.6 and 5.7 (beta)
 - Docker 1.4.1 build 5bc2ff8 (functional tests)

It's not tested in real production situation.

### Unit tests
```bash
go test
```

### Docker tests

Functonal tests with Docker. Test statement based and row based replication. MySql versions 5.5, 5.6, 5.7. 

```bash
cd tests
sudo ./test.sh
```

## MySQL server settings

In your MySQL server configuration file you need to enable replication:

    [mysqld]
    server-id		 = 1
    log_bin			 = /var/log/mysql/mysql-bin.log
    expire_logs_days = 10
    max_binlog_size  = 100M
    binlog-format    = row #Row based replication

## Example
```go
package main

import (
	"fmt"
	"myreplication"
)

var (
	host     = "localhost"
	port     = 3307
	username = "admin"
	password = "admin"
)

func main() {
	newConnection := myreplication.NewConnection()
	serverId := uint32(2)
	err := newConnection.ConnectAndAuth(host, port, username, password)

	if err != nil {
		panic("Client not connected and not autentificate to master server with error:" + err.Error())
	}
	//Get position and file name
	pos, filename, err := newConnection.GetMasterStatus()

	if err != nil {
		panic("Master status fail: " + err.Error())
	}

	el, err := newConnection.StartBinlogDump(pos, filename, serverId)

	if err != nil {
		panic("Cant start bin log: " + err.Error())
	}
	events := el.GetEventChan()
	go func() {
		for {
			event := <-events

			switch e := event.(type) {
			case *myreplication.QueryEvent:
				//Output query event
				println(e.GetQuery())
			case *myreplication.IntVarEvent:
				//Output last insert_id  if statement based replication
				println(e.GetValue())
			case *myreplication.WriteEvent:
				//Output Write (insert) event
				println("Write", e.GetTable())
				//Rows loop
				for i, row := range e.GetRows() {
					//Columns loop
					for j, col := range row {
						//Output row number, column number, column type and column value
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			case *myreplication.DeleteEvent:
				//Output delete event
				println("Delete", e.GetTable())
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			case *myreplication.UpdateEvent:
				//Output update event
				println("Update", e.GetTable())
				//Output old data before update
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
				//Output new
				for i, row := range e.GetNewRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			default:
			}
		}
	}()
	err = el.Start()
	println(err.Error())
}

```
## Links
 - MySql documentation http://dev.mysql.com/doc/internals/en/client-server-protocol.html 
 - Python implementation. MySql 5.6 checksum compatibility https://github.com/noplay/python-mysql-replication
 - Go MySql client. Password encryption method https://github.com/ziutek/mymysql
 - Java implementation. Decimal type parsing https://github.com/whitesock/open-replicator 
 - Ruby implementation. Decimal type parsing https://github.com/jeremycole/mysql_binlog
 - Docker files https://github.com/docker-library/mysql
 

## Licence
[WTFPL](http://www.wtfpl.net/)