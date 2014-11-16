package mysql_replication_listener

const (
	_COM_SLEEP               = 0x00
	_COM_QUIT                = 0x01
	_COM_INIT_DB             = 0x02
	_COM_QUERY               = 0x03
	_COM_FIELD_LIST          = 0x04
	_COM_CREATE_DB           = 0x05
	_COM_DROP_DB             = 0x06
	_COM_REFRESH             = 0x07
	_COM_SHUTDOWN            = 0x08
	_COM_STATISTICS          = 0x09
	_COM_PROCESS_INFO        = 0x0a
	_COM_CONNECT             = 0x0b
	_COM_PROCESS_KILL        = 0x0c
	_COM_DEBUG               = 0x0d
	_COM_PING                = 0x0e
	_COM_TIME                = 0x0f
	_COM_DELAYED_INSERT      = 0x10
	_COM_CHANGE_USER         = 0x11
	_COM_BINLOG_DUMP         = 0x12
	_COM_TABLE_DUMP          = 0x13
	_COM_CONNECT_OUT         = 0x14
	_COM_REGISTER_SLAVE      = 0x15
	_COM_STMT_PREPARE        = 0x16
	_COM_STMT_EXECUTE        = 0x17
	_COM_STMT_SEND_LONG_DATA = 0x18
	_COM_STMT_CLOSE          = 0x19
	_COM_STMT_RESET          = 0x1a
	_COM_SET_OPTION          = 0x1b
	_COM_STMT_FETCH          = 0x1c
	_COM_DAEMON              = 0x1d
	_COM_BINLOG_DUMP_GTID    = 0x1e
	_COM_RESET_CONNECTION    = 0x1f
)
