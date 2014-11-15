package mysql_replication_listener

import (
	"testing"
	//	"bufio"
	"reflect"
)

func TestHandshakeRead(t *testing.T) {
	mockHandshake := []byte{
		//handshake version 10
		0x0a,
		//mysql plain text version
		0x35, 0x2e, 0x35, 0x2e, 0x33, 0x38, 0x2d, 0x30, 0x75, 0x62, 0x75, 0x6e, 0x74, 0x75, 0x30, 0x2e, 0x31,
		0x34, 0x2e, 0x30, 0x34, 0x2e, 0x31, 0x2d, 0x6c, 0x6f, 0x67, 0x00,
		//connection id
		0x05, 0x00, 0x00, 0x00,
		//auth-plugin-data-part-1 = ROw,ng;0
		0x52, 0x4f, 0x77, 0x2c, 0x6e, 0x67, 0x3b, 0x30,
		//filler
		0x00,
		//capability flags (lower 2 bytes)
		0xff, 0xf7,
		//charset
		0x08,
		//status flag
		0x02, 0x00,
		//capability flags (upper 2 bytes)
		0x0f, 0x80,
		//auth data length = 21
		0x15,
		//reserved 10 bytes
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		//auth-plugin-data-part-2 = }F&):(W`Z%Gv
		0x7d, 0x46, 0x26, 0x29, 0x3a, 0x28, 0x57, 0x60, 0x5a, 0x25, 0x47, 0x76, 0x00,
		//auth-plugin name = mysql_native_password
		0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73,
		0x77, 0x6f, 0x72, 0x64, 0x00,
	}
	reader := getProtoReader(mockHandshake)
	handshake := newHandshake()
	err := handshake.readServer(reader, uint32(len(mockHandshake)))

	if err != nil {
		t.Fatal("Handshake read fail", err)
	}

	if handshake.protocol_version != 10 {
		t.Fatal("Mysql protocol is not 10", err)
	}

	serverVersion := []byte("5.5.38-0ubuntu0.14.04.1-log") //27 length

	if !reflect.DeepEqual(handshake.server_version, serverVersion) {
		t.Fatal("Mysql version is not", string(serverVersion), "got", string(handshake.server_version))
	}

	var expectedConnectionId uint32 = 5

	if handshake.connection_id != expectedConnectionId {
		t.Fatal("Connection id is incorrect", "expected", expectedConnectionId, "got", handshake.connection_id)
	}

	var statusFlagExpected uint16 = 2

	if handshake.status_flags != statusFlagExpected {
		t.Fatal("Incorrect status flag", "expected", statusFlagExpected, "got", handshake.status_flags)
	}

	expectedAuthData := []byte("ROw,ng;0}F&):(W`Z%Gv")
	expectedAuthData = append(expectedAuthData, byte(0))

	if !reflect.DeepEqual(handshake.auth_plugin_data, expectedAuthData) {
		t.Fatal("Incorrect auth plugin data", "expected", string(expectedAuthData), "got", string(handshake.auth_plugin_data))
	}

	//CLIENT_PLUGIN_AUTH = 0
	//	expectedAuthPluginName := []byte("mysql_native_password")
	//
	//	if !reflect.DeepEqual(handshake.auth_plugin_name, expectedAuthPluginName) {
	//		t.Fatal("Incorrect auth plugin name", "expected", string(expectedAuthPluginName), "got", string(handshake.auth_plugin_name))
	//	}
}
