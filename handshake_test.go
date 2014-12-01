package mysql_replication_listener

import (
	"bytes"
	"reflect"
	"testing"
)

func TestHandshakeRead(t *testing.T) {
	mockHandshake := []byte{
		//length
		0xF5, 0x00, 0x00,
		//sequence id
		0x00,
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

	packReader := newPackReader(bytes.NewBuffer(mockHandshake))
	pack, _ := packReader.readNextPack()

	handshake := &pkgHandshake{}
	err := handshake.readServer(pack)

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

func TestHandshakeWrite(t *testing.T) {
	username := "test"
	password := "test"

	handshake := &pkgHandshake{}
	handshake.auth_plugin_data = []byte("test")
	handshake.character_set = 2
	handshake.capabilities = _CLIENT_SECURE_CONNECTION

	pack := handshake.writeServer(username, password)
	pack.setSequence(byte(5))
	result := pack.packBytes()

	//Capability test
	expectedCapability := []byte{0xD7, 0xF7, 0x03, 0x00}

	offset := 4

	if !reflect.DeepEqual(expectedCapability, result[offset:offset+4]) {
		t.Fatal("Handshake write capability flags",
			"expected", expectedCapability,
			"got", result[0:4],
		)
	}

	offset += 4

	//max pack size
	expectedMaxPackSize := []byte{0xFF, 0xFF, 0xFF, 0x00}

	if !reflect.DeepEqual(expectedMaxPackSize, result[offset:offset+4]) {
		t.Fatal("Handshake write max pack size",
			"expected", expectedMaxPackSize,
			"got", result[4:8],
		)
	}

	offset += 4

	//charset
	if handshake.character_set != result[offset : offset+1][0] {
		t.Fatal("Handshake write charset",
			"expected", handshake.character_set,
			"got", result[8:9],
		)
	}

	offset += 1

	//filler
	if !reflect.DeepEqual(make([]byte, 23, 23), result[offset:offset+23]) {
		t.Fatal("Handshake filler aftert username",
			"expected 23 zero byte arrys",
			"got", result[9:32],
		)
	}

	offset += 23

	//username
	if !reflect.DeepEqual([]byte(username), result[offset:offset+4]) {
		t.Fatal("Handshake write username",
			"expected", username,
			"got", string(result[9:13]),
		)
	}

	offset += 4

	//username null byte
	if byte(0) != result[offset : offset+1][0] {
		t.Fatal("Handshake not write username zero byte")
	}

	offset += 1

	pass := encryptedPasswd(password, handshake.auth_plugin_data)

	//pass length
	if byte(len(pass)) != result[offset : offset+1][0] {
		t.Fatal("Handshake password length incorrect",
			"expected", len(pass),
			"got", result[offset : offset+1][0],
		)
	}

	offset += 1

	//password
	if !reflect.DeepEqual(pass, result[offset:offset+len(pass)]) {
		t.Fatal("Handshake incorrect password",
			"expected", pass,
			"got", result[offset:offset+len(pass)],
		)
	}

	offset += len(pass)

	//check header
	//length

	expectedLength := []byte{byte(offset - 4), 0, 0}

	if !reflect.DeepEqual(expectedLength, result[0:3]) {
		t.Fatal("Handshake length packet incorrect",
			"expected", expectedLength,
			"got", result[0:3],
		)
	}

	//sequence id
	if result[3:4][0] != pack.getSequence() {
		t.Fatal("Handshake incorrect sequence id",
			"expected", pack.getSequence(),
			"got", result[3:4][0],
		)
	}
}
