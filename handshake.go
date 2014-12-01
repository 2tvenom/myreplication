package mysql_replication_listener

/*
	http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
*/

import ()

type (
	pkgHandshake struct {
		protocol_version byte
		server_version   []byte
		connection_id    uint32
		capabilities     uint32
		character_set    byte
		status_flags     uint16
		auth_plugin_data []byte
		auth_plugin_name []byte
	}
)

func (h *pkgHandshake) readServer(r *pack) (err error) {
	r.readByte(&h.protocol_version)
	if h.protocol_version != _HANDSHAKE_VERSION_10 {
		panic("Support only HandshakeV10")
	}

	h.server_version, err = r.readNilString()

	if err != nil {
		return
	}

	r.readUint32(&h.connection_id)

	h.auth_plugin_data = make([]byte, 8)
	_, err = r.Buffer.Read(h.auth_plugin_data)

	if err != nil {
		return
	}

	//skip one
	r.Buffer.ReadByte()

	var capOne uint16
	r.readUint16(&capOne)

	h.capabilities = uint32(capOne)

	h.character_set, _ = r.Buffer.ReadByte()

	r.readUint16(&h.status_flags)

	var capSecond uint16
	r.readUint16(&capSecond)

	h.capabilities = h.capabilities | (uint32(capSecond) << 8)

	lengthAuthPluginData, _ := r.Buffer.ReadByte()

	filler := make([]byte, 10)
	r.Buffer.Read(filler)

	if h.capabilities&_CLIENT_SECURE_CONNECTION == _CLIENT_SECURE_CONNECTION {
		if lengthAuthPluginData > 0 && (13 < lengthAuthPluginData-8) {
			lengthAuthPluginData -= 8
		} else {
			lengthAuthPluginData = 13
		}

		auth_plugin_data_2 := make([]byte, lengthAuthPluginData-1)
		_, err = r.Buffer.Read(auth_plugin_data_2)

		if err != nil {
			return err
		}

		h.auth_plugin_data = append(h.auth_plugin_data, auth_plugin_data_2...)
	}

	if h.capabilities&_CLIENT_PLUGIN_AUTH == _CLIENT_PLUGIN_AUTH {
		h.auth_plugin_name, err = r.readNilString()
	}

	return
}

func (h *pkgHandshake) writeServer(username, password string) *pack {
	var encPasswd []byte = []byte{}

	if h.capabilities&_CLIENT_SECURE_CONNECTION == _CLIENT_SECURE_CONNECTION {
		encPasswd = encryptedPasswd(password, h.auth_plugin_data)
	}

	pack := newPack()
	pack.writeUInt32(_CLIENT_ALL_FLAGS)
	pack.writeUInt32(_MAX_PACK_SIZE)
	pack.WriteByte(h.character_set)
	pack.Write(make([]byte, 23, 23))
	pack.writeStringNil(username)

	if h.capabilities&_CLIENT_SECURE_CONNECTION == _CLIENT_SECURE_CONNECTION {
		pack.WriteByte(byte(len(encPasswd)))
		pack.Write(encPasswd)
	}

	return pack
}
