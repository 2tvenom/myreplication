package mysql_replication_listener

/*
	http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
*/

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

func newHandshake() *pkgHandshake {
	return &pkgHandshake{}
}

func (h *pkgHandshake) readServer(r *protoReader, length uint32) (err error) {
	h.protocol_version, err = r.ReadByte()
	if h.protocol_version != _HANDSHAKE_VERSION_10 {
		panic("Support only HandshakeV10")
	}
	length--
	if err != nil {
		return
	}

	h.server_version, err = r.readNilString()
	length -= uint32(len(h.server_version))
	if err != nil {
		return
	}

	h.connection_id, err = r.readUint32()
	length -= 4
	if err != nil {
		return
	}

	h.auth_plugin_data = make([]byte, 8)
	_, err = r.Reader.Read(h.auth_plugin_data)

	if err != nil {
		return
	}

	length -= 8

	//skip one
	r.Reader.ReadByte()
	length -= 1

	capOne, err := r.readUint16()
	if err != nil {
		return
	}

	h.capabilities = uint32(capOne)
	length -= 2

	if length == 0 {
		return
	}

	h.character_set, err = r.Reader.ReadByte()

	if err != nil {
		return
	}

	h.status_flags, err = r.readUint16()

	if err != nil {
		return
	}

	capSecond, err := r.readUint16()

	if err != nil {
		return
	}
	h.capabilities = h.capabilities | (uint32(capSecond) << 8)
	length -= 2

	lengthAuthPluginData, err := r.Reader.ReadByte()
	length--
	if err != nil {
		return
	}

	filler := make([]byte, 10)
	_, err = r.Reader.Read(filler)
	length -= 10
	filler = nil
	if err != nil {
		return
	}

	if h.capabilities&_CLIENT_SECURE_CONNECTION == _CLIENT_SECURE_CONNECTION {
		if lengthAuthPluginData > 0 && (13 < lengthAuthPluginData-8) {
			lengthAuthPluginData -= 8
		} else {
			lengthAuthPluginData = 13
		}

		auth_plugin_data_2 := make([]byte, lengthAuthPluginData)
		_, err = r.Reader.Read(auth_plugin_data_2)

		if err != nil {
			return err
		}

		h.auth_plugin_data = append(h.auth_plugin_data, auth_plugin_data_2...)

		length -= uint32(lengthAuthPluginData)
	}

	if h.capabilities&_CLIENT_PLUGIN_AUTH == _CLIENT_PLUGIN_AUTH {
		h.auth_plugin_name, err = r.readNilString()
		println("--")
		if err != nil {
			return err
		}
		length -= uint32(len(h.auth_plugin_name))
	}

	if length < 0 {
		panic("Incorrect length")
	}

	if length == 0 {
		return
	}

	devNullBuff := make([]byte, length)
	r.Reader.Read(devNullBuff)
	devNullBuff = nil
	return
}

func (h *pkgHandshake) writeServer(r *protoWriter, username, passsword string) (err error) {
	r.writeUInt32(_CLIENT_ALL_FLAGS)
	r.writeUInt32(_MAX_PACK_SIZE)
	r.Writer.WriteByte(h.character_set)
	r.Writer.Write(make([]byte, 23, 23))
	r.writeStringNil(username)
	if h.capabilities&_CLIENT_SECURE_CONNECTION == _CLIENT_SECURE_CONNECTION {
		encPasswd := encryptedPasswd(passsword, h.auth_plugin_data)
		r.Writer.WriteByte(byte(len(encPasswd)))
		r.Writer.Write(encPasswd)
	}
	return
}
