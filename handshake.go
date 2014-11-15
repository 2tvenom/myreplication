package mysql_replication_listener

import ()

/*
protocol_version (1) -- 0x0a protocol_version
server_version (string.NUL) -- human-readable server version
connection_id (4) -- connection id
auth_plugin_data_part_1 (string.fix_len) -- [len=8] first 8 bytes of the auth-plugin data
filler_1 (1) -- 0x00
capability_flag_1 (2) -- lower 2 bytes of the Protocol::CapabilityFlags (optional)
character_set (1) -- default server character-set, only the lower 8-bits Protocol::CharacterSet (optional)
status_flags (2) -- Protocol::StatusFlags (optional)
capability_flags_2 (2) -- upper 2 bytes of the Protocol::CapabilityFlags
auth_plugin_data_len (1) -- length of the combined auth_plugin_data, if auth_plugin_data_len is > 0
auth_plugin_name (string.NUL) -- name of the auth_method that the auth_plugin_data belongs to
*/

type (
	pkgHandshake struct {
		protocol_version byte
		server_version   string
		connection_id    uint32
		capability_flag  uint32
		character_set    byte
		status_flags     uint16
		auth_plugin_data_1 []byte
		auth_plugin_data_2 []byte
	}
)

func newHandshake() *pkgHandshake {
	return &pkgHandshake{}
}

func (h *pkgHandshake) readServer(r *protoReader, length uint32) (err error) {
	h.protocol_version, err = r.ReadByte()
	length--
	if err != nil {
		return
	}

	h.server_version, err = r.ReadNilString()
	length -= uint32(len(h.server_version))
	if err != nil {
		return
	}

	h.connection_id, err = r.ReadUint32()
	length -= 4
	if err != nil {
		return
	}

	h.auth_plugin_data_1 = make([]byte, 8)
	_, err = r.Reader.Read(h.auth_plugin_data_1)

	if err != nil {
		return
	}

	length -= 8

	//skip one
	r.Reader.ReadByte()
	length -= 1

	capOne, err := r.ReadUint16()
	if err != nil {
		return
	}

	h.capability_flag = uint32(capOne)
	length -= 2

	if length == 0 {
		return
	}

	h.character_set, err = r.Reader.ReadByte()

	if err != nil {
		return
	}

	h.status_flags, err = r.ReadUint16()

	if err != nil {
		return
	}

	capSecond, err := r.ReadUint16()

	if err != nil {
		return
	}

	h.capability_flag = h.capability_flag & (uint32(capSecond) << 2)
	length -= 2

	_, err = r.Reader.ReadByte()
	length--

	if err != nil {
		return
	}

	if h.capability_flag & _CLIENT_SECURE_CONNECTION != _CLIENT_SECURE_CONNECTION {
		h.auth_plugin_data_2 = make([]byte, 13)
		_, err = r.Reader.Read(h.auth_plugin_data_2)

		if err != nil {
			return err
		}

		length -= 13
	}

	if length < 0 {
		panic("Incorrect length")
	}

	devNullBuff := make([]byte, length)
	r.Reader.Read(devNullBuff)
	devNullBuff = nil
	return
}

func (h *pkgHandshake) writeServer(r *protoWriter, username, passsword string) (err error) {

	r.WriteUInt32(_CLIENT_ALL_FLAGS)

	return
}
