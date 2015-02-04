package myreplication

import "crypto/sha1"

//copied from from github.com/ziutek/mymysql/native/passwd.go
func encryptedPasswd(password string, scramble []byte) (out []byte) {
	if len(password) == 0 {
		return
	}
	// stage1_hash = SHA1(password)
	// SHA1 encode
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stg1Hash := crypt.Sum(nil)
	// token = SHA1(SHA1(stage1_hash), scramble) XOR stage1_hash
	// SHA1 encode again
	crypt.Reset()
	crypt.Write(stg1Hash)
	stg2Hash := crypt.Sum(nil)
	// SHA1 2nd hash and scramble
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(stg2Hash)
	stg3Hash := crypt.Sum(nil)
	// XOR with first hash
	out = make([]byte, len(scramble))
	for ii := range scramble {
		out[ii] = stg3Hash[ii] ^ stg1Hash[ii]
	}
	return
}
