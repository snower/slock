package protocol

import (
	"crypto/md5"
	"encoding/hex"
	"math/rand"
	"net"
	"time"
)

var requestIdIndex uint32 = 0
var lockIdIndex uint32 = 0
var clientIdIndex uint32 = 0

type IProtocol interface {
	Read() (CommandDecode, error)
	Write(CommandEncode) error
	ReadCommand() (CommandDecode, error)
	WriteCommand(CommandEncode) error
	Close() error
	RemoteAddr() net.Addr
}

func GenRequestId() [16]byte {
	now := uint64(time.Now().UnixNano() / 1e6)
	rn := rand.Int63n(0xffffffffffff)
	lid := requestIdIndex
	requestIdIndex++
	return [16]byte{
		byte(now >> 40), byte(now >> 32), byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now),
		byte(rn >> 40), byte(rn >> 32), byte(rn >> 24), byte(rn >> 16), byte(rn >> 8), byte(rn),
		byte(lid >> 24), byte(lid >> 16), byte(lid >> 8), byte(lid),
	}
}

func GenLockId() [16]byte {
	now := uint64(time.Now().UnixNano() / 1e6)
	rn := rand.Int63n(0xffffffffffff)
	lid := lockIdIndex
	lockIdIndex++
	return [16]byte{
		byte(now >> 40), byte(now >> 32), byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now),
		byte(rn >> 40), byte(rn >> 32), byte(rn >> 24), byte(rn >> 16), byte(rn >> 8), byte(rn),
		byte(lid >> 24), byte(lid >> 16), byte(lid >> 8), byte(lid),
	}
}

func GenClientId() [16]byte {
	now := uint64(time.Now().UnixNano() / 1e6)
	rn := rand.Int63n(0xffffffffffff)
	lid := clientIdIndex
	clientIdIndex++
	return [16]byte{
		byte(now >> 40), byte(now >> 32), byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now),
		byte(rn >> 40), byte(rn >> 32), byte(rn >> 24), byte(rn >> 16), byte(rn >> 8), byte(rn),
		byte(lid >> 24), byte(lid >> 16), byte(lid >> 8), byte(lid),
	}
}

func ConvertString2LockKey(key string) [16]byte {
	keyLen := len(key)
	if keyLen == 16 {
		return [16]byte{byte(key[0]), byte(key[1]), byte(key[2]), byte(key[3]), byte(key[4]), byte(key[5]), byte(key[6]), byte(key[7]), byte(key[8]), byte(key[9]), byte(key[10]), byte(key[11]), byte(key[12]), byte(key[13]), byte(key[14]), byte(key[15])}
	} else if keyLen > 16 {
		if keyLen == 32 {
			v, err := hex.DecodeString(key)
			if err == nil {
				return [16]byte{v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]}
			} else {
				v := md5.Sum([]byte(key))
				return [16]byte{v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]}
			}
		} else {
			v := md5.Sum([]byte(key))
			return [16]byte{v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]}
		}
	} else {
		lockKey, keyIndex := [16]byte{}, 16-keyLen
		for i := 0; i < 16; i++ {
			if i < keyIndex {
				lockKey[i] = 0
			} else {
				lockKey[i] = key[i-keyIndex]
			}
		}
		return lockKey
	}
}
