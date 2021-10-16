package protocol

import (
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
