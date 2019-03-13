package protocol

import "net"

type Protocol interface {
    Read() (CommandDecode, error)
    Write(CommandEncode) (error)
    Close() (error)
    RemoteAddr() net.Addr
}