package protocol

import "net"

type IProtocol interface {
    Read() (CommandDecode, error)
    Write(CommandEncode) error
    ReadCommand() (CommandDecode, error)
    WriteCommand(CommandEncode) error
    Close() error
    RemoteAddr() net.Addr
}