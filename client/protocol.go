package client

import (
    "errors"
    "net"
    "github.com/snower/slock/protocol"
)

type ClientProtocol struct {
    stream *Stream
    rbuf []byte
}

func NewClientProtocol(stream *Stream) *ClientProtocol {
    client_protocol := &ClientProtocol{stream, make([]byte, 64)}
    return client_protocol
}

func (self *ClientProtocol) Close() (err error) {
    return self.stream.Close()
}

func (self *ClientProtocol) Read() (command protocol.CommandDecode, err error) {
    n, err := self.stream.ReadBytes(self.rbuf)
    if err != nil {
        return nil, err
    }

    if n != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(self.rbuf[0]) != protocol.MAGIC {
        return nil, errors.New("unknown magic")
    }

    if uint8(self.rbuf[1]) != protocol.VERSION {
        return nil, errors.New("unknown version")
    }

    switch uint8(self.rbuf[2]) {
    case protocol.COMMAND_LOCK:
        command := protocol.LockResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_UNLOCK:
        command := protocol.LockResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case protocol.COMMAND_STATE:
        command := protocol.ResultStateCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    default:
        return nil, errors.New("unknown command")
    }
}

func (self *ClientProtocol) Write(result protocol.CommandEncode) (err error) {
    wbuf := make([]byte, 64)
    err = result.Encode(wbuf)
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(wbuf)
}

func (self *ClientProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}
