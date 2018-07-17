package slock

import (
    "io"
    "net"
    "errors"
)

type Protocol interface {
    Read() (CommandDecode, error)
    Write(CommandEncode) (error)
    Close() (error)
    RemoteAddr() net.Addr
}

type ServerProtocol struct {
    slock *SLock
    stream *Stream
    last_lock *Lock
}

func NewServerProtocol(slock *SLock, stream *Stream) *ServerProtocol {
    protocol := &ServerProtocol{slock,stream, nil}
    slock.Log().Infof("connection open %s", protocol.RemoteAddr().String())
    return protocol
}

func (self *ServerProtocol) Close() (err error) {
    if self.last_lock != nil {
        if !self.last_lock.Expried {
            self.last_lock.ExpriedTime = 0
        }
        self.last_lock = nil
    }
    self.stream.Close()
    self.slock.Log().Infof("connection close %s", self.RemoteAddr().String())
    return nil
}

func (self *ServerProtocol) Read() (command CommandDecode, err error) {
    b, err := self.stream.ReadBytes(64)
    if err == io.EOF {
        return nil, err
    }
    if len(b) != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(b[0]) != MAGIC {
        command := NewCommand(self, b)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_MAGIC))
        return nil, errors.New("unknown magic")
    }

    if uint8(b[1]) != VERSION {
        command := NewCommand(self, b)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_VERSION))
        return nil, errors.New("unknown version")
    }

    switch uint8(b[2]) {
    case COMMAND_LOCK:
        return NewLockCommand(self, b), nil
    case COMMAND_UNLOCK:
        return NewLockCommand(self, b), nil
    case COMMAND_STATE:
        return NewStateCommand(self, b), nil
    default:
        command := NewCommand(self, b)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_VERSION))
        return nil, errors.New("unknown command")
    }
    return nil, nil
}

func (self *ServerProtocol) Write(result CommandEncode) (err error) {
    b, err := result.Encode()
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(b)
}

func (self *ServerProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}


type ClientProtocol struct {
    stream *Stream
}

func NewClientProtocol(stream *Stream) *ClientProtocol {
    protocol := &ClientProtocol{stream}
    return protocol
}

func (self *ClientProtocol) Close() (err error) {
    self.stream.Close()
    return nil
}

func (self *ClientProtocol) Read() (command CommandDecode, err error) {
    b, err := self.stream.ReadBytes(64)
    if err == io.EOF {
        return nil, err
    }
    if len(b) != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(b[0]) != MAGIC {
        return nil, errors.New("unknown magic")
    }

    if uint8(b[1]) != VERSION {
        return nil, errors.New("unknown version")
    }

    switch uint8(b[2]) {
    case COMMAND_LOCK:
        command := LockResultCommand{}
        command.Protocol = self
        command.Decode(b)
        return &command, nil
    case COMMAND_UNLOCK:
        command := LockResultCommand{}
        command.Protocol = self
        command.Decode(b)
        return &command, nil
    case COMMAND_STATE:
        command := ResultStateCommand{}
        command.Protocol = self
        command.Decode(b)
        return &command, nil
    default:
        return nil, errors.New("unknown command")
    }
    return nil, nil
}

func (self *ClientProtocol) Write(result CommandEncode) (err error) {
    b, err := result.Encode()
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(b)
}

func (self *ClientProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}