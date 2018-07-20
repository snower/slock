package slock

import (
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
    rbuf []byte
    wbuf []byte
    last_lock *Lock
    free_commands []*LockCommand
    free_command_count int
    free_result_commands []*LockResultCommand
    free_result_command_count int
}

func NewServerProtocol(slock *SLock, stream *Stream) *ServerProtocol {
    wbuf := make([]byte, 64)
    wbuf[0] = byte(MAGIC)
    wbuf[1] = byte(VERSION)
    
    protocol := &ServerProtocol{slock, stream, make([]byte, 64), wbuf, nil, make([]*LockCommand, 64), -1, make([]*LockResultCommand, 64), -1}
    slock.Log().Infof("connection open %s", protocol.RemoteAddr().String())
    return protocol
}

func (self *ServerProtocol) Close() (err error) {
    if self.last_lock != nil {
        if !self.last_lock.expried {
            self.last_lock.expried_time = 0
        }
        self.last_lock = nil
    }
    self.stream.Close()
    self.slock.Log().Infof("connection close %s", self.RemoteAddr().String())
    return nil
}

func (self *ServerProtocol) Read() (command CommandDecode, err error) {
    n, err := self.stream.ReadBytes(self.rbuf)
    if err != nil {
        return nil, err
    }

    if n != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(self.rbuf[0]) != MAGIC {
        command := NewCommand(self.rbuf)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_MAGIC), true)
        return nil, errors.New("unknown magic")
    }

    if uint8(self.rbuf[1]) != VERSION {
        command := NewCommand(self.rbuf)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_VERSION), true)
        return nil, errors.New("unknown version")
    }

    switch uint8(self.rbuf[2]) {
    case COMMAND_LOCK:
        if self.free_command_count >= 0 {
            lock_command := self.free_commands[self.free_command_count]
            self.free_command_count--
            err := lock_command.Decode(self.rbuf)
            if err != nil {
                return nil, nil
            }
            return lock_command, nil
        }

        return NewLockCommand(self.rbuf), nil
    case COMMAND_UNLOCK:
        if self.free_command_count >= 0 {
            lock_command := self.free_commands[self.free_command_count]
            self.free_command_count--
            err := lock_command.Decode(self.rbuf)
            if err != nil {
                return nil, nil
            }
            return lock_command, nil
        }

        return NewLockCommand(self.rbuf), nil
    case COMMAND_STATE:
        return NewStateCommand(self.rbuf), nil
    default:
        command := NewCommand(self.rbuf)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_VERSION), true)
        return nil, errors.New("unknown command")
    }
    return nil, nil
}

func (self *ServerProtocol) Write(result CommandEncode, use_cached bool) (err error) {
    if use_cached {
        err = result.Encode(self.wbuf)
        if err != nil {
            return err
        }
        return self.stream.WriteBytes(self.wbuf)
    }

    wbuf := make([]byte, 64)
    err = result.Encode(wbuf)
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(wbuf)
}

func (self *ServerProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}

func (self *ServerProtocol) FreeLockCommand(command *LockCommand) net.Addr {
    if self.free_command_count < 63 {
        self.free_command_count++
        self.free_commands[self.free_command_count] = command
    }
    return nil
}

func (self *ServerProtocol) FreeLockResultCommand(command *LockResultCommand) net.Addr {
    if self.free_result_command_count < 63 {
        self.free_result_command_count++
        self.free_result_commands[self.free_result_command_count] = command
    }
    return nil
}

type ClientProtocol struct {
    stream *Stream
    rbuf []byte
}

func NewClientProtocol(stream *Stream) *ClientProtocol {
    protocol := &ClientProtocol{stream, make([]byte, 64)}
    return protocol
}

func (self *ClientProtocol) Close() (err error) {
    self.stream.Close()
    return nil
}

func (self *ClientProtocol) Read() (command CommandDecode, err error) {
    n, err := self.stream.ReadBytes(self.rbuf)
    if err != nil {
        return nil, err
    }

    if n != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(self.rbuf[0]) != MAGIC {
        return nil, errors.New("unknown magic")
    }

    if uint8(self.rbuf[1]) != VERSION {
        return nil, errors.New("unknown version")
    }

    switch uint8(self.rbuf[2]) {
    case COMMAND_LOCK:
        command := LockResultCommand{}
        command.Decode(self.rbuf)
        return &command, nil
    case COMMAND_UNLOCK:
        command := LockResultCommand{}
        command.Decode(self.rbuf)
        return &command, nil
    case COMMAND_STATE:
        command := ResultStateCommand{}
        command.Decode(self.rbuf)
        return &command, nil
    default:
        return nil, errors.New("unknown command")
    }
    return nil, nil
}

func (self *ClientProtocol) Write(result CommandEncode) (err error) {
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
