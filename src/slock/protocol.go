package slock

import (
    "io"
    "net"
    "errors"
)

type Buffer struct {
    buf []byte
}

func (self *Buffer) Write(p []byte) (n int, err error)  {
    self.buf = p
    return 64, nil
}

func (self *Buffer) Read(p []byte) (n int, err error)  {
    n = copy(p, self.buf)
    return n, nil
}

type Protocol interface {
    Read() (CommandDecode, error)
    Write(CommandEncode) (error)
    Close() (error)
    RemoteAddr() net.Addr
}

type ServerProtocol struct {
    slock *SLock
    stream *Stream
    rbuf Buffer
    wbuf Buffer
    last_lock *Lock
    free_commands []*LockCommand
    free_command_count int
    free_result_commands []*LockResultCommand
    free_result_command_count int
}

func NewServerProtocol(slock *SLock, stream *Stream) *ServerProtocol {
    protocol := &ServerProtocol{slock,stream, Buffer{make([]byte, 64)}, Buffer{}, nil, make([]*LockCommand, 64), -1, make([]*LockResultCommand, 64), -1}
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
    n, err := self.stream.ReadBytes(self.rbuf.buf)
    if err == io.EOF {
        return nil, err
    }

    if n != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(self.rbuf.buf[0]) != MAGIC {
        command := NewCommand(&self.rbuf)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_MAGIC))
        return nil, errors.New("unknown magic")
    }

    if uint8(self.rbuf.buf[1]) != VERSION {
        command := NewCommand(&self.rbuf)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_VERSION))
        return nil, errors.New("unknown version")
    }

    switch uint8(self.rbuf.buf[2]) {
    case COMMAND_LOCK:
        if self.free_command_count >= 0 {
            lock_command := self.free_commands[self.free_command_count]
            self.free_command_count--
            err := lock_command.Decode(&self.rbuf)
            if err != nil {
                return nil, nil
            }
            return lock_command, nil
        }

        return NewLockCommand(&self.rbuf), nil
    case COMMAND_UNLOCK:
        if self.free_command_count >= 0 {
            lock_command := self.free_commands[self.free_command_count]
            self.free_command_count--
            err := lock_command.Decode(&self.rbuf)
            if err != nil {
                return nil, nil
            }
            return lock_command, nil
        }

        return NewLockCommand(&self.rbuf), nil
    case COMMAND_STATE:
        return NewStateCommand(&self.rbuf), nil
    default:
        command := NewCommand(&self.rbuf)
        self.Write(NewResultCommand(command, RESULT_UNKNOWN_VERSION))
        return nil, errors.New("unknown command")
    }
    return nil, nil
}

func (self *ServerProtocol) Write(result CommandEncode) (err error) {
    err = result.Encode(&self.wbuf)
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(self.wbuf.buf)
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
    rbuf Buffer
    wbuf Buffer
}

func NewClientProtocol(stream *Stream) *ClientProtocol {
    protocol := &ClientProtocol{stream, Buffer{make([]byte, 64)}, Buffer{}}
    return protocol
}

func (self *ClientProtocol) Close() (err error) {
    self.stream.Close()
    return nil
}

func (self *ClientProtocol) Read() (command CommandDecode, err error) {
    n, err := self.stream.ReadBytes(self.rbuf.buf)
    if err == io.EOF {
        return nil, err
    }
    if n != 64 {
        return nil, errors.New("command data too short")
    }

    if uint8(self.rbuf.buf[0]) != MAGIC {
        return nil, errors.New("unknown magic")
    }

    if uint8(self.rbuf.buf[1]) != VERSION {
        return nil, errors.New("unknown version")
    }

    switch uint8(self.rbuf.buf[2]) {
    case COMMAND_LOCK:
        command := LockResultCommand{}
        command.Decode(&self.rbuf)
        return &command, nil
    case COMMAND_UNLOCK:
        command := LockResultCommand{}
        command.Decode(&self.rbuf)
        return &command, nil
    case COMMAND_STATE:
        command := ResultStateCommand{}
        command.Decode(&self.rbuf)
        return &command, nil
    default:
        return nil, errors.New("unknown command")
    }
    return nil, nil
}

func (self *ClientProtocol) Write(result CommandEncode) (err error) {
    err = result.Encode(&self.wbuf)
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(self.wbuf.buf)
}

func (self *ClientProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}