package slock

import (
    "net"
    "errors"
    "sync/atomic"
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
    free_commands []*LockCommand
    free_command_count *uint32
    free_result_commands []*LockResultCommand
    free_result_command_count *uint32
}

func NewServerProtocol(slock *SLock, stream *Stream, free_commands []*LockCommand, free_command_count *uint32, free_result_commands []*LockResultCommand, free_result_command_count *uint32) *ServerProtocol {
    wbuf := make([]byte, 64)
    wbuf[0] = byte(MAGIC)
    wbuf[1] = byte(VERSION)
    
    protocol := &ServerProtocol{slock, stream, make([]byte, 64), wbuf, free_commands, free_command_count, free_result_commands, free_result_command_count}
    slock.Log().Infof("connection open %s", protocol.RemoteAddr().String())
    return protocol
}

func (self *ServerProtocol) Close() (err error) {
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

    command_type := uint8(self.rbuf[2])
    switch command_type {
    case COMMAND_LOCK:
        free_command_count := atomic.AddUint32(self.free_command_count, 0xffffffff)
        free_index := (free_command_count + 1) & FREE_LOCK_COMMAND_MAX_COUNT
        lock_command := self.free_commands[free_index]
        if lock_command == nil {
            commands := make([]LockCommand, 4096)
            for i := 1; i < 4096; i++ {
                self.FreeLockCommand(&commands[i])
            }
            lock_command = &commands[0]
        } else {
            self.free_commands[free_index] = nil
        }
        buf := self.rbuf

        lock_command.CommandType = command_type

        for i := 0; i < 16; i+=4{
            lock_command.RequestId[i] = buf[3 + i]
            lock_command.RequestId[i + 1] = buf[4 + i]
            lock_command.RequestId[i + 2] = buf[5 + i]
            lock_command.RequestId[i + 3] = buf[6 + i]
        }

        lock_command.Flag = uint8(buf[19])
        lock_command.DbId = uint8(buf[20])

        for i := 0; i < 16; i+=4{
            lock_command.LockId[i] = buf[21 + i]
            lock_command.LockId[i + 1] = buf[22 + i]
            lock_command.LockId[i + 2] = buf[23 + i]
            lock_command.LockId[i + 3] = buf[24 + i]
        }

        for i := 0; i < 16; i+=4{
            lock_command.LockKey[i] = buf[37 + i]
            lock_command.LockKey[i + 1] = buf[38 + i]
            lock_command.LockKey[i + 2] = buf[39 + i]
            lock_command.LockKey[i + 3] = buf[40 + i]
        }

        lock_command.Timeout = uint32(buf[53]) | uint32(buf[54])<<8 | uint32(buf[55])<<16 | uint32(buf[56])<<24
        lock_command.Expried = uint32(buf[57]) | uint32(buf[58])<<8 | uint32(buf[59])<<16 | uint32(buf[60])<<24
        lock_command.Count = uint16(buf[61]) | uint16(buf[62])<<8
        return lock_command, nil
    case COMMAND_UNLOCK:
        free_command_count := atomic.AddUint32(self.free_command_count, 0xffffffff)
        free_index := (free_command_count + 1) & FREE_LOCK_COMMAND_MAX_COUNT
        lock_command := self.free_commands[free_index]
        if lock_command == nil {
            commands := make([]LockCommand, 4096)
            for i := 1; i < 4096; i++ {
                self.FreeLockCommand(&commands[i])
            }
            lock_command = &commands[0]
        } else {
            self.free_commands[free_index] = nil
        }
        buf := self.rbuf

        lock_command.CommandType = command_type

        for i := 0; i < 16; i+=4{
            lock_command.RequestId[i] = buf[3 + i]
            lock_command.RequestId[i + 1] = buf[4 + i]
            lock_command.RequestId[i + 2] = buf[5 + i]
            lock_command.RequestId[i + 3] = buf[6 + i]
        }

        lock_command.Flag = uint8(buf[19])
        lock_command.DbId = uint8(buf[20])

        for i := 0; i < 16; i+=4{
            lock_command.LockId[i] = buf[21 + i]
            lock_command.LockId[i + 1] = buf[22 + i]
            lock_command.LockId[i + 2] = buf[23 + i]
            lock_command.LockId[i + 3] = buf[24 + i]
        }

        for i := 0; i < 16; i+=4{
            lock_command.LockKey[i] = buf[37 + i]
            lock_command.LockKey[i + 1] = buf[38 + i]
            lock_command.LockKey[i + 2] = buf[39 + i]
            lock_command.LockKey[i + 3] = buf[40 + i]
        }

        lock_command.Timeout = uint32(buf[53]) | uint32(buf[54])<<8 | uint32(buf[55])<<16 | uint32(buf[56])<<24
        lock_command.Expried = uint32(buf[57]) | uint32(buf[58])<<8 | uint32(buf[59])<<16 | uint32(buf[60])<<24
        lock_command.Count = uint16(buf[61]) | uint16(buf[62])<<8
        return lock_command, nil
    case COMMAND_STATE:
        state_command := &StateCommand{}
        err := state_command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return state_command, nil
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
    free_command_count := atomic.AddUint32(self.free_command_count, 1)
    self.free_commands[free_command_count & FREE_LOCK_COMMAND_MAX_COUNT] = command
    return nil
}

func (self *ServerProtocol) FreeLockResultCommand(command *LockResultCommand) net.Addr {
    free_result_command_count := atomic.AddUint32(self.free_result_command_count, 1)
    self.free_result_commands[free_result_command_count & FREE_LOCK_RESULT_COMMAND_MAX_COUNT] = command
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
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case COMMAND_UNLOCK:
        command := LockResultCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
        return &command, nil
    case COMMAND_STATE:
        command := ResultStateCommand{}
        err := command.Decode(self.rbuf)
        if err != nil {
            return nil, err
        }
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
