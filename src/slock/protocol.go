package slock

import (
    "net"
    "errors"
    "sync"
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
    owbuf []byte
    free_commands []*LockCommand
    free_command_count int
    free_command_max_count int
    free_result_command_lock sync.Mutex
}

func NewServerProtocol(slock *SLock, stream *Stream) *ServerProtocol {
    wbuf := make([]byte, 64)
    wbuf[0] = byte(MAGIC)
    wbuf[1] = byte(VERSION)

    owbuf := make([]byte, 64)
    owbuf[0] = byte(MAGIC)
    owbuf[1] = byte(VERSION)

    protocol := &ServerProtocol{slock, stream, make([]byte, 64), wbuf, owbuf,
    make([]*LockCommand, 4096), 63, 4095, sync.Mutex{}}
    lock_commands := make([]LockCommand, 64)
    for i := 0; i < 64; i++ {
        protocol.free_commands[i] = &lock_commands[i]
    }

    slock.Log().Infof("connection open %s", protocol.RemoteAddr().String())
    return protocol
}

func (self *ServerProtocol) Close() (err error) {
    self.stream.Close()
    self.slock.Log().Infof("connection close %s", self.RemoteAddr().String())
    return nil
}

func (self *ServerProtocol) Read() (command CommandDecode, err error) {
    buf := self.rbuf

    _, err = self.stream.ReadBytes(buf)
    if err != nil {
        return nil, err
    }

    if len(buf) < 64 {
        return nil, errors.New("command data too short")
    }

    mv := uint16(buf[0]) | uint16(buf[1])<<8
    if mv != 0x0156 {
        if mv & 0xff != MAGIC {
            command := NewCommand(buf)
            self.Write(NewResultCommand(command, RESULT_UNKNOWN_MAGIC), true)
            return nil, errors.New("unknown magic")
        }

        if (mv>>8) & 0xff != VERSION {
            command := NewCommand(buf)
            self.Write(NewResultCommand(command, RESULT_UNKNOWN_VERSION), true)
            return nil, errors.New("unknown version")
        }
    }

    command_type := uint8(buf[2])
    switch command_type {
    case COMMAND_LOCK:
        if self.free_command_count >= 0 {
            lock_command := self.free_commands[self.free_command_count]
            self.free_command_count--

            lock_command.CommandType = command_type

            lock_command.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
            lock_command.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

            lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

            lock_command.LockId[0] = uint64(buf[21]) | uint64(buf[22])<<8 | uint64(buf[23])<<16 | uint64(buf[24])<<24 | uint64(buf[25])<<32 | uint64(buf[26])<<40 | uint64(buf[27])<<48 | uint64(buf[28])<<56
            lock_command.LockId[1] = uint64(buf[29]) | uint64(buf[30])<<8 | uint64(buf[31])<<16 | uint64(buf[32])<<24 | uint64(buf[33])<<32 | uint64(buf[34])<<40 | uint64(buf[35])<<48 | uint64(buf[36])<<56

            lock_command.LockKey[0] = uint64(buf[37]) | uint64(buf[38])<<8 | uint64(buf[39])<<16 | uint64(buf[40])<<24 | uint64(buf[41])<<32 | uint64(buf[42])<<40 | uint64(buf[43])<<48 | uint64(buf[44])<<56
            lock_command.LockKey[1] = uint64(buf[45]) | uint64(buf[46])<<8 | uint64(buf[47])<<16 | uint64(buf[48])<<24 | uint64(buf[49])<<32 | uint64(buf[50])<<40 | uint64(buf[51])<<48 | uint64(buf[52])<<56

            lock_command.Timeout = uint32(buf[53]) | uint32(buf[54])<<8 | uint32(buf[55])<<16 | uint32(buf[56])<<24
            lock_command.Expried = uint32(buf[57]) | uint32(buf[58])<<8 | uint32(buf[59])<<16 | uint32(buf[60])<<24
            lock_command.Count = uint16(buf[61]) | uint16(buf[62])<<8
            return lock_command, nil
        }

        lock_commands := make([]LockCommand, 64)
        lock_command := &lock_commands[0]
        for i := 1; i < 64; i++ {
            self.FreeLockCommand(&lock_commands[i])
        }

        lock_command.CommandType = command_type

        lock_command.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
        lock_command.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

        lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

        lock_command.LockId[0] = uint64(buf[21]) | uint64(buf[22])<<8 | uint64(buf[23])<<16 | uint64(buf[24])<<24 | uint64(buf[25])<<32 | uint64(buf[26])<<40 | uint64(buf[27])<<48 | uint64(buf[28])<<56
        lock_command.LockId[1] = uint64(buf[29]) | uint64(buf[30])<<8 | uint64(buf[31])<<16 | uint64(buf[32])<<24 | uint64(buf[33])<<32 | uint64(buf[34])<<40 | uint64(buf[35])<<48 | uint64(buf[36])<<56

        lock_command.LockKey[0] = uint64(buf[37]) | uint64(buf[38])<<8 | uint64(buf[39])<<16 | uint64(buf[40])<<24 | uint64(buf[41])<<32 | uint64(buf[42])<<40 | uint64(buf[43])<<48 | uint64(buf[44])<<56
        lock_command.LockKey[1] = uint64(buf[45]) | uint64(buf[46])<<8 | uint64(buf[47])<<16 | uint64(buf[48])<<24 | uint64(buf[49])<<32 | uint64(buf[50])<<40 | uint64(buf[51])<<48 | uint64(buf[52])<<56

        lock_command.Timeout = uint32(buf[53]) | uint32(buf[54])<<8 | uint32(buf[55])<<16 | uint32(buf[56])<<24
        lock_command.Expried = uint32(buf[57]) | uint32(buf[58])<<8 | uint32(buf[59])<<16 | uint32(buf[60])<<24
        lock_command.Count = uint16(buf[61]) | uint16(buf[62])<<8
        return lock_command, nil
    case COMMAND_UNLOCK:
        if self.free_command_count >= 0 {
            lock_command := self.free_commands[self.free_command_count]
            self.free_command_count--

            lock_command.CommandType = command_type

            lock_command.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
            lock_command.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

            lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

            lock_command.LockId[0] = uint64(buf[21]) | uint64(buf[22])<<8 | uint64(buf[23])<<16 | uint64(buf[24])<<24 | uint64(buf[25])<<32 | uint64(buf[26])<<40 | uint64(buf[27])<<48 | uint64(buf[28])<<56
            lock_command.LockId[1] = uint64(buf[29]) | uint64(buf[30])<<8 | uint64(buf[31])<<16 | uint64(buf[32])<<24 | uint64(buf[33])<<32 | uint64(buf[34])<<40 | uint64(buf[35])<<48 | uint64(buf[36])<<56

            lock_command.LockKey[0] = uint64(buf[37]) | uint64(buf[38])<<8 | uint64(buf[39])<<16 | uint64(buf[40])<<24 | uint64(buf[41])<<32 | uint64(buf[42])<<40 | uint64(buf[43])<<48 | uint64(buf[44])<<56
            lock_command.LockKey[1] = uint64(buf[45]) | uint64(buf[46])<<8 | uint64(buf[47])<<16 | uint64(buf[48])<<24 | uint64(buf[49])<<32 | uint64(buf[50])<<40 | uint64(buf[51])<<48 | uint64(buf[52])<<56

            lock_command.Timeout = uint32(buf[53]) | uint32(buf[54])<<8 | uint32(buf[55])<<16 | uint32(buf[56])<<24
            lock_command.Expried = uint32(buf[57]) | uint32(buf[58])<<8 | uint32(buf[59])<<16 | uint32(buf[60])<<24
            lock_command.Count = uint16(buf[61]) | uint16(buf[62])<<8
            return lock_command, nil
        }

        lock_commands := make([]LockCommand, 64)
        lock_command := &lock_commands[0]
        for i := 1; i < 64; i++ {
            self.FreeLockCommand(&lock_commands[i])
        }

        lock_command.CommandType = command_type

        lock_command.RequestId[0] = uint64(buf[3]) | uint64(buf[4])<<8 | uint64(buf[5])<<16 | uint64(buf[6])<<24 | uint64(buf[7])<<32 | uint64(buf[8])<<40 | uint64(buf[9])<<48 | uint64(buf[10])<<56
        lock_command.RequestId[1] = uint64(buf[11]) | uint64(buf[12])<<8 | uint64(buf[13])<<16 | uint64(buf[14])<<24 | uint64(buf[15])<<32 | uint64(buf[16])<<40 | uint64(buf[17])<<48 | uint64(buf[18])<<56

        lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

        lock_command.LockId[0] = uint64(buf[21]) | uint64(buf[22])<<8 | uint64(buf[23])<<16 | uint64(buf[24])<<24 | uint64(buf[25])<<32 | uint64(buf[26])<<40 | uint64(buf[27])<<48 | uint64(buf[28])<<56
        lock_command.LockId[1] = uint64(buf[29]) | uint64(buf[30])<<8 | uint64(buf[31])<<16 | uint64(buf[32])<<24 | uint64(buf[33])<<32 | uint64(buf[34])<<40 | uint64(buf[35])<<48 | uint64(buf[36])<<56

        lock_command.LockKey[0] = uint64(buf[37]) | uint64(buf[38])<<8 | uint64(buf[39])<<16 | uint64(buf[40])<<24 | uint64(buf[41])<<32 | uint64(buf[42])<<40 | uint64(buf[43])<<48 | uint64(buf[44])<<56
        lock_command.LockKey[1] = uint64(buf[45]) | uint64(buf[46])<<8 | uint64(buf[47])<<16 | uint64(buf[48])<<24 | uint64(buf[49])<<32 | uint64(buf[50])<<40 | uint64(buf[51])<<48 | uint64(buf[52])<<56

        lock_command.Timeout = uint32(buf[53]) | uint32(buf[54])<<8 | uint32(buf[55])<<16 | uint32(buf[56])<<24
        lock_command.Expried = uint32(buf[57]) | uint32(buf[58])<<8 | uint32(buf[59])<<16 | uint32(buf[60])<<24
        lock_command.Count = uint16(buf[61]) | uint16(buf[62])<<8
        return lock_command, nil
    case COMMAND_STATE:
        state_command := &StateCommand{}
        err := state_command.Decode(buf)
        if err != nil {
            return nil, err
        }
        return state_command, nil
    default:
        command := NewCommand(buf)
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

    err = result.Encode(self.owbuf)
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(self.owbuf)
}

func (self *ServerProtocol) RemoteAddr() net.Addr {
    return self.stream.RemoteAddr()
}

func (self *ServerProtocol) FreeLockCommand(command *LockCommand) net.Addr {
    if self.free_command_count >= self.free_command_max_count {
        self.free_command_max_count = (self.free_command_max_count + 1) * 2 - 1
        free_commands := make([]*LockCommand, self.free_command_max_count + 1)
        copy(free_commands, self.free_commands)
        self.free_commands = free_commands
    }

    self.free_command_count++
    self.free_commands[self.free_command_count] = command
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
