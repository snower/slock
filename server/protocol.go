package server

import (
    "net"
    "errors"
    "sync"
    "github.com/snower/slock/protocol"
)

type ServerProtocol struct {
    slock *SLock
    stream *Stream
    free_commands *LockCommandQueue
    free_result_command_lock *sync.Mutex
    rbuf []byte
    wbuf []byte
    owbuf []byte
}

func NewServerProtocol(slock *SLock, stream *Stream) *ServerProtocol {
    wbuf := make([]byte, 64)
    wbuf[0] = byte(protocol.MAGIC)
    wbuf[1] = byte(protocol.VERSION)

    owbuf := make([]byte, 64)
    owbuf[0] = byte(protocol.MAGIC)
    owbuf[1] = byte(protocol.VERSION)

    server_protocol := &ServerProtocol{slock, stream, NewLockCommandQueue(4, 16, 256),
    &sync.Mutex{}, make([]byte, 64), wbuf, owbuf}

    if slock.free_lock_command_count > 64 {
        slock.free_lock_command_lock.Lock()
        if slock.free_lock_command_count > 64 {
            for i := 0; i < 64; i++ {
                lock_command := slock.free_lock_commands.PopRight()
                if lock_command == nil {
                    break
                }
                slock.free_lock_command_count--
                server_protocol.free_commands.Push(lock_command)
            }
            slock.free_lock_command_lock.Unlock()
        } else {
            slock.free_lock_command_lock.Unlock()

            lock_commands := make([]protocol.LockCommand, 64)
            for i := 0; i < 64; i++ {
                server_protocol.free_commands.Push(&lock_commands[i])
            }
        }
    } else {
        lock_commands := make([]protocol.LockCommand, 64)
        for i := 0; i < 64; i++ {
            server_protocol.free_commands.Push(&lock_commands[i])
        }
    }

    slock.Log().Infof("connection open %s", server_protocol.RemoteAddr().String())
    return server_protocol
}

func (self *ServerProtocol) Close() (err error) {
    if self.stream.Close() != nil {
        self.slock.Log().Infof("connection close error: %s", self.RemoteAddr().String())
    } else {
        self.slock.Log().Infof("connection close %s", self.RemoteAddr().String())
    }

    self.slock.free_lock_command_lock.Lock()
    for ;; {
        command := self.free_commands.PopRight()
        if command == nil {
            break
        }
        self.slock.free_lock_commands.Push(command)
        self.slock.free_lock_command_count++
    }
    self.slock.free_lock_command_lock.Unlock()
    return nil
}

func (self *ServerProtocol) Read() (command protocol.CommandDecode, err error) {
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
        if mv & 0xff != protocol.MAGIC {
            command := protocol.NewCommand(buf)
            self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_MAGIC), true)
            return nil, errors.New("unknown magic")
        }

        if (mv>>8) & 0xff != protocol.VERSION {
            command := protocol.NewCommand(buf)
            self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_VERSION), true)
            return nil, errors.New("unknown version")
        }
    }

    command_type := uint8(buf[2])
    switch command_type {
    case protocol.COMMAND_LOCK:
        lock_command := self.free_commands.PopRight()
        if lock_command == nil {
            if self.slock.free_lock_command_count > 64 {
                self.slock.free_lock_command_lock.Lock()
                if self.slock.free_lock_command_count > 64 {
                    for i := 0; i < 64; i++ {
                        lock_command = self.slock.free_lock_commands.PopRight()
                        if lock_command == nil {
                            break
                        }
                        self.slock.free_lock_command_count--
                        self.free_commands.Push(lock_command)
                    }
                    self.slock.free_lock_command_lock.Unlock()
                } else {
                    self.slock.free_lock_command_lock.Unlock()

                    lock_commands := make([]protocol.LockCommand, 64)
                    for i := 0; i < 64; i++ {
                        self.free_commands.Push(&lock_commands[i])
                    }
                }
            } else {
                lock_commands := make([]protocol.LockCommand, 64)
                for i := 0; i < 64; i++ {
                    self.free_commands.Push(&lock_commands[i])
                }
            }
            lock_command = self.free_commands.PopRight()
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
    case protocol.COMMAND_UNLOCK:
        lock_command := self.free_commands.PopRight()
        if lock_command == nil {
            if self.slock.free_lock_command_count > 64 {
                self.slock.free_lock_command_lock.Lock()
                if self.slock.free_lock_command_count > 64 {
                    for i := 0; i < 64; i++ {
                        lock_command = self.slock.free_lock_commands.PopRight()
                        if lock_command == nil {
                            break
                        }
                        self.slock.free_lock_command_count--
                        self.free_commands.Push(lock_command)
                    }
                    self.slock.free_lock_command_lock.Unlock()
                } else {
                    self.slock.free_lock_command_lock.Unlock()

                    lock_commands := make([]protocol.LockCommand, 64)
                    for i := 0; i < 64; i++ {
                        self.free_commands.Push(&lock_commands[i])
                    }
                }
            } else {
                lock_commands := make([]protocol.LockCommand, 64)
                for i := 0; i < 64; i++ {
                    self.free_commands.Push(&lock_commands[i])
                }
            }
            lock_command = self.free_commands.PopRight()
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
    case protocol.COMMAND_STATE:
        state_command := &protocol.StateCommand{}
        err := state_command.Decode(buf)
        if err != nil {
            return nil, err
        }
        return state_command, nil
    default:
        command := protocol.NewCommand(buf)
        self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_VERSION), true)
        return nil, errors.New("unknown command")
    }
}

func (self *ServerProtocol) Write(result protocol.CommandEncode, use_cached bool) (err error) {
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

func (self *ServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
    return self.free_commands.Push(command)
}