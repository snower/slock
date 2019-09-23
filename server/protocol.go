package server

import (
    "crypto/md5"
    "encoding/hex"
    "fmt"
    "io"
    "math/rand"
    "net"
    "errors"
    "strconv"
    "strings"
    "sync"
    "github.com/snower/slock/protocol"
    "sync/atomic"
    "time"
)

type ServerProtocol interface {
    Init(client_id [2]uint64) error
    Read() (protocol.CommandDecode, error)
    Write(protocol.CommandEncode) (error)
    Process() error
    ProcessParse(buf []byte) error
    ProcessBuild(command protocol.ICommand) error
    ProcessCommad(command protocol.ICommand) error
    ProcessLockCommand(command *protocol.LockCommand) error
    ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, use_cached_command bool) error
    Close() (error)
    GetStream() *Stream
    RemoteAddr() net.Addr
    GetLockCommand() *protocol.LockCommand
    FreeLockCommand(command *protocol.LockCommand) error
}

type BinaryServerProtocol struct {
    slock                       *SLock
    stream                      *Stream
    client_id                   [2]uint64
    free_commands               *LockCommandQueue
    free_result_command_lock    *sync.Mutex
    inited                      bool
    closed                      bool
    rbuf                        []byte
    wbuf                        []byte
    owbuf                       []byte
}

func NewBinaryServerProtocol(slock *SLock, stream *Stream) *BinaryServerProtocol {
    wbuf := make([]byte, 64)
    wbuf[0] = byte(protocol.MAGIC)
    wbuf[1] = byte(protocol.VERSION)

    owbuf := make([]byte, 64)
    owbuf[0] = byte(protocol.MAGIC)
    owbuf[1] = byte(protocol.VERSION)

    server_protocol := &BinaryServerProtocol{slock, stream, [2]uint64{0, 0}, NewLockCommandQueue(4, 16, FREE_COMMAND_QUEUE_INIT_SIZE),
    &sync.Mutex{}, false, false, make([]byte, 64), wbuf, owbuf}
    server_protocol.InitLockCommand()
    return server_protocol
}

func (self *BinaryServerProtocol) Init(client_id [2]uint64) error {
    self.client_id = client_id
    self.inited = true
    return nil
}

func (self *BinaryServerProtocol) Close() error {
    if self.inited {
        self.inited = false
        self.slock.glock.Lock()
        if sp, ok := self.slock.streams[self.client_id]; ok {
            if sp == self {
                delete(self.slock.streams, self.client_id)
            }
        }
        self.slock.glock.Unlock()
    }

    if self.stream != nil {
        if self.stream.Close() != nil {
            self.slock.Log().Errorf("connection close error: %s", self.RemoteAddr().String())
        } else {
            self.slock.Log().Infof("connection close %s", self.RemoteAddr().String())
        }
    }

    self.UnInitLockCommand()
    self.closed = true
    return nil
}

func (self *BinaryServerProtocol) Read() (protocol.CommandDecode, error) {
    buf := self.rbuf

    n, err := self.stream.ReadBytes(buf)
    if err != nil {
        return nil, err
    }

    if n < 64 {
        return nil, errors.New("command data too short")
    }

    if len(buf) < 64 {
        return nil, errors.New("command data too short")
    }

    mv := uint16(buf[0]) | uint16(buf[1])<<8
    if mv != 0x0156 {
        if mv & 0xff != uint16(protocol.MAGIC) {
            return nil, errors.New("unknown magic")
        }

        if (mv>>8) & 0xff != uint16(protocol.VERSION) {
            return nil, errors.New("unknown version")
        }
    }

    command_type := uint8(buf[2])
    switch command_type {
    case protocol.COMMAND_LOCK:
        lock_command := self.GetLockCommand()
        err := lock_command.Decode(buf)
        if err != nil {
            return nil, err
        }
        return lock_command, nil

    case protocol.COMMAND_UNLOCK:
        lock_command := self.GetLockCommand()
        err := lock_command.Decode(buf)
        if err != nil {
            return nil, err
        }
        return lock_command, nil
    default:
        switch command_type {
        case protocol.COMMAND_INIT:
            init_command := &protocol.InitCommand{}
            err := init_command.Decode(buf)
            if err != nil {
                return nil, err
            }
            return init_command, nil

        case protocol.COMMAND_STATE:
            state_command := &protocol.StateCommand{}
            err := state_command.Decode(buf)
            if err != nil {
                return nil, err
            }
            return state_command, nil

        case protocol.COMMAND_ADMIN:
            admin_command := &protocol.AdminCommand{}
            err := admin_command.Decode(buf)
            if err != nil {
                return nil, err
            }
            return admin_command, nil
        }
    }
    return nil, errors.New("Unknown Command")
}

func (self *BinaryServerProtocol) Write(result protocol.CommandEncode) error {
    if self.closed {
        return errors.New("Protocol Closed")
    }

    err := result.Encode(self.wbuf)
    if err != nil {
        return err
    }
    return self.stream.WriteBytes(self.wbuf)
}

func (self *BinaryServerProtocol) Process() error {
    buf := self.rbuf
    for ; !self.closed; {
        n, err := self.stream.ReadBytes(buf)
        if err != nil {
            return err
        }

        if n < 64 {
            return errors.New("command data too short")
        }

        err = self.ProcessParse(buf)
        if err != nil {
            return err
        }
    }
    return io.EOF
}

func (self *BinaryServerProtocol) ProcessParse(buf []byte) error {
    if len(buf) < 64 {
        return errors.New("command data too short")
    }

    mv := uint16(buf[0]) | uint16(buf[1])<<8
    if mv != 0x0156 {
        if mv&0xff != uint16(protocol.MAGIC) {
            command := protocol.NewCommand(buf)
            self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_MAGIC))
            return errors.New("Unknown Magic")
        }

        if (mv>>8)&0xff != uint16(protocol.VERSION) {
            command := protocol.NewCommand(buf)
            self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_VERSION))
            return errors.New("Unknown Version")
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

        lock_command.Timeout, lock_command.TimeoutFlag, lock_command.Expried, lock_command.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
        lock_command.Count, lock_command.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        err := db.Lock(self, lock_command)
        if err != nil {
            return err
        }
        return nil
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

        lock_command.Timeout, lock_command.TimeoutFlag, lock_command.Expried, lock_command.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
        lock_command.Count, lock_command.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])
        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, true)
        }
        err := db.UnLock(self, lock_command)
        if err != nil {
            return err
        }
        return nil
    default:
        var command protocol.ICommand
        switch command_type {
        case protocol.COMMAND_INIT:
            command = &protocol.InitCommand{}
        case protocol.COMMAND_STATE:
            command = &protocol.StateCommand{}
        case protocol.COMMAND_ADMIN:
            command = &protocol.AdminCommand{}
        default:
            command = protocol.NewCommand(buf)
        }
        err := command.Decode(buf)
        if err != nil {
            return err
        }
        err = self.ProcessCommad(command)
        if err != nil {
            return err
        }
    }
    return nil
}

func (self *BinaryServerProtocol) ProcessBuild(command protocol.ICommand) error {
    return self.Write(command)
}

func (self *BinaryServerProtocol) ProcessCommad(command protocol.ICommand) error {
    switch command.GetCommandType() {
    case protocol.COMMAND_LOCK:
        lock_command := command.(*protocol.LockCommand)
        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)

    case protocol.COMMAND_UNLOCK:
        lock_command := command.(*protocol.LockCommand)
        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, true)
        }
        return db.UnLock(self, lock_command)

    default:
        switch command.GetCommandType() {
        case protocol.COMMAND_INIT:
            init_command := command.(*protocol.InitCommand)
            if self.Init(init_command.ClientId) != nil {
                return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_ERROR, 0))
            }
            self.slock.glock.Lock()
            init_type := uint8(0)
            if _, ok := self.slock.streams[init_command.ClientId]; ok {
                init_type = 1
            }
            self.slock.streams[init_command.ClientId] = self
            self.slock.glock.Unlock()
            return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_SUCCED, init_type))

        case protocol.COMMAND_STATE:
            return self.slock.GetState(self, command.(*protocol.StateCommand))

        case protocol.COMMAND_ADMIN:
            admin_command := command.(*protocol.AdminCommand)
            err := self.Write(protocol.NewAdminResultCommand(admin_command, protocol.RESULT_SUCCED))
            if err != nil {
                return err
            }

            server_protocol := NewTextServerProtocol(self.slock, self.stream)
            err = server_protocol.Process()
            if err != nil {
                if err != io.EOF {
                    self.slock.Log().Errorf("Protocol Process Error: %v", err)
                }
            }
            server_protocol.UnInitLockCommand()
            server_protocol.closed = true
            return err

        default:
            return self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_COMMAND))
        }
    }
}

func (self *BinaryServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
    db := self.slock.dbs[lock_command.DbId]

    if lock_command.CommandType == protocol.COMMAND_LOCK {
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)

    }

    if db == nil {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, true)
    }
    return db.UnLock(self, lock_command)
}

func (self *BinaryServerProtocol) ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, use_cached_command bool) error {
    server_protocol := self
    if self.closed {
        if !self.inited {
            return errors.New("Protocol Closed")
        }

        self.slock.glock.Lock()
        if sp, ok := self.slock.streams[self.client_id]; ok {
            self.slock.glock.Unlock()
            server_protocol = sp.(*BinaryServerProtocol)
        } else {
            self.slock.glock.Unlock()
            return errors.New("Protocol Closed")
        }
    }

    if use_cached_command {
        buf := server_protocol.wbuf
        if len(buf) < 64 {
            return errors.New("buf too short")
        }

        buf[2] = byte(command.CommandType)

        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(command.RequestId[0]), byte(command.RequestId[0] >> 8), byte(command.RequestId[0] >> 16), byte(command.RequestId[0] >> 24), byte(command.RequestId[0] >> 32), byte(command.RequestId[0] >> 40), byte(command.RequestId[0] >> 48), byte(command.RequestId[0] >> 56)
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(command.RequestId[1]), byte(command.RequestId[1] >> 8), byte(command.RequestId[1] >> 16), byte(command.RequestId[1] >> 24), byte(command.RequestId[1] >> 32), byte(command.RequestId[1] >> 40), byte(command.RequestId[1] >> 48), byte(command.RequestId[1] >> 56)

        buf[19], buf[20], buf[21] = result, 0x00, byte(command.DbId)

        buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29] = byte(command.LockId[0]), byte(command.LockId[0] >> 8), byte(command.LockId[0] >> 16), byte(command.LockId[0] >> 24), byte(command.LockId[0] >> 32), byte(command.LockId[0] >> 40), byte(command.LockId[0] >> 48), byte(command.LockId[0] >> 56)
        buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] = byte(command.LockId[1]), byte(command.LockId[1] >> 8), byte(command.LockId[1] >> 16), byte(command.LockId[1] >> 24), byte(command.LockId[1] >> 32), byte(command.LockId[1] >> 40), byte(command.LockId[1] >> 48), byte(command.LockId[1] >> 56)

        buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45] = byte(command.LockKey[0]), byte(command.LockKey[0] >> 8), byte(command.LockKey[0] >> 16), byte(command.LockKey[0] >> 24), byte(command.LockKey[0] >> 32), byte(command.LockKey[0] >> 40), byte(command.LockKey[0] >> 48), byte(command.LockKey[0] >> 56)
        buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] = byte(command.LockKey[1]), byte(command.LockKey[1] >> 8), byte(command.LockKey[1] >> 16), byte(command.LockKey[1] >> 24), byte(command.LockKey[1] >> 32), byte(command.LockKey[1] >> 40), byte(command.LockKey[1] >> 48), byte(command.LockKey[1] >> 56)

        buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(lcount), byte(lcount >> 8), byte(command.Count), byte(command.Count >> 8), byte(command.Rcount), 0x00, 0x00, 0x00
        buf[62], buf[63] = 0x00, 0x00

        return server_protocol.stream.WriteBytes(buf)
    }

    free_result_command_lock := server_protocol.free_result_command_lock
    free_result_command_lock.Lock()
    buf := server_protocol.owbuf

    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[2] = byte(command.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(command.RequestId[0]), byte(command.RequestId[0] >> 8), byte(command.RequestId[0] >> 16), byte(command.RequestId[0] >> 24), byte(command.RequestId[0] >> 32), byte(command.RequestId[0] >> 40), byte(command.RequestId[0] >> 48), byte(command.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(command.RequestId[1]), byte(command.RequestId[1] >> 8), byte(command.RequestId[1] >> 16), byte(command.RequestId[1] >> 24), byte(command.RequestId[1] >> 32), byte(command.RequestId[1] >> 40), byte(command.RequestId[1] >> 48), byte(command.RequestId[1] >> 56)

    buf[19], buf[20], buf[21] = result, 0x00, byte(command.DbId)

    buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29] = byte(command.LockId[0]), byte(command.LockId[0] >> 8), byte(command.LockId[0] >> 16), byte(command.LockId[0] >> 24), byte(command.LockId[0] >> 32), byte(command.LockId[0] >> 40), byte(command.LockId[0] >> 48), byte(command.LockId[0] >> 56)
    buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] = byte(command.LockId[1]), byte(command.LockId[1] >> 8), byte(command.LockId[1] >> 16), byte(command.LockId[1] >> 24), byte(command.LockId[1] >> 32), byte(command.LockId[1] >> 40), byte(command.LockId[1] >> 48), byte(command.LockId[1] >> 56)

    buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45] = byte(command.LockKey[0]), byte(command.LockKey[0] >> 8), byte(command.LockKey[0] >> 16), byte(command.LockKey[0] >> 24), byte(command.LockKey[0] >> 32), byte(command.LockKey[0] >> 40), byte(command.LockKey[0] >> 48), byte(command.LockKey[0] >> 56)
    buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] = byte(command.LockKey[1]), byte(command.LockKey[1] >> 8), byte(command.LockKey[1] >> 16), byte(command.LockKey[1] >> 24), byte(command.LockKey[1] >> 32), byte(command.LockKey[1] >> 40), byte(command.LockKey[1] >> 48), byte(command.LockKey[1] >> 56)

    buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(lcount), byte(lcount >> 8), byte(command.Count), byte(command.Count >> 8), byte(command.Rcount), 0x00, 0x00, 0x00
    buf[62], buf[63] = 0x00, 0x00

    err := server_protocol.stream.WriteBytes(buf)
    free_result_command_lock.Unlock()
    return err
}

func (self *BinaryServerProtocol) GetStream() *Stream {
    return self.stream
}


func (self *BinaryServerProtocol) RemoteAddr() net.Addr {
    if self.stream == nil {
        return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
    }
    return self.stream.RemoteAddr()
}

func (self *BinaryServerProtocol) InitLockCommand() {
    if self.slock.free_lock_command_count > 64 {
        self.slock.free_lock_command_lock.Lock()
        if self.slock.free_lock_command_count > 64 {
            for i := 0; i < 64; i++ {
                lock_command := self.slock.free_lock_commands.PopRight()
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
}

func (self *BinaryServerProtocol) UnInitLockCommand() {
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
}

func (self *BinaryServerProtocol) GetLockCommand() *protocol.LockCommand {
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
    return lock_command
}

func (self *BinaryServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
    return self.free_commands.Push(command)
}

type TextServerProtocolParser struct {
    buf         []byte
    buf_index   int
    buf_len     int
    stage       uint8
    args        []string
    args_count  int
    carg        []byte
    carg_len    int
}

func (self *TextServerProtocolParser) Parse() error {
    for ; self.buf_index < self.buf_len; {
        switch self.stage {
        case 0:
            if self.buf[self.buf_index] != '*' {
                return errors.New("Command first byte must by *")
            }
            self.buf_index++
            self.stage = 1
        case 1:
            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Command parse args count error")
                    }

                    args_count, err := strconv.Atoi(string(self.carg))
                    if err != nil {
                        return err
                    }
                    self.args_count = args_count
                    self.carg = self.carg[:0]
                    self.buf_index++
                    self.stage = 2
                    break
                } else if self.buf[self.buf_index] != '\r' {
                    self.carg = append(self.carg, self.buf[self.buf_index])
                }
            }

            if self.stage == 1 {
                return nil
            }
        case 2:
            if self.buf[self.buf_index] != '$' {
                return errors.New("Command first byte must by $")
            }
            self.buf_index++
            self.stage = 3
        case 3:
            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Command parse arg len error")
                    }

                    carg_len, err := strconv.Atoi(string(self.carg))
                    if err != nil {
                        return errors.New("Command parse args count error")
                    }
                    self.carg_len = carg_len
                    self.carg = self.carg[:0]
                    self.buf_index++
                    self.stage = 4
                    break
                } else if self.buf[self.buf_index] != '\r' {
                    self.carg = append(self.carg, self.buf[self.buf_index])
                }
            }

            if self.stage == 3 {
                return nil
            }
        case 4:
            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if len(self.carg) < self.carg_len {
                    self.carg = append(self.carg, self.buf[self.buf_index])
                } else {
                    if self.buf[self.buf_index] == '\n' {
                        if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                            return errors.New("Command parse arg error")
                        }

                        self.args = append(self.args, string(self.carg))
                        self.carg = self.carg[:0]
                        self.carg_len = 0
                        self.buf_index++
                        if len(self.args) < self.args_count {
                            self.stage = 2
                        } else {
                            self.stage = 0
                            return nil
                        }
                        break
                    }
                }
            }

            if self.stage == 4 {
                return nil
            }
        }
    }
    return nil
}

func (self *TextServerProtocolParser) Build(is_success bool, err_msg string, results []string) []byte {
    if !is_success {
        return []byte(fmt.Sprintf("-ERR %s\r\n", err_msg))
    }

    if results == nil || len(results) == 0 {
        return []byte(fmt.Sprintf("+%s\r\n", err_msg))
    }

    buf := make([]byte, 0)
    if len(results) == 1 {
        buf = append(buf, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(results[0]), results[0]))...)
        return buf
    }

    buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(results)))...)
    for _, result := range results {
        buf = append(buf, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(result), result))...)
    }
    return buf
}

type TextServerProtocolCommandHandler func(*TextServerProtocol, []string) error

type TextServerProtocol struct {
    slock                       *SLock
    stream                      *Stream
    free_commands               *LockCommandQueue
    free_command_result         *protocol.LockResultCommand
    free_result_command_lock    *sync.Mutex
    parser                      *TextServerProtocolParser
    handlers                    map[string]TextServerProtocolCommandHandler
    lock_waiter                 chan *protocol.LockResultCommand
    lock_request_id             [2]uint64
    lock_id                     [2]uint64
    db_id                       uint8
    closed                      bool
}

func NewTextServerProtocol(slock *SLock, stream *Stream) *TextServerProtocol {
    parser := &TextServerProtocolParser{make([]byte, 4096), 0, 0, 0,make([]string, 0), 0, make([]byte, 0), 0}
    server_protocol := &TextServerProtocol{slock, stream, NewLockCommandQueue(4, 16, FREE_COMMAND_QUEUE_INIT_SIZE),
        nil, &sync.Mutex{}, parser, make(map[string]TextServerProtocolCommandHandler, 64),
        make(chan *protocol.LockResultCommand, 1),  [2]uint64{0, 0}, [2]uint64{0, 0}, 0, false}
    server_protocol.InitLockCommand()

    server_protocol.handlers["SELECT"] = server_protocol.CommandHandlerSelectDB
    server_protocol.handlers["LOCK"] = server_protocol.CommandHandlerLock
    server_protocol.handlers["UNLOCK"] = server_protocol.CommandHandlerUnlock
    for name, handler := range slock.GetAdmin().GetHandlers() {
        server_protocol.handlers[name] = handler
    }
    return server_protocol
}

func (self *TextServerProtocol) Init(client_id [2]uint64) error{
    return nil
}

func (self *TextServerProtocol) Close() (error) {
    if self.stream != nil {
        if self.stream.Close() != nil {
            self.slock.Log().Errorf("connection close error: %s", self.RemoteAddr().String())
        } else {
            self.slock.Log().Infof("connection close %s", self.RemoteAddr().String())
        }
    }

    self.UnInitLockCommand()
    self.closed = true
    return nil
}

func (self *TextServerProtocol) Read() (protocol.CommandDecode, error) {
    for ; !self.closed; {
        if self.parser.buf_index == self.parser.buf_len {
            n, err := self.stream.Read(self.parser.buf)
            if err != nil {
                return nil, err
            }

            self.parser.buf_len = n
            self.parser.buf_index = 0
        }

        err := self.parser.Parse()
        if err != nil {
            return nil, err
        }

        if self.parser.args_count > 0 && self.parser.args_count == len(self.parser.args) {
            command_name := strings.ToUpper(self.parser.args[0])
            if command_name == "LOCK" || command_name == "UNLOCK" {
                if len(self.parser.args) < 5 {
                    return nil, errors.New("Command Parse Error")
                }

                command, err := self.ArgsToLockComand(self.parser.args)
                self.parser.args = self.parser.args[:0]
                self.parser.args_count = 0
                return command, err
            }
            self.parser.args = self.parser.args[:0]
            self.parser.args_count = 0
        }
    }
    return nil, errors.New("Unknown Command")
}

func (self *TextServerProtocol) Write(result protocol.CommandEncode) error {
    if self.closed {
        return errors.New("Protocol Closed")
    }

    switch result.(type) {
    case *protocol.LockResultCommand:
        lock_result_command := result.(*protocol.LockResultCommand)
        lock_results := []string{
            fmt.Sprintf("%d", lock_result_command.Result),
            "LOCK_ID",
            fmt.Sprintf("%x", self.ConvertUint642ToByte16(lock_result_command.LockId)),
            "LCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lcount),
            "COUNT",
            fmt.Sprintf("%d", lock_result_command.Count),
            "RCOUNT",
            fmt.Sprintf("%d", lock_result_command.Rcount),
        }
        return self.stream.WriteAllBytes(self.parser.Build(true, "", lock_results))
    }
    return self.stream.WriteAllBytes(self.parser.Build(false, "Unknwon Command", nil))
}

func (self *TextServerProtocol) Process() error {
    for ; !self.closed; {
        if self.parser.buf_index == self.parser.buf_len {
            n, err := self.stream.Read(self.parser.buf)
            if err != nil {
                return err
            }

            self.parser.buf_len = n
            self.parser.buf_index = 0
        }

        err := self.parser.Parse()
        if err != nil {
            return err
        }

        if self.parser.args_count > 0 && self.parser.args_count == len(self.parser.args) {
            var err error

            command_name := strings.ToUpper(self.parser.args[0])
            if command_handler, ok := self.handlers[command_name]; ok {
                err = command_handler(self, self.parser.args)
            } else {
                err = self.CommandHandlerUnknownCommand(self, self.parser.args)
            }

            if err != nil {
                return err
            }

            self.parser.args = self.parser.args[:0]
            self.parser.args_count = 0
        }
    }
    return nil
}

func (self *TextServerProtocol) ProcessParse(buf []byte) error {
    copy(self.parser.buf[self.parser.buf_index:], buf)
    self.parser.buf_len += len(buf)
    err := self.parser.Parse()
    if err != nil {
        return err
    }

    if self.parser.args_count > 0 && self.parser.args_count == len(self.parser.args) {
        var err error

        command_name := strings.ToUpper(self.parser.args[0])
        if command_handler, ok := self.handlers[command_name]; ok {
            err = command_handler(self, self.parser.args)
        } else {
            err = self.CommandHandlerUnknownCommand(self, self.parser.args)
        }

        if err != nil {
            return err
        }

        self.parser.args = self.parser.args[:0]
        self.parser.args_count = 0
    }
    return nil
}

func (self *TextServerProtocol) ProcessBuild(command protocol.ICommand) error {
    switch command.GetCommandType() {
    case protocol.COMMAND_LOCK:
        lock_result_command := command.(*protocol.LockResultCommand)
        lock_results := []string{
            fmt.Sprintf("%d", lock_result_command.Result),
            "LOCK_ID",
            fmt.Sprintf("%x", self.ConvertUint642ToByte16(lock_result_command.LockId)),
            "LCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lcount),
            "COUNT",
            fmt.Sprintf("%d", lock_result_command.Count),
            "RCOUNT",
            fmt.Sprintf("%d", lock_result_command.Rcount),
        }
        return self.stream.WriteAllBytes(self.parser.Build(true, "", lock_results))
    case protocol.COMMAND_UNLOCK:
        lock_result_command := command.(*protocol.LockResultCommand)
        lock_results := []string{
            fmt.Sprintf("%d", lock_result_command.Result),
            "LOCK_ID",
            fmt.Sprintf("%x", self.ConvertUint642ToByte16(lock_result_command.LockId)),
            "LCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lcount),
            "COUNT",
            fmt.Sprintf("%d", lock_result_command.Count),
            "RCOUNT",
            fmt.Sprintf("%d", lock_result_command.Rcount),
        }
        return self.stream.WriteAllBytes(self.parser.Build(true, "", lock_results))
    }
    return self.stream.WriteAllBytes(self.parser.Build(false, "Unknwon Command", nil))
}

func (self *TextServerProtocol) ProcessCommad(command protocol.ICommand) error {
    switch command.GetCommandType() {
    case protocol.COMMAND_LOCK:
        lock_command := command.(*protocol.LockCommand)
        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)

    case protocol.COMMAND_UNLOCK:
        lock_command := command.(*protocol.LockCommand)
        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, true)
        }
        return db.UnLock(self, lock_command)

    default:
        switch command.GetCommandType() {
        case protocol.COMMAND_INIT:
            init_command := command.(*protocol.InitCommand)
            if self.Init(init_command.ClientId) != nil {
                return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_ERROR, 0))
            }
            self.slock.glock.Lock()
            init_type := uint8(0)
            if _, ok := self.slock.streams[init_command.ClientId]; ok {
                init_type = 1
            }
            self.slock.streams[init_command.ClientId] = self
            self.slock.glock.Unlock()
            return self.Write(protocol.NewInitResultCommand(init_command, protocol.RESULT_SUCCED, init_type))

        case protocol.COMMAND_STATE:
            return self.slock.GetState(self, command.(*protocol.StateCommand))

        case protocol.COMMAND_ADMIN:
            admin_command := command.(*protocol.AdminCommand)
            err := self.Write(protocol.NewAdminResultCommand(admin_command, protocol.RESULT_SUCCED))
            if err != nil {
                return err
            }

            server_protocol := NewTextServerProtocol(self.slock, self.stream)
            err = server_protocol.Process()
            if err != nil {
                if err != io.EOF {
                    self.slock.Log().Errorf("Protocol Process Error: %v", err)
                }
            }
            server_protocol.UnInitLockCommand()
            server_protocol.closed = true
            return err

        default:
            return self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_COMMAND))
        }
    }
}

func (self *TextServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
    db := self.slock.dbs[lock_command.DbId]

    if lock_command.CommandType == protocol.COMMAND_LOCK {
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)

    }

    if db == nil {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, true)
    }
    return db.UnLock(self, lock_command)
}

func (self *TextServerProtocol) ProcessLockResultCommand(lock_command *protocol.LockCommand, result uint8, lcount uint16, use_cached_command bool) error {
    if lock_command.RequestId != self.lock_request_id {
        return nil
    }

    self.lock_request_id[0] = 0
    self.lock_request_id[1] = 0
    if self.free_command_result == nil {
        lock_result_commad := protocol.NewLockResultCommand(lock_command, result, 0, lcount, lock_command.Count, lock_command.Rcount)
        self.lock_waiter <- lock_result_commad
        return nil
    }

    lock_result_commad := self.free_command_result
    lock_result_commad.CommandType = lock_command.CommandType
    lock_result_commad.RequestId = lock_command.RequestId
    lock_result_commad.Result = result
    lock_result_commad.Flag = 0
    lock_result_commad.DbId = lock_command.DbId
    lock_result_commad.LockId = lock_command.LockId
    lock_result_commad.LockKey = lock_command.LockKey
    lock_result_commad.Lcount = lcount
    lock_result_commad.Count = lock_command.Count
    lock_result_commad.Rcount = lock_command.Rcount
    self.free_command_result = nil
    self.lock_waiter <- lock_result_commad
    return nil
}

func (self *TextServerProtocol) GetStream() *Stream {
    return self.stream
}

func (self *TextServerProtocol) RemoteAddr() net.Addr {
    if self.stream == nil {
        return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
    }
    return self.stream.RemoteAddr()
}

func (self *TextServerProtocol) InitLockCommand() {
    if self.slock.free_lock_command_count > 1 {
        self.slock.free_lock_command_lock.Lock()
        if self.slock.free_lock_command_count > 1 {
            lock_command := self.slock.free_lock_commands.PopRight()
            if lock_command != nil {
                self.slock.free_lock_command_count--
                self.slock.free_lock_command_lock.Unlock()
                return
            }
        }
        self.slock.free_lock_command_lock.Lock()
    }
    self.free_commands.Push(&protocol.LockCommand{})
}

func (self *TextServerProtocol) UnInitLockCommand() {
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
}

func (self *TextServerProtocol) GetLockCommand() *protocol.LockCommand {
    lock_command := self.free_commands.PopRight()
    if lock_command == nil {
        self.slock.free_lock_command_lock.Lock()
        if self.slock.free_lock_command_count > 1 {
            lock_command := self.slock.free_lock_commands.PopRight()
            if lock_command != nil {
                self.slock.free_lock_command_count--
                self.slock.free_lock_command_lock.Unlock()
                return lock_command
            }
        }
        self.slock.free_lock_command_lock.Unlock()
        return &protocol.LockCommand{}
    }
    return lock_command
}

func (self *TextServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
    return self.free_commands.Push(command)
}

func (self *TextServerProtocol) ArgsToLockComandParseId(arg_id string, lock_id *[2]uint64) error {
    block_id := []byte(arg_id)
    arg_len := len(block_id)

    if arg_len == 32 {
        v, err := hex.DecodeString(arg_id)
        if err != nil {
            return errors.New("Command Parse DecodeHex Error")
        }
        block_id = v
    } else if arg_len > 16 {
        block_id = make([]byte, 0)
        for _, v := range md5.Sum(block_id) {
            block_id = append(block_id, v)
        }
    } else if len(block_id) < 16 {
        block_id = make([]byte, 0)
        for i := 0; i < 16 - len(arg_id); i++ {
            block_id = append(block_id, 0)
        }
        for _, v := range arg_id {
            block_id = append(block_id, byte(v))
        }
    }

    lock_id[1] = uint64(block_id[7]) | uint64(block_id[6])<<8 | uint64(block_id[5])<<16 | uint64(block_id[4])<<24 | uint64(block_id[3])<<32 | uint64(block_id[2])<<40 | uint64(block_id[1])<<48 | uint64(block_id[0])<<56
    lock_id[0] = uint64(block_id[15]) | uint64(block_id[14])<<8 | uint64(block_id[13])<<16 | uint64(block_id[12])<<24 | uint64(block_id[11])<<32 | uint64(block_id[10])<<40 | uint64(block_id[9])<<48 | uint64(block_id[8])<<56
    return nil
}

func (self *TextServerProtocol) ArgsToLockComand(args []string) (*protocol.LockCommand, error) {
    if len(args) < 2 {
        return nil, errors.New("Command Parse Len Error")
    }

    command_name := strings.ToUpper(args[0])
    command := self.GetLockCommand()
    command.Magic = protocol.MAGIC
    command.Version = protocol.VERSION
    if command_name == "LOCK" {
        command.CommandType = protocol.COMMAND_LOCK
    } else {
        command.CommandType = protocol.COMMAND_UNLOCK
    }
    command.RequestId = self.GetRequestId()

    err := self.ArgsToLockComandParseId(args[1], &command.LockKey)
    if err != nil {
        return nil, err
    }

    if len(args) % 2 != 0 {
        return nil, errors.New("Command Parse Len Error")
    }

    kv_args := make(map[string]string, 8)
    for i := 2; i < len(args); i+= 2 {
        kv_args[strings.ToUpper(args[i])] = args[i + 1]
    }

    if v, ok := kv_args["LOCK_ID"]; ok {
        err := self.ArgsToLockComandParseId(v, &command.LockId)
        if err != nil {
            return nil, err
        }
    } else {
        if command_name == "LOCK" {
            command.LockId = command.RequestId
        } else {
            command.LockId = self.lock_id
        }
    }

    if v, ok := kv_args["FLAG"]; ok {
        flag, err := strconv.Atoi(v)
        if err != nil {
            return nil, errors.New("Command Parse FLAG Error")
        }
        command.Flag = uint8(flag)
    }
    command.DbId = self.db_id

    if v, ok := kv_args["TIMEOUT"]; ok {
        timeout, err := strconv.Atoi(v)
        if err != nil {
            return nil, errors.New("Command Parse TIMEOUT Error")
        }
        command.Timeout = uint16(timeout & 0xffff)
        command.TimeoutFlag = uint16(timeout >> 23)
    }

    if v, ok := kv_args["EXPRIED"]; ok {
        expried, err := strconv.Atoi(v)
        if err != nil {
            return nil, errors.New("Command Parse EXPRIED Error")
        }
        command.Expried = uint16(expried & 0xffff)
        command.ExpriedFlag = uint16(expried >> 23)
    }

    if v, ok := kv_args["COUNT"]; ok {
        count, err := strconv.Atoi(v)
        if err != nil {
            return nil, errors.New("Command Parse COUNT Error")
        }
        command.Count = uint16(count)
    }

    if v, ok := kv_args["RCOUNT"]; ok {
        rcount, err := strconv.Atoi(v)
        if err != nil {
            return nil, errors.New("Command Parse RCOUNT Error")
        }
        command.Rcount = uint8(rcount)
    }
    return command, nil
}

func (self *TextServerProtocol) CommandHandlerUnknownCommand(server_protocol *TextServerProtocol, args []string) error {
    return self.stream.WriteAllBytes(self.parser.Build(false, "Unknown Command", nil))
}

func (self *TextServerProtocol) CommandHandlerSelectDB(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 2 {
        return self.stream.WriteAllBytes(self.parser.Build(false, "Command Parse Len Error", nil))
    }

    db_id, err := strconv.Atoi(args[1])
    if err != nil {
        return self.stream.WriteAllBytes(self.parser.Build(false, "Command Parse Error", nil))
    }
    self.db_id = uint8(db_id)
    return self.stream.WriteAllBytes(self.parser.Build(true, "OK", nil))
}

func (self *TextServerProtocol) CommandHandlerLock(server_protocol *TextServerProtocol, args []string) error {
    lock_command, err := self.ArgsToLockComand(args)
    if err != nil {
        return self.stream.WriteAllBytes(self.parser.Build(false, err.Error(), nil))
    }

    db := self.slock.dbs[lock_command.DbId]
    if db == nil {
        db = self.slock.GetOrNewDB(lock_command.DbId)
    }
    self.lock_request_id = lock_command.RequestId
    err = db.Lock(self, lock_command)
    if err != nil {
        return self.stream.WriteAllBytes(self.parser.Build(false, "Lock Error", nil))
    }
    lock_command_result := <- self.lock_waiter
    if lock_command_result.Result == 0 {
        self.lock_id = lock_command.LockId
    }
    lock_results := []string{
        fmt.Sprintf("%d", lock_command_result.Result),
        "LOCK_ID",
        fmt.Sprintf("%x", self.ConvertUint642ToByte16(lock_command_result.LockId)),
        "LCOUNT",
        fmt.Sprintf("%d", lock_command_result.Lcount),
        "COUNT",
        fmt.Sprintf("%d", lock_command_result.Count),
        "RCOUNT",
        fmt.Sprintf("%d", lock_command_result.Rcount),
    }
    self.free_command_result = lock_command_result
    return self.stream.WriteAllBytes(self.parser.Build(true, "", lock_results))
}

func (self *TextServerProtocol) CommandHandlerUnlock(server_protocol *TextServerProtocol, args []string) error {
    lock_command, err := self.ArgsToLockComand(args)
    if err != nil {
        return self.stream.WriteAllBytes(self.parser.Build(false, err.Error(), nil))
    }
    db := self.slock.dbs[lock_command.DbId]
    if db == nil {
        return self.stream.WriteAllBytes(self.parser.Build(false, "Uknown DB Error", nil))
    }
    self.lock_request_id = lock_command.RequestId
    err = db.UnLock(self, lock_command)
    if err != nil {
        return self.stream.WriteAllBytes(self.parser.Build(false, "UnLock Error", nil))
    }
    lock_command_result := <- self.lock_waiter
    if lock_command_result.Result == 0 {
        self.lock_id[0] = 0
        self.lock_id[1] = 0
    }
    lock_results := []string{
        fmt.Sprintf("%d", lock_command_result.Result),
        "LOCK_ID",
        fmt.Sprintf("%x", self.ConvertUint642ToByte16(lock_command_result.LockId)),
        "LCOUNT",
        fmt.Sprintf("%d", lock_command_result.Lcount),
        "COUNT",
        fmt.Sprintf("%d", lock_command_result.Count),
        "RCOUNT",
        fmt.Sprintf("%d", lock_command_result.Rcount),
    }
    self.free_command_result = lock_command_result
    return self.stream.WriteAllBytes(self.parser.Build(true, "", lock_results))
}

func (self *TextServerProtocol) GetRequestId() [2]uint64 {
    request_id := [2]uint64{}
    request_id[0] = (uint64(time.Now().Unix()) & 0xffffffff)<<32 | uint64(LETTERS[rand.Intn(52)])<<24 | uint64(LETTERS[rand.Intn(52)])<<16 | uint64(LETTERS[rand.Intn(52)])<<8 | uint64(LETTERS[rand.Intn(52)])
    request_id[1] = atomic.AddUint64(&request_id_index, 1)
    return request_id
}

func (self *TextServerProtocol) ConvertUint642ToByte16(uint642 [2]uint64) [16]byte {
    return [16]byte{
        byte(uint642[1] >> 56), byte(uint642[1] >> 48), byte(uint642[1] >> 40), byte(uint642[1] >> 32),
        byte(uint642[1] >> 24), byte(uint642[1] >> 16), byte(uint642[1] >> 8), byte(uint642[1]),
        byte(uint642[0] >> 56), byte(uint642[0] >> 48), byte(uint642[0] >> 40), byte(uint642[0] >> 32),
        byte(uint642[0] >> 24), byte(uint642[0] >> 16), byte(uint642[0] >> 8), byte(uint642[0]),
    }
}