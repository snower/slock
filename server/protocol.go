package server

import (
    "crypto/md5"
    "encoding/hex"
    "errors"
    "fmt"
    "github.com/snower/slock/protocol"
    "io"
    "math/rand"
    "net"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

type ServerProtocol interface {
    Init(client_id [16]byte) error
    Lock()
    Unlock()
    Read() (protocol.CommandDecode, error)
    Write(protocol.CommandEncode) (error)
    Process() error
    ProcessParse(buf []byte) error
    ProcessBuild(command protocol.ICommand) error
    ProcessCommad(command protocol.ICommand) error
    ProcessLockCommand(command *protocol.LockCommand) error
    ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error
    ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error
    Close() (error)
    GetStream() *Stream
    RemoteAddr() net.Addr
    GetLockCommand() *protocol.LockCommand
    FreeLockCommand(command *protocol.LockCommand) error
    FreeLockCommandLocked(command *protocol.LockCommand) error
}

type MemWaiterServerProtocol struct {
    slock                       *SLock
    glock                       *sync.Mutex
    free_commands               *LockCommandQueue
    waiters                     map[[16]byte]chan *protocol.LockResultCommand
    closed                      bool
}

func NewMemWaiterServerProtocol(slock *SLock) *MemWaiterServerProtocol {
    mem_waiter_server_protocol := &MemWaiterServerProtocol{slock, &sync.Mutex{}, NewLockCommandQueue(4, 64, FREE_COMMAND_QUEUE_INIT_SIZE),
        make(map[[16]byte]chan *protocol.LockResultCommand, 4096), false}
    mem_waiter_server_protocol.InitLockCommand()
    return mem_waiter_server_protocol
}

func (self *MemWaiterServerProtocol) Init(client_id [16]byte) error {
    return nil
}

func (self *MemWaiterServerProtocol) Lock() {
    self.glock.Lock()
}

func (self *MemWaiterServerProtocol) Unlock() {
    self.glock.Unlock()
}

func (self *MemWaiterServerProtocol) Read() (protocol.CommandDecode, error) {
    return nil, errors.New("read error")
}

func (self *MemWaiterServerProtocol) Write(protocol.CommandEncode) (error) {
    return errors.New("write error")
}

func (self *MemWaiterServerProtocol) Process() error {
    return nil
}

func (self *MemWaiterServerProtocol) ProcessParse(buf []byte) error {
    return nil
}

func (self *MemWaiterServerProtocol) ProcessBuild(command protocol.ICommand) error {
    return nil
}

func (self *MemWaiterServerProtocol) ProcessCommad(command protocol.ICommand) error {
    return nil
}

func (self *MemWaiterServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
    db := self.slock.dbs[lock_command.DbId]
    if lock_command.CommandType == protocol.COMMAND_LOCK {
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)
    }

    if db == nil {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
    }
    return db.UnLock(self, lock_command)
}

func (self *MemWaiterServerProtocol)ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
    self.glock.Lock()
    if waiter, ok := self.waiters[command.RequestId]; ok {
        waiter <- protocol.NewLockResultCommand(command, result, 0, lcount, command.Count, lrcount, command.Rcount)
        delete(self.waiters, command.RequestId)
    }
    self.glock.Unlock()
    return nil
}

func (self *MemWaiterServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
    return self.ProcessLockResultCommand(command, result, lcount, lrcount)
}

func (self *MemWaiterServerProtocol) Close() (error) {
    self.UnInitLockCommand()
    self.closed = true
    return nil
}

func (self *MemWaiterServerProtocol) GetStream() *Stream {
    return nil
}

func (self *MemWaiterServerProtocol)RemoteAddr() net.Addr {
    return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
}

func (self *MemWaiterServerProtocol) InitLockCommand() {
    self.slock.free_lock_command_lock.Lock()
    lock_command := self.slock.free_lock_commands.PopRight()
    if lock_command != nil {
        self.slock.free_lock_command_count--
        self.free_commands.Push(lock_command)
    } else {
        self.free_commands.Push(&protocol.LockCommand{})
    }
    self.slock.free_lock_command_lock.Unlock()
}

func (self *MemWaiterServerProtocol) UnInitLockCommand() {
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

func (self *MemWaiterServerProtocol) GetLockCommand() *protocol.LockCommand {
    lock_command := self.free_commands.PopRight()
    if lock_command == nil {
        self.slock.free_lock_command_lock.Lock()
        lock_command := self.slock.free_lock_commands.PopRight()
        if lock_command != nil {
            self.slock.free_lock_command_count--
            self.slock.free_lock_command_lock.Unlock()
            return lock_command
        }
        self.slock.free_lock_command_lock.Unlock()
        return &protocol.LockCommand{}
    }
    return lock_command
}

func (self *MemWaiterServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
    self.glock.Lock()
    self.free_commands.Push(command)
    self.glock.Unlock()
    return nil
}

func (self *MemWaiterServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
    self.glock.Lock()
    if self.closed {
        self.slock.free_lock_command_lock.Lock()
        self.slock.free_lock_commands.Push(command)
        self.slock.free_lock_command_count++
        self.slock.free_lock_command_lock.Unlock()
    } else {
        self.free_commands.Push(command)
    }
    self.glock.Unlock()
    return nil
}

func (self *MemWaiterServerProtocol) AddWaiter(command *protocol.LockCommand, waiter chan *protocol.LockResultCommand) error {
    self.glock.Lock()
    if owaiter, ok := self.waiters[command.RequestId]; ok {
        owaiter <- nil
    }
    self.waiters[command.RequestId] = waiter
    self.glock.Unlock()
    return nil
}

func (self *MemWaiterServerProtocol) RemoveWaiter(command *protocol.LockCommand) error {
    self.glock.Lock()
    if _, ok := self.waiters[command.RequestId]; ok {
        delete(self.waiters, command.RequestId)
    }
    self.glock.Unlock()
    return nil
}

type BinaryServerProtocol struct {
    slock                       *SLock
    glock                       *sync.Mutex
    stream                      *Stream
    client_id                   [16]byte
    free_commands               *LockCommandQueue
    locked_free_commands        *LockCommandQueue
    rbuf                        []byte
    wbuf                        []byte
    total_command_count         uint64
    inited                      bool
    closed                      bool
}

func NewBinaryServerProtocol(slock *SLock, stream *Stream) *BinaryServerProtocol {
    wbuf := make([]byte, 64)
    wbuf[0] = byte(protocol.MAGIC)
    wbuf[1] = byte(protocol.VERSION)

    server_protocol := &BinaryServerProtocol{slock, &sync.Mutex{}, stream, [16]byte{}, NewLockCommandQueue(4, 64, FREE_COMMAND_QUEUE_INIT_SIZE),
        NewLockCommandQueue(4, 64, FREE_COMMAND_QUEUE_INIT_SIZE), make([]byte, 64), wbuf, 0, false, false}
    server_protocol.InitLockCommand()
    stream.protocol = server_protocol
    return server_protocol
}

func (self *BinaryServerProtocol) Init(client_id [16]byte) error {
    self.client_id = client_id
    self.inited = true
    return nil
}

func (self *BinaryServerProtocol) Close() error {
    self.glock.Lock()
    defer self.glock.Unlock()

    if self.closed {
        return nil
    }

    self.slock.glock.Lock()
    if self.inited {
        self.inited = false
        if sp, ok := self.slock.streams[self.client_id]; ok {
            if sp == self {
                delete(self.slock.streams, self.client_id)
            }
        }
    }
    self.slock.stats_total_command_count += self.total_command_count
    self.slock.glock.Unlock()

    if self.stream != nil {
        err := self.stream.Close()
        if err != nil {
            self.slock.Log().Errorf("Connection Close Error: %s %v", self.RemoteAddr().String(), err)
        }
        self.stream.protocol = nil
    }

    self.UnInitLockCommand()
    self.closed = true
    return nil
}

func (self *BinaryServerProtocol) Lock() {
    self.glock.Lock()
}

func (self *BinaryServerProtocol) Unlock() {
    self.glock.Unlock()
}

func (self *BinaryServerProtocol) Read() (protocol.CommandDecode, error) {
    if self.closed {
        return nil, errors.New("Protocol Closed")
    }

    buf := self.rbuf

    n, err := self.stream.Read(buf)
    if err != nil {
        return nil, err
    }

    if n < 64 {
        for ; n < 64; {
            nn, nerr := self.stream.Read(buf[n:])
            if nerr != nil {
                return nil, nerr
            }
            n += nn
        }
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
        case protocol.COMMAND_PING:
            ping_command := &protocol.PingCommand{}
            err := ping_command.Decode(buf)
            if err != nil {
                return nil, err
            }
            return ping_command, nil
        case protocol.COMMAND_QUIT:
            quit_command := &protocol.QuitCommand{}
            err := quit_command.Decode(buf)
            if err != nil {
                return nil, err
            }
            return quit_command, nil
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
        n, err := self.stream.conn.Read(buf)
        if err != nil {
            return err
        }

        if n < 64 {
            for ; n < 64; {
                nn, nerr := self.stream.conn.Read(buf[n:])
                if nerr != nil {
                    return nerr
                }
                n += nn
            }
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

    self.total_command_count++
    command_type := uint8(buf[2])
    switch command_type {
    case protocol.COMMAND_LOCK:
        lock_command := self.free_commands.PopRight()
        if lock_command == nil {
            lock_command = self.GetLockCommandLocked()
        }

        lock_command.CommandType = command_type

        lock_command.RequestId[0], lock_command.RequestId[1], lock_command.RequestId[2], lock_command.RequestId[3], lock_command.RequestId[4], lock_command.RequestId[5], lock_command.RequestId[6], lock_command.RequestId[7],
            lock_command.RequestId[8], lock_command.RequestId[9], lock_command.RequestId[10], lock_command.RequestId[11], lock_command.RequestId[12], lock_command.RequestId[13], lock_command.RequestId[14], lock_command.RequestId[15] =
            buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
            buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

        lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

        lock_command.LockId[0], lock_command.LockId[1], lock_command.LockId[2], lock_command.LockId[3], lock_command.LockId[4], lock_command.LockId[5], lock_command.LockId[6], lock_command.LockId[7],
            lock_command.LockId[8], lock_command.LockId[9], lock_command.LockId[10], lock_command.LockId[11], lock_command.LockId[12], lock_command.LockId[13], lock_command.LockId[14], lock_command.LockId[15] =
            buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
            buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

        lock_command.LockKey[0], lock_command.LockKey[1], lock_command.LockKey[2], lock_command.LockKey[3], lock_command.LockKey[4], lock_command.LockKey[5], lock_command.LockKey[6], lock_command.LockKey[7],
            lock_command.LockKey[8], lock_command.LockKey[9], lock_command.LockKey[10], lock_command.LockKey[11], lock_command.LockKey[12], lock_command.LockKey[13], lock_command.LockKey[14], lock_command.LockKey[15] =
            buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
            buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]
            
        lock_command.Timeout, lock_command.TimeoutFlag, lock_command.Expried, lock_command.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
        lock_command.Count, lock_command.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

        if self.slock.state != STATE_LEADER {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
        }

        if lock_command.DbId == 0xff {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
        }

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
            lock_command = self.GetLockCommandLocked()
        }

        lock_command.CommandType = command_type

        lock_command.RequestId[0], lock_command.RequestId[1], lock_command.RequestId[2], lock_command.RequestId[3], lock_command.RequestId[4], lock_command.RequestId[5], lock_command.RequestId[6], lock_command.RequestId[7],
            lock_command.RequestId[8], lock_command.RequestId[9], lock_command.RequestId[10], lock_command.RequestId[11], lock_command.RequestId[12], lock_command.RequestId[13], lock_command.RequestId[14], lock_command.RequestId[15] =
            buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
            buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

        lock_command.Flag, lock_command.DbId = uint8(buf[19]), uint8(buf[20])

        lock_command.LockId[0], lock_command.LockId[1], lock_command.LockId[2], lock_command.LockId[3], lock_command.LockId[4], lock_command.LockId[5], lock_command.LockId[6], lock_command.LockId[7],
            lock_command.LockId[8], lock_command.LockId[9], lock_command.LockId[10], lock_command.LockId[11], lock_command.LockId[12], lock_command.LockId[13], lock_command.LockId[14], lock_command.LockId[15] =
            buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
            buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

        lock_command.LockKey[0], lock_command.LockKey[1], lock_command.LockKey[2], lock_command.LockKey[3], lock_command.LockKey[4], lock_command.LockKey[5], lock_command.LockKey[6], lock_command.LockKey[7],
            lock_command.LockKey[8], lock_command.LockKey[9], lock_command.LockKey[10], lock_command.LockKey[11], lock_command.LockKey[12], lock_command.LockKey[13], lock_command.LockKey[14], lock_command.LockKey[15] =
            buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
            buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

        lock_command.Timeout, lock_command.TimeoutFlag, lock_command.Expried, lock_command.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
        lock_command.Count, lock_command.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

        if self.slock.state != STATE_LEADER {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
        }

        if lock_command.DbId == 0xff {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
        }

        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
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
        case protocol.COMMAND_PING:
            command = &protocol.PingCommand{}
        case protocol.COMMAND_QUIT:
            command = &protocol.QuitCommand{}
        default:
            command = &protocol.Command{}
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

        if self.slock.state != STATE_LEADER {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
        }

        if lock_command.DbId == 0xff {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
        }

        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)

    case protocol.COMMAND_UNLOCK:
        lock_command := command.(*protocol.LockCommand)

        if self.slock.state != STATE_LEADER {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
        }

        if lock_command.DbId == 0xff {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
        }

        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
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

            if self.stream != nil {
                self.stream.protocol = self
            }
            self.total_command_count += server_protocol.total_command_count
            server_protocol.UnInitLockCommand()
            server_protocol.closed = true
            return err

        case protocol.COMMAND_PING:
            ping_command := command.(*protocol.PingCommand)
            return self.Write(protocol.NewPingResultCommand(ping_command, protocol.RESULT_SUCCED))

        case protocol.COMMAND_QUIT:
            quit_command := command.(*protocol.QuitCommand)
            err := self.Write(protocol.NewQuitResultCommand(quit_command, protocol.RESULT_SUCCED))
            if err == nil {
                return io.EOF
            }
            return err

        default:
            return self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_COMMAND))
        }
    }
}

func (self *BinaryServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
    if self.slock.state != STATE_LEADER {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
    }

    if lock_command.DbId == 0xff {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
    }

    db := self.slock.dbs[lock_command.DbId]
    if lock_command.CommandType == protocol.COMMAND_LOCK {
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)
    }

    if db == nil {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
    }
    return db.UnLock(self, lock_command)
}

func (self *BinaryServerProtocol) ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
    if self.closed {
        if !self.inited {
            return errors.New("Protocol Closed")
        }

        self.slock.glock.Lock()
        if server_protocol, ok := self.slock.streams[self.client_id]; ok {
            self.slock.glock.Unlock()
            return server_protocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
        } else {
            self.slock.glock.Unlock()
            return errors.New("Protocol Closed")
        }
    }

    self.glock.Lock()
    buf := self.wbuf
    if len(buf) < 64 {
        self.glock.Unlock()
        return errors.New("buf too short")
    }

    buf[2] = byte(command.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
        command.RequestId[0], command.RequestId[1], command.RequestId[2], command.RequestId[3], command.RequestId[4], command.RequestId[5], command.RequestId[6], command.RequestId[7],
        command.RequestId[8], command.RequestId[9], command.RequestId[10], command.RequestId[11], command.RequestId[12], command.RequestId[13], command.RequestId[14], command.RequestId[15]

    buf[19], buf[20], buf[21] = result, 0x00, byte(command.DbId)

    buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
        buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] =
        command.LockId[0], command.LockId[1], command.LockId[2], command.LockId[3], command.LockId[4], command.LockId[5], command.LockId[6], command.LockId[7],
        command.LockId[8], command.LockId[9], command.LockId[10], command.LockId[11], command.LockId[12], command.LockId[13], command.LockId[14], command.LockId[15]

    buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45],
        buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] =
        command.LockKey[0], command.LockKey[1], command.LockKey[2], command.LockKey[3], command.LockKey[4], command.LockKey[5], command.LockKey[6], command.LockKey[7],
        command.LockKey[8], command.LockKey[9], command.LockKey[10], command.LockKey[11], command.LockKey[12], command.LockKey[13], command.LockKey[14], command.LockKey[15]

    buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(lcount), byte(lcount >> 8), byte(command.Count), byte(command.Count >> 8), byte(lrcount), byte(command.Rcount), 0x00, 0x00
    buf[62], buf[63] = 0x00, 0x00

    n, err := self.stream.conn.Write(buf)
    if err != nil {
        self.glock.Unlock()
        return err
    }

    if n < 64 {
        for ; n < 64; {
            nn, nerr := self.stream.conn.Write(buf[n:])
            if nerr != nil {
                self.glock.Unlock()
                return nerr
            }
            n += nn
        }
    }
    self.glock.Unlock()
    return nil
}

func (self *BinaryServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
    return self.ProcessLockResultCommand(command, result, lcount, lrcount)
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
    self.slock.free_lock_command_lock.Lock()
    for i := 0; i < 4; i++ {
        lock_command := self.slock.free_lock_commands.PopRight()
        if lock_command != nil {
            self.slock.free_lock_command_count--
            self.free_commands.Push(lock_command)
            continue
        }
        self.free_commands.Push(&protocol.LockCommand{})
    }
    self.slock.free_lock_command_lock.Unlock()
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

    for ;; {
        command := self.locked_free_commands.PopRight()
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
        return self.GetLockCommandLocked()
    }
    return lock_command
}

func (self *BinaryServerProtocol) GetLockCommandLocked() *protocol.LockCommand {
    self.glock.Lock()
    lock_command := self.locked_free_commands.PopRight()
    if lock_command != nil {
        for ;; {
            flock_command := self.locked_free_commands.PopRight()
            if flock_command == nil {
                break
            }
            self.free_commands.Push(flock_command)
        }
        self.glock.Unlock()
        return lock_command
    }
    self.glock.Unlock()

    self.slock.free_lock_command_lock.Lock()
    lock_command = self.slock.free_lock_commands.PopRight()
    if lock_command != nil {
        self.slock.free_lock_command_count--
        for i := 0; i < 8; i++ {
            flock_command := self.slock.free_lock_commands.PopRight()
            if flock_command == nil {
                break
            }
            self.slock.free_lock_command_count--
            self.free_commands.Push(flock_command)
        }
        self.slock.free_lock_command_lock.Unlock()
        return lock_command
    }
    self.slock.free_lock_command_lock.Unlock()
    return &protocol.LockCommand{}
}

func (self *BinaryServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
    return self.free_commands.Push(command)
}

func (self *BinaryServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
    self.glock.Lock()
    if self.closed {
        self.slock.free_lock_command_lock.Lock()
        self.slock.free_lock_commands.Push(command)
        self.slock.free_lock_command_count++
        self.slock.free_lock_command_lock.Unlock()
    } else {
        self.locked_free_commands.Push(command)
    }
    self.glock.Unlock()
    return nil
}

type TextServerProtocolParser struct {
    buf         []byte
    wbuf        []byte
    args        []string
    carg        []byte
    buf_index   int
    buf_len     int
    stage       int
    args_count  int
    carg_index  int
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

                    args_count, err := strconv.Atoi(string(self.carg[:self.carg_index]))
                    if err != nil {
                        return err
                    }
                    self.args_count = args_count
                    self.carg_index = 0
                    self.buf_index++
                    self.stage = 2
                    break
                } else if self.buf[self.buf_index] != '\r' {
                    if self.carg_index >= 64 {
                        return errors.New("Command parse args count error")
                    }
                    self.carg[self.carg_index] = self.buf[self.buf_index]
                    self.carg_index++
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

                    carg_len, err := strconv.Atoi(string(self.carg[:self.carg_index]))
                    if err != nil {
                        return errors.New("Command parse args count error")
                    }
                    self.carg_len = carg_len
                    self.carg_index = 0
                    self.buf_index++
                    self.stage = 4
                    break
                } else if self.buf[self.buf_index] != '\r' {
                    if self.carg_index >= 64 {
                        return errors.New("Command parse args count error")
                    }
                    self.carg[self.carg_index] = self.buf[self.buf_index]
                    self.carg_index++
                }
            }

            if self.stage == 3 {
                return nil
            }
        case 4:
            carg_len := self.carg_len - self.carg_index
            if carg_len > 0 {
                if self.buf_len - self.buf_index < carg_len {
                    if self.carg_index == 0 {
                        self.args = append(self.args, string(self.buf[self.buf_index: self.buf_len]))
                    } else {
                        self.args[len(self.args) - 1] += string(self.buf[self.buf_index: self.buf_len])
                    }
                    self.carg_index += self.buf_len - self.buf_index
                    self.buf_index = self.buf_len
                    return nil
                }

                if self.carg_index == 0 {
                    self.args = append(self.args, string(self.buf[self.buf_index: self.buf_index + carg_len]))
                } else {
                    self.args[len(self.args) - 1] += string(self.buf[self.buf_index: self.buf_index + carg_len])
                }
                self.carg_index = carg_len
                self.buf_index += carg_len
            }

            for ; self.buf_index < self.buf_len; self.buf_index++ {
                if self.buf[self.buf_index] == '\n' {
                    if self.buf_index > 0 && self.buf[self.buf_index-1] != '\r' {
                        return errors.New("Command parse arg error")
                    }

                    self.carg_index = 0
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
    glock                       *sync.Mutex
    stream                      *Stream
    free_commands               *LockCommandQueue
    free_command_result         *protocol.LockResultCommand
    parser                      *TextServerProtocolParser
    handlers                    map[string]TextServerProtocolCommandHandler
    lock_waiter                 chan *protocol.LockResultCommand
    lock_request_id             [16]byte
    lock_id                     [16]byte
    total_command_count         uint64
    db_id                       uint8
    closed                      bool
}

func NewTextServerProtocol(slock *SLock, stream *Stream) *TextServerProtocol {
    parser := &TextServerProtocolParser{make([]byte, 1024), make([]byte, 1024), make([]string, 0), make([]byte, 64),
        0, 0, 0, 0, 0, 0}
    server_protocol := &TextServerProtocol{slock, &sync.Mutex{}, stream, NewLockCommandQueue(4, 16, FREE_COMMAND_QUEUE_INIT_SIZE),
        nil, parser, make(map[string]TextServerProtocolCommandHandler, 64), make(chan *protocol.LockResultCommand, 4),
        [16]byte{}, [16]byte{}, 0, 0, false}
    server_protocol.InitLockCommand()

    server_protocol.handlers["SELECT"] = server_protocol.CommandHandlerSelectDB
    server_protocol.handlers["LOCK"] = server_protocol.CommandHandlerLock
    server_protocol.handlers["UNLOCK"] = server_protocol.CommandHandlerUnlock
    for name, handler := range slock.GetAdmin().GetHandlers() {
        server_protocol.handlers[name] = handler
    }
    stream.protocol = server_protocol
    return server_protocol
}

func (self *TextServerProtocol) Init(client_id [16]byte) error{
    return nil
}

func (self *TextServerProtocol) Lock() {
    self.glock.Lock()
}

func (self *TextServerProtocol) Unlock() {
    self.glock.Unlock()
}

func (self *TextServerProtocol) Close() error {
    self.glock.Lock()
    defer self.glock.Unlock()

    if self.closed {
        return nil
    }

    self.slock.glock.Lock()
    self.slock.stats_total_command_count += self.total_command_count
    self.slock.glock.Unlock()

    if self.stream != nil {
        err := self.stream.Close()
        if err != nil {
            self.slock.Log().Errorf("Connection Close Error: %s %v", self.RemoteAddr().String(), err)
        }
        self.stream.protocol = nil
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

        if self.parser.stage == 0 {
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
    return nil, errors.New("Protocol Closed")
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
            protocol.ERROR_MSG[lock_result_command.Result],
            "LOCK_ID",
            fmt.Sprintf("%x", lock_result_command.LockId),
            "LCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lcount),
            "COUNT",
            fmt.Sprintf("%d", lock_result_command.Count),
            "LRCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lrcount),
            "RCOUNT",
            fmt.Sprintf("%d", lock_result_command.Rcount),
        }
        return self.stream.WriteBytes(self.parser.Build(true, "", lock_results))
    }
    return self.stream.WriteBytes(self.parser.Build(false, "Unknwon Command", nil))
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

        if self.parser.stage == 0 {
            self.total_command_count++
            command_name := strings.ToUpper(self.parser.args[0])
            if command_handler, ok := self.handlers[command_name]; ok {
                err := command_handler(self, self.parser.args)
                if err != nil {
                    return err
                }
            } else {
                err := self.CommandHandlerUnknownCommand(self, self.parser.args)
                if err != nil {
                    return err
                }
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

    if self.parser.stage == 0 {
        self.total_command_count++
        command_name := strings.ToUpper(self.parser.args[0])
        if command_handler, ok := self.handlers[command_name]; ok {
            err := command_handler(self, self.parser.args)
            if err != nil {
                return err
            }
        } else {
            err := self.CommandHandlerUnknownCommand(self, self.parser.args)
            if err != nil {
                return err
            }
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
            protocol.ERROR_MSG[lock_result_command.Result],
            "LOCK_ID",
            fmt.Sprintf("%x", lock_result_command.LockId),
            "LCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lcount),
            "COUNT",
            fmt.Sprintf("%d", lock_result_command.Count),
            "LRCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lrcount),
            "RCOUNT",
            fmt.Sprintf("%d", lock_result_command.Rcount),
        }
        return self.stream.WriteBytes(self.parser.Build(true, "", lock_results))
    case protocol.COMMAND_UNLOCK:
        lock_result_command := command.(*protocol.LockResultCommand)
        lock_results := []string{
            fmt.Sprintf("%d", lock_result_command.Result),
            protocol.ERROR_MSG[lock_result_command.Result],
            "LOCK_ID",
            fmt.Sprintf("%x", lock_result_command.LockId),
            "LCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lcount),
            "COUNT",
            fmt.Sprintf("%d", lock_result_command.Count),
            "LRCOUNT",
            fmt.Sprintf("%d", lock_result_command.Lrcount),
            "RCOUNT",
            fmt.Sprintf("%d", lock_result_command.Rcount),
        }
        return self.stream.WriteBytes(self.parser.Build(true, "", lock_results))
    }
    return self.stream.WriteBytes(self.parser.Build(false, "Unknwon Command", nil))
}

func (self *TextServerProtocol) ProcessCommad(command protocol.ICommand) error {
    switch command.GetCommandType() {
    case protocol.COMMAND_LOCK:
        lock_command := command.(*protocol.LockCommand)

        if self.slock.state != STATE_LEADER {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
        }

        if lock_command.DbId == 0xff {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
        }

        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)

    case protocol.COMMAND_UNLOCK:
        lock_command := command.(*protocol.LockCommand)

        if self.slock.state != STATE_LEADER {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
        }

        if lock_command.DbId == 0xff {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
        }

        db := self.slock.dbs[lock_command.DbId]
        if db == nil {
            return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
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

            if self.stream != nil {
                self.stream.protocol = self
            }
            self.total_command_count += server_protocol.total_command_count
            server_protocol.UnInitLockCommand()
            server_protocol.closed = true
            return err

        case protocol.COMMAND_PING:
            ping_command := command.(*protocol.PingCommand)
            return self.Write(protocol.NewPingResultCommand(ping_command, protocol.RESULT_SUCCED))

        case protocol.COMMAND_QUIT:
            quit_command := command.(*protocol.QuitCommand)
            err := self.Write(protocol.NewQuitResultCommand(quit_command, protocol.RESULT_SUCCED))
            if err == nil {
                return io.EOF
            }
            return err

        default:
            return self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_COMMAND))
        }
    }
}

func (self *TextServerProtocol) ProcessLockCommand(lock_command *protocol.LockCommand) error {
    if self.slock.state != STATE_LEADER {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_STATE_ERROR, 0, 0)
    }

    if lock_command.DbId == 0xff {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
    }

    db := self.slock.dbs[lock_command.DbId]
    if lock_command.CommandType == protocol.COMMAND_LOCK {
        if db == nil {
            db = self.slock.GetOrNewDB(lock_command.DbId)
        }
        return db.Lock(self, lock_command)
    }

    if db == nil {
        return self.ProcessLockResultCommand(lock_command, protocol.RESULT_UNKNOWN_DB, 0, 0)
    }
    return db.UnLock(self, lock_command)
}

func (self *TextServerProtocol) ProcessLockResultCommand(lock_command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
    self.lock_request_id[0], self.lock_request_id[1], self.lock_request_id[2], self.lock_request_id[3], self.lock_request_id[4], self.lock_request_id[5], self.lock_request_id[6], self.lock_request_id[7],
        self.lock_request_id[8], self.lock_request_id[9], self.lock_request_id[10], self.lock_request_id[11], self.lock_request_id[12], self.lock_request_id[13], self.lock_request_id[14], self.lock_request_id[15] =
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0

    if self.free_command_result == nil {
        lock_result_commad := protocol.NewLockResultCommand(lock_command, result, 0, lcount, lock_command.Count, lrcount, lock_command.Rcount)
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
    lock_result_commad.Lrcount = lrcount
    lock_result_commad.Rcount = lock_command.Rcount
    self.free_command_result = nil
    self.lock_waiter <- lock_result_commad
    return nil
}

func (self *TextServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
    self.glock.Lock()
    if command.RequestId != self.lock_request_id {
        self.glock.Unlock()
        return nil
    }

    err := self.ProcessLockResultCommand(command, result, lcount, lrcount)
    self.glock.Unlock()
    return err
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
    self.slock.free_lock_command_lock.Lock()
    lock_command := self.slock.free_lock_commands.PopRight()
    if lock_command != nil {
        self.slock.free_lock_command_count--
        self.free_commands.Push(lock_command)
    } else {
        self.free_commands.Push(&protocol.LockCommand{})
    }
    self.slock.free_lock_command_lock.Unlock()
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
        lock_command := self.slock.free_lock_commands.PopRight()
        if lock_command != nil {
            self.slock.free_lock_command_count--
            self.slock.free_lock_command_lock.Unlock()
            return lock_command
        }
        self.slock.free_lock_command_lock.Unlock()
        return &protocol.LockCommand{}
    }
    return lock_command
}

func (self *TextServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
    self.glock.Lock()
    self.free_commands.Push(command)
    self.glock.Unlock()
    return nil
}

func (self *TextServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
    self.glock.Lock()
    if self.closed {
        self.slock.free_lock_command_lock.Lock()
        self.slock.free_lock_commands.Push(command)
        self.slock.free_lock_command_count++
        self.slock.free_lock_command_lock.Unlock()
    } else {
        self.free_commands.Push(command)
    }
    self.glock.Unlock()
    return nil
}

func (self *TextServerProtocol) ArgsToLockComandParseId(arg_id string, lock_id *[16]byte) {
    arg_len := len(arg_id)
    if arg_len == 16 {
        lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7], 
            lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] = 
            byte(arg_id[0]), byte(arg_id[1]), byte(arg_id[2]), byte(arg_id[3]), byte(arg_id[4]), byte(arg_id[5]), byte(arg_id[6]), 
            byte(arg_id[7]), byte(arg_id[8]), byte(arg_id[9]), byte(arg_id[10]), byte(arg_id[11]), byte(arg_id[12]), byte(arg_id[13]), byte(arg_id[14]), byte(arg_id[15])
    } else if arg_len > 16 {
        if arg_len == 32 {
            v, err := hex.DecodeString(arg_id)
            if err == nil {
                lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7],
                    lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] =
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
            } else {
                v := md5.Sum([]byte(arg_id))
                lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7],
                    lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] =
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
            }
        } else {
            v := md5.Sum([]byte(arg_id))
            lock_id[0], lock_id[1], lock_id[2], lock_id[3], lock_id[4], lock_id[5], lock_id[6], lock_id[7],
                lock_id[8], lock_id[9], lock_id[10], lock_id[11], lock_id[12], lock_id[13], lock_id[14], lock_id[15] =
                v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
        }
    } else {
        arg_index := 16 - arg_len
        for i := 0; i < 16; i++ {
            if i < arg_index {
                lock_id[i] = 0
            } else {
                lock_id[i] = arg_id[i - arg_index]
            }
        }
    }
}

func (self *TextServerProtocol) ArgsToLockComand(args []string) (*protocol.LockCommand, error) {
    if len(args) < 2 || len(args) % 2 != 0 {
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
    command.DbId = self.db_id
    command.Flag = 0
    command.Timeout = 3
    command.TimeoutFlag = 0
    command.Expried = 60
    command.ExpriedFlag = 0
    command.Count = 0
    command.Rcount = 0
    self.ArgsToLockComandParseId(args[1], &command.LockKey)

    has_lock_id := false
    for i := 2; i < len(args); i+= 2 {
        switch strings.ToUpper(args[i]) {
        case "LOCK_ID":
            self.ArgsToLockComandParseId(args[i + 1], &command.LockId)
            has_lock_id = true
        case "FLAG":
            flag, err := strconv.Atoi(args[i + 1])
            if err != nil {
                return nil, errors.New("Command Parse FLAG Error")
            }
            command.Flag = uint8(flag)
        case "TIMEOUT":
            timeout, err := strconv.Atoi(args[i + 1])
            if err != nil {
                return nil, errors.New("Command Parse TIMEOUT Error")
            }
            command.Timeout = uint16(timeout & 0xffff)
            command.TimeoutFlag = uint16(timeout >> 16 & 0xffff)
        case "EXPRIED":
            expried, err := strconv.Atoi(args[i + 1])
            if err != nil {
                return nil, errors.New("Command Parse EXPRIED Error")
            }
            command.Expried = uint16(expried & 0xffff)
            command.ExpriedFlag = uint16(expried >> 16 & 0xffff)
        case "COUNT":
            count, err := strconv.Atoi(args[i + 1])
            if err != nil {
                return nil, errors.New("Command Parse COUNT Error")
            }
            command.Count = uint16(count)
        case "RCOUNT":
            rcount, err := strconv.Atoi(args[i + 1])
            if err != nil {
                return nil, errors.New("Command Parse RCOUNT Error")
            }
            command.Rcount = uint8(rcount)
        }
    }

    if !has_lock_id {
        if command_name == "LOCK" {
            command.LockId = command.RequestId
        } else {
            command.LockId = self.lock_id
        }
    }
    return command, nil
}

func (self *TextServerProtocol) CommandHandlerUnknownCommand(server_protocol *TextServerProtocol, args []string) error {
    return self.stream.WriteBytes(self.parser.Build(false, "Unknown Command", nil))
}

func (self *TextServerProtocol) CommandHandlerSelectDB(server_protocol *TextServerProtocol, args []string) error {
    if len(args) < 2 {
        return self.stream.WriteBytes(self.parser.Build(false, "Command Parse Len Error", nil))
    }

    db_id, err := strconv.Atoi(args[1])
    if err != nil {
        return self.stream.WriteBytes(self.parser.Build(false, "Command Parse DB_ID Error", nil))
    }
    self.db_id = uint8(db_id)
    return self.stream.WriteBytes(self.parser.Build(true, "OK", nil))
}

func (self *TextServerProtocol) CommandHandlerLock(server_protocol *TextServerProtocol, args []string) error {
    lock_command, err := self.ArgsToLockComand(args)
    if err != nil {
        return self.stream.WriteBytes(self.parser.Build(false, err.Error(), nil))
    }

    if self.slock.state != STATE_LEADER {
        return self.stream.WriteBytes(self.parser.Build(false, "State Error", nil))
    }

    if lock_command.DbId == 0xff {
        return self.stream.WriteBytes(self.parser.Build(false, "Uknown DB Error", nil))
    }

    db := self.slock.dbs[lock_command.DbId]
    if db == nil {
        db = self.slock.GetOrNewDB(lock_command.DbId)
    }
    self.lock_request_id = lock_command.RequestId
    err = db.Lock(self, lock_command)
    if err != nil {
        return self.stream.WriteBytes(self.parser.Build(false, "Lock Error", nil))
    }
    lock_command_result := <- self.lock_waiter
    if lock_command_result.Result == 0 {
        self.lock_id = lock_command.LockId
    }

    buf_index := 0
    tr := ""

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("*12\r\n"))

    tr = fmt.Sprintf("%d", lock_command_result.Result)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    tr = protocol.ERROR_MSG[lock_command_result.Result]
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("%x", lock_command_result.LockId)))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$6\r\nLCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$5\r\nCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Count)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$7\r\nLRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lrcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$6\r\nRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Rcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n"))
    self.free_command_result = lock_command_result
    return self.stream.WriteBytes(self.parser.wbuf[:buf_index])
}

func (self *TextServerProtocol) CommandHandlerUnlock(server_protocol *TextServerProtocol, args []string) error {
    lock_command, err := self.ArgsToLockComand(args)
    if err != nil {
        return self.stream.WriteBytes(self.parser.Build(false, err.Error(), nil))
    }

    if self.slock.state != STATE_LEADER {
        return self.stream.WriteBytes(self.parser.Build(false, "State Error", nil))
    }

    if lock_command.DbId == 0xff {
        return self.stream.WriteBytes(self.parser.Build(false, "Uknown DB Error", nil))
    }

    db := self.slock.dbs[lock_command.DbId]
    if db == nil {
        return self.stream.WriteBytes(self.parser.Build(false, "Uknown DB Error", nil))
    }
    self.lock_request_id = lock_command.RequestId
    err = db.UnLock(self, lock_command)
    if err != nil {
        return self.stream.WriteBytes(self.parser.Build(false, "UnLock Error", nil))
    }
    lock_command_result := <- self.lock_waiter
    if lock_command_result.Result == 0 {
        self.lock_id[0], self.lock_id[1], self.lock_id[2], self.lock_id[3], self.lock_id[4], self.lock_id[5], self.lock_id[6], self.lock_id[7],
            self.lock_id[8], self.lock_id[9], self.lock_id[10], self.lock_id[11], self.lock_id[12], self.lock_id[13], self.lock_id[14], self.lock_id[15] = 
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0
    }

    buf_index := 0
    tr := ""

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("*12\r\n"))

    tr = fmt.Sprintf("%d", lock_command_result.Result)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    tr = protocol.ERROR_MSG[lock_command_result.Result]
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("%x", lock_command_result.LockId)))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$6\r\nLCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$5\r\nCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Count)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$7\r\nLRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Lrcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n$6\r\nRCOUNT"))

    tr = fmt.Sprintf("%d", lock_command_result.Rcount)
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
    buf_index += copy(self.parser.wbuf[buf_index:], []byte(tr))

    buf_index += copy(self.parser.wbuf[buf_index:], []byte("\r\n"))

    self.free_command_result = lock_command_result
    return self.stream.WriteBytes(self.parser.wbuf[:buf_index])
}

func (self *TextServerProtocol) GetRequestId() [16]byte {
    now := uint32(time.Now().Unix())
    request_id_index := atomic.AddUint64(&request_id_index, 1)
    return [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(request_id_index >> 40), byte(request_id_index >> 32), byte(request_id_index >> 24), byte(request_id_index >> 16), byte(request_id_index >> 8), byte(request_id_index),
    }
}
