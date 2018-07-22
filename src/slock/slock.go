package slock

import (
    "github.com/hhkbp2/go-logging"
    "sync"
    "sync/atomic"
    "time"
)

const FREE_COMMANDS_SLOT_COUNT = 8
const FREE_LOCK_COMMAND_MAX_COUNT uint32 = 0x0005ffff
const FREE_LOCK_RESULT_COMMAND_MAX_COUNT uint32 = 0x00001fff

type SLock struct {
    dbs    []*LockDB
    glock  sync.Mutex
    logger logging.Logger
    free_lock_commands [][]*LockCommand
    free_lock_command_count []uint32
    free_lock_result_commands [][]*LockResultCommand
    free_lock_result_command_count []uint32
    free_index int
}

func NewSLock(log_file string, log_level string) *SLock {
    logger := InitLogger(log_file, log_level)
    free_lock_commands := make([][]*LockCommand, FREE_COMMANDS_SLOT_COUNT)
    free_lock_command_count := make([]uint32, FREE_COMMANDS_SLOT_COUNT)
    free_lock_result_commands := make([][]*LockResultCommand, FREE_COMMANDS_SLOT_COUNT)
    free_lock_result_command_count := make([]uint32, FREE_COMMANDS_SLOT_COUNT)
    for i := 0; i < FREE_COMMANDS_SLOT_COUNT; i++ {
        free_lock_commands[i] = make([]*LockCommand, FREE_LOCK_COMMAND_MAX_COUNT)
        free_lock_command_count[i] = 0xffffffff
        free_lock_result_commands[i] = make([]*LockResultCommand, FREE_LOCK_RESULT_COMMAND_MAX_COUNT)
        free_lock_result_command_count[i] = 0xffffffff

        lock_commands := make([]LockCommand, 4096)
        for j := 0; j < 4096; j++ {
            free_lock_commands[i][j] = &lock_commands[j]
            free_lock_command_count[i]++
        }

        lock_result_commands := make([]LockResultCommand, 64)
        for j := 0; j < 4096; j++ {
            free_lock_result_commands[i][j] = &lock_result_commands[j]
            free_lock_result_command_count[i]++
        }
    }
    slock := &SLock{make([]*LockDB, 256), sync.Mutex{}, logger, free_lock_commands, free_lock_command_count, free_lock_result_commands, free_lock_result_command_count, 0}
    slock.CheckFreeCommand(0)
    return nil
}

func (self *SLock) CheckFreeCommand(last_lock_count uint64) error{
    lock_count := uint64(0)
    for i := 0; i < 256; i++ {
        db := self.dbs[i]
        if db != nil {
            state := db.GetState()
            lock_count += state.LockCount
        }
    }

    count := int(lock_count - last_lock_count) / 300 * 4
    if count < 4096 {
        count = 4096
    }

    for i := 0; i < FREE_COMMANDS_SLOT_COUNT; i++ {
        command_count := 0
        for j := 0; j < 65536; j++ {
            if self.free_lock_commands[i][j] != nil {
                command_count++
            }
        }

        for ; count < command_count; {
            free_command_count := atomic.AddUint32(&self.free_lock_command_count[i], 0xffffffff)
            self.free_lock_commands[i][(free_command_count + 1) & FREE_LOCK_COMMAND_MAX_COUNT] = nil
        }
    }

    count = int(lock_count - last_lock_count) / 300 * 4
    if count < 64 {
        count = 64
    }

    for i := 0; i < FREE_COMMANDS_SLOT_COUNT; i++ {
        command_count := 0
        for j := 0; j < 4096; j++ {
            if self.free_lock_result_commands[i][j] != nil {
                command_count++
            }
        }

        for ; count < command_count; {
            free_result_command_count := atomic.AddUint32(&self.free_lock_result_command_count[i], 0xffffffff)
            self.free_lock_result_commands[i][(free_result_command_count + 1) & FREE_LOCK_RESULT_COMMAND_MAX_COUNT] = nil
        }
    }

    go func(last_lock_count uint64) {
        time.Sleep(300 * 1e9)
        self.CheckFreeCommand(last_lock_count)
    }(lock_count)
    return nil
}

func (self *SLock) GetOrNewDB(db_id uint8) *LockDB {
    defer self.glock.Unlock()
    self.glock.Lock()

    if self.dbs[db_id] == nil {
        self.dbs[db_id] = NewLockDB(self)
    }
    return self.dbs[db_id]
}

func (self *SLock) GetDB(db_id uint8) *LockDB {
    if self.dbs[db_id] == nil {
        return self.GetOrNewDB(db_id)
    }
    return self.dbs[db_id]
}

func (self *SLock) DoLockComamnd(db *LockDB, protocol *ServerProtocol, command *LockCommand) (err error) {
    return db.Lock(protocol, command)
}

func (self *SLock) DoUnLockComamnd(db *LockDB, protocol *ServerProtocol, command *LockCommand) (err error) {
    return db.UnLock(protocol, command)
}

func (self *SLock) GetState(protocol *ServerProtocol, command *StateCommand) (err error) {
    db_state := uint8(0)

    db := self.dbs[command.DbId]
    if db != nil {
        db_state = 1
    }

    if db == nil {
        protocol.Write(NewStateResultCommand(command, RESULT_SUCCED, 0, db_state, nil), true)
        return nil
    }
    protocol.Write(NewStateResultCommand(command, RESULT_SUCCED, 0, db_state, db.GetState()), true)
    return nil
}

func (self *SLock) Handle(protocol *ServerProtocol, command ICommand) (err error) {
    switch command.GetCommandType() {
    case COMMAND_LOCK:
        lock_command := command.(*LockCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.GetOrNewDB(lock_command.DbId)
        }
        db.Lock(protocol, lock_command)

    case COMMAND_UNLOCK:
        lock_command := command.(*LockCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            self.Active(protocol, lock_command, RESULT_UNKNOWN_DB, true)
            protocol.FreeLockCommand(lock_command)
            return nil
        }
        db.UnLock(protocol, lock_command)

    case COMMAND_STATE:
        self.GetState(protocol, command.(*StateCommand))

    default:
        protocol.Write(NewResultCommand(command, RESULT_UNKNOWN_COMMAND), true)
    }
    return nil
}

func (self *SLock) Active(protocol *ServerProtocol, command *LockCommand, r uint8, use_cached_command bool) (err error) {
    if use_cached_command {
        buf := protocol.wbuf
        buf[2] = byte(command.CommandType)

        for i := 0; i < 16; i+=4 {
            buf[3 + i] = command.RequestId[i]
            buf[4 + i] = command.RequestId[i + 1]
            buf[5 + i] = command.RequestId[i + 2]
            buf[6 + i] = command.RequestId[i + 3]
        }

        buf[19] = uint8(r)
        buf[20] = 0x00
        buf[21] = byte(command.DbId)

        for i := 0; i < 16; i+=4 {
            buf[22 + i] = command.LockId[i]
            buf[23 + i] = command.LockId[i + 1]
            buf[24 + i] = command.LockId[i + 2]
            buf[25 + i] = command.LockId[i + 3]
        }

        for i := 0; i < 16; i+=4 {
            buf[38 + i] = command.LockKey[i]
            buf[39 + i] = command.LockKey[i + 1]
            buf[40 + i] = command.LockKey[i + 2]
            buf[41 + i] = command.LockKey[i + 3]
        }
        
        for i := 0; i < 8; i+=4 {
            buf[54 + i] = 0x00
            buf[55 + i] = 0x00
            buf[56 + i] = 0x00
            buf[57 + i] = 0x00
        }
        buf[62] = 0x00
        buf[63] = 0x00
        
        return protocol.stream.WriteBytes(buf)
    }

    free_result_command_count := atomic.AddUint32(protocol.free_result_command_count, 0xffffffff)
    free_index := (free_result_command_count + 1) & FREE_LOCK_RESULT_COMMAND_MAX_COUNT
    lock_command := protocol.free_result_commands[free_index]
    if lock_command == nil {
        lock_command = &LockResultCommand{}
        lock_command.Magic = MAGIC
        lock_command.Version = VERSION
    } else {
        self.free_lock_result_commands[free_index] = nil
    }

    lock_command.CommandType = command.CommandType
    lock_command.RequestId = command.RequestId
    lock_command.Flag = 0
    lock_command.DbId = command.DbId
    lock_command.LockId = command.LockId
    lock_command.LockKey = command.LockKey
    lock_command.Result = r
    err = protocol.Write(lock_command, use_cached_command)
    protocol.FreeLockResultCommand(lock_command)
    return err
}

func (self *SLock) Log() logging.Logger {
    return self.logger
}
