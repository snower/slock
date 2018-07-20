package slock

import (
    "github.com/hhkbp2/go-logging"
    "sync"
)

type SLock struct {
    dbs    []*LockDB
    glock  sync.Mutex
    logger logging.Logger
}

func NewSLock(log_file string, log_level string) *SLock {
    logger := InitLogger(log_file, log_level)
    return &SLock{make([]*LockDB, 256), sync.Mutex{}, logger}
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
        
        return protocol.stream.WriteBytes(buf)
    }

    result := NewLockResultCommand(command, r, 0)
    return protocol.Write(result, use_cached_command)
}

func (self *SLock) Log() logging.Logger {
    return self.logger
}
