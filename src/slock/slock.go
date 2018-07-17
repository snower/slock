package slock

import (
    "github.com/hhkbp2/go-logging"
    "sync"
)

type SLock struct {
    dbs    []*LockDB
    glock   sync.Mutex
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

func (self *SLock) DoLockComamnd(db *LockDB, command *LockCommand) (err error) {
    return db.Lock(command)
}

func (self *SLock) DoUnLockComamnd(db *LockDB, command *LockCommand) (err error) {
    return db.UnLock(command)
}

func (self *SLock) GetState(command *StateCommand) (err error) {
    db_state := uint8(0)

    db := self.dbs[command.DbId]
    if db != nil {
        db_state = 1
    }

    if db == nil {
        command.Protocol.Write(NewStateResultCommand(command, RESULT_SUCCED, 0, db_state, nil))
        return nil
    }
    command.Protocol.Write(NewStateResultCommand(command, RESULT_SUCCED, 0, db_state, db.GetState()))
    return nil
}

func (self *SLock) Handle(command ICommand) (err error) {
    switch command.GetCommandType() {
    case COMMAND_LOCK:
        lock_command := command.(*LockCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            db = self.GetOrNewDB(lock_command.DbId)
        }
        db.Lock(lock_command)

    case COMMAND_UNLOCK:
        lock_command := command.(*LockCommand)
        db := self.dbs[lock_command.DbId]
        if db == nil {
            self.Active(lock_command, RESULT_UNKNOWN_DB)
            return nil
        }
        db.UnLock(lock_command)

    case COMMAND_STATE:
        self.GetState(command.(*StateCommand))

    default:
        command.GetProtocol().Write(NewResultCommand(command.(*Command), RESULT_UNKNOWN_COMMAND))
    }
    return nil
}

func (self *SLock) Active(command *LockCommand, r uint8) (err error) {
    result := NewLockResultCommand(command, r, 0)
    return command.Protocol.Write(result)
}

func (self *SLock) Log() logging.Logger {
    return self.logger
}
