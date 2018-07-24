package slock

import (
    "github.com/hhkbp2/go-logging"
    "sync"
    "errors"
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
        if len(buf) < 64 {
            return errors.New("buf too short")
        }

        buf[2] = byte(command.CommandType)

        buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(command.RequestId[0]), byte(command.RequestId[0] >> 8), byte(command.RequestId[0] >> 16), byte(command.RequestId[0] >> 24), byte(command.RequestId[0] >> 32), byte(command.RequestId[0] >> 40), byte(command.RequestId[0] >> 48), byte(command.RequestId[0] >> 56)
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(command.RequestId[1]), byte(command.RequestId[1] >> 8), byte(command.RequestId[1] >> 16), byte(command.RequestId[1] >> 24), byte(command.RequestId[1] >> 32), byte(command.RequestId[1] >> 40), byte(command.RequestId[1] >> 48), byte(command.RequestId[1] >> 56)

        buf[19], buf[20], buf[21] = r, 0x00, byte(command.DbId)

        buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29] = byte(command.LockId[0]), byte(command.LockId[0] >> 8), byte(command.LockId[0] >> 16), byte(command.LockId[0] >> 24), byte(command.LockId[0] >> 32), byte(command.LockId[0] >> 40), byte(command.LockId[0] >> 48), byte(command.LockId[0] >> 56)
        buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] = byte(command.LockId[1]), byte(command.LockId[1] >> 8), byte(command.LockId[1] >> 16), byte(command.LockId[1] >> 24), byte(command.LockId[1] >> 32), byte(command.LockId[1] >> 40), byte(command.LockId[1] >> 48), byte(command.LockId[1] >> 56)

        buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45] = byte(command.LockKey[0]), byte(command.LockKey[0] >> 8), byte(command.LockKey[0] >> 16), byte(command.LockKey[0] >> 24), byte(command.LockKey[0] >> 32), byte(command.LockKey[0] >> 40), byte(command.LockKey[0] >> 48), byte(command.LockKey[0] >> 56)
        buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] = byte(command.LockKey[1]), byte(command.LockKey[1] >> 8), byte(command.LockKey[1] >> 16), byte(command.LockKey[1] >> 24), byte(command.LockKey[1] >> 32), byte(command.LockKey[1] >> 40), byte(command.LockKey[1] >> 48), byte(command.LockKey[1] >> 56)

        buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        buf[62], buf[63] = 0x00, 0x00
        
        return protocol.stream.WriteBytes(buf)
    }

    protocol.free_result_command_lock.Lock()
    buf := protocol.owbuf

    if len(buf) < 64 {
        return errors.New("buf too short")
    }

    buf[2] = byte(command.CommandType)

    buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10] = byte(command.RequestId[0]), byte(command.RequestId[0] >> 8), byte(command.RequestId[0] >> 16), byte(command.RequestId[0] >> 24), byte(command.RequestId[0] >> 32), byte(command.RequestId[0] >> 40), byte(command.RequestId[0] >> 48), byte(command.RequestId[0] >> 56)
    buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] = byte(command.RequestId[1]), byte(command.RequestId[1] >> 8), byte(command.RequestId[1] >> 16), byte(command.RequestId[1] >> 24), byte(command.RequestId[1] >> 32), byte(command.RequestId[1] >> 40), byte(command.RequestId[1] >> 48), byte(command.RequestId[1] >> 56)

    buf[19], buf[20], buf[21] = r, 0x00, byte(command.DbId)

    buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29] = byte(command.LockId[0]), byte(command.LockId[0] >> 8), byte(command.LockId[0] >> 16), byte(command.LockId[0] >> 24), byte(command.LockId[0] >> 32), byte(command.LockId[0] >> 40), byte(command.LockId[0] >> 48), byte(command.LockId[0] >> 56)
    buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] = byte(command.LockId[1]), byte(command.LockId[1] >> 8), byte(command.LockId[1] >> 16), byte(command.LockId[1] >> 24), byte(command.LockId[1] >> 32), byte(command.LockId[1] >> 40), byte(command.LockId[1] >> 48), byte(command.LockId[1] >> 56)

    buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45] = byte(command.LockKey[0]), byte(command.LockKey[0] >> 8), byte(command.LockKey[0] >> 16), byte(command.LockKey[0] >> 24), byte(command.LockKey[0] >> 32), byte(command.LockKey[0] >> 40), byte(command.LockKey[0] >> 48), byte(command.LockKey[0] >> 56)
    buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] = byte(command.LockKey[1]), byte(command.LockKey[1] >> 8), byte(command.LockKey[1] >> 16), byte(command.LockKey[1] >> 24), byte(command.LockKey[1] >> 32), byte(command.LockKey[1] >> 40), byte(command.LockKey[1] >> 48), byte(command.LockKey[1] >> 56)

    buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    buf[62], buf[63] = 0x00, 0x00

    err = protocol.stream.WriteBytes(buf)
    protocol.free_result_command_lock.Unlock()
    return err
}

func (self *SLock) Log() logging.Logger {
    return self.logger
}
