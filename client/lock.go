package client

import (
    "errors"
    "fmt"
    "github.com/snower/slock/protocol"
)

type LockError struct {
    Result uint8
    Err   error
}

func (self LockError) Error() string {
    return fmt.Sprintf("%d %s", self.Result, self.Err.Error())
}

type Lock struct {
    db *Database
    request_id [2]uint64
    lock_id [2]uint64
    lock_key [2]uint64
    timeout uint32
    expried uint32
    count uint16
    rcount uint8
}

func NewLock(db *Database, lock_key [2]uint64, timeout uint32, expried uint32, count uint16, rcount uint8) *Lock {
    return &Lock{db, [2]uint64{0, 0}, db.GenLockId(), lock_key, timeout, expried, count, rcount}
}

func (self *Lock) DoLock(flag uint8) *LockError{
    self.request_id = self.db.GetRequestId()
    command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.request_id},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: 0}
    result_command, err := self.db.SendLockCommand(command)
    if err != nil {
        return &LockError{protocol.RESULT_ERROR, err}
    }
    if result_command.Result != protocol.RESULT_SUCCED {
        return &LockError{result_command.Result, errors.New("lock error")}
    }
    return nil
}

func (self *Lock) DoUnlock(flag uint8) *LockError{
    self.request_id = self.db.GetRequestId()
    command := &protocol.LockCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.request_id},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: 0}
    result_command, err := self.db.SendUnLockCommand(command)
    if err != nil {
        return &LockError{protocol.RESULT_ERROR, err}
    }
    if result_command.Result != protocol.RESULT_SUCCED {
        return &LockError{result_command.Result, errors.New("lock error")}
    }
    return nil
}

func (self *Lock) Lock() *LockError{
    return self.DoLock(0)
}

func (self *Lock) Unlock() *LockError{
    return self.DoUnlock(0)
}