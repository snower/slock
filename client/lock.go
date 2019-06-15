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
}

func NewLock(db *Database, lock_key [2]uint64, timeout uint32, expried uint32, count uint16) *Lock {
    return &Lock{db, db.GetRequestId(), db.GenLockId(), lock_key, timeout, expried, count}
}

func (self *Lock) DoLock(flag uint8) *LockError{
    request_id := self.db.GetRequestId()
    command := &protocol.LockCommand{protocol.Command{protocol.MAGIC, protocol.VERSION, protocol.COMMAND_LOCK, request_id},
        flag, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, self.count, 0}
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
    request_id := self.db.GetRequestId()
    command := &protocol.LockCommand{protocol.Command{ protocol.MAGIC, protocol.VERSION, protocol.COMMAND_UNLOCK, request_id},
        flag, self.db.db_id, self.lock_id, self.lock_key, self.timeout, self.expried, self.count, 0}
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