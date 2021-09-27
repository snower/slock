package client

import (
    "errors"
    "fmt"
    "github.com/snower/slock/protocol"
)

type LockError struct {
    Result uint8
    CommandResult *protocol.LockResultCommand
    Err   error
}

func (self LockError) Error() string {
    return fmt.Sprintf("%d %s", self.Result, self.Err.Error())
}

type Lock struct {
    db *Database
    request_id [16]byte
    lock_id [16]byte
    lock_key [16]byte
    timeout uint32
    expried uint32
    count uint16
    rcount uint8
}

func NewLock(db *Database, lock_key [16]byte, timeout uint32, expried uint32, count uint16, rcount uint8) *Lock {
    if count > 0 {
        count -= 1
    }
    if rcount > 0 {
        rcount -= 1
    }
    return &Lock{db, [16]byte{}, db.GenLockId(), lock_key, timeout, expried, count, rcount}
}

func (self *Lock) DoLock(flag uint8) (*protocol.LockResultCommand, *LockError){
    self.request_id = self.db.GetRequestId()
    command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.request_id},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
    result_command, err := self.db.SendLockCommand(command)
    if err != nil {
        return result_command, &LockError{protocol.RESULT_ERROR, result_command, err}
    }
    if result_command.Result != protocol.RESULT_SUCCED {
        return result_command, &LockError{result_command.Result, result_command, errors.New("lock error")}
    }
    return result_command, nil
}

func (self *Lock) DoUnlock(flag uint8) (*protocol.LockResultCommand, *LockError){
    self.request_id = self.db.GetRequestId()
    command := &protocol.LockCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.request_id},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
    result_command, err := self.db.SendUnLockCommand(command)
    if err != nil {
        return result_command, &LockError{protocol.RESULT_ERROR, result_command, err}
    }
    if result_command.Result != protocol.RESULT_SUCCED {
        return result_command, &LockError{result_command.Result, result_command, errors.New("lock error")}
    }
    return result_command, nil
}

func (self *Lock) Lock() *LockError{
    _, err := self.DoLock(0)
    return err
}

func (self *Lock) Unlock() *LockError{
    _, err := self.DoUnlock(0)
    return err
}