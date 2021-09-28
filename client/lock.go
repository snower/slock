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
    if self.Err == nil {
        return fmt.Sprintf("error code %d", self.Result)
    }
    return fmt.Sprintf("%d %s", self.Result, self.Err.Error())
}

type Lock struct {
    db          *Database
    request_id  [16]byte
    lock_id     [16]byte
    lock_key    [16]byte
    timeout     uint32
    expried     uint32
    count       uint16
    rcount      uint8
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

func (self *Lock) DoLock(flag uint8) (*protocol.LockResultCommand, error) {
    self.request_id = self.db.GenRequestId()
    command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.request_id},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
    result_command, err := self.db.executeCommand(command, int(command.Timeout + 1))
    if err != nil {
        return nil, err
    }

    lock_result_command, ok := result_command.(*protocol.LockResultCommand)
    if !ok {
        return nil, errors.New("unknown command result")
    }
    return lock_result_command, nil
}

func (self *Lock) DoUnlock(flag uint8) (*protocol.LockResultCommand, error) {
    self.request_id = self.db.GenRequestId()
    command := &protocol.LockCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.request_id},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
    result_command, err := self.db.executeCommand(command, int(command.Timeout + 1))
    if err != nil {
        return nil, err
    }

    lock_result_command, ok := result_command.(*protocol.LockResultCommand)
    if !ok {
        return nil, errors.New("unknown command result")
    }
    return lock_result_command, nil
}

func (self *Lock) Lock() *LockError{
    lock_result_command, err := self.DoLock(0)
    if err != nil {
        return &LockError{0x80, lock_result_command, err}
    }

    if lock_result_command.Result != 0 {
        return &LockError{lock_result_command.Result, lock_result_command, err}
    }
    return nil
}

func (self *Lock) Unlock() *LockError{
    lock_result_command, err := self.DoUnlock(0)
    if err != nil {
        return &LockError{0x80, lock_result_command, err}
    }

    if lock_result_command.Result != 0 {
        return &LockError{lock_result_command.Result, lock_result_command, err}
    }
    return nil
}