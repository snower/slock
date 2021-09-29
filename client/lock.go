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
    lock_id     [16]byte
    lock_key    [16]byte
    timeout     uint32
    expried     uint32
    count       uint16
    rcount      uint8
}

func NewLock(db *Database, lock_key [16]byte, timeout uint32, expried uint32) *Lock {
    return &Lock{db, db.GenLockId(), lock_key, timeout, expried, 0, 0}
}

func (self *Lock) GetLockKey() [16]byte {
    return self.lock_key
}

func (self *Lock) GetLockId() [16]byte {
    return self.lock_id
}

func (self *Lock) GetTimeout() uint32 {
    return self.timeout
}

func (self *Lock) GetExpried() uint32 {
    return self.expried
}

func (self *Lock) GetCount() uint16 {
    return self.count
}

func (self *Lock) SetCount(count uint16) uint16 {
    self.count = count
    return self.count
}

func (self *Lock) GetRcount() uint8 {
    return self.rcount
}

func (self *Lock) SetRcount(rcount uint8) uint8 {
    self.rcount = rcount
    return self.rcount
}

func (self *Lock) doLock(flag uint8, timeout uint32, expried uint32, count uint16, rcount uint8) (*protocol.LockResultCommand, error) {
    command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.db.GenRequestId()},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(timeout >> 16), Timeout: uint16(timeout),
        ExpriedFlag: uint16(expried >> 16), Expried: uint16(expried), Count: count, Rcount: rcount}
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

func (self *Lock) doUnlock(flag uint8, timeout uint32, expried uint32, count uint16, rcount uint8) (*protocol.LockResultCommand, error) {
    command := &protocol.LockCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.db.GenRequestId()},
        Flag: flag, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(timeout >> 16), Timeout: uint16(timeout),
        ExpriedFlag: uint16(expried >> 16), Expried: uint16(expried), Count: count, Rcount: rcount}
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
    lock_result_command, err := self.doLock(0, self.timeout, self.expried, self.count, self.rcount)
    if err != nil {
        return &LockError{0x80, lock_result_command, err}
    }

    if lock_result_command.Result != 0 {
        return &LockError{lock_result_command.Result, lock_result_command, err}
    }
    return nil
}

func (self *Lock) Unlock() *LockError{
    lock_result_command, err := self.doUnlock(0, self.timeout, self.expried, self.count, self.rcount)
    if err != nil {
        return &LockError{0x80, lock_result_command, err}
    }

    if lock_result_command.Result != 0 {
        return &LockError{lock_result_command.Result, lock_result_command, err}
    }
    return nil
}

func (self *Lock) LockShow() *LockError {
    lock_result_command, err := self.doLock(0x01, 0, 0, 0,0)
    if err != nil {
        return &LockError{0x80, lock_result_command, err}
    }

    if lock_result_command.Result != protocol.RESULT_UNOWN_ERROR {
        return &LockError{lock_result_command.Result, lock_result_command, nil}
    }
    return &LockError{lock_result_command.Result, lock_result_command, errors.New("show error")}
}

func (self *Lock) LockUpdate() *LockError {
    lock_result_command, err := self.doLock(0x02, self.timeout, self.expried, self.count, self.rcount)
    if err != nil {
        return &LockError{0x80, lock_result_command, err}
    }

    if lock_result_command.Result != 0 && lock_result_command.Result != protocol.RESULT_LOCKED_ERROR {
        return &LockError{lock_result_command.Result, lock_result_command, nil}
    }
    return &LockError{lock_result_command.Result, lock_result_command, errors.New("update error")}
}

func (self *Lock) UnlockHead() *LockError {
    lock_result_command, err := self.doUnlock(0x01, self.timeout, self.expried, self.count, self.rcount)
    if err != nil {
        return &LockError{0x80, lock_result_command, err}
    }

    if lock_result_command.Result != 0 {
        return &LockError{lock_result_command.Result, lock_result_command, errors.New("unlock error")}
    }
    return &LockError{lock_result_command.Result, lock_result_command, nil}
}

func (self *Lock) SendLock() error {
    command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.db.GenRequestId()},
        Flag: 0, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
    return self.db.sendCommand(command)
}

func (self *Lock) SendUnlock() error {
    command := &protocol.LockCommand{Command: protocol.Command{ Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.db.GenRequestId()},
        Flag: 0, DbId: self.db.db_id, LockId: self.lock_id, LockKey: self.lock_key, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
        ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
    return self.db.sendCommand(command)
}