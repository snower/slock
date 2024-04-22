package client

import (
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
)

type LockError struct {
	Result        uint8
	CommandResult *protocol.LockResultCommand
	Err           error
}

func (self *LockError) GetLockData() *protocol.LockResultCommandData {
	return self.CommandResult.Data
}

func (self *LockError) GetBytesData() []byte {
	if self.CommandResult != nil && self.CommandResult.Data != nil {
		return self.CommandResult.GetLockData().GetBytesData()
	}
	return nil
}

func (self *LockError) GetStringData() string {
	if self.CommandResult != nil && self.CommandResult.Data != nil {
		return self.CommandResult.GetLockData().GetStringData()
	}
	return ""
}

func (self *LockError) Error() string {
	if self.Err == nil {
		return fmt.Sprintf("error code %d", self.Result)
	}
	return fmt.Sprintf("%d %s", self.Result, self.Err.Error())
}

type Lock struct {
	db      *Database
	lockId  [16]byte
	lockKey [16]byte
	timeout uint32
	expried uint32
	count   uint16
	rcount  uint8
}

func NewLock(db *Database, lockKey [16]byte, timeout uint32, expried uint32) *Lock {
	return &Lock{db, db.GenLockId(), lockKey, timeout, expried, 0, 0}
}

func (self *Lock) GetLockKey() [16]byte {
	return self.lockKey
}

func (self *Lock) GetLockId() [16]byte {
	return self.lockId
}

func (self *Lock) GetTimeout() uint16 {
	return uint16(self.timeout & 0xffff)
}

func (self *Lock) GetTimeoutFlag() uint16 {
	return uint16(self.timeout >> 16)
}

func (self *Lock) SetTimeoutFlag(flag uint16) uint16 {
	oflag := self.GetTimeoutFlag()
	self.timeout = (self.timeout & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (self *Lock) GetExpried() uint16 {
	return uint16(self.expried & 0xffff)
}

func (self *Lock) GetExpriedFlag() uint16 {
	return uint16(self.expried >> 16)
}

func (self *Lock) SetExpriedFlag(flag uint16) uint16 {
	oflag := self.GetExpriedFlag()
	self.expried = (self.expried & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (self *Lock) GetCount() uint16 {
	return self.count
}

func (self *Lock) SetCount(count uint16) uint16 {
	ocount := self.count
	if count > 0 {
		self.count = count - 1
	} else {
		self.count = 0
	}
	return ocount
}

func (self *Lock) GetRcount() uint8 {
	return self.rcount
}

func (self *Lock) SetRcount(rcount uint8) uint8 {
	orcount := self.rcount
	if rcount > 0 {
		self.rcount = rcount - 1
	} else {
		self.rcount = 0
	}
	return orcount
}

func (self *Lock) doLock(flag uint8, lockId [16]byte, timeout uint32, expried uint32, count uint16, rcount uint8, data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.db.GenRequestId()},
		Flag: self.buildLockFlag(flag, data), DbId: self.db.dbId, LockId: lockId, LockKey: self.lockKey, TimeoutFlag: uint16(timeout >> 16), Timeout: uint16(timeout),
		ExpriedFlag: uint16(expried >> 16), Expried: uint16(expried), Count: count, Rcount: rcount, Data: data}
	resultCommand, err := self.db.executeCommand(command, int(command.Timeout+1))
	if err != nil {
		return nil, err
	}

	lockResultCommand, ok := resultCommand.(*protocol.LockResultCommand)
	if !ok {
		return nil, errors.New("unknown command result")
	}
	return lockResultCommand, nil
}

func (self *Lock) doUnlock(flag uint8, lockId [16]byte, timeout uint32, expried uint32, count uint16, rcount uint8, data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.db.GenRequestId()},
		Flag: self.buildUnlockFlag(flag, data), DbId: self.db.dbId, LockId: lockId, LockKey: self.lockKey, TimeoutFlag: uint16(timeout >> 16), Timeout: uint16(timeout),
		ExpriedFlag: uint16(expried >> 16), Expried: uint16(expried), Count: count, Rcount: rcount, Data: data}
	resultCommand, err := self.db.executeCommand(command, int(command.Timeout+1))
	if err != nil {
		return nil, err
	}

	lockResultCommand, ok := resultCommand.(*protocol.LockResultCommand)
	if !ok {
		return nil, errors.New("unknown command result")
	}
	return lockResultCommand, nil
}

func (self *Lock) Lock() *LockError {
	lockResultCommand, err := self.doLock(0, self.lockId, self.timeout, self.expried, self.count, self.rcount, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (self *Lock) LockWithData(data *protocol.LockCommandData) *LockError {
	lockResultCommand, err := self.doLock(0, self.lockId, self.timeout, self.expried, self.count, self.rcount, data)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (self *Lock) Unlock() *LockError {
	lockResultCommand, err := self.doUnlock(0, self.lockId, self.timeout, self.expried, self.count, self.rcount, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (self *Lock) UnlockWithData(data *protocol.LockCommandData) *LockError {
	lockResultCommand, err := self.doUnlock(0, self.lockId, self.timeout, self.expried, self.count, self.rcount, data)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (self *Lock) LockShow() *LockError {
	lockResultCommand, err := self.doLock(protocol.LOCK_FLAG_SHOW_WHEN_LOCKED, [16]byte{}, 0, 0, 0xffff, 0xff, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == protocol.RESULT_UNOWN_ERROR {
		return &LockError{0, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("show error")}
}

func (self *Lock) LockUpdate() *LockError {
	lockResultCommand, err := self.doLock(protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED, self.lockId, self.timeout, self.expried, self.count, self.rcount, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 || lockResultCommand.Result == protocol.RESULT_LOCKED_ERROR {
		return &LockError{0, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("update error")}
}

func (self *Lock) LockUpdateWithData(data *protocol.LockCommandData) *LockError {
	lockResultCommand, err := self.doLock(protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED, self.lockId, self.timeout, self.expried, self.count, self.rcount, data)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 || lockResultCommand.Result == protocol.RESULT_LOCKED_ERROR {
		return &LockError{0, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("update error")}
}

func (self *Lock) UnlockHead() *LockError {
	lockResultCommand, err := self.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED, [16]byte{}, self.timeout, self.expried, self.count, self.rcount, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (self *Lock) UnlockHeadWithData(data *protocol.LockCommandData) *LockError {
	lockResultCommand, err := self.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED, [16]byte{}, self.timeout, self.expried, self.count, self.rcount, data)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (self *Lock) UnlockRetoLockWait() *LockError {
	lockResultCommand, err := self.doUnlock(protocol.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT, self.lockId, self.timeout, self.expried, self.count, self.rcount, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (self *Lock) UnlockRetoLockWaitWithData(data *protocol.LockCommandData) *LockError {
	lockResultCommand, err := self.doUnlock(protocol.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT, self.lockId, self.timeout, self.expried, self.count, self.rcount, data)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (self *Lock) UnlockHeadRetoLockWait() *LockError {
	lockResultCommand, err := self.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED|protocol.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT, [16]byte{}, self.timeout, self.expried, self.count, self.rcount, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (self *Lock) UnlockHeadRetoLockWaitWithData(data *protocol.LockCommandData) *LockError {
	lockResultCommand, err := self.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED|protocol.UNLOCK_FLAG_SUCCED_TO_LOCK_WAIT, [16]byte{}, self.timeout, self.expried, self.count, self.rcount, data)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (self *Lock) CancelWait() *LockError {
	lockResultCommand, err := self.doUnlock(protocol.UNLOCK_FLAG_CANCEL_WAIT_LOCK_WHEN_UNLOCKED, self.lockId, self.timeout, self.expried, self.count, self.rcount, nil)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == protocol.RESULT_LOCKED_ERROR {
		return &LockError{0, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("cancel error")}
}

func (self *Lock) SendLock() error {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.db.GenRequestId()},
		Flag: 0, DbId: self.db.dbId, LockId: self.lockId, LockKey: self.lockKey, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
		ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
	return self.db.sendCommand(command)
}

func (self *Lock) SendLockWithData(data *protocol.LockCommandData) error {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK, RequestId: self.db.GenRequestId()},
		Flag: self.buildLockFlag(0, data), DbId: self.db.dbId, LockId: self.lockId, LockKey: self.lockKey, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
		ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount, Data: data}
	return self.db.sendCommand(command)
}

func (self *Lock) SendUnlock() error {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.db.GenRequestId()},
		Flag: 0, DbId: self.db.dbId, LockId: self.lockId, LockKey: self.lockKey, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
		ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount}
	return self.db.sendCommand(command)
}

func (self *Lock) SendUnlockWithData(data *protocol.LockCommandData) error {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: self.db.GenRequestId()},
		Flag: self.buildUnlockFlag(0, data), DbId: self.db.dbId, LockId: self.lockId, LockKey: self.lockKey, TimeoutFlag: uint16(self.timeout >> 16), Timeout: uint16(self.timeout),
		ExpriedFlag: uint16(self.expried >> 16), Expried: uint16(self.expried), Count: self.count, Rcount: self.rcount, Data: data}
	return self.db.sendCommand(command)
}

func (self *Lock) buildLockFlag(flag uint8, data *protocol.LockCommandData) uint8 {
	if data != nil && data.Data != nil {
		return flag | protocol.LOCK_FLAG_CONTAINS_DATA
	}
	return flag
}

func (self *Lock) buildUnlockFlag(flag uint8, data *protocol.LockCommandData) uint8 {
	if data != nil && data.Data != nil {
		return flag | protocol.UNLOCK_FLAG_CONTAINS_DATA
	}
	return flag
}
