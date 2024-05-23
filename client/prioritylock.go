package client

import (
	"github.com/snower/slock/protocol"
)

type PriorityLock struct {
	db       *Database
	lockKey  [16]byte
	priority uint8
	timeout  uint32
	expried  uint32
	count    uint16
	lock     *Lock
}

func NewPriorityLock(db *Database, lockKey [16]byte, priority uint8, timeout uint32, expried uint32) *PriorityLock {
	return &PriorityLock{db, lockKey, priority, timeout, expried, 0, nil}
}

func (self *PriorityLock) GetLockKey() [16]byte {
	return self.lockKey
}

func (self *PriorityLock) GetTimeout() uint32 {
	return self.timeout
}

func (self *PriorityLock) GetExpried() uint32 {
	return self.expried
}

func (self *PriorityLock) GetCount() uint16 {
	return self.count
}

func (self *PriorityLock) SetCount(count uint16) uint16 {
	ocount := self.count
	if count > 0 {
		self.count = count - 1
	} else {
		self.count = 0
	}
	return ocount
}

func (self *PriorityLock) Lock() (*protocol.LockResultCommand, error) {
	self.lock = &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout | protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY,
		self.expried, self.count, self.priority}
	return self.lock.Lock()
}

func (self *PriorityLock) LockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	self.lock = &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout | protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY,
		self.expried, self.count, self.priority}
	return self.lock.LockWithData(data)
}

func (self *PriorityLock) Unlock() (*protocol.LockResultCommand, error) {
	if self.lock == nil {
		return nil, nil
	}
	return self.lock.Unlock()
}

func (self *PriorityLock) UnlockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	if self.lock == nil {
		return nil, nil
	}
	return self.lock.UnlockWithData(data)
}
