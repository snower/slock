package client

import (
	"github.com/snower/slock/protocol"
	"sync"
)

type GroupEvent struct {
	db        *Database
	groupKey  [16]byte
	clientId  uint64
	versionId uint64
	timeout   uint32
	expried   uint32
	eventLock *Lock
	checkLock *Lock
	waitLock  *Lock
	glock     *sync.Mutex
}

func NewGroupEvent(db *Database, groupKey [16]byte, clientId uint64, versionId uint64, timeout uint32, expried uint32) *GroupEvent {
	return &GroupEvent{db, groupKey, clientId, versionId, timeout,
		expried, nil, nil, nil, &sync.Mutex{}}
}

func (self *GroupEvent) GetGroupKey() [16]byte {
	return self.groupKey
}

func (self *GroupEvent) GetClientId() uint64 {
	return self.clientId
}

func (self *GroupEvent) GetVersionId() uint64 {
	return self.versionId
}

func (self *GroupEvent) GetTimeout() uint32 {
	return self.timeout
}

func (self *GroupEvent) GetExpried() uint32 {
	return self.expried
}

func (self *GroupEvent) Clear() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	lockId := [16]byte{byte(self.versionId), byte(self.versionId >> 8), byte(self.versionId >> 16), byte(self.versionId >> 24),
		byte(self.versionId >> 32), byte(self.versionId >> 40), byte(self.versionId >> 48), byte(self.versionId >> 56),
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	timeout := self.timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)<<16
	self.eventLock = &Lock{self.db, lockId, self.groupKey, timeout, self.expried, 0, 0}
	self.glock.Unlock()
	return self.eventLock.LockUpdate()
}

func (self *GroupEvent) Set() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, [16]byte{}, self.groupKey, self.timeout, self.expried, 0, 0}
	}
	self.glock.Unlock()

	result, err := self.eventLock.UnlockHead()
	if err == nil || (result != nil && result.Result == protocol.RESULT_UNLOCK_ERROR) {
		return result, nil
	}
	return result, err
}

func (self *GroupEvent) SetWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, [16]byte{}, self.groupKey, self.timeout, self.expried, 0, 0}
	}
	self.glock.Unlock()

	result, err := self.eventLock.UnlockHeadWithData(data)
	if err == nil || (result != nil && result.Result == protocol.RESULT_UNLOCK_ERROR) {
		return result, nil
	}
	return result, err
}

func (self *GroupEvent) IsSet() (bool, error) {
	self.checkLock = &Lock{self.db, self.db.GenLockId(), self.groupKey, 0, 0, 0, 0}
	result, err := self.checkLock.Lock()
	if err == nil {
		return true, nil
	}
	if result != nil && result.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}

func (self *GroupEvent) Wakeup() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	lockId := [16]byte{}
	timeout := self.timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)<<16
	self.eventLock = &Lock{self.db, lockId, self.groupKey, timeout, self.expried, 0, 0}
	self.glock.Unlock()

	result, err := self.eventLock.UnlockHeadRetoLockWait()
	if result != nil && result.Result == protocol.RESULT_SUCCED {
		rlockId := result.LockId
		if rlockId != lockId {
			self.versionId = uint64(rlockId[0]) | uint64(rlockId[1])<<8 | uint64(rlockId[2])<<16 | uint64(rlockId[3])<<24 | uint64(rlockId[4])<<32 | uint64(rlockId[5])<<40 | uint64(rlockId[6])<<48 | uint64(rlockId[7])<<56
		}
		return result, nil
	}
	return result, err
}

func (self *GroupEvent) WakeupWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	lockId := [16]byte{}
	timeout := self.timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)<<16
	self.eventLock = &Lock{self.db, lockId, self.groupKey, timeout, self.expried, 0, 0}
	self.glock.Unlock()

	result, err := self.eventLock.UnlockHeadRetoLockWaitWithData(data)
	if result != nil && result.Result == protocol.RESULT_SUCCED {
		rlockId := result.LockId
		if rlockId != lockId {
			self.versionId = uint64(rlockId[0]) | uint64(rlockId[1])<<8 | uint64(rlockId[2])<<16 | uint64(rlockId[3])<<24 | uint64(rlockId[4])<<32 | uint64(rlockId[5])<<40 | uint64(rlockId[6])<<48 | uint64(rlockId[7])<<56
		}
		return result, nil
	}
	return result, err
}

func (self *GroupEvent) Wait(timeout uint32) (*protocol.LockResultCommand, error) {
	lockId := [16]byte{byte(self.versionId), byte(self.versionId >> 8), byte(self.versionId >> 16), byte(self.versionId >> 24),
		byte(self.versionId >> 32), byte(self.versionId >> 40), byte(self.versionId >> 48), byte(self.versionId >> 56),
		byte(self.clientId), byte(self.clientId >> 8), byte(self.clientId >> 16), byte(self.clientId >> 24),
		byte(self.clientId >> 32), byte(self.clientId >> 40), byte(self.clientId >> 48), byte(self.clientId >> 56)}
	self.waitLock = &Lock{self.db, lockId, self.groupKey, timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)<<16, 0, 0, 0}
	lockResultCommand, err := self.waitLock.doLock(0, self.waitLock.lockId, self.waitLock.timeout, self.waitLock.expried, self.waitLock.count, self.waitLock.rcount, nil)
	if err != nil {
		return lockResultCommand, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result == protocol.RESULT_SUCCED {
		rlockId := lockResultCommand.LockId
		if rlockId != lockId {
			self.versionId = uint64(rlockId[0]) | uint64(rlockId[1])<<8 | uint64(rlockId[2])<<16 | uint64(rlockId[3])<<24 | uint64(rlockId[4])<<32 | uint64(rlockId[5])<<40 | uint64(rlockId[6])<<48 | uint64(rlockId[7])<<56
		}
		return lockResultCommand, nil
	}

	if lockResultCommand.Result == protocol.RESULT_TIMEOUT {
		return lockResultCommand, WaitTimeout
	}
	return lockResultCommand, err
}

func (self *GroupEvent) WaitAndTimeoutRetryClear(timeout uint32) (*protocol.LockResultCommand, error) {
	lockId := [16]byte{byte(self.versionId), byte(self.versionId >> 8), byte(self.versionId >> 16), byte(self.versionId >> 24),
		byte(self.versionId >> 32), byte(self.versionId >> 40), byte(self.versionId >> 48), byte(self.versionId >> 56),
		byte(self.clientId), byte(self.clientId >> 8), byte(self.clientId >> 16), byte(self.clientId >> 24),
		byte(self.clientId >> 32), byte(self.clientId >> 40), byte(self.clientId >> 48), byte(self.clientId >> 56)}
	timeout = timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)<<16
	self.waitLock = &Lock{self.db, lockId, self.groupKey, timeout, 0, 0, 0}
	lockResultCommand, err := self.waitLock.doLock(0, self.waitLock.lockId, self.waitLock.timeout, self.waitLock.expried, self.waitLock.count, self.waitLock.rcount, nil)
	if err != nil {
		return lockResultCommand, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result == protocol.RESULT_SUCCED {
		rlockId := lockResultCommand.LockId
		if rlockId != lockId {
			self.versionId = uint64(rlockId[0]) | uint64(rlockId[1])<<8 | uint64(rlockId[2])<<16 | uint64(rlockId[3])<<24 | uint64(rlockId[4])<<32 | uint64(rlockId[5])<<40 | uint64(rlockId[6])<<48 | uint64(rlockId[7])<<56
		}
		return lockResultCommand, nil
	}

	if lockResultCommand.Result == protocol.RESULT_TIMEOUT {
		self.glock.Lock()
		lockId = [16]byte{byte(self.versionId), byte(self.versionId >> 8), byte(self.versionId >> 16), byte(self.versionId >> 24),
			byte(self.versionId >> 32), byte(self.versionId >> 40), byte(self.versionId >> 48), byte(self.versionId >> 56),
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		timeout = self.timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)<<16
		self.eventLock = &Lock{self.db, lockId, self.groupKey, timeout, self.expried, 0, 0}
		self.glock.Unlock()

		rresult, rerr := self.eventLock.LockUpdate()
		if rerr == nil {
			if rresult.Result == protocol.RESULT_SUCCED {
				_, _ = self.eventLock.Unlock()
				return rresult, nil
			}
			if rresult.Result == protocol.RESULT_LOCKED_ERROR {
				return lockResultCommand, WaitTimeout
			}
		}
	}
	return lockResultCommand, err
}
