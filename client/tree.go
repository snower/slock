package client

import "github.com/snower/slock/protocol"

var RootKey = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type TreeLeafLock struct {
	db       *Database
	treeLock *TreeLock
	lock     *Lock
}

func (self *TreeLeafLock) Lock() (*protocol.LockResultCommand, error) {
	parentLock, childLock, cerr := self.treeLock.checkTreeLock()
	if cerr != nil {
		return nil, cerr
	}

	lockResultCommand, err := self.lock.doLock(0, self.lock.lockId, self.lock.timeout|(uint32(protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)<<16), self.lock.expried, 0xffff, 1, nil)
	if err != nil {
		if childLock != nil {
			_, _ = childLock.Unlock()
		}
		if parentLock != nil {
			_, _ = parentLock.Unlock()
		}
		return lockResultCommand, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result != 0 {
		if childLock != nil {
			_, _ = childLock.Unlock()
		}
		if parentLock != nil {
			_, _ = parentLock.Unlock()
		}
		return lockResultCommand, &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return lockResultCommand, nil
}

func (self *TreeLeafLock) Unlock() (*protocol.LockResultCommand, error) {
	lockResultCommand, err := self.lock.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_TREE_LOCK, self.lock.lockId, self.lock.timeout|(uint32(protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)<<16), self.lock.expried, 0xffff, 1, nil)
	if err != nil {
		return lockResultCommand, &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return lockResultCommand, &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return lockResultCommand, nil
}

func (self *TreeLeafLock) UnlockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	lockResultCommand, err := self.lock.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_TREE_LOCK, self.lock.lockId, self.lock.timeout|(uint32(protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)<<16), self.lock.expried, 0xffff, 1, data)
	if err != nil {
		return lockResultCommand, &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return lockResultCommand, &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return lockResultCommand, nil
}

func (self *TreeLeafLock) GetLockKey() [16]byte {
	return self.lock.GetLockKey()
}

func (self *TreeLeafLock) GetLockId() [16]byte {
	return self.lock.lockId
}

type TreeLock struct {
	db        *Database
	parentKey [16]byte
	lockKey   [16]byte
	timeout   uint32
	expried   uint32
	isRoot    bool
	leafLock  *TreeLeafLock
}

func NewTreeLock(db *Database, lockKey [16]byte, parentKey [16]byte, timeout uint32, expried uint32) *TreeLock {
	return &TreeLock{db, parentKey, lockKey, timeout, expried, parentKey == RootKey, nil}
}

func (self *TreeLock) NewLeafLock() *TreeLeafLock {
	lock := &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout | uint32(protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)<<16, self.expried, 0xffff, 1}
	return &TreeLeafLock{self.db, self, lock}
}

func (self *TreeLock) LoadLeafLock(lockId [16]byte) *TreeLeafLock {
	lock := &Lock{self.db, lockId, self.lockKey, self.timeout | (protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY << 16), self.expried, 0xffff, 1}
	return &TreeLeafLock{self.db, self, lock}
}

func (self *TreeLock) NewChild() *TreeLock {
	return &TreeLock{self.db, self.lockKey, self.db.GenLockId(), self.timeout, self.expried, false, nil}
}

func (self *TreeLock) LoadChild(lockKey [16]byte) *TreeLock {
	return &TreeLock{self.db, self.lockKey, lockKey, self.timeout, self.expried, false, nil}
}

func (self *TreeLock) Lock() (*protocol.LockResultCommand, error) {
	checkLock := &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, 0, 0, 0}
	result, err := checkLock.Lock()
	if err != nil {
		return result, err
	}

	if self.leafLock != nil {
		return result, nil
	}
	leafLock := self.NewLeafLock()
	result, err = leafLock.Lock()
	if err != nil {
		return result, err
	}
	self.leafLock = leafLock
	return result, nil
}

func (self *TreeLock) Unlock() (*protocol.LockResultCommand, error) {
	if self.leafLock == nil {
		return nil, nil
	}
	result, err := self.leafLock.Unlock()
	if err != nil {
		return result, err
	}
	self.leafLock = nil
	return result, nil
}

func (self *TreeLock) UnlockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	if self.leafLock == nil {
		return nil, nil
	}
	result, err := self.leafLock.UnlockWithData(data)
	if err != nil {
		return result, err
	}
	self.leafLock = nil
	return result, nil
}

func (self *TreeLock) Wait(timeout uint32) (*protocol.LockResultCommand, error) {
	checkLock := &Lock{self.db, self.db.GenLockId(), self.lockKey, timeout, 0, 0, 0}
	result, err := checkLock.Lock()
	if err != nil {
		return result, err
	}
	if result.Result == protocol.RESULT_TIMEOUT {
		return result, WaitTimeout
	}
	return result, nil
}

func (self *TreeLock) checkTreeLock() (*Lock, *Lock, error) {
	if self.isRoot {
		return nil, nil, nil
	}

	childLock := &Lock{self.db, self.parentKey, self.lockKey, self.timeout, self.expried, 0, 0}
	lockResultCommand, err := childLock.doLock(protocol.LOCK_FLAG_LOCK_TREE_LOCK, childLock.lockId, uint32(protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)<<16, self.expried, 0xffff, 1, nil)
	if err != nil {
		return nil, nil, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result != 0 {
		if lockResultCommand.Result == protocol.RESULT_LOCKED_ERROR {
			return nil, nil, nil
		}
		return nil, nil, &LockError{lockResultCommand.Result, lockResultCommand, err}
	}

	parentLock := &Lock{self.db, self.lockKey, self.parentKey, self.timeout, self.expried, 0, 0}
	lockResultCommand, err = parentLock.doLock(0, parentLock.lockId, uint32(protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY)<<16, self.expried, 0xffff, 1, nil)
	if err != nil {
		_, _ = childLock.Unlock()
		return nil, nil, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result != 0 && lockResultCommand.Result != protocol.RESULT_LOCKED_ERROR {
		_, _ = childLock.Unlock()
		return nil, nil, &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return parentLock, childLock, nil
}

func (self *TreeLock) GetParentKey() [16]byte {
	return self.parentKey
}

func (self *TreeLock) GetLockKey() [16]byte {
	return self.lockKey
}
