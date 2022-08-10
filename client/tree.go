package client

import "github.com/snower/slock/protocol"

var RootKey = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type TreeLeafLock struct {
	db       *Database
	treeLock *TreeLock
	lock     *Lock
}

func (self *TreeLeafLock) Lock() *LockError {
	parentLock, childLock, cerr := self.treeLock.checkTreeLock()
	if cerr != nil {
		return cerr
	}

	lockResultCommand, err := self.lock.doLock(0, self.lock.lockId, self.lock.timeout, self.lock.expried, 0xffff, 0)
	if err != nil {
		if childLock != nil {
			_ = childLock.Unlock()
		}
		if parentLock != nil {
			_ = parentLock.Unlock()
		}
		return &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result != 0 {
		if childLock != nil {
			_ = childLock.Unlock()
		}
		if parentLock != nil {
			_ = parentLock.Unlock()
		}
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (self *TreeLeafLock) Unlock() *LockError {
	lockResultCommand, err := self.lock.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_TREE_LOCK, self.lock.lockId, self.lock.timeout, self.lock.expried, 0xffff, 0)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
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
	lock := &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, self.expried, 0xffff, 0}
	return &TreeLeafLock{self.db, self, lock}
}

func (self *TreeLock) LoadLeafLock(lockId [16]byte) *TreeLeafLock {
	lock := &Lock{self.db, lockId, self.lockKey, self.timeout, self.expried, 0xffff, 0}
	return &TreeLeafLock{self.db, self, lock}
}

func (self *TreeLock) NewChild() *TreeLock {
	return &TreeLock{self.db, self.lockKey, self.db.GenLockId(), self.timeout, self.expried, false, nil}
}

func (self *TreeLock) LoadChild(lockKey [16]byte) *TreeLock {
	return &TreeLock{self.db, self.lockKey, lockKey, self.timeout, self.expried, false, nil}
}

func (self *TreeLock) Lock() *LockError {
	checkLock := &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, 0, 0, 0}
	err := checkLock.Lock()
	if err != nil {
		return err
	}

	if self.leafLock == nil {
		return nil
	}
	self.leafLock = self.NewLeafLock()
	return self.leafLock.Lock()
}

func (self *TreeLock) Unlock() *LockError {
	if self.leafLock == nil {
		return nil
	}
	return self.leafLock.Unlock()
}

func (self *TreeLock) checkTreeLock() (*Lock, *Lock, *LockError) {
	if self.isRoot {
		return nil, nil, nil
	}

	childLock := &Lock{self.db, self.parentKey, self.lockKey, self.timeout, self.expried, 0, 0}
	lockResultCommand, err := childLock.doLock(protocol.LOCK_FLAG_LOCK_TREE_LOCK, childLock.lockId, 0, self.expried, 0xffff, 0)
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
	lockResultCommand, err = parentLock.doLock(0, parentLock.lockId, 0, self.expried, 0xffff, 0)
	if err != nil {
		_ = childLock.Unlock()
		return nil, nil, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result != 0 && lockResultCommand.Result != protocol.RESULT_LOCKED_ERROR {
		_ = childLock.Unlock()
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
