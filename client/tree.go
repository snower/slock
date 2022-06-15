package client

import "github.com/snower/slock/protocol"

var RootKey = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type TreeLockLock struct {
	db       *Database
	treeLock *TreeLock
	lock     *Lock
}

func (self *TreeLockLock) Lock() *LockError {
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

func (self *TreeLockLock) Unlock() *LockError {
	lockResultCommand, err := self.lock.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_TREE_LOCK, self.lock.lockId, self.lock.timeout, self.lock.expried, 0xffff, 0)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

type TreeLock struct {
	db        *Database
	parentKey [16]byte
	lockKey   [16]byte
	timeout   uint32
	expried   uint32
	isRoot    bool
}

func NewTreeLock(db *Database, lockKey [16]byte, parentKey [16]byte, timeout uint32, expried uint32) *TreeLock {
	return &TreeLock{db, parentKey, lockKey, timeout, expried, parentKey == RootKey}
}

func (self *TreeLock) NewLock() *TreeLockLock {
	lock := &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, self.expried, 0xffff, 0}
	return &TreeLockLock{self.db, self, lock}
}

func (self *TreeLock) NewChild() *TreeLock {
	return &TreeLock{self.db, self.lockKey, self.db.GenLockId(), self.timeout, self.expried, false}
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

	parentLock := &Lock{self.db, self.db.GenLockId(), self.parentKey, self.timeout, self.expried, 0, 0}
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
