package client

import "github.com/snower/slock/protocol"

var RootKey = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type TreeLock struct {
	db      *Database
	lockId  [16]byte
	lockKey [16]byte
	timeout uint32
	expried uint32
	isRoot  bool
}

func NewTreeLock(db *Database, lockKey [16]byte, parentKey [16]byte, timeout uint32, expried uint32) *TreeLock {
	if parentKey == RootKey {
		return &TreeLock{db, db.GenLockId(), lockKey, timeout, expried, true}
	}
	return &TreeLock{db, parentKey, lockKey, timeout, expried, false}
}

func (self *TreeLock) Lock() *LockError {
	treeLock := &Lock{self.db, self.lockId, self.lockKey, self.timeout, self.expried, 0, 0}
	lockResultCommand, err := treeLock.doLock(0, self.lockId, self.timeout, self.expried, 0, 0)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (self *TreeLock) Unlock() *LockError {
	treeLock := &Lock{self.db, self.lockId, self.lockKey, self.timeout, self.expried, 0, 0}
	lockResultCommand, err := treeLock.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_TREE_LOCK, self.lockId, self.timeout, self.expried, 0, 0)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (self *TreeLock) NewChild() (*TreeLock, error) {
	treeLock := &Lock{self.db, self.lockId, self.lockKey, 0, 0, 0, 0}
	lockResultCommand, err := treeLock.doLock(0, self.db.GenLockId(), 0, 0, 0, 0)
	if err == nil && lockResultCommand.Result == 0 {
		return nil, &LockError{0x80, lockResultCommand, err}
	}
	return &TreeLock{self.db, self.lockKey, self.db.GenLockId(), self.timeout, self.expried, false}, nil
}

func (self *TreeLock) GetParentKey() [16]byte {
	if self.isRoot {
		return RootKey
	}
	return self.lockId
}

func (self *TreeLock) GetLockKey() [16]byte {
	return self.lockKey
}
