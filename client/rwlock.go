package client

import (
	"errors"
	"github.com/snower/slock/protocol"
	"sync"
)

type RWLock struct {
	db      *Database
	lockKey [16]byte
	timeout uint32
	expried uint32
	rlocks  []*Lock
	wlock   *Lock
	glock   *sync.Mutex
}

func NewRWLock(db *Database, lockKey [16]byte, timeout uint32, expried uint32) *RWLock {
	return &RWLock{db, lockKey, timeout, expried, make([]*Lock, 0), nil, &sync.Mutex{}}
}

func (self *RWLock) GetLockKey() [16]byte {
	return self.lockKey
}

func (self *RWLock) GetTimeout() uint32 {
	return self.timeout
}

func (self *RWLock) GetExpried() uint32 {
	return self.expried
}

func (self *RWLock) RLock() (*protocol.LockResultCommand, error) {
	rlock := &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, self.expried, 0xffff, 0}
	result, err := rlock.Lock()
	if err == nil {
		self.glock.Lock()
		self.rlocks = append(self.rlocks, rlock)
		self.glock.Unlock()
	}
	return result, err
}

func (self *RWLock) RLockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	rlock := &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, self.expried, 0xffff, 0}
	result, err := rlock.LockWithData(data)
	if err == nil {
		self.glock.Lock()
		self.rlocks = append(self.rlocks, rlock)
		self.glock.Unlock()
	}
	return result, err
}

func (self *RWLock) RUnlock() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if len(self.rlocks) == 0 {
		self.glock.Unlock()
		return nil, &LockError{protocol.RESULT_UNLOCK_ERROR, nil, errors.New("rwlock is unlock")}
	}
	rlock := self.rlocks[0]
	self.rlocks = self.rlocks[1:]
	self.glock.Unlock()
	return rlock.Unlock()
}

func (self *RWLock) RUnlockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if len(self.rlocks) == 0 {
		self.glock.Unlock()
		return nil, &LockError{protocol.RESULT_UNLOCK_ERROR, nil, errors.New("rwlock is unlock")}
	}
	rlock := self.rlocks[0]
	self.rlocks = self.rlocks[1:]
	self.glock.Unlock()
	return rlock.UnlockWithData(data)
}

func (self *RWLock) Lock() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.wlock == nil {
		self.wlock = &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, self.expried, 0, 0}
	}
	self.glock.Unlock()
	return self.wlock.Lock()
}

func (self *RWLock) LockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.wlock == nil {
		self.wlock = &Lock{self.db, self.db.GenLockId(), self.lockKey, self.timeout, self.expried, 0, 0}
	}
	self.glock.Unlock()
	return self.wlock.LockWithData(data)
}

func (self *RWLock) Unlock() (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.wlock == nil {
		self.glock.Unlock()
		return nil, &LockError{protocol.RESULT_UNLOCK_ERROR, nil, errors.New("rwlock is unlock")}
	}
	self.glock.Unlock()
	return self.wlock.Unlock()
}

func (self *RWLock) UnlockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	self.glock.Lock()
	if self.wlock == nil {
		self.glock.Unlock()
		return nil, &LockError{protocol.RESULT_UNLOCK_ERROR, nil, errors.New("rwlock is unlock")}
	}
	self.glock.Unlock()
	return self.wlock.UnlockWithData(data)
}
