package client

import "github.com/snower/slock/protocol"

type RLock struct {
	db      *Database
	lockKey [16]byte
	timeout uint32
	expried uint32
	lock    *Lock
}

func NewRLock(db *Database, lockKey [16]byte, timeout uint32, expried uint32) *RLock {
	lock := &Lock{db, db.GenLockId(), lockKey, timeout, expried, 0, 0xff}
	return &RLock{db, lockKey, timeout, expried, lock}
}

func (self *RLock) GetLockKey() [16]byte {
	return self.lockKey
}

func (self *RLock) GetTimeout() uint32 {
	return self.timeout
}

func (self *RLock) GetExpried() uint32 {
	return self.expried
}

func (self *RLock) Lock() (*protocol.LockResultCommand, error) {
	return self.lock.Lock()
}

func (self *RLock) LockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	return self.lock.LockWithData(data)
}

func (self *RLock) Unlock() (*protocol.LockResultCommand, error) {
	return self.lock.Unlock()
}

func (self *RLock) UnlockWithData(data *protocol.LockCommandData) (*protocol.LockResultCommand, error) {
	return self.lock.UnlockWithData(data)
}
