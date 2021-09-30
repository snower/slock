package client

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

func (self *RLock) Lock() error {
	return self.lock.Lock()
}

func (self *RLock) Unlock() error {
	return self.lock.Unlock()
}
