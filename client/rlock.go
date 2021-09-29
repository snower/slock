package client

type RLock struct {
	db       *Database
	lock_key [16]byte
	timeout  uint32
	expried  uint32
	lock     *Lock
}

func NewRLock(db *Database, lock_key [16]byte, timeout uint32, expried uint32) *RLock {
	lock := &Lock{db, db.GenLockId(), lock_key, timeout, expried, 0, 0xff}
	return &RLock{db, lock_key, timeout, expried, lock}
}

func (self *RLock) GetLockKey() [16]byte {
	return self.lock_key
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
