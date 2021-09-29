package client

import "github.com/snower/slock/protocol"

type Semaphore struct {
	db            *Database
	semaphore_key [16]byte
	timeout       uint32
	expried       uint32
	count         uint16
}

func NewSemaphore(db *Database, semaphore_key [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
	return &Semaphore{db, semaphore_key, timeout, expried, count}
}

func (self *Semaphore) GetSemaphoreKey() [16]byte {
	return self.semaphore_key
}

func (self *Semaphore) GetTimeout() uint32 {
	return self.timeout
}

func (self *Semaphore) GetExpried() uint32 {
	return self.expried
}

func (self *Semaphore) Acquire() error {
	lock := &Lock{self.db, self.db.GenLockId(), self.semaphore_key, self.timeout, self.expried, self.count, 0}
	return lock.Lock()
}

func (self *Semaphore) Release() error {
	lock := &Lock{self.db, [16]byte{}, self.semaphore_key, self.timeout, self.expried, self.count, 0}
	err := lock.UnlockHead()
	if err != nil && err.Err != nil {
		return err
	}
	return nil
}

func (self *Semaphore) ReleaseN(n int) (int, error) {
	lock := &Lock{self.db, [16]byte{}, self.semaphore_key, self.timeout, self.expried, self.count, 0}
	for i := 0; i < n; i++ {
		err := lock.UnlockHead()
		if err != nil && err.Err != nil {
			return i + 1, err
		}
	}
	return n, nil
}

func (self *Semaphore) ReleaseAll() error {
	lock := &Lock{self.db, [16]byte{}, self.semaphore_key, self.timeout, self.expried, self.count, 0}
	for {
		err := lock.UnlockHead()
		if err != nil && err.Err != nil {
			return err
		}
	}
}

func (self *Semaphore) Count() (int, error) {
	lock := &Lock{self.db, self.db.GenLockId(), self.semaphore_key, 0, 0, self.count, 0}
	err := lock.LockShow()
	if err == nil {
		return 0, nil
	}

	if err.CommandResult.Result == protocol.RESULT_UNLOCK_ERROR {
		return 0, nil
	}

	if err.CommandResult.Result == protocol.RESULT_UNOWN_ERROR {
		return int(err.CommandResult.Lcount), nil
	}

	if err.CommandResult.Result == protocol.RESULT_TIMEOUT {
		return int(self.count), nil
	}
	return 0, err
}
