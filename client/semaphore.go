package client

import "github.com/snower/slock/protocol"

type Semaphore struct {
	db           *Database
	semaphoreKey [16]byte
	timeout      uint32
	expried      uint32
	count        uint16
}

func NewSemaphore(db *Database, semaphoreKey [16]byte, timeout uint32, expried uint32, count uint16) *Semaphore {
	if count > 0 {
		count = count - 1
	}
	return &Semaphore{db, semaphoreKey, timeout, expried, count}
}

func (self *Semaphore) GetSemaphoreKey() [16]byte {
	return self.semaphoreKey
}

func (self *Semaphore) GetTimeout() uint32 {
	return self.timeout
}

func (self *Semaphore) GetExpried() uint32 {
	return self.expried
}

func (self *Semaphore) Acquire() (*protocol.LockResultCommand, error) {
	lock := &Lock{self.db, self.db.GenLockId(), self.semaphoreKey, self.timeout, self.expried, self.count, 0}
	return lock.Lock()
}

func (self *Semaphore) Release() (*protocol.LockResultCommand, error) {
	lock := &Lock{self.db, [16]byte{}, self.semaphoreKey, self.timeout, self.expried, self.count, 0}
	return lock.UnlockHead()
}

func (self *Semaphore) ReleaseN(n int) (int, error) {
	lock := &Lock{self.db, [16]byte{}, self.semaphoreKey, self.timeout, self.expried, self.count, 0}
	for i := 0; i < n; i++ {
		_, err := lock.UnlockHead()
		if err != nil {
			return i + 1, err
		}
	}
	return n, nil
}

func (self *Semaphore) ReleaseAll() error {
	lock := &Lock{self.db, [16]byte{}, self.semaphoreKey, self.timeout, self.expried, self.count, 0}
	for {
		result, err := lock.UnlockHead()
		if result != nil && result.Result == protocol.RESULT_UNLOCK_ERROR {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (self *Semaphore) Count() (int, error) {
	lock := &Lock{self.db, self.db.GenLockId(), self.semaphoreKey, 0, 0, self.count, 0}
	result, err := lock.LockShow()
	if result != nil {
		if result.Result == protocol.RESULT_SUCCED {
			return 0, nil
		}
		if result.Result == protocol.RESULT_UNOWN_ERROR {
			return int(result.Lcount), nil
		}
		if result.Result == protocol.RESULT_TIMEOUT {
			return int(self.count), nil
		}
	}
	return 0, err
}
