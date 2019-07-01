package client

import (
    "errors"
    "github.com/snower/slock/protocol"
    "sync"
)

type Semaphore struct {
    db *Database
    semaphore_key [2]uint64
    timeout uint32
    expried uint32
    count uint16
    locks []*Lock
    glock *sync.Mutex
}

func NewSemaphore(db *Database, semaphore_key [2]uint64, timeout uint32, expried uint32, count uint16) *Semaphore {
    return &Semaphore{db, semaphore_key, timeout, expried, count, make([]*Lock, 0), &sync.Mutex{}}
}

func (self *Semaphore) Acquire() error {
    lock := &Lock{self.db, self.db.GetRequestId(), self.db.GenLockId(), self.semaphore_key, self.timeout, self.expried, self.count, 0}
    err := lock.Lock()
    if err == nil {
        self.glock.Lock()
        self.locks = append(self.locks, lock)
        self.glock.Unlock()
    }
    return err
}

func (self *Semaphore) Release() error {
    self.glock.Lock()

    if len(self.locks) == 0 {
        self.glock.Unlock()
        return &LockError{protocol.RESULT_UNLOCK_ERROR, errors.New("semaphore is empty")}
    }

    lock := self.locks[0]
    self.locks = self.locks[1:]
    self.glock.Unlock()

    return lock.Unlock()
}