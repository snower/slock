package client

import (
    "errors"
    "github.com/snower/slock/protocol"
    "sync"
)

type RLock struct {
    db *Database
    lock_key [2]uint64
    timeout uint32
    expried uint32
    locks []*Lock
    glock *sync.Mutex
}

func NewRLock(db *Database, lock_key [2]uint64, timeout uint32, expried uint32) *RLock {
    return &RLock{db, lock_key, timeout, expried, make([]*Lock, 0), &sync.Mutex{}}
}

func (self *RLock) Lock() error {
    if len(self.locks) >= 0xff {
        return &LockError{protocol.RESULT_LOCKED_ERROR, errors.New("rlock count full")}
    }

    lock := &Lock{self.db, self.db.GetRequestId(), self.db.GenLockId(), self.lock_key, self.timeout, self.expried, 0, 0xff}
    err := lock.Lock()
    if err == nil {
        self.glock.Lock()
        self.locks = append(self.locks, lock)
        self.glock.Unlock()
    }
    return err
}

func (self *RLock) Unlock() error {
    self.glock.Lock()

    lock_count := len(self.locks)
    if lock_count == 0 {
        self.glock.Unlock()
        return &LockError{protocol.RESULT_UNLOCK_ERROR, errors.New("rlock is empty")}
    }

    tail_index := lock_count - 1
    lock := self.locks[tail_index]
    self.locks = self.locks[0:tail_index]
    self.glock.Unlock()

    return lock.Unlock()
}