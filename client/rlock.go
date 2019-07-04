package client

import (
    "errors"
    "github.com/snower/slock/protocol"
)

type RLock struct {
    db *Database
    lock_key [2]uint64
    timeout uint32
    expried uint32
    lock *Lock
    locked_count uint8
}

func NewRLock(db *Database, lock_key [2]uint64, timeout uint32, expried uint32) *RLock {
    lock := &Lock{db, [2]uint64{0, 0}, db.GenLockId(), lock_key, timeout, expried, 0, 0xff}
    return &RLock{db, lock_key, timeout, expried, lock, 0}
}

func (self *RLock) Lock() error {
    if self.locked_count >= 0xff {
        return &LockError{protocol.RESULT_LOCKED_ERROR, errors.New("rlock count full")}
    }

    err := self.lock.Lock()
    if err == nil {
        self.locked_count++
    }
    return err
}

func (self *RLock) Unlock() error {
    if self.locked_count == 0 {
        return &LockError{protocol.RESULT_UNLOCK_ERROR, errors.New("rlock is empty")}
    }
    self.locked_count--
    return self.lock.Unlock()
}