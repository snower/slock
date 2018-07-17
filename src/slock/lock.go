package slock

import (
    "time"
    "sync"
)

type LockManager struct {
    lock_db *LockDB
    Locked        bool
    DbId          uint8
    LockKey       [16]byte
    Locks         *LockQueue
    LockCount     int32
    UnLockedCount int32
    GLock *sync.Mutex
    current_lock  *Lock
}

func NewLockManager(lock_db *LockDB, command *LockCommand, glock *sync.Mutex) *LockManager {
    return &LockManager{lock_db,false, command.DbId, command.LockKey, nil, 0, 0, glock, nil}
}

func (self *LockManager) GetDB() *LockDB{
    return self.lock_db
}

func (self *LockManager) AddLock(lock *Lock) *Lock {
    if self.Locks == nil {
        self.Locks = NewLockQueue(4, 4)
    }

    self.Locks.Push(lock)
    lock.Locked = true
    self.LockCount++

    if self.current_lock == nil {
        self.current_lock = lock
    }
    return lock
}

func (self *LockManager) RemoveLock(lock *Lock) *Lock {
    lock.Locked = false
    self.UnLockedCount++
    if self.current_lock == lock {
        self.current_lock = nil
    }
    return lock
}

func (self *LockManager) CurrentLock() *Lock {
    if self.current_lock != nil {
        return self.current_lock
    }

    lock := self.Locks.Head()
    for ;lock != nil; {
        if lock.Locked && !lock.Timeouted && !lock.Expried {
            self.current_lock = lock
            return lock
        }else{
            self.Locks.Pop()
        }
        lock = self.Locks.Head()
    }
    return nil
}

func (self *LockManager) Clear() (err error) {
    self.Locks = nil
    self.LockCount = 0
    self.UnLockedCount = 0
    return nil
}

type Lock struct {
    manager             *LockManager
    Command             *LockCommand
    StartTime           int64
    ExpriedTime         int64
    TimeoutTime         int64
    Locked              bool
    Timeouted           bool
    Expried             bool
    TimeoutCheckedCount uint32
    ExpriedCheckedCount uint32
}

func NewLock(manager *LockManager, command *LockCommand) *Lock {
    now := time.Now().Unix()
    return &Lock{manager, command, now, now + int64(command.Expried), now + int64(command.Timeout), false, false, false, 0, 0}
}

func (self *Lock) GetDB() *LockDB{
    return self.manager.GetDB()
}