package slock

import (
    "time"
    "sync"
)

type LockManager struct {
    lock_db *LockDB
    locked        bool
    db_id          uint8
    lock_key       [16]byte
    locks         *LockQueue
    lock_count     int32
    unlocked_count int32
    glock *sync.Mutex
    glock_index int
    current_lock  *Lock
    free_locks []*Lock
    free_lock_count int
}

func NewLockManager(lock_db *LockDB, command *LockCommand, glock *sync.Mutex, glock_index int) *LockManager {
    return &LockManager{lock_db,false, command.DbId, command.LockKey, nil, 0, 0, glock, glock_index,nil, nil, -1}
}

func (self *LockManager) GetDB() *LockDB{
    return self.lock_db
}

func (self *LockManager) AddLock(lock *Lock) *Lock {
    if self.locks == nil {
        self.locks = NewLockQueue(4, 4)
    }

    self.locks.Push(lock)
    lock.locked = true
    self.lock_count++

    if self.current_lock == nil {
        self.current_lock = lock
    }
    return lock
}

func (self *LockManager) RemoveLock(lock *Lock) *Lock {
    lock.locked = false
    self.unlocked_count++
    if self.current_lock == lock {
        self.current_lock = nil
        if lock.freed {
            self.CurrentLock()
        }
    }
    return lock
}

func (self *LockManager) CurrentLock() *Lock {
    if self.current_lock != nil {
        return self.current_lock
    }

    lock := self.locks.Head()
    for ;lock != nil; {
        if lock.locked && !lock.timeouted && !lock.expried {
            self.current_lock = lock
            return lock
        }else{
            lock = self.locks.Pop()
            if lock.freed {
                if self.free_lock_count < 63 {
                    if self.free_locks == nil {
                        self.free_locks = make([]*Lock, 64)
                    }
                    self.free_lock_count++
                    self.free_locks[self.free_lock_count] = lock
                    lock.protocol = nil
                    lock.command = nil
                    lock.expried = false
                    lock.timeouted = false
                    lock.timeout_checked_count = 0
                    lock.expried_checked_count = 0
                }
            }
        }
        lock = self.locks.Head()
    }
    return nil
}

func (self *LockManager) GetOrNewLock(protocol Protocol, command *LockCommand) *Lock {
    if self.free_lock_count >= 0 {
        lock := self.free_locks[self.free_lock_count]
        self.free_lock_count--
        lock.protocol = protocol
        lock.command = command
        lock.start_time = time.Now().Unix()
        lock.expried_time = lock.start_time + int64(command.Expried)
        lock.timeout_time = lock.start_time + int64(command.Timeout)
        return lock
    }

    return NewLock(self, protocol, command)
}

type Lock struct {
    manager             *LockManager
    command             *LockCommand
    protocol            Protocol
    start_time           int64
    expried_time         int64
    timeout_time         int64
    locked              bool
    timeouted           bool
    expried             bool
    freed               bool
    timeout_checked_count uint32
    expried_checked_count uint32
}

func NewLock(manager *LockManager, protocol Protocol, command *LockCommand) *Lock {
    now := time.Now().Unix()
    return &Lock{manager, command, protocol,now, now + int64(command.Expried), now + int64(command.Timeout), false, false, false, true,0, 0}
}

func (self *Lock) GetDB() *LockDB{
    return self.manager.GetDB()
}