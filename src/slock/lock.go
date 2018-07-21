package slock

import (
    "time"
    "sync"
)

type LockManager struct {
    lock_db        *LockDB
    locked         uint16
    db_id          uint8
    lock_key       [16]byte
    current_lock   *Lock
    locks          *LockQueue
    lock_maps      map[[16]byte]*Lock
    wait_locks     *LockQueue
    glock          *sync.Mutex
    glock_index    int
    free_locks     []*Lock
    free_lock_count int
    freed           bool
}

func NewLockManager(lock_db *LockDB, command *LockCommand, glock *sync.Mutex, glock_index int) *LockManager {
    return &LockManager{lock_db,0, command.DbId, command.LockKey, nil, nil, nil, nil, glock, glock_index, nil, -1, false}
}

func (self *LockManager) GetDB() *LockDB{
    return self.lock_db
}

func (self *LockManager) AddLock(lock *Lock) *Lock {
    lock.locked = true
    lock.locked_freed = false

    if self.current_lock == nil {
        self.current_lock = lock
        return lock
    }

    if self.locks == nil {
        self.locks = NewLockQueue(4, 4)
    }

    if self.lock_maps == nil {
        self.lock_maps = make(map[[16]byte]*Lock, 0)
    }

    self.locks.Push(lock)
    self.lock_maps[lock.command.LockId] = lock
    return lock
}

func (self *LockManager) RemoveLock(lock *Lock) *Lock {
    lock.locked = false

    if self.current_lock == lock {
        self.current_lock = nil
        lock.locked_freed = true

        if self.locks == nil {
            return lock
        }

        locked_lock := self.locks.Pop()
        for ; locked_lock != nil; {
            if locked_lock.locked {
                _, ok := self.lock_maps[locked_lock.command.LockId]
                if ok {
                    delete(self.lock_maps, locked_lock.command.LockId)
                }
                self.current_lock = locked_lock
                return lock
            }

            locked_lock.locked_freed = true
            locked_lock = self.locks.Pop()
        }

        return lock
    }

    if self.lock_maps == nil {
        return lock
    }

    _, ok := self.lock_maps[lock.command.LockId]
    if ok {
        delete(self.lock_maps, lock.command.LockId)
    }

    locked_lock := self.locks.Head()
    for ; locked_lock != nil; {
        if locked_lock.locked {
            break
        }

        self.locks.Pop()
        locked_lock.locked_freed = true
        locked_lock = self.locks.Head()
    }
    return lock
}

func (self *LockManager) GetLockedLock(command *LockCommand) *Lock {
    if self.current_lock.command.LockId == command.LockId {
        return self.current_lock
    }

    if self.lock_maps == nil {
        return nil
    }

    locked_lock, ok := self.lock_maps[command.LockId]
    if ok {
        return locked_lock
    }
    return nil
}

func (self *LockManager) AddWaitLock(lock *Lock) *Lock {
    if self.wait_locks == nil {
        self.wait_locks = NewLockQueue(4, 4)
    }

    lock.wait_freed = false
    self.wait_locks.Push(lock)
    return lock
}

func (self *LockManager) GetWaitLock() *Lock {
    if self.wait_locks == nil {
        return nil
    }

    lock := self.wait_locks.Head()
    for ; lock != nil; {
        if lock.timeouted {
            self.wait_locks.Pop()
            lock.wait_freed = true
            lock = self.wait_locks.Head()
        }
        return lock
    }
    return nil
}

func (self *LockManager) FreeLock(lock *Lock) *Lock{
    if self.free_lock_count < 63 {
        if self.free_locks == nil {
            self.free_locks = make([]*Lock, 64)
        }
        self.free_lock_count++
        self.free_locks[self.free_lock_count] = lock
        lock.protocol = nil
        lock.command = nil
        lock.timeout_checked_count = 0
        lock.expried_checked_count = 0
    }
    return lock
}

func (self *LockManager) GetOrNewLock(protocol *ServerProtocol, command *LockCommand) *Lock {
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
    protocol            *ServerProtocol
    start_time          int64
    expried_time        int64
    timeout_time        int64
    locked              bool
    timeouted           bool
    expried             bool
    locked_freed        bool
    wait_freed          bool
    timeout_checked_count uint32
    expried_checked_count uint32
}

func NewLock(manager *LockManager, protocol *ServerProtocol, command *LockCommand) *Lock {
    now := time.Now().Unix()
    return &Lock{manager, command, protocol,now, now + int64(command.Expried), now + int64(command.Timeout), false, false, false, true, true, 0, 0}
}

func (self *Lock) GetDB() *LockDB{
    return self.manager.GetDB()
}