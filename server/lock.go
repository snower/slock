package server

import (
    "sync"
    "github.com/snower/slock/protocol"
)

type LockManager struct {
    lock_db        *LockDB
    lock_key       [16]byte
    current_lock   *Lock
    locks          *LockQueue
    lock_maps      map[[16]byte]*Lock
    wait_locks     *LockQueue
    glock          *sync.Mutex
    free_locks     *LockQueue
    locked         uint16
    ref_count      uint16
    db_id          uint8
    waited         bool
    freed          bool
    glock_index    int8
}

func NewLockManager(lock_db *LockDB, command *protocol.LockCommand, glock *sync.Mutex, glock_index int8, free_locks *LockQueue) *LockManager {
    return &LockManager{lock_db, command.LockKey,
        nil, nil, nil, nil, glock, free_locks, 0, 0,
        command.DbId, false, true, glock_index}
}

func (self *LockManager) GetDB() *LockDB{
    return self.lock_db
}

func (self *LockManager) AddLock(lock *Lock) *Lock {
    if lock.command.ExpriedFlag & 0x0400 == 0 {
        lock.expried_time = self.lock_db.current_time + int64(lock.command.Expried) + 1
    } else if lock.command.ExpriedFlag & 0x4000 != 0 {
        lock.expried_time = 0x7fffffffffffffff
    }

    switch lock.command.ExpriedFlag & 0x0300 {
    case 0x0100:
        lock.aof_time = 0
    case 0x0200:
        lock.aof_time = 0xff
    default:
        lock.aof_time = self.lock_db.aof_time
    }

    if lock.command.ExpriedFlag & 0x1000 != 0 {
        lock.is_aof = true
    }
    lock.locked = 1
    lock.ref_count++

    if self.current_lock == nil {
        self.current_lock = lock
        return lock
    }

    self.locks.Push(lock)
    self.lock_maps[lock.command.LockId] = lock
    return lock
}

func (self *LockManager) RemoveLock(lock *Lock) *Lock {
    lock.locked = 0

    if self.current_lock == lock {
        self.current_lock = nil
        lock.ref_count--

        locked_lock := self.locks.Pop()
        for ; locked_lock != nil; {
            if locked_lock.locked > 0 {
                if _, ok := self.lock_maps[locked_lock.command.LockId]; ok {
                    delete(self.lock_maps, locked_lock.command.LockId)
                }
                self.current_lock = locked_lock
                return lock
            }

            locked_lock.ref_count--
            if locked_lock.ref_count == 0 {
                self.FreeLock(locked_lock)
            }

            locked_lock = self.locks.Pop()
            if self.locks.head_node_index >= 8 {
                self.locks.Resize()
            }
        }
        return lock
    }

    if _, ok := self.lock_maps[lock.command.LockId]; ok {
        delete(self.lock_maps, lock.command.LockId)
    }

    locked_lock := self.locks.Head()
    for ; locked_lock != nil; {
        if locked_lock.locked > 0 {
            break
        }

        self.locks.Pop()
        locked_lock.ref_count--
        if locked_lock.ref_count == 0 {
            self.FreeLock(locked_lock)
        }

        locked_lock = self.locks.Head()
        if self.locks.head_node_index >= 8 {
            self.locks.Resize()
        }
    }
    return lock
}

func (self *LockManager) GetLockedLock(command *protocol.LockCommand) *Lock {
    if self.current_lock.command.LockId == command.LockId {
        return self.current_lock
    }

    locked_lock, ok := self.lock_maps[command.LockId]
    if ok {
        return locked_lock
    }
    return nil
}

func (self *LockManager) UpdateLockedLock(lock *Lock, timeout uint16, timeout_flag uint16, expried uint16, expried_flag uint16, count uint16, rcount uint8) {
    lock.command.Timeout = timeout
    lock.command.TimeoutFlag = timeout_flag
    lock.command.Expried = expried
    lock.command.ExpriedFlag = expried_flag
    lock.command.Count = count
    lock.command.Rcount = rcount

    if timeout_flag & 0x0400 == 0 {
        lock.timeout_time = self.lock_db.current_time + int64(timeout) + 1
    } else {
        lock.timeout_time = 0
    }

    if expried_flag & 0x0400 == 0 {
        lock.expried_time = self.lock_db.current_time + int64(expried) + 1
    } else if lock.command.ExpriedFlag & 0x4000 != 0 {
        lock.expried_time = 0x7fffffffffffffff
    } else {
        lock.expried_time = 0
    }

    if timeout_flag & 0x2000 == 0 {
        lock.timeout_checked_count = 1
    }
    if expried_flag & 0x2000 == 0 {
        lock.expried_checked_count = 1
    }

    switch lock.command.ExpriedFlag & 0x0300 {
    case 0x0100:
        lock.aof_time = 0
    case 0x0200:
        lock.aof_time = 0xff
    default:
        lock.aof_time = self.lock_db.aof_time
    }

    if lock.is_aof {
        self.PushLockAof(lock)
    }
}

func (self *LockManager) AddWaitLock(lock *Lock) *Lock {
    self.wait_locks.Push(lock)
    lock.ref_count++
    self.waited = true
    return lock
}

func (self *LockManager) GetWaitLock() *Lock {
    lock := self.wait_locks.Head()
    for ; lock != nil; {
        if lock.timeouted {
            self.wait_locks.Pop()
            lock.ref_count--
            if lock.ref_count == 0{
                self.FreeLock(lock)
            }

            lock = self.wait_locks.Head()
            if self.wait_locks.head_node_index >= 8 {
                self.wait_locks.Resize()
            }
            continue
        }
        return lock
    }
    return nil
}

func (self *LockManager) PushLockAof(lock *Lock)  {
    if self.lock_db.aof_channels[self.glock_index].Push(lock, protocol.COMMAND_LOCK) != nil {
        self.lock_db.slock.Log().Errorf("Lock Push Aof Lock Error DbId:%d LockKey:%x LockId:%x",
            lock.command.DbId, lock.command.LockKey, lock.command.LockId)
        return
    }
    lock.is_aof = true
}

func (self *LockManager) PushUnLockAof(lock *Lock)  {
    if self.lock_db.aof_channels[self.glock_index].Push(lock, protocol.COMMAND_UNLOCK) != nil {
        self.lock_db.slock.Log().Errorf("Lock Push Aof Unlock Error DbId:%d LockKey:%x LockId:%x",
            lock.command.DbId, lock.command.LockKey, lock.command.LockId)
    }
    lock.is_aof = false
}

func (self *LockManager) FreeLock(lock *Lock) *Lock{
    self.ref_count--
    lock.manager = nil
    lock.protocol = nil
    lock.command = nil
    self.free_locks.Push(lock)
    return lock
}

func (self *LockManager) GetOrNewLock(protocol ServerProtocol, command *protocol.LockCommand) *Lock {
    lock := self.free_locks.PopRight()
    if lock == nil {
        locks := make([]Lock, 8)
        lock = &locks[0]

        for i := 1; i < 8; i++ {
            self.free_locks.Push(&locks[i])
        }
    }

    now := self.lock_db.current_time

    lock.manager = self
    lock.command = command
    lock.protocol = protocol
    lock.start_time = now
    lock.expried_time = 0
    if lock.command.TimeoutFlag & 0x0400 == 0 {
        lock.timeout_time = now + int64(command.Timeout) + 1
    } else {
        lock.timeout_time = 0
    }
    lock.timeout_checked_count = 1
    lock.expried_checked_count = 1
    lock.long_wait_index = 0
    self.ref_count++
    return lock
}

type Lock struct {
    manager                 *LockManager
    command                 *protocol.LockCommand
    protocol                ServerProtocol
    start_time              int64
    expried_time            int64
    timeout_time            int64
    long_wait_index         uint64
    timeout_checked_count   uint8
    expried_checked_count   uint8
    locked                  uint8
    ref_count               uint8
    timeouted               bool
    expried                 bool
    aof_time                uint8
    is_aof                  bool
}

func NewLock(manager *LockManager, protocol ServerProtocol, command *protocol.LockCommand) *Lock {
    now := manager.lock_db.current_time
    return &Lock{manager, command, protocol,now, 0, now + int64(command.Timeout),
        0, 0, 0,0, 0, false, false, 0, false}
}

func (self *Lock) GetDB() *LockDB {
    if self.manager == nil {
        return nil
    }
    return self.manager.GetDB()
}