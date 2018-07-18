package slock

import (
    "sync"
    "time"
)

type LockDBState struct {
    LockCount uint64
    UnLockCount uint64
    LockedCount uint32
    WaitCount uint32
    TimeoutedCount uint32
    ExpriedCount uint32
    UnlockErrorCount uint32
    KeyCount uint32
}

type LockDB struct {
    slock              *SLock
    locks              map[[16]byte]*LockManager
    timeout_locks      map[int64][]*LockQueue
    expried_locks      map[int64][]*LockQueue
    check_timeout_time int64
    check_expried_time int64
    glock              sync.Mutex
    manager_glocks []*sync.Mutex
    manager_glock_index int
    manager_max_glocks int
    is_stop            bool
    state LockDBState
    free_lock_managers []*LockManager
    free_lock_manager_count int
}

func NewLockDB(slock *SLock) *LockDB {
    manager_max_glocks := 64
    manager_glocks := make([]*sync.Mutex, manager_max_glocks)
    for i:=0; i< manager_max_glocks; i++{
        manager_glocks[i] = &sync.Mutex{}
    }
    now := time.Now().Unix()
    state := LockDBState{0, 0, 0, 0, 0, 0, 0, 0}
    db := &LockDB{slock, make(map[[16]byte]*LockManager, 0), make(map[int64][]*LockQueue, 0), make(map[int64][]*LockQueue, 0), now, now, sync.Mutex{}, manager_glocks, 0, manager_max_glocks, false, state, make([]*LockManager, 4096), -1}
    db.ResizeTimeOut()
    db.ResizeExpried()
    go db.CheckTimeOut()
    go db.CheckExpried()
    return db
}

func (self *LockDB) ResizeTimeOut () error{
    start_time := self.check_timeout_time
    end_time := self.check_timeout_time + 256
    for start_time <= end_time {
        _, ok := self.timeout_locks[start_time]
        if !ok {
            self.timeout_locks[start_time] = make([]*LockQueue, self.manager_max_glocks)
            for i := 0; i < self.manager_max_glocks; i++ {
                self.timeout_locks[start_time][i] = NewLockQueue(16, 64)
            }
        }
        start_time++
    }
    return nil
}

func (self *LockDB) ResizeExpried () error{
    start_time := self.check_expried_time
    end_time := self.check_expried_time + 256
    for start_time <= end_time {
        _, ok := self.expried_locks[start_time]
        if !ok {
            self.expried_locks[start_time] = make([]*LockQueue, self.manager_max_glocks)
            for i := 0; i < self.manager_max_glocks; i++ {
                self.expried_locks[start_time][i] = NewLockQueue(32, 64)
            }
        }
        start_time++
    }
    return nil
}

func (self *LockDB) CheckTimeOut() (err error) {
    for !self.is_stop {
        _, ok := self.timeout_locks[self.check_timeout_time + 195]
        if !ok {
            self.ResizeTimeOut()
        }
        time.Sleep(1e9)

        now := time.Now().Unix()
        for self.check_timeout_time <= now {
            timeout_locks, ok := self.timeout_locks[self.check_timeout_time]
            if ok {
                for i := 0; i < self.manager_max_glocks; i++ {
                    lock := timeout_locks[i].Pop()
                    for ; lock != nil; {
                        if !lock.timeouted {
                            if lock.timeout_time <= now {
                                self.DoTimeOut(lock)
                            } else {
                                lock.timeout_checked_count++
                                func() {
                                    defer lock.manager.glock.Unlock()
                                    lock.manager.glock.Lock()
                                    self.AddTimeOut(lock)
                                }()
                            }
                        }
                        lock = timeout_locks[i].Pop()
                    }
                }
                delete(self.timeout_locks, self.check_timeout_time)
            }

            self.check_timeout_time++
        }
    }
    return nil
}

func (self *LockDB) CheckExpried() (err error) {
    for !self.is_stop {
        _, ok := self.expried_locks[self.check_expried_time + 195]
        if !ok {
            self.ResizeExpried()
        }
        time.Sleep(1e9)

        now := time.Now().Unix()
        for self.check_expried_time <= now {
            expried_locks, ok := self.expried_locks[self.check_expried_time]
            if ok {
                for i := 0; i < self.manager_max_glocks; i++ {
                    lock := expried_locks[i].Pop()
                    for ; lock != nil; {
                        if !lock.expried {
                            if lock.expried_time <= now {
                                self.DoExpried(lock)
                                if lock.manager.lock_count <= lock.manager.unlocked_count {
                                    self.RemoveLockManager(lock.manager)
                                }
                            } else {
                                lock.expried_checked_count++
                                func() {
                                    defer lock.manager.glock.Unlock()
                                    lock.manager.glock.Lock()
                                    self.AddExpried(lock)
                                }()
                            }
                        } else {
                            if lock.manager.lock_count > 0 && lock.manager.lock_count <= lock.manager.unlocked_count {
                                self.RemoveLockManager(lock.manager)
                            }
                        }
                        lock = expried_locks[i].Pop()
                    }
                }
                delete(self.expried_locks, self.check_expried_time)
            }

            self.check_expried_time++
        }
    }
    return nil
}

func (self *LockDB) GetOrNewLockManager(command *LockCommand) *LockManager{
    defer self.glock.Unlock()
    self.glock.Lock()

    lock_manager, ok := self.locks[command.LockKey]
    if ok {
        return lock_manager
    }

    if self.free_lock_manager_count >= 0{
        lock_manager = self.free_lock_managers[self.free_lock_manager_count]
        self.free_lock_manager_count--
        lock_manager.lock_key = command.LockKey
    }else{
        lock_manager = NewLockManager(self, command, self.manager_glocks[self.manager_glock_index], self.manager_glock_index)
        self.manager_glock_index++
        if self.manager_glock_index >= 64 {
            self.manager_glock_index = 0
        }
    }

    self.locks[command.LockKey] = lock_manager
    self.state.KeyCount++
    return lock_manager
}

func (self *LockDB) GetLockManager(command *LockCommand) *LockManager{
    defer self.glock.Unlock()
    self.glock.Lock()

    lock_manager, ok := self.locks[command.LockKey]
    if ok {
        return lock_manager
    }

    return nil
}

func (self *LockDB) RemoveLockManager(lock_manager *LockManager) (err error) {
    defer lock_manager.glock.Unlock()
    lock_manager.glock.Lock()

    if lock_manager.lock_count > 0 && lock_manager.lock_count <= lock_manager.unlocked_count && !lock_manager.locked {
        defer self.glock.Unlock()
        self.glock.Lock()
        current_lock_manager, ok := self.locks[lock_manager.lock_key]
        if ok && current_lock_manager == lock_manager {
            delete(self.locks, lock_manager.lock_key)
            if self.free_lock_manager_count < 4095 {
                lock_manager.lock_count = 0
                lock_manager.unlocked_count = 0
                lock_manager.locks.Reset()
                self.free_lock_manager_count++
                self.free_lock_managers[self.free_lock_manager_count] = lock_manager
            }
            self.state.KeyCount--
        }
    }

    return nil
}

func (self *LockDB) AddTimeOut(lock *Lock) (err error) {
    if lock.timeout_time <= lock.start_time {
        go func() {
            self.DoTimeOut(lock)
        }()
        return nil
    }

    if lock.timeout_checked_count >= 8 {
        timeout_time := self.check_timeout_time + 180
        if lock.timeout_time < timeout_time {
            timeout_time = lock.timeout_time
            if timeout_time < self.check_timeout_time {
                timeout_time = self.check_timeout_time + 1
            }
        }

        lock.freed = false
        self.timeout_locks[timeout_time][lock.manager.glock_index].Push(lock)
    } else {
        timeout_time := self.check_timeout_time + 1<<lock.timeout_checked_count
        if lock.timeout_time < timeout_time {
            timeout_time = lock.timeout_time
            if timeout_time < self.check_timeout_time {
                timeout_time = self.check_timeout_time + 1
            }
        }

        lock.freed = false
        self.timeout_locks[timeout_time][lock.manager.glock_index].Push(lock)
    }

    return nil
}

func (self *LockDB) RemoveTimeOut(lock *Lock) (err error) {
    lock.timeouted = true
    self.state.WaitCount--
    return nil
}

func (self *LockDB) DoTimeOut(lock *Lock) (err error) {
    defer lock.manager.glock.Unlock()
    lock.manager.glock.Lock()

    if lock.timeouted {
        return nil
    }

    self.slock.Active(lock.protocol, lock.command, RESULT_TIMEOUT)
    if lock.timeout_time > lock.start_time {
        self.slock.Log().Infof("lock timeout %d %x %x %x %s", lock.command.DbId, lock.command.LockKey, lock.command.LockId, lock.command.RequestId, lock.protocol.RemoteAddr().String())
    }

    lock.timeouted = true
    lock.freed = true
    lock.manager.RemoveLock(lock)
    self.state.WaitCount--
    if lock.timeout_time <= lock.start_time {
        self.state.TimeoutedCount++
    }
    return nil
}

func (self *LockDB) AddExpried(lock *Lock) (err error) {
    if lock.expried_time <= lock.start_time {
        go func() {
            self.DoExpried(lock)
            time.Sleep(1e9)
            if lock.manager.lock_count <= lock.manager.unlocked_count {
                self.RemoveLockManager(lock.manager)
            }
        }()
        return nil
    }

    if lock.expried_checked_count >= 7 {
        expried_time := self.check_expried_time + 180
        if lock.expried_time < expried_time {
            expried_time = lock.expried_time
            if expried_time < self.check_expried_time {
                expried_time = self.check_expried_time + 1
            }
        }

        lock.freed = false
        self.expried_locks[expried_time][lock.manager.glock_index].Push(lock)
    }else{
        expried_time := self.check_expried_time + 2<<lock.expried_checked_count
        if lock.expried_time < expried_time {
            expried_time = lock.expried_time
            if expried_time < self.check_expried_time {
                expried_time = self.check_expried_time + 1
            }
        }

        lock.freed = false
        self.expried_locks[expried_time][lock.manager.glock_index].Push(lock)
    }
    return nil
}

func (self *LockDB) RemoveExpried(lock *Lock) (err error) {
    lock.expried = true
    return nil
}

func (self *LockDB) DoExpried(lock *Lock) (err error) {
    defer lock.manager.glock.Unlock()
    lock.manager.glock.Lock()

    if lock.expried {
        return nil
    }

    if lock.expried_time > lock.start_time {
        self.slock.Active(lock.protocol, lock.command, RESULT_EXPRIED)
        self.state.ExpriedCount++
        self.slock.Log().Infof("lock expried %d %x %x %x %s", lock.command.DbId, lock.command.LockKey, lock.command.LockId, lock.command.RequestId, lock.protocol.RemoteAddr().String())
    }

    lock_manager := lock.manager
    lock.expried = true
    lock.freed = true
    lock_manager.RemoveLock(lock)
    self.state.LockedCount--


    if lock_manager.lock_count <= lock_manager.unlocked_count {
        lock_manager.locked = false
    }else{
        current_lock := lock_manager.CurrentLock()
        if current_lock == nil {
            lock_manager.locked = false
        } else {
            self.RemoveTimeOut(current_lock)
            self.AddExpried(current_lock)
            self.slock.Active(current_lock.protocol, current_lock.command, RESULT_SUCCED)
            self.state.LockCount++
            self.state.LockedCount++
        }
    }

    return nil
}

func (self *LockDB) Lock(protocol Protocol, command *LockCommand) (err error) {
    lock_manager := self.GetOrNewLockManager(command)

    defer lock_manager.glock.Unlock()
    lock_manager.glock.Lock()

    if lock_manager.locked {
        current_lock := lock_manager.CurrentLock()
        if current_lock != nil {
            if current_lock.command.LockId == command.LockId {
                self.slock.Active(protocol, command, RESULT_LOCKED_ERROR)
                return nil
            }
        }
    }

    lock := lock_manager.GetOrNewLock(protocol, command)
    lock_manager.AddLock(lock)
    if lock_manager.locked {
        self.AddTimeOut(lock)
        self.state.WaitCount++
    } else {
        lock_manager.locked = true
        self.AddExpried(lock)
        self.slock.Active(protocol, command, RESULT_SUCCED)
        self.state.LockCount++
        self.state.LockedCount++
    }
    return nil
}

func (self *LockDB) UnLock(protocol Protocol, command *LockCommand) (err error) {
    lock_manager := self.GetLockManager(command)
    if lock_manager == nil {
        self.slock.Active(protocol, command, RESULT_UNLOCK_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    defer lock_manager.glock.Unlock()
    lock_manager.glock.Lock()

    if !lock_manager.locked {
        self.slock.Active(protocol, command, RESULT_UNLOCK_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    current_lock := lock_manager.CurrentLock()
    if current_lock == nil {
        self.slock.Active(protocol, command, RESULT_UNLOCK_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    if current_lock.command.LockId != command.LockId {
        self.slock.Active(protocol, command, RESULT_UNOWN_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    last_lock := lock_manager.RemoveLock(current_lock)
    self.RemoveExpried(last_lock)
    self.slock.Active(protocol, command, RESULT_SUCCED)
    self.state.UnLockCount++
    self.state.LockedCount--

    current_lock = lock_manager.CurrentLock()
    if current_lock == nil {
        lock_manager.locked = false
    } else {
        self.RemoveTimeOut(current_lock)
        self.AddExpried(current_lock)
        self.slock.Active(current_lock.protocol, current_lock.command, RESULT_SUCCED)
        self.state.LockCount++
        self.state.LockedCount++
    }
    return nil
}

func (self *LockDB) GetState() *LockDBState {
    return &self.state
}