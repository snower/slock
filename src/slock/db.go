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
    timeout_locks      map[int64]*LockQueue
    expried_locks      map[int64]*LockQueue
    check_timeout_time int64
    check_expried_time int64
    glock              sync.Mutex
    manager_glocks []*sync.Mutex
    manager_glock_index int
    is_stop            bool
    state LockDBState
}

func NewLockDB(slock *SLock) *LockDB {
    manager_glocks := make([]*sync.Mutex, 64)
    for i:=0; i< 64; i++{
        manager_glocks[i] = &sync.Mutex{}
    }
    now := time.Now().Unix()
    state := LockDBState{0, 0, 0, 0, 0, 0, 0, 0}
    db := &LockDB{slock, make(map[[16]byte]*LockManager, 0), make(map[int64]*LockQueue, 0), make(map[int64]*LockQueue, 0), now, now, sync.Mutex{}, manager_glocks, 0, false, state}
    db.ResizeTimeOut()
    db.ResizeExpried()
    go db.CheckTimeOut()
    go db.CheckExpried()
    return db
}

func (self *LockDB) ResizeTimeOut () error{
    start_time := self.check_timeout_time
    end_time := self.check_timeout_time + 1024
    for start_time <= end_time {
        _, ok := self.timeout_locks[start_time]
        if !ok {
            self.timeout_locks[start_time] = NewLockQueue(64, 128)
        }
        start_time++
    }
    return nil
}

func (self *LockDB) ResizeExpried () error{
    start_time := self.check_expried_time
    end_time := self.check_expried_time + 1024
    for start_time <= end_time {
        _, ok := self.expried_locks[start_time]
        if !ok {
            self.expried_locks[start_time] = NewLockQueue(256, 1024)
        }
        start_time++
    }
    return nil
}

func (self *LockDB) CheckTimeOut() (err error) {
    for !self.is_stop {
        _, ok := self.timeout_locks[self.check_timeout_time + 256]
        if !ok {
            self.ResizeTimeOut()
        }
        time.Sleep(1e9)

        now := time.Now().Unix()
        for self.check_timeout_time <= now {
            timeout_locks, ok := self.timeout_locks[self.check_timeout_time]
            if ok {
                lock := timeout_locks.Pop()
                for ;lock != nil; {
                    if !lock.Timeouted {
                        if lock.TimeoutTime <= now {
                            self.DoTimeOut(lock)
                        } else {
                            lock.TimeoutCheckedCount++
                            func (){
                                defer lock.manager.GLock.Unlock()
                                lock.manager.GLock.Lock()
                                self.AddTimeOut(lock)
                            }()
                        }
                    }
                    lock = timeout_locks.Pop()
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
        _, ok := self.expried_locks[self.check_expried_time + 256]
        if !ok {
            self.ResizeExpried()
        }
        time.Sleep(1e9)

        now := time.Now().Unix()
        for self.check_expried_time <= now {
            expried_locks, ok := self.expried_locks[self.check_expried_time]
            if ok {
                lock := expried_locks.Pop()
                for ;lock != nil; {
                    if !lock.Expried {
                        if lock.ExpriedTime <= now {
                            self.DoExpried(lock)
                            if lock.manager.LockCount <= lock.manager.UnLockedCount {
                                self.RemoveLockManager(lock.manager)
                            }
                        } else {
                            lock.ExpriedCheckedCount++
                            func (){
                                defer lock.manager.GLock.Unlock()
                                lock.manager.GLock.Lock()
                                self.AddExpried(lock)
                            }()
                        }
                    } else {
                        if lock.manager.LockCount > 0 && lock.manager.LockCount <= lock.manager.UnLockedCount {
                            self.RemoveLockManager(lock.manager)
                        }
                    }
                    lock = expried_locks.Pop()
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

    lock_manager = NewLockManager(self, command, self.manager_glocks[self.manager_glock_index])
    self.manager_glock_index++
    if self.manager_glock_index >= 64 {
        self.manager_glock_index = 0
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
    defer lock_manager.GLock.Unlock()
    lock_manager.GLock.Lock()

    if lock_manager.LockCount > 0 && lock_manager.LockCount <= lock_manager.UnLockedCount && !lock_manager.Locked {
        defer self.glock.Unlock()
        self.glock.Lock()
        current_lock_manager, ok := self.locks[lock_manager.LockKey]
        if ok && current_lock_manager == lock_manager {
            delete(self.locks, lock_manager.LockKey)
            self.state.KeyCount--
        }
    }

    return nil
}

func (self *LockDB) AddTimeOut(lock *Lock) (err error) {
    if lock.TimeoutTime <= lock.StartTime {
        go func() {
            self.DoTimeOut(lock)
        }()
        return nil
    }

    if lock.TimeoutCheckedCount >= 8 {
        timeout_time := self.check_timeout_time + 180
        if lock.TimeoutTime < timeout_time {
            timeout_time = lock.TimeoutTime
            if timeout_time < self.check_timeout_time {
                timeout_time = self.check_timeout_time + 1
            }
        }

        self.timeout_locks[timeout_time].Push(lock)
    } else {
        timeout_time := self.check_timeout_time + 1<<lock.TimeoutCheckedCount
        if lock.TimeoutTime < timeout_time {
            timeout_time = lock.TimeoutTime
            if timeout_time < self.check_timeout_time {
                timeout_time = self.check_timeout_time + 1
            }
        }

        self.timeout_locks[timeout_time].Push(lock)
    }

    return nil
}

func (self *LockDB) RemoveTimeOut(lock *Lock) (err error) {
    lock.Timeouted = true
    self.state.WaitCount--
    return nil
}

func (self *LockDB) DoTimeOut(lock *Lock) (err error) {
    defer lock.manager.GLock.Unlock()
    lock.manager.GLock.Lock()

    if lock.Timeouted {
        return nil
    }

    lock.manager.RemoveLock(lock)
    lock.Timeouted = true
    self.slock.Active(lock.Command, RESULT_TIMEOUT)
    self.state.WaitCount--
    if lock.TimeoutTime <= lock.StartTime {
        self.state.TimeoutedCount++
    }

    if lock.TimeoutTime > lock.StartTime {
        self.slock.Log().Infof("lock timeout %d %x %x %x %s", lock.Command.DbId, lock.Command.LockKey, lock.Command.LockId, lock.Command.RequestId, lock.Command.Protocol.RemoteAddr().String())
    }
    return nil
}

func (self *LockDB) AddExpried(lock *Lock) (err error) {
    if lock.ExpriedTime <= lock.StartTime {
        go func() {
            self.DoExpried(lock)
            time.Sleep(1e9)
            if lock.manager.LockCount <= lock.manager.UnLockedCount {
                self.RemoveLockManager(lock.manager)
            }
        }()
        return nil
    }

    if lock.ExpriedCheckedCount >= 8 {
        expried_time := self.check_expried_time + 180
        if lock.ExpriedTime < expried_time {
            expried_time = lock.ExpriedTime
            if expried_time < self.check_expried_time {
                expried_time = self.check_expried_time + 1
            }
        }

        self.expried_locks[expried_time].Push(lock)
    }else{
        expried_time := self.check_expried_time + 2<<lock.ExpriedCheckedCount
        if lock.ExpriedTime < expried_time {
            expried_time = lock.ExpriedTime
            if expried_time < self.check_expried_time {
                expried_time = self.check_expried_time + 1
            }
        }

        self.expried_locks[expried_time].Push(lock)
    }
    return nil
}

func (self *LockDB) RemoveExpried(lock *Lock) (err error) {
    lock.Expried = true
    return nil
}

func (self *LockDB) DoExpried(lock *Lock) (err error) {
    defer lock.manager.GLock.Unlock()
    lock.manager.GLock.Lock()

    if lock.Expried {
        return nil
    }

    lock_manager := lock.manager
    lock_manager.RemoveLock(lock)
    lock.Expried = true

    if lock.ExpriedTime > lock.StartTime {
        self.slock.Active(lock.Command, RESULT_EXPRIED)
        self.state.ExpriedCount++
    }
    self.state.LockedCount--


    if lock_manager.LockCount <= lock_manager.UnLockedCount {
        lock_manager.Locked = false
    }else{
        current_lock := lock_manager.CurrentLock()
        if current_lock == nil {
            lock_manager.Locked = false
        } else {
            self.RemoveTimeOut(current_lock)
            self.AddExpried(current_lock)
            self.slock.Active(current_lock.Command, RESULT_SUCCED)
            self.state.LockCount++
            self.state.LockedCount++
        }
    }

    if lock.ExpriedTime > lock.StartTime {
        self.slock.Log().Infof("lock expried %d %x %x %x %s", lock.Command.DbId, lock.Command.LockKey, lock.Command.LockId, lock.Command.RequestId, lock.Command.Protocol.RemoteAddr().String())
    }
    return nil
}

func (self *LockDB) Lock(command *LockCommand) (err error) {
    lock_manager := self.GetOrNewLockManager(command)

    defer lock_manager.GLock.Unlock()
    lock_manager.GLock.Lock()

    if lock_manager.Locked {
        current_lock := lock_manager.CurrentLock()
        if current_lock != nil {
            if current_lock.Command.LockId == command.LockId {
                self.slock.Active(command, RESULT_LOCKED_ERROR)
                return nil
            }
        }
    }

    lock := NewLock(lock_manager, command)
    lock_manager.AddLock(lock)
    if lock_manager.Locked {
        self.AddTimeOut(lock)
        self.state.WaitCount++
    } else {
        lock_manager.Locked = true
        self.AddExpried(lock)
        self.slock.Active(command, RESULT_SUCCED)
        self.state.LockCount++
        self.state.LockedCount++
    }
    return nil
}

func (self *LockDB) UnLock(command *LockCommand) (err error) {
    lock_manager := self.GetLockManager(command)
    if lock_manager == nil {
        self.slock.Active(command, RESULT_UNLOCK_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    defer lock_manager.GLock.Unlock()
    lock_manager.GLock.Lock()

    if !lock_manager.Locked {
        self.slock.Active(command, RESULT_UNLOCK_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    current_lock := lock_manager.CurrentLock()
    if current_lock == nil {
        self.slock.Active(command, RESULT_UNLOCK_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    if current_lock.Command.LockId != command.LockId {
        self.slock.Active(command, RESULT_UNOWN_ERROR)
        self.state.UnlockErrorCount++
        return nil
    }

    last_lock := lock_manager.RemoveLock(current_lock)
    self.RemoveExpried(last_lock)
    self.slock.Active(command, RESULT_SUCCED)
    self.state.UnLockCount++
    self.state.LockedCount--

    current_lock = lock_manager.CurrentLock()
    if current_lock == nil {
        lock_manager.Locked = false
    } else {
        self.RemoveTimeOut(current_lock)
        self.AddExpried(current_lock)
        self.slock.Active(current_lock.Command, RESULT_SUCCED)
        self.state.LockCount++
        self.state.LockedCount++
    }
    return nil
}

func (self *LockDB) GetState() *LockDBState {
    return &self.state
}