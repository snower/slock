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
    manager_glocks     []*sync.Mutex
    manager_glock_index int
    manager_max_glocks  int
    is_stop             bool
    state LockDBState
    free_lock_managers  []*LockManager
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
    var i int64
    for i = 0; i < 180; i++ {
        _, ok := self.timeout_locks[i]
        if !ok {
            self.timeout_locks[i] = make([]*LockQueue, self.manager_max_glocks)
            for j := 0; j < self.manager_max_glocks; j++ {
                self.timeout_locks[i][j] = NewLockQueue(8, 64)
            }
        }
    }
    return nil
}

func (self *LockDB) ResizeExpried () error{
    var i int64
    for i = 0; i < 180; i++ {
        _, ok := self.expried_locks[i]
        if !ok {
            self.expried_locks[i] = make([]*LockQueue, self.manager_max_glocks)
            for j := 0; j < self.manager_max_glocks; j++ {
                self.expried_locks[i][j] = NewLockQueue(8, 64)
            }
        }
    }
    return nil
}

func (self *LockDB) CheckTimeOut() (err error) {
    for !self.is_stop {
        time.Sleep(1e9)

        now := time.Now().Unix()
        for self.check_timeout_time <= now {
            go self.CheckTimeTimeOut(self.check_timeout_time, now)
            self.check_timeout_time++
        }
    }
    return nil
}

func (self *LockDB) CheckTimeTimeOut(check_timeout_time int64, now int64) (err error) {
    timeout_locks, ok := self.timeout_locks[check_timeout_time % 180]
    if ok {
        for i := 0; i < self.manager_max_glocks; i++ {
            lock := timeout_locks[i].Pop()
            for ; lock != nil; {
                if !lock.timeouted {
                    if lock.timeout_time <= now {
                        self.DoTimeOut(lock)
                        if lock.locked_freed && lock.wait_freed {
                            lock.manager.FreeLock(lock)
                        }
                    } else {
                        lock.timeout_checked_count++
                        func() {
                            defer lock.manager.glock.Unlock()
                            lock.manager.glock.Lock()
                            self.AddTimeOut(lock)
                        }()
                    }
                }else{
                    if lock.locked_freed && lock.wait_freed {
                        lock.manager.FreeLock(lock)
                    }
                }

                lock = timeout_locks[i].Pop()
            }
            timeout_locks[i].Reset()
        }
    }
    return nil
}

func (self *LockDB) CheckExpried() (err error) {
    for !self.is_stop {
        time.Sleep(1e9)

        now := time.Now().Unix()
        for self.check_expried_time <= now {
            go self.CheckTimeExpried(self.check_expried_time, now)
            self.check_expried_time++
        }
    }
    return nil
}

func (self *LockDB) CheckTimeExpried(check_expried_time int64, now int64) (err error) {
    expried_locks, ok := self.expried_locks[check_expried_time % 180]
    if ok {
        for i := 0; i < self.manager_max_glocks; i++ {
            lock := expried_locks[i].Pop()
            for ; lock != nil; {
                if !lock.expried {
                    if lock.expried_time <= now {
                        self.DoExpried(lock)
                        if lock.locked_freed && lock.wait_freed {
                            lock.manager.FreeLock(lock)
                        }

                        if lock.manager.locked <= 0 {
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
                    if lock.locked_freed && lock.wait_freed {
                        lock.manager.FreeLock(lock)
                    }

                    if lock.manager.locked <= 0 {
                        self.RemoveLockManager(lock.manager)
                    }
                }
                lock = expried_locks[i].Pop()
            }
            expried_locks[i].Reset()
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

    if lock_manager.locked <= 0 {
        defer self.glock.Unlock()
        self.glock.Lock()
        current_lock_manager, ok := self.locks[lock_manager.lock_key]
        if ok && current_lock_manager == lock_manager {
            delete(self.locks, lock_manager.lock_key)
            if self.free_lock_manager_count < 4095 {
                lock_manager.locked = 0
                if lock_manager.locks != nil {
                    lock_manager.locks.Reset()
                }
                if lock_manager.wait_locks != nil {
                    lock_manager.wait_locks.Reset()
                }
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
        timeout_time := self.check_timeout_time + 179
        if lock.timeout_time < timeout_time {
            timeout_time = lock.timeout_time
            if timeout_time < self.check_timeout_time {
                timeout_time = self.check_timeout_time + 1
            }
        }

        self.timeout_locks[timeout_time % 180][lock.manager.glock_index].Push(lock)
    } else {
        timeout_time := self.check_timeout_time + 1<<lock.timeout_checked_count
        if lock.timeout_time < timeout_time {
            timeout_time = lock.timeout_time
            if timeout_time < self.check_timeout_time {
                timeout_time = self.check_timeout_time + 1
            }
        }

        self.timeout_locks[timeout_time % 180][lock.manager.glock_index].Push(lock)
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

    self.slock.Active(lock.protocol, lock.command, RESULT_TIMEOUT, false)
    if lock.timeout_time > lock.start_time {
        self.slock.Log().Infof("lock timeout %d %x %x %x %s", lock.command.DbId, lock.command.LockKey, lock.command.LockId, lock.command.RequestId, lock.protocol.RemoteAddr().String())
    }

    lock.timeouted = true
    lock.manager.GetWaitLock()
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
            if lock.manager.locked <= 0 {
                self.RemoveLockManager(lock.manager)
            }
        }()
        return nil
    }

    if lock.expried_checked_count >= 7 {
        expried_time := self.check_expried_time + 179
        if lock.expried_time < expried_time {
            expried_time = lock.expried_time
            if expried_time < self.check_expried_time {
                expried_time = self.check_expried_time + 1
            }
        }

        self.expried_locks[expried_time % 180][lock.manager.glock_index].Push(lock)
    }else{
        expried_time := self.check_expried_time + 2<<lock.expried_checked_count
        if lock.expried_time < expried_time {
            expried_time = lock.expried_time
            if expried_time < self.check_expried_time {
                expried_time = self.check_expried_time + 1
            }
        }

        self.expried_locks[expried_time % 180][lock.manager.glock_index].Push(lock)
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
        self.slock.Active(lock.protocol, lock.command, RESULT_EXPRIED, false)
        self.state.ExpriedCount++
        self.slock.Log().Infof("lock expried %d %x %x %x %s", lock.command.DbId, lock.command.LockKey, lock.command.LockId, lock.command.RequestId, lock.protocol.RemoteAddr().String())
    }

    lock_manager := lock.manager
    lock.expried = true
    lock_manager.RemoveLock(lock)
    lock_manager.locked--
    self.state.LockedCount--

    current_lock := lock_manager.GetWaitLock()
    if current_lock != nil {
        if self.DoLock(current_lock.protocol, lock_manager, current_lock, false) {
            lock_manager.wait_locks.Pop()
            current_lock.wait_freed = true
            self.RemoveTimeOut(current_lock)
        }
    }

    return nil
}

func (self *LockDB) Lock(protocol *ServerProtocol, command *LockCommand) (err error) {
    lock_manager := self.GetOrNewLockManager(command)

    defer lock_manager.glock.Unlock()
    lock_manager.glock.Lock()

    if lock_manager.locked > 0 {
        current_lock := lock_manager.GetLockedLock(command)
        if current_lock != nil {
            self.slock.Active(protocol, command, RESULT_LOCKED_ERROR, true)
            protocol.FreeLockCommand(command)
            return nil
        }
    }

    lock := lock_manager.GetOrNewLock(protocol, command)
    if !self.DoLock(protocol, lock_manager, lock, true) {
        lock_manager.AddWaitLock(lock)
        self.AddTimeOut(lock)
        self.state.WaitCount++
    }
    return nil
}

func (self *LockDB) UnLock(protocol *ServerProtocol, command *LockCommand) (err error) {
    lock_manager := self.GetLockManager(command)
    if lock_manager == nil {
        self.slock.Active(protocol, command, RESULT_UNLOCK_ERROR, true)
        protocol.FreeLockCommand(command)
        self.state.UnlockErrorCount++
        return nil
    }

    defer lock_manager.glock.Unlock()
    lock_manager.glock.Lock()

    if lock_manager.locked <= 0 {
        self.slock.Active(protocol, command, RESULT_UNLOCK_ERROR, true)
        protocol.FreeLockCommand(command)
        self.state.UnlockErrorCount++
        return nil
    }

    current_lock := lock_manager.GetLockedLock(command)
    if current_lock == nil {
        self.slock.Active(protocol, command, RESULT_UNOWN_ERROR, true)
        protocol.FreeLockCommand(command)
        self.state.UnlockErrorCount++
        return nil
    }

    self.RemoveExpried(current_lock)
    lock_manager.RemoveLock(current_lock)
    lock_manager.locked--
    self.slock.Active(protocol, command, RESULT_SUCCED, true)
    protocol.FreeLockCommand(command)
    protocol.FreeLockCommand(current_lock.command)
    self.state.UnLockCount++
    self.state.LockedCount--

    current_lock = lock_manager.GetWaitLock()
    if current_lock != nil {
        if self.DoLock(current_lock.protocol, lock_manager, current_lock, current_lock.protocol == protocol) {
            lock_manager.wait_locks.Pop()
            current_lock.wait_freed = true
            self.RemoveTimeOut(current_lock)
        }
    }
    return nil
}

func (self *LockDB) DoLock(protocol *ServerProtocol, lock_manager *LockManager, lock *Lock, use_cached_command bool) bool{
    if lock_manager.locked == 0 {
        lock_manager.AddLock(lock)
        lock_manager.locked++
        self.AddExpried(lock)
        self.slock.Active(protocol, lock.command, RESULT_SUCCED, use_cached_command)
        self.state.LockCount++
        self.state.LockedCount++
        return true
    }

    if(lock_manager.locked <= lock.command.Count && lock_manager.locked <= lock_manager.current_lock.command.Count){
        lock_manager.AddLock(lock)
        lock_manager.locked++
        self.AddExpried(lock)
        self.slock.Active(protocol, lock.command, RESULT_SUCCED, use_cached_command)
        self.state.LockCount++
        self.state.LockedCount++
        return true
    }

    return false
}

func (self *LockDB) GetState() *LockDBState {
    return &self.state
}