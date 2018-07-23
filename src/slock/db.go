package slock

import (
    "sync"
    "time"
    "sync/atomic"
)

const TIMEOUT_QUEUE_LENGTH int64 = 15
const EXPRIED_QUEUE_LENGTH int64 = 15

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
    timeout_locks      [][]*LockQueue
    expried_locks      [][]*LockQueue
    check_timeout_time int64
    check_expried_time int64
    glock              sync.Mutex
    manager_glocks     []*sync.Mutex
    manager_glock_index int
    manager_max_glocks  int
    is_stop             bool
    state LockDBState
    free_lock_managers  [1048576]*LockManager
    free_lock_manager_count int
    free_lock_manager_timeout bool
    free_locks []*LockQueue
}

func NewLockDB(slock *SLock) *LockDB {
    manager_max_glocks := 64
    manager_glocks := make([]*sync.Mutex, manager_max_glocks)
    free_locks := make([]*LockQueue, manager_max_glocks)
    for i:=0; i< manager_max_glocks; i++{
        manager_glocks[i] = &sync.Mutex{}
        free_locks[i] = NewLockQueue(2, 16, 4096)
    }

    now := time.Now().Unix()
    state := LockDBState{0, 0, 0, 0, 0, 0, 0, 0}
    db := &LockDB{slock, make(map[[16]byte]*LockManager, 0), make([][]*LockQueue, TIMEOUT_QUEUE_LENGTH), make([][]*LockQueue, EXPRIED_QUEUE_LENGTH), now, now, sync.Mutex{}, manager_glocks, 0, manager_max_glocks, false, state, [1048576]*LockManager{}, -1, true, free_locks}
    db.ResizeTimeOut()
    db.ResizeExpried()
    go db.CheckTimeOut()
    go db.CheckExpried()
    return db
}

func (self *LockDB) ResizeTimeOut () error{
    var i int64
    for i = 0; i < TIMEOUT_QUEUE_LENGTH; i++ {
        self.timeout_locks[i] = make([]*LockQueue, self.manager_max_glocks)
        for j := 0; j < self.manager_max_glocks; j++ {
            self.timeout_locks[i][j] = NewLockQueue(4, 16, 4096)
        }
    }
    return nil
}

func (self *LockDB) ResizeExpried () error{
    var i int64
    for i = 0; i < EXPRIED_QUEUE_LENGTH; i++ {
        self.expried_locks[i] = make([]*LockQueue, self.manager_max_glocks)
        for j := 0; j < self.manager_max_glocks; j++ {
            self.expried_locks[i][j] = NewLockQueue(4, 16, 4096)
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
    timeout_locks := self.timeout_locks[check_timeout_time % TIMEOUT_QUEUE_LENGTH]
    for i := 0; i < self.manager_max_glocks; i++ {
        lock := timeout_locks[i].Pop()
        for ; lock != nil; {
            lock.ref_count--
            if !lock.timeouted {
                if lock.timeout_time <= now {
                    self.DoTimeOut(lock)
                    if lock.ref_count == 0 {
                        lock.manager.glock.Lock()
                        if lock.ref_count == 0 {
                            lock.manager.FreeLock(lock)
                        }
                        lock.manager.glock.Unlock()
                    }
                } else {
                    lock.timeout_checked_count++
                    lock.manager.glock.Lock()
                    self.AddTimeOut(lock)
                    lock.manager.glock.Unlock()
                }
            }else{
                if lock.ref_count == 0 {
                    lock.manager.glock.Lock()
                    if lock.ref_count == 0 {
                        lock.manager.FreeLock(lock)
                    }
                    lock.manager.glock.Unlock()
                }
            }

            lock = timeout_locks[i].Pop()
        }
        timeout_locks[i].Reset()
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
    expried_locks := self.expried_locks[check_expried_time % EXPRIED_QUEUE_LENGTH]
    for i := 0; i < self.manager_max_glocks; i++ {
        lock := expried_locks[i].Pop()
        for ; lock != nil; {
            lock.ref_count--
            if !lock.expried {
                if lock.expried_time <= now {
                    self.DoExpried(lock)
                    if lock.ref_count == 0 {
                        lock.manager.glock.Lock()
                        if lock.ref_count == 0 {
                            lock.manager.FreeLock(lock)
                        }
                        lock.manager.glock.Unlock()
                    }

                    if lock.manager.locked <= 0 {
                        self.RemoveLockManager(lock.manager)
                    }
                } else {
                    lock.expried_checked_count++
                    lock.manager.glock.Lock()
                    self.AddExpried(lock)
                    lock.manager.glock.Unlock()
                }
            } else {
                if lock.ref_count == 0 {
                    lock.manager.glock.Lock()
                    if lock.ref_count == 0 {
                        lock.manager.FreeLock(lock)
                    }
                    lock.manager.glock.Unlock()
                }

                if lock.manager.locked <= 0 {
                    self.RemoveLockManager(lock.manager)
                }
            }
            lock = expried_locks[i].Pop()
        }
        expried_locks[i].Reset()
    }
    return nil
}


func (self *LockDB) GetOrNewLockManager(command *LockCommand) *LockManager{
    self.glock.Lock()

    lock_manager, ok := self.locks[command.LockKey]
    if ok {
        self.glock.Unlock()
        return lock_manager
    }

    if self.free_lock_manager_count >= 0{
        lock_manager = self.free_lock_managers[self.free_lock_manager_count]
        self.free_lock_manager_count--
        lock_manager.freed = false
        self.locks[command.LockKey] = lock_manager
        self.glock.Unlock()

        lock_manager.lock_key = command.LockKey
    }else{
        lock_managers := make([]LockManager, 4096)

        for i := 0; i < 4096; i++ {
            lock_managers[i].lock_db = self
            lock_managers[i].db_id = command.DbId
            lock_managers[i].glock = self.manager_glocks[self.manager_glock_index]
            lock_managers[i].glock_index = self.manager_glock_index
            lock_managers[i].free_locks = self.free_locks[self.manager_glock_index]

            self.manager_glock_index++
            if self.manager_glock_index >= self.manager_max_glocks {
                self.manager_glock_index = 0
            }
            self.free_lock_manager_count++
            self.free_lock_managers[self.free_lock_manager_count] = &lock_managers[i]
        }

        lock_manager = self.free_lock_managers[self.free_lock_manager_count]
        self.free_lock_manager_count--
        lock_manager.freed = false
        self.locks[command.LockKey] = lock_manager
        self.glock.Unlock()

        lock_manager.lock_key = command.LockKey
    }

    atomic.AddUint32(&self.state.KeyCount, 1)
    return lock_manager
}

func (self *LockDB) GetLockManager(command *LockCommand) *LockManager{
    self.glock.Lock()

    lock_manager, ok := self.locks[command.LockKey]
    if ok {
        self.glock.Unlock()
        return lock_manager
    }

    self.glock.Unlock()
    return nil
}

func (self *LockDB) RemoveLockManager(lock_manager *LockManager) (err error) {
    lock_manager.glock.Lock()

    if lock_manager.locked <= 0 {
        self.glock.Lock()
        current_lock_manager, ok := self.locks[lock_manager.lock_key]
        if ok && current_lock_manager == lock_manager {
            delete(self.locks, lock_manager.lock_key)
            lock_manager.freed = true

            if self.free_lock_manager_count < 1048575 {
                self.free_lock_manager_count++
                self.free_lock_managers[self.free_lock_manager_count] = lock_manager
                self.glock.Unlock()

                if lock_manager.locks != nil {
                    lock_manager.locks.Reset()
                }
                if lock_manager.wait_locks != nil {
                    lock_manager.wait_locks.Reset()
                }
            } else {
                self.glock.Unlock()

                lock_manager.current_lock = nil
                lock_manager.locks = nil
                lock_manager.lock_maps = nil
                lock_manager.wait_locks = nil
                lock_manager.free_locks = nil
            }

            lock_manager.glock.Unlock()
            atomic.AddUint32(&self.state.KeyCount, 0xffffffff)


            if self.free_lock_manager_timeout && int(self.state.KeyCount * 4) < self.free_lock_manager_count {
                go func(last_lock_count uint64) {
                    time.Sleep(300 * 1e9)
                    self.CheckFreeLockManagerTimeOut(lock_manager, last_lock_count)
                }(self.state.LockCount)
                self.free_lock_manager_timeout = false
            }
        } else {
            self.glock.Unlock()
            lock_manager.glock.Unlock()
        }
        return nil
    }

    lock_manager.glock.Unlock()

    return nil
}

func (self *LockDB) CheckFreeLockManagerTimeOut(lock_manager *LockManager, last_lock_count uint64) (err error) {
    count := int((self.state.LockCount - last_lock_count) / 300 * 4)
    if count < 1024 {
        count = 1024
    }

    self.glock.Lock()
    for ; self.free_lock_manager_count >= count; {
        lock_manager = self.free_lock_managers[self.free_lock_manager_count]
        self.free_lock_managers[self.free_lock_manager_count] = nil
        self.free_lock_manager_count--

        lock_manager.current_lock = nil
        lock_manager.locks = nil
        lock_manager.lock_maps = nil
        lock_manager.wait_locks = nil
        lock_manager.free_locks = nil
    }
    self.glock.Unlock()
    self.free_lock_manager_timeout = true

    if self.free_lock_manager_timeout && int(self.state.KeyCount * 4) < self.free_lock_manager_count {
        go func(last_lock_count uint64) {
            time.Sleep(300 * 1e9)
            self.CheckFreeLockManagerTimeOut(lock_manager, last_lock_count)
        }(self.state.LockCount)
        self.free_lock_manager_timeout = false
    }
    return nil
}

func (self *LockDB) AddTimeOut(lock *Lock) (err error) {
    if lock.timeout_time <= lock.start_time {
        go func() {
            self.DoTimeOut(lock)
            if lock.ref_count == 0 {
                lock.manager.glock.Lock()
                if lock.ref_count == 0 {
                    lock.manager.FreeLock(lock)
                }
                lock.manager.glock.Unlock()
            }
        }()
        return nil
    }

    if lock.timeout_checked_count > 5 {
        if lock.protocol.stream.closed {
            timeout_time := self.check_timeout_time + 2
            self.timeout_locks[timeout_time % TIMEOUT_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
            lock.ref_count++
        } else {
            timeout_time := self.check_timeout_time + 5
            if lock.timeout_time < timeout_time {
                timeout_time = lock.timeout_time
                if timeout_time < self.check_timeout_time {
                    timeout_time = self.check_timeout_time + 2
                }
            }

            self.timeout_locks[timeout_time % TIMEOUT_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
            lock.ref_count++
        }
    } else {
        timeout_time := self.check_timeout_time + lock.timeout_checked_count
        if lock.timeout_time < timeout_time {
            timeout_time = lock.timeout_time
            if timeout_time < self.check_timeout_time {
                timeout_time = self.check_timeout_time + 2
            }
        }

        self.timeout_locks[timeout_time % TIMEOUT_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
        lock.ref_count++
    }

    return nil
}

func (self *LockDB) RemoveTimeOut(lock *Lock) (err error) {
    lock.timeouted = true
    atomic.AddUint32(&self.state.WaitCount, 0xffffffff)
    return nil
}

func (self *LockDB) DoTimeOut(lock *Lock) (err error) {
    lock.manager.glock.Lock()

    if lock.timeouted {
        lock.manager.glock.Unlock()
        return nil
    }

    lock.timeouted = true
    lock.manager.GetWaitLock()
    lock.manager.glock.Unlock()

    self.slock.Active(lock.protocol, lock.command, RESULT_TIMEOUT, false)
    atomic.AddUint32(&self.state.WaitCount, 0xffffffff)
    if lock.timeout_time > lock.start_time {
        self.slock.Log().Infof("lock timeout %d %x %x %x %s", lock.command.DbId, lock.command.LockKey, lock.command.LockId, lock.command.RequestId, lock.protocol.RemoteAddr().String())
        atomic.AddUint32(&self.state.TimeoutedCount, 1)
    }

    return nil
}

func (self *LockDB) AddExpried(lock *Lock) (err error) {
    lock.expried = false

    if lock.expried_time <= lock.start_time {
        go func() {
            self.DoExpried(lock)
            if lock.ref_count == 0 {
                lock.manager.glock.Lock()
                if lock.ref_count == 0 {
                    lock.manager.FreeLock(lock)
                }
                lock.manager.glock.Unlock()
            }

            if lock.manager.locked <= 0 {
                self.RemoveLockManager(lock.manager)
            }
        }()
        return nil
    }

    if lock.expried_checked_count > 5 {
        if lock.protocol.stream.closed {
            expried_time := self.check_expried_time + 2
            self.expried_locks[expried_time % EXPRIED_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
            lock.ref_count++
        } else {
            expried_time := self.check_expried_time + 5
            if lock.expried_time < expried_time {
                expried_time = lock.expried_time
                if expried_time < self.check_expried_time {
                    expried_time = self.check_expried_time + 2
                }
            }

            self.expried_locks[expried_time % EXPRIED_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
            lock.ref_count++
        }
    }else{
        expried_time := self.check_expried_time + lock.expried_checked_count
        if lock.expried_time < expried_time {
            expried_time = lock.expried_time
            if expried_time < self.check_expried_time {
                expried_time = self.check_expried_time + 2
            }
        }

        self.expried_locks[expried_time % EXPRIED_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
        lock.ref_count++
    }
    return nil
}

func (self *LockDB) RemoveExpried(lock *Lock) (err error) {
    lock.expried = true
    return nil
}

func (self *LockDB) DoExpried(lock *Lock) (err error) {
    lock.manager.glock.Lock()

    if lock.expried {
        lock.manager.glock.Unlock()
        return nil
    }

    lock_manager := lock.manager
    lock.expried = true
    lock_manager.locked--
    lock_manager.RemoveLock(lock)

    wait_lock := lock_manager.GetWaitLock()
    if wait_lock != nil {
        if self.DoLock(lock_manager, wait_lock) {
            lock_manager.wait_locks.Pop()
            wait_lock.ref_count--
            //self.RemoveTimeOut(wait_lock)
            lock.timeouted = true
            lock.manager.glock.Unlock()

            atomic.AddUint32(&self.state.LockedCount, 0xffffffff)
            if lock.expried_time > lock.start_time {
                self.slock.Active(lock.protocol, lock.command, RESULT_EXPRIED, false)
                atomic.AddUint32(&self.state.ExpriedCount, 1)
                self.slock.Log().Infof("lock expried %d %x %x %x %s", lock.command.DbId, lock.command.LockKey, lock.command.LockId, lock.command.RequestId, lock.protocol.RemoteAddr().String())
            }

            self.slock.Active(wait_lock.protocol, wait_lock.command, RESULT_SUCCED, false)
            atomic.AddUint64(&self.state.LockCount, 1)
            atomic.AddUint32(&self.state.LockedCount, 1)
            atomic.AddUint32(&self.state.WaitCount, 0xffffffff)
            return nil
        }
    }

    lock.manager.glock.Unlock()
    atomic.AddUint32(&self.state.LockedCount, 0xffffffff)
    if lock.expried_time > lock.start_time {
        self.slock.Active(lock.protocol, lock.command, RESULT_EXPRIED, false)
        atomic.AddUint32(&self.state.ExpriedCount, 1)
        self.slock.Log().Infof("lock expried %d %x %x %x %s", lock.command.DbId, lock.command.LockKey, lock.command.LockId, lock.command.RequestId, lock.protocol.RemoteAddr().String())
    }

    return nil
}

func (self *LockDB) Lock(protocol *ServerProtocol, command *LockCommand) (err error) {
    lock_manager := self.GetOrNewLockManager(command)

    lock_manager.glock.Lock()

    if lock_manager.freed {
        lock_manager.glock.Unlock()
        return self.Lock(protocol, command)
    }

    if lock_manager.locked > 0 {
        if command.Flag & 0x01 == 1 {
            lock_manager.glock.Unlock()

            command.LockId = lock_manager.current_lock.command.LockId
            if lock_manager.current_lock.protocol.stream.closed {
                self.slock.Active(protocol, command, RESULT_SUCCED, true)
            }else {
                self.slock.Active(protocol, command, RESULT_UNOWN_ERROR, true)
            }
            protocol.FreeLockCommand(command)
            return nil
        }

        current_lock := lock_manager.GetLockedLock(command)
        if current_lock != nil {
            lock_manager.glock.Unlock()

            self.slock.Active(protocol, command, RESULT_LOCKED_ERROR, true)
            protocol.FreeLockCommand(command)
            return nil
        }
    }

    lock := lock_manager.GetOrNewLock(protocol, command)
    if !self.DoLock(lock_manager, lock) {
        lock_manager.AddWaitLock(lock)
        self.AddTimeOut(lock)
        lock_manager.glock.Unlock()

        atomic.AddUint32(&self.state.WaitCount, 1)
    } else {
        lock_manager.glock.Unlock()

        self.slock.Active(protocol, lock.command, RESULT_SUCCED, true)
        atomic.AddUint64(&self.state.LockCount, 1)
        atomic.AddUint32(&self.state.LockedCount, 1)
    }

    return nil
}

func (self *LockDB) UnLock(protocol *ServerProtocol, command *LockCommand) (err error) {
    lock_manager := self.GetLockManager(command)
    if lock_manager == nil {
        self.slock.Active(protocol, command, RESULT_UNLOCK_ERROR, true)
        protocol.FreeLockCommand(command)
        atomic.AddUint32(&self.state.UnlockErrorCount, 1)
        return nil
    }

    lock_manager.glock.Lock()

    if lock_manager.locked <= 0 {
        lock_manager.glock.Unlock()

        self.slock.Active(protocol, command, RESULT_UNLOCK_ERROR, true)
        protocol.FreeLockCommand(command)
        atomic.AddUint32(&self.state.UnlockErrorCount, 1)
        return nil
    }

    current_lock := lock_manager.GetLockedLock(command)
    if current_lock == nil {
        current_lock = lock_manager.current_lock

        if command.Flag & 0x01 == 1 {
            if current_lock == nil {
                lock_manager.glock.Unlock()

                self.slock.Active(protocol, command, RESULT_UNOWN_ERROR, true)
                protocol.FreeLockCommand(command)
                atomic.AddUint32(&self.state.UnlockErrorCount, 1)
                return nil
            }

            command.LockId = current_lock.command.LockId
        } else {
            lock_manager.glock.Unlock()

            self.slock.Active(protocol, command, RESULT_UNOWN_ERROR, true)
            protocol.FreeLockCommand(command)
            atomic.AddUint32(&self.state.UnlockErrorCount, 1)
            return nil
        }
    }

    //self.RemoveExpried(current_lock)
    current_lock.expried = true
    lock_manager.RemoveLock(current_lock)
    lock_manager.locked--

    wait_lock := lock_manager.GetWaitLock()
    if wait_lock != nil {
        if self.DoLock(lock_manager, wait_lock) {
            lock_manager.wait_locks.Pop()
            wait_lock.ref_count--
            //self.RemoveTimeOut(wait_lock)
            wait_lock.timeouted = true
            lock_manager.glock.Unlock()

            self.slock.Active(protocol, command, RESULT_SUCCED, true)
            self.slock.Active(wait_lock.protocol, wait_lock.command, RESULT_SUCCED, wait_lock.protocol == protocol)
            atomic.AddUint64(&self.state.LockCount, 1)
            atomic.AddUint32(&self.state.LockedCount, 1)
            atomic.AddUint32(&self.state.WaitCount, 0xffffffff)
        } else{
            lock_manager.glock.Unlock()
            self.slock.Active(protocol, command, RESULT_SUCCED, true)
        }
    }else{
        lock_manager.glock.Unlock()
        self.slock.Active(protocol, command, RESULT_SUCCED, true)
    }

    protocol.FreeLockCommand(command)
    protocol.FreeLockCommand(current_lock.command)
    atomic.AddUint64(&self.state.UnLockCount, 1)
    atomic.AddUint32(&self.state.LockedCount, 0xffffffff)

    return nil
}

func (self *LockDB) DoLock(lock_manager *LockManager, lock *Lock) bool{
    if lock_manager.locked == 0 {
        lock_manager.AddLock(lock)
        lock_manager.locked++
        self.AddExpried(lock)
        return true
    }

    if(lock_manager.locked <= lock.command.Count && lock_manager.locked <= lock_manager.current_lock.command.Count){
        lock_manager.AddLock(lock)
        lock_manager.locked++
        self.AddExpried(lock)
        return true
    }

    return false
}

func (self *LockDB) GetState() *LockDBState {
    return &self.state
}
