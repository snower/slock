package slock

import (
    "sync"
    "time"
    "sync/atomic"
)

const TIMEOUT_QUEUE_LENGTH int64 = 15
const EXPRIED_QUEUE_LENGTH int64 = 15

type LockDBState struct {
    _padding0           [14]uint32
    LockCount           uint64
    _padding1           [14]uint32
    UnLockCount         uint64
    _padding2           [15]uint32
    LockedCount         uint32
    _padding3           [15]uint32
    KeyCount            uint32
    _padding4           [15]uint32
    WaitCount           uint32
    _padding5           [15]uint32
    TimeoutedCount      uint32
    _padding6           [15]uint32
    ExpriedCount        uint32
    _padding7           [15]uint32
    UnlockErrorCount    uint32
    _padding8           [15]uint32
}

type LockDB struct {
    slock              *SLock
    locks              map[[2]uint64]*LockManager
    timeout_locks      [][]*LockQueue
    expried_locks      [][]*LockQueue
    current_time       int64
    check_timeout_time int64
    check_expried_time int64
    glock              sync.Mutex
    manager_glocks     []*sync.Mutex
    free_lock_managers  []*LockManager
    free_locks []*LockQueue
    free_lock_manager_count int32
    manager_glock_index int8
    manager_max_glocks  int8
    is_stop             bool
    free_lock_manager_timeout bool
    state LockDBState
}

func NewLockDB(slock *SLock) *LockDB {
    manager_max_glocks := int8(64)
    manager_glocks := make([]*sync.Mutex, manager_max_glocks)
    free_locks := make([]*LockQueue, manager_max_glocks)
    for i:=int8(0); i< manager_max_glocks; i++{
        manager_glocks[i] = &sync.Mutex{}
        free_locks[i] = NewLockQueue(2, 16, 4096)
    }

    now := time.Now().Unix()
    db := &LockDB{slock, make(map[[2]uint64]*LockManager, 2097152), make([][]*LockQueue, TIMEOUT_QUEUE_LENGTH),
    make([][]*LockQueue, EXPRIED_QUEUE_LENGTH), now, now, now, sync.Mutex{},
    manager_glocks, make([]*LockManager, 2097152), free_locks, -1,
    0, manager_max_glocks, false, true, LockDBState{}}

    db.ResizeTimeOut()
    db.ResizeExpried()
    go db.UpdateCurrentTime()
    go db.CheckTimeOut()
    go db.CheckExpried()
    return db
}

func (self *LockDB) ResizeTimeOut () error{
    for i := int64(0); i < TIMEOUT_QUEUE_LENGTH; i++ {
        self.timeout_locks[i] = make([]*LockQueue, self.manager_max_glocks)
        for j := int8(0); j < self.manager_max_glocks; j++ {
            self.timeout_locks[i][j] = NewLockQueue(4, 16, 4096)
        }
    }
    return nil
}

func (self *LockDB) ResizeExpried () error{
    for i := int64(0); i < EXPRIED_QUEUE_LENGTH; i++ {
        self.expried_locks[i] = make([]*LockQueue, self.manager_max_glocks)
        for j := int8(0); j < self.manager_max_glocks; j++ {
            self.expried_locks[i][j] = NewLockQueue(4, 16, 4096)
        }
    }
    return nil
}

func (self *LockDB) UpdateCurrentTime() (err error) {
    for !self.is_stop {
        time.Sleep(5e8)
        self.current_time = time.Now().Unix()
    }
    return nil
}

func (self *LockDB) CheckTimeOut() (err error) {
    for !self.is_stop {
        time.Sleep(1e9)


        check_timeout_time := self.check_timeout_time

        now := time.Now().Unix()
        self.check_timeout_time = now + 1
        for _, manager_glock := range self.manager_glocks {
            manager_glock.Lock()
            manager_glock.Unlock()
        }

        for ; check_timeout_time <= now; {
            go self.CheckTimeTimeOut(check_timeout_time, now)
            check_timeout_time++
        }
    }
    return nil
}

func (self *LockDB) CheckTimeTimeOut(check_timeout_time int64, now int64) (err error) {
    timeout_locks := self.timeout_locks[check_timeout_time % TIMEOUT_QUEUE_LENGTH]
    for i := int8(0); i < self.manager_max_glocks; i++ {
        lock := timeout_locks[i].Pop()
        for ; lock != nil; {
            lock.ref_count--
            if !lock.timeouted {
                if lock.timeout_time <= now {
                    lock_manager := lock.manager
                    self.DoTimeOut(lock)
                    if lock.ref_count == 0 {
                        lock_manager.glock.Lock()
                        if lock.ref_count == 0 {
                            lock_manager.FreeLock(lock)
                        }
                        lock_manager.glock.Unlock()
                    }

                    if lock.manager.locked <= 0 {
                        self.RemoveLockManager(lock.manager)
                    }
                } else {
                    lock.timeout_checked_count++
                    lock.manager.glock.Lock()
                    self.AddTimeOut(lock)
                    lock.manager.glock.Unlock()
                }
            }else{
                lock_manager := lock.manager
                if lock.ref_count == 0 {
                    lock_manager.glock.Lock()
                    if lock.ref_count == 0 {
                        lock_manager.FreeLock(lock)
                    }
                    lock_manager.glock.Unlock()
                }

                if lock_manager.locked <= 0 {
                    self.RemoveLockManager(lock_manager)
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

        check_expried_time := self.check_expried_time

        now := time.Now().Unix()
        self.check_expried_time = now + 1
        for _, manager_glock := range self.manager_glocks {
            manager_glock.Lock()
            manager_glock.Unlock()
        }

        for ; check_expried_time <= now; {
            go self.CheckTimeExpried(check_expried_time, now)
            check_expried_time++
        }

    }
    return nil
}

func (self *LockDB) CheckTimeExpried(check_expried_time int64, now int64) (err error) {
    expried_locks := self.expried_locks[check_expried_time % EXPRIED_QUEUE_LENGTH]
    for i := int8(0); i < self.manager_max_glocks; i++ {
        lock := expried_locks[i].Pop()
        for ; lock != nil; {
            lock.ref_count--
            if !lock.expried {
                if lock.expried_time <= now {
                    lock_manager := lock.manager
                    self.DoExpried(lock)
                    if lock.ref_count == 0 {
                        lock_manager.glock.Lock()
                        if lock.ref_count == 0 {
                            lock_manager.FreeLock(lock)
                        }
                        lock_manager.glock.Unlock()
                    }

                    if lock_manager.locked <= 0 {
                        self.RemoveLockManager(lock.manager)
                    }
                } else {
                    lock.expried_checked_count++
                    lock.manager.glock.Lock()
                    self.AddExpried(lock)
                    lock.manager.glock.Unlock()
                }
            } else {
                lock_manager := lock.manager
                if lock.ref_count == 0 {
                    lock_manager.glock.Lock()
                    if lock.ref_count == 0 {
                        lock_manager.FreeLock(lock)
                    }
                    lock_manager.glock.Unlock()
                }

                if lock_manager.locked <= 0 {
                    self.RemoveLockManager(lock_manager)
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
            lock_managers[i].locks = NewLockQueue(4, 16, 4)
            lock_managers[i].lock_maps = make(map[[2]uint64]*Lock, 8)
            lock_managers[i].wait_locks = NewLockQueue(4, 16, 4)
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

            if self.free_lock_manager_count < 2097151 {
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


            if self.free_lock_manager_timeout && int32(self.state.KeyCount) * 4 < self.free_lock_manager_count {
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
    count := int32((self.state.LockCount - last_lock_count) / 300 * 4)
    if count < 4096 {
        count = 4096
    }

    free_count := 0
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

        free_count++
        if free_count >= 4096 {
            break
        }
    }
    self.glock.Unlock()

    for ; free_count >= 0; {
        for i := int8(0); i < self.manager_max_glocks; i++ {
            if self.free_locks[i].Len() > 64 {
                self.manager_glocks[i].Lock()
                self.free_locks[i].PopRight()
                self.manager_glocks[i].Unlock()
            }
        }
        free_count -= 64
    }
    self.free_lock_manager_timeout = true

    if self.free_lock_manager_timeout && int32(self.state.KeyCount) * 4 < self.free_lock_manager_count {
        go func(last_lock_count uint64) {
            time.Sleep(300 * 1e9)
            self.CheckFreeLockManagerTimeOut(lock_manager, last_lock_count)
        }(self.state.LockCount)
        self.free_lock_manager_timeout = false
    }
    return nil
}

func (self *LockDB) AddTimeOut(lock *Lock) (err error) {
    lock.timeouted = false

    if lock.timeout_checked_count > 5 {
        if lock.protocol.stream.closed {
            self.timeout_locks[self.check_timeout_time % TIMEOUT_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
            lock.ref_count++
        } else {
            timeout_time := self.check_timeout_time + 5
            if lock.timeout_time < timeout_time {
                timeout_time = lock.timeout_time
                if timeout_time < self.check_timeout_time {
                    timeout_time = self.check_timeout_time
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
                timeout_time = self.check_timeout_time
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
    lock_manager := lock.manager
    if lock_manager == nil{
        return nil
    }

    lock_manager.glock.Lock()
    if lock.timeouted {
        lock_manager.glock.Unlock()
        return nil
    }

    lock.timeouted = true
    lock_protocol, lock_command := lock.protocol, lock.command
    lock_manager.GetWaitLock()
    lock_manager.glock.Unlock()

    self.slock.Active(lock_protocol, lock_command, RESULT_TIMEOUT, false)
    self.slock.FreeLockCommand(lock_command)
    atomic.AddUint32(&self.state.WaitCount, 0xffffffff)
    atomic.AddUint32(&self.state.TimeoutedCount, 1)

    lock_key := [16]byte{byte(lock_command.LockKey[0]), byte(lock_command.LockKey[0] >> 8), byte(lock_command.LockKey[0] >> 16), byte(lock_command.LockKey[0] >> 24),
        byte(lock_command.LockKey[0] >> 32), byte(lock_command.LockKey[0] >> 40), byte(lock_command.LockKey[0] >> 48), byte(lock_command.LockKey[0] >> 56),
        byte(lock_command.LockKey[1]), byte(lock_command.LockKey[1] >> 8), byte(lock_command.LockKey[1] >> 16), byte(lock_command.LockKey[1] >> 24),
        byte(lock_command.LockKey[1] >> 32), byte(lock_command.LockKey[1] >> 40), byte(lock_command.LockKey[1] >> 48), byte(lock_command.LockKey[1] >> 56)}

    lock_id := [16]byte{byte(lock_command.LockId[0]), byte(lock_command.LockId[0] >> 8), byte(lock_command.LockId[0] >> 16), byte(lock_command.LockId[0] >> 24),
        byte(lock_command.LockId[0] >> 32), byte(lock_command.LockId[0] >> 40), byte(lock_command.LockId[0] >> 48), byte(lock_command.LockId[0] >> 56),
        byte(lock_command.LockId[1]), byte(lock_command.LockId[1] >> 8), byte(lock_command.LockId[1] >> 16), byte(lock_command.LockId[1] >> 24),
        byte(lock_command.LockId[1] >> 32), byte(lock_command.LockId[1] >> 40), byte(lock_command.LockId[1] >> 48), byte(lock_command.LockId[1] >> 56)}

    request_id := [16]byte{byte(lock_command.RequestId[0]), byte(lock_command.RequestId[0] >> 8), byte(lock_command.RequestId[0] >> 16), byte(lock_command.RequestId[0] >> 24),
        byte(lock_command.RequestId[0] >> 32), byte(lock_command.RequestId[0] >> 40), byte(lock_command.RequestId[0] >> 48), byte(lock_command.RequestId[0] >> 56),
        byte(lock_command.RequestId[1]), byte(lock_command.RequestId[1] >> 8), byte(lock_command.RequestId[1] >> 16), byte(lock_command.RequestId[1] >> 24),
        byte(lock_command.RequestId[1] >> 32), byte(lock_command.RequestId[1] >> 40), byte(lock_command.RequestId[1] >> 48), byte(lock_command.RequestId[1] >> 56)}

    self.slock.Log().Infof("LockTimeout DbId:%d LockKey:%x LockId:%x RequestId:%x RemoteAddr:%s", lock_command.DbId, lock_key, lock_id, request_id, lock.protocol.RemoteAddr().String())

    return nil
}

func (self *LockDB) AddExpried(lock *Lock) (err error) {
    lock.expried = false

    if lock.expried_checked_count > 5 {
        if lock.protocol.stream.closed {
            self.expried_locks[self.check_expried_time % EXPRIED_QUEUE_LENGTH][lock.manager.glock_index].Push(lock)
            lock.ref_count++
        } else {
            expried_time := self.check_expried_time + 5
            if lock.expried_time < expried_time {
                expried_time = lock.expried_time
                if expried_time < self.check_expried_time {
                    expried_time = self.check_expried_time
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
                expried_time = self.check_expried_time
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
    lock_manager := lock.manager
    if lock_manager == nil{
        return nil
    }

    lock_manager.glock.Lock()
    if lock.expried {
        lock_manager.glock.Unlock()
        return nil
    }

    lock.expried = true
    lock_manager.locked--
    lock_protocol, lock_command := lock.protocol, lock.command
    lock_manager.RemoveLock(lock)

    wait_lock := lock_manager.GetWaitLock()
    lock_manager.glock.Unlock()

    self.slock.Active(lock_protocol, lock_command, RESULT_EXPRIED, false)
    self.slock.FreeLockCommand(lock_command)
    atomic.AddUint32(&self.state.LockedCount, 0xffffffff)
    atomic.AddUint32(&self.state.ExpriedCount, 1)

    lock_key := [16]byte{byte(lock_command.LockKey[0]), byte(lock_command.LockKey[0] >> 8), byte(lock_command.LockKey[0] >> 16), byte(lock_command.LockKey[0] >> 24),
        byte(lock_command.LockKey[0] >> 32), byte(lock_command.LockKey[0] >> 40), byte(lock_command.LockKey[0] >> 48), byte(lock_command.LockKey[0] >> 56),
        byte(lock_command.LockKey[1]), byte(lock_command.LockKey[1] >> 8), byte(lock_command.LockKey[1] >> 16), byte(lock_command.LockKey[1] >> 24),
        byte(lock_command.LockKey[1] >> 32), byte(lock_command.LockKey[1] >> 40), byte(lock_command.LockKey[1] >> 48), byte(lock_command.LockKey[1] >> 56)}

    lock_id := [16]byte{byte(lock_command.LockId[0]), byte(lock_command.LockId[0] >> 8), byte(lock_command.LockId[0] >> 16), byte(lock_command.LockId[0] >> 24),
        byte(lock_command.LockId[0] >> 32), byte(lock_command.LockId[0] >> 40), byte(lock_command.LockId[0] >> 48), byte(lock_command.LockId[0] >> 56),
        byte(lock_command.LockId[1]), byte(lock_command.LockId[1] >> 8), byte(lock_command.LockId[1] >> 16), byte(lock_command.LockId[1] >> 24),
        byte(lock_command.LockId[1] >> 32), byte(lock_command.LockId[1] >> 40), byte(lock_command.LockId[1] >> 48), byte(lock_command.LockId[1] >> 56)}

    request_id := [16]byte{byte(lock_command.RequestId[0]), byte(lock_command.RequestId[0] >> 8), byte(lock_command.RequestId[0] >> 16), byte(lock_command.RequestId[0] >> 24),
        byte(lock_command.RequestId[0] >> 32), byte(lock_command.RequestId[0] >> 40), byte(lock_command.RequestId[0] >> 48), byte(lock_command.RequestId[0] >> 56),
        byte(lock_command.RequestId[1]), byte(lock_command.RequestId[1] >> 8), byte(lock_command.RequestId[1] >> 16), byte(lock_command.RequestId[1] >> 24),
        byte(lock_command.RequestId[1] >> 32), byte(lock_command.RequestId[1] >> 40), byte(lock_command.RequestId[1] >> 48), byte(lock_command.RequestId[1] >> 56)}

    self.slock.Log().Infof("LockExpried DbId:%d LockKey:%x LockId:%x RequestId:%x RemoteAddr:%s", lock_command.DbId, lock_key, lock_id, request_id, lock.protocol.RemoteAddr().String())

    if wait_lock != nil {
        lock_manager.glock.Lock()
        for ;; {
            if self.DoLock(lock_manager, wait_lock) {
                wait_lock = self.WakeUpWaitLock(lock_manager, wait_lock, nil)
                if wait_lock !=  nil {
                    lock_manager.glock.Lock()
                    continue
                }
                lock_manager.waited = false
            }
            return nil
        }

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
    if self.DoLock(lock_manager, lock) {
        if command.Expried > 0 {
            lock_manager.AddLock(lock)
            lock_manager.locked++
            self.AddExpried(lock)
            lock_manager.glock.Unlock()

            self.slock.Active(protocol, command, RESULT_SUCCED, true)
            atomic.AddUint64(&self.state.LockCount, 1)
            atomic.AddUint32(&self.state.LockedCount, 1)
            return nil
        }

        lock_manager.FreeLock(lock)
        lock_manager.glock.Unlock()

        self.slock.Active(protocol, command, RESULT_SUCCED, true)
        protocol.FreeLockCommand(command)
        atomic.AddUint64(&self.state.LockCount, 1)

        if lock_manager.locked <= 0 {
            self.RemoveLockManager(lock_manager)
        }
        return nil
    }

    if command.Timeout > 0 {
        lock_manager.AddWaitLock(lock)
        self.AddTimeOut(lock)
        lock_manager.glock.Unlock()

        atomic.AddUint32(&self.state.WaitCount, 1)
        return nil
    }

    lock_manager.FreeLock(lock)
    lock_manager.glock.Unlock()

    self.slock.Active(protocol, command, RESULT_TIMEOUT, true)
    protocol.FreeLockCommand(command)

    if lock_manager.locked <= 0 {
        self.RemoveLockManager(lock_manager)
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
    current_lock_command := current_lock.command
    lock_manager.RemoveLock(current_lock)
    lock_manager.locked--
    wait_lock := lock_manager.GetWaitLock()
    lock_manager.glock.Unlock()

    self.slock.Active(protocol, command, RESULT_SUCCED, true)
    protocol.FreeLockCommand(command)
    protocol.FreeLockCommand(current_lock_command)
    atomic.AddUint64(&self.state.UnLockCount, 1)
    atomic.AddUint32(&self.state.LockedCount, 0xffffffff)

    if wait_lock != nil {
        lock_manager.glock.Lock()
        for ;; {
            if self.DoLock(lock_manager, wait_lock) {
                wait_lock = self.WakeUpWaitLock(lock_manager, wait_lock, protocol)
                if wait_lock !=  nil {
                    lock_manager.glock.Lock()
                    continue
                }
                lock_manager.waited = false
            }
            return nil
        }

    }
    return nil
}

func (self *LockDB) DoLock(lock_manager *LockManager, lock *Lock) bool{
    if lock_manager.locked == 0 {
        return true
    }

    if lock_manager.waited {
        return false
    }

    if(lock_manager.locked <= lock.command.Count && lock_manager.locked <= lock_manager.current_lock.command.Count){
        return true
    }

    return false
}

func (self *LockDB) WakeUpWaitLock(lock_manager *LockManager, wait_lock *Lock, protocol *ServerProtocol) *Lock {
    if wait_lock.timeouted {
        wait_lock = lock_manager.GetWaitLock()
        lock_manager.glock.Unlock()
        return wait_lock
    }

    //self.RemoveTimeOut(wait_lock)
    wait_lock.timeouted = true

    if wait_lock.command.Expried > 0 {
        lock_manager.AddLock(wait_lock)
        lock_manager.locked++
        self.AddExpried(wait_lock)
        lock_manager.glock.Unlock()

        self.slock.Active(wait_lock.protocol, wait_lock.command, RESULT_SUCCED, wait_lock.protocol == protocol)
        atomic.AddUint64(&self.state.LockCount, 1)
        atomic.AddUint32(&self.state.LockedCount, 1)
        atomic.AddUint32(&self.state.WaitCount, 0xffffffff)
        return nil
    }

    wait_lock_protocol, wait_lock_command := wait_lock.protocol, wait_lock.command
    wait_lock = lock_manager.GetWaitLock()
    lock_manager.glock.Unlock()

    if wait_lock_protocol == protocol {
        self.slock.Active(wait_lock_protocol, wait_lock_command, RESULT_SUCCED, true)
        protocol.FreeLockCommand(wait_lock_command)
    } else {
        self.slock.Active(wait_lock_protocol, wait_lock_command, RESULT_SUCCED, false)
        self.slock.FreeLockCommand(wait_lock_command)
    }

    atomic.AddUint64(&self.state.LockCount, 1)
    atomic.AddUint32(&self.state.WaitCount, 0xffffffff)

    if lock_manager.locked <= 0 {
        self.RemoveLockManager(lock_manager)
    }
    return wait_lock
}

func (self *LockDB) GetState() *LockDBState {
    return &self.state
}
