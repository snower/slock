package server

import (
	"github.com/snower/slock/protocol"
	"sync"
	"sync/atomic"
)

type LockManager struct {
	lockDb       *LockDB
	lockKey      [16]byte
	currentLock  *Lock
	locks        *LockQueue
	lockMaps     map[[16]byte]*Lock
	waitLocks    *LockQueue
	glock        *PriorityMutex
	freeLocks    *LockQueue
	fastKeyValue *FastKeyValue
	state        *protocol.LockDBState
	refCount     uint32
	locked       uint32
	glockIndex   uint16
	dbId         uint8
	waited       bool
}

func NewLockManager(lockDb *LockDB, command *protocol.LockCommand, glock *PriorityMutex, glockIndex uint16, freeLocks *LockQueue, state *protocol.LockDBState) *LockManager {
	return &LockManager{lockDb, command.LockKey,
		nil, nil, nil, nil, glock, freeLocks, nil, state, 0, 0,
		glockIndex, command.DbId, false}
}

func (self *LockManager) GetDB() *LockDB {
	return self.lockDb
}

func (self *LockManager) AddLock(lock *Lock) *Lock {
	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_UNRENEW_EXPRIED_TIME_WHEN_TIMEOUT == 0 {
		lock.startTime = self.lockDb.currentTime
		if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
			lock.expriedTime = 0x7fffffffffffffff
		} else if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
			if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
				lock.expriedTime = lock.startTime + int64(lock.command.Expried)*60 + 1
			} else {
				lock.expriedTime = lock.startTime + int64(lock.command.Expried) + 1
			}
		} else {
			lock.expriedTime = 0
		}
	}

	switch lock.command.ExpriedFlag & 0x1300 {
	case protocol.EXPRIED_FLAG_ZEOR_AOF_TIME:
		lock.aofTime = 0
	case protocol.EXPRIED_FLAG_UNLIMITED_AOF_TIME:
		lock.aofTime = 0xff
	case protocol.EXPRIED_FLAG_AOF_TIME_OF_EXPRIED_PARCENT:
		lock.aofTime = uint8(float64(lock.command.Expried) * Config.DBLockAofParcentTime)
	default:
		lock.aofTime = self.lockDb.aofTime
	}

	lock.locked = 1
	lock.refCount++
	if lock.command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0 {
		lock.isAof = true
	} else {
		if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED != 0 {
			lock.ackCount = 0
		}
	}

	if self.currentLock == nil {
		self.currentLock = lock
		return lock
	}

	_ = self.locks.Push(lock)
	self.lockMaps[lock.command.LockId] = lock
	return lock
}

func (self *LockManager) RemoveLock(lock *Lock) *Lock {
	lock.locked = 0
	lock.ackCount = 0xff

	if self.currentLock == lock {
		self.currentLock = nil
		lock.refCount--

		lockedLock := self.locks.Pop()
		for lockedLock != nil {
			if lockedLock.locked > 0 {
				delete(self.lockMaps, lockedLock.command.LockId)
				self.currentLock = lockedLock
				break
			}

			lockedLock.refCount--
			if lockedLock.refCount == 0 {
				self.FreeLock(lockedLock)
			}
			lockedLock = self.locks.Pop()
		}

		if self.locks.headNodeIndex >= 8 {
			_ = self.locks.Resize()
		}
		return lock
	}

	delete(self.lockMaps, lock.command.LockId)
	lockedLock := self.locks.Head()
	for lockedLock != nil {
		if lockedLock.locked > 0 {
			break
		}

		self.locks.Pop()
		lockedLock.refCount--
		if lockedLock.refCount == 0 {
			self.FreeLock(lockedLock)
		}
		lockedLock = self.locks.Head()
	}

	if self.locks.headNodeIndex >= 8 {
		_ = self.locks.Resize()
	}
	return lock
}

func (self *LockManager) GetLockedLock(command *protocol.LockCommand) *Lock {
	if self.currentLock.command.LockId == command.LockId {
		return self.currentLock
	}

	lockedLock, ok := self.lockMaps[command.LockId]
	if ok {
		return lockedLock
	}
	return nil
}

func (self *LockManager) UpdateLockedLock(lock *Lock, timeout uint16, timeout_flag uint16, expried uint16, expried_flag uint16, count uint16, rcount uint8) {
	lock.command.Timeout = timeout
	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED != 0 {
		lock.command.TimeoutFlag = timeout_flag
		lock.command.TimeoutFlag |= protocol.TIMEOUT_FLAG_REQUIRE_ACKED
	} else {
		lock.command.TimeoutFlag = timeout_flag
	}
	lock.command.Expried = expried
	lock.command.ExpriedFlag = expried_flag
	lock.command.Count = count
	lock.command.Rcount = rcount

	lock.startTime = self.lockDb.currentTime
	if timeout_flag&protocol.TIMEOUT_FLAG_MILLISECOND_TIME == 0 {
		if timeout_flag&protocol.TIMEOUT_FLAG_MINUTE_TIME != 0 {
			lock.timeoutTime = lock.startTime + int64(timeout)*60 + 1
		} else {
			lock.timeoutTime = lock.startTime + int64(timeout) + 1
		}
	} else {
		lock.timeoutTime = 0
	}

	if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
		lock.expriedTime = 0x7fffffffffffffff
	} else if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
		if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
			lock.expriedTime = lock.startTime + int64(lock.command.Expried)*60 + 1
		} else {
			lock.expriedTime = lock.startTime + int64(lock.command.Expried) + 1
		}
	} else {
		lock.expriedTime = 0
	}

	if timeout_flag&protocol.TIMEOUT_FLAG_UPDATE_NO_RESET_TIMEOUT_CHECKED_COUNT == 0 {
		lock.timeoutCheckedCount = 1
	}
	if expried_flag&protocol.EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT == 0 {
		lock.expriedCheckedCount = 1
	}

	switch lock.command.ExpriedFlag & 0x1300 {
	case protocol.EXPRIED_FLAG_ZEOR_AOF_TIME:
		lock.aofTime = 0
	case protocol.EXPRIED_FLAG_UNLIMITED_AOF_TIME:
		lock.aofTime = 0xff
	case protocol.EXPRIED_FLAG_AOF_TIME_OF_EXPRIED_PARCENT:
		lock.aofTime = uint8(float64(lock.command.Expried) * Config.DBLockAofParcentTime)
	default:
		lock.aofTime = self.lockDb.aofTime
	}
}

func (self *LockManager) AddWaitLock(lock *Lock) *Lock {
	_ = self.waitLocks.Push(lock)
	lock.refCount++
	self.waited = true
	return lock
}

func (self *LockManager) GetWaitLock() *Lock {
	lock := self.waitLocks.Head()
	for lock != nil {
		if lock.timeouted {
			self.waitLocks.Pop()
			lock.refCount--
			if lock.refCount == 0 {
				self.FreeLock(lock)
			}
			lock = self.waitLocks.Head()
			continue
		}

		if self.waitLocks.headNodeIndex >= 8 {
			_ = self.waitLocks.Resize()
		}
		return lock
	}

	if self.waitLocks.headNodeIndex >= 8 {
		_ = self.waitLocks.Resize()
	}
	return nil
}

func (self *LockManager) PushLockAof(lock *Lock) error {
	if lock.command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0 {
		lock.isAof = true
		return nil
	}

	fashHash := (uint32(self.lockKey[0])<<24 | uint32(self.lockKey[1])<<16 | uint32(self.lockKey[2])<<8 | uint32(self.lockKey[3])) ^ (uint32(self.lockKey[4])<<24 | uint32(self.lockKey[5])<<16 | uint32(self.lockKey[6])<<8 | uint32(self.lockKey[7])) ^ (uint32(self.lockKey[8])<<24 | uint32(self.lockKey[9])<<16 | uint32(self.lockKey[10])<<8 | uint32(self.lockKey[11])) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fashHash%uint32(self.lockDb.managerMaxGlocks)].Push(lock, protocol.COMMAND_LOCK, nil, 0)
	if err != nil {
		self.lockDb.slock.Log().Errorf("Database lock push aof error DbId:%d LockKey:%x LockId:%x",
			lock.command.DbId, lock.command.LockKey, lock.command.LockId)
		return err
	}
	lock.isAof = true
	return nil
}

func (self *LockManager) PushUnLockAof(lock *Lock, command *protocol.LockCommand, isAof bool, aofFlag uint16) error {
	if command == nil {
		if self.lockDb.status != STATE_LEADER {
			lock.isAof = isAof
			return nil
		}
	} else {
		if command.Flag&protocol.UNLOCK_FLAG_FROM_AOF != 0 {
			lock.isAof = isAof
			return nil
		}
	}

	fashHash := (uint32(self.lockKey[0])<<24 | uint32(self.lockKey[1])<<16 | uint32(self.lockKey[2])<<8 | uint32(self.lockKey[3])) ^ (uint32(self.lockKey[4])<<24 | uint32(self.lockKey[5])<<16 | uint32(self.lockKey[6])<<8 | uint32(self.lockKey[7])) ^ (uint32(self.lockKey[8])<<24 | uint32(self.lockKey[9])<<16 | uint32(self.lockKey[10])<<8 | uint32(self.lockKey[11])) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fashHash%uint32(self.lockDb.managerMaxGlocks)].Push(lock, protocol.COMMAND_UNLOCK, command, aofFlag)
	if err != nil {
		self.lockDb.slock.Log().Errorf("Database lock push aof error DbId:%d LockKey:%x LockId:%x",
			lock.command.DbId, lock.command.LockKey, lock.command.LockId)
		return err
	}
	lock.isAof = isAof
	return nil
}

func (self *LockManager) FreeLock(lock *Lock) *Lock {
	self.refCount--
	lock.manager = nil
	lock.protocol = nil
	lock.command = nil
	_ = self.freeLocks.Push(lock)
	return lock
}

func (self *LockManager) GetOrNewLock(serverProtocol ServerProtocol, command *protocol.LockCommand) *Lock {
	lock := self.freeLocks.PopRight()
	if lock == nil {
		lock = NewLock(self, serverProtocol, command)
	}
	now := self.lockDb.currentTime

	lock.manager = self
	lock.command = command
	lock.protocol = serverProtocol.GetProxy()
	lock.startTime = now
	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_UNRENEW_EXPRIED_TIME_WHEN_TIMEOUT != 0 {
		if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
			lock.expriedTime = 0x7fffffffffffffff
		} else if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
			if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
				lock.expriedTime = lock.startTime + int64(lock.command.Expried)*60 + 1
			} else {
				lock.expriedTime = lock.startTime + int64(lock.command.Expried) + 1
			}
		} else {
			lock.expriedTime = 0
		}
	} else {
		lock.expriedTime = 0
	}
	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_MILLISECOND_TIME == 0 {
		if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_MINUTE_TIME != 0 {
			lock.timeoutTime = now + int64(command.Timeout)*60 + 1
		} else {
			lock.timeoutTime = now + int64(command.Timeout) + 1
		}
	} else {
		lock.timeoutTime = 0
	}
	lock.timeoutCheckedCount = 1
	lock.expriedCheckedCount = 1
	lock.longWaitIndex = 0
	self.refCount++
	return lock
}

type Lock struct {
	manager             *LockManager
	command             *protocol.LockCommand
	protocol            *ServerProtocolProxy
	startTime           int64
	expriedTime         int64
	timeoutTime         int64
	longWaitIndex       uint64
	timeoutCheckedCount uint8
	expriedCheckedCount uint8
	refCount            uint8
	locked              uint8
	ackCount            uint8
	timeouted           bool
	expried             bool
	aofTime             uint8
	isAof               bool
}

func NewLock(manager *LockManager, protocol ServerProtocol, command *protocol.LockCommand) *Lock {
	return &Lock{manager, command, protocol.GetProxy(), 0, 0, 0,
		0, 1, 1, 0, 0, 0xff, false, false, 0, false}
}

func (self *Lock) GetDB() *LockDB {
	if self.manager == nil {
		return nil
	}
	return self.manager.GetDB()
}

type PriorityMutex struct {
	mutex                    sync.Mutex
	highPriority             uint32
	lowPriority              uint32
	highPriorityAcquireCount uint32
	highPrioritywaiter       sync.Mutex
	lowPrioritywaiter        sync.Mutex
	setHighPriorityCount     uint64
	setLowPriorityCount      uint64
}

func NewPriorityMutex() *PriorityMutex {
	return &PriorityMutex{sync.Mutex{}, 0, 0, 0,
		sync.Mutex{}, sync.Mutex{}, 0, 0}
}

func (self *PriorityMutex) Lock() {
	if self.highPriority == 1 {
		if atomic.CompareAndSwapUint32(&self.highPriority, 1, 1) {
			self.highPrioritywaiter.Lock()
			self.highPrioritywaiter.Unlock()
		}
	}
	self.mutex.Lock()
	if self.highPriority == 1 {
		for {
			self.mutex.Unlock()
			if atomic.CompareAndSwapUint32(&self.highPriority, 1, 1) {
				self.highPrioritywaiter.Lock()
				self.highPrioritywaiter.Unlock()
			}
			self.mutex.Lock()
			if self.highPriority == 0 {
				return
			}
		}
	}
}

func (self *PriorityMutex) Unlock() {
	self.mutex.Unlock()
}

func (self *PriorityMutex) HighSetPriority() {
	if atomic.CompareAndSwapUint32(&self.highPriority, 0, 1) {
		self.highPrioritywaiter.Lock()
		self.setHighPriorityCount++
	}
	if atomic.CompareAndSwapUint32(&self.highPriorityAcquireCount, 0, 0) {
		self.HighUnSetPriority()
	}
}

func (self *PriorityMutex) HighUnSetPriority() {
	if atomic.CompareAndSwapUint32(&self.highPriority, 1, 0) {
		self.highPrioritywaiter.Unlock()
	}
}

func (self *PriorityMutex) LowSetPriority() {
	if atomic.CompareAndSwapUint32(&self.lowPriority, 0, 1) {
		self.lowPrioritywaiter.Lock()
		self.setLowPriorityCount++
	}
}

func (self *PriorityMutex) LowUnSetPriority() {
	if atomic.CompareAndSwapUint32(&self.lowPriority, 1, 0) {
		self.lowPrioritywaiter.Unlock()
	}
}

func (self *PriorityMutex) HighPriorityLock() {
	atomic.AddUint32(&self.highPriorityAcquireCount, 1)
	self.mutex.Lock()
}

func (self *PriorityMutex) HighPriorityUnlock() {
	atomic.AddUint32(&self.highPriorityAcquireCount, 0xffffffff)
	if atomic.CompareAndSwapUint32(&self.highPriorityAcquireCount, 0, 0) {
		self.HighUnSetPriority()
	}
	self.mutex.Unlock()
}

func (self *PriorityMutex) LowPriorityLock() {
	if self.lowPriority == 1 {
		if atomic.CompareAndSwapUint32(&self.lowPriority, 1, 1) {
			self.lowPrioritywaiter.Lock()
			self.lowPrioritywaiter.Unlock()
		}
	}
	self.Lock()
	if self.lowPriority == 1 {
		for {
			self.Unlock()
			if atomic.CompareAndSwapUint32(&self.lowPriority, 1, 1) {
				self.lowPrioritywaiter.Lock()
				self.lowPrioritywaiter.Unlock()
			}
			self.Lock()
			if self.lowPriority == 0 {
				return
			}
		}
	}
}

func (self *PriorityMutex) LowPriorityUnlock() {
	self.Unlock()
}
