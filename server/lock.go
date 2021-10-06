package server

import (
	"github.com/snower/slock/protocol"
	"sync"
)

type LockManager struct {
	lockDb       *LockDB
	lockKey      [16]byte
	currentLock  *Lock
	locks        *LockQueue
	lockMaps     map[[16]byte]*Lock
	waitLocks    *LockQueue
	glock        *sync.Mutex
	freeLocks    *LockQueue
	fastKeyValue *FastKeyValue
	refCount     uint32
	locked       uint32
	dbId         uint8
	waited       bool
	freed        bool
	glockIndex   int8
}

func NewLockManager(lockDb *LockDB, command *protocol.LockCommand, glock *sync.Mutex, glockIndex int8, freeLocks *LockQueue) *LockManager {
	return &LockManager{lockDb, command.LockKey,
		nil, nil, nil, nil, glock, freeLocks, nil, 0, 0,
		command.DbId, false, true, glockIndex}
}

func (self *LockManager) GetDB() *LockDB {
	return self.lockDb
}

func (self *LockManager) AddLock(lock *Lock) *Lock {
	if lock.command.TimeoutFlag&0x0100 == 0 {
		lock.startTime = self.lockDb.currentTime
		if lock.command.ExpriedFlag&0x0400 == 0 {
			lock.expriedTime = lock.startTime + int64(lock.command.Expried) + 1
		} else if lock.command.ExpriedFlag&0x4000 != 0 {
			lock.expriedTime = 0x7fffffffffffffff
		}
	}

	switch lock.command.ExpriedFlag & 0x1300 {
	case 0x0100:
		lock.aofTime = 0
	case 0x0200:
		lock.aofTime = 0xff
	case 0x1000:
		lock.aofTime = uint8(float64(lock.command.Expried) * Config.DBLockAofParcentTime)
	default:
		lock.aofTime = self.lockDb.aofTime
	}

	lock.locked = 1
	lock.refCount++
	lock.ackCount = 0

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
	if lock.command.TimeoutFlag&0x1000 != 0 {
		lock.command.TimeoutFlag = timeout_flag
		lock.command.TimeoutFlag |= 0x1000
	} else {
		lock.command.TimeoutFlag = timeout_flag
	}
	lock.command.Expried = expried
	lock.command.ExpriedFlag = expried_flag
	lock.command.Count = count
	lock.command.Rcount = rcount

	lock.startTime = self.lockDb.currentTime
	if timeout_flag&0x0400 == 0 {
		lock.timeoutTime = lock.startTime + int64(timeout) + 1
	} else {
		lock.timeoutTime = 0
	}

	if expried_flag&0x0400 == 0 {
		lock.expriedTime = lock.startTime + int64(expried) + 1
	} else if lock.command.ExpriedFlag&0x4000 != 0 {
		lock.expriedTime = 0x7fffffffffffffff
	} else {
		lock.expriedTime = 0
	}

	if timeout_flag&0x2000 == 0 {
		lock.timeoutCheckedCount = 1
	}
	if expried_flag&0x2000 == 0 {
		lock.expriedCheckedCount = 1
	}

	switch lock.command.ExpriedFlag & 0x1300 {
	case 0x0100:
		lock.aofTime = 0
	case 0x0200:
		lock.aofTime = 0xff
	case 0x1000:
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

		if self.waitLocks.headNodeIndex >= 6 {
			_ = self.waitLocks.Resize()
		}
		return lock
	}

	if self.waitLocks.headNodeIndex >= 6 {
		_ = self.waitLocks.Resize()
	}
	return nil
}

func (self *LockManager) PushLockAof(lock *Lock) error {
	if lock.command.Flag&0x04 != 0 {
		lock.isAof = true
		return nil
	}

	fashHash := (uint32(self.lockKey[0])<<24 | uint32(self.lockKey[1])<<16 | uint32(self.lockKey[2])<<8 | uint32(self.lockKey[3])) ^ (uint32(self.lockKey[4])<<24 | uint32(self.lockKey[5])<<16 | uint32(self.lockKey[6])<<8 | uint32(self.lockKey[7])) ^ (uint32(self.lockKey[8])<<24 | uint32(self.lockKey[9])<<16 | uint32(self.lockKey[10])<<8 | uint32(self.lockKey[11])) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fashHash%uint32(self.lockDb.managerMaxGlocks)].Push(lock, protocol.COMMAND_LOCK, nil)
	if err != nil {
		self.lockDb.slock.Log().Errorf("Database lock push aof error DbId:%d LockKey:%x LockId:%x",
			lock.command.DbId, lock.command.LockKey, lock.command.LockId)
		return err
	}
	lock.isAof = true
	return nil
}

func (self *LockManager) PushUnLockAof(lock *Lock, command *protocol.LockCommand, isAof bool) error {
	if command == nil {
		if self.lockDb.status != STATE_LEADER {
			lock.isAof = isAof
			return nil
		}
	} else {
		if command.Flag&0x04 != 0 {
			lock.isAof = isAof
			return nil
		}
	}

	fashHash := (uint32(self.lockKey[0])<<24 | uint32(self.lockKey[1])<<16 | uint32(self.lockKey[2])<<8 | uint32(self.lockKey[3])) ^ (uint32(self.lockKey[4])<<24 | uint32(self.lockKey[5])<<16 | uint32(self.lockKey[6])<<8 | uint32(self.lockKey[7])) ^ (uint32(self.lockKey[8])<<24 | uint32(self.lockKey[9])<<16 | uint32(self.lockKey[10])<<8 | uint32(self.lockKey[11])) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fashHash%uint32(self.lockDb.managerMaxGlocks)].Push(lock, protocol.COMMAND_UNLOCK, command)
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

func (self *LockManager) GetOrNewLock(protocol ServerProtocol, command *protocol.LockCommand) *Lock {
	lock := self.freeLocks.PopRight()
	if lock == nil {
		locks := make([]Lock, 8)
		lock = &locks[0]

		for i := 1; i < 8; i++ {
			_ = self.freeLocks.Push(&locks[i])
		}
	}

	now := self.lockDb.currentTime

	lock.manager = self
	lock.command = command
	lock.protocol = protocol
	lock.startTime = now
	if lock.command.TimeoutFlag&0x0100 != 0 {
		if lock.command.ExpriedFlag&0x0400 == 0 {
			lock.expriedTime = lock.startTime + int64(lock.command.Expried) + 1
		} else if lock.command.ExpriedFlag&0x4000 != 0 {
			lock.expriedTime = 0x7fffffffffffffff
		}
	} else {
		lock.expriedTime = 0
	}
	if lock.command.TimeoutFlag&0x0400 == 0 {
		lock.timeoutTime = now + int64(command.Timeout) + 1
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
	protocol            ServerProtocol
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
	now := manager.lockDb.currentTime
	return &Lock{manager, command, protocol, now, 0, now + int64(command.Timeout),
		0, 0, 0, 0, 0, 0, false, false, 0, false}
}

func (self *Lock) GetDB() *LockDB {
	if self.manager == nil {
		return nil
	}
	return self.manager.GetDB()
}
