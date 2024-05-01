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
	currentData  *LockManagerData
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
		nil, nil, nil, nil, nil, glock, freeLocks, nil, state, 0, 0,
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
			lock.expriedTime = lock.startTime + int64(lock.command.Expried)/1000 + 1
		}
	}

	if self.currentLock == nil {
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
	} else {
		lock.aofTime = self.currentLock.aofTime
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
	} else {
		_ = self.locks.Push(lock)
		self.lockMaps[lock.command.LockId] = lock
	}
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
		lock.timeoutTime = lock.startTime + int64(timeout)/1000 + 1
	}

	if expried_flag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
		lock.expriedTime = 0x7fffffffffffffff
	} else if expried_flag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
		if expried_flag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
			lock.expriedTime = lock.startTime + int64(expried)*60 + 1
		} else {
			lock.expriedTime = lock.startTime + int64(expried) + 1
		}
	} else {
		lock.expriedTime = lock.startTime + int64(expried)/1000 + 1
	}

	if timeout_flag&protocol.TIMEOUT_FLAG_UPDATE_NO_RESET_TIMEOUT_CHECKED_COUNT == 0 {
		lock.timeoutCheckedCount = 1
	}
	if expried_flag&protocol.EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT == 0 {
		lock.expriedCheckedCount = 1
	}

	if !lock.isAof && self.currentLock == lock && self.locks.Head() == nil {
		switch expried_flag & 0x1300 {
		case protocol.EXPRIED_FLAG_ZEOR_AOF_TIME:
			lock.aofTime = 0
		case protocol.EXPRIED_FLAG_UNLIMITED_AOF_TIME:
			lock.aofTime = 0xff
		case protocol.EXPRIED_FLAG_AOF_TIME_OF_EXPRIED_PARCENT:
			lock.aofTime = uint8(float64(expried) * Config.DBLockAofParcentTime)
		default:
			lock.aofTime = self.lockDb.aofTime
		}
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

func (self *LockManager) PushLockAof(lock *Lock, aofFlag uint16) error {
	if lock.command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0 {
		lock.isAof = true
		return nil
	}

	fashHash := (uint32(self.lockKey[0])<<24 | uint32(self.lockKey[1])<<16 | uint32(self.lockKey[2])<<8 | uint32(self.lockKey[3])) ^ (uint32(self.lockKey[4])<<24 | uint32(self.lockKey[5])<<16 | uint32(self.lockKey[6])<<8 | uint32(self.lockKey[7])) ^ (uint32(self.lockKey[8])<<24 | uint32(self.lockKey[9])<<16 | uint32(self.lockKey[10])<<8 | uint32(self.lockKey[11])) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fashHash%uint32(self.lockDb.managerMaxGlocks)].Push(lock.manager.dbId, lock, protocol.COMMAND_LOCK, lock.command, nil, aofFlag, lock.manager.AofLockData(protocol.COMMAND_LOCK))
	if err != nil {
		self.lockDb.slock.Log().Errorf("Database lock push aof error DbId:%d LockKey:%x LockId:%x",
			lock.command.DbId, lock.command.LockKey, lock.command.LockId)
		return err
	}
	lock.isAof = true
	return nil
}

func (self *LockManager) PushUnLockAof(dbId uint8, lock *Lock, lockCommand *protocol.LockCommand, unLockCommand *protocol.LockCommand, isAof bool, aofFlag uint16) error {
	if unLockCommand == nil {
		if self.lockDb.status != STATE_LEADER {
			lock.isAof = isAof
			return nil
		}
	} else {
		if unLockCommand.Flag&protocol.UNLOCK_FLAG_FROM_AOF != 0 {
			lock.isAof = isAof
			return nil
		}
	}

	fashHash := (uint32(self.lockKey[0])<<24 | uint32(self.lockKey[1])<<16 | uint32(self.lockKey[2])<<8 | uint32(self.lockKey[3])) ^ (uint32(self.lockKey[4])<<24 | uint32(self.lockKey[5])<<16 | uint32(self.lockKey[6])<<8 | uint32(self.lockKey[7])) ^ (uint32(self.lockKey[8])<<24 | uint32(self.lockKey[9])<<16 | uint32(self.lockKey[10])<<8 | uint32(self.lockKey[11])) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fashHash%uint32(self.lockDb.managerMaxGlocks)].Push(dbId, lock, protocol.COMMAND_UNLOCK, lockCommand, unLockCommand, aofFlag, lock.manager.AofLockData(protocol.COMMAND_UNLOCK))
	if err != nil {
		self.lockDb.slock.Log().Errorf("Database lock push aof error DbId:%d LockKey:%x LockId:%x",
			lock.command.DbId, lock.command.LockKey, lock.command.LockId)
		return err
	}
	lock.isAof = isAof
	return nil
}

func (self *LockManager) FreeLock(lock *Lock) *Lock {
	if lock.manager == nil {
		return lock
	}

	self.refCount--
	lock.manager = nil
	lock.protocol = nil
	lock.command = nil
	lock.data = nil
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
			lock.expriedTime = lock.startTime + int64(lock.command.Expried)/1000 + 1
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
		lock.timeoutTime = now + int64(command.Timeout)/1000 + 1
	}
	lock.timeoutCheckedCount = 1
	lock.expriedCheckedCount = 1
	lock.longWaitIndex = 0
	self.refCount++
	return lock
}

func (self *LockManager) GetLockData() []byte {
	if self.currentData != nil {
		return self.currentData.GetData()
	}
	return nil
}

func (self *LockManager) AofLockData(commandType uint8) []byte {
	if self.currentData != nil && (commandType == protocol.COMMAND_LOCK || !self.currentData.isAof) {
		self.currentData.isAof = true
		return self.currentData.data
	}
	return nil
}

func (self *LockManager) ProcessLockData(command *protocol.LockCommand, lock *Lock, requireRecover bool) {
	if command.Data == nil {
		return
	}
	currentLockData := self.currentData
	lockCommandData := command.Data
	if lockCommandData.CommandStage != protocol.LOCK_DATA_STAGE_LOCK && lockCommandData.CommandType != protocol.LOCK_DATA_COMMAND_TYPE_EXECUTE {
		command.Data = nil
		return
	}

	switch lockCommandData.CommandType {
	case protocol.LOCK_DATA_COMMAND_TYPE_SET:
		if command.CommandType == protocol.COMMAND_LOCK && command.ExpriedFlag&0x4440 == 0 && command.Expried == 0 {
			if self.currentData != nil && self.currentData.commandType == protocol.LOCK_DATA_COMMAND_TYPE_SET {
				if self.currentData.Equal(command.Data.Data) {
					command.Data = nil
					return
				}
			}
		}
		self.currentData = NewLockManagerData(lockCommandData.Data, protocol.LOCK_DATA_COMMAND_TYPE_SET)
		if requireRecover {
			lock.SaveRecoverData(currentLockData, nil)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_UNSET:
		if command.CommandType == protocol.COMMAND_LOCK && command.ExpriedFlag&0x4440 == 0 && command.Expried == 0 {
			if self.currentData != nil && self.currentData.commandType == protocol.LOCK_DATA_COMMAND_TYPE_UNSET {
				command.Data = nil
				return
			}
		}
		self.currentData = NewLockManagerDataUnsetData()
		if requireRecover {
			lock.SaveRecoverData(currentLockData, nil)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_INCR:
		incrValue := command.GetLockData().GetIncrValue()
		recoverValue := incrValue
		if self.currentData != nil && self.currentData.GetData() != nil {
			incrValue += self.currentData.GetIncrValue()
		}
		if len(command.Data.Data) == 14 {
			data := command.Data.Data
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, 0
			data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13] = byte(incrValue), byte(incrValue>>8), byte(incrValue>>16), byte(incrValue>>24), byte(incrValue>>32), byte(incrValue>>40), byte(incrValue>>48), byte(incrValue>>56)
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_INCR)
		} else {
			self.currentData = NewLockManagerData([]byte{10, 0, 0, 0, protocol.LOCK_DATA_COMMAND_TYPE_SET, 0,
				byte(incrValue), byte(incrValue >> 8), byte(incrValue >> 16), byte(incrValue >> 24), byte(incrValue >> 32), byte(incrValue >> 40), byte(incrValue >> 48), byte(incrValue >> 56)},
				protocol.LOCK_DATA_COMMAND_TYPE_INCR)
		}
		if requireRecover {
			lock.SaveRecoverData(currentLockData, recoverValue)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_APPEND:
		if self.currentData == nil || self.currentData.GetData() == nil {
			lockCommandData.Data[4] = protocol.LOCK_DATA_COMMAND_TYPE_SET
			self.currentData = NewLockManagerData(lockCommandData.Data, protocol.LOCK_DATA_COMMAND_TYPE_APPEND)
		} else {
			dataLen := len(lockCommandData.Data) + len(self.currentData.data) - 10
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, 0
			copy(data[6:], self.currentData.data[6:])
			copy(data[len(self.currentData.data):], lockCommandData.Data[6:])
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_APPEND)
		}
		if requireRecover {
			lock.SaveRecoverData(currentLockData, uint64(len(self.currentData.data)-len(lockCommandData.Data)+6)<<32|uint64(len(lockCommandData.Data)-6))
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_SHIFT:
		lengthValue := int(lockCommandData.GetShiftLengthValue())
		if currentLockData.GetData() != nil && lengthValue > 0 {
			if lengthValue > len(currentLockData.data) {
				lengthValue = len(currentLockData.data)
			}
			dataLen := len(currentLockData.data) - lengthValue - 4
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, 0
			copy(data[6:], currentLockData.data[6+lengthValue:])
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_SHIFT)
			if requireRecover {
				shiftData := make([]byte, lengthValue)
				copy(shiftData, currentLockData.data[6:6+lengthValue])
				lock.SaveRecoverData(currentLockData, shiftData)
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_EXECUTE:
		if lockCommandData.CommandStage == protocol.LOCK_DATA_STAGE_LOCK {
			lockCommand := lock.protocol.GetLockCommand()
			err := lockCommandData.DecodeLockCommand(lockCommand)
			if err == nil && lockCommand.DbId == lockCommand.DbId {
				_ = self.lockDb.PushExecutorLockCommand(self, lock.protocol, lockCommand)
			}
		} else {
			lock.AddLockCommandData(lockCommandData)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_PIPELINE:
		index, buf := 0, lockCommandData.Data[6:]
		for index < len(buf) {
			dataLen := int(uint32(buf[index]) | uint32(buf[index+1])<<8 | uint32(buf[index+2])<<16 | uint32(buf[index+3])<<24)
			if index+4+dataLen > len(buf) {
				break
			}
			command.Data = protocol.NewLockCommandDataFromOriginBytes(buf[index : index+4+dataLen])
			self.ProcessLockData(command, lock, requireRecover)
			index += dataLen + 4
		}
	}
	command.Data = nil
}

func (self *LockManager) ProcessAckLockData(lock *Lock) []byte {
	lockData := lock.data
	if lockData == nil {
		return self.GetLockData()
	}
	recoverData := lockData.recoverData
	if lockData.ProcessAckClear() {
		lock.data = nil
	}
	if recoverData == nil {
		return nil
	}
	return recoverData.GetData()
}

func (self *LockManager) ProcessRecoverLockData(lock *Lock) {
	if lock.data == nil {
		return
	}
	currentData := lock.data.currentData
	if currentData == nil || (self.currentData.commandType != protocol.LOCK_DATA_COMMAND_TYPE_UNSET && currentData.commandType != self.currentData.commandType) {
		return
	}
	recoverData, recoverValue := lock.data.recoverData, lock.data.recoverValue

	switch currentData.commandType {
	case protocol.LOCK_DATA_COMMAND_TYPE_SET:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData()
		} else {
			self.currentData = recoverData
			self.currentData.isAof = false
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_UNSET:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData()
		} else {
			self.currentData = recoverData
			self.currentData.isAof = false
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_INCR:
		incrValue := recoverValue.(int64)
		if currentData.GetData() != nil {
			incrValue = currentData.GetIncrValue() - incrValue
		}
		self.currentData = NewLockManagerData([]byte{10, 0, 0, 0, protocol.LOCK_DATA_COMMAND_TYPE_SET, 0,
			byte(incrValue), byte(incrValue >> 8), byte(incrValue >> 16), byte(incrValue >> 24), byte(incrValue >> 32), byte(incrValue >> 40), byte(incrValue >> 48), byte(incrValue >> 56)},
			protocol.LOCK_DATA_COMMAND_TYPE_INCR)
	case protocol.LOCK_DATA_COMMAND_TYPE_APPEND:
		posValue := recoverValue.(uint64)
		indexValue, lenValue := int(uint32(posValue>>32)), int(uint32(posValue))
		if len(currentData.data) >= indexValue+lenValue {
			dataLen := len(currentData.data) - 4 - lenValue
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, 0
			copy(data[6:], currentData.data[6:indexValue])
			copy(data[indexValue:], currentData.data[indexValue+lenValue:])
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_APPEND)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_SHIFT:
		shiftData := recoverValue.([]byte)
		dataLen := len(shiftData) + len(currentData.data) - 4
		data := make([]byte, dataLen+4)
		data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
		data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, 0
		copy(data[6:], shiftData)
		copy(data[6+len(shiftData):], currentData.data[6:])
		self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_SHIFT)
	}
	if lock.data.ProcessAckClear() {
		lock.data = nil
	}
}

func (self *LockManager) ProcessExecuteLockCommand(lock *Lock, commandStage uint8) {
	if lock.data == nil {
		return
	}
	lockCommandDatas := lock.data.GetAndClearCommandDatas(commandStage)
	if lockCommandDatas == nil {
		return
	}
	for _, lockCommandData := range lockCommandDatas {
		lockCommand := lock.protocol.GetLockCommand()
		err := lockCommandData.DecodeLockCommand(lockCommand)
		if err == nil && lockCommand.DbId == lockCommand.DbId {
			_ = self.lockDb.PushExecutorLockCommand(self, lock.protocol, lockCommand)
		}
	}
	if lock.data.IsCleared() {
		lock.data = nil
	}
}

type Lock struct {
	manager             *LockManager
	command             *protocol.LockCommand
	protocol            *ProxyServerProtocol
	data                *LockData
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
	return &Lock{manager, command, protocol.GetProxy(), nil, 0, 0, 0,
		0, 1, 1, 0, 0, 0xff, false, false, 0, false}
}

func (self *Lock) GetDB() *LockDB {
	if self.manager == nil {
		return nil
	}
	return self.manager.GetDB()
}

func (self *Lock) SaveRecoverData(recoverData *LockManagerData, recoverValue interface{}) {
	if self.data == nil {
		self.data = &LockData{currentData: self.manager.currentData, recoverData: recoverData, recoverValue: recoverValue}
	} else {
		self.data.currentData = self.manager.currentData
		self.data.recoverData = recoverData
		self.data.recoverValue = recoverValue
	}
}

func (self *Lock) AddLockCommandData(lockCommandData *protocol.LockCommandData) {
	switch lockCommandData.CommandStage {
	case protocol.LOCK_DATA_STAGE_UNLOCK:
		if self.data == nil {
			self.data = &LockData{unlockCommandDatas: make([]*protocol.LockCommandData, 0)}
		} else {
			if self.data.unlockCommandDatas == nil {
				self.data.unlockCommandDatas = make([]*protocol.LockCommandData, 0)
			}
		}
		self.data.unlockCommandDatas = append(self.data.unlockCommandDatas, lockCommandData)
	case protocol.LOCK_DATA_STAGE_TIMEOUT:
		if self.data == nil {
			self.data = &LockData{timeoutCommandDatas: make([]*protocol.LockCommandData, 0)}
		} else {
			if self.data.timeoutCommandDatas == nil {
				self.data.timeoutCommandDatas = make([]*protocol.LockCommandData, 0)
			}
		}
		self.data.timeoutCommandDatas = append(self.data.timeoutCommandDatas, lockCommandData)
	case protocol.LOCK_DATA_STAGE_EXPRIED:
		if self.data == nil {
			self.data = &LockData{expriedCommandDatas: make([]*protocol.LockCommandData, 0)}
		} else {
			if self.data.expriedCommandDatas == nil {
				self.data.expriedCommandDatas = make([]*protocol.LockCommandData, 0)
			}
		}
		self.data.expriedCommandDatas = append(self.data.expriedCommandDatas, lockCommandData)
	}
}

type LockManagerData struct {
	data        []byte
	commandType uint8
	isAof       bool
}

func NewLockManagerData(data []byte, commandType uint8) *LockManagerData {
	return &LockManagerData{data, commandType, false}
}

func NewLockManagerDataUnsetData() *LockManagerData {
	return &LockManagerData{[]byte{2, 0, 0, 0, protocol.LOCK_DATA_COMMAND_TYPE_UNSET, 0}, protocol.LOCK_DATA_COMMAND_TYPE_UNSET, false}
}

func (self *LockManagerData) GetData() []byte {
	if self.data != nil && self.commandType != protocol.LOCK_DATA_COMMAND_TYPE_UNSET {
		return self.data
	}
	return nil
}

func (self *LockManagerData) GetIncrValue() int64 {
	if self.data == nil || self.commandType == protocol.LOCK_DATA_COMMAND_TYPE_UNSET {
		return 0
	}
	value := int64(0)
	for i := 0; i < 8; i++ {
		if i+6 >= len(self.data) {
			break
		}
		if i > 0 {
			value |= int64(self.data[i+6]) << (i * 8)
		} else {
			value |= int64(self.data[i+6])
		}
	}
	return value
}

func (self *LockManagerData) Equal(lockData []byte) bool {
	return len(self.data) == len(lockData) && string(self.data) == string(lockData)
}

type LockData struct {
	currentData         *LockManagerData
	recoverData         *LockManagerData
	recoverValue        interface{}
	unlockCommandDatas  []*protocol.LockCommandData
	timeoutCommandDatas []*protocol.LockCommandData
	expriedCommandDatas []*protocol.LockCommandData
}

func (self *LockData) ProcessAckClear() bool {
	self.currentData = nil
	self.recoverData = nil
	self.recoverValue = nil
	return self.unlockCommandDatas == nil && self.timeoutCommandDatas == nil && self.expriedCommandDatas == nil
}

func (self *LockData) GetAndClearCommandDatas(commandStage uint8) []*protocol.LockCommandData {
	switch commandStage {
	case protocol.LOCK_DATA_STAGE_UNLOCK:
		commandDatas := self.unlockCommandDatas
		self.unlockCommandDatas = nil
		return commandDatas
	case protocol.LOCK_DATA_STAGE_TIMEOUT:
		commandDatas := self.timeoutCommandDatas
		self.timeoutCommandDatas = nil
		return commandDatas
	case protocol.LOCK_DATA_STAGE_EXPRIED:
		commandDatas := self.expriedCommandDatas
		self.expriedCommandDatas = nil
		return commandDatas
	}
	return nil
}

func (self *LockData) IsCleared() bool {
	return self.currentData == nil && self.unlockCommandDatas == nil && self.timeoutCommandDatas == nil && self.expriedCommandDatas == nil
}

type PriorityMutex struct {
	mutex                    sync.Mutex
	highPriority             uint32
	lowPriority              uint32
	highPriorityAcquireCount uint32
	highPriorityMutex        sync.Mutex
	lowPriorityMutex         sync.Mutex
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
			self.highPriorityMutex.Lock()
			self.highPriorityMutex.Unlock()
		}
	}
	self.mutex.Lock()
	if self.highPriority == 1 {
		for {
			self.mutex.Unlock()
			if atomic.CompareAndSwapUint32(&self.highPriority, 1, 1) {
				self.highPriorityMutex.Lock()
				self.highPriorityMutex.Unlock()
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

func (self *PriorityMutex) HighSetPriority() bool {
	if atomic.CompareAndSwapUint32(&self.highPriority, 0, 1) {
		self.highPriorityMutex.Lock()
		self.setHighPriorityCount++
		if atomic.CompareAndSwapUint32(&self.highPriorityAcquireCount, 0, 0) {
			self.HighUnSetPriority()
		}
		return true
	}
	return false
}

func (self *PriorityMutex) HighUnSetPriority() bool {
	if atomic.CompareAndSwapUint32(&self.highPriority, 1, 0) {
		self.highPriorityMutex.Unlock()
		return true
	}
	return false
}

func (self *PriorityMutex) LowSetPriority() bool {
	if atomic.CompareAndSwapUint32(&self.lowPriority, 0, 1) {
		self.lowPriorityMutex.Lock()
		self.setLowPriorityCount++
		return true
	}
	return false
}

func (self *PriorityMutex) LowUnSetPriority() bool {
	if atomic.CompareAndSwapUint32(&self.lowPriority, 1, 0) {
		self.lowPriorityMutex.Unlock()
		return true
	}
	return false
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
			self.lowPriorityMutex.Lock()
			self.lowPriorityMutex.Unlock()
		}
	}
	self.Lock()
	if self.lowPriority == 1 {
		for {
			self.Unlock()
			if atomic.CompareAndSwapUint32(&self.lowPriority, 1, 1) {
				self.lowPriorityMutex.Lock()
				self.lowPriorityMutex.Unlock()
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

func (self *PriorityMutex) HighPriorityMutexWait() {
	if self.highPriority == 1 {
		self.highPriorityMutex.Lock()
		self.highPriorityMutex.Unlock()
	}
}
