package server

import (
	"sync"
	"sync/atomic"

	"github.com/snower/slock/protocol"
)

type ILockManagerRingQueue interface {
	Push(lock *Lock)
	Pop() *Lock
	Head() *Lock
	IterNodes() [][]*Lock
	MaxPriority() uint8
	Len() int
}

type LockManagerRingQueue struct {
	queue []*Lock
	index int
}

func NewLockManagerRingQueue(size int) *LockManagerRingQueue {
	return &LockManagerRingQueue{make([]*Lock, 0, size), 0}
}

func (self *LockManagerRingQueue) Push(lock *Lock) {
	if len(self.queue) == cap(self.queue) {
		if self.index > len(self.queue)/2 {
			copy(self.queue, self.queue[self.index:])
			self.queue = self.queue[:len(self.queue)-self.index]
			self.index = 0
		}
	}
	self.queue = append(self.queue, lock)
}

func (self *LockManagerRingQueue) Pop() *Lock {
	if self.index >= len(self.queue) {
		return nil
	}
	lock := self.queue[self.index]
	self.queue[self.index] = nil
	self.index++
	if self.index >= len(self.queue) {
		self.queue = self.queue[:0]
		self.index = 0
	}
	return lock
}

func (self *LockManagerRingQueue) Head() *Lock {
	if self.index >= len(self.queue) {
		return nil
	}
	return self.queue[self.index]
}

func (self *LockManagerRingQueue) IterNodes() [][]*Lock {
	if self.index < len(self.queue) {
		return [][]*Lock{self.queue[self.index:]}
	}
	return make([][]*Lock, 0)
}

func (self *LockManagerRingQueue) MaxPriority() uint8 {
	if self.index >= len(self.queue) {
		return 0
	}
	command := self.queue[self.index].command
	if command.TimeoutFlag&protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY != 0 {
		return command.Rcount
	}
	return 0
}

func (self *LockManagerRingQueue) Len() int {
	return len(self.queue) - self.index
}

type LockManagerPriorityRingQueueNode struct {
	ringQueue *LockManagerRingQueue
	priority  uint8
}

type LockManagerPriorityRingQueue struct {
	priorityNodes []*LockManagerPriorityRingQueueNode
	size          int
}

func NewLockManagerPriorityRingQueue(size int) *LockManagerPriorityRingQueue {
	return &LockManagerPriorityRingQueue{make([]*LockManagerPriorityRingQueueNode, 0, 2), size}
}

func (self *LockManagerPriorityRingQueue) Push(lock *Lock) {
	lockPriority := uint8(0)
	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY != 0 {
		lockPriority = lock.command.Rcount
	}
	for _, node := range self.priorityNodes {
		if node.priority == lockPriority {
			node.ringQueue.Push(lock)
			return
		}
	}

	node := &LockManagerPriorityRingQueueNode{NewLockManagerRingQueue(self.size), lockPriority}
	if len(self.priorityNodes) == 0 {
		self.priorityNodes = append(self.priorityNodes, node)
	} else if len(self.priorityNodes) == 1 {
		if self.priorityNodes[0].priority > node.priority {
			self.priorityNodes = append(self.priorityNodes, node)
		} else {
			priorityNode := self.priorityNodes[0]
			self.priorityNodes[0] = node
			self.priorityNodes = append(self.priorityNodes, priorityNode)
		}
	} else {
		priorityNodes := self.priorityNodes
		self.priorityNodes = make([]*LockManagerPriorityRingQueueNode, 0, len(priorityNodes)+1)
		for i, priorityNode := range priorityNodes {
			if node.priority > priorityNode.priority {
				self.priorityNodes = append(self.priorityNodes, node)
				self.priorityNodes = append(self.priorityNodes, priorityNodes[i:]...)
				break
			}
			self.priorityNodes = append(self.priorityNodes, priorityNode)
		}
	}
	node.ringQueue.Push(lock)
}

func (self *LockManagerPriorityRingQueue) Pop() *Lock {
	for _, node := range self.priorityNodes {
		lock := node.ringQueue.Pop()
		if lock != nil {
			return lock
		}
	}
	return nil
}

func (self *LockManagerPriorityRingQueue) Head() *Lock {
	for _, node := range self.priorityNodes {
		lock := node.ringQueue.Head()
		if lock != nil {
			return lock
		}
	}
	return nil
}

func (self *LockManagerPriorityRingQueue) IterNodes() [][]*Lock {
	iterNodes := make([][]*Lock, 0)
	for _, node := range self.priorityNodes {
		iterNodes = append(iterNodes, node.ringQueue.IterNodes()...)
	}
	return make([][]*Lock, 0)
}

func (self *LockManagerPriorityRingQueue) MaxPriority() uint8 {
	if len(self.priorityNodes) == 0 {
		return 0
	}
	for _, node := range self.priorityNodes {
		lock := node.ringQueue.Head()
		if lock != nil {
			return node.priority
		}
	}
	return 0
}

func (self *LockManagerPriorityRingQueue) Len() int {
	length := 0
	for _, node := range self.priorityNodes {
		length += node.ringQueue.Len()
	}
	return length
}

type LockManagerLockQueue struct {
	queue *LockQueue
	maps  map[[16]byte]*Lock
}

func NewLockManagerLockQueue() *LockManagerLockQueue {
	return &LockManagerLockQueue{NewLockQueue(4, 8, 4), make(map[[16]byte]*Lock)}
}

func (self *LockManagerLockQueue) Push(lock *Lock) {
	err := self.queue.Push(lock)
	if err == nil {
		self.maps[lock.command.LockId] = lock
	}
}

func (self *LockManagerLockQueue) Pop() *Lock {
	return self.queue.Pop()
}

func (self *LockManagerLockQueue) Head() *Lock {
	return self.queue.Head()
}

type LockManagerWaitQueue struct {
	fastQueue     []*Lock
	fastIndex     int
	ringQueue     ILockManagerRingQueue
	priorityQueue bool
}

func NewLockManagerWaitQueue(priorityQueue bool) *LockManagerWaitQueue {
	if priorityQueue {
		return &LockManagerWaitQueue{make([]*Lock, 0, 8), 0, NewLockManagerPriorityRingQueue(16), true}
	}
	return &LockManagerWaitQueue{make([]*Lock, 0, 8), 0, nil, false}
}

func (self *LockManagerWaitQueue) RePushPriorityRingQueue() {
	ringQueue := NewLockManagerPriorityRingQueue(16)
	if self.fastIndex < len(self.fastQueue) {
		for i := self.fastIndex; i < len(self.fastQueue); i++ {
			ringQueue.Push(self.fastQueue[i])
		}
		self.fastQueue = self.fastQueue[:0]
		self.fastIndex = 0
	}
	if self.ringQueue != nil {
		lock := self.ringQueue.Pop()
		for lock != nil {
			ringQueue.Push(lock)
			lock = self.ringQueue.Pop()
		}
	}
	self.ringQueue = ringQueue
	self.priorityQueue = true
}

func (self *LockManagerWaitQueue) Push(lock *Lock) {
	if self.ringQueue != nil {
		self.ringQueue.Push(lock)
		return
	}
	if len(self.fastQueue) == cap(self.fastQueue) {
		if self.fastIndex > len(self.fastQueue)/2 {
			copy(self.fastQueue, self.fastQueue[self.fastIndex:])
			self.fastQueue = self.fastQueue[:len(self.fastQueue)-self.fastIndex]
			self.fastIndex = 0
		} else {
			self.ringQueue = NewLockManagerRingQueue(64)
			self.ringQueue.Push(lock)
			return
		}
	}
	self.fastQueue = append(self.fastQueue, lock)
}

func (self *LockManagerWaitQueue) Pop() *Lock {
	if self.fastIndex < len(self.fastQueue) {
		lock := self.fastQueue[self.fastIndex]
		self.fastQueue[self.fastIndex] = nil
		self.fastIndex++
		if self.fastIndex >= len(self.fastQueue) {
			self.fastQueue = self.fastQueue[:0]
			self.fastIndex = 0
		}
		return lock
	}
	if self.ringQueue != nil {
		return self.ringQueue.Pop()
	}
	return nil
}

func (self *LockManagerWaitQueue) Head() *Lock {
	if self.fastIndex < len(self.fastQueue) {
		return self.fastQueue[self.fastIndex]
	}
	if self.ringQueue != nil {
		return self.ringQueue.Head()
	}
	return nil
}

func (self *LockManagerWaitQueue) Rellac() {
	self.fastQueue = self.fastQueue[:0]
	self.fastIndex = 0
	self.ringQueue = nil
}

func (self *LockManagerWaitQueue) IterNodes() [][]*Lock {
	lockNodes := make([][]*Lock, 0)
	if self.fastIndex < len(self.fastQueue) {
		lockNodes = append(lockNodes, self.fastQueue[self.fastIndex:])
	}
	if self.ringQueue != nil {
		lockNodes = append(lockNodes, self.ringQueue.IterNodes()...)
	}
	return lockNodes
}

func (self *LockManagerWaitQueue) MaxPriority() uint8 {
	if self.fastIndex < len(self.fastQueue) {
		command := self.fastQueue[self.fastIndex].command
		if command.TimeoutFlag&protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY != 0 {
			return command.Rcount
		}
		return 0
	}
	if self.ringQueue != nil {
		return self.ringQueue.MaxPriority()
	}
	return 0
}

func (self *LockManagerWaitQueue) Len() int {
	if self.ringQueue == nil {
		return len(self.fastQueue) - self.fastIndex
	}
	return len(self.fastQueue) - self.fastIndex + self.ringQueue.Len()
}

type LockManager struct {
	lockDb       *LockDB
	lockKey      [16]byte
	currentLock  *Lock
	currentData  *LockManagerData
	locks        *LockManagerLockQueue
	waitLocks    *LockManagerWaitQueue
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
	return &LockManager{lockDb, command.LockKey, nil, nil, nil, nil,
		glock, freeLocks, nil, state, 0, 0,
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

		if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_ZEOR_AOF_TIME != 0 && lock.expriedTime-lock.startTime > 5 {
			lock.expriedCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
		} else {
			lock.expriedCheckedCount = 1
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
		if self.locks == nil {
			self.locks = NewLockManagerLockQueue()
		}
		self.locks.Push(lock)
	}
	return lock
}

func (self *LockManager) RemoveLock(lock *Lock) *Lock {
	lock.locked = 0
	lock.ackCount = 0xff

	if self.currentLock == lock {
		self.currentLock = nil
		lock.refCount--
		if self.locks == nil {
			return lock
		}

		lockedLock := self.locks.Pop()
		for lockedLock != nil {
			if lockedLock.locked > 0 {
				delete(self.locks.maps, lockedLock.command.LockId)
				self.currentLock = lockedLock
				break
			}

			lockedLock.refCount--
			if lockedLock.refCount == 0 {
				self.FreeLock(lockedLock)
			}
			lockedLock = self.locks.Pop()
		}

		if self.locks.queue.headNodeIndex >= 8 {
			_ = self.locks.queue.Resize()
		}
		return lock
	}

	if self.locks == nil {
		return lock
	}
	delete(self.locks.maps, lock.command.LockId)
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

	if self.locks.queue.headNodeIndex >= 8 {
		_ = self.locks.queue.Resize()
	}
	return lock
}

func (self *LockManager) GetLockedLock(command *protocol.LockCommand) *Lock {
	if self.currentLock.command.LockId == command.LockId {
		return self.currentLock
	}
	if self.locks == nil {
		return nil
	}
	lockedLock, ok := self.locks.maps[command.LockId]
	if ok {
		return lockedLock
	}
	return nil
}

func (self *LockManager) CheckLockedEqual(lock *Lock, command *protocol.LockCommand) bool {
	if command.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
		if command.Expried == 0xffff {
			return self.checkLockedCountEqual(lock, command)
		}
		return lock.expriedTime == 0x7fffffffffffffff && self.checkLockedCountEqual(lock, command)
	}
	if command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
		if command.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
			expriedTime := self.lockDb.currentTime + int64(command.Expried)*60 + 1
			if expriedTime > lock.expriedTime {
				return expriedTime-lock.expriedTime <= 60 && self.checkLockedCountEqual(lock, command)
			}
			return lock.expriedTime-expriedTime <= 60 && self.checkLockedCountEqual(lock, command)
		}
		expriedTime := self.lockDb.currentTime + int64(command.Expried) + 1
		if expriedTime > lock.expriedTime {
			return expriedTime-lock.expriedTime <= 1 && self.checkLockedCountEqual(lock, command)
		}
		return lock.expriedTime-expriedTime <= 1 && self.checkLockedCountEqual(lock, command)
	}
	return self.checkLockedCountEqual(lock, command)
}

func (self *LockManager) checkLockedCountEqual(lock *Lock, command *protocol.LockCommand) bool {
	if command.Count != lock.command.Count {
		return false
	}
	if command.Rcount != lock.command.Rcount {
		return false
	}
	if command.TimeoutFlag&protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY != lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY {
		return false
	}
	return true
}

func (self *LockManager) UpdateLockedLock(lock *Lock, command *protocol.LockCommand) *protocol.LockCommand {
	currentCommand := lock.command
	lock.command = command

	if command.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME == 0 || command.Expried < 0xffff {
		lock.startTime = self.lockDb.currentTime
		if command.TimeoutFlag&protocol.TIMEOUT_FLAG_MILLISECOND_TIME == 0 {
			if command.TimeoutFlag&protocol.TIMEOUT_FLAG_MINUTE_TIME != 0 {
				lock.timeoutTime = lock.startTime + int64(command.Timeout)*60 + 1
			} else {
				lock.timeoutTime = lock.startTime + int64(command.Timeout) + 1
			}
		} else {
			lock.timeoutTime = lock.startTime + int64(command.Timeout)/1000 + 1
		}

		if command.ExpriedFlag&protocol.EXPRIED_FLAG_UNLIMITED_EXPRIED_TIME != 0 {
			lock.expriedTime = 0x7fffffffffffffff
		} else if command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
			if command.ExpriedFlag&protocol.EXPRIED_FLAG_MINUTE_TIME != 0 {
				lock.expriedTime = lock.startTime + int64(command.Expried)*60 + 1
			} else {
				lock.expriedTime = lock.startTime + int64(command.Expried) + 1
			}
		} else {
			lock.expriedTime = lock.startTime + int64(command.Expried)/1000 + 1
		}

		if command.TimeoutFlag&protocol.TIMEOUT_FLAG_UPDATE_NO_RESET_TIMEOUT_CHECKED_COUNT == 0 {
			lock.timeoutCheckedCount = 1
		}
		if command.ExpriedFlag&protocol.EXPRIED_FLAG_UPDATE_NO_RESET_EXPRIED_CHECKED_COUNT == 0 {
			if command.ExpriedFlag&protocol.EXPRIED_FLAG_ZEOR_AOF_TIME != 0 && lock.expriedTime-lock.startTime > 5 {
				lock.expriedCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
			} else {
				lock.expriedCheckedCount = 1
			}
		}
	}

	if !lock.isAof && self.currentLock == lock && (self.locks == nil || self.locks.Head() == nil) {
		switch command.ExpriedFlag & 0x1300 {
		case protocol.EXPRIED_FLAG_ZEOR_AOF_TIME:
			lock.aofTime = 0
		case protocol.EXPRIED_FLAG_UNLIMITED_AOF_TIME:
			lock.aofTime = 0xff
		case protocol.EXPRIED_FLAG_AOF_TIME_OF_EXPRIED_PARCENT:
			lock.aofTime = uint8(float64(command.Expried) * Config.DBLockAofParcentTime)
		default:
			lock.aofTime = self.lockDb.aofTime
		}
	}
	return currentCommand
}

func (self *LockManager) AddWaitLock(lock *Lock) *Lock {
	if self.waitLocks == nil {
		self.waitLocks = NewLockManagerWaitQueue(false)
	} else {
		if self.waited && !self.waitLocks.priorityQueue {
			lockPriority := uint8(0)
			if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY != 0 {
				lockPriority = lock.command.Rcount
			}
			if self.waitLocks.Head() != nil && lockPriority != self.waitLocks.MaxPriority() {
				self.waitLocks.RePushPriorityRingQueue()
			}
		}
	}
	self.waitLocks.Push(lock)
	lock.refCount++
	self.waited = true
	return lock
}

func (self *LockManager) GetWaitLock() *Lock {
	if self.waitLocks == nil {
		return nil
	}
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
		return lock
	}
	return nil
}

func (self *LockManager) PushLockAof(lock *Lock, aofFlag uint16) error {
	if self.lockDb.status != STATE_LEADER {
		return nil
	}
	if lock.command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0 {
		lock.isAof = true
		return nil
	}

	fastHash := (uint32(self.lockKey[0]) | uint32(self.lockKey[1])<<8 | uint32(self.lockKey[2])<<16 | uint32(self.lockKey[3])<<24) ^ (uint32(self.lockKey[4]) | uint32(self.lockKey[5])<<8 | uint32(self.lockKey[6])<<16 | uint32(self.lockKey[7])<<24) ^ (uint32(self.lockKey[8]) | uint32(self.lockKey[9])<<8 | uint32(self.lockKey[10])<<16 | uint32(self.lockKey[11])<<24) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fastHash%uint32(self.lockDb.managerMaxGlocks)].Push(lock.manager.dbId, lock, protocol.COMMAND_LOCK, lock.command, nil, aofFlag, lock.manager.AofLockData(protocol.COMMAND_LOCK, lock))
	if err != nil {
		self.lockDb.slock.Log().Errorf("Database lock push aof error DbId:%d LockKey:%x LockId:%x",
			lock.command.DbId, lock.command.LockKey, lock.command.LockId)
		return err
	}
	lock.isAof = true
	return nil
}

func (self *LockManager) PushUnLockAof(dbId uint8, lock *Lock, lockCommand *protocol.LockCommand, unLockCommand *protocol.LockCommand, isAof bool, aofFlag uint16) error {
	if self.lockDb.status != STATE_LEADER {
		return nil
	}
	if unLockCommand != nil && unLockCommand.Flag&protocol.UNLOCK_FLAG_FROM_AOF != 0 {
		lock.isAof = isAof
		return nil
	}

	fastHash := (uint32(self.lockKey[0]) | uint32(self.lockKey[1])<<8 | uint32(self.lockKey[2])<<16 | uint32(self.lockKey[3])<<24) ^ (uint32(self.lockKey[4]) | uint32(self.lockKey[5])<<8 | uint32(self.lockKey[6])<<16 | uint32(self.lockKey[7])<<24) ^ (uint32(self.lockKey[8]) | uint32(self.lockKey[9])<<8 | uint32(self.lockKey[10])<<16 | uint32(self.lockKey[11])<<24) ^ (uint32(self.lockKey[12])<<24 | uint32(self.lockKey[13])<<16 | uint32(self.lockKey[14])<<8 | uint32(self.lockKey[15]))
	err := self.lockDb.aofChannels[fastHash%uint32(self.lockDb.managerMaxGlocks)].Push(dbId, lock, protocol.COMMAND_UNLOCK, lockCommand, unLockCommand, aofFlag, lock.manager.AofLockData(protocol.COMMAND_UNLOCK, lock))
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

	atomic.AddUint32(&self.refCount, 0xffffffff)
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

		if command.ExpriedFlag&protocol.EXPRIED_FLAG_ZEOR_AOF_TIME != 0 && lock.expriedTime-lock.startTime > 5 {
			lock.expriedCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
		} else {
			lock.expriedCheckedCount = 1
		}
	} else {
		lock.expriedTime = 0
		lock.expriedCheckedCount = 1
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
	lock.longWaitIndex = 0
	atomic.AddUint32(&self.refCount, 1)
	return lock
}

func (self *LockManager) GetLockData() []byte {
	if self.currentData != nil {
		return self.currentData.GetData()
	}
	return nil
}

func (self *LockManager) AofLockData(commandType uint8, lock *Lock) []byte {
	if lock.data != nil && lock.data.aofData != nil {
		aofData := lock.data.aofData
		lock.data.aofData = nil
		if lock.data.IsEmpty() {
			lock.data = nil
		}
		return aofData
	}
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
	if lockCommandData.CommandStage == protocol.LOCK_DATA_STAGE_CURRENT {
		if lockCommandData.DataFlag&protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST != 0 {
			if command.CommandType == protocol.COMMAND_UNLOCK {
				if self.locked != 0 || self.waited {
					command.Data = nil
					return
				}
			} else {
				if self.locked != 1 {
					command.Data = nil
					return
				}
			}
		}
	} else if lockCommandData.CommandType != protocol.LOCK_DATA_COMMAND_TYPE_EXECUTE {
		command.Data = nil
		return
	}

	switch lockCommandData.CommandType {
	case protocol.LOCK_DATA_COMMAND_TYPE_SET:
		if command.CommandType == protocol.COMMAND_LOCK && (command.Flag&protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED != 0 || (command.ExpriedFlag&0x4440 == 0 && command.Expried == 0)) {
			if self.currentData != nil && self.currentData.commandType == protocol.LOCK_DATA_COMMAND_TYPE_SET {
				if self.currentData.Equal(command.Data.Data) {
					command.Data = nil
					return
				}
			}
		}
		self.currentData = NewLockManagerData(lockCommandData.Data, protocol.LOCK_DATA_COMMAND_TYPE_SET, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
		if requireRecover {
			lock.SaveRecoverData(currentLockData, nil)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_UNSET:
		if self.currentData == nil {
			command.Data = nil
			return
		}
		if command.CommandType == protocol.COMMAND_LOCK && (command.Flag&protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED != 0 || (command.ExpriedFlag&0x4440 == 0 && command.Expried == 0)) {
			if self.currentData.commandType == protocol.LOCK_DATA_COMMAND_TYPE_UNSET {
				command.Data = nil
				return
			}
		}
		self.currentData = NewLockManagerDataUnsetData(command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
		if requireRecover {
			lock.SaveRecoverData(currentLockData, nil)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_INCR:
		incrValue := command.GetLockData().GetIncrValue()
		recoverValue := incrValue
		if self.currentData != nil && self.currentData.GetData() != nil {
			incrValue += self.currentData.GetIncrValue()
		}
		if command.Data.GetValueSize() == 8 {
			data, valueOffset := command.Data.Data, command.Data.GetValueOffset()
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, data[5]|protocol.LOCK_DATA_FLAG_VALUE_TYPE_NUMBER
			data[valueOffset], data[valueOffset+1], data[valueOffset+2], data[valueOffset+3], data[valueOffset+4], data[valueOffset+5], data[valueOffset+6], data[valueOffset+7] = byte(incrValue), byte(incrValue>>8), byte(incrValue>>16), byte(incrValue>>24), byte(incrValue>>32), byte(incrValue>>40), byte(incrValue>>48), byte(incrValue>>56)
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_INCR, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
		} else {
			valueOffset := currentLockData.GetValueOffset()
			if valueOffset <= 6 {
				self.currentData = NewLockManagerData([]byte{10, 0, 0, 0, protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
					byte(incrValue), byte(incrValue >> 8), byte(incrValue >> 16), byte(incrValue >> 24), byte(incrValue >> 32), byte(incrValue >> 40), byte(incrValue >> 48), byte(incrValue >> 56)},
					protocol.LOCK_DATA_COMMAND_TYPE_INCR, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
			} else {
				dataLen := currentLockData.GetValueOffset() + 4
				data := make([]byte, dataLen+4)
				data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, currentLockData.data[5]|protocol.LOCK_DATA_FLAG_VALUE_TYPE_NUMBER
				copy(data[6:], currentLockData.data[6:])
				data[valueOffset], data[valueOffset+1], data[valueOffset+2], data[valueOffset+3], data[valueOffset+4], data[valueOffset+5], data[valueOffset+6], data[valueOffset+7] = byte(incrValue), byte(incrValue>>8), byte(incrValue>>16), byte(incrValue>>24), byte(incrValue>>32), byte(incrValue>>40), byte(incrValue>>48), byte(incrValue>>56)
				self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_INCR, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
			}
		}
		if requireRecover {
			lock.SaveRecoverData(currentLockData, recoverValue)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_APPEND:
		if self.currentData == nil || self.currentData.GetData() == nil {
			lockCommandData.Data[4] = protocol.LOCK_DATA_COMMAND_TYPE_SET
			self.currentData = NewLockManagerData(lockCommandData.Data, protocol.LOCK_DATA_COMMAND_TYPE_APPEND, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
		} else {
			dataLen := len(self.currentData.data) - 4 + lockCommandData.GetValueSize()
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, self.currentData.data[5]
			copy(data[6:], self.currentData.data[6:])
			copy(data[len(self.currentData.data):], lockCommandData.GetBytesValue())
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_APPEND, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
		}
		if requireRecover {
			lock.SaveRecoverData(currentLockData, uint64(len(self.currentData.data)-len(lockCommandData.Data)+lockCommandData.GetValueOffset())<<32|uint64(len(lockCommandData.Data)-lockCommandData.GetValueOffset()))
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_SHIFT:
		lengthValue := int(lockCommandData.GetShiftLengthValue())
		if self.currentData != nil && self.currentData.GetData() != nil && lengthValue > 0 {
			if lengthValue > len(currentLockData.data) {
				lengthValue = len(currentLockData.data)
			}
			dataLen, valueOffset := len(currentLockData.data)-lengthValue-4, currentLockData.GetValueOffset()
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, currentLockData.data[5]
			if valueOffset > 6 {
				copy(data[6:], currentLockData.data[6:valueOffset])
			}
			copy(data[valueOffset:], currentLockData.data[valueOffset+lengthValue:])
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_SHIFT, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
			if requireRecover {
				shiftData := make([]byte, lengthValue)
				copy(shiftData, currentLockData.data[valueOffset:valueOffset+lengthValue])
				lock.SaveRecoverData(currentLockData, shiftData)
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_EXECUTE:
		if lockCommandData.CommandStage == protocol.LOCK_DATA_STAGE_CURRENT && !requireRecover {
			lockCommand := lock.protocol.GetLockCommand()
			err := lockCommandData.DecodeLockCommand(lockCommand)
			if err == nil && self.dbId == lockCommand.DbId {
				_ = self.lockDb.PushExecutorLockCommand(lock.protocol, lockCommand)
			}
		} else {
			lock.AddLockCommandData(lockCommandData)
		}
		if lock.data == nil {
			lock.data = &LockData{aofData: lockCommandData.Data}
		} else {
			lock.data.aofData = lockCommandData.Data
		}
		if requireRecover && self.currentData != nil {
			lock.SaveRecoverData(currentLockData, nil)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_PIPELINE:
		index, buf := 0, lockCommandData.Data[lockCommandData.GetValueOffset():]
		for index < len(buf) {
			dataLen := int(uint32(buf[index]) | uint32(buf[index+1])<<8 | uint32(buf[index+2])<<16 | uint32(buf[index+3])<<24)
			if index+4+dataLen > len(buf) {
				break
			}
			command.Data = protocol.NewLockCommandDataFromOriginBytes(buf[index : index+4+dataLen])
			if command.Data.CommandType != protocol.LOCK_DATA_COMMAND_TYPE_EXECUTE && command.CommandType != protocol.LOCK_DATA_COMMAND_TYPE_PIPELINE {
				self.currentData = currentLockData
			}
			self.ProcessLockData(command, lock, requireRecover)
			index += dataLen + 4
		}
		if lock.data == nil {
			lock.data = &LockData{aofData: lockCommandData.Data}
		} else {
			lock.data.aofData = lockCommandData.Data
		}
		if self.currentData != nil {
			if !self.currentData.isAof && (currentLockData == nil || currentLockData.isAof) {
				self.currentData.isAof = true
			}
			if requireRecover {
				lock.SaveRecoverData(currentLockData, nil)
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_PUSH:
		if self.currentData == nil || self.currentData.GetData() == nil || !self.currentData.IsArrayValue() {
			dataLen := len(lockCommandData.Data)
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, (lockCommandData.Data[5]&0xf8)|protocol.LOCK_DATA_FLAG_VALUE_TYPE_ARRAY
			if lockCommandData.GetValueOffset() > 6 {
				copy(data[6:], lockCommandData.Data[6:lockCommandData.GetValueOffset()])
			}
			index, dataSize := lockCommandData.GetValueOffset(), lockCommandData.GetValueSize()
			data[index], data[index+1], data[index+2], data[index+3] = byte(dataSize), byte(dataSize>>8), byte(dataSize>>16), byte(dataSize>>24)
			copy(data[index+4:], lockCommandData.GetBytesValue())
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_PUSH, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
		} else {
			dataLen := len(self.currentData.data) + lockCommandData.GetValueSize()
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, (self.currentData.data[5]&0xf8)|protocol.LOCK_DATA_FLAG_VALUE_TYPE_ARRAY
			copy(data[6:], self.currentData.data[6:])
			index, dataSize := len(self.currentData.data), lockCommandData.GetValueSize()
			data[index], data[index+1], data[index+2], data[index+3] = byte(dataSize), byte(dataSize>>8), byte(dataSize>>16), byte(dataSize>>24)
			copy(data[index+4:], lockCommandData.GetBytesValue())
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_PUSH, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
		}
		if requireRecover {
			lock.SaveRecoverData(currentLockData, lockCommandData.GetBytesValue())
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_POP:
		popCount := int(lockCommandData.GetPopCountValue())
		if self.currentData != nil && self.currentData.GetData() != nil && popCount > 0 && self.currentData.IsArrayValue() {
			values := make([][]byte, 0)
			for i := self.currentData.GetValueOffset(); i+4 < len(self.currentData.data); {
				valueLen := int(uint32(self.currentData.data[i]) | uint32(self.currentData.data[i+1])<<8 | uint32(self.currentData.data[i+2])<<16 | uint32(self.currentData.data[i+3])<<24)
				if valueLen == 0 {
					i += 4
					continue
				}
				values = append(values, self.currentData.data[i+4:i+4+valueLen])
				i += valueLen + 4
			}
			if popCount > len(values) {
				popCount = len(values)
			}
			dataLen := self.currentData.GetValueOffset() - 4
			for _, value := range values[popCount:] {
				dataLen += len(value) + 4
			}
			i, data := self.currentData.GetValueOffset(), make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			copy(data[4:], self.currentData.data[4:i])
			for _, value := range values[popCount:] {
				data[i], data[i+1], data[i+2], data[i+3] = byte(len(value)), byte(len(value)>>8), byte(len(value)>>16), byte(len(value)>>24)
				i += copy(data[i+4:], value) + 4
			}
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_POP, command.Flag&protocol.LOCK_FLAG_FROM_AOF != 0)
			if requireRecover {
				lock.SaveRecoverData(currentLockData, values[:popCount])
			}
		}
	}
	command.Data = nil
}

func (self *LockManager) ProcessAckLockData(lock *Lock) []byte {
	lockData := lock.data
	if lockData == nil {
		return self.GetLockData()
	}
	if lockData.commandDatas != nil {
		commandDatas := make([]*protocol.LockCommandData, 0)
		for _, lockCommandData := range lockData.commandDatas {
			if lockCommandData.CommandStage != protocol.LOCK_DATA_STAGE_CURRENT {
				commandDatas = append(commandDatas, lockCommandData)
				continue
			}
			if lockCommandData.DataFlag&protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST != 0 && self.locked != 1 {
				commandDatas = append(commandDatas, lockCommandData)
				continue
			}
			lockCommand := lock.protocol.GetLockCommand()
			err := lockCommandData.DecodeLockCommand(lockCommand)
			if err == nil && self.dbId == lockCommand.DbId {
				_ = self.lockDb.PushExecutorLockCommand(lock.protocol, lockCommand)
			}
		}
		if len(commandDatas) > 0 {
			lockData.commandDatas = commandDatas
		} else {
			lockData.commandDatas = nil
		}
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
		lock.data.commandDatas = nil
		if lock.data.ProcessAckClear() {
			lock.data = nil
		}
		return
	}
	recoverData, recoverValue := lock.data.recoverData, lock.data.recoverValue

	switch currentData.commandType {
	case protocol.LOCK_DATA_COMMAND_TYPE_SET:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData(false)
		} else {
			self.currentData = recoverData
			self.currentData.isAof = false
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_UNSET:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData(false)
		} else {
			self.currentData = recoverData
			self.currentData.isAof = false
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_INCR:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData(false)
		} else {
			incrValue := recoverValue.(int64)
			if currentData.GetData() != nil {
				incrValue = currentData.GetIncrValue() - incrValue
				valueOffset := currentData.GetValueOffset()
				if valueOffset <= 6 {
					self.currentData = NewLockManagerData([]byte{10, 0, 0, 0, protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
						byte(incrValue), byte(incrValue >> 8), byte(incrValue >> 16), byte(incrValue >> 24), byte(incrValue >> 32), byte(incrValue >> 40), byte(incrValue >> 48), byte(incrValue >> 56)},
						protocol.LOCK_DATA_COMMAND_TYPE_INCR, false)
				} else {
					dataLen := currentData.GetValueOffset() + 4
					data := make([]byte, dataLen+4)
					data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, currentData.data[5]|protocol.LOCK_DATA_FLAG_VALUE_TYPE_NUMBER
					copy(data[6:], currentData.data[6:])
					data[valueOffset], data[valueOffset+1], data[valueOffset+2], data[valueOffset+3], data[valueOffset+4], data[valueOffset+5], data[valueOffset+6], data[valueOffset+7] = byte(incrValue), byte(incrValue>>8), byte(incrValue>>16), byte(incrValue>>24), byte(incrValue>>32), byte(incrValue>>40), byte(incrValue>>48), byte(incrValue>>56)
					self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_INCR, false)
				}
			} else {
				self.currentData = NewLockManagerData([]byte{10, 0, 0, 0, protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
					byte(incrValue), byte(incrValue >> 8), byte(incrValue >> 16), byte(incrValue >> 24), byte(incrValue >> 32), byte(incrValue >> 40), byte(incrValue >> 48), byte(incrValue >> 56)},
					protocol.LOCK_DATA_COMMAND_TYPE_INCR, false)
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_APPEND:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData(false)
		} else {
			posValue := recoverValue.(uint64)
			indexValue, lenValue := int(uint32(posValue>>32)), int(uint32(posValue))
			if len(currentData.data) >= indexValue+lenValue {
				dataLen, valueOffset := len(currentData.data)-4-lenValue, currentData.GetValueOffset()
				data := make([]byte, dataLen+4)
				data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
				data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, currentData.data[5]
				if valueOffset > 6 {
					copy(data[6:], currentData.data[6:valueOffset])
				}
				copy(data[valueOffset:], currentData.data[valueOffset:indexValue])
				copy(data[indexValue:], currentData.data[indexValue+lenValue:])
				self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_APPEND, false)
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_SHIFT:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData(false)
		} else {
			shiftData := recoverValue.([]byte)
			dataLen, valueOffset := len(shiftData)+len(currentData.data)-4, currentData.GetValueOffset()
			data := make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, currentData.data[5]
			copy(data[valueOffset:], shiftData)
			copy(data[valueOffset+len(shiftData):], currentData.data[valueOffset:])
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_SHIFT, false)
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_EXECUTE:
		if recoverData == nil {
			if self.currentData != nil {
				self.currentData = NewLockManagerDataUnsetData(false)
			}
		} else {
			if self.currentData == nil || !self.currentData.Equal(recoverData.data) {
				self.currentData = recoverData
				self.currentData.isAof = false
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_PIPELINE:
		if recoverData == nil {
			if self.currentData != nil {
				self.currentData = NewLockManagerDataUnsetData(false)
			}
		} else {
			if self.currentData == nil || !self.currentData.Equal(recoverData.data) {
				self.currentData = recoverData
				self.currentData.isAof = false
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_PUSH:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData(false)
		} else if recoverData.IsArrayValue() && self.currentData != nil && self.currentData.GetData() != nil && self.currentData.IsArrayValue() {
			values, recoverValueBytes, recoverIndex := make([][]byte, 0), recoverValue.([]byte), 0
			for i := self.currentData.GetValueOffset(); i+4 < len(self.currentData.data); {
				valueLen := int(uint32(self.currentData.data[i]) | uint32(self.currentData.data[i+1])<<8 | uint32(self.currentData.data[i+2])<<16 | uint32(self.currentData.data[i+3])<<24)
				if valueLen == 0 {
					i += 4
					continue
				}
				value := self.currentData.data[i+4 : i+4+valueLen]
				values = append(values, value)
				i += valueLen + 4
				if string(value) == string(recoverValueBytes) {
					recoverIndex = len(values) - 1
				}
			}
			if len(values) > 0 {
				if recoverIndex == 0 {
					values = values[1:]
				} else {
					values = append(values[:recoverIndex], values[recoverIndex+1:]...)
				}
				dataLen := self.currentData.GetValueOffset() - 4
				for _, value := range values {
					dataLen += len(value) + 4
				}
				i, data := self.currentData.GetValueOffset(), make([]byte, dataLen+4)
				data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
				copy(data[4:], self.currentData.data[4:i])
				for _, value := range values {
					data[i], data[i+1], data[i+2], data[i+3] = byte(len(value)), byte(len(value)>>8), byte(len(value)>>16), byte(len(value)>>24)
					i += copy(data[i+4:], value) + 4
				}
				self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_POP, false)
			}
		}
	case protocol.LOCK_DATA_COMMAND_TYPE_POP:
		if recoverData == nil {
			self.currentData = NewLockManagerDataUnsetData(false)
		} else if recoverData.IsArrayValue() && self.currentData != nil && self.currentData.GetData() != nil && self.currentData.IsArrayValue() {
			values := make([][]byte, 0)
			if recoverValue != nil {
				values = append(values, recoverValue.([][]byte)...)
			}
			for i := self.currentData.GetValueOffset(); i+4 < len(self.currentData.data); {
				valueLen := int(uint32(self.currentData.data[i]) | uint32(self.currentData.data[i+1])<<8 | uint32(self.currentData.data[i+2])<<16 | uint32(self.currentData.data[i+3])<<24)
				if valueLen == 0 {
					i += 4
					continue
				}
				values = append(values, self.currentData.data[i+4:i+4+valueLen])
				i += valueLen + 4
			}
			dataLen := self.currentData.GetValueOffset() - 4
			for _, value := range values {
				dataLen += len(value) + 4
			}
			i, data := self.currentData.GetValueOffset(), make([]byte, dataLen+4)
			data[0], data[1], data[2], data[3] = byte(dataLen), byte(dataLen>>8), byte(dataLen>>16), byte(dataLen>>24)
			copy(data[4:], self.currentData.data[4:i])
			for _, value := range values {
				data[i], data[i+1], data[i+2], data[i+3] = byte(len(value)), byte(len(value)>>8), byte(len(value)>>16), byte(len(value)>>24)
				i += copy(data[i+4:], value) + 4
			}
			self.currentData = NewLockManagerData(data, protocol.LOCK_DATA_COMMAND_TYPE_PUSH, false)
		}
	}

	lock.data.commandDatas = nil
	if lock.data.ProcessAckClear() {
		lock.data = nil
	}
}

func (self *LockManager) ProcessExecuteLockCommand(lock *Lock, commandStage uint8) {
	if lock.data == nil || lock.data.commandDatas == nil {
		return
	}
	for _, lockCommandData := range lock.data.commandDatas {
		if lockCommandData.CommandStage != commandStage {
			continue
		}
		if lockCommandData.DataFlag&protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST != 0 && (self.locked != 0 || self.waited) {
			continue
		}
		lockCommand := lock.protocol.GetLockCommand()
		err := lockCommandData.DecodeLockCommand(lockCommand)
		if err == nil && self.dbId == lockCommand.DbId {
			_ = self.lockDb.PushExecutorLockCommand(lock.protocol, lockCommand)
		}
	}
	lock.data.commandDatas = nil
	if lock.data.IsEmpty() {
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
		0, 1, 1, 0, 0, 0xff, true, true, 0, false}
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
	if self.data == nil {
		self.data = &LockData{commandDatas: make([]*protocol.LockCommandData, 0)}
	} else {
		if self.data.commandDatas == nil {
			self.data.commandDatas = make([]*protocol.LockCommandData, 0)
		}
	}
	self.data.commandDatas = append(self.data.commandDatas, lockCommandData)
}

func (self *Lock) ClearLockCommandDatas() {
	if self.data != nil && self.data.commandDatas != nil {
		self.data.commandDatas = nil
	}
}

type LockManagerData struct {
	data        []byte
	commandType uint8
	isAof       bool
}

func NewLockManagerData(data []byte, commandType uint8, isAof bool) *LockManagerData {
	return &LockManagerData{data, commandType, isAof}
}

func NewLockManagerDataUnsetData(isAof bool) *LockManagerData {
	return &LockManagerData{[]byte{2, 0, 0, 0, protocol.LOCK_DATA_COMMAND_TYPE_UNSET, 0}, protocol.LOCK_DATA_COMMAND_TYPE_UNSET, isAof}
}

func (self *LockManagerData) GetValueOffset() int {
	if self.data == nil || len(self.data) < 8 {
		return 6
	}
	if self.data[5]&protocol.LOCK_DATA_FLAG_CONTAINS_PROPERTY != 0 {
		return (int(self.data[6]) | (int(self.data[7]) << 8)) + 8
	}
	return 6
}

func (self *LockManagerData) GetValueSize() int {
	return len(self.data) - self.GetValueOffset()
}

func (self *LockManagerData) IsArrayValue() bool {
	if self.data == nil || len(self.data) < 6 {
		return false
	}
	return self.data[5]&protocol.LOCK_DATA_FLAG_VALUE_TYPE_ARRAY != 0
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
	valueOffset, value := self.GetValueOffset(), int64(0)
	for i := 0; i < 8; i++ {
		if i+valueOffset >= len(self.data) {
			break
		}
		if i > 0 {
			value |= int64(self.data[i+valueOffset]) << (i * 8)
		} else {
			value |= int64(self.data[i+valueOffset])
		}
	}
	return value
}

func (self *LockManagerData) Equal(lockData []byte) bool {
	if self.data == nil {
		if lockData == nil {
			return true
		}
		return false
	} else {
		if lockData == nil {
			return false
		}
		return len(self.data) == len(lockData) && string(self.data) == string(lockData)
	}
}

type LockData struct {
	aofData      []byte
	currentData  *LockManagerData
	recoverData  *LockManagerData
	recoverValue interface{}
	commandDatas []*protocol.LockCommandData
}

func (self *LockData) ProcessAckClear() bool {
	self.currentData = nil
	self.recoverData = nil
	self.recoverValue = nil
	return self.IsEmpty()
}

func (self *LockData) IsEmpty() bool {
	return self.aofData == nil && self.currentData == nil && self.commandDatas == nil
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
	if atomic.LoadUint32(&self.highPriority) != 0 {
		self.highPriorityMutex.Lock()
		self.highPriorityMutex.Unlock()
	}
	self.mutex.Lock()
	if atomic.LoadUint32(&self.highPriority) != 0 {
		for {
			self.mutex.Unlock()
			if atomic.LoadUint32(&self.highPriority) != 0 {
				self.highPriorityMutex.Lock()
				self.highPriorityMutex.Unlock()
			}
			self.mutex.Lock()
			if atomic.LoadUint32(&self.highPriority) == 0 {
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
	if atomic.AddUint32(&self.lowPriority, 1) == 1 {
		self.lowPriorityMutex.Lock()
	}
	atomic.AddUint64(&self.setLowPriorityCount, 1)
	return true
}

func (self *PriorityMutex) LowSetPriorityWithNotTraceCount() bool {
	if atomic.AddUint32(&self.lowPriority, 1) == 1 {
		self.lowPriorityMutex.Lock()
	}
	return true
}

func (self *PriorityMutex) LowUnSetPriority() bool {
	if atomic.AddUint32(&self.lowPriority, 0xffffffff) == 0 {
		self.lowPriorityMutex.Unlock()
	}
	return true
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
	if atomic.LoadUint32(&self.lowPriority) != 0 {
		self.lowPriorityMutex.Lock()
		self.lowPriorityMutex.Unlock()
	}
	self.Lock()
	if atomic.LoadUint32(&self.lowPriority) != 0 {
		for {
			self.Unlock()
			if atomic.LoadUint32(&self.lowPriority) != 0 {
				self.lowPriorityMutex.Lock()
				self.lowPriorityMutex.Unlock()
			}
			self.Lock()
			if atomic.LoadUint32(&self.lowPriority) == 0 {
				return
			}
		}
	}
}

func (self *PriorityMutex) LowPriorityUnlock() {
	self.Unlock()
}

func (self *PriorityMutex) HighPriorityMutexWait() {
	if atomic.LoadUint32(&self.highPriority) != 0 {
		self.highPriorityMutex.Lock()
		self.highPriorityMutex.Unlock()
	}
}
