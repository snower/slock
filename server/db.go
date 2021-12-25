package server

import (
	"github.com/snower/slock/protocol"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type FastKeyValue struct {
	manager *LockManager
	count   uint32
	lock    uint32
}

type LongWaitLockQueue struct {
	locks      *LockQueue
	lockTime   int64
	freeCount  int32
	glockIndex uint16
}

type LongWaitLockFreeQueue struct {
	queues       []*LongWaitLockQueue
	freeIndex    int
	maxFreeCount int
}

type MillisecondWaitLockFreeQueue struct {
	queues       []*LockQueue
	freeIndex    int
	maxFreeCount int
}

type LockDB struct {
	slock                     *SLock
	fastLocks                 []FastKeyValue
	locks                     map[[16]byte]*LockManager
	timeoutLocks              [][]*LockQueue
	expriedLocks              [][]*LockQueue
	longTimeoutLocks          []map[int64]*LongWaitLockQueue
	longExpriedLocks          []map[int64]*LongWaitLockQueue
	millisecondTimeoutLocks   [][]*LockQueue
	millisecondExpriedLocks   [][]*LockQueue
	currentTime               int64
	checkTimeoutTime          int64
	checkExpriedTime          int64
	glock                     *sync.Mutex
	mGlock                    *sync.Mutex
	managerGlocks             []*PriorityMutex
	freeLockManagers          []*LockManager
	freeLocks                 []*LockQueue
	freeLongWaitQueues        []*LongWaitLockFreeQueue
	freeMillisecondWaitQueues []*MillisecondWaitLockFreeQueue
	aofChannels               []*AofChannel
	subscribeChannels         []*SubscribeChannel
	fastKeyCount              uint32
	freeLockManagerHead       uint32
	freeLockManagerTail       uint32
	maxFreeLockManagerCount   uint32
	managerGlockIndex         uint16
	managerMaxGlocks          uint16
	aofTime                   uint8
	status                    uint8
	dbId                      uint8
	states                    []*protocol.LockDBState
}

func NewLockDB(slock *SLock, dbId uint8) *LockDB {
	managerMaxGlocks := uint16(Config.DBConcurrent)
	if managerMaxGlocks == 0 {
		managerMaxGlocks = uint16(runtime.NumCPU()) * 2
	}
	maxFreeLockManagerCount := uint32(managerMaxGlocks) * MANAGER_MAX_GLOCKS_INIT_SIZE
	for uint64(0x100000000)%uint64(maxFreeLockManagerCount) != 0 {
		maxFreeLockManagerCount++
	}
	managerGlocks := make([]*PriorityMutex, managerMaxGlocks)
	freeLocks := make([]*LockQueue, managerMaxGlocks)
	freeLongWaitQueues := make([]*LongWaitLockFreeQueue, managerMaxGlocks)
	freeMillisecondWaitQueues := make([]*MillisecondWaitLockFreeQueue, managerMaxGlocks)
	aofChannels := make([]*AofChannel, managerMaxGlocks)
	subscribeChannels := make([]*SubscribeChannel, managerMaxGlocks)
	states := make([]*protocol.LockDBState, managerMaxGlocks+1)
	for i := uint16(0); i < managerMaxGlocks; i++ {
		managerGlocks[i] = NewPriorityMutex()
		freeLocks[i] = NewLockQueue(2, 16, FREE_LOCK_QUEUE_INIT_SIZE)
		freeLongWaitQueues[i] = &LongWaitLockFreeQueue{make([]*LongWaitLockQueue, FREE_LONG_WAIT_QUEUE_INIT_SIZE), -1, FREE_LONG_WAIT_QUEUE_INIT_SIZE - 1}
		freeMillisecondWaitQueues[i] = &MillisecondWaitLockFreeQueue{make([]*LockQueue, FREE_MILLISECOND_WAIT_QUEUE_INIT_SIZE), -1, FREE_MILLISECOND_WAIT_QUEUE_INIT_SIZE - 1}
		states[i] = &protocol.LockDBState{}
	}
	states[managerMaxGlocks] = &protocol.LockDBState{}
	aofTime := uint8(Config.DBLockAofTime)

	now := time.Now().Unix()
	db := &LockDB{
		slock:                     slock,
		fastLocks:                 make([]FastKeyValue, Config.DBFastKeyCount),
		locks:                     make(map[[16]byte]*LockManager, Config.DBFastKeyCount/uint(managerMaxGlocks)),
		timeoutLocks:              make([][]*LockQueue, TIMEOUT_QUEUE_LENGTH),
		expriedLocks:              make([][]*LockQueue, EXPRIED_QUEUE_LENGTH),
		longTimeoutLocks:          make([]map[int64]*LongWaitLockQueue, managerMaxGlocks),
		longExpriedLocks:          make([]map[int64]*LongWaitLockQueue, managerMaxGlocks),
		millisecondTimeoutLocks:   make([][]*LockQueue, managerMaxGlocks),
		millisecondExpriedLocks:   make([][]*LockQueue, managerMaxGlocks),
		currentTime:               now,
		checkTimeoutTime:          now,
		checkExpriedTime:          now,
		glock:                     &sync.Mutex{},
		mGlock:                    &sync.Mutex{},
		managerGlocks:             managerGlocks,
		freeLockManagers:          make([]*LockManager, maxFreeLockManagerCount),
		freeLocks:                 freeLocks,
		freeLongWaitQueues:        freeLongWaitQueues,
		freeMillisecondWaitQueues: freeMillisecondWaitQueues,
		aofChannels:               aofChannels,
		subscribeChannels:         subscribeChannels,
		fastKeyCount:              uint32(Config.DBFastKeyCount),
		freeLockManagerHead:       0,
		freeLockManagerTail:       0,
		maxFreeLockManagerCount:   maxFreeLockManagerCount,
		managerGlockIndex:         0,
		managerMaxGlocks:          managerMaxGlocks,
		aofTime:                   aofTime,
		status:                    slock.state,
		dbId:                      dbId,
		states:                    states,
	}

	db.resizeAofChannels()
	db.resizeSubScribeChannels()
	db.resizeTimeOut()
	db.resizeExpried()
	db.startCheckLoop()
	return db
}

func (self *LockDB) resizeAofChannels() {
	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		self.aofChannels[i] = self.slock.GetAof().NewAofChannel(self, i, self.managerGlocks[i])
	}
}

func (self *LockDB) resizeSubScribeChannels() {
	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		self.subscribeChannels[i] = self.slock.GetSubscribeManager().NewSubscribeChannel(self, i, self.managerGlocks[i])
	}
}

func (self *LockDB) resizeTimeOut() {
	for i := int64(0); i < TIMEOUT_QUEUE_LENGTH; i++ {
		self.timeoutLocks[i] = make([]*LockQueue, self.managerMaxGlocks)
		for j := uint16(0); j < self.managerMaxGlocks; j++ {
			self.timeoutLocks[i][j] = NewLockQueue(4, 16, TIMEOUT_LOCKS_QUEUE_INIT_SIZE)
		}
	}

	for j := uint16(0); j < self.managerMaxGlocks; j++ {
		self.longTimeoutLocks[j] = make(map[int64]*LongWaitLockQueue, LONG_TIMEOUT_LOCKS_INIT_COUNT)
		self.millisecondTimeoutLocks[j] = make([]*LockQueue, 1000)
	}
}

func (self *LockDB) resizeExpried() {
	for i := int64(0); i < EXPRIED_QUEUE_LENGTH; i++ {
		self.expriedLocks[i] = make([]*LockQueue, self.managerMaxGlocks)
		for j := uint16(0); j < self.managerMaxGlocks; j++ {
			self.expriedLocks[i][j] = NewLockQueue(4, 16, EXPRIED_LOCKS_QUEUE_INIT_SIZE)
		}
	}

	for j := uint16(0); j < self.managerMaxGlocks; j++ {
		self.longExpriedLocks[j] = make(map[int64]*LongWaitLockQueue, LONG_EXPRIED_LOCKS_INIT_COUNT)
		self.millisecondExpriedLocks[j] = make([]*LockQueue, 1000)
	}
}

func (self *LockDB) Close() {
	self.glock.Lock()
	if self.status != STATE_CLOSE {
		self.glock.Unlock()
		return
	}

	self.status = STATE_CLOSE
	self.glock.Unlock()

	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		self.managerGlocks[i].Lock()
		self.flushTimeOut(i, true)
		self.flushExpried(i, false)
		self.slock.GetAof().CloseAofChannel(self.aofChannels[i])
		self.slock.GetSubscribeManager().CloseSubscribeChannel(self.subscribeChannels[i])
		self.managerGlocks[i].Unlock()
	}
}

func (self *LockDB) FlushDB() error {
	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		self.managerGlocks[i].Lock()
	}

	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		self.flushTimeOut(i, true)
		self.flushExpried(i, true)
		self.managerGlocks[i].Unlock()
	}
	return nil
}

func (self *LockDB) startCheckLoop() {
	timeoutWaiter, expriedWaiter := make(chan bool, 16), make(chan bool, 16)
	go self.updateCurrentTime(timeoutWaiter, expriedWaiter)
	go self.checkTimeOut(timeoutWaiter)
	go self.checkExpried(expriedWaiter)
	go self.restructuringLongTimeOutQueue()
	go self.restructuringLongExpriedQueue()
}

func (self *LockDB) updateCurrentTime(timeoutWaiter chan bool, expriedWaiter chan bool) {
	priorityCheckTime := 50 * time.Millisecond
	for self.status != STATE_CLOSE {
		self.currentTime = time.Now().Unix()
		timeoutWaiter <- true
		expriedWaiter <- true
		time.Sleep(priorityCheckTime - time.Duration(time.Now().Nanosecond()))
		for i := uint16(0); i < self.managerMaxGlocks; i++ {
			if self.managerGlocks[i].highPriorityAcquireCount > 0 {
				self.managerGlocks[i].HighSetPriority()
			}
		}
		time.Sleep(time.Second - time.Duration(time.Now().Nanosecond()))
	}
	timeoutWaiter <- false
	expriedWaiter <- false
}

func (self *LockDB) checkTimeOut(waiter chan bool) {
	doTimeoutLockQueues := make([][]*LockQueue, self.managerMaxGlocks)
	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		doTimeoutLockQueues[i] = make([]*LockQueue, 5)
		for j := 0; j < 5; j++ {
			doTimeoutLockQueues[i][j] = NewLockQueue(4, 16, 1024)
		}
	}

	<-waiter
	for self.status != STATE_CLOSE {
		checkTimeoutTime := self.checkTimeoutTime
		now := self.currentTime
		self.checkTimeoutTime = now + 1

		for checkTimeoutTime <= now {
			for i := uint16(0); i < self.managerMaxGlocks; i++ {
				go self.checkTimeTimeOut(checkTimeoutTime, now, i, doTimeoutLockQueues[i])
			}
			checkTimeoutTime++
		}

		<-waiter
	}
}

func (self *LockDB) checkTimeTimeOut(checkTimeoutTime int64, now int64, glockIndex uint16, doTimeoutLockQueues []*LockQueue) {
	timeoutLocks := self.timeoutLocks[checkTimeoutTime&TIMEOUT_QUEUE_LENGTH_MASK]
	doTimeoutLocks := doTimeoutLockQueues[checkTimeoutTime%5]
	if doTimeoutLocks == nil {
		doTimeoutLocks = NewLockQueue(4, 16, 1024)
	} else {
		doTimeoutLockQueues[checkTimeoutTime%5] = nil
	}

	self.managerGlocks[glockIndex].HighPriorityLock()
	lock := timeoutLocks[glockIndex].Pop()
	for lock != nil {
		if !lock.timeouted {
			if lock.timeoutTime > now {
				lock.timeoutCheckedCount++
				self.AddTimeOut(lock, lock.timeoutTime)
				lock = timeoutLocks[glockIndex].Pop()
				continue
			}

			_ = doTimeoutLocks.Push(lock)
			lock = timeoutLocks[glockIndex].Pop()
			continue
		}

		lockManager := lock.manager
		lock.refCount--
		if lock.refCount == 0 {
			lockManager.FreeLock(lock)
			if lockManager.refCount == 0 {
				self.RemoveLockManager(lockManager)
			}
		}

		lock = timeoutLocks[glockIndex].Pop()
	}
	_ = timeoutLocks[glockIndex].Rellac()

	if longLocks, ok := self.longTimeoutLocks[glockIndex][checkTimeoutTime]; ok {
		longLockCount := longLocks.locks.Len()
		for longLockCount > 0 {
			lock := longLocks.locks.Pop()
			if lock != nil {
				lock.longWaitIndex = 0
				if !lock.timeouted {
					_ = doTimeoutLocks.Push(lock)
				} else {
					lockManager := lock.manager
					lock.refCount--
					if lock.refCount == 0 {
						lockManager.FreeLock(lock)
						if lockManager.refCount == 0 {
							self.RemoveLockManager(lockManager)
						}
					}
				}
			}
			longLockCount--
		}

		delete(self.longTimeoutLocks[glockIndex], checkTimeoutTime)
		freeLongWaitQueue := self.freeLongWaitQueues[glockIndex]
		if freeLongWaitQueue.freeIndex < freeLongWaitQueue.maxFreeCount {
			_ = longLocks.locks.Reset()
			longLocks.freeCount = 0
			freeLongWaitQueue.freeIndex++
			freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex] = longLocks
		}
	}
	self.managerGlocks[glockIndex].HighPriorityUnlock()

	lock = doTimeoutLocks.Pop()
	for lock != nil {
		self.doTimeOut(lock, false)
		lock = doTimeoutLocks.Pop()
	}
	_ = doTimeoutLocks.Rellac()
	doTimeoutLockQueues[checkTimeoutTime%5] = doTimeoutLocks
}

func (self *LockDB) checkMillisecondTimeOut(ms int64, glockIndex uint16) {
	sleepMs := ms - time.Now().UnixNano()/1e6
	if sleepMs > 0 {
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}

	self.managerGlocks[glockIndex].HighPriorityLock()
	lockQueue := self.millisecondTimeoutLocks[glockIndex][ms%1000]
	if lockQueue == nil {
		self.managerGlocks[glockIndex].HighPriorityUnlock()
		return
	}

	self.millisecondTimeoutLocks[glockIndex][ms%1000] = nil
	for i := range lockQueue.IterNodes() {
		nodeQueues := lockQueue.IterNodeQueues(int32(i))
		for j, lock := range nodeQueues {
			if !lock.timeouted {
				timeoutSeconds := int64(lock.command.Timeout / 1000)
				lock.timeoutTime = self.currentTime + timeoutSeconds + 1
				if timeoutSeconds > 0 {
					self.AddTimeOut(lock, lock.timeoutTime)
					nodeQueues[j] = nil
					continue
				}
				continue
			}

			lockManager := lock.manager
			lock.refCount--
			if lock.refCount == 0 {
				lockManager.FreeLock(lock)
				if lockManager.refCount == 0 {
					self.RemoveLockManager(lockManager)
				}
			}
			nodeQueues[j] = nil
		}
	}
	self.managerGlocks[glockIndex].HighPriorityUnlock()

	for i := range lockQueue.IterNodes() {
		nodeQueues := lockQueue.IterNodeQueues(int32(i))
		for j, lock := range nodeQueues {
			if lock != nil {
				self.doTimeOut(lock, false)
				nodeQueues[j] = nil
			}
		}
	}

	self.managerGlocks[glockIndex].Lock()
	freeMillisecondWaitQueue := self.freeMillisecondWaitQueues[glockIndex]
	if freeMillisecondWaitQueue.freeIndex < freeMillisecondWaitQueue.maxFreeCount {
		_ = lockQueue.Reset()
		freeMillisecondWaitQueue.freeIndex++
		freeMillisecondWaitQueue.queues[freeMillisecondWaitQueue.freeIndex] = lockQueue
	}
	self.managerGlocks[glockIndex].Unlock()
}

func (self *LockDB) restructuringLongTimeOutQueue() {
	time.Sleep(120 * time.Second)

	for self.status != STATE_CLOSE {
		for i := uint16(0); i < self.managerMaxGlocks; i++ {
			self.managerGlocks[i].Lock()
			for lockTime, longLocks := range self.longTimeoutLocks[i] {
				if lockTime < self.checkTimeoutTime+int64(TIMEOUT_QUEUE_MAX_WAIT) {
					continue
				}

				if longLocks.freeCount*3 < longLocks.locks.Len() {
					continue
				}

				tailNodeIndex, tailQueueIndex := longLocks.locks.tailNodeIndex, longLocks.locks.tailQueueIndex
				longLocks.locks.headNodeIndex = 0
				longLocks.locks.headQueueIndex = 0
				longLocks.locks.headQueue = longLocks.locks.queues[0]
				longLocks.locks.tailQueue = longLocks.locks.queues[0]
				longLocks.locks.tailNodeIndex = 0
				longLocks.locks.tailQueueIndex = 0
				longLocks.locks.headQueueSize = longLocks.locks.nodeQueueSizes[0]
				longLocks.locks.tailQueueSize = longLocks.locks.nodeQueueSizes[0]

				for j := int32(0); j < tailNodeIndex; j++ {
					for k := int32(0); k < longLocks.locks.nodeQueueSizes[j]; k++ {
						lock := longLocks.locks.queues[j][k]
						if lock == nil {
							continue
						}
						longLocks.locks.queues[j][k] = nil

						lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
						if longLocks.locks.Push(lock) != nil {
							lock.longWaitIndex = 0
						}
					}
				}

				for k := int32(0); k < tailQueueIndex; k++ {
					lock := longLocks.locks.queues[tailNodeIndex][k]
					if lock == nil {
						continue
					}
					longLocks.locks.queues[tailNodeIndex][k] = nil

					lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
					if longLocks.locks.Push(lock) != nil {
						lock.longWaitIndex = 0
					}
				}

				for tailNodeIndex > longLocks.locks.tailNodeIndex+1 {
					longLocks.locks.queues[tailNodeIndex] = nil
					longLocks.locks.nodeQueueSizes[tailNodeIndex] = 0
					tailNodeIndex--
				}
				longLocks.locks.queueSize = longLocks.locks.baseQueueSize * int32(uint32(1)<<uint32(tailNodeIndex))
				if longLocks.locks.queueSize > QUEUE_MAX_MALLOC_SIZE {
					longLocks.locks.queueSize = QUEUE_MAX_MALLOC_SIZE
				}

				longLocks.freeCount = 0
				if longLocks.locks.Len() == 0 {
					delete(self.longTimeoutLocks[i], lockTime)
					freeLongWaitQueue := self.freeLongWaitQueues[i]
					if freeLongWaitQueue.freeIndex < freeLongWaitQueue.maxFreeCount {
						_ = longLocks.locks.Reset()
						longLocks.freeCount = 0
						freeLongWaitQueue.freeIndex++
						freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex] = longLocks
					}
				}
			}
			self.managerGlocks[i].Unlock()
		}

		time.Sleep(120 * time.Second)
	}
}

func (self *LockDB) flushTimeOut(glockIndex uint16, doTimeout bool) {
	doTimeoutLocks := make([]*Lock, 0)

	for i := int64(0); i < TIMEOUT_QUEUE_LENGTH; i++ {
		lock := self.timeoutLocks[i][glockIndex].Pop()
		for lock != nil {
			lock, doTimeoutLocks = self.flushTimeoutCheckLock(self.timeoutLocks[i][glockIndex], lock, doTimeoutLocks)
		}
		_ = self.timeoutLocks[i][glockIndex].Reset()
	}

	for checkTimeoutTime, longLocks := range self.longTimeoutLocks[glockIndex] {
		longLockCount := longLocks.locks.Len()
		lock := longLocks.locks.Pop()
		for longLockCount > 0 {
			if lock != nil {
				lock.longWaitIndex = 0
				lock, doTimeoutLocks = self.flushTimeoutCheckLock(longLocks.locks, lock, doTimeoutLocks)
			} else {
				lock = longLocks.locks.Pop()
			}
			longLockCount--
		}

		delete(self.longTimeoutLocks[glockIndex], checkTimeoutTime)
		freeLongWaitQueue := self.freeLongWaitQueues[glockIndex]
		if freeLongWaitQueue.freeIndex < freeLongWaitQueue.maxFreeCount {
			_ = longLocks.locks.Reset()
			longLocks.freeCount = 0
			freeLongWaitQueue.freeIndex++
			freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex] = longLocks
		}
	}

	for i, lockQueue := range self.millisecondTimeoutLocks[glockIndex] {
		if lockQueue != nil {
			lock := lockQueue.Pop()
			for lock != nil {
				lock, doTimeoutLocks = self.flushTimeoutCheckLock(lockQueue, lock, doTimeoutLocks)
			}

			freeMillisecondWaitQueue := self.freeMillisecondWaitQueues[glockIndex]
			if freeMillisecondWaitQueue.freeIndex < freeMillisecondWaitQueue.maxFreeCount {
				_ = lockQueue.Reset()
				freeMillisecondWaitQueue.freeIndex++
				freeMillisecondWaitQueue.queues[freeMillisecondWaitQueue.freeIndex] = lockQueue
			}
			self.millisecondTimeoutLocks[glockIndex][i] = nil
		}
	}

	if doTimeout {
		self.managerGlocks[glockIndex].Unlock()
		for _, lock := range doTimeoutLocks {
			self.doTimeOut(lock, true)
		}
		self.managerGlocks[glockIndex].Lock()
	}
}

func (self *LockDB) flushTimeoutCheckLock(lockQueue *LockQueue, lock *Lock, doTimeoutLocks []*Lock) (*Lock, []*Lock) {
	if !lock.timeouted {
		doTimeoutLocks = append(doTimeoutLocks, lock)
		return lockQueue.Pop(), doTimeoutLocks
	}

	lockManager := lock.manager
	lock.refCount--
	if lock.refCount == 0 {
		lockManager.FreeLock(lock)
		if lockManager.refCount == 0 {
			self.RemoveLockManager(lockManager)
		}
	}
	return lockQueue.Pop(), doTimeoutLocks
}

func (self *LockDB) checkExpried(waiter chan bool) {
	doExpriedLockQueues := make([][]*LockQueue, self.managerMaxGlocks)
	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		doExpriedLockQueues[i] = make([]*LockQueue, 5)
		for j := 0; j < 5; j++ {
			doExpriedLockQueues[i][j] = NewLockQueue(4, 16, 1024)
		}
	}

	<-waiter
	for self.status != STATE_CLOSE {
		checkExpriedTime := self.checkExpriedTime
		now := self.currentTime
		self.checkExpriedTime = now + 1

		for checkExpriedTime <= now {
			for i := uint16(0); i < self.managerMaxGlocks; i++ {
				go self.checkTimeExpried(checkExpriedTime, now, i, doExpriedLockQueues[i])
			}
			checkExpriedTime++
		}

		<-waiter
	}
}

func (self *LockDB) checkTimeExpried(checkExpriedTime int64, now int64, glockIndex uint16, doExpriedLockQueues []*LockQueue) {
	expriedLocks := self.expriedLocks[checkExpriedTime&EXPRIED_QUEUE_LENGTH_MASK]
	doExpriedLocks := doExpriedLockQueues[checkExpriedTime%5]
	if doExpriedLocks == nil {
		doExpriedLocks = NewLockQueue(4, 16, 1024)
	} else {
		doExpriedLockQueues[checkExpriedTime%5] = nil
	}

	self.managerGlocks[glockIndex].HighPriorityLock()
	lock := expriedLocks[glockIndex].Pop()
	for lock != nil {
		if !lock.expried {
			if lock.expriedTime > now {
				lock.expriedCheckedCount++
				self.AddExpried(lock, lock.expriedTime)

				lock = expriedLocks[glockIndex].Pop()
				continue
			}

			_ = doExpriedLocks.Push(lock)
			lock = expriedLocks[glockIndex].Pop()
			continue
		}

		lockManager := lock.manager
		lock.refCount--
		if lock.refCount == 0 {
			lockManager.FreeLock(lock)
			if lockManager.refCount == 0 {
				self.RemoveLockManager(lockManager)
			}
		}
		lock = expriedLocks[glockIndex].Pop()
	}
	_ = expriedLocks[glockIndex].Rellac()

	if longLocks, ok := self.longExpriedLocks[glockIndex][checkExpriedTime]; ok {
		longLockCount := longLocks.locks.Len()
		for longLockCount > 0 {
			lock := longLocks.locks.Pop()
			if lock != nil {
				lock.longWaitIndex = 0
				if !lock.expried {
					_ = doExpriedLocks.Push(lock)
				} else {
					lockManager := lock.manager
					lock.refCount--
					if lock.refCount == 0 {
						lockManager.FreeLock(lock)
						if lockManager.refCount == 0 {
							self.RemoveLockManager(lockManager)
						}
					}
				}
			}
			longLockCount--
		}

		delete(self.longExpriedLocks[glockIndex], checkExpriedTime)
		freeLongWaitQueue := self.freeLongWaitQueues[glockIndex]
		if freeLongWaitQueue.freeIndex < freeLongWaitQueue.maxFreeCount {
			_ = longLocks.locks.Reset()
			longLocks.freeCount = 0
			freeLongWaitQueue.freeIndex++
			freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex] = longLocks
		}
	}
	self.managerGlocks[glockIndex].HighPriorityUnlock()

	lock = doExpriedLocks.Pop()
	for lock != nil {
		self.doExpried(lock, false)
		lock = doExpriedLocks.Pop()
	}
	_ = doExpriedLocks.Rellac()
	doExpriedLockQueues[checkExpriedTime%5] = doExpriedLocks
}

func (self *LockDB) checkMillisecondExpried(ms int64, glockIndex uint16) {
	sleepMs := ms - time.Now().UnixNano()/1e6
	if sleepMs > 0 {
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}

	self.managerGlocks[glockIndex].HighPriorityLock()
	lockQueue := self.millisecondExpriedLocks[glockIndex][ms%1000]
	if lockQueue == nil {
		self.managerGlocks[glockIndex].HighPriorityUnlock()
		return
	}

	self.millisecondExpriedLocks[glockIndex][ms%1000] = nil
	for i := range lockQueue.IterNodes() {
		nodeQueues := lockQueue.IterNodeQueues(int32(i))
		for j, lock := range nodeQueues {
			if !lock.expried {
				expriedSeconds := int64(lock.command.Expried / 1000)
				lock.expriedTime = self.currentTime + expriedSeconds + 1
				if expriedSeconds > 0 {
					self.AddExpried(lock, lock.expriedTime)
					nodeQueues[j] = nil
					continue
				}
				continue
			}

			lockManager := lock.manager
			lock.refCount--
			if lock.refCount == 0 {
				lockManager.FreeLock(lock)
				if lockManager.refCount == 0 {
					self.RemoveLockManager(lockManager)
				}
			}
			nodeQueues[j] = nil
		}
	}
	self.managerGlocks[glockIndex].HighPriorityUnlock()

	for i := range lockQueue.IterNodes() {
		nodeQueues := lockQueue.IterNodeQueues(int32(i))
		for j, lock := range nodeQueues {
			if lock != nil {
				self.doExpried(lock, false)
				nodeQueues[j] = nil
			}
		}
	}

	self.managerGlocks[glockIndex].Lock()
	freeMillisecondWaitQueue := self.freeMillisecondWaitQueues[glockIndex]
	if freeMillisecondWaitQueue.freeIndex < freeMillisecondWaitQueue.maxFreeCount {
		_ = lockQueue.Reset()
		freeMillisecondWaitQueue.freeIndex++
		freeMillisecondWaitQueue.queues[freeMillisecondWaitQueue.freeIndex] = lockQueue
	}
	self.managerGlocks[glockIndex].Unlock()
}

func (self *LockDB) restructuringLongExpriedQueue() {
	time.Sleep(120 * time.Second)

	for self.status != STATE_CLOSE {
		for i := uint16(0); i < self.managerMaxGlocks; i++ {
			self.managerGlocks[i].Lock()
			for lockTime, longLocks := range self.longExpriedLocks[i] {
				if lockTime < self.checkExpriedTime+int64(EXPRIED_QUEUE_MAX_WAIT) {
					continue
				}

				if longLocks.freeCount*3 < longLocks.locks.Len() {
					continue
				}

				tailNodeIndex, tailQueueIndex := longLocks.locks.tailNodeIndex, longLocks.locks.tailQueueIndex
				longLocks.locks.headNodeIndex = 0
				longLocks.locks.headQueueIndex = 0
				longLocks.locks.headQueue = longLocks.locks.queues[0]
				longLocks.locks.tailQueue = longLocks.locks.queues[0]
				longLocks.locks.tailNodeIndex = 0
				longLocks.locks.tailQueueIndex = 0
				longLocks.locks.headQueueSize = longLocks.locks.nodeQueueSizes[0]
				longLocks.locks.tailQueueSize = longLocks.locks.nodeQueueSizes[0]

				for j := int32(0); j < tailNodeIndex; j++ {
					for k := int32(0); k < longLocks.locks.nodeQueueSizes[j]; k++ {
						lock := longLocks.locks.queues[j][k]
						if lock == nil {
							continue
						}
						longLocks.locks.queues[j][k] = nil

						lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
						if longLocks.locks.Push(lock) != nil {
							lock.longWaitIndex = 0
						}
					}
				}

				for k := int32(0); k < tailQueueIndex; k++ {
					lock := longLocks.locks.queues[tailNodeIndex][k]
					if lock == nil {
						continue
					}
					longLocks.locks.queues[tailNodeIndex][k] = nil

					lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
					if longLocks.locks.Push(lock) != nil {
						lock.longWaitIndex = 0
					}
				}

				for tailNodeIndex > longLocks.locks.tailNodeIndex+1 {
					longLocks.locks.queues[tailNodeIndex] = nil
					longLocks.locks.nodeQueueSizes[tailNodeIndex] = 0
					tailNodeIndex--
				}
				longLocks.locks.queueSize = longLocks.locks.baseQueueSize * int32(uint32(1)<<uint32(tailNodeIndex))
				if longLocks.locks.queueSize > QUEUE_MAX_MALLOC_SIZE {
					longLocks.locks.queueSize = QUEUE_MAX_MALLOC_SIZE
				}

				longLocks.freeCount = 0
				if longLocks.locks.Len() == 0 {
					delete(self.longExpriedLocks[i], lockTime)
					freeLongWaitQueue := self.freeLongWaitQueues[i]
					if freeLongWaitQueue.freeIndex < freeLongWaitQueue.maxFreeCount {
						_ = longLocks.locks.Reset()
						longLocks.freeCount = 0
						freeLongWaitQueue.freeIndex++
						freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex] = longLocks
					}
				}
			}
			self.managerGlocks[i].Unlock()
		}

		time.Sleep(120 * time.Second)
	}
}

func (self *LockDB) flushExpried(glockIndex uint16, doExpried bool) {
	doExpriedLocks := make([]*Lock, 0)

	for i := int64(0); i < EXPRIED_QUEUE_LENGTH; i++ {
		lock := self.expriedLocks[i][glockIndex].Pop()
		for lock != nil {
			lock, doExpriedLocks = self.flushExpriedCheckLock(self.expriedLocks[i][glockIndex], lock, doExpriedLocks)
		}
		_ = self.expriedLocks[i][glockIndex].Reset()
	}

	for checkExpriedTime, longLocks := range self.longExpriedLocks[glockIndex] {
		longLockCount := longLocks.locks.Len()
		lock := longLocks.locks.Pop()
		for longLockCount > 0 {
			if lock != nil {
				lock.longWaitIndex = 0
				lock, doExpriedLocks = self.flushExpriedCheckLock(longLocks.locks, lock, doExpriedLocks)
			} else {
				lock = longLocks.locks.Pop()
			}
			longLockCount--
		}

		delete(self.longExpriedLocks[glockIndex], checkExpriedTime)
		freeLongWaitQueue := self.freeLongWaitQueues[glockIndex]
		if freeLongWaitQueue.freeIndex < freeLongWaitQueue.maxFreeCount {
			_ = longLocks.locks.Reset()
			longLocks.freeCount = 0
			freeLongWaitQueue.freeIndex++
			freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex] = longLocks
		}
	}

	for i, lockQueue := range self.millisecondExpriedLocks[glockIndex] {
		if lockQueue != nil {
			lock := lockQueue.Pop()
			for lock != nil {
				lock, doExpriedLocks = self.flushExpriedCheckLock(lockQueue, lock, doExpriedLocks)
			}
			freeMillisecondWaitQueue := self.freeMillisecondWaitQueues[glockIndex]
			if freeMillisecondWaitQueue.freeIndex < freeMillisecondWaitQueue.maxFreeCount {
				_ = lockQueue.Reset()
				freeMillisecondWaitQueue.freeIndex++
				freeMillisecondWaitQueue.queues[freeMillisecondWaitQueue.freeIndex] = lockQueue
			}
			self.millisecondExpriedLocks[glockIndex][i] = nil
		}
	}

	if doExpried {
		self.managerGlocks[glockIndex].Unlock()
		for _, lock := range doExpriedLocks {
			self.doExpried(lock, true)
		}
		self.managerGlocks[glockIndex].Lock()
	} else {
		for _, lock := range doExpriedLocks {
			if !lock.isAof && lock.aofTime != 0xff {
				_ = lock.manager.PushLockAof(lock)
			}
		}
	}
}

func (self *LockDB) flushExpriedCheckLock(lockQueue *LockQueue, lock *Lock, doExpriedLocks []*Lock) (*Lock, []*Lock) {
	if !lock.expried {
		doExpriedLocks = append(doExpriedLocks, lock)
		return lockQueue.Pop(), doExpriedLocks
	}

	lockManager := lock.manager
	lock.refCount--
	if lock.refCount == 0 {
		lockManager.FreeLock(lock)
		if lockManager.refCount == 0 {
			self.RemoveLockManager(lockManager)
		}
	}
	return lockQueue.Pop(), doExpriedLocks
}

func (self *LockDB) initNewLockManager(dbId uint8) {
	for i := uint16(0); i < self.managerMaxGlocks; i++ {
		if self.managerGlocks[i].highPriority == 1 || self.managerGlocks[i].lowPriority == 1 {
			self.managerGlocks[i].LowPriorityLock()
			self.managerGlocks[i].LowPriorityUnlock()
		}
	}

	self.glock.Lock()
	lockManager := self.freeLockManagers[self.freeLockManagerTail%self.maxFreeLockManagerCount]
	if lockManager != nil {
		self.glock.Unlock()
		return
	}

	lockManagers := make([]LockManager, 16)
	for i := 0; i < 16; i++ {
		if self.freeLockManagers[(self.freeLockManagerHead+1)%self.maxFreeLockManagerCount] != nil {
			self.glock.Unlock()
			return
		}
		freeLockManagerHead := atomic.AddUint32(&self.freeLockManagerHead, 1) % self.maxFreeLockManagerCount
		if self.freeLockManagers[freeLockManagerHead] != nil {
			self.glock.Unlock()
			atomic.AddUint32(&self.freeLockManagerHead, 0xffffffff)
			return
		}

		lockManagers[i].lockDb = self
		lockManagers[i].dbId = dbId
		lockManagers[i].locks = NewLockQueue(4, 16, 4)
		lockManagers[i].lockMaps = make(map[[16]byte]*Lock, 8)
		lockManagers[i].waitLocks = NewLockQueue(4, 32, 4)
		lockManagers[i].glock = self.managerGlocks[self.managerGlockIndex]
		lockManagers[i].glockIndex = self.managerGlockIndex
		lockManagers[i].freeLocks = self.freeLocks[self.managerGlockIndex]
		lockManagers[i].state = self.states[self.managerGlockIndex]
		lockManagers[i].fastKeyValue = nil

		self.managerGlockIndex++
		if self.managerGlockIndex >= self.managerMaxGlocks {
			self.managerGlockIndex = 0
		}
		self.freeLockManagers[freeLockManagerHead] = &lockManagers[i]
	}
	self.glock.Unlock()
}

func (self *LockDB) GetOrNewLockManager(command *protocol.LockCommand) *LockManager {
	fashHash := (uint32(command.LockKey[0])<<24 | uint32(command.LockKey[1])<<16 | uint32(command.LockKey[2])<<8 | uint32(command.LockKey[3])) ^ (uint32(command.LockKey[4])<<24 | uint32(command.LockKey[5])<<16 | uint32(command.LockKey[6])<<8 | uint32(command.LockKey[7])) ^ (uint32(command.LockKey[8])<<24 | uint32(command.LockKey[9])<<16 | uint32(command.LockKey[10])<<8 | uint32(command.LockKey[11])) ^ (uint32(command.LockKey[12])<<24 | uint32(command.LockKey[13])<<16 | uint32(command.LockKey[14])<<8 | uint32(command.LockKey[15]))
	fastValue := &self.fastLocks[fashHash%self.fastKeyCount]

	if atomic.CompareAndSwapUint32(&fastValue.count, 0, 0) {
		for {
			if atomic.CompareAndSwapUint32(&fastValue.lock, 0, 1) {
				freeLockManagerTail := atomic.AddUint32(&self.freeLockManagerTail, 1) % self.maxFreeLockManagerCount
				lockManager := self.freeLockManagers[freeLockManagerTail]
				for lockManager == nil {
					self.initNewLockManager(command.DbId)
					lockManager = self.freeLockManagers[freeLockManagerTail]
				}
				self.freeLockManagers[freeLockManagerTail] = nil

				lockManager.lockKey = command.LockKey
				lockManager.fastKeyValue = fastValue
				fastValue.manager = lockManager
				atomic.AddUint32(&fastValue.count, 1)
				atomic.AddUint32(&lockManager.state.KeyCount, 1)
				return lockManager
			}

			if atomic.CompareAndSwapUint32(&fastValue.count, 0, 0) {
				for i := uint16(0); i < self.managerMaxGlocks; i++ {
					if self.managerGlocks[i].highPriority == 1 || self.managerGlocks[i].lowPriority == 1 {
						self.managerGlocks[i].LowPriorityLock()
						self.managerGlocks[i].LowPriorityUnlock()
					}
				}
				time.Sleep(time.Nanosecond)
				continue
			}
			break
		}
	}

	fastLockManager := fastValue.manager
	if fastLockManager != nil && fastLockManager.lockKey == command.LockKey {
		return fastLockManager
	}
	self.mGlock.Lock()
	if lockManager, ok := self.locks[command.LockKey]; ok {
		self.mGlock.Unlock()
		return lockManager
	}

	freeLockManagerTail := atomic.AddUint32(&self.freeLockManagerTail, 1) % self.maxFreeLockManagerCount
	lockManager := self.freeLockManagers[freeLockManagerTail]
	for lockManager == nil {
		self.initNewLockManager(command.DbId)
		lockManager = self.freeLockManagers[freeLockManagerTail]
	}
	self.freeLockManagers[freeLockManagerTail] = nil
	self.locks[command.LockKey] = lockManager
	self.mGlock.Unlock()

	lockManager.lockKey = command.LockKey
	lockManager.fastKeyValue = fastValue
	atomic.AddUint32(&fastValue.count, 1)
	atomic.AddUint32(&lockManager.state.KeyCount, 1)
	return lockManager
}

func (self *LockDB) GetLockManager(command *protocol.LockCommand) *LockManager {
	fashHash := (uint32(command.LockKey[0])<<24 | uint32(command.LockKey[1])<<16 | uint32(command.LockKey[2])<<8 | uint32(command.LockKey[3])) ^ (uint32(command.LockKey[4])<<24 | uint32(command.LockKey[5])<<16 | uint32(command.LockKey[6])<<8 | uint32(command.LockKey[7])) ^ (uint32(command.LockKey[8])<<24 | uint32(command.LockKey[9])<<16 | uint32(command.LockKey[10])<<8 | uint32(command.LockKey[11])) ^ (uint32(command.LockKey[12])<<24 | uint32(command.LockKey[13])<<16 | uint32(command.LockKey[14])<<8 | uint32(command.LockKey[15]))
	fastValue := &self.fastLocks[fashHash%self.fastKeyCount]

	if atomic.CompareAndSwapUint32(&fastValue.count, 0, 0) {
		return nil
	}
	fastLockManager := fastValue.manager
	if fastLockManager != nil && fastLockManager.lockKey == command.LockKey {
		return fastLockManager
	}

	self.mGlock.Lock()
	if lockManager, ok := self.locks[command.LockKey]; ok {
		self.mGlock.Unlock()
		return lockManager
	}
	self.mGlock.Unlock()
	return nil
}

func (self *LockDB) RemoveLockManager(lockManager *LockManager) {
	fastValue := lockManager.fastKeyValue
	if fastValue == nil {
		return
	}

	if fastValue.manager == lockManager {
		if !atomic.CompareAndSwapUint32(&fastValue.lock, 1, 0) {
			return
		}

		lockManager.lockKey[0], lockManager.lockKey[1], lockManager.lockKey[2], lockManager.lockKey[3], lockManager.lockKey[4], lockManager.lockKey[5], lockManager.lockKey[6], lockManager.lockKey[7],
			lockManager.lockKey[8], lockManager.lockKey[9], lockManager.lockKey[10], lockManager.lockKey[11], lockManager.lockKey[12], lockManager.lockKey[13], lockManager.lockKey[14], lockManager.lockKey[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
		lockManager.fastKeyValue = nil
		fastValue.manager = nil
		atomic.AddUint32(&fastValue.count, 0xffffffff)

		if self.freeLockManagers[(self.freeLockManagerHead+1)%self.maxFreeLockManagerCount] == nil {
			freeLockManagerHead := atomic.AddUint32(&self.freeLockManagerHead, 1) % self.maxFreeLockManagerCount
			if self.freeLockManagers[freeLockManagerHead] == nil {
				self.freeLockManagers[freeLockManagerHead] = lockManager

				if lockManager.locks != nil {
					_ = lockManager.locks.Rellac()
				}
				if lockManager.waitLocks != nil {
					_ = lockManager.waitLocks.Rellac()
				}
				atomic.AddUint32(&lockManager.state.KeyCount, 0xffffffff)
				return
			}
			atomic.AddUint32(&self.freeLockManagerHead, 0xffffffff)
		}

		lockManager.currentLock = nil
		lockManager.locks = nil
		lockManager.lockMaps = nil
		lockManager.waitLocks = nil
		lockManager.freeLocks = nil
		atomic.AddUint32(&lockManager.state.KeyCount, 0xffffffff)
		return
	}

	self.mGlock.Lock()
	if _, ok := self.locks[lockManager.lockKey]; !ok {
		self.mGlock.Unlock()
		return
	}

	delete(self.locks, lockManager.lockKey)
	self.mGlock.Unlock()
	lockManager.lockKey[0], lockManager.lockKey[1], lockManager.lockKey[2], lockManager.lockKey[3], lockManager.lockKey[4], lockManager.lockKey[5], lockManager.lockKey[6], lockManager.lockKey[7],
		lockManager.lockKey[8], lockManager.lockKey[9], lockManager.lockKey[10], lockManager.lockKey[11], lockManager.lockKey[12], lockManager.lockKey[13], lockManager.lockKey[14], lockManager.lockKey[15] =
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0
	lockManager.fastKeyValue = nil
	atomic.AddUint32(&fastValue.count, 0xffffffff)

	if self.freeLockManagers[(self.freeLockManagerHead+1)%self.maxFreeLockManagerCount] == nil {
		freeLockManagerHead := atomic.AddUint32(&self.freeLockManagerHead, 1) % self.maxFreeLockManagerCount
		if self.freeLockManagers[freeLockManagerHead] == nil {
			self.freeLockManagers[freeLockManagerHead] = lockManager

			if lockManager.locks != nil {
				_ = lockManager.locks.Rellac()
			}
			if lockManager.waitLocks != nil {
				_ = lockManager.waitLocks.Rellac()
			}
			atomic.AddUint32(&lockManager.state.KeyCount, 0xffffffff)
			return
		}
		atomic.AddUint32(&self.freeLockManagerHead, 0xffffffff)
	}

	lockManager.currentLock = nil
	lockManager.locks = nil
	lockManager.lockMaps = nil
	lockManager.waitLocks = nil
	lockManager.freeLocks = nil
	atomic.AddUint32(&lockManager.state.KeyCount, 0xffffffff)
}

func (self *LockDB) AddTimeOut(lock *Lock, lockTimeoutTime int64) {
	lock.timeouted = false

	if lock.timeoutCheckedCount > TIMEOUT_QUEUE_MAX_WAIT {
		if lockTimeoutTime < self.checkTimeoutTime {
			lockTimeoutTime = self.checkTimeoutTime
		}

		if longLocks, ok := self.longTimeoutLocks[lock.manager.glockIndex][lockTimeoutTime]; !ok {
			freeLongWaitQueue := self.freeLongWaitQueues[lock.manager.glockIndex]
			if freeLongWaitQueue.freeIndex < 0 {
				longLocks = &LongWaitLockQueue{NewLockQueue(4, 64, LONG_LOCKS_QUEUE_INIT_SIZE), lockTimeoutTime, 0, lock.manager.glockIndex}
			} else {
				longLocks = freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex]
				freeLongWaitQueue.freeIndex--
				longLocks.lockTime = lockTimeoutTime
			}
			self.longTimeoutLocks[lock.manager.glockIndex][lockTimeoutTime] = longLocks
			lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
			if longLocks.locks.Push(lock) != nil {
				lock.longWaitIndex = 0
			}
		} else {
			lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
			if longLocks.locks.Push(lock) != nil {
				lock.longWaitIndex = 0
			}
		}
	} else {
		doTimeoutTime := self.checkTimeoutTime + int64(lock.timeoutCheckedCount)
		if lockTimeoutTime < doTimeoutTime {
			doTimeoutTime = lockTimeoutTime
			if doTimeoutTime < self.checkTimeoutTime {
				doTimeoutTime = self.checkTimeoutTime
			}
		}

		_ = self.timeoutLocks[doTimeoutTime&TIMEOUT_QUEUE_LENGTH_MASK][lock.manager.glockIndex].Push(lock)
	}
}

func (self *LockDB) RemoveTimeOut(lock *Lock) {
	lock.timeouted = true
	lock.manager.state.WaitCount--
}

func (self *LockDB) RemoveLongTimeOut(lock *Lock) {
	// lock.timeouted = true
	longLocks := self.longTimeoutLocks[lock.manager.glockIndex][lock.timeoutTime]
	longLocks.locks.queues[int32(lock.longWaitIndex>>32)][int32(lock.longWaitIndex&0xffffffff)-1] = nil
	longLocks.freeCount++
	lock.longWaitIndex = 0
	lock.refCount--
}

func (self *LockDB) doTimeOut(lock *Lock, forcedExpried bool) {
	lockManager := lock.manager
	lockManager.glock.Lock()
	if lock.timeouted {
		lock.refCount--
		if lock.refCount == 0 {
			lockManager.FreeLock(lock)
			if lockManager.refCount == 0 {
				self.RemoveLockManager(lockManager)
			}
		}

		lockManager.glock.Unlock()
		return
	}

	if !forcedExpried {
		if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_KEEPLIVED != 0 {
			stream := lock.protocol.GetStream()
			if stream != nil && !stream.closed {
				lock.timeoutTime = self.currentTime + int64(lock.command.Timeout)
				self.AddTimeOut(lock, lock.timeoutTime)
				lockManager.glock.Unlock()
				return
			}
		}
	}

	lockLocked := lock.locked
	lock.timeouted = true
	lockProtocol, lockCommand := lock.protocol, lock.command

	if lockLocked > 0 {
		lockManager.locked -= uint32(lockLocked)
		lockManager.RemoveLock(lock)
		if lock.isAof {
			_ = lockManager.PushUnLockAof(lock, nil, false, AOF_FLAG_TIMEOUTED)
		}
	} else {
		if lockManager.GetWaitLock() == nil {
			lockManager.waited = false
		}
		lockManager.state.WaitCount--
	}
	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_PUSH_SUBSCRIBE != 0 {
		_ = self.subscribeChannels[lockManager.glockIndex].Push(lockCommand, protocol.RESULT_TIMEOUT, uint16(lockManager.locked), lock.locked)
	}

	lock.refCount--
	if lock.refCount == 0 {
		lockManager.FreeLock(lock)
		if lockManager.refCount == 0 {
			self.RemoveLockManager(lockManager)
		}
	}
	lockManager.state.TimeoutedCount++
	lockManager.glock.Unlock()

	timeoutFlag := lockCommand.TimeoutFlag
	if timeoutFlag&protocol.TIMEOUT_FLAG_LOG_ERROR_WHEN_TIMEOUT != 0 {
		self.slock.Log().Errorf("Database lock timeout DbId:%d LockKey:%x LockId:%x RequestId:%x RemoteAddr:%s", lockCommand.DbId,
			lockCommand.LockKey, lockCommand.LockId, lockCommand.RequestId, lockProtocol.RemoteAddr().String())
	} else {
		self.slock.Log().Debugf("Database lock timeout DbId:%d LockKey:%x LockId:%x RequestId:%x RemoteAddr:%s", lockCommand.DbId,
			lockCommand.LockKey, lockCommand.LockId, lockCommand.RequestId, lockProtocol.RemoteAddr().String())
	}

	_ = lockProtocol.ProcessLockResultCommandLocked(lockCommand, protocol.RESULT_TIMEOUT, uint16(lockManager.locked), lock.locked)
	if lockLocked > 0 {
		self.wakeUpWaitLocks(lockManager, nil)
	} else {
		if timeoutFlag&protocol.TIMEOUT_FLAG_REVERSE_KEY_LOCK_WHEN_TIMEOUT != 0 {
			lockCommand.TimeoutFlag = 0
			lockKey := lockCommand.LockKey
			lockCommand.LockKey[0], lockCommand.LockKey[1], lockCommand.LockKey[2], lockCommand.LockKey[3], lockCommand.LockKey[4], lockCommand.LockKey[5], lockCommand.LockKey[6], lockCommand.LockKey[7],
				lockCommand.LockKey[8], lockCommand.LockKey[9], lockCommand.LockKey[10], lockCommand.LockKey[11], lockCommand.LockKey[12], lockCommand.LockKey[13], lockCommand.LockKey[14], lockCommand.LockKey[15] =
				lockKey[15], lockKey[14], lockKey[13], lockKey[12], lockKey[11], lockKey[10], lockKey[9], lockKey[8],
				lockKey[7], lockKey[6], lockKey[5], lockKey[4], lockKey[3], lockKey[2], lockKey[1], lockKey[0]

			_ = self.Lock(lockProtocol.serverProtocol, lockCommand)
		} else {
			_ = lockProtocol.FreeLockCommandLocked(lockCommand)
		}
	}
}

func (self *LockDB) AddMillisecondTimeOut(lock *Lock) {
	lock.timeouted = false
	ms := time.Now().UnixNano()/1e6 + int64(lock.command.Timeout%1000)

	lockQueue := self.millisecondTimeoutLocks[lock.manager.glockIndex][ms%1000]
	if lockQueue == nil {
		freeMillisecondWaitQueue := self.freeMillisecondWaitQueues[lock.manager.glockIndex]
		if freeMillisecondWaitQueue.freeIndex < 0 {
			lockQueue = NewLockQueue(4, 64, MILLISECOND_LOCKS_QUEUE_INIT_SIZE)
		} else {
			lockQueue = freeMillisecondWaitQueue.queues[freeMillisecondWaitQueue.freeIndex]
			freeMillisecondWaitQueue.freeIndex--
		}

		self.millisecondTimeoutLocks[lock.manager.glockIndex][ms%1000] = lockQueue
		go self.checkMillisecondTimeOut(ms, lock.manager.glockIndex)
	}
	_ = lockQueue.Push(lock)
}

func (self *LockDB) AddExpried(lock *Lock, lockExpriedTime int64) {
	lock.expried = false

	if lock.expriedCheckedCount > EXPRIED_QUEUE_MAX_WAIT {
		if lockExpriedTime < self.checkExpriedTime {
			lockExpriedTime = self.checkExpriedTime
		}

		if longLocks, ok := self.longExpriedLocks[lock.manager.glockIndex][lockExpriedTime]; !ok {
			freeLongWaitQueue := self.freeLongWaitQueues[lock.manager.glockIndex]
			if freeLongWaitQueue.freeIndex < 0 {
				longLocks = &LongWaitLockQueue{NewLockQueue(4, 64, LONG_LOCKS_QUEUE_INIT_SIZE), lockExpriedTime, 0, lock.manager.glockIndex}
			} else {
				longLocks = freeLongWaitQueue.queues[freeLongWaitQueue.freeIndex]
				freeLongWaitQueue.freeIndex--
				longLocks.lockTime = lockExpriedTime
			}
			self.longExpriedLocks[lock.manager.glockIndex][lockExpriedTime] = longLocks
			lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
			if longLocks.locks.Push(lock) != nil {
				lock.longWaitIndex = 0
			}
		} else {
			lock.longWaitIndex = uint64(longLocks.locks.tailNodeIndex)<<32 | uint64(longLocks.locks.tailQueueIndex+1)
			if longLocks.locks.Push(lock) != nil {
				lock.longWaitIndex = 0
			}
		}
	} else {
		doExpriedTime := self.checkExpriedTime + int64(lock.expriedCheckedCount)
		if lockExpriedTime < doExpriedTime {
			doExpriedTime = lockExpriedTime
			if doExpriedTime < self.checkExpriedTime {
				doExpriedTime = self.checkExpriedTime
			}
		}

		_ = self.expriedLocks[doExpriedTime&EXPRIED_QUEUE_LENGTH_MASK][lock.manager.glockIndex].Push(lock)
		if !lock.isAof && lock.aofTime != 0xff {
			if self.currentTime-lock.startTime >= int64(lock.aofTime) {
				for i := uint8(0); i < lock.locked; i++ {
					_ = lock.manager.PushLockAof(lock)
				}
			}
		}
	}
}

func (self *LockDB) RemoveExpried(lock *Lock) {
	lock.expried = true
	lock.manager.state.ExpriedCount--
}

func (self *LockDB) RemoveLongExpried(lock *Lock) {
	longLocks := self.longExpriedLocks[lock.manager.glockIndex][lock.expriedTime]
	longLocks.locks.queues[int32(lock.longWaitIndex>>32)][int32(lock.longWaitIndex&0xffffffff)-1] = nil
	longLocks.freeCount++
	lock.longWaitIndex = 0
	lock.refCount--
}

func (self *LockDB) doExpried(lock *Lock, forcedExpried bool) {
	lockManager := lock.manager
	lockManager.glock.Lock()

	if lock.expried {
		lock.refCount--
		if lock.refCount == 0 {
			lockManager.FreeLock(lock)
			if lockManager.refCount == 0 {
				self.RemoveLockManager(lockManager)
			}
		}

		lockManager.glock.Unlock()
		return
	}

	if !forcedExpried {
		if self.status != STATE_LEADER {
			if lock.expriedTime <= 0 || self.currentTime-lock.expriedTime < EXPRIED_WAIT_LEADER_MAX_TIME {
				self.AddExpried(lock, self.currentTime+30)
				lockManager.glock.Unlock()
				return
			}
		}

		if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_KEEPLIVED != 0 {
			stream := lock.protocol.GetStream()
			if stream != nil && !stream.closed {
				lock.expriedTime = self.currentTime + int64(lock.command.Expried)
				self.AddExpried(lock, lock.expriedTime)
				lockManager.glock.Unlock()
				return
			}
		}
	}

	lockLocked := lock.locked
	lock.expried = true
	lockManager.locked -= uint32(lockLocked)
	lockProtocol, lockCommand := lock.protocol, lock.command
	lockManager.RemoveLock(lock)
	if lock.isAof {
		_ = lockManager.PushUnLockAof(lock, nil, false, AOF_FLAG_EXPRIED)
	}
	if lockCommand.ExpriedFlag&protocol.EXPRIED_FLAG_PUSH_SUBSCRIBE != 0 {
		_ = self.subscribeChannels[lockManager.glockIndex].Push(lockCommand, protocol.RESULT_EXPRIED, uint16(lockManager.locked), lock.locked)
	}

	lock.refCount--
	if lock.refCount == 0 {
		lockManager.FreeLock(lock)
		if lockManager.refCount == 0 {
			self.RemoveLockManager(lockManager)
		}
	}
	lockManager.state.LockedCount -= uint32(lockLocked)
	lockManager.state.ExpriedCount++
	lockManager.glock.Unlock()

	expriedFlag := lockCommand.ExpriedFlag
	if expriedFlag&protocol.EXPRIED_FLAG_LOG_ERROR_WHEN_EXPRIED != 0 {
		self.slock.Log().Errorf("Database lock expried DbId:%d LockKey:%x LockId:%x RequestId:%x RemoteAddr:%s", lockCommand.DbId,
			lockCommand.LockKey, lockCommand.LockId, lockCommand.RequestId, lockProtocol.RemoteAddr().String())
	} else {
		self.slock.Log().Debugf("Database lock expried DbId:%d LockKey:%x LockId:%x RequestId:%x RemoteAddr:%s", lockCommand.DbId,
			lockCommand.LockKey, lockCommand.LockId, lockCommand.RequestId, lockProtocol.RemoteAddr().String())
	}

	_ = lockProtocol.ProcessLockResultCommandLocked(lockCommand, protocol.RESULT_EXPRIED, uint16(lockManager.locked), lock.locked)
	if expriedFlag&protocol.EXPRIED_FLAG_REVERSE_KEY_LOCK_WHEN_EXPRIED == 0 {
		_ = lockProtocol.FreeLockCommandLocked(lockCommand)
	}

	self.wakeUpWaitLocks(lockManager, nil)

	if expriedFlag&protocol.EXPRIED_FLAG_REVERSE_KEY_LOCK_WHEN_EXPRIED != 0 {
		lockCommand.ExpriedFlag = 0
		lockCommand.Expried = lockCommand.Timeout
		lockKey := lockCommand.LockKey
		lockCommand.LockKey[0], lockCommand.LockKey[1], lockCommand.LockKey[2], lockCommand.LockKey[3], lockCommand.LockKey[4], lockCommand.LockKey[5], lockCommand.LockKey[6], lockCommand.LockKey[7],
			lockCommand.LockKey[8], lockCommand.LockKey[9], lockCommand.LockKey[10], lockCommand.LockKey[11], lockCommand.LockKey[12], lockCommand.LockKey[13], lockCommand.LockKey[14], lockCommand.LockKey[15] =
			lockKey[15], lockKey[14], lockKey[13], lockKey[12], lockKey[11], lockKey[10], lockKey[9], lockKey[8],
			lockKey[7], lockKey[6], lockKey[5], lockKey[4], lockKey[3], lockKey[2], lockKey[1], lockKey[0]

		_ = self.Lock(lockProtocol.serverProtocol, lockCommand)
	}
}

func (self *LockDB) AddMillisecondExpried(lock *Lock) {
	lock.expried = false
	ms := time.Now().UnixNano()/1e6 + int64(lock.command.Expried%1000)

	lockQueue := self.millisecondExpriedLocks[lock.manager.glockIndex][ms%1000]
	if lockQueue == nil {
		freeMillisecondWaitQueue := self.freeMillisecondWaitQueues[lock.manager.glockIndex]
		if freeMillisecondWaitQueue.freeIndex < 0 {
			lockQueue = NewLockQueue(4, 64, MILLISECOND_LOCKS_QUEUE_INIT_SIZE)
		} else {
			lockQueue = freeMillisecondWaitQueue.queues[freeMillisecondWaitQueue.freeIndex]
			freeMillisecondWaitQueue.freeIndex--
		}

		self.millisecondExpriedLocks[lock.manager.glockIndex][ms%1000] = lockQueue
		go self.checkMillisecondExpried(ms, lock.manager.glockIndex)
	}
	_ = lockQueue.Push(lock)

	if !lock.isAof && lock.aofTime == 0 {
		_ = lock.manager.PushLockAof(lock)
	}
}

func (self *LockDB) Lock(serverProtocol ServerProtocol, command *protocol.LockCommand) error {
	/*
	   protocol.LockCommand.Flag
	   |7              |        4       |    2   |           1           |         0           |
	   |---------------|----------------|--------|-----------------------|---------------------|
	   |               |concurrent_check|from_aof|when_locked_update_lock|when_locked_show_lock|
	*/

	lockManager := self.GetOrNewLockManager(command)
	if command.Timeout == 0 && command.Flag&protocol.LOCK_FLAG_CONCURRENT_CHECK != 0 {
		if lockManager.locked == 0xffff || lockManager.locked > uint32(command.Count) {
			if lockManager.refCount > 0 {
				_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_TIMEOUT, uint16(lockManager.locked), 0)
				_ = serverProtocol.FreeLockCommand(command)
				return nil
			}
		}
	}

	if command.Flag&protocol.LOCK_FLAG_FROM_AOF == 0 {
		lockManager.glock.LowPriorityLock()
	} else {
		lockManager.glock.Lock()
	}
	if lockManager.lockKey != command.LockKey {
		lockManager.glock.Unlock()
		return self.Lock(serverProtocol, command)
	}

	if self.status != STATE_LEADER {
		if command.Flag&protocol.LOCK_FLAG_FROM_AOF == 0 {
			if lockManager.refCount == 0 {
				self.RemoveLockManager(lockManager)
			}
			lockManager.glock.Unlock()
			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_STATE_ERROR, uint16(lockManager.locked), 0)
			_ = serverProtocol.FreeLockCommand(command)
			return nil
		}
	}

	waited := lockManager.waited
	if lockManager.locked > 0 {
		if command.Flag&protocol.LOCK_FLAG_SHOW_WHEN_LOCKED != 0 {
			currentLock := lockManager.currentLock
			command.LockId = currentLock.command.LockId
			command.Expried = uint16(currentLock.expriedTime - currentLock.startTime)
			command.Timeout = currentLock.command.Timeout
			command.Count = currentLock.command.Count
			command.Rcount = currentLock.command.Rcount
			lockManager.glock.Unlock()

			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNOWN_ERROR, uint16(lockManager.locked), currentLock.locked)
			_ = serverProtocol.FreeLockCommand(command)
			return nil
		}

		currentLock := lockManager.GetLockedLock(command)
		if currentLock != nil {
			if currentLock.ackCount != 0xff {
				lockManager.glock.Unlock()

				_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNLOCK_ERROR, uint16(lockManager.locked), currentLock.locked)
				_ = serverProtocol.FreeLockCommand(command)
				return nil
			}

			if command.Flag&protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED != 0 {
				if currentLock.longWaitIndex > 0 {
					self.RemoveLongExpried(currentLock)
					lockManager.UpdateLockedLock(currentLock, command.Timeout, command.TimeoutFlag, command.Expried, command.ExpriedFlag, command.Count, command.Rcount)
					if command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
						self.AddExpried(currentLock, currentLock.expriedTime)
					} else {
						self.AddMillisecondExpried(currentLock)
					}

					currentLock.refCount++
				} else {
					lockManager.UpdateLockedLock(currentLock, command.TimeoutFlag, command.Timeout, command.Expried, command.ExpriedFlag, command.Count, command.Rcount)
				}
				currentLock.protocol = serverProtocol.GetProxy()
				if currentLock.isAof {
					_ = lockManager.PushLockAof(currentLock)
				}

				command.Expried = uint16(currentLock.expriedTime - currentLock.startTime)
				command.Timeout = currentLock.command.Timeout
				command.Count = currentLock.command.Count
				command.Rcount = currentLock.command.Rcount
				lockManager.glock.Unlock()
			} else if currentLock.locked < 0xff && currentLock.locked <= command.Rcount {
				if command.Expried == 0 {
					command.Expried = uint16(currentLock.expriedTime - currentLock.startTime)
					command.Timeout = currentLock.command.Timeout
					command.Count = currentLock.command.Count
					command.Rcount = currentLock.command.Rcount
					lockManager.glock.Unlock()

					_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_EXPRIED, uint16(lockManager.locked), currentLock.locked)
					_ = serverProtocol.FreeLockCommand(command)
					return nil
				}

				lockManager.locked++
				currentLock.locked++
				if currentLock.longWaitIndex > 0 {
					self.RemoveLongExpried(currentLock)
					lockManager.UpdateLockedLock(currentLock, command.Timeout, command.TimeoutFlag, command.Expried, command.ExpriedFlag, command.Count, command.Rcount)
					if command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
						self.AddExpried(currentLock, currentLock.expriedTime)
					} else {
						self.AddMillisecondExpried(currentLock)
					}
					currentLock.refCount++
				} else {
					lockManager.UpdateLockedLock(currentLock, command.Timeout, command.TimeoutFlag, command.Expried, command.ExpriedFlag, command.Count, command.Rcount)
				}
				currentLock.protocol = serverProtocol.GetProxy()
				if currentLock.isAof {
					_ = lockManager.PushLockAof(currentLock)
				}
				lockManager.state.LockCount++
				lockManager.state.LockedCount++
				lockManager.glock.Unlock()

				_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_SUCCED, uint16(lockManager.locked), currentLock.locked)
				_ = serverProtocol.FreeLockCommand(command)
				return nil
			} else {
				lockManager.glock.Unlock()
			}

			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_LOCKED_ERROR, uint16(lockManager.locked), currentLock.locked)
			_ = serverProtocol.FreeLockCommand(command)
			return nil
		}
	} else {
		if command.TimeoutFlag&protocol.TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK != 0 {
			if lockManager.waited {
				lockManager.glock.Unlock()

				_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNOWN_ERROR, uint16(lockManager.locked), 0)
				_ = serverProtocol.FreeLockCommand(command)
				return nil
			}

			waited = true
		} else {
			waited = false
		}
	}

	lock := lockManager.GetOrNewLock(serverProtocol, command)
	if !waited && self.doLock(lockManager, lock) {
		requireWakeup := lockManager.waited && lock.locked == 0
		if command.Expried > 0 {
			lockManager.AddLock(lock)
			lockManager.locked++

			if command.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED != 0 && !lock.isAof && lock.aofTime != 0xff {
				if command.TimeoutFlag&protocol.TIMEOUT_FLAG_MILLISECOND_TIME == 0 {
					self.AddTimeOut(lock, lock.timeoutTime)
				} else {
					self.AddMillisecondTimeOut(lock)
				}
				lock.refCount += 2
				err := lockManager.PushLockAof(lock)
				if err == nil {
					lockManager.glock.Unlock()
				} else {
					lockManager.glock.Unlock()
					self.DoAckLock(lock, false)
				}
				return nil
			}

			if command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
				self.AddExpried(lock, lock.expriedTime)
			} else {
				self.AddMillisecondExpried(lock)
			}
			lock.refCount++
			lockManager.state.LockCount++
			lockManager.state.LockedCount++
			lockManager.glock.Unlock()

			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_SUCCED, uint16(lockManager.locked), lock.locked)

			if requireWakeup {
				self.wakeUpWaitLocks(lockManager, serverProtocol)
			}
			return nil
		}

		if command.ExpriedFlag&protocol.EXPRIED_FLAG_PUSH_SUBSCRIBE != 0 {
			_ = self.subscribeChannels[lockManager.glockIndex].Push(command, protocol.RESULT_EXPRIED, uint16(lockManager.locked), lock.locked)
		}
		lockManager.FreeLock(lock)
		if lockManager.refCount == 0 {
			self.RemoveLockManager(lockManager)
		}
		lockManager.state.LockCount++
		lockManager.glock.Unlock()

		_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_SUCCED, uint16(lockManager.locked), lock.locked)
		_ = serverProtocol.FreeLockCommand(command)

		if requireWakeup {
			self.wakeUpWaitLocks(lockManager, serverProtocol)
		}
		return nil
	}

	if command.Timeout > 0 {
		lockManager.AddWaitLock(lock)
		if command.TimeoutFlag&protocol.TIMEOUT_FLAG_MILLISECOND_TIME == 0 {
			self.AddTimeOut(lock, lock.timeoutTime)
		} else {
			self.AddMillisecondTimeOut(lock)
		}
		lock.refCount++
		lockManager.state.WaitCount++
		lockManager.glock.Unlock()
		return nil
	}

	if lock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_PUSH_SUBSCRIBE != 0 {
		_ = self.subscribeChannels[lockManager.glockIndex].Push(command, protocol.RESULT_TIMEOUT, uint16(lockManager.locked), lock.locked)
	}
	lockManager.FreeLock(lock)
	if lockManager.refCount == 0 {
		self.RemoveLockManager(lockManager)
	}
	lockManager.glock.Unlock()

	_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_TIMEOUT, uint16(lockManager.locked), lock.locked)
	_ = serverProtocol.FreeLockCommand(command)
	return nil
}

func (self *LockDB) UnLock(serverProtocol ServerProtocol, command *protocol.LockCommand) error {
	/*
	   protocol.LockCommand.Flag
	   |7                  |    2   |           1             |               0               |
	   |-------------------|--------|-------------------------|-------------------------------|
	   |                   |from_aof|when_unlocked_cancel_wait|when_unlocked_unlock_first_lock|
	*/

	lockManager := self.GetLockManager(command)
	if lockManager == nil {
		_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNLOCK_ERROR, 0, 0)
		_ = serverProtocol.FreeLockCommand(command)
		atomic.AddUint32(&self.states[self.managerMaxGlocks].UnlockErrorCount, 1)
		return nil
	}

	if command.Flag&protocol.UNLOCK_FLAG_FROM_AOF == 0 {
		lockManager.glock.LowPriorityLock()
	} else {
		lockManager.glock.Lock()
	}
	if lockManager.lockKey != command.LockKey {
		lockManager.glock.Unlock()
		return self.UnLock(serverProtocol, command)
	}

	if self.status != STATE_LEADER {
		if command.Flag&protocol.UNLOCK_FLAG_FROM_AOF == 0 {
			lockManager.state.UnlockErrorCount++
			lockManager.glock.Unlock()

			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_STATE_ERROR, uint16(lockManager.locked), 0)
			_ = serverProtocol.FreeLockCommand(command)
			return nil
		}
	}

	if lockManager.locked == 0 {
		if command.Flag&protocol.UNLOCK_FLAG_CANCEL_WAIT_LOCK_WHEN_UNLOCKED != 0 {
			self.cancelWaitLock(lockManager, command, serverProtocol)
			return nil
		}
		lockManager.state.UnlockErrorCount++
		lockManager.glock.Unlock()

		_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNLOCK_ERROR, uint16(lockManager.locked), 0)
		_ = serverProtocol.FreeLockCommand(command)
		return nil
	}

	currentLock := lockManager.GetLockedLock(command)
	if currentLock == nil {
		if command.Flag&protocol.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED != 0 {
			currentLock = lockManager.currentLock

			if currentLock == nil {
				lockManager.state.UnlockErrorCount++
				lockManager.glock.Unlock()

				_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNOWN_ERROR, uint16(lockManager.locked), 0)
				_ = serverProtocol.FreeLockCommand(command)
				return nil
			}

			command.LockId = currentLock.command.LockId
			command.Expried = currentLock.command.Expried
			command.Timeout = currentLock.command.Timeout
			command.Count = currentLock.command.Count
			command.Rcount = currentLock.command.Rcount
		} else if command.Flag&protocol.UNLOCK_FLAG_CANCEL_WAIT_LOCK_WHEN_UNLOCKED != 0 {
			self.cancelWaitLock(lockManager, command, serverProtocol)
			return nil
		} else {
			lockManager.state.UnlockErrorCount++
			lockManager.glock.Unlock()

			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNOWN_ERROR, uint16(lockManager.locked), 0)
			_ = serverProtocol.FreeLockCommand(command)
			return nil
		}
	} else {
		if currentLock.ackCount != 0xff {
			lockManager.state.UnlockErrorCount++
			lockManager.glock.Unlock()

			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_LOCKED_ERROR, uint16(lockManager.locked), currentLock.locked)
			_ = serverProtocol.FreeLockCommand(command)
			return nil
		}
	}

	if currentLock.locked > 1 {
		unlocked, lockLocked := false, currentLock.locked
		if command.Rcount > 0 {
			currentLock.locked--
			lockManager.locked--
			if currentLock.locked == 0 {
				unlocked = true
			} else {
				if currentLock.isAof {
					_ = lockManager.PushUnLockAof(currentLock, command, true, 0)
				}
				lockManager.state.UnLockCount++
				lockManager.state.LockedCount--
				lockManager.glock.Unlock()

				_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_SUCCED, uint16(lockManager.locked), currentLock.locked)
				_ = serverProtocol.FreeLockCommand(command)
			}
		} else {
			unlocked = true
			lockManager.locked -= uint32(lockLocked)
		}

		if unlocked {
			//self.RemoveExpried(current_lock)
			currentLockCommand := currentLock.command
			currentLock.expried = true
			if currentLock.longWaitIndex > 0 {
				self.RemoveLongExpried(currentLock)
				lockManager.RemoveLock(currentLock)
				if currentLock.isAof {
					_ = lockManager.PushUnLockAof(currentLock, command, false, 0)
				}

				if currentLock.refCount == 0 {
					lockManager.FreeLock(currentLock)
					if lockManager.refCount == 0 {
						self.RemoveLockManager(lockManager)
					}
				}
			} else {
				lockManager.RemoveLock(currentLock)
				if currentLock.isAof {
					_ = lockManager.PushUnLockAof(currentLock, command, false, 0)
				}
			}
			lockManager.state.UnLockCount += uint64(lockLocked)
			lockManager.state.LockedCount -= uint32(lockLocked)
			lockManager.glock.Unlock()

			_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_SUCCED, uint16(lockManager.locked), currentLock.locked)
			_ = serverProtocol.FreeLockCommand(command)
			_ = serverProtocol.FreeLockCommand(currentLockCommand)
		}
	} else {
		currentLockCommand := currentLock.command
		//self.RemoveExpried(current_lock)
		currentLock.expried = true
		if currentLock.longWaitIndex > 0 {
			self.RemoveLongExpried(currentLock)
			lockManager.RemoveLock(currentLock)
			if currentLock.isAof {
				_ = lockManager.PushUnLockAof(currentLock, command, false, 0)
			}
			lockManager.locked--

			if currentLock.refCount == 0 {
				lockManager.FreeLock(currentLock)
				if lockManager.refCount == 0 {
					self.RemoveLockManager(lockManager)
				}
			}
		} else {
			lockManager.RemoveLock(currentLock)
			if currentLock.isAof {
				_ = lockManager.PushUnLockAof(currentLock, command, false, 0)
			}
			lockManager.locked--
		}
		lockManager.state.UnLockCount++
		lockManager.state.LockedCount--
		lockManager.glock.Unlock()

		_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_SUCCED, uint16(lockManager.locked), currentLock.locked)
		_ = serverProtocol.FreeLockCommand(command)
		_ = serverProtocol.FreeLockCommand(currentLockCommand)
	}

	self.wakeUpWaitLocks(lockManager, serverProtocol)
	return nil
}

func (self *LockDB) doLock(lockManager *LockManager, lock *Lock) bool {
	if lockManager.locked == 0 {
		return true
	}

	if lockManager.locked == 0xffff {
		return false
	}

	if lockManager.locked <= uint32(lockManager.currentLock.command.Count) {
		if lockManager.locked <= uint32(lock.command.Count) {
			return true
		}
	}

	return false
}

func (self *LockDB) wakeUpWaitLocks(lockManager *LockManager, serverProtocol ServerProtocol) {
	if lockManager.waited {
		lockManager.glock.Lock()
		waitLock := lockManager.GetWaitLock()
		for waitLock != nil {
			if !self.doLock(lockManager, waitLock) {
				lockManager.glock.Unlock()
				return
			}

			self.wakeUpWaitLock(lockManager, waitLock, serverProtocol)
			lockManager.glock.Lock()
			waitLock = lockManager.GetWaitLock()
		}

		if lockManager.waited {
			lockManager.waited = false
			if lockManager.refCount == 0 {
				self.RemoveLockManager(lockManager)
			}
		}
		lockManager.glock.Unlock()
	}
}

func (self *LockDB) wakeUpWaitLock(lockManager *LockManager, waitLock *Lock, serverProtocol ServerProtocol) {
	//self.RemoveTimeOut(wait_lock)
	if waitLock.command.TimeoutFlag&protocol.TIMEOUT_FLAG_REQUIRE_ACKED != 0 && !waitLock.isAof && waitLock.aofTime != 0xff && waitLock.command.Flag&protocol.LOCK_FLAG_FROM_AOF == 0 {
		lockManager.AddLock(waitLock)
		lockManager.locked++
		waitLock.refCount++
		err := lockManager.PushLockAof(waitLock)
		if err == nil {
			lockManager.glock.Unlock()
		} else {
			lockManager.glock.Unlock()
			self.DoAckLock(waitLock, false)
		}
		return
	}

	waitLock.timeouted = true
	if waitLock.longWaitIndex > 0 {
		self.RemoveLongTimeOut(waitLock)
	}

	if waitLock.command.Expried > 0 {
		lockManager.AddLock(waitLock)
		lockManager.locked++

		if waitLock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
			self.AddExpried(waitLock, waitLock.expriedTime)
		} else {
			self.AddMillisecondExpried(waitLock)
		}
		waitLock.refCount++
		waitLockProtocol, waitLockCommand := waitLock.protocol, waitLock.command
		lockManager.state.LockCount++
		lockManager.state.LockedCount++
		lockManager.state.WaitCount--
		lockManager.glock.Unlock()

		if waitLockProtocol.serverProtocol == serverProtocol {
			_ = serverProtocol.ProcessLockResultCommand(waitLockCommand, protocol.RESULT_SUCCED, uint16(lockManager.locked), waitLock.locked)
		} else {
			_ = waitLockProtocol.ProcessLockResultCommandLocked(waitLockCommand, protocol.RESULT_SUCCED, uint16(lockManager.locked), waitLock.locked)
		}
		return
	}

	waitLockProtocol, waitLockCommand := waitLock.protocol, waitLock.command
	lockManager.state.LockCount++
	lockManager.state.WaitCount--
	if waitLockCommand.ExpriedFlag&protocol.EXPRIED_FLAG_PUSH_SUBSCRIBE != 0 {
		_ = self.subscribeChannels[lockManager.glockIndex].Push(waitLockCommand, protocol.RESULT_EXPRIED, uint16(lockManager.locked), waitLock.locked)
	}
	lockManager.glock.Unlock()

	if waitLockProtocol.serverProtocol == serverProtocol {
		_ = serverProtocol.ProcessLockResultCommand(waitLockCommand, protocol.RESULT_SUCCED, uint16(lockManager.locked), waitLock.locked)
		_ = serverProtocol.FreeLockCommand(waitLockCommand)
	} else {
		_ = waitLockProtocol.ProcessLockResultCommandLocked(waitLockCommand, protocol.RESULT_SUCCED, uint16(lockManager.locked), waitLock.locked)
		_ = waitLockProtocol.FreeLockCommandLocked(waitLockCommand)
	}
}

func (self *LockDB) cancelWaitLock(lockManager *LockManager, command *protocol.LockCommand, serverProtocol ServerProtocol) {
	var waitLock *Lock = nil
	if lockManager.waitLocks != nil {
		for i := range lockManager.waitLocks.IterNodes() {
			nodeQueues := lockManager.waitLocks.IterNodeQueues(int32(i))
			for _, lock := range nodeQueues {
				if lock.timeouted {
					continue
				}

				if lock.command.LockId == command.LockId {
					waitLock = lock
				}
			}
		}
	}

	if waitLock == nil {
		lockManager.state.UnlockErrorCount++
		lockManager.glock.Unlock()

		_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_UNLOCK_ERROR, uint16(lockManager.locked), 0)
		_ = serverProtocol.FreeLockCommand(command)
		return
	}

	lockLocked := waitLock.locked
	waitLock.timeouted = true
	if waitLock.longWaitIndex > 0 {
		self.RemoveLongTimeOut(waitLock)
	}
	lockProtocol, lockCommand := waitLock.protocol, waitLock.command

	if lockLocked > 0 {
		lockManager.locked -= uint32(lockLocked)
		lockManager.RemoveLock(waitLock)
		if waitLock.isAof {
			_ = lockManager.PushUnLockAof(waitLock, nil, false, 0)
		}
	} else {
		if lockManager.GetWaitLock() == nil {
			lockManager.waited = false
		}
		lockManager.state.WaitCount--
	}

	if lockManager.refCount == 0 {
		self.RemoveLockManager(lockManager)
	}
	lockManager.state.UnLockCount++
	lockManager.glock.Unlock()

	_ = serverProtocol.ProcessLockResultCommand(command, protocol.RESULT_LOCKED_ERROR, uint16(lockManager.locked), waitLock.locked)
	_ = serverProtocol.FreeLockCommand(command)
	_ = lockProtocol.ProcessLockResultCommandLocked(lockCommand, protocol.RESULT_UNLOCK_ERROR, uint16(lockManager.locked), waitLock.locked)
	_ = lockProtocol.FreeLockCommandLocked(lockCommand)

	if lockLocked > 0 {
		self.wakeUpWaitLocks(lockManager, nil)
	}
}

func (self *LockDB) DoAckLock(lock *Lock, succed bool) {
	lockManager := lock.manager
	lockManager.glock.Lock()

	if !lock.timeouted {
		lock.timeouted = true
		if lock.longWaitIndex > 0 {
			self.RemoveLongTimeOut(lock)
		}
	}

	if lock.ackCount == 0xff {
		lock.refCount--
		if lock.refCount == 0 {
			lockManager.FreeLock(lock)
			if lockManager.refCount == 0 {
				self.RemoveLockManager(lockManager)
			}
		}
		lockManager.glock.Unlock()
		return
	}

	if succed {
		lock.ackCount = 0xff
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

		if lock.command.ExpriedFlag&protocol.EXPRIED_FLAG_MILLISECOND_TIME == 0 {
			self.AddExpried(lock, lock.expriedTime)
		} else {
			self.AddMillisecondExpried(lock)
		}
		lockProtocol, lockCommand := lock.protocol, lock.command
		lockManager.state.LockCount++
		lockManager.state.LockedCount++
		lockManager.glock.Unlock()

		_ = lockProtocol.ProcessLockResultCommandLocked(lockCommand, protocol.RESULT_SUCCED, uint16(lockManager.locked), lock.locked)
		return
	}

	lockLocked := lock.locked
	lockManager.locked -= uint32(lockLocked)
	lockProtocol, lockCommand := lock.protocol, lock.command
	lockManager.RemoveLock(lock)
	if lock.isAof {
		_ = lockManager.PushUnLockAof(lock, nil, false, 0)
	}

	lock.refCount--
	if lock.refCount == 0 {
		lockManager.FreeLock(lock)
		if lockManager.refCount == 0 {
			self.RemoveLockManager(lockManager)
		}
	}
	lockManager.glock.Unlock()

	_ = lockProtocol.ProcessLockResultCommandLocked(lockCommand, protocol.RESULT_ERROR, uint16(lockManager.locked), lock.locked)
	_ = lockProtocol.FreeLockCommandLocked(lockCommand)

	self.wakeUpWaitLocks(lockManager, nil)
}

func (self *LockDB) HasLock(command *protocol.LockCommand) bool {
	lockManager := self.GetLockManager(command)
	if lockManager == nil {
		return false
	}

	lockManager.glock.Lock()
	for lockManager.lockKey != command.LockKey {
		lockManager.glock.Unlock()
		lockManager = self.GetLockManager(command)
		if lockManager == nil {
			return false
		}
		lockManager.glock.Lock()
	}

	if lockManager.locked == 0 {
		lockManager.glock.Unlock()
		return false
	}
	currentLock := lockManager.GetLockedLock(command)
	if currentLock != nil {
		lockManager.glock.Unlock()
		return true
	}
	lockManager.glock.Unlock()
	return false
}

func (self *LockDB) GetState() *protocol.LockDBState {
	state := protocol.LockDBState{}
	for _, s := range self.states {
		state.LockCount += s.LockCount
		state.UnLockCount += s.UnLockCount
		state.LockedCount += s.LockedCount
		state.KeyCount += s.KeyCount
		state.WaitCount += s.WaitCount
		state.TimeoutedCount += s.TimeoutedCount
		state.ExpriedCount += s.ExpriedCount
		state.UnlockErrorCount += s.UnlockErrorCount
	}
	return &state
}
