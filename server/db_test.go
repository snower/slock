package server

import (
	"sync"
	"testing"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/snower/slock/protocol"
)

func testWithLockDB(t *testing.T, doTestFunc func(db *LockDB)) {
	serverConfig := &ServerConfig{}
	parse := flags.NewParser(serverConfig, flags.Default)
	_, err := parse.ParseArgs([]string{})
	if err != nil {
		t.Errorf("Init LockDB Fail %v", err)
		return
	}
	logger, _ := InitLogger(serverConfig)
	slock := NewSLock(serverConfig, logger)
	slock.state = STATE_LEADER
	db := NewLockDB(slock, 0)
	defer db.Close()
	doTestFunc(db)
}

func TestLockDB_LockTimeoutLongWait(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockTimeoutTime := time.Now().Unix() + 200
		locks := make([]*Lock, 0)
		for i := 0; i < 100000; i++ {
			command := &protocol.LockCommand{DbId: 0}
			lock := NewLock(NewLockManager(db, command, db.managerGlocks[0], 0, db.freeLocks[0], db.states[0]), defaultServerProtocol, command)
			lock.timeoutCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
			lock.timeoutTime = lockTimeoutTime
			db.AddTimeOut(lock)
			if i%10 != 0 {
				db.RemoveLongTimeOut(lock)
			} else {
				locks = append(locks, lock)
			}
		}

		db.restructuringLongTimeOutQueue(db.longTimeoutLocks[0][lockTimeoutTime])
		if longLocks, ok := db.longTimeoutLocks[0][lockTimeoutTime]; ok {
			if longLocks.Len() != 10000 {
				t.Errorf("longTimeoutLocks Size Error %v", longLocks.Len())
				return
			}
		} else {
			t.Errorf("longTimeoutLocks Is Not Exist")
			return
		}
		db.managerGlocks[0].Lock()
		for _, lock := range locks {
			db.RemoveLongTimeOut(lock)
		}
		db.managerGlocks[0].Unlock()
		if longLocks, ok := db.longTimeoutLocks[0][lockTimeoutTime]; ok {
			t.Errorf("longTimeoutLocks Is Exist %v", longLocks.Len())
			return
		}

		for i := 0; i < 100000; i++ {
			command := &protocol.LockCommand{DbId: 0}
			lock := NewLock(NewLockManager(db, command, db.managerGlocks[1], 1, db.freeLocks[1], db.states[1]), defaultServerProtocol, command)
			if i%10 == 0 {
				lock.timeoutCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
				lock.timeoutTime = lockTimeoutTime
				db.AddTimeOut(lock)
			} else {
				lock.timeoutTime = lockTimeoutTime
				db.AddTimeOut(lock)
			}
		}
		if longLocks, ok := db.longTimeoutLocks[1][lockTimeoutTime]; ok {
			if longLocks.Len() != 10000 {
				t.Errorf("longTimeoutLocks Size Error %v", longLocks.Len())
				return
			}
		} else {
			t.Errorf("longTimeoutLocks Is Not Exist")
			return
		}
		db.flushTimeOut(1, false)
		if longLocks, ok := db.longTimeoutLocks[1][lockTimeoutTime]; ok {
			t.Errorf("longTimeoutLocks Is Exist %v", longLocks.Len())
			return
		}
	})
}

func TestLockDB_LockExpriedLongWait(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockExpriedTime := time.Now().Unix() + 200
		locks := make([]*Lock, 0)
		for i := 0; i < 100000; i++ {
			command := &protocol.LockCommand{DbId: 0}
			lock := NewLock(NewLockManager(db, command, db.managerGlocks[0], 0, db.freeLocks[0], db.states[0]), defaultServerProtocol, command)
			lock.expriedCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
			lock.expriedTime = lockExpriedTime
			db.AddExpried(lock)
			if i%10 != 0 {
				db.RemoveLongExpried(lock, lock.expriedTime)
			} else {
				locks = append(locks, lock)
			}
		}

		db.restructuringLongExpriedQueue(db.longExpriedLocks[0][lockExpriedTime])
		if longLocks, ok := db.longExpriedLocks[0][lockExpriedTime]; ok {
			if longLocks.Len() != 10000 {
				t.Errorf("longExpriedLocks Size Error %v", longLocks.Len())
				return
			}
		} else {
			t.Errorf("longExpriedLocks Is Not Exist")
			return
		}
		db.managerGlocks[0].Lock()
		for _, lock := range locks {
			db.RemoveLongExpried(lock, lock.expriedTime)
		}
		db.managerGlocks[0].Unlock()
		if longLocks, ok := db.longExpriedLocks[0][lockExpriedTime]; ok {
			t.Errorf("longExpriedLocks Is Exist %v", longLocks.Len())
			return
		}

		for i := 0; i < 100000; i++ {
			command := &protocol.LockCommand{DbId: 0}
			lock := NewLock(NewLockManager(db, command, db.managerGlocks[1], 1, db.freeLocks[1], db.states[1]), defaultServerProtocol, command)
			if i%10 == 0 {
				lock.expriedCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
				lock.expriedTime = lockExpriedTime
				db.AddExpried(lock)
			} else {
				lock.expriedTime = lockExpriedTime
				db.AddExpried(lock)
			}
		}
		if longLocks, ok := db.longExpriedLocks[1][lockExpriedTime]; ok {
			if longLocks.Len() != 10000 {
				t.Errorf("longExpriedLocks Size Error %v", longLocks.Len())
				return
			}
		} else {
			t.Errorf("longExpriedLocks Is Not Exist")
			return
		}
		db.flushExpried(1, false)
		if longLocks, ok := db.longExpriedLocks[1][lockExpriedTime]; ok {
			t.Errorf("longExpriedLocks Is Exist %v", longLocks.Len())
			return
		}
	})
}

func TestLockDBExecutorFlushQueueDrainsAllTasks(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		executor := &LockDBExecutor{
			db:            db,
			glock:         db.managerGlocks[0],
			queueLock:     &sync.Mutex{},
			freeTasks:     make([]*LockDBExecutorTask, 4),
			freeTaskMax:   4,
			glockAcquired: true,
			queueCount:    2,
		}

		lockManager1 := NewLockManager(db, &protocol.LockCommand{DbId: 0}, db.managerGlocks[0], 0, db.freeLocks[0], db.states[0])
		lockManager2 := NewLockManager(db, &protocol.LockCommand{DbId: 0}, db.managerGlocks[0], 0, db.freeLocks[0], db.states[0])
		lockManager1.refCount = 1
		lockManager2.refCount = 1
		lockManager1.glock.LowSetPriorityWithNotTraceCount()
		lockManager2.glock.LowSetPriorityWithNotTraceCount()

		task2 := &LockDBExecutorTask{serverProtocol: defaultServerProtocol, command: &protocol.LockCommand{}, lockManager: lockManager2}
		task1 := &LockDBExecutorTask{next: task2, serverProtocol: defaultServerProtocol, command: &protocol.LockCommand{}, lockManager: lockManager1}
		executor.queueTail = task1
		executor.queueHead = task2

		executor.FlushQueue()

		if executor.queueTail != nil || executor.queueHead != nil {
			t.Fatal("expected queue to be fully cleared")
		}
		if executor.queueCount != 0 {
			t.Fatalf("expected queueCount to be 0, got %d", executor.queueCount)
		}
		if executor.freeTaskIndex != 2 {
			t.Fatalf("expected 2 tasks to be returned to free list, got %d", executor.freeTaskIndex)
		}
	})
}

func TestLockDBFreeCollectorCollectShrinksFreePools(t *testing.T) {
	serverConfig := &ServerConfig{DBConcurrent: 1}
	parse := flags.NewParser(serverConfig, flags.Default)
	_, err := parse.ParseArgs([]string{})
	if err != nil {
		t.Fatalf("Init LockDB Fail %v", err)
	}
	logger, _ := InitLogger(serverConfig)
	serverConfig.DBConcurrent = 1
	slock := NewSLock(serverConfig, logger)
	slock.state = STATE_LEADER
	db := NewLockDB(slock, 0)
	db.status = STATE_LEADER
	defer func() {
		db.status = STATE_CLOSE
		db.Close()
	}()

	lockCount := uint64(1)
	db.states[0].LockCount = lockCount
	db.currentTime = time.Now().Unix()

	db.initNewLockManager(0, 0)
	db.initNewLockManager(0, 0)
	db.initNewLockManager(0, 0)
	db.initNewLockManager(0, 0)
	for i := 0; i < 100; i++ {
		_ = db.freeLocks[0].Push(&Lock{})
		db.freeLongWaitQueues[0].FreeLongWaitLockQueue(NewLongWaitLockQueue(4, 64, LONG_LOCKS_QUEUE_INIT_SIZE, 0, 0), db.currentTime-2)
		db.freeMillisecondWaitQueues[0].FreeLockQueue(NewMillisecondWaitLockQueue(4, 64, LONG_LOCKS_QUEUE_INIT_SIZE), db.currentTime-2)
	}

	collector := &LockDBFreeCollector{
		lastCollectTime:          db.currentTime - 1,
		lastLockCount:            0,
		lastLockAvgCount:         10,
		lastFreeLockManagerCount: db.GetFreeLockManagerLen(),
		lastFreeLockCount:        int(db.freeLocks[0].Len()),
	}
	if err := collector.Collect(db); err != nil {
		t.Fatalf("collect failed: %v", err)
	}

	if db.GetFreeLockManagerLen() != 31 {
		t.Fatalf("expected one free lock to be reclaimed, got %d", db.freeLocks[0].Len())
	}
	if db.freeLocks[0].Len() != 95 {
		t.Fatalf("expected one free lock to be reclaimed, got %d", db.freeLocks[0].Len())
	}
	if db.freeLongWaitQueues[0].Len() != 90 {
		t.Fatalf("expected one long-wait queue slot to be reclaimed, got %d", db.freeLongWaitQueues[0].Len())
	}
	if db.freeMillisecondWaitQueues[0].Len() != 90 {
		t.Fatalf("expected one millisecond queue slot to be reclaimed, got %d", db.freeMillisecondWaitQueues[0].Len())
	}
}
