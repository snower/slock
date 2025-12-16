package server

import (
	"math/rand"
	"testing"
	"time"

	"github.com/snower/slock/protocol"
)

func TestLockManagerRingQueue(t *testing.T) {
	queue := NewLockManagerRingQueue(4)

	lock := &Lock{}
	queue.Push(lock)
	if queue.Head() != lock || queue.Pop() != lock || queue.index != 0 {
		t.Errorf("LockManagerRingQueue Push Pop fail")
		return
	}

	for i := 0; i < 4; i++ {
		queue.Push(lock)
		if len(queue.queue) != i+1 || cap(queue.queue) != 4 {
			t.Errorf("LockManagerRingQueue Push Size fail")
			return
		}
	}
	for i := 0; i < 3; i++ {
		if queue.Pop() != lock || queue.index != i+1 {
			t.Errorf("LockManagerRingQueue Pop fail")
			return
		}
	}
	queue.Push(lock)
	if len(queue.queue) != 2 || cap(queue.queue) != 4 || queue.index != 0 {
		t.Errorf("LockManagerRingQueue Push Size fail")
		return
	}
	for i := 0; i < 2; i++ {
		queue.Push(lock)
		if len(queue.queue) != i+3 || cap(queue.queue) != 4 {
			t.Errorf("LockManagerRingQueue Push Size fail")
			return
		}
	}
	for i := 0; i < 2; i++ {
		queue.Push(lock)
		if len(queue.queue) != i+5 || cap(queue.queue) != 8 {
			t.Errorf("LockManagerRingQueue Push Size fail")
			return
		}
	}
	for i := 0; i < 5; i++ {
		if queue.Pop() != lock || queue.index != i+1 || cap(queue.queue) != 8 {
			t.Errorf("LockManagerRingQueue Pop fail")
			return
		}
	}
	if queue.Head() != lock || queue.Pop() != lock || queue.index != 0 || cap(queue.queue) != 8 || queue.Head() != nil {
		t.Errorf("LockManagerRingQueue Pop fail")
		return
	}

	for i := 0; i < 1000000; i++ {
		queue.Push(lock)
	}
	if len(queue.queue) != 1000000 || cap(queue.queue) < 1000000 {
		t.Errorf("LockManagerRingQueue Push Size fail")
		return
	}
	capSize := cap(queue.queue)
	for i := 0; i < 1000000; i++ {
		if queue.Pop() != lock {
			t.Errorf("LockManagerRingQueue Pop fail")
			return
		}
	}
	if len(queue.queue) != 0 || cap(queue.queue) != capSize || queue.index != 0 {
		t.Errorf("LockManagerRingQueue Pop Size fail")
		return
	}
}

func TestLockManagerPriorityRingQueue(t *testing.T) {
	queue := NewLockManagerPriorityRingQueue(4)

	lock := &Lock{command: &protocol.LockCommand{TimeoutFlag: protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY, Rcount: 1}}
	queue.Push(lock)
	if queue.Head() != lock || queue.Pop() != lock || len(queue.priorityNodes) != 1 || queue.priorityNodes[0].priority != 1 || queue.priorityNodes[0].ringQueue.index != 0 {
		t.Errorf("LockManagerPriorityRingQueue Push Pop fail")
		return
	}

	lock1 := &Lock{command: &protocol.LockCommand{TimeoutFlag: protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY, Rcount: 2}}
	queue.Push(lock1)
	lock2 := &Lock{command: &protocol.LockCommand{TimeoutFlag: protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY, Rcount: 1}}
	queue.Push(lock2)
	if len(queue.priorityNodes) != 2 || queue.priorityNodes[0].priority != 1 || queue.priorityNodes[1].priority != 2 {
		t.Errorf("LockManagerPriorityRingQueue Push Priority fail")
		return
	}
	if queue.Head() != lock2 || queue.Pop() != lock2 || len(queue.priorityNodes) != 2 || queue.priorityNodes[0].priority != 1 || queue.priorityNodes[0].ringQueue.index != 0 {
		t.Errorf("LockManagerPriorityRingQueue Push Pop fail")
		return
	}
	if queue.Head() != lock1 || queue.Pop() != lock1 || len(queue.priorityNodes) != 2 || queue.priorityNodes[1].priority != 2 || queue.priorityNodes[1].ringQueue.index != 0 {
		t.Errorf("LockManagerPriorityRingQueue Push Pop fail")
		return
	}
	if queue.MaxPriority() != 2 {
		t.Errorf("LockManagerPriorityRingQueue MaxPriority fail")
		return
	}

	for i := 0; i < 10000; i++ {
		lock = &Lock{command: &protocol.LockCommand{TimeoutFlag: protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY, Rcount: uint8(rand.Intn(50) + 1)}}
		queue.Push(lock)
	}
	currentPriority, currentMaxPriority, queueMaxPriority := uint8(0), uint8(0), queue.MaxPriority()
	for queue.Head() != nil {
		lock = queue.Pop()
		if lock == nil || lock.command.Rcount < currentPriority {
			t.Errorf("LockManagerPriorityRingQueue Pop fail")
			return
		}
		currentPriority = lock.command.Rcount
		if currentMaxPriority < currentPriority {
			currentMaxPriority = currentPriority
		}
	}
	if currentMaxPriority != queueMaxPriority {
		t.Errorf("LockManagerPriorityRingQueue MaxPriority fail")
		return
	}

	for i := 0; i < 10000; i++ {
		lock = &Lock{command: &protocol.LockCommand{TimeoutFlag: protocol.TIMEOUT_FLAG_RCOUNT_IS_PRIORITY, Rcount: uint8(rand.Intn(50) + 1)}}
		queue.Push(lock)
	}
	currentPriority, currentMaxPriority, queueMaxPriority = uint8(0), uint8(0), queue.MaxPriority()
	for _, node := range queue.priorityNodes {
		if node.priority <= currentPriority {
			t.Errorf("LockManagerPriorityRingQueue priorityNodes fail")
			return
		}
		if node.ringQueue.index != 0 {
			t.Errorf("LockManagerPriorityRingQueue priorityNodes ringQueue fail")
			return
		}
		currentPriority = node.priority
		if currentMaxPriority < currentPriority {
			currentMaxPriority = currentPriority
		}
	}
	if currentMaxPriority != queueMaxPriority {
		t.Errorf("LockManagerPriorityRingQueue MaxPriority fail")
		return
	}
}

func TestLockManagerWaitQueue(t *testing.T) {
	queue := NewLockManagerWaitQueue(false)

	lock := &Lock{}
	queue.Push(lock)
	if queue.Head() != lock || queue.Pop() != lock || queue.fastIndex != 0 {
		t.Errorf("LockManagerWaitQueue Push Pop fail")
		return
	}

	for i := 0; i < 8; i++ {
		queue.Push(lock)
		if len(queue.fastQueue) != i+1 || cap(queue.fastQueue) != 8 {
			t.Errorf("LockManagerWaitQueue Push Size fail")
			return
		}
	}
	for i := 0; i < 7; i++ {
		if queue.Pop() != lock || queue.fastIndex != i+1 {
			t.Errorf("LockManagerWaitQueue Pop fail")
			return
		}
	}
	queue.Push(lock)
	if len(queue.fastQueue) != 2 || cap(queue.fastQueue) != 8 || queue.fastIndex != 0 {
		t.Errorf("LockManagerWaitQueue Push Size fail")
		return
	}
	for i := 0; i < 6; i++ {
		queue.Push(lock)
		if len(queue.fastQueue) != i+3 || cap(queue.fastQueue) != 8 {
			t.Errorf("LockManagerWaitQueue Push Size fail")
			return
		}
	}
	for i := 0; i < 1024; i++ {
		queue.Push(lock)
		ringQueue := queue.ringQueue.(*LockManagerRingQueue)
		if len(queue.fastQueue) != 8 || cap(queue.fastQueue) != 8 || queue.ringQueue == nil || len(ringQueue.queue) != i+1 {
			t.Errorf("LockManagerWaitQueue Push Size fail")
			return
		}
	}
	for i := 0; i < 7; i++ {
		if queue.Pop() != lock || queue.fastIndex != i+1 || cap(queue.fastQueue) != 8 {
			t.Errorf("LockManagerWaitQueue Pop fail")
			return
		}
	}
	if queue.Head() != lock || queue.Pop() != lock || queue.fastIndex != 0 || cap(queue.fastQueue) != 8 || queue.Head() != lock {
		t.Errorf("LockManagerWaitQueue Pop fail")
		return
	}
	for i := 0; i < 1023; i++ {
		if queue.Pop() != lock {
			t.Errorf("LockManagerWaitQueue Pop fail")
			return
		}
	}
	if queue.Head() != lock || queue.Pop() != lock || queue.Head() != nil {
		t.Errorf("LockManagerWaitQueue Pop fail")
		return
	}
	queue.Rellac()
	if queue.fastIndex != 0 || len(queue.fastQueue) != 0 || cap(queue.fastQueue) != 8 || queue.ringQueue != nil {
		t.Errorf("LockManagerWaitQueue Rellac fail")
		return
	}
}

func BenchmarkLockManagerWaitQueue(b *testing.B) {
	lock := &Lock{}
	queue := NewLockManagerWaitQueue(false)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			n := rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Push(lock)
			}
			n = rand.Intn(1024)
			for k := 0; k < n; k++ {
				queue.Pop()
			}
		}
	}
}

func TestLockManager_ProcessLockDataSet(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockKey := protocol.GenLockId()
		lockCommand := protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		lockManager := db.GetOrNewLockManager(lockCommand)
		lock := lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" {
			t.Errorf("LockManager ProcessLockData SetData fail")
			return
		}
		lockManager.ProcessRecoverLockData(lock)
		if lockManager.currentData == nil || lockManager.currentData.commandType != protocol.LOCK_DATA_COMMAND_TYPE_UNSET {
			t.Errorf("LockManager ProcessLockData SetData Recover fail")
			return
		}
		lockManager.FreeLock(lock)

		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" {
			t.Errorf("LockManager ProcessLockData SetData fail")
			return
		}
		lockManager.FreeLock(lock)

		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("bbb")
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "bbb" {
			t.Errorf("LockManager ProcessLockData SetData fail")
			return
		}
		lockManager.ProcessRecoverLockData(lock)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" {
			t.Errorf("LockManager ProcessLockData SetData Recover fail")
			return
		}
		lockManager.FreeLock(lock)

		db.RemoveLockManager(lockManager)
		if lockManager.currentData != nil {
			t.Errorf("LockManager FreeLock fail")
			return
		}
	})
}

func TestLockManager_ProcessLockDataIncr(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockKey := protocol.GenLockId()
		lockCommand := protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataIncrData(2)
		lockManager := db.GetOrNewLockManager(lockCommand)
		lock := lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || lockManager.currentData.GetIncrValue() != 2 {
			t.Errorf("LockManager ProcessLockData IncrData fail")
			return
		}
		lockManager.ProcessRecoverLockData(lock)
		if lockManager.currentData == nil || lockManager.currentData.commandType != protocol.LOCK_DATA_COMMAND_TYPE_UNSET {
			t.Errorf("LockManager ProcessLockData IncrData Recover fail")
			return
		}
		lockManager.FreeLock(lock)

		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataIncrData(-3)
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || lockManager.currentData.GetIncrValue() != -3 {
			t.Errorf("LockManager ProcessLockData IncrData fail")
			return
		}
		lockManager.FreeLock(lock)
		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataIncrData(4)
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || lockManager.currentData.GetIncrValue() != 1 {
			t.Errorf("LockManager ProcessLockData IncrData fail")
			return
		}
		lockManager.ProcessRecoverLockData(lock)
		if lockManager.currentData == nil || lockManager.currentData.GetIncrValue() != -3 {
			t.Errorf("LockManager ProcessLockData IncrData Recover fail")
			return
		}
		lockManager.FreeLock(lock)

		db.RemoveLockManager(lockManager)
		if lockManager.currentData != nil {
			t.Errorf("LockManager FreeLock fail")
			return
		}
	})
}

func TestLockManager_ProcessLockDataAppend(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockKey := protocol.GenLockId()
		lockCommand := protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataAppendString("aaa")
		lockManager := db.GetOrNewLockManager(lockCommand)
		lock := lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" {
			t.Errorf("LockManager ProcessLockData AppendData fail")
			return
		}
		lockManager.ProcessRecoverLockData(lock)
		if lockManager.currentData == nil || lockManager.currentData.commandType != protocol.LOCK_DATA_COMMAND_TYPE_UNSET {
			t.Errorf("LockManager ProcessLockData AppendData Recover fail")
			return
		}
		lockManager.FreeLock(lock)

		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataAppendString("aaa")
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" {
			t.Errorf("LockManager ProcessLockData AppendData fail")
			return
		}
		lockManager.FreeLock(lock)

		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataAppendString("bbb")
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaabbb" {
			t.Errorf("LockManager ProcessLockData AppendData fail")
			return
		}
		lockManager.ProcessRecoverLockData(lock)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" {
			t.Errorf("LockManager ProcessLockData AppendData Recover fail")
			return
		}
		lockManager.FreeLock(lock)

		db.RemoveLockManager(lockManager)
		if lockManager.currentData != nil {
			t.Errorf("LockManager FreeLock fail")
			return
		}
	})
}

func TestLockManager_ProcessLockDataShift(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockKey := protocol.GenLockId()
		lockCommand := protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataAppendString("aaa")
		lockManager := db.GetOrNewLockManager(lockCommand)
		lock := lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" {
			t.Errorf("LockManager ProcessLockData ShiftData fail")
			return
		}
		lockManager.FreeLock(lock)

		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataShiftData(1)
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aa" {
			t.Errorf("LockManager ProcessLockData ShiftData fail")
			return
		}
		lockManager.FreeLock(lock)

		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataShiftData(2)
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "" {
			t.Errorf("LockManager ProcessLockData ShiftData fail")
			return
		}
		lockManager.ProcessRecoverLockData(lock)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aa" {
			t.Errorf("LockManager ProcessLockData ShiftData Recover fail")
			return
		}
		lockManager.FreeLock(lock)

		db.RemoveLockManager(lockManager)
		if lockManager.currentData != nil {
			t.Errorf("LockManager FreeLock fail")
			return
		}
	})
}

func TestLockManager_ProcessLockDataExecute(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockKey, executeLockKey := protocol.GenLockId(), protocol.GenLockId()
		executeLockCommand := protocol.NewLockCommand(db.dbId, executeLockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand := protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_CURRENT)
		lockManager := db.GetOrNewLockManager(lockCommand)
		lock := lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData != nil || lock.data == nil || lock.data.commandDatas == nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager := db.GetLockManager(executeLockCommand)
		if executeLockManager != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		lock.data.aofData = nil
		lockManager.ProcessAckLockData(lock)
		if lock.data != nil {
			t.Errorf("LockManager ProcessAckLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager = db.GetLockManager(executeLockCommand)
		if executeLockManager == nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		if lockManager.currentData != nil || lock.data != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		lockManager.FreeLock(lock)

		executeLockKey = protocol.GenLockId()
		executeLockCommand = protocol.NewLockCommand(db.dbId, executeLockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_CURRENT)
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData != nil || lock.data == nil || lock.data.commandDatas == nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager = db.GetLockManager(executeLockCommand)
		if executeLockManager != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		lock.data.aofData = nil
		lockManager.ProcessRecoverLockData(lock)
		if lock.data != nil {
			t.Errorf("LockManager ProcessAckLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager = db.GetLockManager(executeLockCommand)
		if executeLockManager != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		if lockManager.currentData != nil || lock.data != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		lockManager.FreeLock(lock)

		db.RemoveLockManager(lockManager)
		if lockManager.currentData != nil {
			t.Errorf("LockManager FreeLock fail")
			return
		}
	})
}

func TestLockManager_ProcessLockDataPipeline(t *testing.T) {
	testWithLockDB(t, func(db *LockDB) {
		lockKey, executeLockKey := protocol.GenLockId(), protocol.GenLockId()
		executeLockCommand := protocol.NewLockCommand(db.dbId, executeLockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand := protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataPipelineData([]*protocol.LockCommandData{
			protocol.NewLockCommandDataIncrData(2),
			protocol.NewLockCommandDataSetString("aaa"),
			protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_CURRENT),
		})
		lockManager := db.GetOrNewLockManager(lockCommand)
		lock := lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" || lock.data == nil || lock.data.commandDatas == nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager := db.GetLockManager(executeLockCommand)
		if executeLockManager != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		lock.data.aofData = nil
		lockManager.ProcessAckLockData(lock)
		if lock.data != nil {
			t.Errorf("LockManager ProcessAckLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager = db.GetLockManager(executeLockCommand)
		if executeLockManager == nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" || lock.data != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		lockManager.FreeLock(lock)
		db.RemoveLockManager(lockManager)
		if lockManager.currentData != nil {
			t.Errorf("LockManager FreeLock fail")
			return
		}

		executeLockKey = protocol.GenLockId()
		executeLockCommand = protocol.NewLockCommand(db.dbId, executeLockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand = protocol.NewLockCommand(db.dbId, lockKey, protocol.GenLockId(), 10, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataPipelineData([]*protocol.LockCommandData{
			protocol.NewLockCommandDataIncrData(2),
			protocol.NewLockCommandDataSetString("aaa"),
			protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_CURRENT),
		})
		lockManager = db.GetOrNewLockManager(lockCommand)
		lock = lockManager.GetOrNewLock(defaultServerProtocol, lockCommand)
		lockManager.ProcessLockData(lockCommand, lock, true)
		if lockManager.currentData == nil || string(lockManager.GetLockData()[6:]) != "aaa" || lock.data == nil || lock.data.commandDatas == nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager = db.GetLockManager(executeLockCommand)
		if executeLockManager != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		lock.data.aofData = nil
		lockManager.ProcessRecoverLockData(lock)
		if lock.data != nil {
			t.Errorf("LockManager ProcessAckLockData ExecuteData fail")
			return
		}
		time.Sleep(10 * time.Millisecond)
		executeLockManager = db.GetLockManager(executeLockCommand)
		if executeLockManager != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData Check LockManager fail")
			return
		}
		if lockManager.currentData == nil || lockManager.currentData.commandType != protocol.LOCK_DATA_COMMAND_TYPE_UNSET || lock.data != nil {
			t.Errorf("LockManager ProcessLockData ExecuteData fail")
			return
		}
		lockManager.FreeLock(lock)

		db.RemoveLockManager(lockManager)
		if lockManager.currentData != nil {
			t.Errorf("LockManager FreeLock fail")
			return
		}
	})
}
