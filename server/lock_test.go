package server

import (
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

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
		lockCommand.Data = protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_LOCK)
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
		lockCommand.Data = protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_LOCK)
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
			protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_LOCK),
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
			protocol.NewLockCommandDataExecuteData(executeLockCommand, protocol.LOCK_DATA_STAGE_LOCK),
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
