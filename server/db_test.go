package server

import (
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

func TestLockTimeoutLongWait(t *testing.T) {
	serverConfig := &ServerConfig{}
	logger, _ := InitLogger(serverConfig)
	db := NewLockDB(NewSLock(serverConfig, logger), 0)
	defer db.Close()

	lockTimeoutTime := time.Now().Unix() + 200
	locks := make([]*Lock, 0)
	for i := 0; i < 100000; i++ {
		command := &protocol.LockCommand{DbId: 0}
		lock := NewLock(NewLockManager(db, command, db.managerGlocks[0], 0, db.freeLocks[0], db.states[0]), defaultServerProtocol, command)
		lock.timeoutCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
		lock.timeoutTime = lockTimeoutTime
		db.AddTimeOut(lock, lock.timeoutTime)
		if i%10 != 0 {
			db.RemoveLongTimeOut(lock)
		} else {
			locks = append(locks, lock)
		}
	}

	db.restructuringLongTimeOutQueue()
	if longLocks, ok := db.longTimeoutLocks[0][lockTimeoutTime]; ok {
		if longLocks.locks.Len() != 10000 {
			t.Errorf("longExpriedLocks Size Error %v", longLocks.locks.Len())
			return
		}
	} else {
		t.Errorf("longExpriedLocks Is Not Exist")
		return
	}
	db.managerGlocks[0].Lock()
	for _, lock := range locks {
		db.RemoveLongTimeOut(lock)
	}
	db.managerGlocks[0].Unlock()
	db.restructuringLongTimeOutQueue()
	if longLocks, ok := db.longTimeoutLocks[0][lockTimeoutTime]; ok {
		t.Errorf("longExpriedLocks Is Exist %v", longLocks.locks.Len())
		return
	}
}

func TestLockExpriedLongWait(t *testing.T) {
	serverConfig := &ServerConfig{}
	logger, _ := InitLogger(serverConfig)
	db := NewLockDB(NewSLock(serverConfig, logger), 0)
	defer db.Close()

	lockExpriedTime := time.Now().Unix() + 200
	locks := make([]*Lock, 0)
	for i := 0; i < 100000; i++ {
		command := &protocol.LockCommand{DbId: 0}
		lock := NewLock(NewLockManager(db, command, db.managerGlocks[0], 0, db.freeLocks[0], db.states[0]), defaultServerProtocol, command)
		lock.expriedCheckedCount = EXPRIED_QUEUE_MAX_WAIT + 1
		lock.expriedTime = lockExpriedTime
		db.AddExpried(lock, lock.expriedTime)
		if i%10 != 0 {
			db.RemoveLongExpried(lock)
		} else {
			locks = append(locks, lock)
		}
	}

	db.restructuringLongExpriedQueue()
	if longLocks, ok := db.longExpriedLocks[0][lockExpriedTime]; ok {
		if longLocks.locks.Len() != 10000 {
			t.Errorf("longExpriedLocks Size Error %v", longLocks.locks.Len())
			return
		}
	} else {
		t.Errorf("longExpriedLocks Is Not Exist")
		return
	}
	db.managerGlocks[0].Lock()
	for _, lock := range locks {
		db.RemoveLongExpried(lock)
	}
	db.managerGlocks[0].Unlock()
	db.restructuringLongExpriedQueue()
	if longLocks, ok := db.longExpriedLocks[0][lockExpriedTime]; ok {
		t.Errorf("longExpriedLocks Is Exist %v", longLocks.locks.Len())
		return
	}
}
