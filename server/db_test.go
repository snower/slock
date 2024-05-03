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
		db.AddExpried(lock)
		if i%10 != 0 {
			db.RemoveLongExpried(lock)
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
		db.RemoveLongExpried(lock)
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
}
