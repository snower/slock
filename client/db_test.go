package client

import (
	"github.com/snower/slock/protocol/protobuf"
	"testing"
	"time"
)

func TestDB_ListLocks(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestListLocks"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("DB Lock Fail %v", err)
			return
		}

		result, lerr := client.SelectDB(0).ListLocks(5)
		if lerr != nil {
			t.Errorf("DB ListLocks Fail %v", err)
			return
		}
		if len(result.Locks) == 0 {
			t.Errorf("DB Count Error %v", result)
			return
		}

		var listLock *protobuf.LockDBLock = nil
		for _, l := range result.Locks {
			lockKey := [16]byte{}
			copy(lockKey[:], l.LockKey)
			if lock.lockKey == lockKey {
				listLock = l
			}
		}
		if listLock == nil || listLock.LockedCount != 1 {
			if len(result.Locks) == 0 {
				t.Errorf("DB Not Find Error %v", result)
				return
			}
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("DB Unlock Fail %v", err)
			return
		}
	})
}

func TestDB_ListLockeds(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestListLockeds"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("DB Lock Fail %v", err)
			return
		}

		result, lerr := client.SelectDB(0).ListLockLockeds(lock.lockKey, 5)
		if lerr != nil {
			t.Errorf("DB ListLockLockeds Fail %v", err)
			return
		}
		if len(result.Locks) != 1 {
			t.Errorf("DB Count Error %v", result)
			return
		}

		lockId := [16]byte{}
		copy(lockId[:], result.Locks[0].LockId)
		if lockId != lock.lockId {
			t.Errorf("DB Find Error %v", result)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("DB Unlock Fail %v", err)
			return
		}
	})
}

func TestDB_ListWaits(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestListWaits"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("DB Lock Fail %v", err)
			return
		}

		waitLock := client.Lock(testString2Key("TestListWaits"), 5, 0)
		go func() {
			err := waitLock.Lock()
			if err != nil {
				t.Errorf("DB Wait Lock Fail %v", err)
				return
			}
		}()
		time.Sleep(10 * time.Millisecond)

		result, lerr := client.SelectDB(0).ListLockWaits(lock.lockKey, 5)
		if lerr != nil {
			t.Errorf("DB ListLockWaits Fail %v", err)
			return
		}
		if len(result.Locks) != 1 {
			t.Errorf("DB Count Error %d %v", len(result.Locks), result)
			return
		}

		lockId := [16]byte{}
		copy(lockId[:], result.Locks[0].LockId)
		if lockId != waitLock.lockId {
			t.Errorf("DB Find Error %v", result)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("DB Unlock Fail %v", err)
			return
		}
		time.Sleep(20 * time.Millisecond)
	})
}
