package client

import (
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

func TestLock_LockAndUnLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockUnLock"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}

		relock := client.Lock(testString2Key("TestLockUnLock"), 5, 5)
		err = relock.Lock()
		if err != nil {
			t.Errorf("Lock ReLock Fail %v", err)
			return
		}

		err = relock.Unlock()
		if err != nil {
			t.Errorf("Lock ReUnlock Fail %v", err)
			return
		}
	})
}

func TestLock_LockUpdate(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockUpdate"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		updateLock := client.Lock(testString2Key("TestLockUpdate"), 5, 5)
		updateLock.lockId = lock.lockId
		err = updateLock.LockUpdate()
		if err.CommandResult.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("Lock Lock Update Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_LockShow(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockShow"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		showLock := client.Lock(testString2Key("TestLockShow"), 5, 5)
		err = showLock.LockShow()
		if err.CommandResult.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("Lock Show Fail %v", err)
			return
		}

		if err.CommandResult.LockId != lock.lockId {
			t.Errorf("Lock Show LockId Error %x %x", err.CommandResult.LockId, lock.lockId)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_UnLockHead(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestUnLockHead"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		checkLock := client.Lock(testString2Key("TestUnLockHead"), 0, 5)
		err = checkLock.Lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		unlockHeadLock := client.Lock(testString2Key("TestUnLockHead"), 5, 5)
		err = unlockHeadLock.UnlockHead()
		if err.CommandResult.Result != 0 {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}

		relock := client.Lock(testString2Key("TestUnLockHead"), 5, 5)
		err = relock.Lock()
		if err != nil {
			t.Errorf("Lock ReLock Fail %v", err)
			return
		}

		err = relock.Unlock()
		if err != nil {
			t.Errorf("Lock ReUnlock Fail %v", err)
			return
		}
	})
}

func TestLock_CancelWait(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestCancelWait"), 5, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		waitLock := client.Lock(testString2Key("TestCancelWait"), 5, 0)
		go func() {
			err = waitLock.Lock()
			if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
				t.Errorf("Lock Wait Cancel Lock Fail %v", err)
				return
			}
		}()
		time.Sleep(10 * time.Millisecond)

		err = waitLock.CancelWait()
		if err.CommandResult.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("Lock Cancel Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		time.Sleep(20 * time.Millisecond)
	})
}

func TestLock_LockCount(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestLockCount"), 0, 5)
		lock1.SetCount(2)
		err := lock1.Lock()
		if err != nil {
			t.Errorf("Lock Lock1 Fail %v", err)
			return
		}

		lock2 := client.Lock(testString2Key("TestLockCount"), 0, 5)
		lock2.SetCount(2)
		err = lock2.Lock()
		if err != nil {
			t.Errorf("Lock Lock2 Fail %v", err)
			return
		}

		lock3 := client.Lock(testString2Key("TestLockCount"), 0, 5)
		lock3.SetCount(2)
		err = lock3.Lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock Lock3 Fail %v", err)
			return
		}

		showLock := client.Lock(testString2Key("TestLockCount"), 5, 5)
		err = showLock.LockShow()
		if err.CommandResult.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("Lock Show Fail %v", err)
			return
		}
		if err.CommandResult.Lcount != 2 {
			t.Errorf("Lock Count Fail %v", err.CommandResult.Lcount)
			return
		}

		err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock1 Fail %v", err)
			return
		}

		err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock2 Fail %v", err)
			return
		}

		err = lock3.Unlock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
			t.Errorf("Lock Unlock3 Fail %v", err)
			return
		}
	})
}

func TestLock_LockRCount(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockRCount"), 0, 5)
		lock.SetRcount(2)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		err = lock.Lock()
		if err != nil {
			t.Errorf("Lock ReLock Fail %v", err)
			return
		}

		err = lock.Lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		showLock := client.Lock(testString2Key("TestLockRCount"), 5, 5)
		err = showLock.LockShow()
		if err.CommandResult.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("Lock Show Fail %v", err)
			return
		}
		if err.CommandResult.Lrcount != 2 {
			t.Errorf("Lock Count Fail %v", err.CommandResult.Lrcount)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock ReUnlock Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
			t.Errorf("Lock Check Unlock Fail %v", err)
			return
		}

		lock = client.Lock(testString2Key("TestLockRCount"), 0, 5)
		lock.SetRcount(2)
		err = lock.Lock()
		if err != nil {
			t.Errorf("Lock Check All Lock Fail %v", err)
			return
		}

		err = lock.Lock()
		if err != nil {
			t.Errorf("Lock Check All ReLock Fail %v", err)
			return
		}

		lock.SetRcount(0)
		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Check All Unlock Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_UNLOCK_ERROR {
			t.Errorf("Lock Check All Check Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ZeroTimeout(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestZeroTimeout"), 0, 5)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		err = lock.Lock()
		if err == nil || err.CommandResult.Result == protocol.RESULT_TIMEOUT {
			t.Errorf("Lock Check Lock Timeout Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ZeroExpried(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestZeroExpried"), 0, 0)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		err = lock.Lock()
		if err != nil {
			t.Errorf("Lock ReLock Fail %v", err)
			return
		}
	})
}

func TestLock_UnlockWait(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestUnlockWait"), 0, 5)
		lock.SetTimeoutFlag(protocol.TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK)
		err := lock.Lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}
	})
}

func TestLock_TimeoutTeverseKeyLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestTimeoutRKL"), 10, 5)
		lock.SetTimeoutFlag(protocol.TIMEOUT_FLAG_MILLISECOND_TIME | protocol.TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK | protocol.TIMEOUT_FLAG_REVERSE_KEY_LOCK_WHEN_TIMEOUT)
		err := lock.Lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		lockKey := [16]byte{}
		for i := 0; i < 16; i++ {
			lockKey[i] = lock.lockKey[15-i]
		}
		lock = client.Lock(lockKey, 0, 5)
		time.Sleep(20 * time.Millisecond)
		err = lock.Lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		err = lock.UnlockHead()
		if err.CommandResult.Result != 0 {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ExpriedReverseKeyLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestExpriedRKL"), 50, 10)
		lock.SetExpriedFlag(protocol.EXPRIED_FLAG_MILLISECOND_TIME | protocol.EXPRIED_FLAG_REVERSE_KEY_LOCK_WHEN_EXPRIED)
		err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		lockKey := [16]byte{}
		for i := 0; i < 16; i++ {
			lockKey[i] = lock.lockKey[15-i]
		}
		lock = client.Lock(lockKey, 0, 5)
		time.Sleep(20 * time.Millisecond)
		err = lock.Lock()
		if err == nil || err.CommandResult.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		err = lock.UnlockHead()
		if err.CommandResult.Result != 0 {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}
