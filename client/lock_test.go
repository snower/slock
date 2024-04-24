package client

import (
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

func TestLock_LockAndUnLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockUnLock"), 5, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}

		relock := client.Lock(testString2Key("TestLockUnLock"), 5, 5)
		_, err = relock.Lock()
		if err != nil {
			t.Errorf("Lock ReLock Fail %v", err)
			return
		}

		_, err = relock.Unlock()
		if err != nil {
			t.Errorf("Lock ReUnlock Fail %v", err)
			return
		}
	})
}

func TestLock_LockUpdate(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockUpdate"), 5, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		updateLock := client.Lock(testString2Key("TestLockUpdate"), 5, 5)
		updateLock.lockId = lock.lockId
		result, err := updateLock.LockUpdate()
		if result != nil && result.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("Lock Lock Update Fail %v", err)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_LockShow(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockShow"), 5, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		showLock := client.Lock(testString2Key("TestLockShow"), 5, 5)
		result, err := showLock.LockShow()
		if result != nil && result.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("Lock Show Fail %v", err)
			return
		}
		if result != nil && result.LockId != lock.lockId {
			t.Errorf("Lock Show LockId Error %x %x", result.LockId, lock.lockId)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_UnLockHead(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestUnLockHead"), 5, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		checkLock := client.Lock(testString2Key("TestUnLockHead"), 0, 5)
		result, err := checkLock.Lock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		unlockHeadLock := client.Lock(testString2Key("TestUnLockHead"), 5, 5)
		result, err = unlockHeadLock.UnlockHead()
		if result != nil && result.Result != 0 {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}

		relock := client.Lock(testString2Key("TestUnLockHead"), 5, 5)
		_, err = relock.Lock()
		if err != nil {
			t.Errorf("Lock ReLock Fail %v", err)
			return
		}

		_, err = relock.Unlock()
		if err != nil {
			t.Errorf("Lock ReUnlock Fail %v", err)
			return
		}
	})
}

func TestLock_CancelWait(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestCancelWait"), 5, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		waitLock := client.Lock(testString2Key("TestCancelWait"), 5, 0)
		go func() {
			result, err := waitLock.Lock()
			if err == nil || (result != nil && result.Result != protocol.RESULT_UNLOCK_ERROR) {
				t.Errorf("Lock Wait Cancel Lock Fail %v", err)
				return
			}
		}()
		time.Sleep(10 * time.Millisecond)

		result, err := waitLock.CancelWait()
		if result != nil && result.Result != protocol.RESULT_LOCKED_ERROR {
			t.Errorf("Lock Cancel Fail %v", err)
			return
		}

		_, err = lock.Unlock()
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
		_, err := lock1.Lock()
		if err != nil {
			t.Errorf("Lock Lock1 Fail %v", err)
			return
		}

		lock2 := client.Lock(testString2Key("TestLockCount"), 0, 5)
		lock2.SetCount(2)
		_, err = lock2.Lock()
		if err != nil {
			t.Errorf("Lock Lock2 Fail %v", err)
			return
		}

		lock3 := client.Lock(testString2Key("TestLockCount"), 0, 5)
		lock3.SetCount(2)
		result, err := lock3.Lock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("Lock Lock3 Fail %v", err)
			return
		}

		showLock := client.Lock(testString2Key("TestLockCount"), 5, 5)
		result, err = showLock.LockShow()
		if result != nil && result.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("Lock Show Fail %v", err)
			return
		}
		if result != nil && result.Lcount != 2 {
			t.Errorf("Lock Count Fail %v", result.Lcount)
			return
		}

		_, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock1 Fail %v", err)
			return
		}

		_, err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock2 Fail %v", err)
			return
		}

		result, err = lock3.Unlock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_UNLOCK_ERROR) {
			t.Errorf("Lock Unlock3 Fail %v", err)
			return
		}
	})
}

func TestLock_LockRCount(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestLockRCount"), 0, 5)
		lock.SetRcount(2)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		_, err = lock.Lock()
		if err != nil {
			t.Errorf("Lock ReLock Fail %v", err)
			return
		}

		result, err := lock.Lock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_LOCKED_ERROR) {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		showLock := client.Lock(testString2Key("TestLockRCount"), 5, 5)
		result, err = showLock.LockShow()
		if result != nil && result.Result != protocol.RESULT_UNOWN_ERROR {
			t.Errorf("Lock Show Fail %v", err)
			return
		}
		if result != nil && result.Lrcount != 2 {
			t.Errorf("Lock Count Fail %v", result.Lrcount)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock ReUnlock Fail %v", err)
			return
		}

		result, err = lock.Unlock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_UNLOCK_ERROR) {
			t.Errorf("Lock Check Unlock Fail %v", err)
			return
		}

		lock = client.Lock(testString2Key("TestLockRCount"), 0, 5)
		lock.SetRcount(2)
		_, err = lock.Lock()
		if err != nil {
			t.Errorf("Lock Check All Lock Fail %v", err)
			return
		}

		_, err = lock.Lock()
		if err != nil {
			t.Errorf("Lock Check All ReLock Fail %v", err)
			return
		}

		lock.SetRcount(0)
		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Check All Unlock Fail %v", err)
			return
		}

		result, err = lock.Unlock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_UNLOCK_ERROR) {
			t.Errorf("Lock Check All Check Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ZeroTimeout(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestZeroTimeout"), 0, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		result, err := lock.Lock()
		if err == nil || (result != nil && result.Result == protocol.RESULT_TIMEOUT) {
			t.Errorf("Lock Check Lock Timeout Fail %v", err)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ZeroExpried(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestZeroExpried"), 0, 0)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		_, err = lock.Lock()
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
		result, err := lock.Lock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}
	})
}

func TestLock_TimeoutTeverseKeyLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestTimeoutRKL"), 10, 5)
		lock.SetTimeoutFlag(protocol.TIMEOUT_FLAG_MILLISECOND_TIME | protocol.TIMEOUT_FLAG_LOCK_WAIT_WHEN_UNLOCK | protocol.TIMEOUT_FLAG_REVERSE_KEY_LOCK_WHEN_TIMEOUT)
		result, err := lock.Lock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("Lock Lock Fail %v", err)
			return
		}

		lockKey := [16]byte{}
		for i := 0; i < 16; i++ {
			lockKey[i] = lock.lockKey[15-i]
		}
		lock = client.Lock(lockKey, 0, 5)
		time.Sleep(20 * time.Millisecond)
		result, err = lock.Lock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		result, err = lock.UnlockHead()
		if result != nil && result.Result != 0 {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ExpriedReverseKeyLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestExpriedRKL"), 50, 10)
		lock.SetExpriedFlag(protocol.EXPRIED_FLAG_MILLISECOND_TIME | protocol.EXPRIED_FLAG_REVERSE_KEY_LOCK_WHEN_EXPRIED)
		_, err := lock.Lock()
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
		result, err := lock.Lock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("Lock Check Lock Fail %v", err)
			return
		}

		result, err = lock.UnlockHead()
		if result != nil && result.Result != 0 {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_WithData(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestData"), 50, 10)
		result, err := lock.LockWithData(protocol.NewLockCommandDataSetString("aaa"))
		if err != nil {
			t.Errorf("Lock LockWithData Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "aaa" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}

		lock = client.Lock(testString2Key("TestData1"), 50, 10)
		lock.SetCount(10)
		result, err = lock.LockWithData(protocol.NewLockCommandDataSetString("aaa"))
		if err != nil {
			t.Errorf("Lock LockWithData Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock1 := client.Lock(testString2Key("TestData1"), 50, 10)
		ulock1.SetCount(10)
		result, err = ulock1.LockWithData(protocol.NewLockCommandDataSetString("bbb"))
		if err != nil {
			t.Errorf("Lock LockWithData1 Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "aaa" {
			t.Errorf("Lock LockWithData1 Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock2 := client.Lock(testString2Key("TestData1"), 50, 10)
		ulock2.SetCount(10)
		result, err = ulock2.LockWithData(protocol.NewLockCommandDataSetString("ccc"))
		if err != nil {
			t.Errorf("Lock LockWithData2 Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "bbb" {
			t.Errorf("Lock LockWithData2 Expried Result LockData Fail %v", result.GetLockData())
			return
		}

		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "ccc" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock1.UnlockWithData(protocol.NewLockCommandDataUnsetData())
		if err != nil {
			t.Errorf("Lock Unlock1 Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "ccc" {
			t.Errorf("Lock Unlock1 Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock2 Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock2 Result LockData Fail %v", result.GetLockData())
			return
		}

		lock = client.Lock(testString2Key("TestData2"), 50, 10)
		lock.SetCount(10)
		result, err = lock.LockWithData(protocol.NewLockCommandDataSetString("aaa"))
		if err != nil {
			t.Errorf("Lock LockWithData Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock1 = client.Lock(testString2Key("TestData2"), 50, 0)
		ulock1.SetCount(10)
		result, err = ulock1.LockWithData(protocol.NewLockCommandDataSetString("bbb"))
		if err != nil {
			t.Errorf("Lock LockWithData1 Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "aaa" {
			t.Errorf("Lock LockWithData1 Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "bbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_RequireAck(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestAck"), 50, 10)
		lock.SetTimeoutFlag(protocol.TIMEOUT_FLAG_REQUIRE_ACKED)
		result, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock LockWithData Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}

		lock = client.Lock(testString2Key("TestAck1"), 50, 10)
		lock.SetTimeoutFlag(protocol.TIMEOUT_FLAG_REQUIRE_ACKED)
		lock.SetCount(10)
		result, err = lock.LockWithData(protocol.NewLockCommandDataSetString("aaa"))
		if err != nil {
			t.Errorf("Lock LockWithData Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock1 := client.Lock(testString2Key("TestAck1"), 50, 10)
		lock.SetTimeoutFlag(protocol.TIMEOUT_FLAG_REQUIRE_ACKED)
		ulock1.SetCount(10)
		result, err = ulock1.LockWithData(protocol.NewLockCommandDataSetString("bbb"))
		if err != nil {
			t.Errorf("Lock LockWithData1 Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "aaa" {
			t.Errorf("Lock LockWithData1 Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock2 := client.Lock(testString2Key("TestAck1"), 50, 10)
		lock.SetTimeoutFlag(protocol.TIMEOUT_FLAG_REQUIRE_ACKED)
		ulock2.SetCount(10)
		result, err = ulock2.LockWithData(protocol.NewLockCommandDataSetString("ccc"))
		if err != nil {
			t.Errorf("Lock LockWithData2 Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "bbb" {
			t.Errorf("Lock LockWithData2 Expried Result LockData Fail %v", result.GetLockData())
			return
		}

		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "ccc" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock1.UnlockWithData(protocol.NewLockCommandDataUnsetData())
		if err != nil {
			t.Errorf("Lock Unlock1 Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "ccc" {
			t.Errorf("Lock Unlock1 Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock2 Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock2 Result LockData Fail %v", result.GetLockData())
			return
		}
	})
}
