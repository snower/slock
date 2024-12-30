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

func TestLock_WithData_SetUnset(t *testing.T) {
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
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
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
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
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
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "bbb" {
			t.Errorf("Lock LockWithData2 Expried Result LockData Fail %v", result.GetLockData())
			return
		}

		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "ccc" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock1.UnlockWithData(protocol.NewLockCommandDataUnsetData())
		if err != nil {
			t.Errorf("Lock Unlock1 Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "ccc" {
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
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock LockWithData1 Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "bbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_WithData_Incr(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestData3"), 50, 10)
		lock.SetCount(10)
		result, err := lock.LockWithData(protocol.NewLockCommandDataIncrData(2))
		if err != nil {
			t.Errorf("Lock LockWithData Incr Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Incr Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock1 := client.Lock(testString2Key("TestData3"), 50, 0)
		ulock1.SetCount(10)
		result, err = ulock1.LockWithData(protocol.NewLockCommandDataIncrData(-3))
		if err != nil {
			t.Errorf("Lock LockWithData1 Incr Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetIncrValue() != 2 {
			t.Errorf("Lock LockWithData1 Incr Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Incr Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetIncrValue() != -1 {
			t.Errorf("Lock Unlock Incr Result LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_WithData_AppendShift(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestData4"), 50, 10)
		lock.SetCount(10)
		result, err := lock.LockWithData(protocol.NewLockCommandDataAppendString("aaa"))
		if err != nil {
			t.Errorf("Lock LockWithData Append Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Append Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock1 := client.Lock(testString2Key("TestData4"), 50, 10)
		ulock1.SetCount(10)
		result, err = ulock1.LockWithData(protocol.NewLockCommandDataAppendString("bbb"))
		if err != nil {
			t.Errorf("Lock LockWithData1 Append Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock LockWithData1 Append Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.UnlockWithData(protocol.NewLockCommandDataShiftData(2))
		if err != nil {
			t.Errorf("Lock Unlock Append Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaabbb" {
			t.Errorf("Lock Unlock Append Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock1 Append Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "abbb" {
			t.Errorf("Lock Unlock1 Append Result LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_WithData_Execute(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lockKey, lockId := protocol.GenLockId(), protocol.GenLockId()
		lock := client.Lock(testString2Key("TestData5"), 50, 10)
		lockCommand := protocol.NewLockCommand(0, lockKey, lockId, 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err := lock.LockWithData(protocol.NewLockCommandDataExecuteData(lockCommand, protocol.LOCK_DATA_STAGE_UNLOCK))
		if err != nil {
			t.Errorf("Lock LockWithData Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		time.Sleep(200 * time.Millisecond)
		lock = client.Lock(lockKey, 0, 10)
		result, err = lock.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Execute Check Fail %v %v", err, result)
			return
		}
		result, err = lock.UnlockHead()
		if err != nil {
			t.Errorf("Lock Unlock Execute Release Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Execute Release LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_WithData_Pipeline(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lockKey1, lockId1, lockKey2, lockId2 := protocol.GenLockId(), protocol.GenLockId(), protocol.GenLockId(), protocol.GenLockId()
		lock := client.Lock(testString2Key("TestData6"), 50, 10)
		lockCommand1 := protocol.NewLockCommand(0, lockKey1, lockId1, 0, 10, 0)
		lockCommand2 := protocol.NewLockCommand(0, lockKey2, lockId2, 0, 10, 0)
		lockCommand2.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err := lock.LockWithData(protocol.NewLockCommandDataPipelineData([]*protocol.LockCommandData{
			protocol.NewLockCommandDataExecuteData(lockCommand1, protocol.LOCK_DATA_STAGE_UNLOCK),
			protocol.NewLockCommandDataSetString("aaa"),
			protocol.NewLockCommandDataExecuteData(lockCommand2, protocol.LOCK_DATA_STAGE_UNLOCK),
		}))
		if err != nil {
			t.Errorf("Lock LockWithData Pipeline Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Pipeline Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Pipeline Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Pipeline Result LockData Fail %v", result.GetLockData())
			return
		}
		time.Sleep(200 * time.Millisecond)
		lock = client.Lock(lockKey1, 0, 10)
		result, err = lock.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Pipeline Check Fail %v %v", err, result)
			return
		}
		result, err = lock.UnlockHead()
		if err != nil {
			t.Errorf("Lock Unlock Pipeline Release Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Pipeline Release LockData Fail %v", result.GetLockData())
			return
		}
		lock = client.Lock(lockKey2, 0, 10)
		result, err = lock.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Pipeline Check Fail %v %v", err, result)
			return
		}
		result, err = lock.UnlockHead()
		if err != nil {
			t.Errorf("Lock Unlock Pipeline Release Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Pipeline Release LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_WithData_PushPop(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("TestData_PushPop1"), 50, 10)
		lock.SetCount(10)
		result, err := lock.LockWithData(protocol.NewLockCommandDataPushString("aaa"))
		if err != nil {
			t.Errorf("Lock LockWithData Push Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Push Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock1 := client.Lock(testString2Key("TestData_PushPop1"), 50, 10)
		ulock1.SetCount(10)
		result, err = ulock1.LockWithData(protocol.NewLockCommandDataPushString("bbb"))
		if err != nil {
			t.Errorf("Lock LockWithData1 Push Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock LockWithData1 Push Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		values := result.GetLockData().GetArrayValue()
		if values == nil || len(values) != 1 && string(values[0]) != "aaa" {
			t.Errorf("Lock LockWithData1 Push Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock2 := client.Lock(testString2Key("TestData_PushPop1"), 50, 10)
		ulock2.SetCount(10)
		result, err = ulock2.LockWithData(protocol.NewLockCommandDataPushString("ccc"))
		if err != nil {
			t.Errorf("Lock LockWithData1 Push Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock LockWithData1 Push Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		values = result.GetLockData().GetArrayValue()
		if values == nil || len(values) != 2 && string(values[0]) != "aaa" && string(values[1]) != "bbb" {
			t.Errorf("Lock LockWithData1 Push Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.UnlockWithData(protocol.NewLockCommandDataPopData(1))
		if err != nil {
			t.Errorf("Lock Unlock Push Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock Push Result LockData Fail %v", result.GetLockData())
			return
		}
		values = result.GetLockData().GetArrayValue()
		if values == nil || len(values) != 3 && string(values[0]) != "aaa" && string(values[1]) != "bbb" && string(values[1]) != "ccc" {
			t.Errorf("Lock Unlock Push Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock1.UnlockWithData(protocol.NewLockCommandDataPopData(2))
		if err != nil {
			t.Errorf("Lock Unlock1 Push Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock1 Push Result LockData Fail %v", result.GetLockData())
			return
		}
		values = result.GetLockData().GetArrayValue()
		if values == nil || len(values) != 2 && string(values[1]) != "bbb" && string(values[1]) != "ccc" {
			t.Errorf("Lock Unlock Push Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock1 Push Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock1 Push Result LockData Fail %v", result.GetLockData())
			return
		}
		values = result.GetLockData().GetArrayValue()
		if values == nil || len(values) != 0 {
			t.Errorf("Lock Unlock Push Result LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_WithData_SetWithProperty(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestDataSetProperty"), 50, 10)
		lock1.SetCount(10)
		lock2 := client.Lock(testString2Key("TestDataSetProperty"), 50, 10)
		lock2.SetCount(10)
		result, err := lock1.LockWithData(protocol.NewLockCommandDataSetStringWithProperty("aaa", []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("bbbb"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithDataDataProperty Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.LockWithData(protocol.NewLockCommandDataSetStringWithProperty("bbb", []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("cccc"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "bbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		property := result.GetLockData().GetDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY)
		if property == nil || property.GetValueString() != "cccc" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_WithData_IncrWithProperty(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestDataIncrProperty"), 50, 10)
		lock1.SetCount(10)
		lock2 := client.Lock(testString2Key("TestDataIncrProperty"), 50, 10)
		lock2.SetCount(10)
		result, err := lock1.LockWithData(protocol.NewLockCommandDataIncrDataWithProperty(10, []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("bbbb"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithDataDataProperty Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.LockWithData(protocol.NewLockCommandDataIncrDataWithProperty(-12, []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("cccc"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetIncrValue() != 10 {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetIncrValue() != -2 {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		property := result.GetLockData().GetDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY)
		if property == nil || property.GetValueString() != "cccc" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_WithData_AppendWithProperty(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestDataAppendProperty"), 50, 10)
		lock1.SetCount(10)
		lock2 := client.Lock(testString2Key("TestDataAppendProperty"), 50, 10)
		lock2.SetCount(10)
		result, err := lock1.LockWithData(protocol.NewLockCommandDataAppendStringWithProperty("aaa", []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("bbbb"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithDataDataProperty Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.LockWithData(protocol.NewLockCommandDataAppendStringWithProperty("bbb", []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("cccc"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaabbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		property := result.GetLockData().GetDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY)
		if property == nil || property.GetValueString() != "bbbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_WithData_PushWithProperty(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestDataPushProperty"), 50, 10)
		lock1.SetCount(10)
		lock2 := client.Lock(testString2Key("TestDataPushProperty"), 50, 10)
		lock2.SetCount(10)
		result, err := lock1.LockWithData(protocol.NewLockCommandDataPushStringWithProperty("aaa", []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("bbbb"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithDataDataProperty Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.LockWithData(protocol.NewLockCommandDataPushStringWithProperty("bbb", []*protocol.LockCommandDataProperty{
			protocol.NewLockCommandDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY, []byte("cccc"))}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		values := result.GetLockData().GetArrayValue()
		if values == nil || len(values) != 1 && string(values[0]) != "aaa" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		values = result.GetLockData().GetArrayValue()
		if values == nil || len(values) != 2 && string(values[0]) != "aaa" && string(values[0]) != "bbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		property := result.GetLockData().GetDataProperty(protocol.LOCK_DATA_PROPERTY_CODE_KEY)
		if property == nil || property.GetValueString() != "bbbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_WithData_SetArray(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestDataArraySet"), 50, 10)
		lock1.SetCount(10)
		lock2 := client.Lock(testString2Key("TestDataArraySet"), 50, 10)
		lock2.SetCount(10)
		result, err := lock1.LockWithData(protocol.NewLockCommandDataSetArray([][]byte{[]byte("aaa"), []byte("bbb")}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithDataDataProperty Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.LockWithData(protocol.NewLockCommandDataSetArray([][]byte{[]byte("ccc"), []byte("ddd")}))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		valeus := result.GetLockData().GetArrayValue()
		if valeus == nil || len(valeus) != 2 || string(valeus[0]) != "aaa" || string(valeus[1]) != "bbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		valeus = result.GetLockData().GetArrayValue()
		if valeus == nil || len(valeus) != 2 || string(valeus[0]) != "ccc" || string(valeus[1]) != "ddd" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_WithData_SetKV(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestDataKVSet"), 50, 10)
		lock1.SetCount(10)
		lock2 := client.Lock(testString2Key("TestDataKVSet"), 50, 10)
		lock2.SetCount(10)
		kvvalues := make(map[string][]byte)
		kvvalues["aaa"] = []byte("aaa")
		kvvalues["bbb"] = []byte("bbb")
		result, err := lock1.LockWithData(protocol.NewLockCommandDataSetKV(kvvalues))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithDataDataProperty Result LockData Fail %v", result.GetLockData())
			return
		}
		kvvalues = make(map[string][]byte)
		kvvalues["ccc"] = []byte("ccc")
		kvvalues["ddd"] = []byte("ddd")
		result, err = lock2.LockWithData(protocol.NewLockCommandDataSetKV(kvvalues))
		if err != nil {
			t.Errorf("Lock LockWithDataDataProperty Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		kvvalues = result.GetLockData().GetKVValue()
		if kvvalues == nil || len(kvvalues) != 2 || string(kvvalues["aaa"]) != "aaa" || string(kvvalues["bbb"]) != "bbb" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		kvvalues = result.GetLockData().GetKVValue()
		if kvvalues == nil || len(kvvalues) != 2 || string(kvvalues["ccc"]) != "ccc" || string(kvvalues["ddd"]) != "ddd" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
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
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
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
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "bbb" {
			t.Errorf("Lock LockWithData2 Expried Result LockData Fail %v", result.GetLockData())
			return
		}

		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "ccc" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock1.UnlockWithData(protocol.NewLockCommandDataUnsetData())
		if err != nil {
			t.Errorf("Lock Unlock1 Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "ccc" {
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

func TestLock_MultiLockCheckVersion(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestMultiLockCheckVersion"), 0, 10)
		lock1.SetTimeoutFlag(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
		lock1.SetCount(0xffff)
		lock1.lockId = [16]byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0}
		lock2 := client.Lock(testString2Key("TestMultiLockCheckVersion"), 0, 10)
		lock2.SetTimeoutFlag(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
		lock2.SetCount(0xffff)
		lock2.lockId = [16]byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0}
		_, err := lock1.Lock()
		if err != nil {
			t.Errorf("MultiLockCheckVersion Lock Fail %v", err)
			return
		}
		_, err = lock2.Lock()
		if err != nil {
			t.Errorf("MultiLockCheckVersion Lock Fail %v", err)
			return
		}
		_, err = lock1.Unlock()
		if err != nil {
			t.Errorf("MultiLockCheckVersion Unlock Fail %v", err)
			return
		}
		_, err = lock2.Unlock()
		if err != nil {
			t.Errorf("MultiLockCheckVersion Unlock Fail %v", err)
			return
		}

		lock1 = client.Lock(testString2Key("TestMultiLockCheckVersion"), 0, 10)
		lock1.SetTimeoutFlag(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
		lock1.SetCount(0xffff)
		lock1.lockId = [16]byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0}
		lock2 = client.Lock(testString2Key("TestMultiLockCheckVersion"), 0, 10)
		lock2.SetTimeoutFlag(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCED)
		lock2.SetCount(0xffff)
		lock2.lockId = [16]byte{2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0}
		_, err = lock1.Lock()
		if err != nil {
			t.Errorf("MultiLockCheckVersion Lock Fail %v", err)
			return
		}
		result, err := lock2.Lock()
		if err == nil || result == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("MultiLockCheckVersion Lock Fail %v", err)
			return
		}
		_, err = lock1.Unlock()
		if err != nil {
			t.Errorf("MultiLockCheckVersion Unlock Fail %v", err)
			return
		}
	})
}

func TestLock_ProcessLockDataFirstOrLast_Set(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock1 := client.Lock(testString2Key("TestDataFirstOrLast1"), 50, 10)
		result, err := lock1.LockWithData(protocol.NewLockCommandDataFromString("aaa", protocol.LOCK_DATA_STAGE_CURRENT,
			protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST, nil))
		if err != nil {
			t.Errorf("Lock LockWithData Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}

		lock1 = client.Lock(testString2Key("TestDataFirstOrLast2"), 50, 10)
		lock1.SetCount(10)
		result, err = lock1.LockWithData(protocol.NewLockCommandDataFromString("aaa", protocol.LOCK_DATA_STAGE_CURRENT,
			protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST, nil))
		if err != nil {
			t.Errorf("Lock LockWithData Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock1 := client.Lock(testString2Key("TestDataFirstOrLast2"), 50, 10)
		ulock1.SetCount(10)
		result, err = ulock1.LockWithData(protocol.NewLockCommandDataFromString("bbb", protocol.LOCK_DATA_STAGE_CURRENT,
			protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST, nil))
		if err != nil {
			t.Errorf("Lock LockWithData1 Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock LockWithData1 Expried Result LockData Fail %v", result.GetLockData())
			return
		}
		ulock2 := client.Lock(testString2Key("TestDataFirstOrLast2"), 50, 10)
		ulock2.SetCount(10)
		result, err = ulock2.LockWithData(protocol.NewLockCommandDataFromString("ccc", protocol.LOCK_DATA_STAGE_CURRENT,
			protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST, nil))
		if err != nil {
			t.Errorf("Lock LockWithData2 Expried Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock LockWithData2 Expried Result LockData Fail %v", result.GetLockData())
			return
		}

		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock1.UnlockWithData(protocol.NewLockCommandDataFromString("ddd", protocol.LOCK_DATA_STAGE_CURRENT,
			protocol.LOCK_DATA_COMMAND_TYPE_SET, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST, nil))
		if err != nil {
			t.Errorf("Lock Unlock1 Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock1 Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = ulock2.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock2 Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock2 Result LockData Fail %v", result.GetLockData())
			return
		}
	})
}

func TestLock_ProcessLockDataFirstOrLast_Execute(t *testing.T) {
	testWithClient(t, func(client *Client) {

		lockKey1, lockKey2 := protocol.GenLockId(), protocol.GenLockId()
		lock1 := client.Lock(testString2Key("TestDataFirstOrLast3"), 50, 10)
		lockCommand := protocol.NewLockCommand(0, lockKey1, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err := lock1.LockWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_CURRENT, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST))
		if err != nil {
			t.Errorf("Lock LockWithData Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		lockCommand = protocol.NewLockCommand(0, lockKey2, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err = lock1.UnlockWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_CURRENT, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST))
		if err != nil {
			t.Errorf("Lock Unlock Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		time.Sleep(200 * time.Millisecond)
		lock1 = client.Lock(lockKey1, 0, 10)
		result, err = lock1.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Execute Check Fail %v %v", err, result)
			return
		}
		result, err = lock1.UnlockHead()
		if err != nil {
			t.Errorf("Lock Unlock Execute Release Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Execute Release LockData Fail %v", result.GetLockData())
			return
		}
		lock1 = client.Lock(lockKey2, 0, 10)
		result, err = lock1.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Execute Check Fail %v %v", err, result)
			return
		}
		result, err = lock1.UnlockHead()
		if err != nil {
			t.Errorf("Lock Unlock Execute Release Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
			t.Errorf("Lock Unlock Execute Release LockData Fail %v", result.GetLockData())
			return
		}

		lockKey1, lockKey2 = protocol.GenLockId(), protocol.GenLockId()
		lock := client.Lock(testString2Key("TestDataFirstOrLast4"), 50, 10)
		lock.SetCount(10)
		result, err = lock.Lock()
		if err != nil {
			t.Errorf("Lock LockWithData Execute Fail %v", err)
			return
		}
		lock1 = client.Lock(testString2Key("TestDataFirstOrLast4"), 50, 10)
		lock1.SetCount(10)
		lockCommand = protocol.NewLockCommand(0, lockKey1, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err = lock1.LockWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_UNLOCK, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST))
		if err != nil {
			t.Errorf("Lock LockWithData Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		lockCommand = protocol.NewLockCommand(0, lockKey2, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err = lock1.UnlockWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_CURRENT, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST))
		if err != nil {
			t.Errorf("Lock Unlock Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Execute Fail %v", err)
			return
		}
		time.Sleep(200 * time.Millisecond)
		lock1 = client.Lock(lockKey1, 0, 0)
		result, err = lock1.Lock()
		if err != nil && result.Result != protocol.RESULT_SUCCED {
			t.Errorf("Lock LockWithData Execute Check Fail %v %v", err, result)
			return
		}
		lock1 = client.Lock(lockKey2, 0, 0)
		result, err = lock1.Lock()
		if err != nil && result.Result != protocol.RESULT_SUCCED {
			t.Errorf("Lock LockWithData Execute Check Fail %v %v", err, result)
			return
		}

		lockKey1, lockKey2 = protocol.GenLockId(), protocol.GenLockId()
		lock = client.Lock(testString2Key("TestDataFirstOrLast5"), 50, 10)
		lock.SetCount(10)
		result, err = lock.Lock()
		if err != nil {
			t.Errorf("Lock LockWithData Execute Fail %v", err)
			return
		}
		lock1 = client.Lock(testString2Key("TestDataFirstOrLast5"), 50, 10)
		lock1.SetCount(10)
		lockCommand = protocol.NewLockCommand(0, lockKey1, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err = lock1.LockWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_UNLOCK, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST))
		if err != nil {
			t.Errorf("Lock LockWithData Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Execute Fail %v", err)
			return
		}
		lockCommand = protocol.NewLockCommand(0, lockKey2, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err = lock1.UnlockWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_CURRENT, protocol.LOCK_DATA_FLAG_PROCESS_FIRST_OR_LAST))
		if err != nil {
			t.Errorf("Lock Unlock Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		time.Sleep(200 * time.Millisecond)
		lock1 = client.Lock(lockKey1, 0, 0)
		result, err = lock1.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Execute Check Fail %v %v", err, result)
			return
		}
		lock1 = client.Lock(lockKey2, 0, 0)
		result, err = lock1.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Execute Check Fail %v %v", err, result)
			return
		}
	})
}

func TestLock_ProcessLockDataUpdate_Execute(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lockKey1, lockKey2 := protocol.GenLockId(), protocol.GenLockId()
		lock1 := client.Lock(testString2Key("TestDataUpdateExecute1"), 50, 10)
		lockCommand := protocol.NewLockCommand(0, lockKey1, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("aaa")
		result, err := lock1.LockWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_UNLOCK, 0))
		if err != nil {
			t.Errorf("Lock LockWithData Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock LockWithData Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		lockCommand = protocol.NewLockCommand(0, lockKey2, protocol.GenLockId(), 0, 10, 0)
		lockCommand.Data = protocol.NewLockCommandDataSetString("bbb")
		result, err = lock1.LockUpdateWithData(protocol.NewLockCommandDataExecuteDataWithFlag(lockCommand, protocol.LOCK_DATA_STAGE_UNLOCK, 0))
		if err != nil {
			t.Errorf("Lock Unlock Execute Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Execute Result LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Unlock Execute Release Fail %v", err)
			return
		}
		if result.GetLockData() != nil {
			t.Errorf("Lock Unlock Execute Release LockData Fail %v", result.GetLockData())
			return
		}
		lock1 = client.Lock(lockKey1, 0, 0)
		result, err = lock1.Lock()
		if err != nil || result.Result != protocol.RESULT_SUCCED {
			t.Errorf("Lock LockWithData Execute Update Fail %v %v", err, result)
			return
		}
		lock1 = client.Lock(lockKey2, 0, 0)
		result, err = lock1.Lock()
		if err == nil || result.Result != protocol.RESULT_TIMEOUT {
			t.Errorf("Lock LockWithData Unlock Execute Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "bbb" {
			t.Errorf("Lock LockWithData Unlock Execute LockData Fail %v", result.GetLockData())
			return
		}
		result, err = lock1.UnlockHead()
		if err != nil || result.Result != protocol.RESULT_SUCCED {
			t.Errorf("Lock LockWithData UnlockHead Check Fail %v %v", err, result)
			return
		}
	})
}
