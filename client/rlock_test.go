package client

import (
	"github.com/snower/slock/protocol"
	"testing"
)

func TestRLock_LockAndUnLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.RLock(testString2Key("TestRLock"), 5, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("RLock Lock1 Fail %v", err)
			return
		}

		_, err = lock.Lock()
		if err != nil {
			t.Errorf("RLock Lock2 Fail %v", err)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("RLock UnLock1 Fail %v", err)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("RLock UnLock2 Fail %v", err)
			return
		}

		result, err := lock.Unlock()
		if err == nil || (result != nil && result.Result != protocol.RESULT_UNLOCK_ERROR) {
			t.Errorf("RLock UnLock Fail %v", err)
			return
		}
	})
}
