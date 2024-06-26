package client

import "testing"

func TestRWLock_LockAndUnLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.RWLock(testString2Key("TestRWLock"), 0, 5)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("RWLock Lock Fail %v", err)
			return
		}

		_, err = lock.RLock()
		if err == nil {
			t.Errorf("RWLock RLock Fail %v", err)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("RWLock UnLock Fail %v", err)
			return
		}

		_, err = lock.RLock()
		if err != nil {
			t.Errorf("RWLock RLock Fail %v", err)
			return
		}

		_, err = lock.Lock()
		if err == nil {
			t.Errorf("RWLock Lock Fail %v", err)
			return
		}

		_, err = lock.RUnlock()
		if err != nil {
			t.Errorf("RWLock RUnLock Fail %v", err)
			return
		}
	})
}
