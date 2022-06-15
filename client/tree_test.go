package client

import (
	"testing"
)

func TestTreeLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		rootLock := client.TreeLock(testString2Key("TestTreeLock"), RootKey, 5, 10)
		lock := rootLock.NewLock()
		err := lock.Lock()
		if err != nil {
			t.Errorf("TreeLock Root Lock Fail %v", err)
			return
		}

		childLock := rootLock.NewChild()
		clock1 := childLock.NewLock()
		err = clock1.Lock()
		if err != nil {
			t.Errorf("TreeLock Child Lock1 Fail %v", err)
			return
		}
		clock2 := childLock.NewLock()
		err = clock2.Lock()
		if err != nil {
			t.Errorf("TreeLock Child Lock2 Fail %v", err)
			return
		}

		testLock := client.Lock(rootLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err == nil {
			t.Errorf("TreeLock Test childLock Locked Root Lock Fail %v", err)
			return
		}
		testLock = client.Lock(childLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err == nil {
			t.Errorf("TreeLock Test childLock Locked Child Lock Fail %v", err)
			return
		}

		err = lock.Unlock()
		if err != nil {
			t.Errorf("TreeLock Root UnLock Fail %v", err)
			return
		}

		testLock = client.Lock(rootLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err == nil {
			t.Errorf("TreeLock Test Root Unlocked Root Lock Fail %v", err)
			return
		}
		testLock = client.Lock(childLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err == nil {
			t.Errorf("TreeLock Test Root Unlocked childLock Locked Child Lock Fail %v", err)
			return
		}

		err = clock1.Unlock()
		if err != nil {
			t.Errorf("TreeLock Child UnLock1 Fail %v", err)
			return
		}

		testLock = client.Lock(rootLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err == nil {
			t.Errorf("TreeLock Test childLock Unlocked Root Lock Fail %v", err)
			return
		}
		testLock = client.Lock(childLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err == nil {
			t.Errorf("TreeLock Test childLock Unlocked childLock Locked Child Lock Fail %v", err)
			return
		}

		err = clock2.Unlock()
		if err != nil {
			t.Errorf("TreeLock Child UnLock2 Fail %v", err)
			return
		}

		testLock = client.Lock(rootLock.GetLockKey(), 1, 0)
		err = testLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Test Root Lock Fail %v", err)
			return
		}

		testLock = client.Lock(childLock.GetLockKey(), 1, 0)
		err = testLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Test Child Lock Fail %v", err)
			return
		}
	})
}
