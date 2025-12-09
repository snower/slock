package client

import (
	"testing"
	"time"
)

func checkChildTreeLock(t *testing.T, client *Client, rootLock *TreeLock, childLock *TreeLock, lock *TreeLeafLock, depth int) {
	clock1 := childLock.NewLeafLock()
	_, err := clock1.Lock()
	if err != nil {
		t.Errorf("TreeLock Child Lock1 Fail %v", err)
		return
	}
	clock2 := childLock.NewLeafLock()
	_, err = clock2.Lock()
	if err != nil {
		t.Errorf("TreeLock Child Lock2 Fail %v", err)
		return
	}

	testLock := client.Lock(rootLock.GetLockKey(), 0, 0)
	_, err = testLock.Lock()
	if err == nil {
		t.Errorf("TreeLock Test childLock Locked Root Lock Fail %v", err)
		return
	}
	testLock = client.Lock(childLock.GetLockKey(), 0, 0)
	_, err = testLock.Lock()
	if err == nil {
		t.Errorf("TreeLock Test childLock Locked Child Lock Fail %v", err)
		return
	}

	_, err = lock.Unlock()
	if err != nil {
		t.Errorf("TreeLock Root UnLock Fail %v", err)
		return
	}

	testLock = client.Lock(rootLock.GetLockKey(), 0, 0)
	_, err = testLock.Lock()
	if err == nil {
		t.Errorf("TreeLock Test Root Unlocked Root Lock Fail %v", err)
		return
	}
	testLock = client.Lock(childLock.GetLockKey(), 0, 0)
	_, err = testLock.Lock()
	if err == nil {
		t.Errorf("TreeLock Test Root Unlocked childLock Locked Child Lock Fail %v", err)
		return
	}

	if depth-1 > 0 {
		_, _ = lock.Lock()
		checkChildTreeLock(t, client, childLock, childLock.NewChild(), lock, depth-1)
		_, _ = lock.Lock()
		checkChildTreeLock(t, client, childLock, childLock.NewChild(), lock, depth-1)
	}

	_, err = clock1.Unlock()
	if err != nil {
		t.Errorf("TreeLock Child UnLock1 Fail %v", err)
		return
	}

	testLock = client.Lock(rootLock.GetLockKey(), 0, 0)
	_, err = testLock.Lock()
	if err == nil {
		t.Errorf("TreeLock Test childLock Unlocked Root Lock Fail %v", err)
		return
	}
	testLock = client.Lock(childLock.GetLockKey(), 0, 0)
	_, err = testLock.Lock()
	if err == nil {
		t.Errorf("TreeLock Test childLock Unlocked childLock Locked Child Lock Fail %v", err)
		return
	}

	_, err = clock2.Unlock()
	if err != nil {
		t.Errorf("TreeLock Child UnLock2 Fail %v", err)
		return
	}

	testLock = client.Lock(childLock.GetLockKey(), 1, 0)
	_, err = testLock.Lock()
	if err != nil {
		t.Errorf("TreeLock Test Child Lock Fail %v", err)
		return
	}
}

func TestTreeLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		rootLock := client.TreeLock(testString2Key("TestTreeLock"), RootKey, 5, 10)

		_, err := rootLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Lock Fail %v", err)
			return
		}
		_, err = rootLock.Unlock()
		if err != nil {
			t.Errorf("TreeLock UnLock Fail %v", err)
			return
		}
		_, err = rootLock.Wait(10)
		if err != nil {
			t.Errorf("TreeLock Wait Fail %v", err)
			return
		}

		lock := rootLock.NewLeafLock()
		_, err = lock.Lock()
		if err != nil {
			t.Errorf("TreeLock Root Lock Fail %v", err)
			return
		}

		go func() {
			_, _ = rootLock.Wait(10)
		}()
		time.Sleep(100 * time.Millisecond)
		checkChildTreeLock(t, client, rootLock, rootLock.NewChild(), lock, 5)

		_, err = rootLock.Wait(10)
		if err != nil {
			t.Errorf("TreeLock Wait Fail %v", err)
			return
		}

		testLock := client.Lock(rootLock.GetLockKey(), 1, 0)
		_, err = testLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Test Root Lock Fail %v", err)
			return
		}
	})
}
