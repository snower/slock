package client

import (
	"testing"
)

func TestTreeLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		rootLock := client.TreeLock(testString2Key("TestTreeLock"), RootKey, 5, 10)
		err := rootLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Root Lock Fail %v", err)
			return
		}

		childLock, cerr := rootLock.NewChild()
		if cerr != nil {
			t.Errorf("TreeLock NewChild Lock Fail %v", cerr)
			return
		}
		err = childLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Child Lock Fail %v", err)
			return
		}

		err = childLock.Unlock()
		if err != nil {
			t.Errorf("TreeLock Child UnLock Fail %v", err)
			return
		}

		testLock := client.Lock(rootLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Test Root Lock Fail %v", err)
			return
		}

		testLock = client.Lock(childLock.GetLockKey(), 0, 0)
		err = testLock.Lock()
		if err != nil {
			t.Errorf("TreeLock Test Child Lock Fail %v", err)
			return
		}
	})
}
