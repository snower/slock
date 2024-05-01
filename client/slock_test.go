package client

import (
	"github.com/snower/slock/protocol"
	"sync"
	"testing"
)

var testClient *Client = nil
var testGlock sync.Mutex

func testString2Key(key string) [16]byte {
	bkey := [16]byte{}
	klen := 16
	if len(key) < 16 {
		klen = len(key)
	}

	for i := 0; i < klen; i++ {
		bkey[i] = key[i]
	}
	return bkey
}

func testWithClient(t *testing.T, doTestFunc func(client *Client)) {
	testGlock.Lock()
	client := testClient
	testGlock.Unlock()

	if client == nil || client.protocol == nil {
		client = NewClient("127.0.0.1", 5658)
		err := client.Open()
		if err != nil {
			t.Errorf("Client Open Fail %v", err)
			return
		}
	}
	doTestFunc(client)

	testGlock.Lock()
	if testClient != nil {
		testGlock.Unlock()
		_ = client.Close()
		return
	}
	testClient = client
	testGlock.Unlock()
}

func TestClient_Open(t *testing.T) {
	client := NewClient("127.0.0.1", 5658)
	err := client.Open()
	if err != nil {
		t.Errorf("Client Open Fail %v", err)
		return
	}

	lock := client.Lock(testString2Key("testClient"), 5, 5)
	_, lerr := lock.Lock()
	if lerr != nil {
		t.Errorf("Client Lock Fail %v", lerr)
		return
	}

	_, ulerr := lock.Unlock()
	if ulerr != nil {
		t.Errorf("Client UnLock Fail %v", ulerr)
		return
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Client Close Fail %v", err)
		return
	}
}

func TestReplsetClient_Open(t *testing.T) {
	client := NewReplsetClient([]string{"127.0.0.1:5658"})
	err := client.Open()
	if err != nil {
		t.Errorf("Client Open Fail %v", err)
		return
	}

	lock := client.Lock(testString2Key("testReplset"), 5, 5)
	_, lerr := lock.Lock()
	if lerr != nil {
		t.Errorf("Client Lock Fail %v", lerr)
		return
	}

	_, ulerr := lock.Unlock()
	if ulerr != nil {
		t.Errorf("Client UnLock Fail %v", ulerr)
		return
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Client Close Fail %v", err)
		return
	}
}

func TestClient_ListLocks(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.Lock(testString2Key("testList"), 5, 10)
		_, err := lock.LockWithData(protocol.NewLockCommandDataSetString("aaa"))
		if err != nil {
			t.Errorf("TestClient_ListLocks Lock Fail %v", err)
			return
		}

		response, err := client.SelectDB(0).ListLocks(10)
		if err != nil {
			t.Errorf("TestClient_ListLocks ListLocks Fail %v", err)
			return
		}
		if response.Locks == nil || len(response.Locks) != 1 {
			t.Errorf("TestClient_ListLocks ListLocks Empty %v", response.Locks)
			return
		}
		if response.Locks[0].LockData == nil || string(response.Locks[0].LockData.Data) != "aaa" {
			t.Errorf("TestClient_ListLocks ListLocks Data Empty %v", response.Locks)
			return
		}

		lockKey := [16]byte{}
		for i := 0; i < 16; i++ {
			lockKey[i] = response.Locks[0].LockKey[i]
		}
		lockResponse, err := client.SelectDB(0).ListLockLockeds(lockKey, 10)
		if err != nil {
			t.Errorf("TestClient_ListLocks ListLockLockeds Fail %v", err)
			return
		}
		if lockResponse.Locks == nil || len(lockResponse.Locks) != 1 {
			t.Errorf("TestClient_ListLocks ListLockLockeds Empty %v", response.Locks)
			return
		}
		if lockResponse.LockData == nil || string(lockResponse.LockData.Data) != "aaa" {
			t.Errorf("TestClient_ListLocks ListLockLockeds Data Empty %v", response.Locks)
			return
		}

		waitResponse, err := client.SelectDB(0).ListLockWaits(lockKey, 10)
		if err != nil {
			t.Errorf("TestClient_ListLocks ListLockWaits Fail %v", err)
			return
		}
		if waitResponse.Locks != nil && len(waitResponse.Locks) != 1 {
			t.Errorf("TestClient_ListLocks ListLockWaits Not Empty %v", response.Locks)
			return
		}
		if waitResponse.LockData == nil || string(waitResponse.LockData.Data) != "aaa" {
			t.Errorf("TestClient_ListLocks ListLockWaits Data Empty %v", response.Locks)
			return
		}

		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("TestClient_ListLocks Lock Fail %v", err)
			return
		}
	})
}
