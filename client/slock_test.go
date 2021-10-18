package client

import (
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
	lerr := lock.Lock()
	if lerr != nil {
		t.Errorf("Client Lock Fail %v", lerr)
		return
	}

	ulerr := lock.Unlock()
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
	lerr := lock.Lock()
	if lerr != nil {
		t.Errorf("Client Lock Fail %v", lerr)
		return
	}

	ulerr := lock.Unlock()
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
