package client

import (
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

func TestFlow_MaxConcurrentFlow(t *testing.T) {
	testWithClient(t, func(client *Client) {
		flow := client.MaxConcurrentFlow(testString2Key("TestMaxConcFlow"), 1, 5, 5)
		_, err := flow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Acquire Fail %v", err)
			return
		}

		checkFlow := client.MaxConcurrentFlow(testString2Key("TestMaxConcFlow"), 1, 0, 5)
		result, err := checkFlow.Acquire()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("MaxConcurrentFlow Check Acquire Fail %v", err)
			return
		}

		_, err = flow.Release()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Release Fail %v", err)
			return
		}

		recheckFlow := client.MaxConcurrentFlow(testString2Key("TestMaxConcFlow"), 1, 5, 0)
		_, err = recheckFlow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Recheck Acquire Fail %v", err)
			return
		}
	})
}

func TestFlow_TokenBucketFlow(t *testing.T) {
	testWithClient(t, func(client *Client) {
		flow := client.TokenBucketFlow(testString2Key("TestTokBucFlow"), 1, 5, 0.1)
		_, err := flow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Acquire Fail %v", err)
			return
		}

		checkFlow := client.TokenBucketFlow(testString2Key("TestTokBucFlow"), 1, 0, 0.1)
		result, err := checkFlow.Acquire()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("MaxConcurrentFlow Check Acquire Fail %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond)
		recheckFlow := client.TokenBucketFlow(testString2Key("TestTokBucFlow"), 1, 5, 0.1)
		_, err = recheckFlow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Recheck Acquire Fail %v", err)
			return
		}
	})
}
