package client

import (
	"testing"
	"time"

	"github.com/snower/slock/protocol"
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

		flow = client.MaxConcurrentFlow(testString2Key("TestMaxConcFlow"), 1, 5, 5)
		flow.SetPriority(10)
		_, err = flow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Acquire Fail %v", err)
			return
		}

		checkFlow = client.MaxConcurrentFlow(testString2Key("TestMaxConcFlow"), 1, 0, 5)
		checkFlow.SetPriority(10)
		result, err = checkFlow.Acquire()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("MaxConcurrentFlow Check Acquire Fail %v", err)
			return
		}

		_, err = flow.Release()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Release Fail %v", err)
			return
		}

		recheckFlow = client.MaxConcurrentFlow(testString2Key("TestMaxConcFlow"), 1, 5, 0)
		recheckFlow.SetPriority(10)
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

		flow = client.TokenBucketFlow(testString2Key("TestTokBucFlow"), 1, 5, 0.1)
		flow.SetPriority(10)
		_, err = flow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Acquire Fail %v", err)
			return
		}

		checkFlow = client.TokenBucketFlow(testString2Key("TestTokBucFlow"), 1, 0, 0.1)
		checkFlow.SetPriority(10)
		result, err = checkFlow.Acquire()
		if err == nil || (result != nil && result.Result != protocol.RESULT_TIMEOUT) {
			t.Errorf("MaxConcurrentFlow Check Acquire Fail %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond)
		recheckFlow = client.TokenBucketFlow(testString2Key("TestTokBucFlow"), 1, 5, 0.1)
		recheckFlow.SetPriority(10)
		_, err = recheckFlow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Recheck Acquire Fail %v", err)
			return
		}
	})
}
