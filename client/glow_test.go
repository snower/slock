package client

import (
	"testing"
)

func TestFlow_MaxConcurrentFlow(t *testing.T) {
	testWithClient(t, func(client *Client) {
		flow := client.MaxConcurrentFlow(testString2Key("TestMaxConcFlow"), 5, 5, 5)
		err := flow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Acquire Fail %v", err)
			return
		}

		err = flow.Release()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Release Fail %v", err)
			return
		}
	})
}

func TestFlow_TokenBucketFlow(t *testing.T) {
	testWithClient(t, func(client *Client) {
		flow := client.TokenBucketFlow(testString2Key("TestTokBucFlow"), 5, 5, 0.1)
		err := flow.Acquire()
		if err != nil {
			t.Errorf("MaxConcurrentFlow Acquire Fail %v", err)
			return
		}
	})
}
