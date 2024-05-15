package client

import (
	"github.com/snower/slock/protocol"
	"sync"
	"testing"
	"time"
)

func TestGroupEvent_Default(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.GroupEvent(testString2Key("TestGroupEvent"), 1, 1, 5, 5)
		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("Event Check Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("Event Check Seted Status Error %v", err)
			return
		}

		_, err = event.Clear()
		if err != nil {
			t.Errorf("Event Clear Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("Event Clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Clear Seted Status Error %v", err)
			return
		}

		_, err = event.Set()
		if err != nil {
			t.Errorf("Event Set Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("Event Set Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("Event Set Seted Status Error %v", err)
			return
		}

		_, err = event.Clear()
		if err != nil {
			t.Errorf("Event Wait Clear Fail %v", err)
			return
		}

		go func() {
			time.Sleep(20 * time.Millisecond)
			_, err = event.Set()
			if err != nil {
				t.Errorf("Event Wakeup Set Fail %v", err)
				return
			}
		}()

		_, err = event.Wait(60)
		if err != nil {
			t.Errorf("Event Wait Fail %v", err)
			return
		}
	})
}

func TestGroupEvent_Wakeup(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.GroupEvent(testString2Key("TestGroupWakeup"), 1, 1, 5, 5)
		_, err := event.Clear()
		if err != nil {
			t.Errorf("Event Wait Clear Fail %v", err)
			return
		}

		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("Event Clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Clear Seted Status Error %v", isSeted)
			return
		}

		succedCount := 0
		glock := sync.Mutex{}
		for i := 0; i < 4; i++ {
			go func(i int) {
				defer func() {
					glock.Lock()
					succedCount += 1
					glock.Unlock()
				}()
				event := client.GroupEvent(testString2Key("TestGroupWakeup"), uint64(i+2), 1, 5, 5)
				_, err := event.Wait(60)
				if err != nil {
					t.Errorf("Event Wait Fail %v", err)
					return
				}
			}(i)
		}

		time.Sleep(20 * time.Millisecond)
		if succedCount != 0 {
			t.Errorf("Event Group Wait Fail %v", err)
			return
		}
		_, err = event.Wakeup()
		if err != nil {
			t.Errorf("Event Wakeup Set Fail %v", err)
			return
		}
		time.Sleep(time.Second)
		if succedCount != 4 {
			t.Errorf("Event Wakeup Succed Count Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("Event Wakeup Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Wakeup Seted Status Error %v", isSeted)
			return
		}

		event = client.GroupEvent(testString2Key("TestGroupWakeup"), 1, 1, 5, 5)
		_, err = event.Wait(1)
		if err != nil {
			t.Errorf("Event Wait Less Version Fail %v", err)
			return
		}

		_, err = event.Set()
		if err != nil {
			t.Errorf("Event Set Fail %v", err)
			return
		}
	})
}

func TestGroupEvent_DefaultWithData(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.GroupEvent(testString2Key("TestGroupEvent2"), 1, 1, 5, 5)
		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("Event Check Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("Event Check Seted Status Error %v", err)
			return
		}

		_, err = event.Clear()
		if err != nil {
			t.Errorf("Event Clear Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("Event Clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Clear Seted Status Error %v", err)
			return
		}

		_, err = event.SetWithData(protocol.NewLockCommandDataSetString("aaa"))
		if err != nil {
			t.Errorf("Event Set Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("Event Set Seted Fail %v", err)
			return
		}
		if !isSeted {
			t.Errorf("Event Set Seted Status Error %v", err)
			return
		}

		_, err = event.Clear()
		if err != nil {
			t.Errorf("Event Wait Clear Fail %v", err)
			return
		}

		go func() {
			time.Sleep(20 * time.Millisecond)
			_, err = event.SetWithData(protocol.NewLockCommandDataSetString("bbb"))
			if err != nil {
				t.Errorf("Event Wakeup Set Fail %v", err)
				return
			}
		}()

		result, err := event.Wait(60)
		if err != nil {
			t.Errorf("Event Wait Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "bbb" {
			t.Errorf("Event Wait Data Fail %v", result.GetLockData())
			return
		}
	})
}

func TestGroupEvent_WakeupWithData(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.GroupEvent(testString2Key("TestGroupWakeup2"), 1, 1, 5, 5)
		_, err := event.Clear()
		if err != nil {
			t.Errorf("Event Wait Clear Fail %v", err)
			return
		}

		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("Event Clear Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Clear Seted Status Error %v", isSeted)
			return
		}

		succedCount := 0
		glock := sync.Mutex{}
		for i := 0; i < 4; i++ {
			go func(i int) {
				defer func() {
					glock.Lock()
					succedCount += 1
					glock.Unlock()
				}()
				event := client.GroupEvent(testString2Key("TestGroupWakeup2"), uint64(i+2), 1, 5, 5)
				result, err := event.Wait(60)
				if err != nil {
					t.Errorf("Event Wait Fail %v", err)
					return
				}
				if result.GetLockData() == nil || result.GetLockData().GetStringValue() != "aaa" {
					t.Errorf("Event Wait Data Fail %v", result.GetLockData())
					return
				}
			}(i)
		}

		time.Sleep(20 * time.Millisecond)
		if succedCount != 0 {
			t.Errorf("Event Group Wait Fail %v", err)
			return
		}
		_, err = event.WakeupWithData(protocol.NewLockCommandDataSetString("aaa"))
		if err != nil {
			t.Errorf("Event Wakeup Set Fail %v", err)
			return
		}
		time.Sleep(time.Second)
		if succedCount != 4 {
			t.Errorf("Event Wakeup Succed Count Fail %v", err)
			return
		}

		isSeted, err = event.IsSet()
		if err != nil {
			t.Errorf("Event Wakeup Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Wakeup Seted Status Error %v", isSeted)
			return
		}

		event = client.GroupEvent(testString2Key("TestGroupWakeup2"), 1, 1, 5, 5)
		_, err = event.Wait(1)
		if err != nil {
			t.Errorf("Event Wait Less Version Fail %v", err)
			return
		}

		_, err = event.SetWithData(protocol.NewLockCommandDataSetString("bbb"))
		if err != nil {
			t.Errorf("Event Set Fail %v", err)
			return
		}
	})
}
