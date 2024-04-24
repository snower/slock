package client

import (
	"github.com/snower/slock/protocol"
	"testing"
	"time"
)

func TestEvent_DefaultSet(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.Event(testString2Key("TestDefaultSet"), 5, 5, true)
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

func TestEvent_DefaultClear(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.Event(testString2Key("TestDefaultClear"), 5, 5, false)
		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("Event Check Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Seted Status Error %v", err)
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

		_, err = event.Clear()
		if err != nil {
			t.Errorf("Event Clear Fail %v", err)
			return
		}
	})
}

func TestEvent_DefaultSetWithData(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.Event(testString2Key("TestDefaultSet2"), 5, 5, true)
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

		_, err = event.ClearWithUnsetData()
		if err != nil {
			t.Errorf("Event Wait Clear Fail %v", err)
			return
		}

		go func() {
			time.Sleep(20 * time.Millisecond)
			result, serr := event.SetWithData(protocol.NewLockCommandDataSetString("bbb"))
			if serr != nil {
				t.Errorf("Event Wakeup Set Fail %v", serr)
				return
			}
			if result.GetLockData() != nil {
				t.Errorf("Event Wakeup Set Data Fail %v", result.GetLockData())
				return
			}
		}()

		result, err := event.Wait(60)
		if err != nil {
			t.Errorf("Event Wait Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "bbb" {
			t.Errorf("Event Wait Data Fail %v", result.GetLockData())
			return
		}
	})
}

func TestEvent_DefaultClearWithData(t *testing.T) {
	testWithClient(t, func(client *Client) {
		event := client.Event(testString2Key("TestDefaultClear2"), 5, 5, false)
		isSeted, err := event.IsSet()
		if err != nil {
			t.Errorf("Event Check Seted Fail %v", err)
			return
		}
		if isSeted {
			t.Errorf("Event Seted Status Error %v", err)
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

		_, err = event.ClearWithUnsetData()
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

		go func() {
			time.Sleep(20 * time.Millisecond)
			result, serr := event.SetWithData(protocol.NewLockCommandDataSetString("bbb"))
			if serr != nil {
				t.Errorf("Event Wakeup Set Fail %v", serr)
				return
			}
			if result.GetLockData() != nil {
				t.Errorf("Event Wakeup Set Data Fail %v", result.GetLockData())
				return
			}
		}()

		result, err := event.Wait(60)
		if err != nil {
			t.Errorf("Event Wait Fail %v", err)
			return
		}
		if result.GetLockData() == nil || result.GetLockData().GetStringData() != "bbb" {
			t.Errorf("Event Wait Data Fail %v", result.GetLockData())
			return
		}

		_, err = event.Clear()
		if err != nil {
			t.Errorf("Event Clear Fail %v", err)
			return
		}
	})
}
