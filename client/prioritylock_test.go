package client

import (
	"sync"
	"testing"
	"time"
)

func TestPriorityLock_LockAndUnLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		lock := client.PriorityLock(testString2Key("TestPriorityLock"), 0, 5, 10)
		_, err := lock.Lock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}

		lock = client.PriorityLock(testString2Key("TestPriorityLock"), 1, 5, 10)
		_, err = lock.Lock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}

		lock1 := client.PriorityLock(testString2Key("TestPriorityLock"), 0, 0, 10)
		lock1.SetCount(10)
		_, err = lock1.Lock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		lock2 := client.PriorityLock(testString2Key("TestPriorityLock"), 0, 0, 10)
		_, err = lock2.Lock()
		if err == nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		lock3 := client.PriorityLock(testString2Key("TestPriorityLock"), 1, 0, 10)
		lock3.SetCount(10)
		_, err = lock3.Lock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		_, err = lock1.Unlock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		_, err = lock3.Unlock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}

		wg := sync.WaitGroup{}
		lockOrders := make([]int, 0)
		lock = client.PriorityLock(testString2Key("TestPriorityLock"), 10, 5, 10)
		_, err = lock.Lock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock1 = client.PriorityLock(testString2Key("TestPriorityLock"), 1, 5, 10)
			_, err = lock1.Lock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
			lockOrders = append(lockOrders, 1)
			_, err = lock1.Unlock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
		}()
		time.Sleep(100 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock2 = client.PriorityLock(testString2Key("TestPriorityLock"), 0, 5, 10)
			_, err = lock2.Lock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
			lockOrders = append(lockOrders, 2)
			_, err = lock2.Unlock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
		}()
		time.Sleep(100 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock3 = client.PriorityLock(testString2Key("TestPriorityLock"), 2, 5, 10)
			_, err = lock3.Lock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
			lockOrders = append(lockOrders, 3)
			_, err = lock3.Unlock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
		}()
		time.Sleep(100 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock4 := client.Lock(testString2Key("TestPriorityLock"), 5, 10)
			lock4.SetRcount(5)
			_, err = lock4.Lock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
			lockOrders = append(lockOrders, 4)
			_, err = lock4.Unlock()
			if err != nil {
				t.Errorf("Lock Fail %v", err)
				return
			}
		}()
		time.Sleep(100 * time.Millisecond)
		_, err = lock.Unlock()
		if err != nil {
			t.Errorf("Lock Fail %v", err)
			return
		}
		wg.Wait()
		if len(lockOrders) != 4 || lockOrders[0] != 3 || lockOrders[1] != 1 || lockOrders[2] != 2 || lockOrders[3] != 4 {
			t.Errorf("Lock Priority Fail %v", lockOrders)
		}
	})
}
