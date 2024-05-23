package client

import (
	"math/rand"
	"sync"
	"testing"
)

func TestPriorityLock_LockAndUnLock(t *testing.T) {
	testWithClient(t, func(client *Client) {
		waiter := sync.WaitGroup{}
		locks := make([]*PriorityLock, 0)
		for i := 0; i < 1000; i++ {
			waiter.Add(1)
			go func() {
				lock := client.PriorityLock(testString2Key("TestPriorityLock"), uint8(rand.Intn(50)+1), 5, 10)
				_, err := lock.Lock()
				if err == nil {
					locks = append(locks, lock)
					_, _ = lock.Unlock()
				}
				waiter.Done()
			}()
		}

		waiter.Wait()
		currentPriority := uint8(0)
		for _, lock := range locks {
			if lock.priority < currentPriority {
				t.Errorf("TestPriorityLock priority fail")
				return
			}
		}
	})
}
