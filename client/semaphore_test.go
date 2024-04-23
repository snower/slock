package client

import "testing"

func TestSemaphore_Acquire(t *testing.T) {
	testWithClient(t, func(client *Client) {
		semaphore := client.Semaphore(testString2Key("TestRWLock"), 0, 5, 2)
		_, err := semaphore.Acquire()
		if err != nil {
			t.Errorf("Semaphore Acquire1 Fail %v", err)
			return
		}

		_, err = semaphore.Acquire()
		if err != nil {
			t.Errorf("Semaphore Acquire2 Fail %v", err)
			return
		}

		count, err := semaphore.Count()
		if err != nil {
			t.Errorf("Semaphore Count Fail %v", err)
			return
		}
		if count != 2 {
			t.Errorf("Semaphore Count Error %v", count)
			return
		}

		_, err = semaphore.Acquire()
		if err == nil {
			t.Errorf("Semaphore Acquire3 Fail %v", err)
			return
		}

		_, err = semaphore.Release()
		if err != nil {
			t.Errorf("Semaphore Release1 Fail %v", err)
			return
		}

		count, err = semaphore.Count()
		if err != nil {
			t.Errorf("Semaphore ReCount Fail %v", err)
			return
		}
		if count != 1 {
			t.Errorf("Semaphore ReCount Error %v", count)
			return
		}

		_, err = semaphore.Acquire()
		if err != nil {
			t.Errorf("Semaphore ReAcquire3 Fail %v", err)
			return
		}

		err = semaphore.ReleaseAll()
		if err != nil {
			t.Errorf("Semaphore ReleaseAll Fail %v", err)
			return
		}

		count, err = semaphore.Count()
		if err != nil {
			t.Errorf("Semaphore Clear Count Fail %v", err)
			return
		}
		if count != 0 {
			t.Errorf("Semaphore Clear Count Error %v", count)
			return
		}
	})
}
