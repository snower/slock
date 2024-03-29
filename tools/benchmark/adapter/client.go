package adapter

import (
	"fmt"
	"github.com/snower/slock/client"
	"math/rand"
	"sync/atomic"
	"time"
)

func runClientBenchmark(slockClient *client.Client, count *uint32, maxCount uint32, keys [][16]byte, waiter chan bool, timeout uint32, expried uint32) {
	var lockKey [16]byte
	for {
		if keys == nil {
			lockKey = slockClient.SelectDB(0).GenLockId()
		} else {
			lockKey = keys[rand.Intn(len(keys))]
		}
		lock := slockClient.Lock(lockKey, timeout, expried)

		err := lock.Lock()
		if err != nil {
			fmt.Printf("Lock Error %v\n", err)
			continue
		}

		err = lock.Unlock()
		if err != nil {
			fmt.Printf("UnLock Error %v\n", err)
			continue
		}

		atomic.AddUint32(count, 2)
		if *count > maxCount {
			close(waiter)
			return
		}
	}
}

func StartClientBenchmark(clientCount int, concurrentc int, maxCount int, keys [][16]byte, port int, host string, timeout uint32, expried uint32) {
	fmt.Printf("Run %d Client, %d concurrentc, %d Count Lock and Unlock\n", clientCount, concurrentc, maxCount)

	clients := make([]*client.Client, clientCount)
	waiters := make([]chan bool, concurrentc)
	defer func() {
		for _, c := range clients {
			if c == nil {
				continue
			}
			_ = c.Close()
		}
	}()

	for i := 0; i < clientCount; i++ {
		c := client.NewClient(host, uint(port))
		err := c.Open()
		if err != nil {
			fmt.Printf("Connect Error: %v", err)
			return
		}
		clients[i] = c
	}
	fmt.Printf("Client Opened %d\n", len(clients))

	var count uint32
	startTime := time.Now().UnixNano()
	for i := 0; i < concurrentc; i++ {
		waiters[i] = make(chan bool, 1)
		go runClientBenchmark(clients[i%clientCount], &count, uint32(maxCount), keys, waiters[i], timeout, expried)
	}
	for _, waiter := range waiters {
		<-waiter
	}
	endTime := time.Now().UnixNano()
	pt := float64(endTime-startTime) / 1000000000.0
	fmt.Printf("%d %fs %fr/s\n\n", count, pt, float64(count)/pt)
}
