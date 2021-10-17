package adapter

import (
	"fmt"
	"github.com/snower/slock/client"
	"sync/atomic"
	"time"
)

func runClientBenchmark(slockClient *client.Client, count *uint32, maxCount uint32, key string, waiter chan bool, timeoutFlag int, expriedFlag int) {
	var lockKey [16]byte
	if key != "" {
		lockKey = parseLockKey(key)
	}
	for {
		if key == "" {
			lockKey = slockClient.SelectDB(0).GenLockId()
		}
		lock := slockClient.Lock(lockKey, (uint32(timeoutFlag)<<16)|5, (uint32(expriedFlag)<<16)|5)

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

		atomic.AddUint32(count, 1)
		if *count > maxCount {
			close(waiter)
			return
		}
	}
}

func StartClientBenchmark(clientCount int, concurrentc int, maxCount int, key string, port int, host string, timeoutFlag int, expriedFlag int) {
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
		go runClientBenchmark(clients[i%clientCount], &count, uint32(maxCount), key, waiters[i], timeoutFlag, expriedFlag)
	}
	for _, waiter := range waiters {
		<-waiter
	}
	endTime := time.Now().UnixNano()
	pt := float64(endTime-startTime) / 1000000000.0
	fmt.Printf("%d %fs %fr/s\n\n", count, pt, float64(count)/pt)
}
