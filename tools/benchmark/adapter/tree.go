package adapter

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/snower/slock/client"
)

func runTreeBenchmark(slockClient *client.Client, count *uint32, maxCount uint32, keys [][16]byte, waiter chan bool, timeout uint32, expried uint32, dataLength int, dataRate float64) {
	isClose := false
	go func() {
		<-slockClient.Unavailable()
		isClose = true
	}()

	var lockKey [16]byte
	for !isClose {
		if keys == nil {
			lockKey = slockClient.SelectDB(0).GenLockId()
		} else {
			lockKey = keys[rand.Intn(len(keys))]
		}
		treeLock := slockClient.TreeLock(lockKey, lockKey, timeout, expried)
		leafLock := treeLock.NewLeafLock()
		_, err := leafLock.Lock()
		if err != nil {
			fmt.Printf("LeafLock Error %v\n", err)
			continue
		}
		childTreeLock := treeLock.NewChild()
		childLeafLock := childTreeLock.NewLeafLock()
		_, err = childLeafLock.Lock()
		if err != nil {
			fmt.Printf("ChildLeafLock Error %v\n", err)
			continue
		}
		childChildTreeLock := childTreeLock.NewChild()
		childChildLeafLock := childChildTreeLock.NewLeafLock()
		_, err = childChildLeafLock.Lock()
		if err != nil {
			fmt.Printf("ChildChildLeafLock Error %v\n", err)
			continue
		}

		if (expried & 0xffff) > 0 {
			if dataLength > 0 && rand.Float64() >= dataRate {
				_, err = leafLock.UnlockWithData(randLockData(dataLength))
				if err != nil {
					fmt.Printf("LeafUnlock Error %v\n", err)
					continue
				}
				_, err = childChildLeafLock.UnlockWithData(randLockData(dataLength))
				if err != nil {
					fmt.Printf("ChildChildLeafUnlock Error %v\n", err)
					continue
				}
				_, err = childLeafLock.UnlockWithData(randLockData(dataLength))
				if err != nil {
					fmt.Printf("ChildLeafUnlock Error %v\n", err)
					continue
				}
			} else {
				_, err = leafLock.Unlock()
				if err != nil {
					fmt.Printf("LeafUnlock Error %v\n", err)
					continue
				}
				_, err = childChildLeafLock.Unlock()
				if err != nil {
					fmt.Printf("ChildChildLeafUnlock Error %v\n", err)
					continue
				}
				_, err = childLeafLock.Unlock()
				if err != nil {
					fmt.Printf("ChildLeafUnlock Error %v\n", err)
					continue
				}
			}
		}

		if keys == nil {
			lock := slockClient.Lock(lockKey, 0, expried)
			if dataLength > 0 && rand.Float64() >= dataRate {
				_, err = lock.LockWithData(randLockData(dataLength))
				if err != nil {
					fmt.Printf("Lock Error %v\n", err)
					continue
				}
			} else {
				_, err = lock.Lock()
				if err != nil {
					fmt.Printf("Lock Error %v\n", err)
					continue
				}
			}
			if (expried & 0xffff) > 0 {
				if dataLength > 0 && rand.Float64() >= dataRate {
					_, err = lock.UnlockWithData(randLockData(dataLength))
					if err != nil {
						fmt.Printf("UnLock Error %v\n", err)
						continue
					}
				} else {
					_, err = lock.Unlock()
					if err != nil {
						fmt.Printf("UnLock Error %v\n", err)
						continue
					}
				}
			}
		}

		atomic.AddUint32(count, 10)
		if *count > maxCount {
			break
		}
	}
	close(waiter)
}

func StartTreeBenchmark(clientCount int, concurrentc int, maxCount int, keys [][16]byte, port int, host string, timeout uint32, expried uint32, dataLength int, dataRate float64, dataType int) {
	fmt.Printf("Run %d Client, %d concurrentc, %d Count Lock and Unlock\n", clientCount, concurrentc, maxCount)

	clients := make([]*client.Client, clientCount)
	waiters := make([]chan bool, concurrentc)
	defer func() {
		for _, c := range clients {
			if c != nil {
				_ = c.Close()
			}
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
		go runTreeBenchmark(clients[i%clientCount], &count, uint32(maxCount), keys, waiters[i], timeout, expried, dataLength, dataRate)
	}
	for _, waiter := range waiters {
		<-waiter
	}
	endTime := time.Now().UnixNano()
	pt := float64(endTime-startTime) / 1000000000.0
	for _, slockClient := range clients {
		_ = slockClient.Close()
	}
	fmt.Printf("%d %fs %fr/s\n\n", count, pt, float64(count)/pt)
}
