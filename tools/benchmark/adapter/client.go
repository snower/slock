package adapter

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/snower/slock/client"
	"github.com/snower/slock/protocol"
)

func randLockData(dataLength int) *protocol.LockCommandData {
	size := rand.Intn(dataLength) + 3
	data := make([]byte, size+4)
	data[0], data[1], data[2], data[3] = byte(size), byte(size>>8), byte(size>>16), byte(size>>24)
	for i := 0; i < size; i++ {
		data[i+4] = byte(rand.Intn(95) + 32)
	}
	data[4], data[5] = protocol.LOCK_DATA_COMMAND_TYPE_SET, 0
	return protocol.NewLockCommandDataFromOriginBytes(data)
}

func buildLockData(dataLength int, dataType int, lockType uint8, lockId [16]byte) *protocol.LockCommandData {
	switch dataType {
	case 1:
		command := protocol.NewLockCommand(0, lockId, [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 5, 10, 0)
		command.CommandType = lockType
		return protocol.NewLockCommandDataExecuteData(command, 0)
	case 2:
		return protocol.NewLockCommandDataIncrData(1)
	default:
		return randLockData(dataLength)
	}
}

func runClientBenchmark(slockClient *client.Client, count *uint32, maxCount uint32, keys [][16]byte, waiter chan bool, timeout uint32, expried uint32, dataLength int, dataRate float64, dataType int) {
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
		lock := slockClient.Lock(lockKey, timeout, expried)

		if dataLength > 0 && rand.Float64() >= dataRate {
			_, err := lock.LockWithData(buildLockData(dataLength, dataType, protocol.COMMAND_LOCK, lock.GetLockId()))
			if err != nil {
				fmt.Printf("Lock Error %v\n", err)
				continue
			}
		} else {
			_, err := lock.Lock()
			if err != nil {
				fmt.Printf("Lock Error %v\n", err)
				continue
			}
		}

		if (expried & 0xffff) > 0 {
			if dataLength > 0 && rand.Float64() >= dataRate {
				_, err := lock.UnlockWithData(buildLockData(dataLength, dataType, protocol.COMMAND_UNLOCK, lock.GetLockId()))
				if err != nil {
					fmt.Printf("UnLock Error %v\n", err)
					continue
				}
			} else {
				_, err := lock.Unlock()
				if err != nil {
					fmt.Printf("UnLock Error %v\n", err)
					continue
				}
			}
		}

		atomic.AddUint32(count, 2)
		if *count > maxCount {
			break
		}
	}
	close(waiter)
}

func StartClientBenchmark(clientCount int, concurrentc int, maxCount int, keys [][16]byte, port int, host string, timeout uint32, expried uint32, dataLength int, dataRate float64, dataType int) {
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
		go runClientBenchmark(clients[i%clientCount], &count, uint32(maxCount), keys, waiters[i], timeout, expried, dataLength, dataRate, dataType)
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
