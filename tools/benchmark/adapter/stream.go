package adapter

import (
	"fmt"
	"github.com/snower/slock/protocol"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

var benchStreamCount uint8

func writeStreamAll(client net.Conn, buf []byte, dataLen int) error {
	wlen := 0
	for wlen < dataLen {
		n, err := client.Write(buf[wlen:])
		if err != nil {
			return err
		}
		wlen += n
	}
	return nil
}

func readStreamAll(client net.Conn, buf []byte, dataLen int) error {
	rlen := 0
	for rlen < dataLen {
		n, err := client.Read(buf[rlen:])
		if err != nil {
			return err
		}
		rlen += n
	}
	return nil
}

func runStreamBenchmark(client net.Conn, count *uint32, wcount *uint32, maxCount uint32, keys [][16]byte, waiter chan bool, index uint32, timeout uint32, expried uint32) {
	lockKey := protocol.GenLockId()

	rcount := uint32(0)
	wbuf := make([]byte, 4096)
	rbuf := make([]byte, 4096)

	for i := 0; i < 64; i++ {
		wbuf[i*64+0], wbuf[i*64+1], wbuf[i*64+2] = byte(0x56), byte(0x01), byte(0x01)

		wbuf[i*64+3], wbuf[i*64+4], wbuf[i*64+5], wbuf[i*64+6], wbuf[i*64+7], wbuf[i*64+8], wbuf[i*64+9], wbuf[i*64+10] = byte(0), byte(0), byte(0), byte(benchStreamCount), byte(index>>24), byte(index>>16), byte(index>>8), byte(index)
		wbuf[i*64+11], wbuf[i*64+12], wbuf[i*64+13], wbuf[i*64+14], wbuf[i*64+15], wbuf[i*64+16], wbuf[i*64+17], wbuf[i*64+18] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)

		wbuf[i*64+19], wbuf[i*64+20] = byte(0), byte(0)

		wbuf[i*64+21], wbuf[i*64+22], wbuf[i*64+23], wbuf[i*64+24], wbuf[i*64+25], wbuf[i*64+26], wbuf[i*64+27], wbuf[i*64+28] = byte(0), byte(0), byte(0), byte(benchStreamCount), byte(index>>24), byte(index>>16), byte(index>>8), byte(index)
		wbuf[i*64+29], wbuf[i*64+30], wbuf[i*64+31], wbuf[i*64+32], wbuf[i*64+33], wbuf[i*64+34], wbuf[i*64+35], wbuf[i*64+36] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)

		if keys == nil {
			wbuf[i*64+37], wbuf[i*64+38], wbuf[i*64+39], wbuf[i*64+40], wbuf[i*64+41], wbuf[i*64+42], wbuf[i*64+43], wbuf[i*64+44] = byte(0), byte(0), byte(0), byte(benchStreamCount), byte(index>>24), byte(index>>16), byte(index>>8), byte(index)
			wbuf[i*64+45], wbuf[i*64+46], wbuf[i*64+47], wbuf[i*64+48], wbuf[i*64+49], wbuf[i*64+50], wbuf[i*64+51], wbuf[i*64+52] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
		} else {
			if i%2 == 0 {
				lockKey = keys[rand.Intn(len(keys))]
			}
			wbuf[i*64+37], wbuf[i*64+38], wbuf[i*64+39], wbuf[i*64+40], wbuf[i*64+41], wbuf[i*64+42], wbuf[i*64+43], wbuf[i*64+44] = lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7]
			wbuf[i*64+45], wbuf[i*64+46], wbuf[i*64+47], wbuf[i*64+48], wbuf[i*64+49], wbuf[i*64+50], wbuf[i*64+51], wbuf[i*64+52] = lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15]
		}

		wbuf[i*64+53], wbuf[i*64+54], wbuf[i*64+55], wbuf[i*64+56], wbuf[i*64+57], wbuf[i*64+58], wbuf[i*64+59], wbuf[i*64+60] = byte(timeout), byte(timeout>>8), byte(timeout>>16), byte(timeout>>24), byte(expried), byte(expried>>8), byte(expried>>16), byte(expried>>24)
		wbuf[i*64+61], wbuf[i*64+62], wbuf[i*64+63] = byte(0xff), byte(0xff), byte(0)
	}

	go func() {
		for {
			err := readStreamAll(client, rbuf, 4096)
			if err != nil {
				_ = client.Close()
				return
			}

			succed := uint32(0)
			for i := 0; i < 64; i++ {
				if rbuf[i*64+19] != 9 {
					succed++
				}
			}

			atomic.AddUint32(count, succed)
			if *count > maxCount {
				_ = client.Close()
				return
			}
		}
	}()

	for {
		for i := 0; i < 64; i++ {
			wbuf[i*64+11], wbuf[i*64+12], wbuf[i*64+13], wbuf[i*64+14], wbuf[i*64+15], wbuf[i*64+16], wbuf[i*64+17], wbuf[i*64+18] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
			wbuf[i*64+29], wbuf[i*64+30], wbuf[i*64+31], wbuf[i*64+32], wbuf[i*64+33], wbuf[i*64+34], wbuf[i*64+35], wbuf[i*64+36] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
			if keys == nil {
				wbuf[i*64+45], wbuf[i*64+46], wbuf[i*64+47], wbuf[i*64+48], wbuf[i*64+49], wbuf[i*64+50], wbuf[i*64+51], wbuf[i*64+52] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
			} else {
				if i%2 == 0 {
					lockKey = keys[rand.Intn(len(keys))]
				}
				wbuf[i*64+37], wbuf[i*64+38], wbuf[i*64+39], wbuf[i*64+40], wbuf[i*64+41], wbuf[i*64+42], wbuf[i*64+43], wbuf[i*64+44] = lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7]
				wbuf[i*64+45], wbuf[i*64+46], wbuf[i*64+47], wbuf[i*64+48], wbuf[i*64+49], wbuf[i*64+50], wbuf[i*64+51], wbuf[i*64+52] = lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15]
			}
			if i%2 == 0 {
				wbuf[i*64+2] = byte(0x01)
			} else {
				wbuf[i*64+2] = byte(0x02)
				rcount++
			}
		}

		err := writeStreamAll(client, wbuf, 4096)
		if err != nil {
			_ = client.Close()
			close(waiter)
			return
		}
		atomic.AddUint32(wcount, 64)
	}
}

func StartStreamBenchmark(clientCount int, concurrentc int, maxCount int, keys [][16]byte, port int, host string, timeout uint32, expried uint32) {
	benchStreamCount++

	fmt.Printf("Run %d Client, %d concurrentc, %d Count Lock and Unlock\n", clientCount, concurrentc, maxCount)

	addr := fmt.Sprintf("%s:%d", host, port)
	clients := make([]net.Conn, clientCount)
	waiters := make([]chan bool, concurrentc)
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	for i := 0; i < clientCount; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("Client open error %v\n", err)
			return
		}
		clients[i] = conn
	}
	fmt.Printf("Client Opened %d\n", len(clients))

	var count uint32
	var wcount uint32
	startTime := time.Now().UnixNano()
	for i := 0; i < concurrentc; i++ {
		waiters[i] = make(chan bool, 1)
		go runStreamBenchmark(clients[i%clientCount], &count, &wcount, uint32(maxCount), keys, waiters[i], uint32(i), timeout, expried)
	}

	for _, waiter := range waiters {
		<-waiter
	}
	endTime := time.Now().UnixNano()
	pt := float64(endTime-startTime) / 1000000000.0
	fmt.Printf("%d %fs %fr/s\n\n", count, pt, float64(count)/pt)
}
