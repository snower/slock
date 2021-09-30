package main

import (
    "flag"
    "fmt"
    "io"
    "math/rand"
    "net"
    "sync/atomic"
    "time"
)

var benchCount uint8
var benchKey [16]byte
var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GenLockId() ([16]byte) {
    now := uint32(time.Now().Unix())
    requestIdIndex := 1
    return [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(requestIdIndex >> 40), byte(requestIdIndex >> 32), byte(requestIdIndex >> 24), byte(requestIdIndex >> 16), byte(requestIdIndex >> 8), byte(requestIdIndex),
    }
}

func writeAll(client net.Conn, buf []byte, dataLen int) error{
    wlen := 0
    for ; wlen < dataLen; {
        n, err := client.Write(buf[wlen:])
        if err != nil {
            fmt.Printf("write lock error %v %n\n", err, n)
            return err
        }
        wlen += n
    }
    return nil
}

func readAll(client net.Conn, buf []byte, dataLen int) error{
    rlen := 0
    for ; rlen < dataLen; {
        n, err := client.Read(buf[rlen:])
        if (err == io.EOF) {
            fmt.Printf("read lock error %v %n\n", err, n)
            return err
        }
        rlen += n
    }
    return nil
}

func run2(client net.Conn, count *uint32, wcount *uint32, maxCount uint32, waiter chan bool, index uint32) {
    rcount := uint32(0)
    wbuf := make([]byte, 4096)
    rbuf := make([]byte, 4096)

    for i :=0; i<64; i++ {
        wbuf[i*64+0], wbuf[i*64+1], wbuf[i*64+2] = byte(0x56), byte(0x01), byte(0x01)

        wbuf[i*64+3], wbuf[i*64+4], wbuf[i*64+5], wbuf[i*64+6], wbuf[i*64+7], wbuf[i*64+8], wbuf[i*64+9], wbuf[i*64+10] = byte(0), byte(0), byte(0), byte(benchCount), byte(index>>24),  byte(index>>16), byte(index>>8), byte(index)
        wbuf[i*64+11], wbuf[i*64+12], wbuf[i*64+13], wbuf[i*64+14], wbuf[i*64+15], wbuf[i*64+16], wbuf[i*64+17], wbuf[i*64+18] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)

        wbuf[i*64+19], wbuf[i*64+20] = byte(0), byte(0)

        wbuf[i*64+21], wbuf[i*64+22], wbuf[i*64+23], wbuf[i*64+24], wbuf[i*64+25], wbuf[i*64+26], wbuf[i*64+27], wbuf[i*64+28] = byte(0), byte(0), byte(0), byte(benchCount), byte(index>>24),  byte(index>>16), byte(index>>8), byte(index)
        wbuf[i*64+29], wbuf[i*64+30], wbuf[i*64+31], wbuf[i*64+32], wbuf[i*64+33], wbuf[i*64+34], wbuf[i*64+35], wbuf[i*64+36] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)

        wbuf[i*64+37], wbuf[i*64+38], wbuf[i*64+39], wbuf[i*64+40], wbuf[i*64+41], wbuf[i*64+42], wbuf[i*64+43], wbuf[i*64+44] = benchKey[0], benchKey[1], benchKey[2], benchKey[3], benchKey[4], benchKey[5], benchKey[6], benchKey[7]
        wbuf[i*64+45], wbuf[i*64+46], wbuf[i*64+47], wbuf[i*64+48], wbuf[i*64+49], wbuf[i*64+50], wbuf[i*64+51], wbuf[i*64+52] = benchKey[8], benchKey[9], benchKey[10], benchKey[11], benchKey[12], benchKey[13], benchKey[14], benchKey[15]

        wbuf[i*64+53], wbuf[i*64+54], wbuf[i*64+55], wbuf[i*64+56], wbuf[i*64+57], wbuf[i*64+58], wbuf[i*64+59], wbuf[i*64+60] = byte(5), byte(0), byte(0), byte(0), byte(5), byte(0), byte(0), byte(0)

        wbuf[i*64+61], wbuf[i*64+62], wbuf[i*64+63] = byte(1), byte(0), 0x00
    }

    for i := 0; i<4; i++ {
        for i :=0; i<64; i++ {
            wbuf[i*64+11], wbuf[i*64+12], wbuf[i*64+13], wbuf[i*64+14], wbuf[i*64+15], wbuf[i*64+16], wbuf[i*64+17], wbuf[i*64+18] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
            wbuf[i*64+29], wbuf[i*64+30], wbuf[i*64+31], wbuf[i*64+32], wbuf[i*64+33], wbuf[i*64+34], wbuf[i*64+35], wbuf[i*64+36] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
            if i % 2 == 0 {
                wbuf[i*64+2] = byte(0x01)
            } else {
                wbuf[i*64+2] = byte(0x02)
                rcount++
            }
        }
        
        err := writeAll(client, wbuf, 4096)
        if err != nil {
            return
        }
        atomic.AddUint32(wcount, 64)
    }

    for ;; {
        for i :=0; i<64; i++ {
            wbuf[i*64+11], wbuf[i*64+12], wbuf[i*64+13], wbuf[i*64+14], wbuf[i*64+15], wbuf[i*64+16], wbuf[i*64+17], wbuf[i*64+18] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
            wbuf[i*64+29], wbuf[i*64+30], wbuf[i*64+31], wbuf[i*64+32], wbuf[i*64+33], wbuf[i*64+34], wbuf[i*64+35], wbuf[i*64+36] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)
            if i % 2 == 0 {
                wbuf[i*64+2] = byte(0x01)
            } else {
                wbuf[i*64+2] = byte(0x02)
                rcount++
            }
        }

        err := writeAll(client, wbuf, 4096)
        if err != nil {
            close(waiter)
            return
        }
        atomic.AddUint32(wcount, 64)

        err = readAll(client, rbuf, 4096)
        if(err != nil) {
            close(waiter)
            return
        }

        atomic.AddUint32(count, 64)
        if *count > maxCount {
            close(waiter)
            return
        }
    }
}

func bench2(clientCount int, concurrentc int, maxCount int, port int, host string)  {
    benchCount++
    benchKey = GenLockId()

    fmt.Printf("Run %d Client, %d concurrentc, %d Count Lock and Unlock\n", clientCount, concurrentc, maxCount)

    addr := fmt.Sprintf("%s:%d", host, port)
    clients := make([]net.Conn, clientCount)
    waiters := make([]chan bool, concurrentc)
    defer func() {
        for _, c := range clients {
            c.Close()
        }
    }()

    for c := 0; c < clientCount; c++ {
        conn, err := net.Dial("tcp", addr)
        if(err != nil) {
            fmt.Printf("Client open error %v\n", err)
            return;
        }
        clients[c] = conn
    }
    fmt.Printf("Client Opened %d\n", len(clients))

    var count uint32
    var wcount uint32
    startTime := time.Now().UnixNano()
    for i:=0; i < concurrentc; i++{
        waiters[i] = make(chan bool, 1)
        go run2(clients[i %clientCount], &count, &wcount, uint32(maxCount), waiters[i], uint32(i))
    }

    for _, waiter := range waiters {
        <- waiter
    }
    endTime := time.Now().UnixNano()
    pt := float64(endTime-startTime) / 1000000000.0
    fmt.Printf("%d %fs %fr/s\n\n", count, pt, float64(count) / pt)
}

func main()  {
    port := flag.Int("port", 5658, "port")
    host := flag.String("host", "127.0.0.1", "host")
    clientCount := flag.Int("client", 0, "client count")
    conc := flag.Int("conc", 0, "concurrentc")
    count := flag.Int("count", 0, "lock and unlock count")

    flag.Parse()

    if *clientCount > 0 || *conc > 0 || *count > 0 {
        if *clientCount <= 0 {
            *clientCount = 16
        }

        if *conc <= 0 {
            *conc = 512
        }

        if *count <= 0 {
            *count = 500000
        }

        bench2(*clientCount, *conc, *count, *port, *host)
        fmt.Println("Succed")
        return
    }

    bench2(1, 1, 2000000, *port, *host)

    bench2(16, 16, 5000000, *port, *host)

    bench2(32, 32, 5000000, *port, *host)

    bench2(64, 64, 5000000, *port, *host)

    bench2(128, 128, 5000000, *port, *host)

    bench2(256, 256, 5000000, *port, *host)

    bench2(512, 512, 5000000, *port, *host)

    fmt.Println("Succed")
}
