package main

import (
    "flag"
    "fmt"
    "github.com/snower/slock/client"
    "sync/atomic"
    "time"
)

var lockKey [16]byte

func run(slockClient *client.Client, count *uint32, maxCount uint32, waiter chan bool) {
    for ;; {
        lock := client.NewLock(slockClient.SelectDB(0), lockKey, 60, 60)
        lock.SetCount(31)

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

func bench(clientCount int, concurrentc int, maxCount int, port int, host string)  {
    fmt.Printf("Run %d Client, %d concurrentc, %d Count Lock and Unlock\n", clientCount, concurrentc, maxCount)

    clients := make([]*client.Client, clientCount)
    waiters := make([]chan bool, concurrentc)
    defer func() {
        for _, c := range clients {
            if c == nil {
                continue
            }
            c.Close()
        }
    }()

    for c := 0; c < clientCount; c++ {
        slockClient := client.NewClient(host, uint(port))
        if(c == 0) {
            lockKey = slockClient.SelectDB(0).GenLockId()
        }
        err := slockClient.Open()
        if err != nil {
            fmt.Printf("Connect Error: %v", err)
            return
        }
        clients[c] = slockClient
    }
    fmt.Printf("Client Opened %d\n", len(clients))

    var count uint32
    startTime := time.Now().UnixNano()
    for i:=0; i < concurrentc; i++{
        waiters[i] = make(chan bool, 1)
        go run(clients[i %clientCount], &count, uint32(maxCount), waiters[i])
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

        bench(*clientCount, *conc, *count, *port, *host)
        fmt.Println("Succed")
        return
    }

    bench(1, 1, 200000, *port, *host)

    bench(1, 64, 300000, *port, *host)

    bench(64, 64, 500000, *port, *host)

    bench(8, 64, 500000, *port, *host)

    bench(16, 64, 500000, *port, *host)

    bench(16, 256, 500000, *port, *host)

    bench(64, 512, 500000, *port, *host)

    bench(512, 512, 500000, *port, *host)

    bench(64, 4096, 500000, *port, *host)

    bench(4096, 4096, 500000, *port, *host)

    fmt.Println("Succed")
}
