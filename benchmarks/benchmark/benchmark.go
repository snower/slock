package main

import (
    "github.com/snower/slock/client"
    "fmt"
    "time"
    "sync"
    "flag"
)

func run(slock_client *client.Client, count *int, max_count int, end_count *int, clock *sync.Mutex) {
    for ;; {
        lock_key := slock_client.SelectDB(0).GenLockId()
        lock := slock_client.Lock(lock_key, 5, 5)

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

        *count++
        if *count > max_count {
            clock.Lock()
            *end_count++
            clock.Unlock()
            return
        }
    }
}

func bench(client_count int, concurrentc int, max_count int, port int, host string)  {
    clock := sync.Mutex{}

    fmt.Printf("Run %d Client, %d concurrentc, %d Count Lock and Unlock\n", client_count, concurrentc, max_count)

    clients := make([]*client.Client, client_count)
    defer func() {
        for _, c := range clients {
            c.Close()
        }
    }()

    for c := 0; c < client_count; c++ {
        slock_client := client.NewClient(host, uint(port))
        err := slock_client.Open()
        if err != nil {
            fmt.Printf("Connect Error: %v", err)
            return
        }
        clients[c] = slock_client
    }
    fmt.Printf("Client Opened %d\n", len(clients))

    var count int
    var end_count int
    start_time := time.Now().UnixNano()
    for i:=0; i < concurrentc; i++{
        go run(clients[i % client_count], &count, max_count, &end_count, &clock)
    }
    for ;; {
        if end_count >= concurrentc {
            break
        }
        time.Sleep(1e9)
    }
    end_time := time.Now().UnixNano()
    pt := float64(end_time - start_time) / 1000000000.0
    fmt.Printf("%d %fs %fr/s\n\n", count, pt, float64(count) / pt)
}

func main()  {
    port := flag.Int("port", 5658, "port")
    host := flag.String("host", "127.0.0.1", "host")
    client := flag.Int("client", 0, "client count")
    conc := flag.Int("conc", 0, "concurrentc")
    count := flag.Int("count", 0, "lock and unlock count")

    flag.Parse()

    if *client > 0 || *conc > 0 || *count > 0 {
        if *client <= 0 {
            *client = 16
        }

        if *conc <= 0 {
            *conc = 512
        }

        if *count <= 0 {
            *count = 500000
        }

        bench(*client, *conc, *count, *port, *host)
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
