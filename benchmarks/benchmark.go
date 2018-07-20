package main

import (
    "slock"
    "fmt"
    "time"
)

func run(client *slock.Client, count *int, max_count int, end_count *int) {
    for ;; {
        lock_key := client.SelectDB(0).GenLockId()
        lock := client.Lock(lock_key, 5, 5)
        lock.Lock()
        lock.Unlock()
        *count++
        if *count > max_count {
            *end_count++
            return
        }
    }
}

func bench(client_count int, concurrentc int, max_count int)  {
    fmt.Printf("Run %d Client, %d concurrentc, %d\n", client_count, concurrentc, max_count)

    clients := make([]*slock.Client, client_count)
    for c := 0; c < client_count; c++ {
        client := slock.NewClient("127.0.0.1", 5658)
        err := client.Open()
        if err != nil {
            fmt.Printf("Connect Error: %v", err)
            return
        }
        clients[c] = client
    }

    var count int
    var end_count int
    start_time := time.Now().UnixNano()
    for i:=0; i < concurrentc; i++{
        go run(clients[i % client_count], &count, max_count, &end_count)
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

    for _, c := range clients {
        c.Close()
    }
}

func main()  {
    bench(1, 1, 200000)

    bench(1, 64, 300000)

    bench(64, 64, 500000)

    bench(8, 64, 500000)

    bench(16, 64, 500000)

    bench(16, 256, 500000)
}
