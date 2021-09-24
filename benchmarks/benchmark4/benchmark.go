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

var bench_count uint8
var bench_key [16]byte
var LETTERS = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GenLockId() ([16]byte) {
    now := uint32(time.Now().Unix())
    request_id_index := 1
    return [16]byte{
        byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now), LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)],
        LETTERS[rand.Intn(52)], LETTERS[rand.Intn(52)], byte(request_id_index >> 40), byte(request_id_index >> 32), byte(request_id_index >> 24), byte(request_id_index >> 16), byte(request_id_index >> 8), byte(request_id_index),
    }
}

func writeAll(client net.Conn, buf []byte, data_len int) error{
    wlen := 0
    for ; wlen < data_len; {
        n, err := client.Write(buf[wlen:])
        if err != nil {
            fmt.Printf("write lock error %v %n\n", err, n)
            return err
        }
        wlen += n
    }
    return nil
}

func readAll(client net.Conn, buf []byte, data_len int) error{
    rlen := 0
    for ; rlen < data_len; {
        n, err := client.Read(buf[rlen:])
        if (err == io.EOF) {
            fmt.Printf("read lock error %v %n\n", err, n)
            return err
        }
        rlen += n
    }
    return nil
}

func run2(client net.Conn, count *uint32, wcount *uint32, max_count uint32, waiter chan bool, index uint32) {
    rcount := uint32(0)
    wbuf := make([]byte, 4096)
    rbuf := make([]byte, 4096)

    for i :=0; i<64; i++ {
        wbuf[i*64+0], wbuf[i*64+1], wbuf[i*64+2] = byte(0x56), byte(0x01), byte(0x01)

        wbuf[i*64+3], wbuf[i*64+4], wbuf[i*64+5], wbuf[i*64+6], wbuf[i*64+7], wbuf[i*64+8], wbuf[i*64+9], wbuf[i*64+10] = byte(0), byte(0), byte(0), byte(bench_count), byte(index>>24),  byte(index>>16), byte(index>>8), byte(index)
        wbuf[i*64+11], wbuf[i*64+12], wbuf[i*64+13], wbuf[i*64+14], wbuf[i*64+15], wbuf[i*64+16], wbuf[i*64+17], wbuf[i*64+18] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)

        wbuf[i*64+19], wbuf[i*64+20] = byte(0), byte(0)

        wbuf[i*64+21], wbuf[i*64+22], wbuf[i*64+23], wbuf[i*64+24], wbuf[i*64+25], wbuf[i*64+26], wbuf[i*64+27], wbuf[i*64+28] = byte(0), byte(0), byte(0), byte(bench_count), byte(index>>24),  byte(index>>16), byte(index>>8), byte(index)
        wbuf[i*64+29], wbuf[i*64+30], wbuf[i*64+31], wbuf[i*64+32], wbuf[i*64+33], wbuf[i*64+34], wbuf[i*64+35], wbuf[i*64+36] = byte(0), byte(0), byte(rcount>>24), byte(rcount>>16), byte(index>>8), byte(index), byte(rcount>>8), byte(rcount)

        wbuf[i*64+37], wbuf[i*64+38], wbuf[i*64+39], wbuf[i*64+40], wbuf[i*64+41], wbuf[i*64+42], wbuf[i*64+43], wbuf[i*64+44] = bench_key[0], bench_key[1], bench_key[2], bench_key[3], bench_key[4], bench_key[5], bench_key[6], bench_key[7]
        wbuf[i*64+45], wbuf[i*64+46], wbuf[i*64+47], wbuf[i*64+48], wbuf[i*64+49], wbuf[i*64+50], wbuf[i*64+51], wbuf[i*64+52] = bench_key[8], bench_key[9], bench_key[10], bench_key[11], bench_key[12], bench_key[13], bench_key[14], bench_key[15]

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
        if *count > max_count {
            close(waiter)
            return
        }
    }
}

func bench2(client_count int, concurrentc int, max_count int, port int, host string)  {
    bench_count ++
    bench_key = GenLockId()

    fmt.Printf("Run %d Client, %d concurrentc, %d Count Lock and Unlock\n", client_count, concurrentc, max_count)

    addr := fmt.Sprintf("%s:%d", host, port)
    clients := make([]net.Conn, client_count)
    waiters := make([]chan bool, concurrentc)
    defer func() {
        for _, c := range clients {
            c.Close()
        }
    }()

    for c := 0; c < client_count; c++ {
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
    start_time := time.Now().UnixNano()
    for i:=0; i < concurrentc; i++{
        waiters[i] = make(chan bool, 1)
        go run2(clients[i % client_count], &count, &wcount, uint32(max_count), waiters[i], uint32(i))
    }

    for _, waiter := range waiters {
        <- waiter
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

        bench2(*client, *conc, *count, *port, *host)
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
