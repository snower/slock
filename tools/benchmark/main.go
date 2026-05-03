package main

import (
	"flag"
	"fmt"

	"github.com/snower/slock/protocol"
	"github.com/snower/slock/tools/benchmark/adapter"
)

func main() {
	port := flag.Int("port", 5658, "port")
	host := flag.String("host", "127.0.0.1", "host")
	clientCount := flag.Int("client", 0, "client count")
	conc := flag.Int("conc", 0, "concurrentc")
	count := flag.Int("count", 0, "lock and unlock count")
	key_count := flag.Int("key", 0, "lock key count")
	mode := flag.String("mode", "client", "benchmark mode")
	timeout := flag.Int("timeout", 5, "timeout")
	timeoutFlag := flag.Int("timeout_flag", 0, "timeout_flag")
	expried := flag.Int("expried", 5, "expried")
	expriedFlag := flag.Int("expried_flag", 0, "expried_flag")
	dataLength := flag.Int("data_length", 0, "data_length")
	dataRate := flag.Float64("data_rate", 0.5, "data_rate")
	dataType := flag.Int("data_type", 0, "data_type")

	flag.Parse()

	benchFunc := adapter.StartClientBenchmark
	if *mode != "client" {
		if *mode == "tree" {
			benchFunc = adapter.StartTreeBenchmark
		} else {
			benchFunc = adapter.StartStreamBenchmark
		}
	}
	var keys [][16]byte = nil
	if *key_count > 0 {
		keys = make([][16]byte, *key_count)
		for i := 0; i < *key_count; i++ {
			keys = append(keys, protocol.GenLockId())
		}
	}

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

		benchFunc(*clientCount, *conc, *count, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)
		fmt.Println("Succed")
		return
	}

	benchFunc(1, 1, 200000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	benchFunc(1, 16, 300000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	benchFunc(8, 64, 500000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	benchFunc(16, 64, 500000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	benchFunc(64, 64, 500000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	benchFunc(16, 256, 500000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	benchFunc(64, 512, 500000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	benchFunc(512, 512, 500000, keys, *port, *host, uint32(*timeout)|uint32(*timeoutFlag)<<16, uint32(*expried)|uint32(*expriedFlag)<<16, *dataLength, *dataRate, *dataType)

	fmt.Println("Succed")
}
