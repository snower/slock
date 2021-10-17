package main

import (
	"flag"
	"fmt"
	"github.com/snower/slock/tools/benchmark/adapter"
)

func main() {
	port := flag.Int("port", 5658, "port")
	host := flag.String("host", "127.0.0.1", "host")
	clientCount := flag.Int("client", 0, "client count")
	conc := flag.Int("conc", 0, "concurrentc")
	count := flag.Int("count", 0, "lock and unlock count")
	key := flag.String("key", "", "lock key")
	mode := flag.String("mode", "client", "benchmark mode")
	timeoutFlag := flag.Int("timeout_flag", 0, "timeout_flag")
	expriedFlag := flag.Int("expried_flag", 0, "expried_flag")

	flag.Parse()

	benchFunc := adapter.StartClientBenchmark
	if *mode != "client" {
		benchFunc = adapter.StartStreamBenchmark
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

		benchFunc(*clientCount, *conc, *count, *key, *port, *host, *timeoutFlag, *expriedFlag)
		fmt.Println("Succed")
		return
	}

	benchFunc(1, 1, 200000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	benchFunc(1, 16, 300000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	benchFunc(8, 64, 500000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	benchFunc(16, 64, 500000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	benchFunc(64, 64, 500000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	benchFunc(16, 256, 500000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	benchFunc(64, 512, 500000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	benchFunc(512, 512, 500000, *key, *port, *host, *timeoutFlag, *expriedFlag)

	fmt.Println("Succed")
}
