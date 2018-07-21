# slock

High-performance distributed shared lock service

# About

High-performance distributed shared lock service, Support sync Lock and Event.

# Installation

```
git clone https://github.com/snower/slock.git
cd slock && go get && ./build.sh
```

# Start

```
./slock -h
Usage of ./slock:
  -bind string
    	bind host (default "0.0.0.0")
  -host string
    	client host (default "127.0.0.1")
  -info int
    	show db state info (default -1)
  -log string
    	log filename (default "-")
  -log_level string
    	log_level (default "INFO")
  -port int
    	bind port (default 5658)    	
```

```
./slock --bind=0.0.0.0 --port=5658 --log=/var/log/slock.log
```

# Show State

```
./slock --host=127.0.01 --port=5658 --info=0
slock DB ID:	0
LockCount:	2
UnLockCount:	2
LockedCount:	0
WaitCount:	0
TimeoutedCount:	0
ExpriedCount:	0
UnlockErrorCount:	0
KeyCount:	0
```

# Benchmark

```
go run benchmarks/benchmark.go

Run 1 Client, 1 concurrentc, 200000 Count Lock and Unlock
Client Opened 1
200001 11.000591s 18180.932783r/s

Run 1 Client, 64 concurrentc, 300000 Count Lock and Unlock
Client Opened 1
300064 4.000925s 74998.656167r/s

Run 64 Client, 64 concurrentc, 500000 Count Lock and Unlock
Client Opened 64
500062 6.000585s 83335.541868r/s

Run 8 Client, 64 concurrentc, 500000 Count Lock and Unlock
Client Opened 8
500064 5.000616s 100000.484100r/s

Run 16 Client, 64 concurrentc, 500000 Count Lock and Unlock
Client Opened 16
500064 5.000482s 100003.154116r/s

Run 16 Client, 256 concurrentc, 500000 Count Lock and Unlock
Client Opened 16
500256 5.002156s 100008.078737r/s

Run 64 Client, 512 concurrentc, 500000 Count Lock and Unlock
Client Opened 64
500512 5.003930s 100023.780488r/s

Run 512 Client, 512 concurrentc, 500000 Count Lock and Unlock
Client Opened 512
500511 7.009299s 71406.716735r/s

Run 64 Client, 4096 concurrentc, 500000 Count Lock and Unlock
Client Opened 64
504070 5.042368s 99966.930058r/s

Run 4096 Client, 4096 concurrentc, 500000 Count Lock and Unlock
Client Opened 4096
504084 7.042551s 71576.909286r/s

```

# License

slock uses the MIT license, see LICENSE file for the details.