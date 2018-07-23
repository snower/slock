# slock

High-performance distributed shared lock service

# About

High-performance distributed shared lock service, Support sync Lock and Event.

# Installation

```
git clone https://github.com/snower/slock.git
cd slock && ./init.sh && ./build.sh
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
200001 12.000736s 16665.727692r/s

Run 1 Client, 64 concurrentc, 300000 Count Lock and Unlock
Client Opened 1
300064 4.000302s 75010.336326r/s

Run 64 Client, 64 concurrentc, 500000 Count Lock and Unlock
Client Opened 64
500064 6.004223s 83285.379641r/s

Run 8 Client, 64 concurrentc, 500000 Count Lock and Unlock
Client Opened 8
500064 5.000532s 100002.165810r/s

Run 16 Client, 64 concurrentc, 500000 Count Lock and Unlock
Client Opened 16
500064 5.002968s 99953.464025r/s

Run 16 Client, 256 concurrentc, 500000 Count Lock and Unlock
Client Opened 16
500256 4.000981s 125033.329293r/s

Run 64 Client, 512 concurrentc, 500000 Count Lock and Unlock
Client Opened 64
500512 4.008893s 124850.438685r/s

Run 512 Client, 512 concurrentc, 500000 Count Lock and Unlock
Client Opened 512
500504 6.014160s 83220.933636r/s

Run 64 Client, 4096 concurrentc, 500000 Count Lock and Unlock
Client Opened 64
504058 5.042950s 99953.006215r/s

Run 4096 Client, 4096 concurrentc, 500000 Count Lock and Unlock
Client Opened 4096
504066 6.139644s 82100.197687r/s

```

# License

slock uses the MIT license, see LICENSE file for the details.