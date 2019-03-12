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
go run benchmarks/benchmark2.go
Run 1 Client, 1 concurrentc, 2000000 Count Lock and Unlock
Client Opened 1
2000064 8.000466s 249993.436789r/s

Run 16 Client, 16 concurrentc, 5000000 Count Lock and Unlock
Client Opened 16
5001024 6.001325s 833319.959898r/s

Run 32 Client, 32 concurrentc, 5000000 Count Lock and Unlock
Client Opened 32
5002048 6.001126s 833518.306551r/s

Run 64 Client, 64 concurrentc, 5000000 Count Lock and Unlock
Client Opened 64
5004096 6.003500s 833529.779297r/s

Run 128 Client, 128 concurrentc, 5000000 Count Lock and Unlock
Client Opened 128
5008192 7.000599s 715394.808603r/s

Run 256 Client, 256 concurrentc, 5000000 Count Lock and Unlock
Client Opened 256
5016384 7.003221s 716296.734062r/s

Run 512 Client, 512 concurrentc, 5000000 Count Lock and Unlock
Client Opened 512
5032768 8.004369s 628752.618546r/s

```

# Resources

Go Client [goslock](https://github.com/snower/goslock)

PHP Client [phslock](https://github.com/snower/phslock)

Pyhon Client [pyslock](https://github.com/snower/pyslock)

# License

slock uses the MIT license, see LICENSE file for the details.