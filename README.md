# slock

[![Build Status](https://travis-ci.org/snower/slock.svg?branch=master)](https://travis-ci.org/snower/slock)

High-performance distributed sync lock service

# About

High-performance distributed sync lock service. Provides good multi-core support through lock queues, high-performance asynchronous binary network protocols.
Can be used for spikes, synchronization, event notification, concurrency control, etc.

# Installation

```
go get github.com/snower/slock
```

# Start

```
Usage:
  slock [info]
	default start slock server
	info command show db state

Application Options:
      --bind=                                bind address (default: 127.0.0.1)
      --port=                                bind port (default: 5658)
      --log=                                 log filename, default is output stdout (default: -)
      --log_level=[DEBUG|INFO|Warning|ERROR] log level (default: INFO)
      --db_concurrent_lock=                  db concurrent lock count (default: 64)

Help Options:
  -h, --help                                 Show this help message    	
```

```
./bin/slock --bind=0.0.0.0 --port=5658 --log=/var/log/slock.log
```

# Show State

```
./bin/slock info --host=127.0.01 --port=5658
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

# Support Lock Type

- Lock - regular lock, not reentrant
- Event - distributed event
- CycleEvent - loop wait event
- RLock - reentrant lock,max reentrant 0xff
- Semaphore - semaphore, max 0xffff
- RWLock - read-write lock, max concurrent reading 0xffff

# Benchmark

```
go run src/github.com/snower/slock/benchmarks/benchmark2.go
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

PHP Client [phslock](https://github.com/snower/phslock)

Python Client [pyslock](https://github.com/snower/pyslock)

# License

slock uses the MIT license, see LICENSE file for the details.