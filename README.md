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

# License

slock uses the MIT license, see LICENSE file for the details.