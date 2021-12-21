# slock

[![Build Status](https://travis-ci.com/snower/slock.svg?branch=master)](https://travis-ci.com/snower/slock)

High-performance distributed sync service and atomic DB

[中文](README-zh-hans.md)

# About

High-performance distributed sync service and atomic DB. Provides good multi-core support through lock queues, high-performance asynchronous binary network protocols.
Can be used for spikes, synchronization, event notification, concurrency control, etc. Support Redis client.

* [Install](#install)
* [Quick Start Server](#quick-Start-server)
* [Show Server Info](#show-server-info)
* [Support Lock Type](#support-lock-type)
* [Benchmark](#benchmark)
* [SlaveOf](#slaveof)
* [Replset](#replset)
* [Protocol](#Protocol)
* [Client Resources](#client-resources)
* [Docker](#docker)

# Install

```
go get github.com/snower/slock
```

# Quick Start Server

```bash
./bin/slock -h
Usage:
  slock [info]
        default start slock server
        info command show db state

Application Options:
      --conf=                                toml conf filename
      --bind=                                bind address (default: 127.0.0.1)
      --port=                                bind port (default: 5658)
      --log=                                 log filename, default is output stdout (default: -)
      --log_level=[DEBUG|INFO|Warning|ERROR] log level (default: INFO)
      --log_rotating_size=                   log rotating byte size (default: 67108864)
      --log_backup_count=                    log backup count (default: 5)
      --log_buffer_size=                     log buffer byte size (default: 0)
      --log_buffer_flush_time=               log buffer flush seconds time (default: 1)
      --data_dir=                            data dir (default: ./data/)
      --db_fast_key_count=                   db fast key count (default: 4194304)
      --db_concurrent=                       db concurrent count (default: 8)
      --db_lock_aof_time=                    db lock aof time (default: 1)
      --db_lock_aof_parcent_time=            db lock aof parcent expried time (default: 0.3)
      --aof_queue_size=                      aof channel queue size (default: 65536)
      --aof_file_rewrite_size=               aof file rewrite size (default: 67174400)
      --aof_file_buffer_size=                aof file buffer size (default: 4096)
      --aof_ring_buffer_size=                slave sync ring buffer size (default: 1048576)
      --aof_ring_buffer_max_size=            slave sync ring buffer max size (default: 268435456)
      --slaveof=                             slave of to master sync
      --replset=                             replset name, start replset mode when it set

Help Options:
  -h, --help                                 Show this help message
```

```bash
./bin/slock --bind=0.0.0.0 --port=5658 --log=/var/log/slock.log
```

# Show Server Info

```bash
redis-cli -h 127.0.0.1 -p 5658 info
```

# Support Lock Type

- Lock - regular lock, not reentrant
- Event - distributed event, support default seted and unseted event
- RLock - reentrant lock,max reentrant 0xff
- Semaphore - semaphore, max count 0xffff
- RWLock - read-write lock, max concurrent reading 0xffff
- MaxConcurrentFlow - maximum concurrent flow limit
- TokenBucketFlow - token bucket flow restriction

# Benchmark

```bash
go run tools/benchmark/main.go --mode=stream
# Intel(R) Core(TM) i5-4590 CPU @ 3.30GHz 4 Processor
# MemTotal: 8038144 kB

Run 1 Client, 1 concurrentc, 2000000 Count Lock and Unlock
Client Opened 1
2000064 6.426136s 311238.965232r/s

Run 16 Client, 16 concurrentc, 5000000 Count Lock and Unlock
Client Opened 16
5001024 3.738898s 1337566.282457r/s

Run 32 Client, 32 concurrentc, 5000000 Count Lock and Unlock
Client Opened 32
5002048 3.375306s 1481953.845101r/s

Run 64 Client, 64 concurrentc, 5000000 Count Lock and Unlock
Client Opened 64
5004096 3.148570s 1589323.402086r/s

Run 128 Client, 128 concurrentc, 5000000 Count Lock and Unlock
Client Opened 128
5008192 3.713254s 1348734.158725r/s

Run 256 Client, 256 concurrentc, 5000000 Count Lock and Unlock
Client Opened 256
5016384 4.233933s 1184804.690796r/s

Run 512 Client, 512 concurrentc, 5000000 Count Lock and Unlock
Client Opened 512
5032768 4.738539s 1062092.867154r/s

Succed
```

# SlaveOf

Non-replset start is the leader node by default, you can use this parameter in the start parameters to specify the leader node connection parameter information to start as follower node, follower node will automatically register as an agent to forward lock and unlock instructions to the leader node operation.
```bash
./bin/slock --bind=0.0.0.0 --port=5659 --log=/var/log/slock.log --slaveof=127.0.0.1:5658
```

# Replset

Use the replset parameter to specify the cluster name to start as a cluster mode (cluster mode will ignore the slaveof parameter), after the successful start of the cluster automatically vote for the leader node, the rest of the follower nodes will start the proxy forwarding lock and unlock command mode, automatically track the master node changes, that is, after the start of the cluster any node can normally accept client connections, the nodes are completely consistent.

Cluster management using redis-cli, configuration management can be done at any node in the cluster.

### Cluster creation examples

#### Step 1: Start cluster nodes

```bash
./bin/slock --data_dir=./data1 --bind=0.0.0.0 --port=5657 --log=/var/log/slock1.log --replset=s1

./bin/slock --data_dir=./data2 --bind=0.0.0.0 --port=5658 --log=/var/log/slock2.log --replset=s1

./bin/slock --data_dir=./data3 --bind=0.0.0.0 --port=5659 --log=/var/log/slock3.log --replset=s1
```

#### Step 2: Connect using redis-cli

```bash
redis-cli -h 127.0.0.1 -p 5657
```

#### Step 3: Configure the initialized cluster

```bash
# Execute in redis-cli after a successful connection

replset config 127.0.0.1:5657 weight 1 arbiter 0
```

#### Step 4: Add other members

```bash
# Execute in redis-cli after a successful connection

replset add 127.0.0.1:5658 weight 1 arbiter 0

replset add 127.0.0.1:5659 weight 1 arbiter 0
```

### Configuration commands

```
# Add member
replset add <host> weight <weight> arbiter <arbiter>

# Update member parameters
replset set <host> weight <weight> arbiter <arbiter>

# Remove member
replset remove <host>

# View member Information
replset get <host>

# View the list of cluster members
replset members
```

### Parameter Description

#### host 
ip:port or domain:port format
```
Caution.
    The nodes in the cluster need to connect to each other, so the specified host needs to ensure that all other nodes in the cluster can connect to that address properly.
```

#### weight
Numbers, voting weight priority, voting priority to select the node with the latest data, and then select the highest weight node, weight is 0 will never be selected as the leader node, that is, by setting weight to 0 so that the node is simply a data node.

#### arbiter
Number, non-zero that is only voting node, at this time still start the cluster agent, at this time the node is also the cluster agent node.

# Protocol

### Slock Binary Protocol

```
# Request Command
|     0     |      1    |     2     |     3     |     4     |     5     |     6     |     7     |
|-----------|-----------|-----------|-----------|-----------|-----------|-----------|-----------|
|   Magic   |  Version  |CommandType|                      RequestId                            |
|-----------------------------------------------------------------------------------------------|
|                                           RequestId                                           |
|-----------------------------------------------------------------------------------------------|
|           RequestId               |   FLAG    |   DBID    |               LockId              |
|-----------------------------------------------------------------------------------------------|
|                                           LockId                                              |
|-----------------------------------------------------------------------------------------------|
|                       LockId                              |               LockKey             |
|-----------------------------------------------------------------------------------------------|
|                                           LockKey                                             |
|-----------------------------------------------------------------------------------------------|
|                       LockKey                             |       Timeout         |TimeoutFlag|
|-----------------------------------------------------------------------------------------------|
|TimeoutFlag|         Expried       |     ExpriedFlag       |        Count         |   RCount   |
|-----------------------------------------------------------------------------------------------|

# Response Command
|     0     |      1    |     2     |     3     |     4     |     5     |     6     |     7     |
|-----------|-----------|-----------|-----------|-----------|-----------|-----------|-----------|
|   Magic   |  Version  |CommandType|                      RequestId                            |
|-----------------------------------------------------------------------------------------------|
|                                           RequestId                                           |
|-----------------------------------------------------------------------------------------------|
|           RequestId               |   Result  |   FLAG    |   DBID    |        LockId         |
|-----------------------------------------------------------------------------------------------|
|                                           LockId                                              |
|-----------------------------------------------------------------------------------------------|
|                             LockId                                    |        LockKey        |
|-----------------------------------------------------------------------------------------------|
|                                           LockKey                                             |
|-----------------------------------------------------------------------------------------------|
|                             LockKey                                   |        LCount         |
|-----------------------------------------------------------------------------------------------|
|        Count          |  LRCount  |   RCount  |                 PADDING                       |
|-----------------------------------------------------------------------------------------------|

# Request and return and so on are fixed 64 bytes, Magic value is 0x56, Version current is 0x01
# CommandType Lock call value is 1, UnLock call value is 2
# Digital encoding byte order for the network byte continued, that is, the low bit in front, RequestId, LockId, LockKey three for the byte array normal array order
# protocol for full-duplex asynchronous protocol, does not guarantee the order of return, to RequestId prevail
# RequestId connection-level unique, LockId the same LockKey under the unique can
# Binary protocol and redis text protocol are using the same port, the first time the data received will distinguish between the protocol type, later need to use the same protocol communication
# Count is the maximum number of times a LockKey can be locked, RCoun is the number of times a single LockId can be reentered
# Return command LCount and LRCount are the current number of locks for Count and RCount respectively
```

### Redis Text Protocol

```
LOCK lock_key [TIMEOUT seconds] [EXPRIED seconds] [LOCK_ID lock_id_string] [FLAG flag_uint8] [COUNT count_uint16] [RCOUNT rcount_uint8] [WILL will_uint8]

Add lock to lock_key.
- LOCK_KEY The key value to be locked, length 16 bytes, less than 16 front plus 0x00 to make up, 32 bytes is to try hex decoding, more than 16 bytes to take MD5
- TIMEOUT has been locked then waiting time, 4 bytes unsigned integer, high 2 bytes is FLAG, low 2 bytes is time, optional
- EXPRIED timeout after lock, 4 bytes unsigned integer, high 2 bytes is FLAG, low 2 bytes is time, optional
- LOCK_ID The lock ID is automatically generated without specifying the lock_id, length 16 bytes, less than 16 bytes plus 0x00, 32 bytes is to try to hex decode, more than 16 bytes to take MD5, optional
- FLAG marker, optional
- COUNT LOCK_KEY maximum locking times, not more than two bytes unsigned integer, optional
- RCOUNT LOCK_ID repeat lock count, not more than one byte unsigned integer, optional
- WILL Set the value of 1 to indicate the subcommand last word command, which is sent for execution after the connection is disconnected, optional

Return [RESULT_CODE, RESULG_MSG, 'LOCK_ID', lock_id, 'LCOUNT', lcount, 'COUNT', count, 'LRCOUNT', lrcoutn, 'RCOUNT', rcount]
- RESULT_CODE Return value, numeric, 0 for success
- RESULG_MSG Put back the value message prompt, OK is success
- LOCK_ID The locking ID, unlocking is required
- LCOUNT The number of times LOCK_KEY has been locked
- COUNT LOCK_KEY maximum number of locks
- LRCOUNT LOCK_ID has been locked number of times
- RCOUNT LOCK_ID maximum lock count
- WILL Set the value of 1 to indicate the subcommand last word command, which is sent for execution after the connection is disconnected, optional

UNLOCK lock_key [LOCK_ID lock_id_string] [FLAG flag_uint8] [RCOUNT rcount_uint8] [WILL will_uint8]

Unlock the lock_key.
- LOCK_KEY The key value to be locked, length 16 bytes, add 0x00 before less than 16 to make up, 32 bytes is to try to hex decode, more than 16 bytes to take MD5
- LOCK_ID The ID of this lock, if not specified, the last lock_id will be used automatically, length 16 bytes, add 0x00 before less than 16 to make up, 32 bytes is to try to decode hex, more than 16 bytes take MD5, optional
- FLAG marker, optional
- RCOUNT LOCK_ID repeat lock count, not more than one byte unsigned integer, optional
- WILL Set the value of 1 to indicate the subcommand last word command, which is sent for execution after the connection is disconnected, optional

Return [RESULT_CODE, RESULG_MSG, 'LOCK_ID', lock_id, 'LCOUNT', lcount, 'COUNT', count, 'LRCOUNT', lrcoutn, 'RCOUNT', rcount]
- RESULT_CODE Return value, numeric, 0 for success
- RESULG_MSG Put back the value message prompt, OK is success
- LOCK_ID The locking ID, unlocking is required
- LCOUNT The number of times LOCK_KEY has been locked
- COUNT LOCK_KEY maximum number of locks
- LRCOUNT LOCK_ID has been locked number of times
- RCOUNT LOCK_ID maximum number of locks

PUSH lock_key [TIMEOUT seconds] [EXPRIED seconds] [LOCK_ID lock_id_string] [FLAG flag_uint8] [COUNT count_uint16] [RCOUNT rcount_uint8]

Push to lock_key lock command, do not wait for the result to return.
- LOCK_KEY The key value to be locked, length 16 bytes, less than 16 front plus 0x00 to make up, 32 bytes is to try hex decoding, more than 16 bytes to take MD5
- TIMEOUT has been locked then wait for the length of time, 4 bytes unsigned integer, high 2 bytes is FLAG, low 2 bytes is time, optional
- EXPRIED timeout after lock, 4 bytes unsigned integer, high 2 bytes is FLAG, low 2 bytes is time, optional
- LOCK_ID The lock ID is automatically generated without specifying the lock_id, length 16 bytes, less than 16 bytes plus 0x00, 32 bytes is to try to hex decode, more than 16 bytes to take MD5, optional
- FLAG marker, optional
- COUNT LOCK_KEY maximum locking times, not more than two bytes unsigned integer, optional
- RCOUNT LOCK_ID repeat lock count, not more than one byte unsigned integer, optional
- WILL Set the value of 1 to indicate the subcommand last word command, which is sent for execution after the connection is disconnected, optional

Return [RESULT_CODE, RESULG_MSG, 'LOCK_ID', lock_id, 'LCOUNT', lcount, 'COUNT', count, 'LRCOUNT', lrcoutn, 'RCOUNT', rcount]
- RESULT_CODE Return value, numeric, 0 for success
- RESULG_MSG Put back the value message prompt, OK is success
- LOCK_ID The locking ID, unlocking is required
- LCOUNT The number of times LOCK_KEY has been locked
- COUNT LOCK_KEY maximum number of locks
- LRCOUNT LOCK_ID has been locked number of times
- RCOUNT LOCK_ID maximum number of locks
```


### Flags

#### Lock Command FLAG
```
|7                    |           1           |         0           |
|---------------------|-----------------------|---------------------|
|                     |when_locked_update_lock|when_locked_show_lock|

0x01 when_locked_show_lock Returns lock information when locked, expires when actually set to 0 can be used to query lock information
0x02 when_locked_update_lock Update lock information when locked
```

#### UnLock Command FLAG
```
|7                  |           1             |               0               |
|-------------------|-------------------------|-------------------------------|
|                   |when_unlocked_cancel_wait|when_unlocked_unlock_first_lock|

0x01 when_unlocked_unlock_first_lock unlock the first locked lock directly when unlocked, and return the lock information
0x02 when_unlocked_cancel_wait Cancel wait when waiting for lock
```

#### Timeout Parameter FLAG

```
|    15  |                13                   |  12 |        11      |       10       |      9       |           8        |             7          |      6    |       5      |4           0|
|--------|-------------------------------------|-----|----------------|----------------|--------------|--------------------|------------------------|-----------|--------------|-------------|
|keeplive|update_no_reset_timeout_checked_count|acked|timeout_is_error|millisecond_time|unlock_to_wait|unrenew_expried_time|timeout_reverse_key_lock|minute_time|push_subscribe|             |

0x0020 push_subscribe push timeout subscription message on timeout
0x0040 minute_time Timeout time in minutes
0x0080 timeout_reverse_key_lock Invoke lock command after reversing LockKey on timeout
0x0100 unrenew_expried_time No recalculation of the expiration time when locking, i.e. the expiration time is calculated when the command is received
0x0200 unlock_to_wait If the LockKey does not have any lock currently, it waits, otherwise it performs the normal locking process
0x0400 millisecond_time The timeout time is in milliseconds
0x0800 timeout_is_error output error in the log at the ERROR level, can be used to debug deadlocks and other exceptions during development
0x1000 acked need to wait for all active nodes in the cluster to be locked successfully before returning success, strong consistent locking
0x2000 update_no_reset_timeout_checked_count Use Lock command to update Flag to update locking information, not reset the timeout queue counter
0x8000 keeplive connection does not timeout if it is not open, then the timeout time set at this time is the delay interval to check the connection survival status
```

#### Expried Parameter FLAG

```
|    15  |          14          |                13                   |                12         |        11      |       10       |         9        |        8    |            7           |     6     |    5         |4            0|
|--------|----------------------|-------------------------------------|---------------------------|----------------|----------------|------------------|-------------|------------------------|-----------|--------------|--------------|
|keeplive|unlimited_expried_time|update_no_reset_expried_checked_count|aof_time_of_expried_parcent|expried_is_error|millisecond_time|unlimited_aof_time|zeor_aof_time|expried_reverse_key_lock|minute_time|push_subscribe|              |

0x0020 push_subscribe Push expired subscription information when expired
0x0040 minute_time Expired time in minutes
0x0080 expried_reverse_key_lock Invoke lock command after reversing LockKey on expiration
0x0100 zeor_aof_time Immediately persist the command
0x0200 unlimited_aof_time does not persist the command
0x0400 millisecond_time Expires in milliseconds
0x0800 expried_is_error expires with an error in the log at the ERROR level, which can be used for debugging deadlocks and other exceptions during development
0x1000 aof_time_of_expried_parcent The persistence time is a percentage of the expiration time, the percentage value is specified by the startup parameter db_lock_aof_parcent_time
0x2000 update_no_reset_expried_checked_count Updating the lock information with the Lock command to update the Flag is not resetting the expired queue counter
0x4000 unlimited_expried_time never expires (use with caution)
0x8000 keeplive The connection does not expire if it is not open, then the timeout set at this point is the delay interval to check the connection's alive status
```

# Client Resources

PHP Client [phslock](https://github.com/snower/phslock)

Python Client [pyslock](https://github.com/snower/pyslock)

Java Client [jaslock](https://github.com/snower/jaslock)

openresty Client [slock-lua-nginx](https://github.com/snower/slock-lua-nginx)

# docker

[build](docker/README.md)

[image](https://hub.docker.com/repository/docker/sujin190/slock)

# License

slock uses the MIT license, see LICENSE file for the details.