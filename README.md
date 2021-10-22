# slock

[![Build Status](https://travis-ci.org/snower/slock.svg?branch=master)](https://travis-ci.org/snower/slock)

High-performance distributed sync service and atomic DB

# About

High-performance distributed sync service and atomic DB. Provides good multi-core support through lock queues, high-performance asynchronous binary network protocols.
Can be used for spikes, synchronization, event notification, concurrency control, etc. Support Redis client.

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

# Redis Text Protocol Command

```
LOCK lock_key [TIMEOUT seconds] [EXPRIED seconds] [LOCK_ID lock_id_string] [FLAG flag_uint8] [COUNT count_uint16] [RCOUNT rcount_uint8] [WILL will_uint8]

对lock_key加锁。
- LOCK_KEY 需要加锁的key值，长度16字节，不足16前面加0x00补足，32字节是尝试hex解码，超过16字节取MD5
- TIMEOUT 已锁定则等待时长，不超过两字节无符号整型，可选
- EXPRIED 锁定后超时时长，不超过两字节无符号整型，可选
- LOCK_ID 本次加锁ID，不指明lock_id则自动生成一个，长度16字节，不足16前面加0x00补足，32字节是尝试hex解码，超过16字节取MD5，可选
- FLAG 标识，可选
- COUNT LOCK_KEY最大锁定次数，不超过两字节无符号整型，可选
- RCOUNT LOCK_ID 重复锁定次数，不超过一字节无符号整型，可选
- WILL 设置值1表示次命令遗言命令，在连接断开后被发送执行，可选

返回 [RESULT_CODE, RESULG_MSG, 'LOCK_ID', lock_id, 'LCOUNT', lcount, 'COUNT', count, 'LRCOUNT', lrcoutn, 'RCOUNT', rcount]
- RESULT_CODE 返回值，数字，0为成功
- RESULG_MSG 放回值消息提示，OK为成功
- LOCK_ID 本次加锁ID，解锁是需要
- LCOUNT LOCK_KEY已锁定次数
- COUNT LOCK_KEY最大锁定次数
- LRCOUNT LOCK_ID已锁定次数
- RCOUNT LOCK_ID最大锁定次数
- WILL 设置值1表示次命令遗言命令，在连接断开后被发送执行，可选

UNLOCK lock_key [LOCK_ID lock_id_string] [FLAG flag_uint8] [RCOUNT rcount_uint8] [WILL will_uint8]

对lock_key解锁。
- LOCK_KEY 需要加锁的key值，长度16字节，不足16前面加0x00补足，32字节是尝试hex解码，超过16字节取MD5
- LOCK_ID 本次加锁ID，不指明则自动使用上次锁定lock_id，长度16字节，不足16前面加0x00补足，32字节是尝试hex解码，超过16字节取MD5，可选
- FLAG 标识，可选
- RCOUNT LOCK_ID 重复锁定次数，不超过一字节无符号整型，可选
- WILL 设置值1表示次命令遗言命令，在连接断开后被发送执行，可选

返回 [RESULT_CODE, RESULG_MSG, 'LOCK_ID', lock_id, 'LCOUNT', lcount, 'COUNT', count, 'LRCOUNT', lrcoutn, 'RCOUNT', rcount]
- RESULT_CODE 返回值，数字，0为成功
- RESULG_MSG 放回值消息提示，OK为成功
- LOCK_ID 本次加锁ID，解锁是需要
- LCOUNT LOCK_KEY已锁定次数
- COUNT LOCK_KEY最大锁定次数
- LRCOUNT LOCK_ID已锁定次数
- RCOUNT LOCK_ID最大锁定次数

PUSH lock_key [TIMEOUT seconds] [EXPRIED seconds] [LOCK_ID lock_id_string] [FLAG flag_uint8] [COUNT count_uint16] [RCOUNT rcount_uint8]

推送对lock_key加锁命令，不等待结果返回。
- LOCK_KEY 需要加锁的key值，长度16字节，不足16前面加0x00补足，32字节是尝试hex解码，超过16字节取MD5
- TIMEOUT 已锁定则等待时长，不超过两字节无符号整型，可选
- EXPRIED 锁定后超时时长，不超过两字节无符号整型，可选
- LOCK_ID 本次加锁ID，不指明lock_id则自动生成一个，长度16字节，不足16前面加0x00补足，32字节是尝试hex解码，超过16字节取MD5，可选
- FLAG 标识，可选
- COUNT LOCK_KEY最大锁定次数，不超过两字节无符号整型，可选
- RCOUNT LOCK_ID 重复锁定次数，不超过一字节无符号整型，可选
- WILL 设置值1表示次命令遗言命令，在连接断开后被发送执行，可选

返回 [RESULT_CODE, RESULG_MSG, 'LOCK_ID', lock_id, 'LCOUNT', lcount, 'COUNT', count, 'LRCOUNT', lrcoutn, 'RCOUNT', rcount]
- RESULT_CODE 返回值，数字，0为成功
- RESULG_MSG 放回值消息提示，OK为成功
- LOCK_ID 本次加锁ID，解锁是需要
- LCOUNT LOCK_KEY已锁定次数
- COUNT LOCK_KEY最大锁定次数
- LRCOUNT LOCK_ID已锁定次数
- RCOUNT LOCK_ID最大锁定次数
```

# Benchmark

```
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

# Resources

PHP Client [phslock](https://github.com/snower/phslock)

Python Client [pyslock](https://github.com/snower/pyslock)

# License

slock uses the MIT license, see LICENSE file for the details.