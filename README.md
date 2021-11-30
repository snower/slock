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

```
./bin/slock --bind=0.0.0.0 --port=5658 --log=/var/log/slock.log
```

# Show Info

```
redis-cli -h 127.0.0.1 -p 5658 info
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
- TIMEOUT 已锁定则等待时长，4字节无符号整型，高2字节是FLAG，低2字节是时间，可选
- EXPRIED 锁定后超时时长，4字节无符号整型，高2字节是FLAG，低2字节是时间，可选
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
- TIMEOUT 已锁定则等待时长，4字节无符号整型，高2字节是FLAG，低2字节是时间，可选
- EXPRIED 锁定后超时时长，4字节无符号整型，高2字节是FLAG，低2字节是时间，可选
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

# SlaveOf

非replset启动是默认为leader节点，可在启动参数中使用该参数指定leader节点连接参数信息启动为follower节点，follower节点会自动注册为代理转发lock和unlock指令到leader节点操作。

```
./bin/slock --bind=0.0.0.0 --port=5659 --log=/var/log/slock.log --slaveof=127.0.0.1:5658
```

# Replset

使用replset参数指定集群名称即可启动为集群模式（集群模式会忽略slaveof参数），集群启动成功后自动投票选举leader节点，其余follower节点会启动代理转发lock和unlock命令模式，自动跟踪主节点变更，即集群启动后任意节点都可正常接受客户端连接，各节点完全一致。

集群管理使用redis-cli，配置管理可在集群内任意节点完成。

### 集群创建示例

#### 第一步：启动集群节点

```
./bin/slock --data_dir=./data1 --bind=0.0.0.0 --port=5657 --log=/var/log/slock1.log --replset=s1

./bin/slock --data_dir=./data2 --bind=0.0.0.0 --port=5658 --log=/var/log/slock2.log --replset=s1

./bin/slock --data_dir=./data3 --bind=0.0.0.0 --port=5659 --log=/var/log/slock3.log --replset=s1
```

#### 第二步：使用redis-cli连接

```
redis-cli -h 127.0.0.1 -p 5657
```

#### 第二步：配置初始化集群

```
# 在连接成功后的redis-cli中执行

replset config 127.0.0.1:5657 weight 1 arbiter 0
```

#### 第三步：添加其它节点

```
# 在连接成功后的redis-cli中执行

replset add 127.0.0.1:5658 weight 1 arbiter 0

replset add 127.0.0.1:5659 weight 1 arbiter 0
```

### 配置指令

```
# 添加节点
replset add <host> weight <weight> arbiter <arbiter>

# 更新节点参数
replset set <host> weight <weight> arbiter <arbiter>

# 移除节点
replset remove <host>

# 查看节点信息
replset get <host>

# 查询集群节点列表
replset members
```

### 参数说明

#### host 
ip:port或domain:port格式
```
注意：
    集群内各节点需要相互连接，所以指定的host需要保证集群内的其它节点都要能正常连接到该地址。
```

#### weight
数字，投票权重优先级，投票优先选择拥有最新数据节点，再选择weight最高节点，weight为0时永远不会被选为leader节点，即可通过设置weight为0使该节点单纯为数据节点。

#### arbiter
数字，非0即仅投票节点，此时依然启动集群代理，此时该节点也为集群agent节点。

# Flags

### Lock命令FLAG

```
|7                    |           1           |         0           |
|---------------------|-----------------------|---------------------|
|                     |when_locked_update_lock|when_locked_show_lock|

0x01 when_locked_show_lock 已锁定时返回锁定信息，过期实际设置为0可用于查询锁信息
0x02 when_locked_update_lock 已锁定时更新锁定信息
```

### UnLock命令FLAG
```
|7                  |           1             |               0               |
|-------------------|-------------------------|-------------------------------|
|                   |when_unlocked_cancel_wait|when_unlocked_unlock_first_lock|

0x01 when_unlocked_unlock_first_lock 未锁定时直接取消第一个已锁定的锁，并返回锁定信息
0x02 when_unlocked_cancel_wait 等待锁定时取消等待
```

### Timeout参数FLAG

```
|    15  |                13                   |  12 |        11      |       10       |      9       |           8        |             7          |      6    |       5      |4           0|
|--------|-------------------------------------|-----|----------------|----------------|--------------|--------------------|------------------------|-----------|--------------|-------------|
|keeplive|update_no_reset_timeout_checked_count|acked|timeout_is_error|millisecond_time|unlock_to_wait|unrenew_expried_time|timeout_reverse_key_lock|minute_time|push_subscribe|             |

0x0020 push_subscribe 超时时推送超时订阅信息
0x0040 minute_time 超时时间是分钟单位
0x0080 timeout_reverse_key_lock 超时时反转LockKey后调用锁定指令
0x0100 unrenew_expried_time 锁定时不重计过期时间，即过期时间以接收到命令时计算
0x0200 unlock_to_wait 该LockKey当前没有任何锁锁定则等待，否则执行正常锁定流程
0x0400 millisecond_time 超时时间是毫秒单位
0x0800 timeout_is_error 超时时以ERROR级别在日志中输出错误，可用于开发时调试死锁等异常
0x1000 acked 需等待整个集群所有活动节点均锁定成功才返回成功，强一致锁定
0x2000 update_no_reset_timeout_checked_count 用Lock命令更新Flag更新锁定信息是，不重置超时队列计数器
0x8000 keeplive 连接不断开则不超时，则此时设置的超时时间为检查连接存活状态的延时间隔
```

### Expried参数FLAG

```
|    15  |          14          |                13                   |                12         |        11      |       10       |         9        |        8    |            7           |     6     |    5         |4            0|
|--------|----------------------|-------------------------------------|---------------------------|----------------|----------------|------------------|-------------|------------------------|-----------|--------------|--------------|
|keeplive|unlimited_expried_time|update_no_reset_expried_checked_count|aof_time_of_expried_parcent|expried_is_error|millisecond_time|unlimited_aof_time|zeor_aof_time|expried_reverse_key_lock|minute_time|push_subscribe|              |

0x0020 push_subscribe 过期时推送过期订阅信息
0x0040 minute_time 过期时间是分钟单位
0x0080 expried_reverse_key_lock 过期时反转LockKey后调用锁定指令
0x0100 zeor_aof_time 立刻持久化该命令
0x0200 unlimited_aof_time 不持久化该命令
0x0400 millisecond_time 过期时间是毫秒单位
0x0800 expried_is_error 过期时以ERROR级别在日志中输出错误，可用于开发时调试死锁等异常
0x1000 aof_time_of_expried_parcent 持久化时间是过期时间的百分比，百分比值有启动参数db_lock_aof_parcent_time指定
0x2000 update_no_reset_expried_checked_count 用Lock命令更新Flag更新锁定信息是，不重置过期队列计数器
0x4000 unlimited_expried_time 永远不过期（谨慎使用）
0x8000 keeplive 连接不断开则不过期，则此时设置的超时时间为检查连接存活状态的延时间隔
```

# Resources

PHP Client [phslock](https://github.com/snower/phslock)

Python Client [pyslock](https://github.com/snower/pyslock)

# License

slock uses the MIT license, see LICENSE file for the details.