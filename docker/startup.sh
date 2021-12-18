#!/bin/bash

if [ ! -d "/slock" ]; then
  mkdir /slock
fi

if [ ! -d "/slock/data" ]; then
  mkdir /slock/data
fi

cd /slock || exit

CMD_ARG="--bind=\"0.0.0.0\" --port=5658"

if [ -n "$ARG_LOG" ]; then
  CMD_ARG="$CMD_ARG --log=\"$ARG_LOG\""
else
  CMD_ARG="$CMD_ARG --log=-"
fi

if [ -n "$ARG_LOG_LEVEL" ]; then
  CMD_ARG="$CMD_ARG --log_level=\"$ARG_LOG_LEVEL\""
fi

if [ -n "$ARG_LOG_ROTATING_SIZE" ]; then
  CMD_ARG="$CMD_ARG --log_rotating_size=$ARG_LOG_ROTATING_SIZE"
fi

if [ -n "$ARG_LOG_BACKUP_COUNT" ]; then
  CMD_ARG="$CMD_ARG --log_backup_count=$ARG_LOG_BACKUP_COUNT"
fi

if [ -n "$ARG_LOG_BUFFER_SIZE" ]; then
  CMD_ARG="$CMD_ARG --log_buffer_size=$ARG_LOG_BUFFER_SIZE"
fi

if [ -n "$ARG_LOG_BUFFER_FLUSH_TIME" ]; then
  CMD_ARG="$CMD_ARG --log_buffer_flush_time=$ARG_LOG_BUFFER_FLUSH_TIME"
fi

if [ -n "$ARG_DATA_DIR" ]; then
  CMD_ARG="$CMD_ARG --data_dir=\"$ARG_DATA_DIR\""
else
  CMD_ARG="$CMD_ARG --data_dir=/slock/data"
fi

if [ -n "$ARG_DB_FAST_KEY_COUNT" ]; then
  CMD_ARG="$CMD_ARG --db_fast_key_count=$ARG_DB_FAST_KEY_COUNT"
fi

if [ -n "$ARG_DB_CONCURRENT" ]; then
  CMD_ARG="$CMD_ARG --db_concurrent=$ARG_DB_CONCURRENT"
fi

if [ -n "$ARG_DB_LOCK_AOF_TIME" ]; then
  CMD_ARG="$CMD_ARG --db_lock_aof_time=$ARG_DB_LOCK_AOF_TIME"
fi

if [ -n "$ARG_DB_LOCK_AOF_PARCENT_TIME" ]; then
  CMD_ARG="$CMD_ARG --db_lock_aof_parcent_time=$ARG_DB_LOCK_AOF_PARCENT_TIME"
fi

if [ -n "$ARG_AOF_QUEUE_SIZE" ]; then
  CMD_ARG="$CMD_ARG --aof_queue_size=$ARG_AOF_QUEUE_SIZE"
fi

if [ -n "$ARG_AOF_FILE_REWRITE_SIZE" ]; then
  CMD_ARG="$CMD_ARG --aof_file_rewrite_size=$ARG_AOF_FILE_REWRITE_SIZE"
fi

if [ -n "$ARG_AOF_FILE_BUFFER_SIZE" ]; then
  CMD_ARG="$CMD_ARG --aof_file_buffer_size=$ARG_AOF_FILE_BUFFER_SIZE"
fi

if [ -n "$ARG_AOF_RING_BUFFER_SIZE" ]; then
  CMD_ARG="$CMD_ARG --aof_ring_buffer_size=$ARG_AOF_RING_BUFFER_SIZE"
fi

if [ -n "$ARG_AOF_RING_BUFFER_MAX_SIZE" ]; then
  CMD_ARG="$CMD_ARG --aof_ring_buffer_max_size=$ARG_AOF_RING_BUFFER_MAX_SIZE"
fi

if [ -n "$ARG_SLAVEOF" ]; then
  CMD_ARG="$CMD_ARG --slaveof=\"$ARG_SLAVEOF\""
fi

if [ -n "$ARG_REPLSET" ]; then
  CMD_ARG="$CMD_ARG --replset=\"$ARG_REPLSET\""
fi

CMD="/go/bin/slock $CMD_ARG"
echo "$CMD"
$CMD