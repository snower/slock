package server

const QUEUE_MAX_MALLOC_SIZE = 0x3fffff

const TIMEOUT_QUEUE_LENGTH int64 = 0x10
const EXPRIED_QUEUE_LENGTH int64 = 0x10
const TIMEOUT_QUEUE_LENGTH_MASK int64 = 0x0f
const EXPRIED_QUEUE_LENGTH_MASK int64 = 0x0f
const TIMEOUT_QUEUE_MAX_WAIT uint8 = 0x08
const EXPRIED_QUEUE_MAX_WAIT uint8 = 0x08

const MANAGER_MAX_GLOCKS_INIT_SIZE  = 1024 * 1024 / 8
const TIMEOUT_LOCKS_QUEUE_INIT_SIZE  = 4096
const EXPRIED_LOCKS_QUEUE_INIT_SIZE  = 4096
const LONG_TIMEOUT_LOCKS_INIT_COUNT  = 16384
const LONG_EXPRIED_LOCKS_INIT_COUNT  = 16384
const LONG_LOCKS_QUEUE_INIT_SIZE  = 256
const FREE_LOCK_QUEUE_INIT_SIZE  = 4096
const FREE_LONG_WAIT_QUEUE_INIT_SIZE  = 8096
const FREE_COMMAND_QUEUE_INIT_SIZE  = 256
const STREAMS_INIT_COUNT  = 65536

type ServerConfig struct{
    Bind string                 `long:"bind" description:"bind address" default:"127.0.0.1"`
    Port uint                   `long:"port" description:"bind port" default:"5658"`
    Log  string                 `long:"log" description:"log filename, default is output stdout" default:"-"`
    LogLevel string             `long:"log_level" description:"log level" default:"INFO" choice:"DEBUG" choice:"INFO" choice:"Warning" choice:"ERROR"`
    DataDir string              `long:"data_dir" description:"data dir" default:"./data/"`
    DBConcurrentLock uint       `long:"db_concurrent_lock" description:"db concurrent lock count" default:"8"`
    DBLockAofTime uint          `long:"db_lock_aof_time" description:"db lock aof time" default:"2"`
    AofQueueSize uint           `long:"aof_queue_size" description:"aof channel queue size" default:"4096"`
    AofFileRewriteSize uint     `long:"aof_file_rewrite_size" description:"aof file rewrite size" default:"67174400"`
    AofFileBufferSize uint      `long:"aof_file_buffer_size" description:"aof file buffer size" default:"4096"`
}

var Config *ServerConfig = nil

func GetConfig() *ServerConfig{
    return Config
}

func SetConfig(config *ServerConfig) {
    Config = config
}