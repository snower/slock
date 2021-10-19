package server

const VERSION = "2.0.1"

const QUEUE_MAX_MALLOC_SIZE = 0x3fffff

const TIMEOUT_QUEUE_LENGTH int64 = 0x10
const EXPRIED_QUEUE_LENGTH int64 = 0x10
const TIMEOUT_QUEUE_LENGTH_MASK int64 = 0x0f
const EXPRIED_QUEUE_LENGTH_MASK int64 = 0x0f
const TIMEOUT_QUEUE_MAX_WAIT uint8 = 0x08
const EXPRIED_QUEUE_MAX_WAIT uint8 = 0x08

const MANAGER_MAX_GLOCKS_INIT_SIZE = 1024 * 256
const TIMEOUT_LOCKS_QUEUE_INIT_SIZE = 4096
const EXPRIED_LOCKS_QUEUE_INIT_SIZE = 4096
const LONG_TIMEOUT_LOCKS_INIT_COUNT = 16384
const LONG_EXPRIED_LOCKS_INIT_COUNT = 16384
const LONG_LOCKS_QUEUE_INIT_SIZE = 256
const MILLISECOND_LOCKS_QUEUE_INIT_SIZE = 256
const FREE_LOCK_QUEUE_INIT_SIZE = 4096
const FREE_LONG_WAIT_QUEUE_INIT_SIZE = 8096
const FREE_MILLISECOND_WAIT_QUEUE_INIT_SIZE = 8096
const FREE_COMMAND_MAX_SIZE = 256
const FREE_COMMAND_QUEUE_INIT_SIZE = 256
const STREAMS_INIT_COUNT = 65536

const REPLICATION_ACK_DB_INIT_SIZE = 1024 * 1024
const REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE = 4096

type ServerConfig struct {
	Bind                 string  `long:"bind" description:"bind address" default:"127.0.0.1"`
	Port                 uint    `long:"port" description:"bind port" default:"5658"`
	Log                  string  `long:"log" description:"log filename, default is output stdout" default:"-"`
	LogLevel             string  `long:"log_level" description:"log level" default:"INFO" choice:"DEBUG" choice:"INFO" choice:"Warning" choice:"ERROR"`
	LogRotatingSize      uint    `long:"log_rotating_size" description:"log rotating byte size" default:"67108864"`
	LogBackupCount       uint    `long:"log_backup_count" description:"log backup count" default:"5"`
	LogBufferSize        uint    `long:"log_buffer_size" description:"log buffer byte size" default:"0"`
	LogBufferFlushTime   uint    `long:"log_buffer_flush_time" description:"log buffer flush seconds time" default:"1"`
	DataDir              string  `long:"data_dir" description:"data dir" default:"./data/"`
	DBFastKeyCount       uint    `long:"db_fast_key_count" description:"db fast key count" default:"4194304"`
	DBConcurrent         uint    `long:"db_concurrent" description:"db concurrent count" default:"8"`
	DBLockAofTime        uint    `long:"db_lock_aof_time" description:"db lock aof time" default:"1"`
	DBLockAofParcentTime float64 `long:"db_lock_aof_parcent_time" description:"db lock aof parcent expried time" default:"0.3"`
	AofQueueSize         uint    `long:"aof_queue_size" description:"aof channel queue size" default:"65536"`
	AofFileRewriteSize   uint    `long:"aof_file_rewrite_size" description:"aof file rewrite size" default:"67174400"`
	AofFileBufferSize    uint    `long:"aof_file_buffer_size" description:"aof file buffer size" default:"4096"`
	AofRingBufferSize    uint    `long:"aof_ring_buffer_size" description:"slave sync ring buffer size" default:"1048576"`
	AofRingBufferMaxSize uint    `long:"aof_ring_buffer_max_size" description:"slave sync ring buffer max size" default:"268435456"`
	SlaveOf              string  `long:"slaveof" description:"slave of to master sync" default:""`
	ReplSet              string  `long:"replset" description:"replset name, start replset mode when it set" default:""`
}

var Config *ServerConfig = nil

func GetConfig() *ServerConfig {
	return Config
}

func SetConfig(config *ServerConfig) {
	Config = config
}
