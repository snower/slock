package server

const VERSION = "2.0.1"

const QUEUE_MAX_MALLOC_SIZE = 0x3fffff

const TIMEOUT_QUEUE_LENGTH int64 = 0x10
const EXPRIED_QUEUE_LENGTH int64 = 0x10
const TIMEOUT_QUEUE_LENGTH_MASK int64 = 0x0f
const EXPRIED_QUEUE_LENGTH_MASK int64 = 0x0f
const TIMEOUT_QUEUE_MAX_WAIT uint8 = 0x08
const EXPRIED_QUEUE_MAX_WAIT uint8 = 0x08
const EXPRIED_WAIT_LEADER_MAX_TIME int64 = 300
const MILLISECOND_QUEUE_LENGTH = 3000

const MANAGER_MAX_GLOCKS_INIT_SIZE = 262144
const TIMEOUT_LOCKS_QUEUE_INIT_SIZE = 4096
const EXPRIED_LOCKS_QUEUE_INIT_SIZE = 4096
const LONG_TIMEOUT_LOCKS_INIT_COUNT = 256
const LONG_EXPRIED_LOCKS_INIT_COUNT = 256
const LONG_LOCKS_QUEUE_INIT_SIZE = 1024
const MILLISECOND_LOCKS_QUEUE_INIT_SIZE = 1024
const FREE_LOCK_QUEUE_INIT_SIZE = 4096
const FREE_LONG_WAIT_QUEUE_INIT_SIZE = 8096
const FREE_MILLISECOND_WAIT_QUEUE_INIT_SIZE = 8096
const FREE_COMMAND_MAX_SIZE = 256
const FREE_COMMAND_QUEUE_INIT_SIZE = 256
const STREAMS_INIT_COUNT = 256

const REPLICATION_ACK_DB_INIT_SIZE = 256
const REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE = 4096

type ServerConfig struct {
	Conf                 string  `toml:"conf" long:"conf" description:"toml conf filename" default:""`
	Bind                 string  `toml:"bind" long:"bind" description:"bind address" default:"127.0.0.1"`
	Port                 uint    `toml:"port" long:"port" description:"bind port" default:"5658"`
	Log                  string  `toml:"log" long:"log" description:"log filename, default is output stdout" default:"-"`
	LogLevel             string  `toml:"log_level" long:"log_level" description:"log level" default:"INFO" choice:"DEBUG" choice:"INFO" choice:"Warning" choice:"ERROR"`
	LogRotatingSize      uint    `toml:"log_rotating_size" long:"log_rotating_size" description:"log rotating byte size" default:"67108864"`
	LogBackupCount       uint    `toml:"log_backup_count" long:"log_backup_count" description:"log backup count" default:"5"`
	LogBufferSize        uint    `toml:"log_buffer_size" long:"log_buffer_size" description:"log buffer byte size" default:"0"`
	LogBufferFlushTime   uint    `toml:"log_buffer_flush_time" long:"log_buffer_flush_time" description:"log buffer flush seconds time" default:"1"`
	DataDir              string  `toml:"data_dir" long:"data_dir" description:"data dir" default:"./data/"`
	DBFastKeyCount       uint    `toml:"db_fast_key_count" long:"db_fast_key_count" description:"db fast key count" default:"4194304"`
	DBConcurrent         uint    `toml:"db_concurrent" long:"db_concurrent" description:"db concurrent count" default:"8"`
	DBLockAofTime        uint    `toml:"db_lock_aof_time" long:"db_lock_aof_time" description:"db lock aof time" default:"1"`
	DBLockAofParcentTime float64 `toml:"db_lock_aof_parcent_time" long:"db_lock_aof_parcent_time" description:"db lock aof parcent expried time" default:"0.3"`
	AofQueueSize         uint    `toml:"aof_queue_size" long:"aof_queue_size" description:"aof channel queue size" default:"65536"`
	AofFileRewriteSize   uint    `toml:"aof_file_rewrite_size" long:"aof_file_rewrite_size" description:"aof file rewrite size" default:"67174400"`
	AofFileBufferSize    uint    `toml:"aof_file_buffer_size" long:"aof_file_buffer_size" description:"aof file buffer size" default:"4096"`
	AofRingBufferSize    uint    `toml:"aof_ring_buffer_size" long:"aof_ring_buffer_size" description:"slave sync ring buffer size" default:"4194304"`
	AofRingBufferMaxSize uint    `toml:"aof_ring_buffer_max_size" long:"aof_ring_buffer_max_size" description:"slave sync ring buffer max size" default:"268435456"`
	SlaveOf              string  `toml:"slaveof" long:"slaveof" description:"slave of to master sync" default:""`
	ReplSet              string  `toml:"replset" long:"replset" description:"replset name, start replset mode when it set" default:""`
}

var Config *ServerConfig = nil

func GetConfig() *ServerConfig {
	return Config
}

func SetConfig(config *ServerConfig) {
	Config = config
}

func ExtendConfig(config *ServerConfig, oconfig *ServerConfig) *ServerConfig {
	if config.Bind == "127.0.0.1" && oconfig.Bind != "" {
		config.Bind = oconfig.Bind
	}
	if config.Port == 5658 && oconfig.Port != 0 {
		config.Port = oconfig.Port
	}
	if config.Log == "-" && oconfig.Log != "" {
		config.Log = oconfig.Log
	}
	if config.LogLevel == "INFO" && oconfig.LogLevel != "" {
		config.LogLevel = oconfig.LogLevel
	}
	if config.LogRotatingSize == 67108864 && oconfig.LogRotatingSize != 0 {
		config.LogRotatingSize = oconfig.LogRotatingSize
	}
	if config.LogBackupCount == 5 && oconfig.LogBackupCount != 0 {
		config.LogBackupCount = oconfig.LogBackupCount
	}
	if config.LogBufferSize == 0 && oconfig.LogBufferSize != 0 {
		config.LogBufferSize = oconfig.LogBufferSize
	}
	if config.LogBufferFlushTime == 1 && oconfig.LogBufferFlushTime != 0 {
		config.LogBufferFlushTime = oconfig.LogBufferFlushTime
	}
	if config.DataDir == "./data/" && oconfig.DataDir != "" {
		config.DataDir = oconfig.DataDir
	}
	if config.DBFastKeyCount == 4194304 && oconfig.DBFastKeyCount != 0 {
		config.DBFastKeyCount = oconfig.DBFastKeyCount
	}
	if config.DBConcurrent == 8 && oconfig.DBConcurrent != 0 {
		config.DBConcurrent = oconfig.DBConcurrent
	}
	if config.DBLockAofTime == 1 && oconfig.DBLockAofTime != 0 {
		config.DBLockAofTime = oconfig.DBLockAofTime
	}
	if config.DBLockAofParcentTime == 0.3 && oconfig.DBLockAofParcentTime != 0 {
		config.DBLockAofParcentTime = oconfig.DBLockAofParcentTime
	}
	if config.AofQueueSize == 65536 && oconfig.AofQueueSize != 0 {
		config.AofQueueSize = oconfig.AofQueueSize
	}
	if config.AofFileRewriteSize == 67174400 && oconfig.AofFileRewriteSize != 0 {
		config.AofFileRewriteSize = oconfig.AofFileRewriteSize
	}
	if config.AofFileBufferSize == 4096 && oconfig.AofFileBufferSize != 0 {
		config.AofFileBufferSize = oconfig.AofFileBufferSize
	}
	if config.AofRingBufferSize == 4194304 && oconfig.AofRingBufferSize != 0 {
		config.AofRingBufferSize = oconfig.AofRingBufferSize
	}
	if config.AofRingBufferMaxSize == 268435456 && oconfig.AofRingBufferMaxSize != 0 {
		config.AofRingBufferMaxSize = oconfig.AofRingBufferMaxSize
	}
	if config.SlaveOf == "" && oconfig.SlaveOf != "" {
		config.SlaveOf = oconfig.SlaveOf
	}
	if config.ReplSet == "" && oconfig.ReplSet != "" {
		config.ReplSet = oconfig.ReplSet
	}
	return config
}
