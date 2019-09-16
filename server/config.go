package server

type ServerConfig struct{
    Bind string                 `long:"bind" description:"bind address" default:"127.0.0.1"`
    Port uint                   `long:"port" description:"bind port" default:"5658"`
    Log  string                 `long:"log" description:"log filename, default is output stdout" default:"-"`
    LogLevel string             `long:"log_level" description:"log level" default:"INFO" choice:"DEBUG" choice:"INFO" choice:"Warning" choice:"ERROR"`
    DataDir string              `long:"data_dir" description:"data dir" default:"./data/"`
    DBConcurrentLock uint       `long:"db_concurrent_lock" description:"db concurrent lock count" default:"64"`
    DBLockAofTime uint          `long:"db_lock_aof_time" description:"db lock aof time" default:"2"`
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