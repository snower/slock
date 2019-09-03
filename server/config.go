package server

type ServerConfig struct{
    Bind string                 `long:"bind" description:"bind address" default:"127.0.0.1"`
    Port uint                   `long:"port" description:"bind port" default:"5658"`
    Log  string                 `long:"log" description:"log filename, default is output stdout" default:"-"`
    LogLevel string             `long:"log_level" description:"log level" default:"INFO" choice:"DEBUG" choice:"INFO" choice:"Warning" choice:"ERROR"`
    DBConcurrentLock uint       `long:"db_concurrent_lock" description:"db concurrent lock count" default:"64"`
}

var Config *ServerConfig = nil

func GetConfig() *ServerConfig{
    return Config
}

func SetConfig(config *ServerConfig) {
    Config = config
}