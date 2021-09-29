package server

import (
	"github.com/hhkbp2/go-logging"
	"os"
	"time"
)

func GetFormatter() logging.Formatter {
	formatter := logging.NewStandardFormatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S.%3n")
	return formatter
}

func InitConsoleLogger(formatter logging.Formatter) logging.Handler {
	handler := logging.NewStdoutHandler()
	handler.SetFormatter(formatter)
	return handler
}

func InitFileLogger(log_file string, formatter logging.Formatter) logging.Handler {
	handler := logging.MustNewRotatingFileHandler(
		log_file, os.O_APPEND, int(Config.LogBufferSize), time.Duration(Config.LogBufferFlushTime)*time.Second, 64,
		uint64(Config.LogRotatingSize), uint32(Config.LogBackupCount))

	handler.SetFormatter(formatter)
	return handler
}

func InitLogger(log_file string, log_level string) logging.Logger {
	logger := logging.GetLogger("")
	formatter := GetFormatter()

	logging_level := logging.LevelInfo
	switch log_level {
	case "DEBUG":
		logging_level = logging.LevelDebug
	case "WARNING":
		logging_level = logging.LevelWarning
	case "ERROR":
		logging_level = logging.LevelError
	default:
		logging_level = logging.LevelInfo
	}

	if log_file == "" || log_file == "-" {
		handler := InitConsoleLogger(formatter)
		_ = handler.SetLevel(logging_level)
		_ = logger.SetLevel(logging_level)
		logger.AddHandler(handler)
		logger.Infof("Logger start consolelogger %s %s", log_level, log_file)
	} else {
		handler := InitFileLogger(log_file, formatter)
		_ = handler.SetLevel(logging_level)
		_ = logger.SetLevel(logging_level)
		logger.AddHandler(handler)
		logger.Infof("Logger start filelogger %s %s", log_level, log_file)
	}
	return logger
}
