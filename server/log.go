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

func InitFileLogger(logFile string, formatter logging.Formatter) logging.Handler {
	handler := logging.MustNewRotatingFileHandler(
		logFile, os.O_APPEND, int(Config.LogBufferSize), time.Duration(Config.LogBufferFlushTime)*time.Second, 64,
		uint64(Config.LogRotatingSize), uint32(Config.LogBackupCount))

	handler.SetFormatter(formatter)
	return handler
}

func InitLogger(logFile string, logLevel string) logging.Logger {
	logger := logging.GetLogger("")
	formatter := GetFormatter()

	loggingLevel := logging.LevelInfo
	switch logLevel {
	case "DEBUG":
		loggingLevel = logging.LevelDebug
	case "WARNING":
		loggingLevel = logging.LevelWarning
	case "ERROR":
		loggingLevel = logging.LevelError
	default:
		loggingLevel = logging.LevelInfo
	}

	if logFile == "" || logFile == "-" {
		handler := InitConsoleLogger(formatter)
		_ = handler.SetLevel(loggingLevel)
		_ = logger.SetLevel(loggingLevel)
		logger.AddHandler(handler)
		logger.Infof("Logger start consolelogger %s %s", logLevel, logFile)
	} else {
		handler := InitFileLogger(logFile, formatter)
		_ = handler.SetLevel(loggingLevel)
		_ = logger.SetLevel(loggingLevel)
		logger.AddHandler(handler)
		logger.Infof("Logger start filelogger %s %s", logLevel, logFile)
	}
	return logger
}
