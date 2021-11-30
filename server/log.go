package server

import (
	"github.com/hhkbp2/go-logging"
	"os"
	"strings"
	"time"
)

func initFormatter() (logging.Formatter, error) {
	formatter := logging.NewStandardFormatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S.%3n")
	return formatter, nil
}

func initConsoleLogger(formatter logging.Formatter) (logging.Handler, error) {
	handler := logging.NewStdoutHandler()
	handler.SetFormatter(formatter)
	return handler, nil
}

func initFileLogger(config *ServerConfig, formatter logging.Formatter) (logging.Handler, error) {
	handler, err := logging.NewRotatingFileHandler(
		config.Log, os.O_APPEND, int(config.LogBufferSize), time.Duration(config.LogBufferFlushTime)*time.Second, 64,
		uint64(config.LogRotatingSize), uint32(config.LogBackupCount))
	if err != nil {
		return nil, err
	}

	handler.SetFormatter(formatter)
	return handler, nil
}

func InitLogger(config *ServerConfig) (logging.Logger, error) {
	logger := logging.GetLogger("")
	formatter, err := initFormatter()
	if err != nil {
		return nil, err
	}

	loggingLevel := logging.LevelInfo
	switch strings.ToUpper(config.LogLevel) {
	case "DEBUG":
		loggingLevel = logging.LevelDebug
	case "WARNING":
		loggingLevel = logging.LevelWarning
	case "ERROR":
		loggingLevel = logging.LevelError
	default:
		loggingLevel = logging.LevelInfo
	}

	if config.Log == "" || config.Log == "-" {
		handler, err := initConsoleLogger(formatter)
		if err != nil {
			return nil, err
		}
		_ = handler.SetLevel(loggingLevel)
		_ = logger.SetLevel(loggingLevel)
		logger.AddHandler(handler)
		logger.Infof("Logger start consolelogger %s %s", config.LogLevel, config.Log)
		return logger, nil
	}

	handler, err := initFileLogger(config, formatter)
	if err != nil {
		return nil, err
	}
	_ = handler.SetLevel(loggingLevel)
	_ = logger.SetLevel(loggingLevel)
	logger.AddHandler(handler)
	logger.Infof("Logger start filelogger %s %s", config.LogLevel, config.Log)
	return logger, nil
}
