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
        log_file, os.O_APPEND, 0, 1*time.Second, 1,
        uint64(1024*1024*1024), 5)

    handler.SetFormatter(formatter)
    return handler
}

func InitLogger(log_file string, log_level string) logging.Logger {
    logger := logging.GetLogger("")
    formatter := GetFormatter()

    if log_file == "" || log_file == "-" {
        handler := InitConsoleLogger(formatter)
        if log_level == "ERROR" {
            handler.SetLevel(logging.LevelError)
            logger.SetLevel(logging.LevelError)
        } else {
            handler.SetLevel(logging.LevelInfo)
            logger.SetLevel(logging.LevelInfo)
        }

        logger.AddHandler(handler)
        logger.Infof("start ConsoleLogger %s %s", log_level, log_file)
    } else {
        handler := InitFileLogger(log_file, formatter)
        if log_level == "ERROR" {
            handler.SetLevel(logging.LevelError)
            logger.SetLevel(logging.LevelError)
        } else {
            handler.SetLevel(logging.LevelInfo)
            logger.SetLevel(logging.LevelInfo)
        }

        logger.AddHandler(handler)
        logger.Infof("start FileLogger %s %s", log_level, log_file)
    }
    return logger
}
