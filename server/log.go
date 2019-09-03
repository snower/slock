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
        handler.SetLevel(logging_level)
        logger.SetLevel(logging_level)
        logger.AddHandler(handler)
        logger.Infof("start ConsoleLogger %s %s", log_level, log_file)
    } else {
        handler := InitFileLogger(log_file, formatter)
        handler.SetLevel(logging_level)
        logger.SetLevel(logging_level)
        logger.AddHandler(handler)
        logger.Infof("start FileLogger %s %s", log_level, log_file)
    }
    return logger
}
