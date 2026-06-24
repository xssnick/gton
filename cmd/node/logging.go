package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/natefinch/lumberjack"
)

const (
	defaultLogFileMaxSizeMB  = 100
	defaultLogFileMaxBackups = 10
	defaultLogFileMaxAgeDays = 30
)

type logFileOptions struct {
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
	Compress   bool
}

func defaultLogFileOptions() logFileOptions {
	return logFileOptions{
		MaxSizeMB:  defaultLogFileMaxSizeMB,
		MaxBackups: defaultLogFileMaxBackups,
		MaxAgeDays: defaultLogFileMaxAgeDays,
	}
}

func newLogOutput(out io.Writer, filePath string, rotation logFileOptions) (io.Writer, *lumberjack.Logger, error) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return out, nil, nil
	}

	if rotation.MaxSizeMB <= 0 {
		return nil, nil, fmt.Errorf("log file max size must be positive")
	}
	if rotation.MaxBackups < 0 {
		return nil, nil, fmt.Errorf("log file max backups cannot be negative")
	}
	if rotation.MaxAgeDays < 0 {
		return nil, nil, fmt.Errorf("log file max age cannot be negative")
	}

	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, nil, fmt.Errorf("create log file directory %q: %w", dir, err)
		}
	}

	if info, err := os.Stat(filePath); err == nil && info.IsDir() {
		return nil, nil, fmt.Errorf("log file %q is a directory", filePath)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file %q: %w", filePath, err)
	}
	if err = file.Close(); err != nil {
		return nil, nil, fmt.Errorf("close log file %q: %w", filePath, err)
	}

	logFile := &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    rotation.MaxSizeMB,
		MaxBackups: rotation.MaxBackups,
		MaxAge:     rotation.MaxAgeDays,
		Compress:   rotation.Compress,
	}

	return io.MultiWriter(logFile, out), logFile, nil
}
