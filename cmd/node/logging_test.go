package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewLogOutputReturnsConsoleOnlyWhenFileDisabled(t *testing.T) {
	var out bytes.Buffer

	writer, logFile, err := newLogOutput(&out, "", logRotationOptions{
		MaxSizeMB: defaultLogFileMaxSizeMB,
	})
	if err != nil {
		t.Fatalf("new log output: %v", err)
	}
	if logFile != nil {
		t.Fatal("expected no log file")
	}

	if _, err = writer.Write([]byte("console\n")); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if out.String() != "console\n" {
		t.Fatalf("console output = %q", out.String())
	}
}

func TestNewLogOutputWritesConsoleAndFile(t *testing.T) {
	var out bytes.Buffer
	path := filepath.Join(t.TempDir(), "logs", "node.log")

	writer, logFile, err := newLogOutput(&out, path, logRotationOptions{
		MaxSizeMB:  defaultLogFileMaxSizeMB,
		MaxBackups: 3,
		MaxAgeDays: 7,
		Compress:   true,
	})
	if err != nil {
		t.Fatalf("new log output: %v", err)
	}
	defer logFile.Close()

	if logFile.Filename != path {
		t.Fatalf("log filename = %q, want %q", logFile.Filename, path)
	}
	if logFile.MaxSize != defaultLogFileMaxSizeMB {
		t.Fatalf("log max size = %d, want %d", logFile.MaxSize, defaultLogFileMaxSizeMB)
	}
	if logFile.MaxBackups != 3 {
		t.Fatalf("log max backups = %d, want 3", logFile.MaxBackups)
	}
	if logFile.MaxAge != 7 {
		t.Fatalf("log max age = %d, want 7", logFile.MaxAge)
	}
	if !logFile.Compress {
		t.Fatal("expected compressed rotated logs")
	}

	if _, err = writer.Write([]byte("rotating\n")); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if out.String() != "rotating\n" {
		t.Fatalf("console output = %q", out.String())
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if string(data) != "rotating\n" {
		t.Fatalf("log file output = %q", data)
	}
}

func TestNewLogOutputRejectsInvalidRotation(t *testing.T) {
	type invalidLogRotationCase struct {
		name string
		opts logRotationOptions
		want string
	}

	tests := []invalidLogRotationCase{
		{name: "max size", opts: logRotationOptions{MaxSizeMB: 0}, want: "max size"},
		{name: "max backups", opts: logRotationOptions{MaxSizeMB: 1, MaxBackups: -1}, want: "max backups"},
		{name: "max age", opts: logRotationOptions{MaxSizeMB: 1, MaxAgeDays: -1}, want: "max age"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			_, _, err := newLogOutput(&out, filepath.Join(t.TempDir(), "node.log"), tt.opts)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want substring %q", err, tt.want)
			}
		})
	}
}

func TestNewLogOutputRejectsDirectoryPath(t *testing.T) {
	var out bytes.Buffer
	_, _, err := newLogOutput(&out, t.TempDir(), logRotationOptions{MaxSizeMB: defaultLogFileMaxSizeMB})
	if err == nil {
		t.Fatal("expected directory path error")
	}
}
