package logutil

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

func ParseLevel(raw string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "trace":
		return zerolog.TraceLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	case "", "info":
		return zerolog.InfoLevel, nil
	case "warn", "warning":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return 0, fmt.Errorf("expected one of: trace, debug, info, warn, error")
	}
}

func New(out io.Writer, level zerolog.Level, useJSON bool) zerolog.Logger {
	if out == nil {
		out = os.Stdout
	}

	writer := out
	if !useJSON {
		console := zerolog.ConsoleWriter{
			Out:        out,
			TimeFormat: "2006-01-02 15:04:05",
		}
		console.FormatLevel = func(value interface{}) string {
			return strings.ToUpper(fmt.Sprintf("%-5v", value))
		}
		console.FormatFieldName = func(value interface{}) string {
			return fmt.Sprintf("%s=", value)
		}
		writer = console
	}

	return zerolog.New(writer).Level(level).With().Timestamp().Logger()
}

func WithComponent(base *zerolog.Logger, component string) zerolog.Logger {
	logger := zerolog.Nop()
	if base != nil {
		logger = *base
	}
	if component == "" {
		return logger
	}
	return logger.With().Str("component", component).Logger()
}

func Discard() zerolog.Logger {
	return New(io.Discard, zerolog.DebugLevel, false)
}

func init() {
	zerolog.TimeFieldFormat = time.RFC3339
}
