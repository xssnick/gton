package logutil

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type Config struct {
	Level     zerolog.Level
	Overrides map[string]zerolog.Level
	JSON      bool
}

type Factory struct {
	writer    io.Writer
	level     zerolog.Level
	overrides map[string]zerolog.Level
}

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
	return newLogger(logWriter(out, useJSON), level)
}

func NewFactory(out io.Writer, cfg Config) Factory {
	overrides := make(map[string]zerolog.Level, len(cfg.Overrides))
	for category, level := range cfg.Overrides {
		category = normalizeCategory(category)
		if category == "" {
			continue
		}
		overrides[category] = level
	}

	return Factory{
		writer:    logWriter(out, cfg.JSON),
		level:     cfg.Level,
		overrides: overrides,
	}
}

func ParseLevelOverrides(raw string) (map[string]zerolog.Level, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	items := strings.Split(raw, ",")
	overrides := make(map[string]zerolog.Level, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		category, levelRaw, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("invalid log level override %q, expected category=level", item)
		}

		category = normalizeCategory(category)
		if category == "" {
			return nil, fmt.Errorf("invalid log level override %q: category is empty", item)
		}

		level, err := ParseLevel(levelRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid log level override %q: %w", item, err)
		}
		overrides[category] = level
	}
	return overrides, nil
}

func FormatLevelOverrides(overrides map[string]zerolog.Level) string {
	if len(overrides) == 0 {
		return ""
	}

	categories := make([]string, 0, len(overrides))
	for category := range overrides {
		categories = append(categories, category)
	}
	sort.Strings(categories)

	parts := make([]string, 0, len(categories))
	for _, category := range categories {
		parts = append(parts, category+"="+overrides[category].String())
	}
	return strings.Join(parts, ",")
}

func (f Factory) Category(category string) zerolog.Logger {
	return newLogger(f.writer, f.LevelFor(category))
}

func (f Factory) CategoryPtr(category string) *zerolog.Logger {
	logger := f.Category(category)
	return &logger
}

func (f Factory) Component(component string) zerolog.Logger {
	logger := f.Category(component)
	if component == "" {
		return logger
	}
	return logger.With().Str("component", component).Logger()
}

func (f Factory) LevelFor(category string) zerolog.Level {
	if level, ok := f.overrides[normalizeCategory(category)]; ok {
		return level
	}
	return f.level
}

func logWriter(out io.Writer, useJSON bool) io.Writer {
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

	return writer
}

func newLogger(writer io.Writer, level zerolog.Level) zerolog.Logger {
	return zerolog.New(writer).Level(level).With().Timestamp().Logger()
}

func normalizeCategory(category string) string {
	return strings.ToLower(strings.TrimSpace(category))
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
