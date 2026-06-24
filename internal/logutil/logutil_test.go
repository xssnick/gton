package logutil

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestParseLevelOverrides(t *testing.T) {
	overrides, err := ParseLevelOverrides("liteserver=debug, p2p=warn, pebblestore=error")
	if err != nil {
		t.Fatalf("parse overrides: %v", err)
	}

	if overrides["liteserver"] != zerolog.DebugLevel {
		t.Fatalf("liteserver level = %s, want debug", overrides["liteserver"])
	}
	if overrides["p2p"] != zerolog.WarnLevel {
		t.Fatalf("p2p level = %s, want warn", overrides["p2p"])
	}
	if overrides["pebblestore"] != zerolog.ErrorLevel {
		t.Fatalf("pebblestore level = %s, want error", overrides["pebblestore"])
	}
}

func TestParseLevelOverridesRejectsInvalidInput(t *testing.T) {
	if _, err := ParseLevelOverrides("liteserver"); err == nil {
		t.Fatal("expected missing level separator error")
	}
	if _, err := ParseLevelOverrides("=debug"); err == nil {
		t.Fatal("expected empty category error")
	}
	if _, err := ParseLevelOverrides("liteserver=nope"); err == nil {
		t.Fatal("expected invalid level error")
	}
}

func TestFactoryAppliesCategoryOverride(t *testing.T) {
	var out bytes.Buffer
	factory := NewFactory(&out, Config{
		Level: zerolog.InfoLevel,
		Overrides: map[string]zerolog.Level{
			"liteserver": zerolog.DebugLevel,
			"p2p":        zerolog.WarnLevel,
		},
		JSON: true,
	})

	liteserver := factory.Category("liteserver")
	if !liteserver.Debug().Enabled() {
		t.Fatal("liteserver debug should be enabled")
	}
	p2p := factory.Category("p2p")
	if p2p.Info().Enabled() {
		t.Fatal("p2p info should be disabled")
	}
	service := factory.Category("service")
	if service.Debug().Enabled() {
		t.Fatal("service debug should inherit global info level")
	}
}

func TestFactoryComponentUsesCategoryLevel(t *testing.T) {
	var out bytes.Buffer
	factory := NewFactory(&out, Config{
		Level: zerolog.InfoLevel,
		Overrides: map[string]zerolog.Level{
			"service": zerolog.DebugLevel,
		},
		JSON: true,
	})

	logger := factory.Component("service")
	if !logger.Debug().Enabled() {
		t.Fatal("service component debug should be enabled")
	}
}

func TestFactoryCategoryWritesOverrideWithoutComponentField(t *testing.T) {
	var out bytes.Buffer
	factory := NewFactory(&out, Config{
		Level: zerolog.InfoLevel,
		Overrides: map[string]zerolog.Level{
			"p2p": zerolog.DebugLevel,
		},
		JSON: true,
	})

	logger := factory.Category("p2p")
	logger.Debug().Msg("category debug")

	if got := out.String(); !strings.Contains(got, `"message":"category debug"`) {
		t.Fatalf("category logger should write debug override, got %s", got)
	}
}

func TestFactoryBaseLoggerFiltersOverridesByComponent(t *testing.T) {
	var out bytes.Buffer
	factory := NewFactory(&out, Config{
		Level: zerolog.InfoLevel,
		Overrides: map[string]zerolog.Level{
			"p2p": zerolog.DebugLevel,
		},
		JSON: true,
	})

	logger := factory.Base()
	logger.Debug().Str("component", "p2p").Msg("p2p debug")
	logger.Debug().Str("component", "service").Msg("service debug")
	logger.Info().Str("component", "service").Msg("service info")

	got := out.String()
	if !strings.Contains(got, `"message":"p2p debug"`) {
		t.Fatalf("base logger should include p2p debug override, got %s", got)
	}
	if strings.Contains(got, `"message":"service debug"`) {
		t.Fatalf("base logger should filter service debug at global info level, got %s", got)
	}
	if !strings.Contains(got, `"message":"service info"`) {
		t.Fatalf("base logger should include service info, got %s", got)
	}
}

func TestFormatLevelOverridesIsStable(t *testing.T) {
	got := FormatLevelOverrides(map[string]zerolog.Level{
		"p2p":        zerolog.WarnLevel,
		"liteserver": zerolog.DebugLevel,
	})
	want := "liteserver=debug,p2p=warn"
	if got != want {
		t.Fatalf("formatted overrides = %q, want %q", got, want)
	}
}

func TestFormatByteRate(t *testing.T) {
	tests := []struct {
		name    string
		bytes   int64
		elapsed time.Duration
		want    string
	}{
		{name: "zero bytes", bytes: 0, elapsed: time.Second, want: "0 B/s"},
		{name: "bytes", bytes: 512, elapsed: time.Second, want: "512 B/s"},
		{name: "kibibytes", bytes: 1536, elapsed: time.Second, want: "1.50 KB/s"},
		{name: "mebibytes", bytes: 5 << 20, elapsed: 2 * time.Second, want: "2.50 MB/s"},
		{name: "gibibytes", bytes: 3 << 30, elapsed: time.Second, want: "3.00 GB/s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatByteRate(tt.bytes, tt.elapsed); got != tt.want {
				t.Fatalf("FormatByteRate() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatCellRate(t *testing.T) {
	tests := []struct {
		name    string
		cells   uint64
		elapsed time.Duration
		want    string
	}{
		{name: "zero cells", cells: 0, elapsed: time.Second, want: "0 cells/s"},
		{name: "cells", cells: 512, elapsed: time.Second, want: "512 cells/s"},
		{name: "kilocells", cells: 1536, elapsed: time.Second, want: "1.54 Kcells/s"},
		{name: "megacells", cells: 5_000_000, elapsed: 2 * time.Second, want: "2.50 Mcells/s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatCellRate(tt.cells, tt.elapsed); got != tt.want {
				t.Fatalf("FormatCellRate() = %q, want %q", got, tt.want)
			}
		})
	}
}
