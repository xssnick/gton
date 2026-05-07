package logutil

import (
	"bytes"
	"testing"

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
