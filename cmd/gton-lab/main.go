package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	exitCode, err := runCLI(ctx, os.Args[1:], os.Stdout)
	if err != nil {
		logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
		if format := os.Getenv("GTON_LAB_LOG_FORMAT"); format == "console" {
			logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
		}
		logger.Error().Err(err).Msg("gton-lab command failed")
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		exitCode = 130
	}
	os.Exit(exitCode)
}
