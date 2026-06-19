package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

func signalContexts() (context.Context, context.Context, func()) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			signal.Stop(signals)
			cancelRun()
			cancelShutdown()
		})
	}

	go func() {
		select {
		case <-signals:
			cancelRun()
		case <-runCtx.Done():
			return
		}

		select {
		case <-signals:
			cancelShutdown()
			signal.Stop(signals)
		case <-shutdownCtx.Done():
		}
	}()

	return runCtx, shutdownCtx, stop
}

type storedZeroStateReader interface {
	StoredZeroStateBlocks(ctx context.Context) ([]ton.BlockIDExt, error)
}

func zeroStateBlockFromGlobalConfig(path string) (ton.BlockIDExt, error) {
	cfg, err := liteclient.GetConfigFromFile(path)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	block := ton.BlockIDExt{
		Workchain: cfg.Validator.ZeroState.Workchain,
		Shard:     topShard,
		SeqNo:     0,
		RootHash:  append([]byte(nil), cfg.Validator.ZeroState.RootHash...),
		FileHash:  append([]byte(nil), cfg.Validator.ZeroState.FileHash...),
	}
	if block.Workchain != -1 || !validBlockID(block) {
		return ton.BlockIDExt{}, fmt.Errorf("global config contains invalid zero_state")
	}
	return block, nil
}

func zeroStateIDFromBlock(block ton.BlockIDExt) ton.ZeroStateIDExt {
	return ton.ZeroStateIDExt{
		Workchain: block.Workchain,
		RootHash:  append([]byte(nil), block.RootHash...),
		FileHash:  append([]byte(nil), block.FileHash...),
	}
}

func ensureStoredZeroStateMatchesGlobalConfig(ctx context.Context, store storedZeroStateReader, configured ton.BlockIDExt) error {
	stored, err := store.StoredZeroStateBlocks(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load stored zerostate: %w", err)
	}

	for _, block := range stored {
		if block.Workchain != configured.Workchain {
			continue
		}
		if !block.Equals(&configured) {
			return fmt.Errorf("stored zerostate %s does not match global config zerostate %s",
				formatBlockID(block), formatBlockID(configured))
		}
	}
	return nil
}

func validBlockID(block ton.BlockIDExt) bool {
	return len(block.RootHash) == 32 && len(block.FileHash) == 32
}

func formatBlockID(block ton.BlockIDExt) string {
	return fmt.Sprintf(
		"wc=%d shard=%016x seqno=%d root=%x file=%x",
		block.Workchain,
		uint64(block.Shard),
		block.SeqNo,
		block.RootHash,
		block.FileHash,
	)
}

func closeStorage(logger zerolog.Logger, store io.Closer) {
	started := time.Now()
	logger.Info().Msg("closing storage")
	if err := store.Close(); err != nil {
		logger.Error().Err(err).Dur("elapsed", time.Since(started)).Msg("failed to close storage")
		return
	}
	logger.Info().Dur("elapsed", time.Since(started)).Msg("storage closed")
}

func startPprof(ctx context.Context, logger zerolog.Logger, addr string) {
	if addr == "" {
		return
	}

	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn().Err(err).Str("pprof_addr", addr).Msg("failed to stop pprof server")
		}
	}()

	go func() {
		logger.Info().
			Str("pprof_addr", addr).
			Str("heap_url", "http://"+addr+"/debug/pprof/heap").
			Str("profile_url", "http://"+addr+"/debug/pprof/profile").
			Msg("started pprof server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Str("pprof_addr", addr).Msg("pprof server stopped")
		}
	}()
}

func startMetricsServer(ctx context.Context, logger zerolog.Logger, addr string, handler http.Handler) error {
	if addr == "" || handler == nil {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn().Err(err).Str("metrics_addr", addr).Msg("failed to stop metrics server")
		}
	}()

	go func() {
		logger.Info().
			Str("metrics_addr", addr).
			Str("metrics_url", "http://"+addr+"/metrics").
			Msg("started prometheus metrics server")
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Str("metrics_addr", addr).Msg("metrics server stopped")
		}
	}()

	return nil
}

func resolveConfigPath(longPath string) string {
	longPath = strings.TrimSpace(longPath)
	if longPath == "" {
		return nodeconfig.DefaultPath
	}
	return longPath
}

func displayConfigPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		path = nodeconfig.DefaultPath
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func logFormat(useJSON bool) string {
	if useJSON {
		return "json"
	}
	return "console"
}
