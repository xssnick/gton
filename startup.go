package gton

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

func signalContexts(parent context.Context) (context.Context, context.Context, func()) {
	runCtx, cancelRun := context.WithCancel(parent)
	shutdownCtx, cancelShutdown := context.WithCancel(parent)
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var stopOnce sync.Once
	stopped := make(chan struct{})
	stop := func() {
		stopOnce.Do(func() {
			close(stopped)
			signal.Stop(signals)
			cancelRun()
			cancelShutdown()
		})
	}

	go func() {
		select {
		case <-parent.Done():
			stop()
		case <-stopped:
		}
	}()

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

func zeroStateBlockFromGlobalConfig(cfg *liteclient.GlobalConfig) (ton.BlockIDExt, error) {
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

func startMetricsServer(ctx context.Context, logger zerolog.Logger, addr string, handler http.Handler) error {
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
