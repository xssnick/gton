package gton

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/console"
)

type metricsServer struct {
	server *http.Server
	done   chan struct{}
}

func metricsHTTPHandler(logger zerolog.Logger, metrics http.Handler, commands *console.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics)
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "status requires GET", http.StatusMethodNotAllowed)
			return
		}

		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil || len(query["mode"]) > 1 {
			http.Error(w, "invalid status query", http.StatusBadRequest)
			return
		}

		command := "status"
		switch query.Get("mode") {
		case "", "short":
		case "full":
			command = "status full"
		case "db":
			command = "status db"
		default:
			http.Error(w, "status mode must be short, full or db", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		output, err := commands.Execute(ctx, command)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			logger.Warn().Err(err).Str("command", command).Msg("http status command failed")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err = io.WriteString(w, output); err != nil {
			logger.Debug().Err(err).Msg("http status response write failed")
		}
	})

	return mux
}

func startMetricsServer(
	ctx context.Context,
	logger zerolog.Logger,
	addr string,
	handler http.Handler,
) (*metricsServer, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	server := &metricsServer{
		server: &http.Server{
			Addr:              listener.Addr().String(),
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
			BaseContext: func(net.Listener) context.Context {
				return ctx
			},
		},
		done: make(chan struct{}),
	}

	go func() {
		defer close(server.done)
		if err := server.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Str("metrics_addr", addr).Msg("metrics server stopped")
		}
	}()

	logger.Info().
		Str("metrics_addr", listener.Addr().String()).
		Str("metrics_url", "http://"+listener.Addr().String()+"/metrics").
		Str("status_url", "http://"+listener.Addr().String()+"/status").
		Msg("started metrics and status server")

	return server, nil
}

func (s *metricsServer) Close(ctx context.Context) error {
	if err := s.server.Shutdown(ctx); err != nil {
		return err
	}
	<-s.done

	return nil
}
