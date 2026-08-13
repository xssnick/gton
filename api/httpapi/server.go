package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/xssnick/gton/service/externalmsg"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
)

const (
	DefaultAPIVersion     = "v2.1.13-7b98025"
	maxBodyBytes          = int64(8 << 20)
	defaultRequestTimeout = 10 * time.Second
)

type Options struct {
	Logger         *zerolog.Logger
	Store          Store
	Network        Network
	TVM            *tvm.TVM
	ListenAddr     string
	RequestTimeout time.Duration
	Now            func() time.Time
	ZeroState      ton.ZeroStateIDExt
}

type Server struct {
	log            zerolog.Logger
	store          Store
	messageSender  *externalmsg.Sender
	tvm            *tvm.TVM
	listenAddr     string
	requestTimeout time.Duration
	now            func() time.Time
	zeroState      ton.ZeroStateIDExt
	routes         map[string]route
	suspended      suspendedAddressCache

	server   *http.Server
	listener net.Listener
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func New(opts Options) (*Server, error) {
	if strings.TrimSpace(opts.ListenAddr) == "" {
		return nil, fmt.Errorf("http api listen address is empty")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("http api store is required")
	}

	log := zerolog.Nop()
	if opts.Logger != nil {
		log = opts.Logger.With().Str("component", "httpapi").Logger()
	}

	requestTimeout := opts.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultRequestTimeout
	}
	if requestTimeout < 0 {
		return nil, fmt.Errorf("http api request timeout cannot be negative")
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	tvmInstance := opts.TVM
	if tvmInstance == nil {
		tvmInstance = tvm.NewTVM()
	}

	var messageSender *externalmsg.Sender
	if opts.Network != nil {
		var err error
		messageSender, err = externalmsg.NewSender(externalmsg.SenderOptions{
			Check:     opts.Network.CheckExternalMessage,
			Broadcast: opts.Network.SendCheckedExternalMessage,
			Now:       now,
		})
		if err != nil {
			return nil, err
		}
	}

	server := &Server{
		log:            log,
		store:          opts.Store,
		messageSender:  messageSender,
		tvm:            tvmInstance,
		listenAddr:     strings.TrimSpace(opts.ListenAddr),
		requestTimeout: requestTimeout,
		now:            now,
		zeroState:      cloneZeroState(opts.ZeroState),
	}
	server.routes = server.buildRoutes()
	return server, nil
}

func (s *Server) Start(ctx context.Context) error {
	if s.server != nil {
		return fmt.Errorf("http api already started")
	}

	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}

	s.listener = listener
	s.server = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       s.requestTimeout,
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-runCtx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.log.Warn().Err(err).Str("listen_addr", s.listenAddr).Msg("failed to stop http api server")
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		s.log.Info().Str("listen_addr", s.listenAddr).Msg("started http api server")
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error().Err(err).Str("listen_addr", s.listenAddr).Msg("http api server stopped")
		}
	}()

	return nil
}

func (s *Server) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.server == nil {
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil {
		return errors.Join(err, s.server.Close())
	}
	return nil
}

func (s *Server) Wait() {
	s.wg.Wait()
}
