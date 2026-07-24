package liteserver

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	admission "github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultVersion      int32 = 0x101
	DefaultCapabilities int64 = 15
)

type Store interface {
	liveview.Backing
	CurrentAccountBlocks(ctx context.Context, workchain int32, account []byte) (liveview.CurrentAccountBlockIDs, error)
	BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
	BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveview.BlockView, error)
	CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error)
	MasterchainSeqnoReady(seqno uint32) bool
	WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error
	NonfinalPendingShardBlocks(filter *storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt)
}

// prefixLookupStore is an optional capability kept out of Store so released
// Store implementations remain source compatible.
type prefixLookupStore interface {
	LookupBlockBySeqNoForPrefix(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
	LookupBlockByLTForPrefix(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTimeForPrefix(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
}

type MessageSender interface {
	SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error
}

type MessageChecker interface {
	CheckExternalMessage(ctx context.Context, body []byte, root *cell.Cell, msg *tlb.ExternalMessage) (admission.CheckResult, error)
}

type QueryObserver interface {
	AddLiteserverInflight(delta int)
	ObserveLiteserverQuery(QueryObservation)
}

type QueryObservation struct {
	Method       string
	Response     string
	Error        bool
	ErrorCode    int32
	ErrorReason  string
	Duration     time.Duration
	WaitDuration time.Duration
}

type Options struct {
	Logger                  *zerolog.Logger
	Store                   Store
	MessageSender           MessageSender
	QueryObserver           QueryObserver
	PrivateKey              ed25519.PrivateKey
	ListenAddr              string
	NonFinal                bool
	AllowDuplicateExternals bool
	ZeroState               ton.ZeroStateIDExt
	RequestLimits           RequestLimitOptions
	// QueryConcurrency bounds concurrently executing query handlers; parked
	// waitMasterchainSeqno requests and inline snapshot queries do not consume
	// it. Zero means GOMAXPROCS*4.
	QueryConcurrency int
}

type Server struct {
	log                     zerolog.Logger
	store                   Store
	messageSender           MessageSender
	externalMessageCheck    admission.CheckFunc
	queryObserver           QueryObserver
	privateKey              ed25519.PrivateKey
	listenAddr              string
	nonFinal                bool
	allowDuplicateExternals bool
	zeroState               ton.ZeroStateIDExt
	version                 int32
	capabilities            int64
	now                     func() time.Time
	tvm                     *tvm.TVM
	requestLimits           RequestLimitOptions

	sendMessageCache       *admission.MessageCache
	externalMessageSender  *admission.Sender
	externalMessageLimiter *extmsg.AddressLimiter
	requestLimiter         *requestLimiter
	connectionTracker      *clientConnectionTracker
	respCache              *liteResponseCache
	queryExecutor          *queryExecutor
	waitLimiter            *waitLimiter

	server *liteclient.Server
	cancel context.CancelFunc
	wg     sync.WaitGroup

	blockProofBasesMu   sync.Mutex
	blockProofBases     map[liteBlockKey]*blockProofBase
	blockProofBaseOrder []liteBlockKey
	blockProofBaseLoad  liveLoadGroup[liteBlockKey]
}

// New preserves the released standalone composition: it owns a TVM and uses
// the sender's checker when available, otherwise constructing the legacy local
// checker. Internal composition paths should pass ready dependencies to newServer.
func New(opts Options) (*Server, error) {
	if err := validateServerOptions(opts); err != nil {
		return nil, err
	}

	log := liteserverLogger(opts.Logger)
	tvmInstance := tvm.NewTVM()

	var checkExternalMessage admission.CheckFunc
	if messageChecker, ok := opts.MessageSender.(MessageChecker); ok && messageChecker != nil {
		checkExternalMessage = messageChecker.CheckExternalMessage
	} else {
		externalMessageChecker, err := admission.NewChecker(admission.Options{
			Logger: &log,
			Store:  opts.Store,
			TVM:    tvmInstance,
		})
		if err != nil {
			return nil, err
		}
		checkExternalMessage = externalMessageChecker.Check
	}

	return newServer(opts, tvmInstance, checkExternalMessage)
}

func newServer(opts Options, tvmInstance *tvm.TVM, checkExternalMessage admission.CheckFunc) (*Server, error) {
	if err := validateServerOptions(opts); err != nil {
		return nil, err
	}

	log := liteserverLogger(opts.Logger)
	server := &Server{
		log:                     log,
		store:                   opts.Store,
		messageSender:           opts.MessageSender,
		externalMessageCheck:    checkExternalMessage,
		queryObserver:           opts.QueryObserver,
		privateKey:              opts.PrivateKey,
		listenAddr:              opts.ListenAddr,
		nonFinal:                opts.NonFinal,
		allowDuplicateExternals: opts.AllowDuplicateExternals,
		zeroState:               cloneZeroState(opts.ZeroState),
		version:                 DefaultVersion,
		capabilities:            DefaultCapabilities,
		now:                     time.Now,
		tvm:                     tvmInstance,
		requestLimits:           opts.RequestLimits,
		sendMessageCache:        admission.NewMessageCache(),
		externalMessageLimiter:  extmsg.NewDefaultAddressLimiter(),
		requestLimiter:          newRequestLimiter(opts.RequestLimits),
		connectionTracker:       newClientConnectionTracker(opts.RequestLimits),
		respCache:               newLiteResponseCache(),
		queryExecutor:           newQueryExecutor(opts.QueryConcurrency),
		waitLimiter:             newWaitLimiter(opts.RequestLimits.MaxWaitsPerIP),
		blockProofBases:         make(map[liteBlockKey]*blockProofBase),
	}
	if server.messageSender != nil {
		externalMessageSender, err := server.newExternalMessageSender()
		if err != nil {
			return nil, err
		}
		server.externalMessageSender = externalMessageSender
	}
	return server, nil
}

func validateServerOptions(opts Options) error {
	if len(opts.PrivateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("liteserver private key must be %d bytes", ed25519.PrivateKeySize)
	}
	if opts.ListenAddr == "" {
		return fmt.Errorf("liteserver listen address is empty")
	}
	if opts.Store == nil {
		return fmt.Errorf("liteserver store is required")
	}

	if err := validateRequestLimitOptions(opts.RequestLimits); err != nil {
		return err
	}
	return nil
}

func liteserverLogger(logger *zerolog.Logger) zerolog.Logger {
	if logger == nil {
		return zerolog.Nop()
	}
	return logger.With().Str("component", "liteserver").Logger()
}

func (s *Server) Start(ctx context.Context) error {
	if s.server != nil {
		return fmt.Errorf("liteserver already started")
	}

	configureLiteclientLogger(s.log)

	srv := liteclient.NewServer([]ed25519.PrivateKey{s.privateKey})
	if s.requestLimiter != nil {
		srv.SetQueryPrecheck(s.precheckQueryRequest)
	}
	srv.SetQueryHandler(s.dispatchQueryRequest)
	srv.SetConnectionHook(func(client *liteclient.ServerClient) error {
		event := s.log.Debug().Str("remote_ip", client.IP()).Uint16("remote_port", client.Port())
		if s.connectionTracker != nil {
			connections, err := s.connectionTracker.accept(client, s.now())
			if err != nil {
				event.Msg("rejected liteserver connection")
				return err
			}
			event = event.Int("connections", connections)
		}
		event.Msg("accepted liteserver connection")
		return nil
	})
	srv.SetDisconnectHook(func(client *liteclient.ServerClient) {
		event := s.log.Debug().Str("remote_ip", client.IP()).Uint16("remote_port", client.Port())
		if s.connectionTracker != nil {
			connections := s.connectionTracker.disconnect(client)
			event = event.Int("connections", connections)
		}
		event.Msg("closed liteserver connection")
	})
	s.server = srv
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	if s.connectionTracker != nil && s.connectionTracker.keepAliveEnabled() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ticker := time.NewTicker(liteserverConnectionCleanupInterval)
			defer ticker.Stop()

			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
					closed := s.connectionTracker.closeIdle(s.now())
					if closed > 0 {
						s.log.Debug().Int("closed", closed).Msg("closed idle liteserver connections")
					}
				}
			}
		}()
	}
	if s.requestLimiter != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ticker := time.NewTicker(requestLimitPruneInterval)
			defer ticker.Stop()

			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
					deleted := s.requestLimiter.prune(s.now())
					if deleted > 0 {
						s.log.Debug().Int("deleted", deleted).Msg("pruned idle liteserver request limit buckets")
					}
				}
			}
		}()
	}

	errCh := make(chan error, 1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := srv.Listen(s.listenAddr); err != nil {
			select {
			case errCh <- err:
			default:
			}
			if runCtx.Err() == nil {
				s.log.Error().Err(err).Str("listen_addr", s.listenAddr).Msg("liteserver listener stopped")
			}
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-runCtx.Done()
		if err := srv.Close(); err != nil {
			s.log.Warn().Err(err).Msg("failed to close liteserver listener")
		}
	}()

	select {
	case err := <-errCh:
		cancel()
		_ = srv.Close()
		s.server = nil
		s.cancel = nil
		return err
	case <-runCtx.Done():
		cancel()
		_ = srv.Close()
		s.server = nil
		s.cancel = nil
		return runCtx.Err()
	case <-time.After(100 * time.Millisecond):
	}

	s.log.Info().
		Str("listen_addr", s.listenAddr).
		Str("public_key", base64.StdEncoding.EncodeToString(s.privateKey.Public().(ed25519.PublicKey))).
		Msg("started liteserver")
	return nil
}

func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	var err error
	if s.server != nil {
		err = s.server.Close()
	}
	if s.queryExecutor != nil {
		s.queryExecutor.Close()
	}
	if s.server == nil {
		return nil
	}
	return err
}

func (s *Server) Wait() {
	s.wg.Wait()
	if s.queryExecutor != nil {
		s.queryExecutor.Wait()
	}
}

func configureLiteclientLogger(logger zerolog.Logger) {
	liteclient.Logger = func(args ...any) {
		msg := strings.TrimSpace(fmt.Sprintln(args...))
		if msg == "" {
			return
		}

		remoteAddr, msg := splitLiteclientLogRemote(args, msg)
		event := logger.Debug()
		if strings.Contains(msg, "failed to accept connection") {
			event = logger.Warn()
		}
		if !event.Enabled() {
			return
		}

		if remoteAddr != "" {
			event = event.Str("remote_addr", remoteAddr)
			if host, port, err := net.SplitHostPort(remoteAddr); err == nil {
				event = event.Str("remote_ip", host).Str("remote_port", port)
			}
		}
		event.Str("source", "tonutils-go/liteclient").Msg(msg)
	}
}

func splitLiteclientLogRemote(args []any, fallback string) (string, string) {
	if len(args) == 0 {
		return "", fallback
	}

	first, ok := args[0].(string)
	if !ok || len(first) < 3 || first[0] != '[' || first[len(first)-1] != ']' {
		return "", fallback
	}

	remoteAddr := first[1 : len(first)-1]
	msg := strings.TrimSpace(fmt.Sprintln(args[1:]...))
	if msg == "" {
		msg = fallback
	}
	return remoteAddr, msg
}

func (s *Server) precheckQueryRequest(ctx context.Context, client *liteclient.ServerClient) (tl.Serializable, bool) {
	now := s.now()
	if s.requestLimiter != nil && !s.requestLimiter.allow(client.IP(), now) {
		resp := ton.LSError{Code: errCodeTooManyRequests, Text: "too many requests"}
		timing := queryLogTiming{
			errorReason: queryErrorReasonRateLimited,
		}
		if s.queryObserver != nil {
			s.queryObserver.ObserveLiteserverQuery(queryObservationFromResponse(resp, timing))
		}
		if event := s.log.Debug(); event.Enabled() {
			event.
				Str("query", timing.queryName()).
				Str("response", liteserverTypeName(resp)).
				Str("remote_ip", client.IP()).
				Uint16("remote_port", client.Port()).
				Int32("error_code", resp.Code).
				Str("error_reason", timing.errorReason).
				Msg("rate limited liteserver query")
		}
		return resp, true
	}
	if s.connectionTracker != nil && s.connectionTracker.keepAliveEnabled() {
		s.connectionTracker.beginRequest(client, now)
	}

	return nil, false
}

func queryObservationFromResponse(resp tl.Serializable, timing queryLogTiming) QueryObservation {
	observation := QueryObservation{
		Method:       timing.queryName(),
		Response:     liteserverTypeName(resp),
		ErrorReason:  timing.errorReason,
		Duration:     timing.duration,
		WaitDuration: timing.waitDuration,
	}
	if lsErr, ok := resp.(ton.LSError); ok {
		observation.Error = true
		observation.ErrorCode = lsErr.Code
	}
	return observation
}

func liteserverQueryLogName(data any) string {
	switch q := data.(type) {
	case liteclient.LiteServerQuery:
		return liteserverQueryLogName(q.Data)
	case []tl.Serializable:
		return liteserverQuerySequenceLogName(q)
	default:
		return liteserverTypeName(q)
	}
}

func liteserverQuerySequenceLogName(items []tl.Serializable) string {
	if len(items) == 0 {
		return "empty"
	}

	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, liteserverQueryLogName(item))
	}
	return strings.Join(names, "+")
}

func liteserverTypeName(v any) string {
	if v == nil {
		return "nil"
	}

	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if name := t.Name(); name != "" {
		return name
	}
	return t.String()
}
