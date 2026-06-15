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
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultVersion      int32 = 0x101
	DefaultCapabilities int64 = 15
)

type Store interface {
	LiveStoreBacking
	CurrentAccountBlocks(ctx context.Context, workchain int32, account []byte) (CurrentAccountBlockIDs, error)
	BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
	BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveBlockFragments, error)
	CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error)
	WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error
	NonfinalPendingShardBlocks(filter *storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt)
}

type LiveStoreBacking interface {
	// BlockData returns bytes that remain valid for the duration of the call
	// chain. The liteserver treats the returned slice as read-only.
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
	LazyCellLoader() cell.LazyCellLoader
}

type MessageSender interface {
	SendExternalMessage(ctx context.Context, body []byte, address extmsg.AddressKey) error
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
	Logger              *zerolog.Logger
	Store               Store
	MessageSender       MessageSender
	QueryObserver       QueryObserver
	PrivateKey          ed25519.PrivateKey
	ListenAddr          string
	NonFinal            bool
	SendMessageTVMTrace bool
	ZeroState           ton.ZeroStateIDExt
	Version             int32
	Capabilities        int64
}

type Server struct {
	log                 zerolog.Logger
	store               Store
	messageSender       MessageSender
	queryObserver       QueryObserver
	privateKey          ed25519.PrivateKey
	listenAddr          string
	nonFinal            bool
	sendMessageTVMTrace bool
	zeroState           ton.ZeroStateIDExt
	version             int32
	capabilities        int64
	now                 func() time.Time
	tvm                 *tvm.TVM

	sendMessageCache       *sendMessageCache
	externalMessageLimiter *extmsg.AddressLimiter

	server *liteclient.Server
	cancel context.CancelFunc
	wg     sync.WaitGroup

	blockProofBasesMu   sync.Mutex
	blockProofBases     map[storage.BlockRootHash]*blockProofBase
	blockProofBaseOrder []storage.BlockRootHash
	blockProofBaseLoad  liveLoadGroup[storage.BlockRootHash]
}

func New(opts Options) (*Server, error) {
	if len(opts.PrivateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("liteserver private key must be %d bytes", ed25519.PrivateKeySize)
	}
	if opts.ListenAddr == "" {
		return nil, fmt.Errorf("liteserver listen address is empty")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("liteserver store is required")
	}

	log := zerolog.Nop()
	if opts.Logger != nil {
		log = opts.Logger.With().Str("component", "liteserver").Logger()
	}

	version := opts.Version
	if version == 0 {
		version = DefaultVersion
	}

	capabilities := opts.Capabilities
	if capabilities == 0 {
		capabilities = DefaultCapabilities
	}

	return &Server{
		log:                    log,
		store:                  opts.Store,
		messageSender:          opts.MessageSender,
		queryObserver:          opts.QueryObserver,
		privateKey:             opts.PrivateKey,
		listenAddr:             opts.ListenAddr,
		nonFinal:               opts.NonFinal,
		sendMessageTVMTrace:    opts.SendMessageTVMTrace,
		zeroState:              cloneZeroState(opts.ZeroState),
		version:                version,
		capabilities:           capabilities,
		now:                    time.Now,
		tvm:                    tvm.NewTVM(),
		sendMessageCache:       newSendMessageCache(),
		externalMessageLimiter: extmsg.NewDefaultAddressLimiter(),
		blockProofBases:        make(map[storage.BlockRootHash]*blockProofBase),
	}, nil
}

func (s *Server) Start(ctx context.Context) error {
	if s.server != nil {
		return fmt.Errorf("liteserver already started")
	}

	configureLiteclientLogger(s.log)

	srv := liteclient.NewServer([]ed25519.PrivateKey{s.privateKey})
	srv.SetQueryHandler(s.handleQueryRequest)
	srv.SetConnectionHook(func(client *liteclient.ServerClient) error {
		s.log.Debug().Str("remote_ip", client.IP()).Uint16("remote_port", client.Port()).Msg("accepted liteserver connection")
		return nil
	})
	srv.SetDisconnectHook(func(client *liteclient.ServerClient) {
		s.log.Debug().Str("remote_ip", client.IP()).Uint16("remote_port", client.Port()).Msg("closed liteserver connection")
	})
	s.server = srv
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

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
	if s.server == nil {
		return nil
	}
	return s.server.Close()
}

func (s *Server) Wait() {
	s.wg.Wait()
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

func (s *Server) handleQueryRequest(ctx context.Context, client *liteclient.ServerClient, data tl.Serializable) (tl.Serializable, error) {
	event := s.log.Debug()
	if !event.Enabled() && s.queryObserver == nil {
		return s.handleQueryData(ctx, data), nil
	}

	if s.queryObserver != nil {
		s.queryObserver.AddLiteserverInflight(1)
		defer s.queryObserver.AddLiteserverInflight(-1)
	}

	resp, timing := s.handleQueryDataWithTiming(ctx, data)
	if s.queryObserver != nil {
		s.queryObserver.ObserveLiteserverQuery(queryObservationFromResponse(resp, timing))
	}
	if !event.Enabled() {
		return resp, nil
	}

	event = event.
		Str("query", timing.queryName()).
		Str("response", liteserverTypeName(resp)).
		Dur("duration", timing.duration).
		Str("remote_ip", client.IP()).
		Uint16("remote_port", client.Port())
	if timing.sequence != "" && timing.sequence != timing.query {
		event = event.Str("sequence", timing.sequence)
	}
	if timing.waitDuration > 0 {
		event = event.Dur("wait_duration", timing.waitDuration)
	}
	if lsErr, ok := resp.(ton.LSError); ok {
		event = event.Int32("error_code", lsErr.Code)
	}
	if timing.errorReason != "" {
		event = event.Str("error_reason", timing.errorReason)
	}
	event.Msg("handled liteserver query")

	return resp, nil
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
	case tl.Raw:
		items, err := parseQuerySequence(q)
		if err != nil {
			return "raw"
		}
		return liteserverQuerySequenceLogName(items)
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
