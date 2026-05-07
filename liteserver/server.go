package liteserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"flexserver/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultVersion      int32 = 0x101
	DefaultCapabilities int64 = 7
)

type Store interface {
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, uint64, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
}

type MessageSender interface {
	SendExternalMessage(ctx context.Context, body []byte) error
}

type Options struct {
	Logger        *zerolog.Logger
	Store         Store
	MessageSender MessageSender
	PrivateKey    ed25519.PrivateKey
	ListenAddr    string
	ZeroState     ton.ZeroStateIDExt
	Version       int32
	Capabilities  int64
	Now           func() time.Time
}

type Server struct {
	log           zerolog.Logger
	store         Store
	messageSender MessageSender
	privateKey    ed25519.PrivateKey
	listenAddr    string
	zeroState     ton.ZeroStateIDExt
	version       int32
	capabilities  int64
	now           func() time.Time

	server *liteclient.Server
	wg     sync.WaitGroup
}

func New(opts Options) (*Server, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("liteserver storage is nil")
	}
	if len(opts.PrivateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("liteserver private key must be %d bytes", ed25519.PrivateKeySize)
	}
	if opts.ListenAddr == "" {
		return nil, fmt.Errorf("liteserver listen address is empty")
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

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	return &Server{
		log:           log,
		store:         opts.Store,
		messageSender: opts.MessageSender,
		privateKey:    opts.PrivateKey,
		listenAddr:    opts.ListenAddr,
		zeroState:     cloneZeroState(opts.ZeroState),
		version:       version,
		capabilities:  capabilities,
		now:           now,
	}, nil
}

func (s *Server) Start(ctx context.Context) error {
	if s.server != nil {
		return fmt.Errorf("liteserver already started")
	}

	configureLiteclientLogger(s.log)

	srv := liteclient.NewServer([]ed25519.PrivateKey{s.privateKey})
	srv.SetMessageHandler(s.handleMessage)
	srv.SetConnectionHook(func(client *liteclient.ServerClient) error {
		s.log.Debug().Str("remote_ip", client.IP()).Uint16("remote_port", client.Port()).Msg("accepted liteserver connection")
		return nil
	})
	srv.SetDisconnectHook(func(client *liteclient.ServerClient) {
		s.log.Debug().Str("remote_ip", client.IP()).Uint16("remote_port", client.Port()).Msg("closed liteserver connection")
	})
	s.server = srv

	errCh := make(chan error, 1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := srv.Listen(s.listenAddr); err != nil {
			select {
			case errCh <- err:
			default:
			}
			if ctx.Err() == nil {
				s.log.Error().Err(err).Str("listen_addr", s.listenAddr).Msg("liteserver listener stopped")
			}
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		if err := srv.Close(); err != nil {
			s.log.Warn().Err(err).Msg("failed to close liteserver listener")
		}
	}()

	select {
	case err := <-errCh:
		_ = srv.Close()
		return err
	case <-ctx.Done():
		_ = srv.Close()
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
	}

	s.log.Info().
		Str("listen_addr", s.listenAddr).
		Str("public_key", base64.StdEncoding.EncodeToString(s.privateKey.Public().(ed25519.PublicKey))).
		Msg("started liteserver")
	return nil
}

func (s *Server) Close() error {
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

func (s *Server) handleMessage(ctx context.Context, client *liteclient.ServerClient, msg tl.Serializable) error {
	switch m := msg.(type) {
	case adnl.MessageQuery:
		return s.handleMessageQuery(ctx, client, m.ID, m.Data)
	case liteclient.TCPPing:
		return client.Send(liteclient.TCPPong{RandomID: m.RandomID})
	case liteclient.TCPAuthenticate:
		nonce, err := randomNonce()
		if err != nil {
			return fmt.Errorf("generate liteserver authentication nonce: %w", err)
		}
		return client.Send(liteclient.TCPAuthenticationNonce{Nonce: nonce})
	case liteclient.TCPAuthenticationComplete:
		return nil
	default:
		return fmt.Errorf("unknown liteserver TCP message %T", msg)
	}
}

func (s *Server) handleMessageQuery(ctx context.Context, client *liteclient.ServerClient, id []byte, data any) error {
	event := s.log.Debug()
	if !event.Enabled() {
		return client.Send(adnl.MessageAnswer{ID: id, Data: s.handleQueryData(ctx, data)})
	}

	query := liteserverQueryLogName(data)
	started := time.Now()
	resp := s.handleQueryData(ctx, data)
	err := client.Send(adnl.MessageAnswer{ID: id, Data: resp})

	event = event.
		Str("query", query).
		Str("response", liteserverTypeName(resp)).
		Dur("duration", time.Since(started)).
		Str("remote_ip", client.IP()).
		Uint16("remote_port", client.Port()).
		Err(err)
	if lsErr, ok := resp.(ton.LSError); ok {
		event = event.Int32("error_code", lsErr.Code)
	}
	event.Msg("handled liteserver query")

	return err
}

func randomNonce() ([]byte, error) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
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
