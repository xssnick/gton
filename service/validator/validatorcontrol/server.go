package validatorcontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/blockstats"
	"github.com/xssnick/gton/service/validator/keyring"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	PermissionDefault uint32 = 1 << iota
	PermissionModify
	PermissionUnsafe
)

const (
	controlErrorCode    int32 = 602
	controlNotReadyCode int32 = 651
)

type Keys interface {
	Generate(ctx context.Context) ([32]byte, error)
	AddPermanent(ctx context.Context, keyID [32]byte, electionDate, expireAt uint32) error
	AddTemp(ctx context.Context, permanentKeyID, keyID [32]byte, expireAt uint32) error
	AddADNL(ctx context.Context, permanentKeyID, adnlID [32]byte, expireAt uint32) error
	PublicKeyFor(keyID [32]byte) (ed25519.PublicKey, error)
	Sign(keyID [32]byte, payload []byte) ([]byte, error)
	Entries() []keyring.KeyInfo
}

type StateReader interface {
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
}

// BlockStatsReader provides the process-lifetime terminal block counters.
type BlockStatsReader interface {
	BlockStats() blockstats.Snapshot
}

type TrustedClient struct {
	ID          [32]byte
	Permissions uint32
}

type Options struct {
	ListenAddress  string
	ServerKey      ed25519.PrivateKey
	TrustedClients []TrustedClient
	Keys           Keys
	LocalADNLID    [32]byte
	State          StateReader
	BlockStats     BlockStatsReader
	Logger         zerolog.Logger
}

type Server struct {
	listenAddress string
	transport     *liteclient.Server
	permissions   map[[32]byte]uint32
	keys          Keys
	localADNLID   [32]byte
	state         StateReader
	blockStats    BlockStatsReader
	logger        zerolog.Logger

	lifecycleMu sync.Mutex
	mu          sync.Mutex
	running     bool
	address     string
	listener    net.Listener
	serveDone   chan struct{}
	connections map[*liteclient.ServerClient]struct{}
	clients     sync.WaitGroup
	startedAt   time.Time
}

func New(options Options) (*Server, error) {
	if options.ListenAddress == "" {
		return nil, errors.New("validator control: listen address is required")
	}
	if len(options.ServerKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf(
			"validator control: server key must be %d bytes, got %d",
			ed25519.PrivateKeySize,
			len(options.ServerKey),
		)
	}
	if len(options.TrustedClients) == 0 {
		return nil, errors.New("validator control: at least one trusted client is required")
	}
	if options.Keys == nil {
		return nil, errors.New("validator control: keys are required")
	}
	if options.State == nil {
		return nil, errors.New("validator control: state reader is required")
	}
	if options.BlockStats == nil {
		return nil, errors.New("validator control: block stats reader is required")
	}
	if options.LocalADNLID == ([32]byte{}) {
		return nil, errors.New("validator control: local ADNL ID is required")
	}

	permissions := make(map[[32]byte]uint32, len(options.TrustedClients))
	trustedClientIDs := make([][32]byte, 0, len(options.TrustedClients))
	for i, client := range options.TrustedClients {
		if client.ID == ([32]byte{}) {
			return nil, fmt.Errorf("validator control: trusted client %d ID is required", i)
		}
		if _, duplicate := permissions[client.ID]; duplicate {
			return nil, fmt.Errorf("validator control: duplicate trusted client %x", client.ID)
		}
		if client.Permissions == 0 {
			return nil, fmt.Errorf("validator control: trusted client %d permissions are required", i)
		}
		if client.Permissions == 0 {
			return nil, fmt.Errorf("validator control: trusted client %d permissions are required", i)
		}

		permissions[client.ID] = client.Permissions
		trustedClientIDs = append(trustedClientIDs, client.ID)
	}

	transport := liteclient.NewServerWithTrustedClients(
		[]ed25519.PrivateKey{options.ServerKey},
		trustedClientIDs,
	)
	server := &Server{
		listenAddress: options.ListenAddress,
		transport:     transport,
		permissions:   permissions,
		keys:          options.Keys,
		localADNLID:   options.LocalADNLID,
		state:         options.State,
		blockStats:    options.BlockStats,
		logger:        options.Logger,
		connections:   make(map[*liteclient.ServerClient]struct{}),
	}
	transport.SetConnectionHook(server.onConnect)
	transport.SetDisconnectHook(server.onDisconnect)
	transport.SetQueryHandler(server.handleQuery)

	return server, nil
}

func (s *Server) Start() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	listener, err := net.Listen("tcp", s.listenAddress)
	if err != nil {
		return fmt.Errorf("validator control: listen on %s: %w", s.listenAddress, err)
	}

	ready := make(chan struct{})
	readyListener := &startListener{
		Listener: listener,
		ready:    ready,
	}
	done := make(chan struct{})
	s.serveDone = done
	s.running = true
	s.address = listener.Addr().String()
	s.listener = readyListener
	s.startedAt = time.Now()

	go func() {
		err := s.transport.Serve(readyListener)
		if err != nil {
			s.logger.Error().Err(err).Msg("validator control server stopped")
		}
		close(done)
	}()
	<-ready

	s.logger.Info().Str("address", listener.Addr().String()).Msg("validator control server started")

	return nil
}

func (s *Server) Close() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()

		return nil
	}

	s.running = false
	done := s.serveDone
	clients := make([]*liteclient.ServerClient, 0, len(s.connections))
	for client := range s.connections {
		clients = append(clients, client)
	}
	s.mu.Unlock()

	err := s.transport.Close()
	for _, client := range clients {
		client.Close()
	}
	<-done
	s.clients.Wait()

	s.mu.Lock()
	s.serveDone = nil
	s.address = ""
	s.listener = nil
	s.mu.Unlock()

	s.logger.Info().Msg("validator control server stopped")

	if err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("validator control: close listener: %w", err)
	}

	return nil
}

func (s *Server) onConnect(client *liteclient.ServerClient) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return errors.New("validator control server is stopped")
	}

	s.connections[client] = struct{}{}
	s.clients.Add(1)

	return nil
}

func (s *Server) onDisconnect(client *liteclient.ServerClient) {
	s.mu.Lock()
	if _, exists := s.connections[client]; exists {
		delete(s.connections, client)
		s.clients.Done()
	}
	s.mu.Unlock()
}

func (s *Server) handleQuery(
	ctx context.Context,
	client *liteclient.ServerClient,
	queryID []byte,
	query tl.Serializable,
) {
	answer := func(response tl.Serializable) {
		if !client.Answer(queryID, response) {
			s.logger.Debug().Hex("query_id", queryID).Msg("validator control response dropped")
		}
	}

	clientID, authenticated := client.ClientID()
	if !authenticated {
		answer(controlError("not authorized"))

		return
	}

	permissions, trusted := s.permissions[clientID]
	if !trusted {
		answer(controlError("not authorized"))

		return
	}

	envelope, ok := query.(ControlQuery)
	if !ok {
		answer(controlError("expected engine.validator.controlQuery"))

		return
	}

	inner, err := parseControlQuery(envelope.Data)
	if err != nil {
		answer(controlError(err.Error()))

		return
	}

	answer(s.execute(ctx, clientID, permissions, inner))
}

func (s *Server) execute(
	ctx context.Context,
	clientID [32]byte,
	permissions uint32,
	query tl.Serializable,
) tl.Serializable {
	s.logger.Debug().Hex("client_id", clientID[:]).Str("query", fmt.Sprintf("%T", query)).Msg("validator control query")

	switch request := query.(type) {
	case GenerateKeyPair:
		if !hasPermission(permissions, PermissionDefault) {
			return controlError("not authorized")
		}

		keyID, err := s.keys.Generate(ctx)
		if err != nil {
			return controlError("failed to generate key pair: " + err.Error())
		}

		return KeyHash{KeyHash: keyID[:]}

	case ExportPublicKey:
		if !hasPermission(permissions, PermissionDefault) {
			return controlError("not authorized")
		}

		keyID := keyIDFromBytes(request.KeyHash)
		publicKey, err := s.keys.PublicKeyFor(keyID)
		if err != nil {
			return controlError("failed to get public key: " + err.Error())
		}

		return keys.PublicKeyED25519{Key: publicKey}

	case AddValidatorPermanentKey:
		if !hasPermission(permissions, PermissionModify) {
			return controlError("not authorized")
		}

		err := s.keys.AddPermanent(
			ctx,
			keyIDFromBytes(request.KeyHash),
			request.ElectionDate,
			request.TTL,
		)
		if err != nil {
			return controlError("failed to add validator permanent key: " + err.Error())
		}

		return Success{}

	case AddValidatorTempKey:
		if !hasPermission(permissions, PermissionModify) {
			return controlError("not authorized")
		}

		err := s.keys.AddTemp(
			ctx,
			keyIDFromBytes(request.PermanentKeyHash),
			keyIDFromBytes(request.KeyHash),
			request.TTL,
		)
		if err != nil {
			return controlError("failed to add validator temp key: " + err.Error())
		}

		return Success{}

	case AddValidatorADNLAddress:
		if !hasPermission(permissions, PermissionModify) {
			return controlError("not authorized")
		}

		adnlID := keyIDFromBytes(request.KeyHash)
		if adnlID != s.localADNLID {
			return controlError(fmt.Sprintf("failed to get public key: unknown local ADNL ID %x", adnlID))
		}

		err := s.keys.AddADNL(
			ctx,
			keyIDFromBytes(request.PermanentKeyHash),
			adnlID,
			request.TTL,
		)
		if err != nil {
			return controlError("failed to add validator ADNL address: " + err.Error())
		}

		return Success{}

	case Sign:
		if !hasPermission(permissions, PermissionUnsafe) {
			return controlError("not authorized")
		}

		keyID := keyIDFromBytes(request.KeyHash)
		s.logger.Warn().
			Hex("client_id", clientID[:]).
			Hex("key_id", keyID[:]).
			Int("data_size", len(request.Data)).
			Msg("validator control signing request")

		signature, err := s.keys.Sign(keyID, request.Data)
		if err != nil {
			return controlError("failed to sign: " + err.Error())
		}

		return Signature{Signature: signature}

	case GetConfig:
		if !hasPermission(permissions, PermissionDefault) {
			return controlError("not authorized")
		}

		config, err := s.configJSON()
		if err != nil {
			return controlError("failed to serialize config: " + err.Error())
		}

		return JSONConfig{Data: config}

	case GetStats:
		if !hasPermission(permissions, PermissionDefault) {
			return controlError("not authorized")
		}

		stats, err := s.stats(ctx)
		if err != nil {
			return ControlQueryError{
				Code:    controlNotReadyCode,
				Message: "failed to get stats: " + err.Error(),
			}
		}

		return stats

	default:
		return controlError(fmt.Sprintf("unsupported control query %T", query))
	}
}

func parseControlQuery(data []byte) (tl.Serializable, error) {
	var query any
	rest, err := tl.Parse(&query, data, true)
	if err != nil {
		return nil, fmt.Errorf("parse control query: %w", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("parse control query: %d trailing bytes", len(rest))
	}

	return query, nil
}

func keyIDFromBytes(value []byte) [32]byte {
	var id [32]byte
	copy(id[:], value)

	return id
}

func hasPermission(permissions, required uint32) bool {
	return permissions&required != 0
}

func controlError(message string) ControlQueryError {
	return ControlQueryError{
		Code:    controlErrorCode,
		Message: message,
	}
}

type startListener struct {
	net.Listener
	once  sync.Once
	ready chan struct{}
}

func (l *startListener) Accept() (net.Conn, error) {
	l.once.Do(func() {
		close(l.ready)
	})

	return l.Listener.Accept()
}

func compareKeyIDs(left, right [32]byte) int {
	return bytes.Compare(left[:], right[:])
}
