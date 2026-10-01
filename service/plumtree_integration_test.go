package service

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	livePlumtreeCandidateKind = "tonNode.newBlockCandidateBroadcastCompressed"
	livePlumtreeFinalityKind  = "tonNode.blockFinalityBroadcast"
)

type livePlumtreeObserver struct {
	observations chan p2p.BroadcastPipelineStageObservation
}

func (o livePlumtreeObserver) ObserveBroadcastPipelineStage(
	observation p2p.BroadcastPipelineStageObservation,
) {
	if observation.Delivery != p2p.DeliveryPlumtree {
		return
	}

	select {
	case o.observations <- observation:
	default:
	}
}

type livePlumtreeAdmission struct{}

func (livePlumtreeAdmission) CanAcceptBroadcast(
	req p2p.BroadcastAdmissionRequest,
) bool {
	return req.Kind == livePlumtreeCandidateKind ||
		req.Kind == livePlumtreeFinalityKind
}

type livePlumtreeSignatureVerifier struct {
	config *tlb.BlockchainConfig
}

func (v livePlumtreeSignatureVerifier) CheckBlockBroadcastSignatures(
	context.Context,
	p2p.BlockBroadcastSignatureCheck,
) error {
	return errors.New("full block broadcasts are outside this live test")
}

func (v livePlumtreeSignatureVerifier) CheckBlockFinalitySignatures(
	_ context.Context,
	req p2p.BlockFinalitySignatureCheck,
) (*p2p.BlockFinalitySignatureCheckResult, error) {
	validators, err := broadcastValidatorSetFromConfig(
		v.config,
		req.Block,
		req.Signatures.CatchainSeqno(),
		req.Signatures.ValidatorSetHash(),
	)
	if err != nil {
		return nil, err
	}
	if err = blockproof.CheckPreparedSignatures(
		req.Block,
		req.Signatures,
		validators,
	); err != nil {
		return nil, err
	}

	var signaturesCell *cell.Cell
	if req.Block.Workchain == -1 {
		signaturesCell, err = req.Signatures.FinalitySignaturesCell(validators)
		if err != nil {
			return nil, err
		}
	}

	return &p2p.BlockFinalitySignatureCheckResult{
		SignaturesCell:        signaturesCell,
		SignaturesVerifiedKey: req.Signatures.ContentKey(req.Block),
	}, nil
}

func (livePlumtreeSignatureVerifier) ValidateShardDescriptionBroadcast(
	context.Context,
	p2p.ShardDescriptionSignatureCheck,
) (*p2p.ShardBlockDescription, error) {
	return nil, errors.New("shard descriptions are outside this live test")
}

type livePlumtreePeerStore struct{}

func (livePlumtreePeerStore) BlockMeta(
	context.Context,
	ton.BlockIDExt,
) (*storage.BlockMeta, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) BlockFull(
	context.Context,
	ton.BlockIDExt,
) (*storage.ServedBlockFull, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) NextBlockFull(
	context.Context,
	ton.BlockIDExt,
) (*storage.ServedBlockFull, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) BlockData(
	context.Context,
	ton.BlockIDExt,
) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) BlockProof(
	context.Context,
	storage.ServedProofKind,
	ton.BlockIDExt,
) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) NextKeyBlocks(context.Context, uint32, int) ([]ton.BlockIDExt, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) ZeroStateSize(
	context.Context,
	ton.BlockIDExt,
) (int64, error) {
	return 0, storage.ErrNotFound
}

func (livePlumtreePeerStore) ZeroState(
	context.Context,
	ton.BlockIDExt,
) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) PersistentStateSize(
	context.Context,
	ton.BlockIDExt,
	ton.BlockIDExt,
	int64,
) (int64, error) {
	return 0, storage.ErrNotFound
}

func (livePlumtreePeerStore) PersistentStateSlice(
	context.Context,
	ton.BlockIDExt,
	ton.BlockIDExt,
	int64,
	int64,
	int64,
) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (livePlumtreePeerStore) ArchiveInfo(
	context.Context,
	int32,
	int32,
	int64,
) (int64, error) {
	return 0, storage.ErrNotFound
}

func (livePlumtreePeerStore) ArchiveSlice(
	context.Context,
	int64,
	int64,
	int32,
) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func TestIntegrationReceivesPublicPlumtreeBroadcasts(t *testing.T) {
	if os.Getenv("RUN_TON_INTEGRATION") == "" {
		t.Skip("set RUN_TON_INTEGRATION=1 to run live TON network test")
	}

	listenText := os.Getenv("TON_INTEGRATION_ADNL_LISTEN")
	externalIPText := os.Getenv("TON_INTEGRATION_EXTERNAL_IP")
	seedText := os.Getenv("TON_INTEGRATION_ADNL_SEED")
	if listenText == "" || externalIPText == "" || seedText == "" {
		t.Skip("set TON_INTEGRATION_ADNL_LISTEN, TON_INTEGRATION_EXTERNAL_IP, and TON_INTEGRATION_ADNL_SEED")
	}

	listen, err := netip.ParseAddrPort(listenText)
	if err != nil || listen.Port() == 0 {
		t.Fatalf("parse TON_INTEGRATION_ADNL_LISTEN: %v", err)
	}
	externalAddr, err := netip.ParseAddr(externalIPText)
	if err != nil || !externalAddr.Is4() || externalAddr.IsUnspecified() {
		t.Fatalf("invalid TON_INTEGRATION_EXTERNAL_IP %q", externalIPText)
	}
	externalIP := net.IP(externalAddr.AsSlice())

	seed, err := base64.StdEncoding.DecodeString(seedText)
	if err != nil {
		t.Fatalf("decode TON_INTEGRATION_ADNL_SEED: %v", err)
	}
	if len(seed) != ed25519.SeedSize {
		t.Fatalf(
			"TON_INTEGRATION_ADNL_SEED is %d bytes, want %d",
			len(seed),
			ed25519.SeedSize,
		)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	globalConfig, err := liteclient.GetConfigFromUrl(
		ctx,
		"https://tonutils.com/global.config.json",
	)
	if err != nil {
		t.Fatalf("load global config: %v", err)
	}

	pool := liteclient.NewConnectionPool()
	t.Cleanup(pool.Stop)
	if err = pool.AddConnectionsFromConfig(ctx, globalConfig); err != nil {
		t.Fatalf("connect to liteservers: %v", err)
	}

	api := ton.NewAPIClient(pool).WithRetry()
	master, err := api.GetMasterchainInfo(ctx)
	if err != nil {
		t.Fatalf("get masterchain info: %v", err)
	}
	blockchainConfig, err := api.WaitForBlock(master.SeqNo).
		GetBlockchainConfig(ctx, master)
	if err != nil {
		t.Fatalf("get blockchain config at masterchain %d: %v", master.SeqNo, err)
	}

	fastSync := fastSyncConfigFromConfig(blockchainConfig)
	if !fastSync.plumtreeEnabled(-1) || !fastSync.plumtreeEnabled(0) {
		t.Fatalf(
			"public Plumtree is disabled: masterchain=%d shard=%d",
			fastSync.masterchainPlumtreeVersion,
			fastSync.shardPlumtreeVersion,
		)
	}
	policy := fastSync.plumtreePolicy()

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.InfoLevel)
	node, err := p2p.New(p2p.Options{
		Logger:        &logger,
		GlobalConfig:  globalConfig,
		ListenAddr:    listen.String(),
		ExternalIP:    externalIP,
		ExternalPort:  listen.Port(),
		PrivateKey:    privateKey,
		StateFilesDir: t.TempDir(),
		PeerStorage:   livePlumtreePeerStore{},
	})
	if err != nil {
		t.Fatalf("create public p2p node: %v", err)
	}
	if err = node.BindRuntimeCallbacks(p2p.RuntimeCallbacks{
		SignatureVerifier:  livePlumtreeSignatureVerifier{config: blockchainConfig},
		BroadcastAdmission: livePlumtreeAdmission{},
	}); err != nil {
		t.Fatalf("bind public p2p runtime callbacks: %v", err)
	}
	node.SetPlumtreePolicy(policy)

	observer := livePlumtreeObserver{
		observations: make(chan p2p.BroadcastPipelineStageObservation, 256),
	}
	node.SetBroadcastPipelineObserver(observer)
	if err = node.Start(ctx); err != nil {
		t.Fatalf("start public p2p node: %v", err)
	}
	t.Logf("public test ADNL id: %s", node.LocalID())
	defer func() {
		cancel()
		node.Wait()
	}()

	var (
		candidateDecoded bool
		finalityVerified bool
		assembled        bool
		eventsReceived   uint64
	)
	for !candidateDecoded || !finalityVerified || !assembled {
		select {
		case <-ctx.Done():
			status := node.StatusSnapshot()
			t.Fatalf(
				"timed out waiting for public Plumtree broadcasts: candidate_decoded=%v finality_verified=%v assembled=%v quic_peers=%d quic_accepted=%d events=%d",
				candidateDecoded,
				finalityVerified,
				assembled,
				status.QUICPeers,
				status.QUICPeersAccepted,
				eventsReceived,
			)
		case observation := <-observer.observations:
			switch {
			case observation.Kind == livePlumtreeCandidateKind &&
				observation.Stage == "candidate_decode" &&
				observation.Result == "success":
				candidateDecoded = true
			case observation.Kind == livePlumtreeFinalityKind &&
				observation.Stage == "block_finality_signature_check" &&
				observation.Result == "success":
				finalityVerified = true
			}
		case event, ok := <-node.Events():
			if !ok {
				t.Fatal("public p2p node stopped before receiving Plumtree broadcasts")
			}
			eventsReceived++
			if event.Delivery == p2p.DeliveryPlumtree &&
				event.Kind == livePlumtreeFinalityKind &&
				event.Downloaded != nil {
				assembled = true
			}
		}
	}

	t.Logf(
		"received public QUIC/Plumtree candidate and finality at masterchain %d via ADNL %s, QUIC port %d",
		master.SeqNo,
		listen,
		listen.Port()+1000,
	)
}
