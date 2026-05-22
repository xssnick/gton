package state

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const masterchainShard = int64(-1 << 63)

const (
	keyBlockLookupLimit        = 8
	keyBlockProgressInfoEvery  = 64
	keyBlockLookupRetryDelay   = time.Second
	keyBlockLookupRetryLogEach = 5
	DefaultSyncBefore          = time.Hour
	initialStateDownloadWindow = 8 * time.Hour
)

type trustedKeyBlock struct {
	block   ton.BlockIDExt
	config  *tlb.BlockchainConfig
	utime   uint32
	resumed bool
}

type keyBlockCandidate struct {
	block                    ton.BlockIDExt
	utime                    uint32
	allowBoundaryWithoutPrev bool
}

var errNoPersistentKeyBlockCandidate = errors.New("no persistent key block state snapshot candidate")

func (s *Syncer) persistentMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	configured, err := s.configuredTrustedKeyBlockAnchor(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	trusted := configured
	resumed, err := s.resumeTrustedKeyBlockAnchor(ctx, configured)
	if err == nil {
		if err = validateTrustedKeyBlockAnchor(resumed.block); err != nil {
			return ton.BlockIDExt{}, err
		}
		trusted = resumed
	} else if !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}

	block, err := s.persistentMasterchainBlockFromTrusted(ctx, trusted)
	if err == nil {
		return block, nil
	}
	if !trusted.resumed || !errors.Is(err, errNoPersistentKeyBlockCandidate) {
		return ton.BlockIDExt{}, err
	}

	s.log.Info().
		Str("verified_key", storage.FormatBlockRef(trusted.block)).
		Str("configured_anchor", storage.FormatBlockRef(configured.block)).
		Msg("verified key block progress has no persistent state candidate, restarting selection from configured anchor")
	return s.persistentMasterchainBlockFromTrusted(ctx, configured)
}

func (s *Syncer) persistentMasterchainBlockFromTrusted(ctx context.Context, trusted trustedKeyBlock) (ton.BlockIDExt, error) {
	blocks, err := s.keyBlockIDs(ctx, trusted.block)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	latestKey := trusted.block
	if len(blocks) > 0 {
		latestKey = blocks[len(blocks)-1]
	}

	s.log.Info().
		Str("from", storage.FormatBlockRef(trusted.block)).
		Str("latest_key", storage.FormatBlockRef(latestKey)).
		Msg("verifying key block chain for persistent state selection")

	candidates, trusted, err := s.verifiedKeyBlockCandidates(ctx, trusted, blocks)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	block, ok := choosePersistentKeyBlock(candidates, time.Now(), s.syncBefore)
	if ok {
		return block, nil
	}

	return ton.BlockIDExt{}, fmt.Errorf(
		"%w: latest_verified=%s key_blocks=%d sync_before=%s download_window=%s",
		errNoPersistentKeyBlockCandidate,
		storage.FormatBlockRef(trusted.block),
		len(candidates),
		s.syncBefore,
		initialStateDownloadWindow,
	)
}

func (s *Syncer) configuredTrustedKeyBlockAnchor(ctx context.Context) (trustedKeyBlock, error) {
	var trusted trustedKeyBlock
	var err error
	if s.fromZero {
		zeroBlock, err := s.source.ZeroStateBlock(ctx)
		if err != nil {
			return trustedKeyBlock{}, err
		}
		trusted, err = s.trustedZeroState(ctx, zeroBlock)
	} else {
		initBlock, err := s.source.InitBlock(ctx)
		if err != nil {
			return trustedKeyBlock{}, err
		}
		if initBlock.SeqNo == 0 {
			zeroBlock, err := s.source.ZeroStateBlock(ctx)
			if err != nil {
				return trustedKeyBlock{}, err
			}
			trusted, err = s.trustedZeroState(ctx, zeroBlock)
		} else {
			trusted, err = s.trustedInitBlock(ctx, initBlock)
		}
	}
	if err != nil {
		return trustedKeyBlock{}, err
	}

	if err = validateTrustedKeyBlockAnchor(trusted.block); err != nil {
		return trustedKeyBlock{}, err
	}
	s.log.Info().
		Str("block", storage.FormatBlockRef(trusted.block)).
		Uint32("utime", trusted.utime).
		Msg("using trusted key block anchor")
	return trusted, nil
}

func validateTrustedKeyBlockAnchor(block ton.BlockIDExt) error {
	if block.Workchain != -1 || block.Shard != masterchainShard {
		return fmt.Errorf("trusted key block anchor is not masterchain: %s", storage.FormatBlockRef(block))
	}
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return fmt.Errorf("trusted key block anchor has invalid hashes: %s root_hash_len=%d file_hash_len=%d", storage.FormatBlockRef(block), len(block.RootHash), len(block.FileHash))
	}
	return nil
}

func (s *Syncer) resumeTrustedKeyBlockAnchor(ctx context.Context, trusted trustedKeyBlock) (trustedKeyBlock, error) {
	progress, err := s.storage.VerifiedKeyBlockProgress(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return trustedKeyBlock{}, storage.ErrNotFound
		}
		return trustedKeyBlock{}, fmt.Errorf("load verified key block progress: %w", err)
	}
	if progress.Workchain != -1 || progress.Shard != masterchainShard || progress.SeqNo <= trusted.block.SeqNo {
		return trustedKeyBlock{}, storage.ErrNotFound
	}

	resumed, err := s.trustedKeyBlockFromStoredProof(ctx, progress)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			s.log.Debug().
				Str("block", storage.FormatBlockRef(progress)).
				Msg("verified key block progress has no stored full proof, starting from configured anchor")
			return trustedKeyBlock{}, storage.ErrNotFound
		}
		return trustedKeyBlock{}, err
	}

	s.log.Info().
		Str("configured_anchor", storage.FormatBlockRef(trusted.block)).
		Str("verified_key", storage.FormatBlockRef(resumed.block)).
		Uint32("utime", resumed.utime).
		Msg("resuming key block proof verification from stored progress")
	resumed.resumed = true
	return resumed, nil
}

func (s *Syncer) trustedKeyBlockFromStoredProof(ctx context.Context, block ton.BlockIDExt) (trustedKeyBlock, error) {
	proofs, ok := s.storage.(storage.PeerServingStorage)
	if !ok {
		return trustedKeyBlock{}, storage.ErrNotFound
	}

	proof, err := proofs.BlockProof(ctx, storage.ServedProofKeyBlock, block)
	if err != nil {
		return trustedKeyBlock{}, err
	}

	parsed, err := blockproof.ParseBOC(block, proof)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("parse stored verified key block proof %s: %w", storage.FormatBlockRef(block), err)
	}
	if !parsed.Block.BlockInfo.KeyBlock {
		return trustedKeyBlock{}, fmt.Errorf("stored verified key block proof %s is not a key block", storage.FormatBlockRef(block))
	}

	cfg, err := blockproof.ConfigFromKeyBlock(parsed.Block)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("load stored verified key block config %s: %w", storage.FormatBlockRef(block), err)
	}
	return trustedKeyBlock{
		block:  block,
		config: cfg,
		utime:  parsed.Block.BlockInfo.GenUtime,
	}, nil
}

func (s *Syncer) verifiedKeyBlockCandidates(ctx context.Context, trusted trustedKeyBlock, blocks []ton.BlockIDExt) ([]keyBlockCandidate, trustedKeyBlock, error) {
	candidates := make([]keyBlockCandidate, 0, len(blocks)+1)
	candidates = append(candidates, keyBlockCandidate{
		block:                    trusted.block,
		utime:                    trusted.utime,
		allowBoundaryWithoutPrev: trusted.block.SeqNo > 0 && !trusted.resumed,
	})

	latest := trusted.block
	if len(blocks) > 0 {
		latest = blocks[len(blocks)-1]
	}
	s.log.Info().
		Str("from", storage.FormatBlockRef(trusted.block)).
		Str("latest_key", storage.FormatBlockRef(latest)).
		Int("key_blocks", len(blocks)).
		Msg("downloaded key block ids for persistent state selection")

	verified := 0
	for idx, block := range blocks {
		if block.Equals(&trusted.block) || block.SeqNo <= trusted.block.SeqNo {
			continue
		}
		if block.SeqNo > latest.SeqNo {
			break
		}

		s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Str("trusted_key", storage.FormatBlockRef(trusted.block)).
			Str("latest_key", storage.FormatBlockRef(latest)).
			Int("index", idx+1).
			Int("key_blocks", len(blocks)).
			Msg("verifying key block proof in chain")

		next, err := s.verifyMasterchainBlockFromTrustedKey(ctx, trusted, block, true)
		if err != nil {
			return nil, trustedKeyBlock{}, fmt.Errorf("verify key block %s after %s: %w", storage.FormatBlockRef(block), storage.FormatBlockRef(trusted.block), err)
		}
		trusted = next
		candidates = append(candidates, keyBlockCandidate{
			block: trusted.block,
			utime: trusted.utime,
		})
		verified++

		if shouldSaveKeyBlockProgress(verified, len(blocks)) {
			if err = s.storage.SaveVerifiedKeyBlockProgress(ctx, trusted.block); err != nil {
				return nil, trustedKeyBlock{}, fmt.Errorf("save verified key block progress %s: %w", storage.FormatBlockRef(trusted.block), err)
			}
		}
		if shouldLogKeyBlockProgress(verified, len(blocks)) {
			s.log.Info().
				Str("block", storage.FormatBlockRef(trusted.block)).
				Str("latest", storage.FormatBlockRef(latest)).
				Int("verified", verified).
				Int("key_blocks", len(blocks)).
				Msg("key block proof download and verification progress")
		}
	}

	s.log.Info().
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Str("latest_key", storage.FormatBlockRef(latest)).
		Int("candidates", len(candidates)).
		Msg("key block proof chain verified")
	return candidates, trusted, nil
}

func (s *Syncer) trustedZeroState(ctx context.Context, zeroBlock ton.BlockIDExt) (trustedKeyBlock, error) {
	if err := validateTrustedKeyBlockAnchor(zeroBlock); err != nil {
		return trustedKeyBlock{}, err
	}
	if zeroBlock.SeqNo != 0 {
		return trustedKeyBlock{}, fmt.Errorf("trusted zero state anchor is not zerostate: %s", storage.FormatBlockRef(zeroBlock))
	}

	s.log.Info().
		Str("block", storage.FormatBlockRef(zeroBlock)).
		Msg("loading trusted zero state")

	downloaded, err := s.source.ZeroState(ctx, zeroBlock)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("download zero state %s: %w", storage.FormatBlockRef(zeroBlock), err)
	}
	defer func() {
		if cleanupErr := downloaded.Cleanup(); cleanupErr != nil {
			s.log.Warn().
				Err(cleanupErr).
				Str("block", storage.FormatBlockRef(zeroBlock)).
				Msg("failed to cleanup zero state")
		}
	}()

	downloadedBlock := downloaded.Block()
	if !downloadedBlock.Equals(&zeroBlock) {
		return trustedKeyBlock{}, fmt.Errorf("zero state artifact is for %s, expected %s", storage.FormatBlockRef(downloadedBlock), storage.FormatBlockRef(zeroBlock))
	}

	state, err := downloaded.Decode(ctx)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("decode zero state %s: %w", storage.FormatBlockRef(zeroBlock), err)
	}
	cfg, err := blockproof.ConfigFromMasterchainState(state)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("load zero state config %s: %w", storage.FormatBlockRef(zeroBlock), err)
	}

	utime := uint32(0)
	if state.Parsed != nil {
		utime = state.Parsed.GenUTime
	}
	s.log.Info().
		Str("block", storage.FormatBlockRef(zeroBlock)).
		Uint32("utime", utime).
		Msg("trusted zero state config loaded")
	return trustedKeyBlock{
		block:  zeroBlock,
		config: cfg,
		utime:  utime,
	}, nil
}

func (s *Syncer) trustedInitBlock(ctx context.Context, block ton.BlockIDExt) (trustedKeyBlock, error) {
	if block.Workchain != -1 || block.Shard != masterchainShard {
		return trustedKeyBlock{}, fmt.Errorf("expected masterchain init block, got %s", storage.FormatBlockRef(block))
	}

	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Msg("loading trusted init block proof link")

	proof, err := s.source.InitBlockProof(ctx, block)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("download trusted init block proof %s: %w", storage.FormatBlockRef(block), err)
	}

	parsed, err := blockproof.ParseBOC(block, proof.Data)
	if err != nil {
		return trustedKeyBlock{}, err
	}
	if !parsed.Block.BlockInfo.KeyBlock {
		return trustedKeyBlock{}, fmt.Errorf("trusted init block %s is not a key block", storage.FormatBlockRef(block))
	}

	cfg, err := blockproof.ConfigFromKeyBlock(parsed.Block)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("load trusted init block config %s: %w", storage.FormatBlockRef(block), err)
	}
	if err = s.saveKeyBlockProofDownload(block, proof); err != nil {
		return trustedKeyBlock{}, err
	}

	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Uint32("utime", parsed.Block.BlockInfo.GenUtime).
		Bool("proof_link", proof.Link).
		Msg("trusted init block config loaded")
	return trustedKeyBlock{
		block:  block,
		config: cfg,
		utime:  parsed.Block.BlockInfo.GenUtime,
	}, nil
}

func (s *Syncer) verifyMasterchainBlockFromTrustedKey(ctx context.Context, trusted trustedKeyBlock, block ton.BlockIDExt, requireKey bool) (trustedKeyBlock, error) {
	if block.Workchain != -1 || block.Shard != masterchainShard {
		return trustedKeyBlock{}, fmt.Errorf("expected masterchain block, got %s", storage.FormatBlockRef(block))
	}
	if block.Equals(&trusted.block) {
		return trusted, nil
	}
	if block.SeqNo <= trusted.block.SeqNo {
		return trustedKeyBlock{}, fmt.Errorf("block does not advance trusted key block")
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Bool("require_key", requireKey).
		Msg("downloading masterchain proof for trusted chain verification")

	proof, err := s.source.MasterchainProof(ctx, block, requireKey)
	if err != nil {
		return trustedKeyBlock{}, err
	}

	parsed, err := blockproof.ParseBOC(block, proof)
	if err != nil {
		return trustedKeyBlock{}, err
	}
	if requireKey && !parsed.Block.BlockInfo.KeyBlock {
		return trustedKeyBlock{}, fmt.Errorf("block %s is not a key block", storage.FormatBlockRef(block))
	}
	if parsed.Block.BlockInfo.PrevKeyBlockSeqno != trusted.block.SeqNo {
		return trustedKeyBlock{}, fmt.Errorf("block %s references prev key block seqno %d, trusted key is %d", storage.FormatBlockRef(block), parsed.Block.BlockInfo.PrevKeyBlockSeqno, trusted.block.SeqNo)
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Bool("key_block", parsed.Block.BlockInfo.KeyBlock).
		Msg("checking masterchain proof signatures")

	if err = blockproof.CheckMasterchainSignatures(block, parsed.Block, parsed.Proof.Signatures, trusted.config); err != nil {
		return trustedKeyBlock{}, err
	}

	next := trustedKeyBlock{
		block:  block,
		config: trusted.config,
		utime:  parsed.Block.BlockInfo.GenUtime,
	}
	if parsed.Block.BlockInfo.KeyBlock {
		cfg, err := blockproof.ConfigFromKeyBlock(parsed.Block)
		if err != nil {
			return trustedKeyBlock{}, fmt.Errorf("load key block config %s: %w", storage.FormatBlockRef(block), err)
		}
		next.config = cfg
		if err = s.saveVerifiedKeyBlockProof(block, proof); err != nil {
			return trustedKeyBlock{}, err
		}
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Bool("key_block", parsed.Block.BlockInfo.KeyBlock).
		Msg("masterchain proof signatures verified")
	return next, nil
}

func (s *Syncer) saveVerifiedKeyBlockProof(block ton.BlockIDExt, proof []byte) error {
	return s.saveKeyBlockProofDownload(block, p2p.ProofDownload{Data: proof})
}

func (s *Syncer) saveKeyBlockProofDownload(block ton.BlockIDExt, proof p2p.ProofDownload) error {
	writer, ok := s.storage.(storage.PeerServingStorageWriter)
	if !ok {
		return nil
	}

	if proof.Link {
		if err := writer.SaveBlockProof(storage.ServedProofKeyBlockLink, block, proof.Data, nil); err != nil {
			return fmt.Errorf("store key block proof link %s: %w", storage.FormatBlockRef(block), err)
		}
		return nil
	}

	if err := writer.SaveBlockProof(storage.ServedProofKeyBlock, block, proof.Data, nil); err != nil {
		return fmt.Errorf("store key block proof %s: %w", storage.FormatBlockRef(block), err)
	}
	link, err := blockproof.LinkBOC(block, proof.Data)
	if err != nil {
		return fmt.Errorf("derive key block proof link %s: %w", storage.FormatBlockRef(block), err)
	}
	if err = writer.SaveBlockProof(storage.ServedProofKeyBlockLink, block, link, nil); err != nil {
		return fmt.Errorf("store key block proof link %s: %w", storage.FormatBlockRef(block), err)
	}
	return nil
}

func (s *P2PSource) MasterchainProof(ctx context.Context, block ton.BlockIDExt, requireKey bool) ([]byte, error) {
	if requireKey {
		s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Msg("requesting key block proof")

		downloaded, err := s.keyBlockProof(ctx, block, false)
		if err == nil && !downloaded.Link {
			return downloaded.Data, nil
		}
		if err != nil && !isExpectedProofUnavailable(err) {
			return nil, fmt.Errorf("download key block proof %s: %w", storage.FormatBlockRef(block), err)
		}

		evt := s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Bool("proof_link", downloaded.Link)
		if err != nil {
			evt = evt.Err(err)
		}
		evt.Msg("key block proof unavailable as full proof, downloading block full")
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Bool("require_key", requireKey).
		Msg("requesting masterchain block full for proof")

	downloaded, err := s.node.DownloadBlockFull(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("download block full %s: %w", storage.FormatBlockRef(block), err)
	}
	if downloaded == nil {
		return nil, fmt.Errorf("download block full %s: empty response", storage.FormatBlockRef(block))
	}
	return downloaded.ProofBOC, nil
}

func (s *Syncer) keyBlockIDs(ctx context.Context, fromBlock ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	if fromBlock.Workchain != -1 || fromBlock.Shard != masterchainShard {
		return nil, fmt.Errorf("next key block lookup requires masterchain anchor, got %s", storage.FormatBlockRef(fromBlock))
	}

	var all []ton.BlockIDExt
	from := fromBlock
	retries := 0

	for {
		logEvent := s.log.Debug()
		if len(all) == 0 || len(all)%keyBlockProgressInfoEvery == 0 {
			logEvent = s.log.Info()
		}
		logEvent.
			Str("from", storage.FormatBlockRef(from)).
			Uint32("from_seqno", from.SeqNo).
			Int("limit", keyBlockLookupLimit).
			Msg("requesting next key block ids")

		batch, err := s.source.NextKeyBlocks(ctx, from, keyBlockLookupLimit)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}

			retries++
			evt := s.log.Debug()
			if retries == 1 || retries%keyBlockLookupRetryLogEach == 0 {
				evt = s.log.Warn()
			}
			evt.
				Err(err).
				Str("from", storage.FormatBlockRef(from)).
				Uint32("from_seqno", from.SeqNo).
				Int("attempt", retries).
				Dur("retry_in", keyBlockLookupRetryDelay).
				Msg("next key block id lookup failed, retrying same key block")

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(keyBlockLookupRetryDelay):
			}
			continue
		}
		retries = 0

		if len(batch.Blocks) == 0 {
			s.log.Info().
				Str("from", storage.FormatBlockRef(from)).
				Int("key_blocks", len(all)).
				Bool("incomplete", batch.Incomplete).
				Msg("next key block lookup reached tail")
			break
		}

		advanced := false
		firstAccepted := ton.BlockIDExt{}
		lastAccepted := ton.BlockIDExt{}
		accepted := 0
		for _, block := range batch.Blocks {
			if block.SeqNo <= from.SeqNo {
				continue
			}

			if accepted == 0 {
				firstAccepted = block
			}
			lastAccepted = block
			accepted++
			all = append(all, block)
			from = block
			advanced = true
		}

		evt := s.log.Debug().
			Str("current", storage.FormatBlockRef(from)).
			Int("batch", len(batch.Blocks)).
			Int("accepted", accepted).
			Int("key_blocks", len(all)).
			Bool("incomplete", batch.Incomplete)
		if accepted > 0 && len(all)%keyBlockProgressInfoEvery == 0 {
			evt = s.log.Info().
				Str("current", storage.FormatBlockRef(from)).
				Int("batch", len(batch.Blocks)).
				Int("accepted", accepted).
				Int("key_blocks", len(all)).
				Bool("incomplete", batch.Incomplete)
		}
		if accepted > 0 {
			evt = evt.
				Str("first", storage.FormatBlockRef(firstAccepted)).
				Str("last", storage.FormatBlockRef(lastAccepted)).
				Uint32("first_seqno", firstAccepted.SeqNo).
				Uint32("last_seqno", lastAccepted.SeqNo)
		}
		evt.Msg("received next key block id batch")

		if !advanced {
			s.log.Info().
				Str("from", storage.FormatBlockRef(from)).
				Int("key_blocks", len(all)).
				Msg("next key block lookup did not advance")
			break
		}
	}

	return all, nil
}

func choosePersistentKeyBlock(candidates []keyBlockCandidate, now time.Time, syncBefore time.Duration) (ton.BlockIDExt, bool) {
	if syncBefore <= 0 {
		syncBefore = DefaultSyncBefore
	}
	nowUnix := uint64(now.Unix())
	minAge := uint64(syncBefore / time.Second)
	minTTL := uint64(initialStateDownloadWindow / time.Second)

	for i := len(candidates) - 1; i >= 0; i-- {
		candidate := candidates[i]
		if uint64(candidate.utime)+minAge > nowUnix {
			continue
		}

		persistent := candidate.allowBoundaryWithoutPrev
		if i > 0 {
			persistent = IsPersistentState(candidate.utime, candidates[i-1].utime)
		}
		if !persistent {
			continue
		}
		if PersistentStateTTL(candidate.utime) <= nowUnix+minTTL {
			continue
		}

		return candidate.block, true
	}

	return ton.BlockIDExt{}, false
}

func IsPersistentState(ts, prevTS uint32) bool {
	return ts/(1<<17) != prevTS/(1<<17)
}

func PersistentStateTTL(ts uint32) uint64 {
	x := uint64(ts) / (1 << 17)
	if x == 0 {
		return uint64(ts)
	}

	return uint64(ts) + ((uint64(1) << 18) << bits.TrailingZeros64(x))
}

func shouldLogKeyBlockProgress(done, total int) bool {
	return done == 1 || done == total || done%keyBlockProgressInfoEvery == 0
}

func shouldSaveKeyBlockProgress(done, total int) bool {
	return shouldLogKeyBlockProgress(done, total)
}

func isExpectedProofUnavailable(err error) bool {
	return errors.Is(err, p2p.ErrBlockNotAvailable)
}
