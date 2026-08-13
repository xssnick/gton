package state

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const masterchainShard = int64(-1 << 63)

const (
	// TON C++ full nodes clamp tonNode.getNextKeyBlockIds max_size to 8.
	keyBlockLookupLimit          = 8
	keyBlockProofDownloadWorkers = 4
	keyBlockProgressInfoEvery    = 64
	keyBlockLookupRetryDelay     = time.Second
	keyBlockLookupRetryLogEach   = 5
	DefaultSyncBefore            = 4 * time.Hour
	initialStateDownloadWindow   = 8 * time.Hour
)

type trustedKeyBlock struct {
	block  ton.BlockIDExt
	config *tlb.BlockchainConfig
	utime  uint32
}

type keyBlockCandidate struct {
	block                    ton.BlockIDExt
	utime                    uint32
	allowBoundaryWithoutPrev bool
}

type keyBlockProofDownload struct {
	data []byte
	err  error
}

var errNoPersistentKeyBlockCandidate = errors.New("no persistent key block state snapshot candidate")

func (s *Syncer) persistentMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	configured, err := s.configuredTrustedKeyBlockAnchor(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return s.persistentMasterchainBlockFromTrusted(ctx, configured)
}

func (s *Syncer) persistentMasterchainBlockFromTrusted(ctx context.Context, trusted trustedKeyBlock) (ton.BlockIDExt, error) {
	anchor := trusted
	s.log.Info().
		Str("from", storage.FormatBlockRef(trusted.block)).
		Msg("verifying key block chain for persistent state selection")

	candidates, trusted, err := s.verifiedKeyBlockCandidates(ctx, trusted)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	block, err := choosePersistentKeyBlock(candidates, time.Now(), s.syncBefore, s.syncUntil)
	if err == nil {
		return block, nil
	}
	if errors.Is(err, errNoPersistentKeyBlockCandidate) &&
		(s.syncUntil == 0 || anchor.utime <= s.syncUntil) {
		s.log.Info().
			Str("block", storage.FormatBlockRef(anchor.block)).
			Uint32("utime", anchor.utime).
			Msg("no eligible persistent state, using trusted init state")

		return anchor.block, nil
	}

	return ton.BlockIDExt{}, fmt.Errorf(
		"%w: latest_verified=%s key_blocks=%d sync_before=%s sync_until=%d download_window=%s",
		err,
		storage.FormatBlockRef(trusted.block),
		len(candidates),
		s.syncBefore,
		s.syncUntil,
		initialStateDownloadWindow,
	)
}

func (s *Syncer) configuredTrustedKeyBlockAnchor(ctx context.Context) (trustedKeyBlock, error) {
	initBlock, err := s.source.InitBlock(ctx)
	if err != nil {
		return trustedKeyBlock{}, err
	}

	var trusted trustedKeyBlock
	if initBlock.SeqNo == 0 {
		var zeroBlock ton.BlockIDExt
		zeroBlock, err = s.source.ZeroStateBlock(ctx)
		if err != nil {
			return trustedKeyBlock{}, err
		}
		trusted, err = s.trustedZeroState(ctx, zeroBlock)
		if err != nil {
			return trustedKeyBlock{}, err
		}
	} else {
		trusted, err = s.trustedInitBlock(ctx, initBlock)
		if err != nil {
			return trustedKeyBlock{}, err
		}
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

func (s *Syncer) verifiedKeyBlockCandidates(ctx context.Context, trusted trustedKeyBlock) ([]keyBlockCandidate, trustedKeyBlock, error) {
	candidates := make([]keyBlockCandidate, 0, keyBlockLookupLimit+1)
	candidates = append(candidates, keyBlockCandidate{
		block:                    trusted.block,
		utime:                    trusted.utime,
		allowBoundaryWithoutPrev: trusted.block.SeqNo > 0,
	})

	verified := 0
	retries := 0
	for {
		logEvent := s.log.Debug()
		if verified == 0 || verified%keyBlockProgressInfoEvery == 0 {
			logEvent = s.log.Info()
		}
		logEvent.
			Str("from", storage.FormatBlockRef(trusted.block)).
			Uint32("from_seqno", trusted.block.SeqNo).
			Int("limit", keyBlockLookupLimit).
			Msg("requesting next key block ids")

		batch, err := s.source.NextKeyBlocks(ctx, trusted.block, keyBlockLookupLimit)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, trustedKeyBlock{}, ctxErr
			}
			retries++
			s.logRetryNextKeyBlocks(err, trusted.block, retries)
			if err = waitNextKeyBlockRetry(ctx); err != nil {
				return nil, trustedKeyBlock{}, err
			}
			continue
		}

		if len(batch.Blocks) == 0 {
			s.log.Info().
				Str("from", storage.FormatBlockRef(trusted.block)).
				Int("verified", verified).
				Bool("incomplete", batch.Incomplete).
				Msg("next key block lookup reached tail")
			break
		}

		advanced, err := s.verifyKeyBlockBatch(ctx, &trusted, &candidates, batch.Blocks, &verified)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, trustedKeyBlock{}, ctxErr
			}
			retries++
			s.logRetryNextKeyBlocks(err, trusted.block, retries)
			if err = waitNextKeyBlockRetry(ctx); err != nil {
				return nil, trustedKeyBlock{}, err
			}
			continue
		}

		if !advanced {
			s.log.Info().
				Str("from", storage.FormatBlockRef(trusted.block)).
				Int("verified", verified).
				Msg("next key block lookup did not advance")
			break
		}
		retries = 0
	}

	s.log.Info().
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Int("candidates", len(candidates)).
		Msg("key block proof chain verified")
	return candidates, trusted, nil
}

func (s *Syncer) verifyKeyBlockBatch(ctx context.Context, trusted *trustedKeyBlock, candidates *[]keyBlockCandidate, blocks []ton.BlockIDExt, verified *int) (bool, error) {
	downloads := s.downloadKeyBlockProofBatch(ctx, blocks)
	if err := ctx.Err(); err != nil {
		return false, err
	}

	advanced := false
	firstAccepted := ton.BlockIDExt{}
	lastAccepted := ton.BlockIDExt{}
	accepted := 0

	for idx, block := range blocks {
		if err := ctx.Err(); err != nil {
			return advanced, err
		}

		if block.Equals(&trusted.block) || block.SeqNo <= trusted.block.SeqNo {
			continue
		}

		s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Str("trusted_key", storage.FormatBlockRef(trusted.block)).
			Int("index", idx+1).
			Int("batch", len(blocks)).
			Msg("verifying key block proof in chain")

		download := downloads[idx]
		if download.err != nil {
			return advanced, fmt.Errorf("download key block proof %s: %w", storage.FormatBlockRef(block), download.err)
		}

		next, err := s.verifyKeyBlockProofFromTrusted(ctx, *trusted, block, download.data)
		if err != nil {
			return advanced, fmt.Errorf("verify key block %s after %s: %w", storage.FormatBlockRef(block), storage.FormatBlockRef(trusted.block), err)
		}

		*trusted = next
		*candidates = append(*candidates, keyBlockCandidate{
			block: trusted.block,
			utime: trusted.utime,
		})
		(*verified)++
		if accepted == 0 {
			firstAccepted = trusted.block
		}
		lastAccepted = trusted.block
		accepted++
		advanced = true

		if shouldLogKeyBlockProgress(*verified, 0) {
			s.log.Info().
				Str("block", storage.FormatBlockRef(trusted.block)).
				Int("verified", *verified).
				Msg("key block proof download and verification progress")
		}
	}

	evt := s.log.Debug().
		Str("current", storage.FormatBlockRef(trusted.block)).
		Int("batch", len(blocks)).
		Int("accepted", accepted).
		Int("verified", *verified)
	if accepted > 0 {
		evt = evt.
			Str("first", storage.FormatBlockRef(firstAccepted)).
			Str("last", storage.FormatBlockRef(lastAccepted)).
			Uint32("first_seqno", firstAccepted.SeqNo).
			Uint32("last_seqno", lastAccepted.SeqNo)
	}
	evt.Msg("verified next key block id batch")

	return advanced, nil
}

func (s *Syncer) downloadKeyBlockProofBatch(ctx context.Context, blocks []ton.BlockIDExt) []keyBlockProofDownload {
	downloads := make([]keyBlockProofDownload, len(blocks))
	jobs := make(chan int, len(blocks))
	for idx := range blocks {
		jobs <- idx
	}
	close(jobs)

	var workers sync.WaitGroup
	for range min(keyBlockProofDownloadWorkers, len(blocks)) {
		workers.Go(func() {
			for idx := range jobs {
				if err := ctx.Err(); err != nil {
					downloads[idx].err = err
					continue
				}

				downloads[idx].data, downloads[idx].err = s.downloadKeyBlockProof(ctx, blocks[idx])
			}
		})
	}
	workers.Wait()

	return downloads
}

func (s *Syncer) downloadKeyBlockProof(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	if block.Workchain != -1 || block.Shard != masterchainShard {
		return nil, fmt.Errorf("expected masterchain block, got %s", storage.FormatBlockRef(block))
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Msg("downloading key block proof for trusted chain verification")

	return s.source.MasterchainProof(ctx, block, true)
}

func (s *Syncer) logRetryNextKeyBlocks(err error, from ton.BlockIDExt, retries int) {
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
		Msg("next key block lookup failed, retrying same verified key block")
}

func waitNextKeyBlockRetry(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(keyBlockLookupRetryDelay):
		return nil
	}
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

func (s *Syncer) verifyKeyBlockProofFromTrusted(ctx context.Context, trusted trustedKeyBlock, block ton.BlockIDExt, proof []byte) (trustedKeyBlock, error) {
	if block.Workchain != -1 || block.Shard != masterchainShard {
		return trustedKeyBlock{}, fmt.Errorf("expected masterchain block, got %s", storage.FormatBlockRef(block))
	}
	if block.Equals(&trusted.block) {
		return trusted, nil
	}
	if block.SeqNo <= trusted.block.SeqNo {
		return trustedKeyBlock{}, fmt.Errorf("block does not advance trusted key block")
	}

	parsed, err := blockproof.ParseBOC(block, proof)
	if err != nil {
		return trustedKeyBlock{}, err
	}
	if !parsed.Block.BlockInfo.KeyBlock {
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

	signaturesChecked := true
	if err = blockproof.CheckMasterchainSignatures(block, parsed.Block, parsed.Proof.Signatures, trusted.config); err != nil {
		if !s.source.IsHardfork(ctx, block) {
			return trustedKeyBlock{}, err
		}
		if hardforkErr := blockproof.ValidateHardforkBlock(block, parsed.Block); hardforkErr != nil {
			return trustedKeyBlock{}, hardforkErr
		}
		s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Str("trusted_key", storage.FormatBlockRef(trusted.block)).
			Msg("accepted configured hardfork key block proof without validator signatures")
		signaturesChecked = false
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
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Bool("key_block", parsed.Block.BlockInfo.KeyBlock).
		Bool("signatures_checked", signaturesChecked).
		Msg("masterchain proof accepted")
	return next, nil
}

func choosePersistentKeyBlock(candidates []keyBlockCandidate, now time.Time, syncBefore time.Duration, syncUntil uint32) (ton.BlockIDExt, error) {
	if syncBefore <= 0 {
		syncBefore = DefaultSyncBefore
	}
	nowUnix := uint64(now.Unix())
	minAge := uint64(syncBefore / time.Second)
	minTTL := uint64(initialStateDownloadWindow / time.Second)

	for i := len(candidates) - 1; i >= 0; i-- {
		candidate := candidates[i]
		if syncUntil != 0 && candidate.utime > syncUntil {
			continue
		}
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

		return candidate.block, nil
	}

	return ton.BlockIDExt{}, errNoPersistentKeyBlockCandidate
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
