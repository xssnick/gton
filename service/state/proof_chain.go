package state

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"time"

	"flexserver/service/archive"
	"flexserver/service/blockproof"
	"flexserver/service/p2p"
	"flexserver/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const masterchainShard = int64(-1 << 63)

const (
	keyBlockLookupLimit        = 8
	keyBlockProgressInfoEvery  = 64
	keyBlockLookupRetryDelay   = time.Second
	keyBlockLookupRetryLogEach = 5
	initialStateMinAge         = time.Hour
	initialStateDownloadWindow = 8 * time.Hour
)

type trustedKeyBlock struct {
	block  ton.BlockIDExt
	config *tlb.BlockchainConfig
	utime  uint32
}

type keyBlockCandidate struct {
	block ton.BlockIDExt
	utime uint32
}

func (s *Syncer) persistentMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	initBlock, err := s.source.TrustedInitBlock(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	blocks, err := s.keyBlockIDs(ctx, initBlock)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if len(blocks) == 0 {
		return ton.BlockIDExt{}, fmt.Errorf("key block walk returned no blocks")
	}

	latestKey := blocks[len(blocks)-1]
	if len(blocks) == 1 {
		s.log.Debug().
			Str("block", storage.FormatBlockRef(initBlock)).
			Msg("using trusted init masterchain block for state snapshot")
		return initBlock, nil
	}

	s.log.Info().
		Str("from", storage.FormatBlockRef(initBlock)).
		Str("latest_key", storage.FormatBlockRef(latestKey)).
		Msg("verifying key block chain for persistent state selection")

	candidates, trusted, err := s.verifiedKeyBlockCandidates(ctx, initBlock, blocks)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if err = s.storage.SaveSeenMasterchainBlock(ctx, trusted.block); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("save latest verified key block %s: %w", storage.FormatBlockRef(trusted.block), err)
	}

	block, ok := choosePersistentKeyBlock(candidates, time.Now())
	if ok {
		return block, nil
	}

	s.log.Info().
		Str("block", storage.FormatBlockRef(trusted.block)).
		Int("key_blocks", len(candidates)).
		Msg("using latest verified key block for state snapshot")
	return trusted.block, nil
}

func (s *Syncer) verifiedKeyBlockCandidates(ctx context.Context, initBlock ton.BlockIDExt, blocks []ton.BlockIDExt) ([]keyBlockCandidate, trustedKeyBlock, error) {
	trusted, err := s.trustedInitKeyBlock(ctx, initBlock)
	if err != nil {
		return nil, trustedKeyBlock{}, err
	}

	candidates := []keyBlockCandidate{{
		block: trusted.block,
		utime: trusted.utime,
	}}

	latest := blocks[len(blocks)-1]
	s.log.Info().
		Str("from", storage.FormatBlockRef(initBlock)).
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

		if shouldLogKeyBlockProgress(verified, len(blocks)-1) {
			s.log.Info().
				Str("block", storage.FormatBlockRef(trusted.block)).
				Str("latest", storage.FormatBlockRef(latest)).
				Int("verified", verified).
				Int("key_blocks", len(blocks)-1).
				Msg("key block proof verification progress")
		}
	}

	s.log.Info().
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Str("latest_key", storage.FormatBlockRef(latest)).
		Int("candidates", len(candidates)).
		Msg("key block proof chain verified")
	return candidates, trusted, nil
}

func (s *Syncer) trustedInitKeyBlock(ctx context.Context, initBlock ton.BlockIDExt) (trustedKeyBlock, error) {
	s.log.Info().
		Str("block", storage.FormatBlockRef(initBlock)).
		Msg("downloading trusted init block proof link")

	proof, err := s.source.TrustedInitProof(ctx, initBlock)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("download trusted init block proof link %s: %w", storage.FormatBlockRef(initBlock), err)
	}

	parsed, err := blockproof.ParseBOC(initBlock, proof)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("parse trusted init block proof link %s: %w", storage.FormatBlockRef(initBlock), err)
	}
	if !parsed.Block.BlockInfo.KeyBlock {
		return trustedKeyBlock{}, fmt.Errorf("trusted init block %s is not a key block", storage.FormatBlockRef(initBlock))
	}

	cfg, err := blockproof.ConfigFromKeyBlock(parsed.Block)
	if err != nil {
		return trustedKeyBlock{}, fmt.Errorf("load trusted init key block config %s: %w", storage.FormatBlockRef(initBlock), err)
	}

	s.log.Info().
		Str("block", storage.FormatBlockRef(initBlock)).
		Uint32("utime", parsed.Block.BlockInfo.GenUtime).
		Msg("trusted init block proof link verified by global config hash")
	return trustedKeyBlock{
		block:  initBlock,
		config: cfg,
		utime:  parsed.Block.BlockInfo.GenUtime,
	}, nil
}

func (s *P2PSource) TrustedInitProof(ctx context.Context, initBlock ton.BlockIDExt) ([]byte, error) {
	downloaded, err := s.node.DownloadBlockProof(ctx, initBlock, true)
	if err == nil {
		return downloaded.Data, nil
	}
	if errors.Is(err, context.Canceled) {
		return nil, err
	}

	s.log.Info().
		Err(err).
		Str("block", storage.FormatBlockRef(initBlock)).
		Msg("trusted init proof unavailable from proof endpoint, trying archive package")

	return s.trustedInitProofFromArchive(ctx, initBlock)
}

func (s *P2PSource) trustedInitProofFromArchive(ctx context.Context, initBlock ton.BlockIDExt) ([]byte, error) {
	downloaded, err := s.node.DownloadArchive(ctx, initBlock.SeqNo, archive.ShardIDFromBlock(initBlock), "")
	if err != nil {
		return nil, err
	}
	defer cleanupDownloadedArchiveFile(s.log, downloaded)

	proof, err := trustedInitProofFromArchiveImport(initBlock, downloaded.Imported)
	if err != nil {
		return nil, fmt.Errorf("read trusted init proof from archive #%d: %w", downloaded.ArchiveID, err)
	}

	s.log.Info().
		Str("block", storage.FormatBlockRef(initBlock)).
		Int64("archive_id", downloaded.ArchiveID).
		Str("peer", downloaded.Peer).
		Msg("loaded trusted init proof from archive package")
	return proof, nil
}

func trustedInitProofFromArchiveImport(block ton.BlockIDExt, imported *archive.Imported) ([]byte, error) {
	if imported == nil {
		return nil, storage.ErrNotFound
	}

	for _, full := range imported.FullBlocks {
		if full.ID.Equals(&block) && len(full.Proof) > 0 {
			return append([]byte(nil), full.Proof...), nil
		}
	}

	for _, kind := range []storage.ServedProofKind{storage.ServedProofBlockLink, storage.ServedProofBlock, storage.ServedProofKeyBlockLink, storage.ServedProofKeyBlock} {
		for _, proof := range imported.Proofs {
			if proof.Kind == kind && proof.ID.Equals(&block) && len(proof.Data) > 0 {
				return append([]byte(nil), proof.Data...), nil
			}
		}
	}

	return nil, storage.ErrNotFound
}

func cleanupDownloadedArchiveFile(log zerolog.Logger, downloaded *archive.Downloaded) {
	if downloaded == nil || downloaded.Path == "" {
		return
	}
	if err := os.Remove(downloaded.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Debug().
			Err(err).
			Str("path", downloaded.Path).
			Msg("failed to remove temporary trusted init archive package")
	}
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
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("trusted_key", storage.FormatBlockRef(trusted.block)).
		Bool("key_block", parsed.Block.BlockInfo.KeyBlock).
		Msg("masterchain proof signatures verified")
	return next, nil
}

func (s *P2PSource) MasterchainProof(ctx context.Context, block ton.BlockIDExt, requireKey bool) ([]byte, error) {
	if requireKey {
		s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Msg("requesting key block proof")

		downloaded, err := s.node.DownloadKeyBlockProof(ctx, block, false)
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

func (s *Syncer) keyBlockIDs(ctx context.Context, initBlock ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	all := []ton.BlockIDExt{initBlock}
	from := initBlock
	retries := 0

	for {
		logEvent := s.log.Debug()
		if len(all) == 1 || (len(all)-1)%keyBlockProgressInfoEvery == 0 {
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
		if accepted > 0 && (len(all)-1)%keyBlockProgressInfoEvery == 0 {
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

func choosePersistentKeyBlock(candidates []keyBlockCandidate, now time.Time) (ton.BlockIDExt, bool) {
	nowUnix := uint64(now.Unix())
	minAge := uint64(initialStateMinAge / time.Second)
	minTTL := uint64(initialStateDownloadWindow / time.Second)

	for i := len(candidates) - 1; i >= 0; i-- {
		candidate := candidates[i]
		if uint64(candidate.utime)+minAge > nowUnix {
			continue
		}

		persistent := i == 0 || isPersistentState(candidate.utime, candidates[i-1].utime)
		if !persistent {
			continue
		}
		if persistentStateTTL(candidate.utime) <= nowUnix+minTTL {
			continue
		}

		return candidate.block, true
	}

	return ton.BlockIDExt{}, false
}

func isPersistentState(ts, prevTS uint32) bool {
	return ts/(1<<17) != prevTS/(1<<17)
}

func persistentStateTTL(ts uint32) uint64 {
	x := uint64(ts) / (1 << 17)
	if x == 0 {
		return uint64(ts)
	}

	return uint64(ts) + ((uint64(1) << 18) << bits.TrailingZeros64(x))
}

func shouldLogKeyBlockProgress(done, total int) bool {
	return done == 1 || done == total || done%keyBlockProgressInfoEvery == 0
}

func isExpectedProofUnavailable(err error) bool {
	return errors.Is(err, p2p.ErrBlockNotAvailable)
}
