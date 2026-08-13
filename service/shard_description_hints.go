package service

import (
	"context"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	shardDescriptionHintTTL   = 3 * time.Minute
	shardDescriptionHintLimit = 4096
)

type shardDescriptionHint struct {
	Description p2p.ShardBlockDescription
	Overlay     string
	Kind        string
	ReceivedAt  time.Time
}

func (s *SyncCoordinator) runShardDescriptionProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-s.blockSync.ShardDescriptions():
			if !ok {
				return
			}
			if s.syncUntilFrozen() {
				continue
			}
			if !s.rememberShardDescriptionHint(ev) {
				continue
			}
			s.observeShardTopBlockDescription(ctx, ev)
		}
	}
}

func (s *SyncCoordinator) rememberShardDescriptionHint(ev p2p.BroadcastEvent) bool {
	if s.syncUntilFrozen() {
		return false
	}
	if ev.ShardDescription == nil || ev.ShardDescriptionRoot == nil || ev.Block.Workchain == -1 ||
		!ev.ShardDescription.Block.Equals(&ev.Block) {
		return false
	}

	receivedAt := ev.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now()
	}

	description, err := cloneShardBlockDescription(ev.ShardDescription)
	if err != nil {
		s.log.Warn().
			Err(err).
			Str("block", storage.FormatBlockRef(ev.Block)).
			Msg("failed to retain verified shard top block description")
		return false
	}
	hint := shardDescriptionHint{
		Description: description,
		Overlay:     ev.Overlay,
		Kind:        ev.Kind,
		ReceivedAt:  receivedAt,
	}

	key := storage.BlockKey(ev.Block)

	s.shardDescriptionMu.Lock()
	if _, ok := s.shardDescriptionHints[key]; !ok {
		s.shardDescriptionOrder = append(s.shardDescriptionOrder, key)
	}
	s.shardDescriptionHints[key] = hint
	s.pruneShardDescriptionHintsLocked(time.Now())
	s.shardDescriptionMu.Unlock()

	s.log.Debug().
		Str("block", storage.FormatBlockRef(ev.Block)).
		Str("overlay", ev.Overlay).
		Uint32("catchain_seqno", ev.ShardDescription.CatchainSeqno).
		Int("chain_links", len(ev.ShardDescription.Chain)).
		Msg("remembered shard block description broadcast")

	s.rememberShardDescriptionProofs(hint)
	s.signalShardDescriptionWake()
	s.wakeCurrentStateSync()
	return true
}

func (s *SyncCoordinator) observeShardTopBlockDescription(ctx context.Context, ev p2p.BroadcastEvent) {
	if s.shardTopObserver == nil {
		return
	}

	if err := s.shardTopObserver.ObserveShardTopBlockDescription(
		ctx,
		ev.ShardDescription,
		ev.ShardDescriptionRoot,
	); err != nil {
		s.log.Warn().
			Err(err).
			Str("block", storage.FormatBlockRef(ev.Block)).
			Msg("shard top block description observer failed")
	}
}

func (s *SyncCoordinator) rememberShardDescriptionProofs(hint shardDescriptionHint) {
	desc := hint.Description
	proofs := make([]p2p.ShardDescriptionProof, 0, len(desc.Chain))
	for _, link := range desc.Chain {
		proofs = append(proofs, p2p.ShardDescriptionProof{
			Block:    link.Block,
			Proof:    link.ProofRoot,
			ProofBOC: link.ProofBOC,
		})
	}
	s.node.RememberShardDescriptionProofs(proofs)
}

func cloneShardBlockDescription(desc *p2p.ShardBlockDescription) (p2p.ShardBlockDescription, error) {
	cloned := p2p.ShardBlockDescription{
		Block:            cloneServiceBlockID(desc.Block),
		CatchainSeqno:    desc.CatchainSeqno,
		ValidatorSetHash: desc.ValidatorSetHash,
		Chain:            make([]p2p.ShardDescriptionLink, 0, len(desc.Chain)),
	}
	for _, link := range desc.Chain {
		fees, err := cloneShardDescriptionCurrency(link.FeesCollected)
		if err != nil {
			return p2p.ShardBlockDescription{}, fmt.Errorf("clone collected fees: %w", err)
		}
		created, err := cloneShardDescriptionCurrency(link.FundsCreated)
		if err != nil {
			return p2p.ShardBlockDescription{}, fmt.Errorf("clone created funds: %w", err)
		}

		var masterchainRef *ton.BlockIDExt
		if link.MasterchainRef != nil {
			ref := cloneServiceBlockID(*link.MasterchainRef)
			masterchainRef = &ref
		}
		cloned.Chain = append(cloned.Chain, p2p.ShardDescriptionLink{
			Block:          cloneServiceBlockID(link.Block),
			PrevRefs:       cloneServiceBlockIDs(link.PrevRefs),
			MasterchainRef: masterchainRef,
			TopBlockProof:  link.TopBlockProof,
			ProofRoot:      link.ProofRoot,
			ProofBOC:       link.ProofBOC,
			GenUtime:       link.GenUtime,
			VertSeqno:      link.VertSeqno,
			StartLT:        link.StartLT,
			EndLT:          link.EndLT,
			MinRefMCSeqno:  link.MinRefMCSeqno,
			BeforeSplit:    link.BeforeSplit,
			AfterSplit:     link.AfterSplit,
			AfterMerge:     link.AfterMerge,
			WantSplit:      link.WantSplit,
			WantMerge:      link.WantMerge,
			CreatedBy:      link.CreatedBy,
			FeesCollected:  fees,
			FundsCreated:   created,
		})
	}
	return cloned, nil
}

func cloneShardDescriptionCurrency(value tlb.CurrencyCollection) (tlb.CurrencyCollection, error) {
	return value.Add(tlb.CurrencyCollection{})
}

func cloneServiceBlockIDs(blocks []ton.BlockIDExt) []ton.BlockIDExt {
	if len(blocks) == 0 {
		return nil
	}

	cloned := make([]ton.BlockIDExt, len(blocks))
	for i := range blocks {
		cloned[i] = cloneServiceBlockID(blocks[i])
	}
	return cloned
}

func (s *SyncCoordinator) shardDescriptionHintSnapshot(now time.Time) []shardDescriptionHint {
	s.shardDescriptionMu.Lock()
	defer s.shardDescriptionMu.Unlock()

	s.pruneShardDescriptionHintsLocked(now)

	// Hints are cloned privately at remember time and never mutated afterwards,
	// so the snapshot shares them instead of deep-cloning every Chain again.
	hints := make([]shardDescriptionHint, 0, len(s.shardDescriptionOrder))
	for _, key := range s.shardDescriptionOrder {
		hint, ok := s.shardDescriptionHints[key]
		if !ok {
			continue
		}
		hints = append(hints, hint)
	}
	return hints
}

func (s *SyncCoordinator) dropShardDescriptionHint(block ton.BlockIDExt) {
	key := storage.BlockKey(block)

	s.shardDescriptionMu.Lock()
	delete(s.shardDescriptionHints, key)
	s.shardDescriptionMu.Unlock()
}

func (s *SyncCoordinator) pruneShardDescriptionHintsLocked(now time.Time) {
	if len(s.shardDescriptionHints) == 0 {
		s.shardDescriptionOrder = nil
		return
	}

	cutoff := now.Add(-shardDescriptionHintTTL)
	write := 0
	for _, key := range s.shardDescriptionOrder {
		hint, ok := s.shardDescriptionHints[key]
		if !ok {
			continue
		}
		if hint.ReceivedAt.Before(cutoff) {
			delete(s.shardDescriptionHints, key)
			continue
		}
		s.shardDescriptionOrder[write] = key
		write++
	}
	s.shardDescriptionOrder = s.shardDescriptionOrder[:write]

	if overflow := len(s.shardDescriptionOrder) - shardDescriptionHintLimit; overflow > 0 {
		for _, key := range s.shardDescriptionOrder[:overflow] {
			delete(s.shardDescriptionHints, key)
		}
		copy(s.shardDescriptionOrder, s.shardDescriptionOrder[overflow:])
		for i := len(s.shardDescriptionOrder) - overflow; i < len(s.shardDescriptionOrder); i++ {
			s.shardDescriptionOrder[i] = storage.BlockRootHash{}
		}
		s.shardDescriptionOrder = s.shardDescriptionOrder[:len(s.shardDescriptionOrder)-overflow]
	}
}

func (s *SyncCoordinator) signalShardDescriptionWake() {
	select {
	case s.shardDescriptionWake <- struct{}{}:
	default:
	}
}
