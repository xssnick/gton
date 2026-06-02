package service

import (
	"context"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

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

func (s *Service) runShardDescriptionProcessor(ctx context.Context) {
	if s.blockSync == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-s.blockSync.ShardDescriptions():
			if !ok {
				return
			}
			s.rememberShardDescriptionHint(ev)
		}
	}
}

func (s *Service) rememberShardDescriptionHint(ev p2p.BroadcastEvent) {
	if ev.ShardDescription == nil || ev.Block.Workchain == -1 || !ev.ShardDescription.Block.Equals(&ev.Block) {
		return
	}

	receivedAt := ev.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now()
	}

	hint := shardDescriptionHint{
		Description: cloneShardBlockDescription(ev.ShardDescription),
		Overlay:     ev.Overlay,
		Kind:        ev.Kind,
		ReceivedAt:  receivedAt,
	}

	key := storage.BlockKey(ev.Block)

	s.shardDescriptionMu.Lock()
	if s.shardDescriptionHints == nil {
		s.shardDescriptionHints = map[storage.BlockRootHash]shardDescriptionHint{}
	}
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
}

func (s *Service) rememberShardDescriptionProofs(hint shardDescriptionHint) {
	if s.node == nil {
		return
	}

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

func cloneShardBlockDescription(desc *p2p.ShardBlockDescription) p2p.ShardBlockDescription {
	if desc == nil {
		return p2p.ShardBlockDescription{}
	}

	cloned := p2p.ShardBlockDescription{
		Block:            desc.Block,
		CatchainSeqno:    desc.CatchainSeqno,
		ValidatorSetHash: desc.ValidatorSetHash,
		Data:             desc.Data,
		Chain:            make([]p2p.ShardDescriptionLink, 0, len(desc.Chain)),
	}
	for _, link := range desc.Chain {
		var masterchainRef *ton.BlockIDExt
		if link.MasterchainRef != nil {
			ref := *link.MasterchainRef
			masterchainRef = &ref
		}
		cloned.Chain = append(cloned.Chain, p2p.ShardDescriptionLink{
			Block:          link.Block,
			PrevRefs:       link.PrevRefs,
			MasterchainRef: masterchainRef,
			ProofRoot:      link.ProofRoot,
			ProofBOC:       link.ProofBOC,
		})
	}
	return cloned
}

func (s *Service) shardDescriptionHintSnapshot(now time.Time) []shardDescriptionHint {
	s.shardDescriptionMu.Lock()
	defer s.shardDescriptionMu.Unlock()

	s.pruneShardDescriptionHintsLocked(now)

	hints := make([]shardDescriptionHint, 0, len(s.shardDescriptionOrder))
	for _, key := range s.shardDescriptionOrder {
		hint, ok := s.shardDescriptionHints[key]
		if !ok {
			continue
		}
		hint.Description = cloneShardBlockDescription(&hint.Description)
		hints = append(hints, hint)
	}
	return hints
}

func (s *Service) dropShardDescriptionHint(block ton.BlockIDExt) {
	key := storage.BlockKey(block)

	s.shardDescriptionMu.Lock()
	delete(s.shardDescriptionHints, key)
	s.shardDescriptionMu.Unlock()
}

func (s *Service) pruneShardDescriptionHintsLocked(now time.Time) {
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

func (s *Service) signalShardDescriptionWake() {
	if s.shardDescriptionWake == nil {
		return
	}

	select {
	case s.shardDescriptionWake <- struct{}{}:
	default:
	}
}
