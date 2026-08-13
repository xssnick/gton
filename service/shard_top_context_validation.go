package service

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const maxShardTopDescriptionChain = 8

// validateShardTopDescriptionContext is the mandatory state-aware half of
// TopBlockDescr authentication. It uses the same immutable masterchain view as
// validator-set selection, so a rotation cannot mix topology and signatures.
func validateShardTopDescriptionContext(
	ctx context.Context,
	view *shardTopValidationView,
	description *p2p.ShardBlockDescription,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if description == nil || description.Block.Workchain == -1 ||
		len(description.Chain) == 0 || len(description.Chain) > maxShardTopDescriptionChain {
		return shardTopDescriptionInvalid(description, "invalid target or proof-chain length")
	}

	head := &description.Chain[0]
	if !sameShardTopBlock(head.Block, description.Block) {
		return shardTopDescriptionInvalid(description, "proof-chain head differs from the target")
	}
	for index := range description.Chain {
		if description.Chain[index].VertSeqno != head.VertSeqno {
			return shardTopDescriptionInvalid(description, "proof-chain vertical seqno is inconsistent")
		}
	}
	vertSeqno := view.validatorContext.VerticalSeqno()
	if head.VertSeqno < vertSeqno {
		return shardTopDescriptionInvalid(description, "vertical seqno is older than the current masterchain view")
	}
	if head.VertSeqno > vertSeqno {
		return shardTopDescriptionNotReady(description, "vertical seqno is newer than the current masterchain view", nil)
	}

	tooNew, err := validateShardTopMasterchainReferences(view, description)
	if err != nil {
		return err
	}
	anchors, err := view.validatorContext.ShardTopAnchors(description.Block)
	if err != nil {
		if errors.Is(err, blockproof.ErrShardTopValidatorContextNotReady) {
			return shardTopDescriptionNotReady(description, "shard registry cannot resolve the target", err)
		}
		return fmt.Errorf("resolve shard registry anchors for %s: %w", shardTopDescriptionBlock(description), err)
	}
	if err = validateShardTopRegistryAnchors(description, anchors, tooNew); err != nil {
		return err
	}

	return nil
}

func validateShardTopMasterchainReferences(
	view *shardTopValidationView,
	description *p2p.ShardBlockDescription,
) (bool, error) {
	masterchain := view.masterchain.Block
	tooNew := false
	nextSeqno := uint32(math.MaxUint32)
	for index := range description.Chain {
		ref := description.Chain[index].MasterchainRef
		if ref == nil || ref.Workchain != -1 || ref.Shard != shard.Root {
			return false, shardTopDescriptionInvalid(description, "proof chain has an invalid masterchain reference")
		}
		if ref.SeqNo > nextSeqno {
			return false, shardTopDescriptionInvalid(description, "masterchain references are not monotonic")
		}
		nextSeqno = ref.SeqNo
		if ref.SeqNo > masterchain.SeqNo {
			tooNew = true
			continue
		}
		if ref.SeqNo == masterchain.SeqNo {
			if !ref.Equals(&masterchain) {
				return false, shardTopDescriptionInvalid(description, "masterchain reference points to a known-height fork")
			}
			continue
		}

		ancestor, err := view.validatorContext.IsMasterchainAncestor(*ref)
		if err != nil {
			if errors.Is(err, blockproof.ErrShardTopValidatorContextNotReady) {
				return false, shardTopDescriptionNotReady(description, "masterchain ancestry is unavailable", err)
			}
			return false, fmt.Errorf("check masterchain ancestry for %s: %w", shardTopDescriptionBlock(description), err)
		}
		if !ancestor {
			return false, shardTopDescriptionInvalid(description, "masterchain reference is not an ancestor")
		}
	}

	return tooNew, nil
}

func validateShardTopRegistryAnchors(
	description *p2p.ShardBlockDescription,
	anchors blockproof.ShardTopAnchors,
	tooNew bool,
) error {
	if anchors.Count != 1 && anchors.Count != 2 {
		return shardTopDescriptionNotReady(description, "shard registry has no usable target anchor", nil)
	}
	if anchors.Left.Seqno >= description.Block.SeqNo ||
		(anchors.Count == 2 && anchors.Right.Seqno >= description.Block.SeqNo) {
		return shardTopDescriptionInvalid(description, "shard-registry anchor is not older than the target")
	}

	terminal := &description.Chain[len(description.Chain)-1]
	if len(terminal.PrevRefs) == 0 || len(terminal.PrevRefs) > 2 {
		return shardTopDescriptionInvalid(description, "terminal proof-chain link has an invalid predecessor count")
	}
	if shardTopAnchorBehindPredecessor(anchors, terminal.PrevRefs) {
		return shardTopDescriptionNotReady(description, "proof chain starts after the current shard-registry anchor", nil)
	}
	if tooNew {
		return nil
	}

	maxSeqno := anchors.Left.Seqno
	if anchors.Count == 2 {
		maxSeqno = max(maxSeqno, anchors.Right.Seqno)
	}
	chainLength := int(description.Block.SeqNo - maxSeqno)
	if chainLength > len(description.Chain) {
		return shardTopDescriptionNotReady(description, "proof chain does not reach the current shard-registry anchor", nil)
	}
	if chainLength < len(description.Chain) {
		if anchors.Count != 1 || anchors.Left.Shard != description.Block.Shard ||
			!anchors.Left.Matches(description.Chain[chainLength].Block) {
			return shardTopDescriptionInvalid(description, "proof chain disagrees with the current shard registry")
		}
		return nil
	}

	if len(terminal.PrevRefs) != int(anchors.Count) ||
		!shardTopPredecessorsContain(terminal.PrevRefs, anchors.Left) ||
		(anchors.Count == 2 && !shardTopPredecessorsContain(terminal.PrevRefs, anchors.Right)) {
		return shardTopDescriptionInvalid(description, "proof chain disagrees with the current shard registry")
	}

	return nil
}

func shardTopAnchorBehindPredecessor(anchors blockproof.ShardTopAnchors, predecessors []ton.BlockIDExt) bool {
	for index := range predecessors {
		predecessor := predecessors[index]
		if anchors.Left.Shard == predecessor.Shard && anchors.Left.Seqno < predecessor.SeqNo {
			return true
		}
		if anchors.Count == 2 && anchors.Right.Shard == predecessor.Shard && anchors.Right.Seqno < predecessor.SeqNo {
			return true
		}
	}

	return false
}

func shardTopPredecessorsContain(predecessors []ton.BlockIDExt, anchor blockproof.ShardTopAnchor) bool {
	for index := range predecessors {
		if anchor.Matches(predecessors[index]) {
			return true
		}
	}

	return false
}

func sameShardTopBlock(left, right ton.BlockIDExt) bool {
	return left.Equals(&right)
}

func shardTopDescriptionInvalid(description *p2p.ShardBlockDescription, reason string) error {
	return fmt.Errorf("shard top %s %s", shardTopDescriptionBlock(description), reason)
}

func shardTopDescriptionNotReady(
	description *p2p.ShardBlockDescription,
	reason string,
	cause error,
) error {
	if cause != nil {
		return fmt.Errorf("%w: shard top %s %s: %v", p2p.ErrBroadcastSignatureRetryable, shardTopDescriptionBlock(description), reason, cause)
	}

	return fmt.Errorf("%w: shard top %s %s", p2p.ErrBroadcastSignatureRetryable, shardTopDescriptionBlock(description), reason)
}

func shardTopDescriptionBlock(description *p2p.ShardBlockDescription) string {
	if description == nil {
		return "<nil>"
	}

	return storage.FormatBlockRef(description.Block)
}
