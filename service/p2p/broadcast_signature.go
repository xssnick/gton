package p2p

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ErrBroadcastSignatureRetryable marks local verifier state gaps that should not poison broadcast dedupe.
var ErrBroadcastSignatureRetryable = errors.New("broadcast signature check is retryable")

func (n *Node) checkBlockBroadcastSignatures(kind string, block ton.BlockIDExt, proof *cell.Cell, signatures *blockproof.ValidatorSignatureSet) error {
	if n.signatureVerifier == nil {
		return fmt.Errorf("broadcast signature verifier is not configured")
	}
	if proof == nil {
		return fmt.Errorf("block broadcast %s has no proof root", formatBlockRef(block))
	}
	if signatures == nil {
		return fmt.Errorf("block broadcast %s has no validator signatures", formatBlockRef(block))
	}

	ctx, cancel := context.WithTimeout(n.runtimeContext(), broadcastSignatureTimeout)
	defer cancel()

	return n.signatureVerifier.CheckBlockBroadcastSignatures(ctx, BlockBroadcastSignatureCheck{
		Kind:       kind,
		Block:      block,
		Proof:      proof,
		Signatures: signatures,
	})
}

func (n *Node) checkBlockFinalitySignatures(kind string, block ton.BlockIDExt, signatures *blockproof.ValidatorSignatureSet) (*BlockFinalitySignatureCheckResult, error) {
	if n.signatureVerifier == nil {
		return nil, fmt.Errorf("broadcast signature verifier is not configured")
	}

	ctx, cancel := context.WithTimeout(n.runtimeContext(), broadcastSignatureTimeout)
	defer cancel()

	return n.signatureVerifier.CheckBlockFinalitySignatures(ctx, BlockFinalitySignatureCheck{
		Kind:       kind,
		Block:      block,
		Signatures: signatures,
	})
}

func (n *Node) validateShardDescriptionBroadcast(block ton.BlockIDExt, catchainSeqno int32, data []byte) (*ShardBlockDescription, error) {
	if n.signatureVerifier == nil {
		return nil, fmt.Errorf("broadcast signature verifier is not configured")
	}

	ctx, cancel := context.WithTimeout(n.runtimeContext(), broadcastSignatureTimeout)
	defer cancel()

	return n.signatureVerifier.ValidateShardDescriptionBroadcast(ctx, ShardDescriptionSignatureCheck{
		Block:         block,
		CatchainSeqno: catchainSeqno,
		Data:          data,
	})
}
