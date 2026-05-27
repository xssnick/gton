package p2p

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

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

	ctx := n.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, broadcastSignatureTimeout)
	defer cancel()

	return n.signatureVerifier.CheckBlockBroadcastSignatures(ctx, BlockBroadcastSignatureCheck{
		Kind:       kind,
		Block:      block,
		Proof:      proof,
		Signatures: signatures,
	})
}

func (n *Node) checkShardDescriptionSignatures(block ton.BlockIDExt, catchainSeqno int32, data []byte) error {
	if n.signatureVerifier == nil {
		return fmt.Errorf("broadcast signature verifier is not configured")
	}

	ctx := n.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, broadcastSignatureTimeout)
	defer cancel()

	return n.signatureVerifier.CheckShardDescriptionSignatures(ctx, ShardDescriptionSignatureCheck{
		Block:         block,
		CatchainSeqno: catchainSeqno,
		Data:          append([]byte(nil), data...),
	})
}
