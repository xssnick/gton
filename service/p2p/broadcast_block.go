package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errBroadcastSignatureSetUnsupported = errors.New("broadcast signature set is not supported")
var errBroadcastSignatureSetNonFinal = errors.New("broadcast signature set is not final")
var errBroadcastDecompressionStateNotReady = errors.New("compressed broadcast previous state is not ready")
var errBlockFileHashMismatch = errors.New("file hash mismatch")

type broadcastDecompressionStateNotReadyError struct {
	err       error
	prev      ton.BlockIDExt
	proofRoot *cell.Cell
}

func (e *broadcastDecompressionStateNotReadyError) Error() string {
	return e.err.Error()
}

func (e *broadcastDecompressionStateNotReadyError) Unwrap() error {
	return errBroadcastDecompressionStateNotReady
}

// decodeBroadcastBlock decodes a full block broadcast. proofRoot and signatures
// may carry the values already parsed by predecodeBlockBroadcastSignatureCheck
// so the proof BOC and the signature set are not parsed a second time; nil
// values fall back to parsing from the payload.
func (n *Node) decodeBroadcastBlock(ctx context.Context, msg any, proofRoot *cell.Cell, signatures *blockproof.ValidatorSignatureSet) (*DownloadedBlock, *blockproof.ValidatorSignatureSet, error) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		if proofRoot == nil || signatures == nil {
			return decodeBroadcastBlock(msg)
		}
		block, err := decodeRawDownloadedBlockWithShape("tonNode.blockBroadcast", data.ID, data.Proof, data.Data, broadcastProofIsLink(data.ID), proofRoot, false)
		if err != nil {
			return nil, nil, err
		}
		return block, signatures, nil
	case tonnodeapi.BlockBroadcastCompressedV2:
		if proofRoot == nil {
			var err error
			proofRoot, err = parseDownloadedBlockProof(data.Proof)
			if err != nil {
				return nil, nil, fmt.Errorf("parse tonNode.blockBroadcastCompressedV2 proof: %w", err)
			}
		}
		return n.decodeBlockBroadcastCompressedV2WithProofRoot(ctx, data, proofRoot, signatures, ton.BlockIDExt{})
	default:
		return decodeBroadcastBlock(msg)
	}
}

func isBroadcastDecompressionStateNotReady(err error) bool {
	return errors.Is(err, errBroadcastDecompressionStateNotReady)
}

func broadcastDecompressionStateNotReady(err error) (*broadcastDecompressionStateNotReadyError, bool) {
	var stateErr *broadcastDecompressionStateNotReadyError
	if errors.As(err, &stateErr) {
		return stateErr, true
	}
	return nil, false
}

func decodeBroadcastBlock(msg any) (*DownloadedBlock, *blockproof.ValidatorSignatureSet, error) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		signatures, err := ordinaryBroadcastValidatorSignatureSet(data.CatchainSeqno, data.ValidatorSetHash, data.Signatures)
		if err != nil {
			return nil, nil, err
		}

		block, err := decodeRawDownloadedBlock("tonNode.blockBroadcast", data.ID, data.Proof, data.Data, broadcastProofIsLink(data.ID))
		if err != nil {
			return nil, nil, err
		}
		return block, signatures, nil
	case tonnodeapi.BlockBroadcastCompressed:
		return decodeBlockBroadcastCompressed(data)
	case tonnodeapi.BlockBroadcastCompressedV2:
		return nil, nil, fmt.Errorf("tonNode.blockBroadcastCompressedV2 requires state-aware decode")
	default:
		return nil, nil, fmt.Errorf("unexpected broadcast block %T", msg)
	}
}

func predecodeBlockBroadcastSignatureCheck(block ton.BlockIDExt, msg any) (*cell.Cell, *blockproof.ValidatorSignatureSet, bool, error) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		proofRoot, err := parseDownloadedBlockProof(data.Proof)
		if err != nil {
			return nil, nil, true, fmt.Errorf("parse tonNode.blockBroadcast proof: %w", err)
		}
		signatures, err := ordinaryBroadcastValidatorSignatureSet(data.CatchainSeqno, data.ValidatorSetHash, data.Signatures)
		if err != nil {
			return nil, nil, true, err
		}
		return proofRoot, signatures, true, nil
	case tonnodeapi.BlockBroadcastCompressedV2:
		proofRoot, err := parseDownloadedBlockProof(data.Proof)
		if err != nil {
			return nil, nil, true, fmt.Errorf("parse tonNode.blockBroadcastCompressedV2 proof: %w", err)
		}
		signatures, err := broadcastSignatureSetFromTL(data.SignatureSet)
		if err != nil {
			return nil, nil, true, err
		}
		if isMasterchainBlock(block) && !signatures.Final() {
			return nil, nil, true, errBroadcastSignatureSetNonFinal
		}
		return proofRoot, signatures, true, nil
	default:
		return nil, nil, false, nil
	}
}

func decodeBlockCandidateBroadcast(msg any) (*DownloadedBlock, error) {
	switch data := msg.(type) {
	case tonnodeapi.NewBlockCandidateBroadcast:
		return decodeRawBlockCandidateBroadcast("tonNode.newBlockCandidateBroadcast", data.ID, data.Data)
	case tonnodeapi.NewBlockCandidateBroadcastCompressed:
		decompressed, err := decompressLZ4Block(data.Compressed, maxDecompressedBlockSize)
		if err != nil {
			return nil, fmt.Errorf("decompress tonNode.newBlockCandidateBroadcastCompressed: %w", err)
		}

		// the wire payload is a mode-2 re-serialization (see the C++ sender),
		// while FileHash covers the canonical mode-31 bytes — parse and
		// re-serialize in one pass over the parser's cell graph
		roots, blockBOC, err := cell.ReserializeBOC(decompressed, compressedBlockRootSerializeOptions)
		if err != nil {
			return nil, fmt.Errorf("parse tonNode.newBlockCandidateBroadcastCompressed block: %w", err)
		}
		if len(roots) != 1 {
			return nil, fmt.Errorf("expected 1 root in tonNode.newBlockCandidateBroadcastCompressed, got %d", len(roots))
		}
		downloaded, err := newVerifiedBlockCandidateBroadcast("tonNode.newBlockCandidateBroadcastCompressed", data.ID, blockBOC, roots[0])
		if errors.Is(err, errBlockFileHashMismatch) {
			// the graph fast path keeps sender-side duplicate cells while the
			// reference node deduplicates on re-serialization; retry with the
			// canonical serializer before rejecting, so a candidate a C++ node
			// would accept is not dropped here
			downloaded, err = newVerifiedBlockCandidateBroadcast("tonNode.newBlockCandidateBroadcastCompressed", data.ID, serializeCompressedBlockRoot(roots[0]), roots[0])
		}
		return downloaded, err
	case tonnodeapi.NewBlockCandidateBroadcastCompressedV2:
		roots, blockBOC, err := cell.DecompressBOCSerialized(data.Compressed, maxDecompressedBlockSize, nil, compressedBlockRootSerializeOptions)
		if err != nil {
			return nil, fmt.Errorf("decompress tonNode.newBlockCandidateBroadcastCompressedV2: %w", err)
		}
		if len(roots) != 1 {
			return nil, fmt.Errorf("expected 1 root in tonNode.newBlockCandidateBroadcastCompressedV2, got %d", len(roots))
		}
		return newVerifiedBlockCandidateBroadcast("tonNode.newBlockCandidateBroadcastCompressedV2", data.ID, blockBOC, roots[0])
	default:
		return nil, fmt.Errorf("unexpected block candidate broadcast %T", msg)
	}
}

func decodeRawBlockCandidateBroadcast(kind string, id ton.BlockIDExt, data []byte) (*DownloadedBlock, error) {
	root, err := parseDownloadedBlockData(data)
	if err != nil {
		return nil, fmt.Errorf("parse %s block: %w", kind, err)
	}
	return newVerifiedBlockCandidateBroadcast(kind, id, data, root)
}

func newVerifiedBlockCandidateBroadcast(kind string, id ton.BlockIDExt, data []byte, root *cell.Cell) (*DownloadedBlock, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%s block is empty", kind)
	}

	effectiveRoot, err := effectiveDownloadedBlockRoot(id, false, root)
	if err != nil {
		return nil, fmt.Errorf("%s block root: %w", kind, err)
	}

	rootHash := effectiveRoot.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return nil, fmt.Errorf("%s root hash mismatch for %s", kind, formatBlockRef(id))
	}

	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], id.FileHash) {
		return nil, fmt.Errorf("%s %w for %s", kind, errBlockFileHashMismatch, formatBlockRef(id))
	}

	parsed, err := tnstore.ParseVerifiedBlockCell(id, effectiveRoot)
	if err != nil {
		return nil, fmt.Errorf("%s parse verified block %s: %w", kind, formatBlockRef(id), err)
	}
	if parsed.StateUpdate == nil {
		return nil, fmt.Errorf("%s block %s has no state update", kind, formatBlockRef(id))
	}
	// The standalone merkle-update walk is intentionally skipped: candidate
	// content is anchored by the root/file hash checks above, the finality
	// assemble path only pairs candidates whose full ID matches a
	// signature-verified block, and the liveview non-final store validates
	// candidate-origin updates itself before building speculative state.
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		return nil, fmt.Errorf("%s build block meta %s: %w", kind, formatBlockRef(id), err)
	}

	return &DownloadedBlock{
		ID:               id,
		Kind:             kind,
		Block:            effectiveRoot,
		BlockBOC:         data,
		Meta:             meta,
		StateUpdate:      parsed.StateUpdate,
		VerifiedRootHash: true,
	}, nil
}

func decodeBlockBroadcastCompressed(data tonnodeapi.BlockBroadcastCompressed) (*DownloadedBlock, *blockproof.ValidatorSignatureSet, error) {
	decompressed, err := decompressLZ4Block(data.Compressed, maxDecompressedBlockSize)
	if err != nil {
		return nil, nil, fmt.Errorf("decompress tonNode.blockBroadcastCompressed: %w", err)
	}

	var payload tonnodeapi.BlockBroadcastCompressedData
	left, err := tl.Parse(&payload, decompressed, true)
	if err != nil {
		return nil, nil, fmt.Errorf("parse tonNode.blockBroadcastCompressed.data: %w", err)
	}
	if len(left) > 0 {
		return nil, nil, fmt.Errorf("tonNode.blockBroadcastCompressed.data has %d trailing bytes", len(left))
	}

	roots, err := cell.FromBOCMultiRoot(payload.ProofData)
	if err != nil {
		return nil, nil, fmt.Errorf("parse tonNode.blockBroadcastCompressed proof_data: %w", err)
	}
	if len(roots) != 2 {
		return nil, nil, fmt.Errorf("expected 2 roots in tonNode.blockBroadcastCompressed proof_data, got %d", len(roots))
	}

	signatures, err := ordinaryBroadcastValidatorSignatureSet(data.CatchainSeqno, data.ValidatorSetHash, payload.Signatures)
	if err != nil {
		return nil, nil, err
	}

	proof := cell.ToBOCWithOptions([]*cell.Cell{roots[0]}, cell.BOCSerializeOptions{WithCRC32C: false})
	blockBOC := serializeCompressedBlockRoot(roots[1])

	block, err := newVerifiedDownloadedBlockWithShape(
		"tonNode.blockBroadcastCompressed",
		data.ID,
		proof,
		blockBOC,
		broadcastProofIsLink(data.ID),
		roots[0],
		roots[1],
		false,
		false,
	)
	if err != nil {
		return nil, nil, err
	}
	return block, signatures, nil
}

func (n *Node) decodeBlockBroadcastCompressedV2WithProofRoot(ctx context.Context, data tonnodeapi.BlockBroadcastCompressedV2, proofRoot *cell.Cell, signatures *blockproof.ValidatorSignatureSet, prev ton.BlockIDExt) (*DownloadedBlock, *blockproof.ValidatorSignatureSet, error) {
	needState, err := cell.NeedStateForDecompression(data.DataCompressed)
	if err != nil {
		return nil, nil, fmt.Errorf("check tonNode.blockBroadcastCompressedV2 compression: %w", err)
	}
	if !needState {
		return decodeBlockBroadcastCompressedV2WithProofRoot(data, nil, proofRoot, signatures)
	}
	if len(prev.RootHash) == 0 {
		prev, err = compressedBlockPreviousState(data.ID, proofRoot)
		if err != nil {
			return nil, nil, err
		}
	}

	state, err := n.stateForCompressedBlockDecompressionPrev(ctx, prev)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, nil, &broadcastDecompressionStateNotReadyError{
				err:       fmt.Errorf("%w: %v", errBroadcastDecompressionStateNotReady, err),
				prev:      prev,
				proofRoot: proofRoot,
			}
		}
		return nil, nil, err
	}
	if state == nil {
		return nil, nil, errBroadcastDecompressionStateNotReady
	}
	downloaded, sigSet, err := decodeBlockBroadcastCompressedV2WithProofRoot(data, state, proofRoot, signatures)
	if err == nil {
		n.scheduleCompressedStateChain(state, downloaded)
	}
	return downloaded, sigSet, err
}

// scheduleCompressedStateChain applies the just-decoded merkle update to the
// state it was decompressed against and remembers the resulting materialized
// next-state tree, so the following block's state-aware decompression finds
// an in-memory root instead of walking lazy celldb cells. Every current
// caller decodes only after the validator-signature check, but the chain does
// not depend on that ordering: the content is pinned by the block's root/file
// hash checks, so entries for real block IDs are always genuine and forged
// IDs only occupy bounded TTL'd cache slots nobody asks for. The apply
// pipeline later overwrites the entry with the canonical state.
func (n *Node) scheduleCompressedStateChain(prevState *cell.Cell, downloaded *DownloadedBlock) {
	if n.compressedState == nil || prevState == nil || downloaded == nil || downloaded.StateUpdate == nil {
		return
	}
	meta := downloaded.Meta
	if meta == nil || len(meta.StateRootHash) != 32 {
		return
	}

	block := downloaded.ID
	update := downloaded.StateUpdate
	stateRootHash := append([]byte(nil), meta.StateRootHash...)
	n.runAsync(func() {
		next, _, err := cell.ApplyMerkleUpdate(prevState, update)
		if err != nil {
			n.log.Debug().
				Err(err).
				Str("block", formatBlockRef(block)).
				Msg("skip compressed state chain: merkle update does not apply")
			return
		}
		nextHash := next.HashKey(0)
		if !bytes.Equal(nextHash[:], stateRootHash) {
			n.log.Debug().
				Str("block", formatBlockRef(block)).
				Msg("skip compressed state chain: state root hash mismatch")
			return
		}
		if n.compressedState.RememberCompressedBlockState(&tnstore.BlockState{
			Block:         block,
			StateRootHash: stateRootHash,
			Cell:          next,
		}) {
			n.NotifyCompressedBlockStateReady(block)
		}
	})
}

// decodeBlockBroadcastCompressedV2WithProofRoot decodes a compressed-v2 block
// broadcast. A nil signatures set is built (and Final-checked) from the TL
// payload; a non-nil one was already built and Final-checked by predecode.
// The caller resolved the state requirement: state is nil only for payloads
// that do not need one. The state-update consistency check is skipped here —
// the block content is anchored by root/file hash and validator signatures,
// and the apply path re-validates the merkle update against the actual state.
func decodeBlockBroadcastCompressedV2WithProofRoot(data tonnodeapi.BlockBroadcastCompressedV2, state *cell.Cell, proofRoot *cell.Cell, signatures *blockproof.ValidatorSignatureSet) (*DownloadedBlock, *blockproof.ValidatorSignatureSet, error) {
	if signatures == nil {
		var err error
		signatures, err = broadcastSignatureSetFromTL(data.SignatureSet)
		if err != nil {
			return nil, nil, err
		}
		if isMasterchainBlock(data.ID) && !signatures.Final() {
			return nil, nil, errBroadcastSignatureSetNonFinal
		}
	}

	roots, blockBOC, err := cell.DecompressBOCSerialized(data.DataCompressed, maxDecompressedBlockSize, state, compressedBlockRootSerializeOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("decompress tonNode.blockBroadcastCompressedV2: %w", err)
	}
	if len(roots) != 1 {
		return nil, nil, fmt.Errorf("expected 1 root in tonNode.blockBroadcastCompressedV2, got %d", len(roots))
	}

	block, err := newVerifiedDownloadedBlockWithShape(
		"tonNode.blockBroadcastCompressedV2",
		data.ID,
		data.Proof,
		blockBOC,
		broadcastProofIsLink(data.ID),
		proofRoot,
		roots[0],
		false,
		false,
	)
	if err != nil {
		return nil, nil, err
	}
	return block, signatures, nil
}

func broadcastSignatureSetFromTL(sigSet any) (*blockproof.ValidatorSignatureSet, error) {
	switch sig := sigSet.(type) {
	case tonnodeapi.SignatureSetOrdinary:
		return ordinaryBroadcastValidatorSignatureSet(sig.CatchainSeqno, sig.ValidatorSetHash, sig.Signatures)
	case tonnodeapi.SignatureSetSimplex:
		return simplexBroadcastValidatorSignatureSet(sig)
	default:
		return nil, fmt.Errorf("%w: %T", errBroadcastSignatureSetUnsupported, sigSet)
	}
}

func ordinaryBroadcastValidatorSignatureSet(catchainSeqno int32, validatorSetHash int32, signatures []tonnodeapi.BlockSignature) (*blockproof.ValidatorSignatureSet, error) {
	parsed, err := broadcastSignatures(signatures)
	if err != nil {
		return nil, err
	}
	return blockproof.NewOrdinaryValidatorSignatureSet(uint32(catchainSeqno), uint32(validatorSetHash), parsed), nil
}

func simplexBroadcastValidatorSignatureSet(sig tonnodeapi.SignatureSetSimplex) (*blockproof.ValidatorSignatureSet, error) {
	if len(sig.SessionID) != 32 {
		return nil, fmt.Errorf("invalid simplex session id len %d", len(sig.SessionID))
	}
	candidate, err := tl.Serialize(sig.Candidate, true)
	if err != nil {
		return nil, fmt.Errorf("serialize simplex candidate: %w", err)
	}
	signatures, err := broadcastSignatures(sig.Signatures)
	if err != nil {
		return nil, err
	}
	return blockproof.NewSimplexValidatorSignatureSet(uint32(sig.CatchainSeqno), uint32(sig.ValidatorSetHash), signatures, sig.Final, sig.SessionID, sig.Slot, candidate), nil
}

func broadcastProofIsLink(id ton.BlockIDExt) bool {
	return id.Workchain != -1 || id.Shard != topShard
}

func broadcastSignatures(signatures []tonnodeapi.BlockSignature) ([]ton.Signature, error) {
	parsed := make([]ton.Signature, 0, len(signatures))
	for _, sig := range signatures {
		if len(sig.Who) != 32 {
			return nil, fmt.Errorf("invalid validator node id len %d", len(sig.Who))
		}
		if len(sig.Signature) != 64 {
			return nil, fmt.Errorf("invalid validator signature len %d", len(sig.Signature))
		}
		parsed = append(parsed, ton.Signature{
			NodeIDShort: append([]byte(nil), sig.Who...),
			Signature:   append([]byte(nil), sig.Signature...),
		})
	}
	return parsed, nil
}
