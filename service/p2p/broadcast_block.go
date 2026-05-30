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

func (n *Node) decodeBroadcastBlock(ctx context.Context, msg any) (*DownloadedBlock, error) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcastCompressedV2:
		return n.decodeBlockBroadcastCompressedV2(ctx, data)
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

func decodeBroadcastBlock(msg any) (*DownloadedBlock, error) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		signatures, err := ordinaryBroadcastSignatureSetCell(data.CatchainSeqno, data.ValidatorSetHash, data.Signatures)
		if err != nil {
			return nil, err
		}

		block, err := decodeRawDownloadedBlock("tonNode.blockBroadcast", data.ID, data.Proof, data.Data, broadcastProofIsLink(data.ID))
		if err != nil {
			return nil, err
		}
		block.BroadcastSignatures = signatures
		return block, nil
	case tonnodeapi.BlockBroadcastCompressed:
		return decodeBlockBroadcastCompressed(data)
	case tonnodeapi.BlockBroadcastCompressedV2:
		return nil, fmt.Errorf("tonNode.blockBroadcastCompressedV2 requires state-aware decode")
	default:
		return nil, fmt.Errorf("unexpected broadcast block %T", msg)
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

		root, err := parseDownloadedBlockData("tonNode.newBlockCandidateBroadcastCompressed", decompressed)
		if err != nil {
			return nil, err
		}
		return newVerifiedBlockCandidateBroadcast("tonNode.newBlockCandidateBroadcastCompressed", data.ID, serializeCompressedBlockRoot(root), root)
	case tonnodeapi.NewBlockCandidateBroadcastCompressedV2:
		roots, err := cell.DecompressBOC(data.Compressed, maxDecompressedBlockSize, nil)
		if err != nil {
			return nil, fmt.Errorf("decompress tonNode.newBlockCandidateBroadcastCompressedV2: %w", err)
		}
		if len(roots) != 1 {
			return nil, fmt.Errorf("expected 1 root in tonNode.newBlockCandidateBroadcastCompressedV2, got %d", len(roots))
		}
		return newVerifiedBlockCandidateBroadcast("tonNode.newBlockCandidateBroadcastCompressedV2", data.ID, serializeCompressedBlockRoot(roots[0]), roots[0])
	default:
		return nil, fmt.Errorf("unexpected block candidate broadcast %T", msg)
	}
}

func decodeRawBlockCandidateBroadcast(kind string, id ton.BlockIDExt, data []byte) (*DownloadedBlock, error) {
	root, err := parseDownloadedBlockData(kind, data)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("%s file hash mismatch for %s", kind, formatBlockRef(id))
	}

	parsed, err := tnstore.ParseVerifiedBlockCell(id, effectiveRoot)
	if err != nil {
		return nil, fmt.Errorf("%s parse verified block %s: %w", kind, formatBlockRef(id), err)
	}
	if parsed.StateUpdate == nil {
		return nil, fmt.Errorf("%s block %s has no state update", kind, formatBlockRef(id))
	}
	if err = cell.ValidateMerkleUpdate(parsed.StateUpdate); err != nil {
		return nil, fmt.Errorf("%s validate state update %s: %w", kind, formatBlockRef(id), err)
	}
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

func decodeBlockBroadcastCompressed(data tonnodeapi.BlockBroadcastCompressed) (*DownloadedBlock, error) {
	decompressed, err := decompressLZ4Block(data.Compressed, maxDecompressedBlockSize)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.blockBroadcastCompressed: %w", err)
	}

	var payload tonnodeapi.BlockBroadcastCompressedData
	left, err := tl.Parse(&payload, decompressed, true)
	if err != nil {
		return nil, fmt.Errorf("parse tonNode.blockBroadcastCompressed.data: %w", err)
	}
	if len(left) > 0 {
		return nil, fmt.Errorf("tonNode.blockBroadcastCompressed.data has %d trailing bytes", len(left))
	}

	roots, err := cell.FromBOCMultiRoot(payload.ProofData)
	if err != nil {
		return nil, fmt.Errorf("parse tonNode.blockBroadcastCompressed proof_data: %w", err)
	}
	if len(roots) != 2 {
		return nil, fmt.Errorf("expected 2 roots in tonNode.blockBroadcastCompressed proof_data, got %d", len(roots))
	}

	signatures, err := ordinaryBroadcastSignatureSetCell(data.CatchainSeqno, data.ValidatorSetHash, payload.Signatures)
	if err != nil {
		return nil, err
	}

	proof := cell.ToBOCWithOptions([]*cell.Cell{roots[0]}, cell.BOCSerializeOptions{WithCRC32C: false})
	blockBOC := serializeCompressedBlockRoot(roots[1])

	block, err := newVerifiedDownloadedBlock("tonNode.blockBroadcastCompressed", data.ID, proof, blockBOC, broadcastProofIsLink(data.ID), roots[0], roots[1])
	if err != nil {
		return nil, err
	}
	block.BroadcastSignatures = signatures
	return block, nil
}

func decodeBlockBroadcastCompressedV2(data tonnodeapi.BlockBroadcastCompressedV2, state *cell.Cell) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof("tonNode.blockBroadcastCompressedV2", data.Proof)
	if err != nil {
		return nil, err
	}
	return decodeBlockBroadcastCompressedV2WithProofRoot(data, state, proofRoot)
}

func (n *Node) decodeBlockBroadcastCompressedV2(ctx context.Context, data tonnodeapi.BlockBroadcastCompressedV2) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof("tonNode.blockBroadcastCompressedV2", data.Proof)
	if err != nil {
		return nil, err
	}
	return n.decodeBlockBroadcastCompressedV2WithProofRoot(ctx, data, proofRoot, ton.BlockIDExt{})
}

func (n *Node) decodeBlockBroadcastCompressedV2WithProofRoot(ctx context.Context, data tonnodeapi.BlockBroadcastCompressedV2, proofRoot *cell.Cell, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	needState, err := cell.NeedStateForDecompression(data.DataCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.blockBroadcastCompressedV2 compression: %w", err)
	}
	if !needState {
		return decodeBlockBroadcastCompressedV2WithProofRoot(data, nil, proofRoot)
	}
	if len(prev.RootHash) == 0 {
		prev, err = compressedBlockPreviousState(data.ID, proofRoot)
		if err != nil {
			return nil, err
		}
	}

	if ctx == nil {
		ctx = context.Background()
	}
	state, err := n.stateForCompressedBlockDecompressionPrev(ctx, prev)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, &broadcastDecompressionStateNotReadyError{
				err:       fmt.Errorf("%w: %v", errBroadcastDecompressionStateNotReady, err),
				prev:      prev,
				proofRoot: proofRoot,
			}
		}
		return nil, err
	}
	return decodeBlockBroadcastCompressedV2WithProofRoot(data, state, proofRoot)
}

func decodeBlockBroadcastCompressedV2WithProofRoot(data tonnodeapi.BlockBroadcastCompressedV2, state *cell.Cell, proofRoot *cell.Cell) (*DownloadedBlock, error) {
	signatures, err := blockBroadcastCompressedV2Signatures(data)
	if err != nil {
		return nil, err
	}

	needState, err := cell.NeedStateForDecompression(data.DataCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.blockBroadcastCompressedV2 compression: %w", err)
	}
	if needState && state == nil {
		return nil, errBroadcastDecompressionStateNotReady
	}

	roots, err := cell.DecompressBOC(data.DataCompressed, maxDecompressedBlockSize, state)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.blockBroadcastCompressedV2: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("expected 1 root in tonNode.blockBroadcastCompressedV2, got %d", len(roots))
	}

	blockBOC := serializeCompressedBlockRoot(roots[0])
	block, err := newVerifiedDownloadedBlock("tonNode.blockBroadcastCompressedV2", data.ID, data.Proof, blockBOC, broadcastProofIsLink(data.ID), proofRoot, roots[0])
	if err != nil {
		return nil, err
	}
	block.BroadcastSignatures = signatures
	return block, nil
}

func blockBroadcastCompressedV2Signatures(data tonnodeapi.BlockBroadcastCompressedV2) (*cell.Cell, error) {
	signatures, err := broadcastSignatureSetCell(data.SignatureSet)
	if errors.Is(err, errBroadcastSignatureSetNonFinal) && !isMasterchainBlock(data.ID) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return signatures, nil
}

func broadcastSignatureSetFromDecoded(msg any, downloaded *DownloadedBlock) (*blockproof.ValidatorSignatureSet, error) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcastCompressedV2:
		return broadcastSignatureSetFromTL(data.SignatureSet)
	default:
		if downloaded == nil || downloaded.BroadcastSignatures == nil {
			return nil, fmt.Errorf("broadcast block has no validator signatures")
		}
		return blockproof.ParseValidatorSignatureSetCell(downloaded.BroadcastSignatures)
	}
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

func broadcastSignatureSetCell(sigSet any) (*cell.Cell, error) {
	switch sig := sigSet.(type) {
	case tonnodeapi.SignatureSetOrdinary:
		return ordinaryBroadcastSignatureSetCell(sig.CatchainSeqno, sig.ValidatorSetHash, sig.Signatures)
	case tonnodeapi.SignatureSetSimplex:
		return simplexBroadcastSignatureSetCell(sig)
	default:
		return nil, fmt.Errorf("%w: %T", errBroadcastSignatureSetUnsupported, sigSet)
	}
}

func ordinaryBroadcastSignatureSetCell(catchainSeqno int32, validatorSetHash int32, signatures []tonnodeapi.BlockSignature) (*cell.Cell, error) {
	dict, err := broadcastSignatureDict(signatures)
	if err != nil {
		return nil, err
	}

	return cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(uint64(uint32(validatorSetHash)), 32).
		MustStoreUInt(uint64(uint32(catchainSeqno)), 32).
		MustStoreUInt(uint64(len(signatures)), 32).
		MustStoreUInt(0, 64).
		MustStoreDict(dict).
		EndCell(), nil
}

func simplexBroadcastSignatureSetCell(sig tonnodeapi.SignatureSetSimplex) (*cell.Cell, error) {
	if !sig.Final {
		return nil, errBroadcastSignatureSetNonFinal
	}
	if len(sig.SessionID) != 32 {
		return nil, fmt.Errorf("invalid simplex session id len %d", len(sig.SessionID))
	}

	candidate, err := tl.Serialize(sig.Candidate, true)
	if err != nil {
		return nil, fmt.Errorf("serialize simplex candidate: %w", err)
	}

	dict, err := broadcastSignatureDict(sig.Signatures)
	if err != nil {
		return nil, err
	}

	candidateCell := cell.BeginCell().MustStoreBinarySnake(candidate).EndCell()
	return cell.BeginCell().
		MustStoreUInt(0x12, 8).
		MustStoreUInt(uint64(uint32(sig.ValidatorSetHash)), 32).
		MustStoreUInt(uint64(uint32(sig.CatchainSeqno)), 32).
		MustStoreUInt(uint64(len(sig.Signatures)), 32).
		MustStoreUInt(0, 64).
		MustStoreDict(dict).
		MustStoreSlice(sig.SessionID, 256).
		MustStoreUInt(uint64(uint32(sig.Slot)), 32).
		MustStoreRef(candidateCell).
		EndCell(), nil
}

func broadcastSignatureDict(signatures []tonnodeapi.BlockSignature) (*cell.Dictionary, error) {
	dict := cell.NewDict(16)
	for i, sig := range signatures {
		if len(sig.Who) != 32 {
			return nil, fmt.Errorf("invalid validator node id len %d", len(sig.Who))
		}
		if len(sig.Signature) != 64 {
			return nil, fmt.Errorf("invalid validator signature len %d", len(sig.Signature))
		}

		key := cell.BeginCell().MustStoreUInt(uint64(i), 16).EndCell()
		value := cell.BeginCell().
			MustStoreSlice(sig.Who, 256).
			MustStoreUInt(5, 4).
			MustStoreSlice(sig.Signature[:32], 256).
			MustStoreSlice(sig.Signature[32:], 256).
			EndCell()
		if err := dict.Set(key, value); err != nil {
			return nil, fmt.Errorf("store broadcast signature %d: %w", i, err)
		}
	}
	return dict, nil
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
