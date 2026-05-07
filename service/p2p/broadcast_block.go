package p2p

import (
	"context"
	"errors"
	"fmt"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errBroadcastSignatureSetUnsupported = errors.New("broadcast signature set is not supported")

func (n *Node) decodeBroadcastBlock(ctx context.Context, msg any) (*DownloadedBlock, error) {
	block, err := decodeBroadcastBlock(msg)
	if !errors.Is(err, ErrCompressedBlockV2Unsupported) {
		return block, err
	}

	data, ok := msg.(BlockBroadcastCompressedV2)
	if !ok {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	state, stateErr := n.stateForCompressedBlockDecompression(ctx, data.ID, data.Proof)
	if stateErr != nil {
		return nil, fmt.Errorf("%w: %v", err, stateErr)
	}
	return decodeBlockBroadcastCompressedV2(data, state)
}

func decodeBroadcastBlock(msg any) (*DownloadedBlock, error) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		signatures, err := ordinaryBroadcastSignatureSetCell(data.CatchainSeqno, data.ValidatorSetHash, data.Signatures)
		if err != nil {
			return nil, err
		}

		block, err := normalizeDownloadedBlock("tonNode.blockBroadcast", data.ID, data.Proof, data.Data, broadcastProofIsLink(data.ID), true, nil)
		if err != nil {
			return nil, err
		}
		block.BroadcastSignatures = signatures
		return block, nil
	case BlockBroadcastCompressed:
		return decodeBlockBroadcastCompressed(data)
	case BlockBroadcastCompressedV2:
		return decodeBlockBroadcastCompressedV2(data, nil)
	default:
		return nil, fmt.Errorf("unexpected broadcast block %T", msg)
	}
}

func decodeBlockBroadcastCompressed(data BlockBroadcastCompressed) (*DownloadedBlock, error) {
	decompressed, err := decompressLZ4Block(data.Compressed, maxDecompressedBlockSize)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.blockBroadcastCompressed: %w", err)
	}

	var payload BlockBroadcastCompressedData
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

	block, err := normalizeDownloadedBlock("tonNode.blockBroadcastCompressed", data.ID, proof, blockBOC, broadcastProofIsLink(data.ID), true, roots[1])
	if err != nil {
		return nil, err
	}
	block.BroadcastSignatures = signatures
	return block, nil
}

func decodeBlockBroadcastCompressedV2(data BlockBroadcastCompressedV2, state *cell.Cell) (*DownloadedBlock, error) {
	signatures, err := broadcastSignatureSetCell(data.SignatureSet)
	if err != nil {
		return nil, err
	}

	needState, err := cell.NeedStateForDecompression(data.DataCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.blockBroadcastCompressedV2 compression: %w", err)
	}
	if needState && state == nil {
		return nil, ErrCompressedBlockV2Unsupported
	}

	roots, err := cell.DecompressBOC(data.DataCompressed, maxDecompressedBlockSize, state)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.blockBroadcastCompressedV2: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("expected 1 root in tonNode.blockBroadcastCompressedV2, got %d", len(roots))
	}

	blockBOC := serializeCompressedBlockRoot(roots[0])
	block, err := normalizeDownloadedBlock("tonNode.blockBroadcastCompressedV2", data.ID, data.Proof, blockBOC, broadcastProofIsLink(data.ID), true, roots[0])
	if err != nil {
		return nil, err
	}
	block.BroadcastSignatures = signatures
	return block, nil
}

func broadcastProofIsLink(id ton.BlockIDExt) bool {
	return id.Workchain != -1 || id.Shard != topShard
}

func broadcastSignatureSetCell(sigSet any) (*cell.Cell, error) {
	switch sig := sigSet.(type) {
	case SignatureSetOrdinary:
		return ordinaryBroadcastSignatureSetCell(sig.CatchainSeqno, sig.ValidatorSetHash, sig.Signatures)
	case SignatureSetSimplex:
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

func simplexBroadcastSignatureSetCell(sig SignatureSetSimplex) (*cell.Cell, error) {
	if !sig.Final {
		return nil, fmt.Errorf("%w: non-final simplex signature set", errBroadcastSignatureSetUnsupported)
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
