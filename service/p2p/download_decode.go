package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/pierrec/lz4/v4"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (n *Node) decodeDownloadedBlock(ctx context.Context, resp tl.Serializable) (*DownloadedBlock, error) {
	switch data := resp.(type) {
	case tonnodeapi.DataFull:
		downloaded, err := decodeRawDownloadedBlock("tonNode.dataFull", data.ID, data.Proof, data.Block, data.IsLink)
		if err == nil || !n.IsHardfork(data.ID) {
			return downloaded, err
		}
		return decodeRawDownloadedHardforkBlock("tonNode.dataFull", data.ID, data.Proof, data.Block, data.IsLink, err)
	case DataFullCompressed:
		downloaded, err := decodeCompressedBlock(data)
		if err == nil || !n.IsHardfork(data.ID) {
			return downloaded, err
		}
		return decodeCompressedHardforkBlock(data, err)
	case DataFullCompressedV2:
		return n.decodeDataFullCompressedV2(ctx, data)
	default:
		return decodeDownloadedBlock(resp)
	}
}

func decodeDownloadedBlock(resp tl.Serializable) (*DownloadedBlock, error) {
	switch data := resp.(type) {
	case tonnodeapi.DataFull:
		return decodeRawDownloadedBlock("tonNode.dataFull", data.ID, data.Proof, data.Block, data.IsLink)
	case tonnodeapi.DataFullEmpty:
		return nil, ErrBlockNotAvailable
	case DataFullCompressed:
		return decodeCompressedBlock(data)
	case DataFullCompressedV2:
		return nil, fmt.Errorf("tonNode.dataFullCompressedV2 requires state-aware decode")
	default:
		return nil, fmt.Errorf("unexpected download response %T", resp)
	}
}

func decodeCompressedBlock(data DataFullCompressed) (*DownloadedBlock, error) {
	decompressed, err := decompressLZ4Block(data.Compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.dataFullCompressed: %w", err)
	}

	roots, err := cell.FromBOCMultiRoot(decompressed)
	if err != nil {
		return nil, fmt.Errorf("parse decompressed multi-root boc: %w", err)
	}
	if len(roots) != 2 {
		return nil, fmt.Errorf("expected 2 roots in tonNode.dataFullCompressed, got %d", len(roots))
	}

	proof := cell.ToBOCWithOptions([]*cell.Cell{roots[0]}, cell.BOCSerializeOptions{WithCRC32C: false})
	block := serializeCompressedBlockRoot(roots[1])

	return newVerifiedDownloadedBlockWithProofShape(
		"tonNode.dataFullCompressed",
		data.ID,
		proof,
		block,
		data.IsLink,
		roots[0],
		roots[1],
		false,
	)
}

func decodeCompressedHardforkBlock(data DataFullCompressed, cause error) (*DownloadedBlock, error) {
	decompressed, err := decompressLZ4Block(data.Compressed)
	if err != nil {
		return nil, cause
	}

	roots, err := cell.FromBOCMultiRoot(decompressed)
	if err != nil || len(roots) != 2 {
		return nil, cause
	}

	proof := cell.ToBOCWithOptions([]*cell.Cell{roots[0]}, cell.BOCSerializeOptions{WithCRC32C: false})
	block := serializeCompressedBlockRoot(roots[1])
	return newVerifiedDownloadedHardforkBlock("tonNode.dataFullCompressed", data.ID, proof, block, data.IsLink, roots[0], roots[1], cause)
}

func (n *Node) decodeDataFullCompressedV2(ctx context.Context, data DataFullCompressedV2) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(data.Proof)
	if err != nil {
		return nil, fmt.Errorf("parse tonNode.dataFullCompressedV2 proof: %w", err)
	}

	needState, err := cell.NeedStateForDecompression(data.BlockCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.dataFullCompressedV2 compression: %w", err)
	}
	if !needState {
		downloaded, err := decodeCompressedBlockV2WithProofRoot(data, nil, proofRoot)
		if err == nil || !n.IsHardfork(data.ID) {
			return downloaded, err
		}
		return decodeCompressedBlockV2WithProofRootForHardfork(data, nil, proofRoot, err)
	}

	state, err := n.stateForCompressedBlockDecompression(ctx, data.ID, proofRoot)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: %v", ErrCompressedBlockStateNotReady, err)
		}
		return nil, err
	}
	downloaded, err := decodeCompressedBlockV2WithProofRoot(data, state, proofRoot)
	if err == nil || !n.IsHardfork(data.ID) {
		return downloaded, err
	}
	return decodeCompressedBlockV2WithProofRootForHardfork(data, state, proofRoot, err)
}

func decodeCompressedBlockV2WithProofRoot(data DataFullCompressedV2, state *cell.Cell, proofRoot *cell.Cell) (*DownloadedBlock, error) {
	needState, err := cell.NeedStateForDecompression(data.BlockCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.dataFullCompressedV2 compression: %w", err)
	}
	if needState && state == nil {
		return nil, ErrCompressedBlockStateNotReady
	}

	roots, block, err := cell.DecompressBOCSerialized(data.BlockCompressed, maxDecompressedBlockSize, state, compressedBlockRootSerializeOptions)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.dataFullCompressedV2: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("expected 1 root in tonNode.dataFullCompressedV2, got %d", len(roots))
	}

	return newVerifiedDownloadedBlockWithProofShape(
		"tonNode.dataFullCompressedV2",
		data.ID,
		data.Proof,
		block,
		data.IsLink,
		proofRoot,
		roots[0],
		false,
	)
}

func decodeCompressedBlockV2WithProofRootForHardfork(data DataFullCompressedV2, state *cell.Cell, proofRoot *cell.Cell, cause error) (*DownloadedBlock, error) {
	needState, err := cell.NeedStateForDecompression(data.BlockCompressed)
	if err != nil {
		return nil, cause
	}
	if needState && state == nil {
		return nil, cause
	}

	roots, block, err := cell.DecompressBOCSerialized(data.BlockCompressed, maxDecompressedBlockSize, state, compressedBlockRootSerializeOptions)
	if err != nil || len(roots) != 1 {
		return nil, cause
	}

	return newVerifiedDownloadedHardforkBlock("tonNode.dataFullCompressedV2", data.ID, data.Proof, block, data.IsLink, proofRoot, roots[0], cause)
}

func (n *Node) stateForCompressedBlockDecompression(ctx context.Context, block ton.BlockIDExt, proofRoot *cell.Cell) (*cell.Cell, error) {
	if n.stateArtifacts == nil {
		return nil, fmt.Errorf("state storage is not configured")
	}

	prev, err := compressedBlockPreviousState(block, proofRoot)
	if err != nil {
		return nil, err
	}
	return n.stateForCompressedBlockDecompressionPrev(ctx, prev)
}

func compressedBlockPreviousState(block ton.BlockIDExt, proofRoot *cell.Cell) (ton.BlockIDExt, error) {
	prevBlocks, err := prevBlocksFromBlockProof(block, proofRoot)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if len(prevBlocks) != 1 {
		return ton.BlockIDExt{}, fmt.Errorf("state-aware decompression with %d previous blocks is not supported", len(prevBlocks))
	}
	return prevBlocks[0], nil
}

func (n *Node) stateForCompressedBlockDecompressionPrev(ctx context.Context, prev ton.BlockIDExt) (*cell.Cell, error) {
	if n.stateArtifacts == nil {
		return nil, fmt.Errorf("state storage is not configured")
	}

	if n.compressedState != nil {
		state, err := n.compressedState.StateRootForCompressedBlock(ctx, prev)
		if err == nil {
			return state, nil
		}
		if !errors.Is(err, tnstore.ErrNotFound) {
			return nil, err
		}
	}

	meta, err := n.peerStorage.BlockMeta(ctx, prev)
	if errors.Is(err, tnstore.ErrNotFound) {
		state, stateErr := n.stateArtifacts.BlockState(ctx, prev)
		if stateErr != nil {
			return nil, stateErr
		}
		meta, err = tnstore.BuildBlockMetaFromState(*state)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("%w: previous state root hash is not known for %s", tnstore.ErrNotFound, tnstore.FormatBlockRef(prev))
	}

	root, err := n.stateArtifacts.LoadStateCellTree(ctx, prev, meta.StateRootHash)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func prevBlocksFromBlockProof(block ton.BlockIDExt, proofRoot *cell.Cell) ([]ton.BlockIDExt, error) {
	parsed, err := parseBlockProofForBlock(block, proofRoot)
	if err != nil {
		return nil, fmt.Errorf("parse previous blocks from proof: %w", err)
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(block, parsed)
	if err != nil {
		return nil, err
	}
	return meta.PrevRefs, nil
}

func decodeRawDownloadedBlock(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(proof)
	if err != nil {
		return nil, fmt.Errorf("parse %s proof: %w", kind, err)
	}
	return decodeRawDownloadedBlockWithProofRoot(kind, id, proof, data, isLink, proofRoot)
}

func decodeRawDownloadedBlockWithProofRoot(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell) (*DownloadedBlock, error) {
	return decodeRawDownloadedBlockWithShape(kind, id, proof, data, isLink, proofRoot, true)
}

func decodeRawDownloadedBlockWithShape(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, validateStateUpdate bool) (*DownloadedBlock, error) {
	blockRoot, err := parseDownloadedBlockData(data)
	if err != nil {
		return nil, fmt.Errorf("parse %s block: %w", kind, err)
	}
	return newVerifiedDownloadedBlockWithShape(
		kind,
		id,
		proof,
		data,
		isLink,
		proofRoot,
		blockRoot,
		false,
		validateStateUpdate,
	)
}

func decodeRawDownloadedHardforkBlock(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, cause error) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(proof)
	if err != nil {
		return nil, cause
	}
	blockRoot, err := parseDownloadedBlockData(data)
	if err != nil {
		return nil, cause
	}
	return newVerifiedDownloadedHardforkBlock(kind, id, proof, data, isLink, proofRoot, blockRoot, cause)
}

func parseDownloadedBlockProof(proof []byte) (*cell.Cell, error) {
	if len(proof) == 0 {
		return nil, fmt.Errorf("proof is empty")
	}

	proofRoot, err := cell.FromBOC(proof)
	if err != nil {
		return nil, fmt.Errorf("proof is not a valid BOC: %w", err)
	}
	return proofRoot, nil
}

func parseDownloadedBlockData(data []byte) (*cell.Cell, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("block is empty")
	}

	blockRoot, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("block is not a valid BOC: %w", err)
	}
	return blockRoot, nil
}

func newVerifiedDownloadedHardforkBlock(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, blockRoot *cell.Cell, cause error) (*DownloadedBlock, error) {
	downloaded, err := newVerifiedDownloadedBlockWithProofShape(kind, id, proof, data, isLink, proofRoot, blockRoot, true)
	if err != nil {
		return nil, fmt.Errorf("%v; hardfork decode: %w", cause, err)
	}
	return downloaded, nil
}

func newVerifiedDownloadedBlockWithProofShape(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, blockRoot *cell.Cell, hardfork bool) (*DownloadedBlock, error) {
	return newVerifiedDownloadedBlockWithShape(kind, id, proof, data, isLink, proofRoot, blockRoot, hardfork, true)
}

// newVerifiedDownloadedBlockWithShape verifies and assembles a downloaded
// block. validateStateUpdate runs the standalone merkle-update consistency
// walk; broadcast decode paths skip it because the block content is anchored
// by the root/file hash checks below plus validator signatures, and the apply
// path re-validates the update against the actual previous state.
func newVerifiedDownloadedBlockWithShape(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, blockRoot *cell.Cell, hardfork bool, validateStateUpdate bool) (*DownloadedBlock, error) {
	if len(proof) == 0 {
		return nil, fmt.Errorf("%s proof is empty", kind)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%s block is empty", kind)
	}

	var err error
	if hardfork {
		err = blockproof.CheckHardforkProofShape(id, proofRoot, isLink)
	} else {
		err = blockproof.CheckProofShape(id, proofRoot, isLink)
	}
	if err != nil {
		return nil, fmt.Errorf("%s proof shape: %w", kind, err)
	}

	effectiveRoot, err := effectiveDownloadedBlockRoot(id, isLink, blockRoot)
	if err != nil {
		return nil, fmt.Errorf("%s block root: %w", kind, err)
	}

	rootHash := effectiveRoot.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return nil, fmt.Errorf("%s root hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], id.FileHash) {
		return nil, fmt.Errorf("%s file hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	parsed, err := tnstore.ParseVerifiedBlockCell(id, effectiveRoot)
	if err != nil {
		return nil, fmt.Errorf("%s parse verified block %s: %w", kind, tnstore.FormatBlockRef(id), err)
	}
	if hardfork {
		if err = blockproof.ValidateHardforkBlock(id, parsed); err != nil {
			return nil, fmt.Errorf("%s validate hardfork block %s: %w", kind, tnstore.FormatBlockRef(id), err)
		}
	}
	if parsed.StateUpdate == nil {
		return nil, fmt.Errorf("%s block %s has no state update", kind, tnstore.FormatBlockRef(id))
	}
	if validateStateUpdate {
		if err := cell.ValidateMerkleUpdate(parsed.StateUpdate); err != nil {
			return nil, fmt.Errorf("%s validate state update %s: %w", kind, tnstore.FormatBlockRef(id), err)
		}
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		return nil, fmt.Errorf("%s build block meta %s: %w", kind, tnstore.FormatBlockRef(id), err)
	}

	return &DownloadedBlock{
		ID:               id,
		Kind:             kind,
		Block:            effectiveRoot,
		Proof:            proofRoot,
		BlockBOC:         data,
		ProofBOC:         proof,
		Meta:             meta,
		StateUpdate:      parsed.StateUpdate,
		IsLink:           isLink,
		VerifiedRootHash: true,
	}, nil
}

func effectiveDownloadedBlockRoot(id ton.BlockIDExt, isLink bool, root *cell.Cell) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("block %s has no parsed root", tnstore.FormatBlockRef(id))
	}
	if !isLink || root.GetType() != cell.MerkleProofCellType {
		return root, nil
	}
	unwrapped, err := cell.UnwrapProof(root, id.RootHash)
	if err != nil {
		return nil, fmt.Errorf("unwrap merkle proof link for %s: %w", tnstore.FormatBlockRef(id), err)
	}
	return unwrapped, nil
}

func decompressLZ4Block(data []byte) ([]byte, error) {
	// blocks compress roughly 2-4x, so 4x the compressed size lands within
	// one attempt for almost every payload; every retry re-decodes the whole
	// prefix, so undershooting is far more expensive than overshooting
	size := 1 << 20
	if estimated := 4 * len(data); estimated > size {
		size = estimated
	}
	if size > maxDecompressedBlockSize {
		size = maxDecompressedBlockSize
	}

	for {
		buf := make([]byte, size)
		n, err := lz4.UncompressBlock(data, buf)
		switch {
		case err == nil:
			return buf[:n], nil
		case !errors.Is(err, lz4.ErrInvalidSourceShortBuffer):
			return nil, err
		case size == maxDecompressedBlockSize:
			return nil, fmt.Errorf("decompressed data exceeds %d bytes", maxDecompressedBlockSize)
		}

		size *= 4
		if size > maxDecompressedBlockSize {
			size = maxDecompressedBlockSize
		}
	}
}

// compressedBlockRootSerializeOptions reproduce reference
// std_boc_serialize(root, 31). The reference does not mark the root for a top
// hash, so WithTopHash intentionally remains disabled.
var compressedBlockRootSerializeOptions = cell.BOCSerializeOptions{
	WithCRC32C:    true,
	WithIndex:     true,
	WithCacheBits: true,
	WithIntHashes: true,
}

func serializeCompressedBlockRoot(root *cell.Cell) []byte {
	return root.ToBOCWithOptions(compressedBlockRootSerializeOptions)
}
