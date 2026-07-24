package blockproof

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	masterchainID           int32 = -1
	workchainInvalid              = int32(-1 << 31)
	MaxShardBlockProofLinks       = 8
)

type ProofStore interface {
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
}

func MasterRefForBlock(ctx context.Context, store ProofStore, id ton.BlockIDExt) (ton.BlockIDExt, error) {
	if id.Workchain == masterchainID {
		return id, nil
	}

	meta, err := store.BlockMeta(ctx, id)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{}, fmt.Errorf("%w: block doesn't have masterchain ref", storage.ErrNotFound)
	}
	if !meta.MasterchainRefKnown() {
		return ton.BlockIDExt{}, fmt.Errorf("%w: block doesn't have masterchain ref", storage.ErrNotFound)
	}

	resolved, err := store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     meta.MasterchainRefSeqno,
	})
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("lookup masterchain ref #%d: %w", meta.MasterchainRefSeqno, err)
	}
	if !IsFullBlockID(resolved) {
		return ton.BlockIDExt{}, fmt.Errorf("resolved masterchain ref is not full: %s", storage.FormatBlockRef(resolved))
	}
	return resolved, nil
}

func ShardBlockProofLinks(ctx context.Context, store ProofStore, master ton.BlockIDExt, target ton.BlockIDExt) ([]ton.ShardBlockLink, error) {
	if master.Workchain != masterchainID {
		return nil, fmt.Errorf("masterchain block id must be specified")
	}

	current := master
	links := make([]ton.ShardBlockLink, 0, 2)
	for {
		root, err := store.BlockRoot(ctx, current)
		if err != nil {
			return nil, err
		}

		next, err := nextShardProofBlock(current, root, target)
		if err != nil {
			return nil, err
		}

		proof, err := shardLinkProofBOC(current, root)
		if err != nil {
			return nil, err
		}
		links = append(links, ton.ShardBlockLink{
			ID:    CloneBlockID(next),
			Proof: proof,
		})

		if BlockIDEqual(next, target) {
			return links, nil
		}
		if len(links) == MaxShardBlockProofLinks {
			return nil, fmt.Errorf("proof chain is too long")
		}

		if current.Workchain != masterchainID && next.SeqNo >= current.SeqNo {
			return nil, fmt.Errorf("proof chain does not progress")
		}

		current = next
	}
}

func BlockLinkBackward(ctx context.Context, store ProofStore, from ton.BlockIDExt, to ton.BlockIDExt) (ton.BlockLinkBackward, error) {
	fromRoot, _, err := StoredMasterProofRoot(ctx, store, from, true)
	if err != nil {
		return ton.BlockLinkBackward{}, fmt.Errorf("load source proof link for %s: %w", storage.FormatBlockRef(from), err)
	}

	stateRoot, err := LoadStateRoot(ctx, store, from)
	if err != nil {
		return ton.BlockLinkBackward{}, err
	}
	proof, err := BlockStateRootProof(fromRoot)
	if err != nil {
		return ton.BlockLinkBackward{}, err
	}

	stateProof, err := OldMasterBlockStateProof(stateRoot, to)
	if err != nil {
		return ton.BlockLinkBackward{}, err
	}

	var destProof []byte
	toKeyBlock := to.SeqNo == 0
	if to.SeqNo != 0 {
		toRoot, err := store.BlockRoot(ctx, to)
		if err != nil {
			return ton.BlockLinkBackward{}, err
		}
		destProof, err = BlockHeaderProofBOC(toRoot, to, 0)
		if err != nil {
			return ton.BlockLinkBackward{}, err
		}

		meta, err := store.BlockMeta(ctx, to)
		if err != nil {
			return ton.BlockLinkBackward{}, err
		}
		toKeyBlock = meta.Has(storage.BlockMetaIsKeyBlock)
	}

	return ton.BlockLinkBackward{
		ToKeyBlock: toKeyBlock,
		From:       CloneBlockID(from),
		To:         CloneBlockID(to),
		DestProof:  destProof,
		Proof:      proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		StateProof: stateProof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}, nil
}

func LoadStateRoot(ctx context.Context, store ProofStore, id ton.BlockIDExt) (*cell.Cell, error) {
	meta, err := store.BlockMeta(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load block meta for state %s: %w", storage.FormatBlockRef(id), err)
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash is missing for %s", storage.FormatBlockRef(id))
	}

	root, err := store.LoadStateCellTree(ctx, id, meta.StateRootHash)
	if err != nil {
		return nil, fmt.Errorf("load state root %x for %s: %w", meta.StateRootHash, storage.FormatBlockRef(id), err)
	}
	return root, nil
}

func StoredMasterProofRoot(ctx context.Context, store ProofStore, id ton.BlockIDExt, link bool) (*cell.Cell, *Parsed, error) {
	data, err := StoredMasterProof(ctx, store, id, link)
	if err != nil {
		return nil, nil, err
	}

	proofRoot, err := cell.FromBOC(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parse stored proof: %w", err)
	}
	if err = CheckProofShape(id, proofRoot, link); err != nil {
		return nil, nil, err
	}

	parsed, err := ParseCell(id, proofRoot)
	if err != nil {
		return nil, nil, err
	}

	root, err := cell.UnwrapProofVirtualized(parsed.Proof.Root, id.RootHash)
	if err != nil {
		return nil, nil, err
	}
	return root, parsed, nil
}

func StoredMasterProof(ctx context.Context, store ProofStore, id ton.BlockIDExt, link bool) ([]byte, error) {
	if id.Workchain != masterchainID {
		return nil, fmt.Errorf("block must be a masterchain block")
	}

	meta, err := store.BlockMeta(ctx, id)
	if err != nil {
		return nil, err
	}

	if link {
		return storedMasterProofLink(ctx, store, id, meta)
	}

	kind := storedMasterFullProofKind(meta)
	if !meta.HasProof(kind) {
		return nil, storage.ErrNotFound
	}
	return store.BlockProof(ctx, kind, id)
}

func storedMasterProofLink(ctx context.Context, store ProofStore, id ton.BlockIDExt, meta *storage.BlockMeta) ([]byte, error) {
	fullKind := storedMasterFullProofKind(meta)
	if meta.HasProof(fullKind) {
		full, err := store.BlockProof(ctx, fullKind, id)
		if err != nil {
			return nil, err
		}
		return LinkBOC(id, full)
	}

	linkKind := storedMasterLinkProofKind(meta)
	if !meta.HasProof(linkKind) {
		return nil, storage.ErrNotFound
	}
	return store.BlockProof(ctx, linkKind, id)
}

func BlockHeaderProofBOC(root *cell.Cell, id ton.BlockIDExt, mode uint32) ([]byte, error) {
	proof, err := BlockHeaderProof(root, id, mode)
	if err != nil {
		return nil, err
	}
	return proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), nil
}

func BlockHeaderProof(root *cell.Cell, id ton.BlockIDExt, mode uint32) (*cell.Cell, error) {
	return CreateUsageProof(root, func(root *cell.Cell) error {
		return visitBlockHeader(root, id, mode)
	})
}

func BroadcastProofRoot(id ton.BlockIDExt, root *cell.Cell) (*cell.Cell, error) {
	hash := root.HashKey()
	if !bytes.Equal(hash[:], id.RootHash) {
		return nil, fmt.Errorf("block root hash mismatch for %s", storage.FormatBlockRef(id))
	}

	const mode = 1 | 2 | 16
	return CreateUsageProof(root, func(root *cell.Cell) error {
		if err := visitBlockHeader(root, id, mode); err != nil {
			return err
		}
		return visitKeyBlockValidatorConfig(root)
	})
}

func BlockStateRootProof(root *cell.Cell) (*cell.Cell, error) {
	if root != nil {
		root = root.Virtualize(0)
	}

	rootLoader, err := VisitBlockRoot(root)
	if err != nil {
		return nil, err
	}

	info, err := rootLoader.PeekRefCellAt(0)
	if err != nil {
		return nil, err
	}
	info, err = info.Prewarm()
	if err != nil {
		return nil, err
	}
	valueFlow, err := rootLoader.PeekRefCellAt(1)
	if err != nil {
		return nil, err
	}
	update, err := rootLoader.PeekRefCellAt(2)
	if err != nil {
		return nil, fmt.Errorf("block has no state update: %w", err)
	}
	extra, err := rootLoader.PeekRefCellAt(3)
	if err != nil {
		return nil, err
	}

	proofUpdate, err := blockStateRootProofUpdate(update)
	if err != nil {
		return nil, err
	}
	valueFlowProof, err := cell.CreatePrunedBranch(valueFlow, 1, 0)
	if err != nil {
		return nil, err
	}
	extraProof, err := cell.CreatePrunedBranch(extra, 1, 0)
	if err != nil {
		return nil, err
	}
	body, err := root.RebuildWithRefs([]*cell.Cell{info, valueFlowProof, proofUpdate, extraProof})
	if err != nil {
		return nil, err
	}
	return cell.CreateMerkleProof(body)
}

func OldMasterBlockStateProof(stateRoot *cell.Cell, id ton.BlockIDExt) (*cell.Cell, error) {
	return CreateUsageProof(stateRoot, func(root *cell.Cell) error {
		prefix, err := LoadMcStateExtraPrefix(root, false)
		if err != nil {
			return err
		}
		if err = VisitMcStateExtraInfo(prefix.Info); err != nil {
			return err
		}
		if err = VisitCell(prefix.Config.Config); err != nil {
			return err
		}

		seqno, err := ShardStateSeqno(root)
		if err != nil {
			return err
		}
		if seqno != 0 {
			if _, err = oldMasterBlockIDFromInfo(prefix.Info, 0); err != nil {
				return err
			}
		}

		old, err := oldMasterBlockIDFromInfo(prefix.Info, id.SeqNo)
		if err != nil {
			return err
		}
		if !BlockIDEqual(old, id) {
			return fmt.Errorf("state contains %s for seqno %d", storage.FormatBlockRef(old), id.SeqNo)
		}
		return nil
	})
}

func nextShardProofBlock(current ton.BlockIDExt, root *cell.Cell, target ton.BlockIDExt) (ton.BlockIDExt, error) {
	block, err := storage.ParseVerifiedBlockCell(current, root)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	if current.Workchain == masterchainID {
		if block.Extra == nil || block.Extra.Custom == nil || block.Extra.Custom.ShardHashes == nil {
			return ton.BlockIDExt{}, fmt.Errorf("masterchain block is missing shard hashes")
		}
		next, _, err := ShardInfoFromHashes(block.Extra.Custom.ShardHashes, target.Workchain, shardProofLookupShard(target.Shard), false)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
		return next, nil
	}

	meta, err := storage.BuildBlockMetaFromParsedBlock(current, block)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	for _, prev := range meta.PrevRefs {
		if ShardIntersects(storage.ShardKeyFromBlock(prev), storage.ShardKeyFromBlock(target)) {
			return prev, nil
		}
	}
	return ton.BlockIDExt{}, fmt.Errorf("failed to find block chain")
}

func shardProofLookupShard(shard int64) int64 {
	prefixLen := ShardPrefixLen(uint64(shard))
	if prefixLen < 0 {
		return shard
	}
	return int64((uint64(shard) &^ (uint64(1) << (63 - prefixLen))) | 1)
}

func shardLinkProofBOC(id ton.BlockIDExt, root *cell.Cell) ([]byte, error) {
	mode := uint32(0)
	if id.Workchain == masterchainID {
		mode = 16 | 32
	}
	return BlockHeaderProofBOC(root, id, mode)
}

func blockStateRootProofUpdate(update *cell.Cell) (*cell.Cell, error) {
	loader, err := update.BeginParse()
	if err != nil {
		return nil, err
	}
	if loader.BaseCell().GetType() != cell.MerkleUpdateCellType {
		return nil, fmt.Errorf("invalid Merkle update in block")
	}
	if loader.RefsNum() < 2 {
		return nil, fmt.Errorf("invalid Merkle update in block")
	}

	from, err := loader.PeekRefCellAt(0)
	if err != nil {
		return nil, err
	}
	to, err := loader.PeekRefCellAt(1)
	if err != nil {
		return nil, err
	}

	fromProof, err := cell.CreatePrunedBranch(from, 2, 1)
	if err != nil {
		return nil, err
	}
	toProof, err := cell.CreatePrunedBranch(to, 2, 1)
	if err != nil {
		return nil, err
	}

	return cell.CreateMerkleUpdate(fromProof, toProof)
}

func CreateUsageProof(root *cell.Cell, visit func(*cell.Cell) error) (*cell.Cell, error) {
	builder := NewProofBuilder(root)
	if visit != nil {
		if err := visit(builder.Root()); err != nil {
			return nil, err
		}
	}
	return builder.CreateProof()
}

func NewProofBuilder(root *cell.Cell) *cell.MerkleProofBuilder {
	if root != nil {
		root = root.Virtualize(0)
	}
	return cell.NewMerkleProofBuilder(root)
}

func visitBlockHeader(root *cell.Cell, id ton.BlockIDExt, mode uint32) error {
	rootLoader, err := VisitBlockRoot(root)
	if err != nil {
		return err
	}

	info, err := rootLoader.PeekRefCellAt(0)
	if err != nil {
		return err
	}
	if err = VisitCellRecursive(info); err != nil {
		return err
	}

	if mode&1 != 0 {
		update, err := rootLoader.PeekRefCellAt(2)
		if err != nil {
			return fmt.Errorf("block %s has no state update: %w", storage.FormatBlockRef(id), err)
		}
		if err = visitMerkleUpdate(update); err != nil {
			return err
		}
	}
	if mode&2 != 0 {
		valueFlow, err := rootLoader.PeekRefCellAt(1)
		if err != nil {
			return fmt.Errorf("block %s has no value flow: %w", storage.FormatBlockRef(id), err)
		}
		if err = VisitCellRecursive(valueFlow); err != nil {
			return err
		}
	}
	if mode&16 != 0 {
		extra, err := LoadBlockExtra(root)
		if err != nil {
			return err
		}
		if id.Workchain == masterchainID {
			if extra.Custom == nil {
				return fmt.Errorf("masterchain block extra is missing custom data")
			}
			if mode&32 != 0 {
				if extra.Custom.ShardHashes == nil {
					return fmt.Errorf("masterchain block extra is missing shard hashes")
				}
				if err = VisitCellRecursive(extra.Custom.ShardHashes.AsCell()); err != nil {
					return err
				}
			}
			if mode&64 != 0 && extra.Custom.Details.PrevBlockSignatures != nil {
				if err = VisitCellRecursive(extra.Custom.Details.PrevBlockSignatures.AsCell()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func VisitBlockRoot(root *cell.Cell) (*cell.Slice, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	magic, err := loader.LoadUInt(32)
	if err != nil {
		return nil, err
	}
	if magic != 0x11ef55aa {
		return nil, fmt.Errorf("invalid block magic %x", magic)
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if loader.RefsNum() < 4 {
		return nil, fmt.Errorf("block has too few refs")
	}
	return loader, nil
}

func visitMerkleUpdate(update *cell.Cell) error {
	loader, err := update.BeginParse()
	if err != nil {
		return err
	}
	if loader.BaseCell().GetType() != cell.MerkleUpdateCellType {
		return fmt.Errorf("invalid Merkle update in block")
	}
	if loader.RefsNum() < 2 {
		return fmt.Errorf("invalid Merkle update in block")
	}
	to, err := loader.PeekRefCellAt(1)
	if err != nil {
		return err
	}
	if err = VisitCell(to); err != nil {
		return err
	}
	return nil
}

func LoadBlockExtra(root *cell.Cell) (tlb.BlockExtra, error) {
	rootLoader, err := root.BeginParse()
	if err != nil {
		return tlb.BlockExtra{}, err
	}
	extra, err := rootLoader.PeekRefCellAt(3)
	if err != nil {
		return tlb.BlockExtra{}, err
	}
	loader, err := extra.BeginParse()
	if err != nil {
		return tlb.BlockExtra{}, err
	}

	var blockExtra tlb.BlockExtra
	if err = tlb.LoadFromCell(&blockExtra, loader); err != nil {
		return tlb.BlockExtra{}, err
	}
	return blockExtra, nil
}

func visitKeyBlockValidatorConfig(root *cell.Cell) error {
	extra, err := LoadBlockExtra(root)
	if err != nil {
		return err
	}
	if extra.Custom == nil || !extra.Custom.KeyBlock {
		return nil
	}
	if extra.Custom.ConfigParams == nil || extra.Custom.ConfigParams.Config.Params == nil {
		return fmt.Errorf("key block extra is missing config params")
	}

	cfg := tlb.BlockchainConfig{Root: extra.Custom.ConfigParams.Config.Params.AsCell()}
	for _, id := range []uint32{
		tlb.ConfigParamCatchainConfig,
		tlb.ConfigParamPrevValidators,
		tlb.ConfigParamPrevTempValidators,
		tlb.ConfigParamCurrentValidators,
		tlb.ConfigParamCurrentTempValidators,
		tlb.ConfigParamNextValidators,
		tlb.ConfigParamNextTempValidators,
	} {
		if err = visitConfigParam(cfg, id); err != nil {
			return err
		}
	}
	return nil
}

func visitConfigParam(cfg tlb.BlockchainConfig, id uint32) error {
	param, err := cfg.GetParam(id)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	return VisitCellRecursive(param)
}

func VisitMcStateExtraInfo(info *cell.Cell) error {
	loader, err := info.BeginParse()
	if err != nil {
		return err
	}
	flags, err := loader.LoadUInt(16)
	if err != nil {
		return err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err = prevBlocks.LoadFromCell(loader); err != nil {
		return err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return err
	}
	hasLastKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasLastKeyBlock {
		var lastKey tlb.ExtBlkRef
		if err = tlb.LoadFromCell(&lastKey, loader); err != nil {
			return err
		}
	}
	if flags&1 != 0 {
		return visitBlockCreateStats(loader)
	}
	return nil
}

func visitBlockCreateStats(loader *cell.Slice) error {
	magic, err := loader.LoadUInt(8)
	if err != nil {
		return err
	}
	switch magic {
	case 0x17:
		_, err = loader.LoadDict(256)
		return err
	case 0x34:
		_, err = loader.LoadAugDict(256, cell.ReadOnlyAugmentation{SkipExtraFn: skipUint32Boundary}, false)
		return err
	default:
		return fmt.Errorf("invalid block_create_stats magic %x", magic)
	}
}

func skipUint32Boundary(loader *cell.Slice) error {
	_, err := loader.LoadUInt(32)
	return err
}

func VisitCell(root *cell.Cell) error {
	if root == nil {
		return nil
	}
	_, err := root.BeginParse()
	return err
}

func VisitCellRecursive(root *cell.Cell) error {
	return visitCellRecursiveSeen(root, map[cell.Hash]struct{}{})
}

func visitCellRecursiveSeen(root *cell.Cell, seen map[cell.Hash]struct{}) error {
	if root == nil {
		return nil
	}
	key := root.HashKey()
	if _, ok := seen[key]; ok {
		return nil
	}
	seen[key] = struct{}{}

	loader, err := root.BeginParse()
	if err != nil {
		return err
	}
	for loader.RefsNum() > 0 {
		ref, err := loader.LoadRefCell()
		if err != nil {
			return fmt.Errorf("load proof ref: %w", err)
		}
		if err = visitCellRecursiveSeen(ref, seen); err != nil {
			return err
		}
	}
	return nil
}

func ShardInfoFromHashes(hashes *cell.Dictionary, wc int32, shard int64, exact bool) (ton.BlockIDExt, *cell.Cell, error) {
	if hashes == nil {
		return ton.BlockIDExt{}, nil, storage.ErrNotFound
	}

	value, err := hashes.LoadValue(cell.BeginCell().MustStoreInt(int64(wc), 32).EndCell())
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return ton.BlockIDExt{}, nil, storage.ErrNotFound
	}
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	root, err := value.LoadRefCell()
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	trueShard, leaf, err := findShardLeaf(root, uint64(shard), exact)
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}
	block, err := BlockIDFromShardLeaf(wc, trueShard, leaf)
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}
	return block, leaf, nil
}

func findShardLeaf(root *cell.Cell, shard uint64, exact bool) (int64, *cell.Cell, error) {
	prefixLen := ShardPrefixLen(shard)
	if prefixLen < 0 {
		return 0, nil, storage.ErrNotFound
	}

	node := root
	z := shard
	mask := uint64(math.MaxUint64)
	remaining := prefixLen
	for {
		loader, err := node.BeginParse()
		if err != nil {
			return 0, nil, err
		}
		typ, err := loader.LoadUInt(1)
		if err != nil {
			return 0, nil, err
		}
		if typ == 0 {
			if remaining != 0 && exact {
				return 0, nil, storage.ErrNotFound
			}
			trueShard := (shard | mask) - (mask >> 1)
			return int64(trueShard), node, nil
		}

		if remaining == 0 || loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
			return 0, nil, storage.ErrNotFound
		}

		node, err = loader.BaseCell().PeekRef(int(z >> 63))
		if err != nil {
			return 0, nil, err
		}
		z <<= 1
		remaining--
		mask >>= 1
	}
}

func BlockIDFromShardLeaf(wc int32, shard int64, leaf *cell.Cell) (ton.BlockIDExt, error) {
	loader, err := leaf.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	typ, err := loader.LoadUInt(1)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if typ != 0 {
		return ton.BlockIDExt{}, fmt.Errorf("shard leaf has invalid tag")
	}

	magic, err := loader.LoadUInt(4)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	switch magic {
	case 0xa:
		var desc tlb.ShardDesc
		if err = tlb.LoadFromCell(&desc, loader, true); err != nil {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{Workchain: wc, Shard: shard, SeqNo: desc.SeqNo, RootHash: desc.RootHash, FileHash: desc.FileHash}, nil
	case 0xb:
		var desc tlb.ShardDescB
		if err = tlb.LoadFromCell(&desc, loader, true); err != nil {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{Workchain: wc, Shard: shard, SeqNo: desc.SeqNo, RootHash: desc.RootHash, FileHash: desc.FileHash}, nil
	default:
		return ton.BlockIDExt{}, fmt.Errorf("wrong ShardDesc magic: %x", magic)
	}
}

func ShardPrefixLen(shard uint64) int {
	if shard == 0 {
		return -1
	}
	low := shard & -shard
	if low == 0 {
		return -1
	}
	return 63 - bits.TrailingZeros64(low)
}

func ShardIntersects(a, b storage.ShardKey) bool {
	if a.Workchain != b.Workchain {
		return false
	}
	aLen := ShardPrefixLen(uint64(a.Shard))
	bLen := ShardPrefixLen(uint64(b.Shard))
	if aLen < 0 || bLen < 0 {
		return false
	}
	minLen := aLen
	if bLen < minLen {
		minLen = bLen
	}
	return shardPrefix(uint64(a.Shard), minLen) == shardPrefix(uint64(b.Shard), minLen)
}

func shardPrefix(shard uint64, length int) uint64 {
	if length <= 0 {
		return 0
	}
	return shard >> (64 - length)
}

func oldMasterBlockIDFromInfo(info *cell.Cell, seqno uint32) (ton.BlockIDExt, error) {
	loader, err := info.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(16); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		return ton.BlockIDExt{}, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err := prevBlocks.LoadFromCell(loader); err != nil {
		return ton.BlockIDExt{}, err
	}
	return oldMasterBlockID(prevBlocks, seqno)
}

func ShardStateSeqno(stateRoot *cell.Cell) (uint32, error) {
	var state tlb.ShardStateUnsplit
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return 0, err
	}
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		return 0, err
	}
	return state.Seqno, nil
}

func storedMasterFullProofKind(meta *storage.BlockMeta) storage.ServedProofKind {
	if meta.Has(storage.BlockMetaIsKeyBlock) {
		return storage.ServedProofKeyBlock
	}
	return storage.ServedProofBlock
}

func storedMasterLinkProofKind(meta *storage.BlockMeta) storage.ServedProofKind {
	if meta.Has(storage.BlockMetaIsKeyBlock) {
		return storage.ServedProofKeyBlockLink
	}
	return storage.ServedProofBlockLink
}

func IsFullBlockID(id ton.BlockIDExt) bool {
	if len(id.RootHash) != 32 || len(id.FileHash) != 32 {
		return false
	}
	if id.Workchain == workchainInvalid || id.Shard == 0 || uint64(id.Shard)&7 != 0 || id.SeqNo > math.MaxInt32 {
		return false
	}
	if id.Workchain == masterchainID && id.Shard != masterchainShard {
		return false
	}

	var zeroHash [32]byte
	return [32]byte(id.RootHash) != zeroHash && [32]byte(id.FileHash) != zeroHash
}

func CloneBlockID(id ton.BlockIDExt) *ton.BlockIDExt {
	cloned := id
	cloned.RootHash = bytes.Clone(id.RootHash)
	cloned.FileHash = bytes.Clone(id.FileHash)
	return &cloned
}

func BlockIDEqual(a ton.BlockIDExt, b ton.BlockIDExt) bool {
	return a.Workchain == b.Workchain &&
		a.Shard == b.Shard &&
		a.SeqNo == b.SeqNo &&
		bytes.Equal(a.RootHash, b.RootHash) &&
		bytes.Equal(a.FileHash, b.FileHash)
}
