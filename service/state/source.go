package state

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var ErrStateNotAvailable = errors.New("state snapshot is not available")

type KeyBlockBatch struct {
	Blocks     []ton.BlockIDExt
	Incomplete bool
}

type ProofDownload struct {
	Data []byte
	Link bool
}

type Source interface {
	InitBlock(ctx context.Context) (ton.BlockIDExt, error)
	ZeroStateBlock(ctx context.Context) (ton.BlockIDExt, error)
	IsHardfork(ctx context.Context, block ton.BlockIDExt) bool
	ZeroState(ctx context.Context, block ton.BlockIDExt) (storage.DownloadedState, error)
	NextKeyBlocks(ctx context.Context, from ton.BlockIDExt, limit int32) (KeyBlockBatch, error)
	InitBlockProof(ctx context.Context, block ton.BlockIDExt) (ProofDownload, error)
	MasterchainProof(ctx context.Context, block ton.BlockIDExt, requireKey bool) ([]byte, error)
	BlockFull(ctx context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error)
	DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (storage.DownloadedState, error)
}

func ShardBlocksFromMasterState(state *storage.BlockState) ([]ton.BlockIDExt, error) {
	if state.Block.Workchain != -1 {
		return nil, fmt.Errorf("expected masterchain state, got %s", storage.FormatBlockRef(state.Block))
	}
	if state.Parsed == nil || state.Parsed.McStateExtra == nil {
		return nil, fmt.Errorf("masterchain state %s is missing parsed mc_state_extra", storage.FormatBlockRef(state.Block))
	}

	loader, err := state.Parsed.McStateExtra.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, loader); err != nil {
		return nil, fmt.Errorf("parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	shards, err := loadShardsFromHashes(extra.ShardHashes)
	if err != nil {
		return nil, fmt.Errorf("load shard hashes for %s: %w", storage.FormatBlockRef(state.Block), err)
	}

	res := make([]ton.BlockIDExt, 0, len(shards))
	for _, shard := range shards {
		res = append(res, *shard)
	}
	return res, nil
}

func ShardBlocksFromMasterBlock(block ton.BlockIDExt, parsed *tlb.Block) ([]ton.BlockIDExt, error) {
	if block.Workchain != -1 {
		return nil, fmt.Errorf("expected masterchain block, got %s", storage.FormatBlockRef(block))
	}
	if parsed == nil || parsed.Extra == nil || parsed.Extra.Custom == nil {
		return nil, fmt.Errorf("masterchain block %s is missing mc block extra", storage.FormatBlockRef(block))
	}
	shards, err := loadShardsFromHashes(parsed.Extra.Custom.ShardHashes)
	if err != nil {
		return nil, fmt.Errorf("load shard hashes for %s: %w", storage.FormatBlockRef(block), err)
	}

	res := make([]ton.BlockIDExt, 0, len(shards))
	for _, shard := range shards {
		res = append(res, *shard)
	}
	return res, nil
}

func loadShardsFromHashes(shardHashes *cell.Dictionary) ([]*ton.BlockIDExt, error) {
	if shardHashes == nil {
		return []*ton.BlockIDExt{}, nil
	}

	kvs, err := shardHashes.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to load shard hashes dict: %w", err)
	}

	var shards []*ton.BlockIDExt
	for _, kv := range kvs {
		workchain, err := kv.Key.LoadInt(32)
		if err != nil {
			return nil, fmt.Errorf("failed to load workchain: %w", err)
		}

		root, err := kv.Value.LoadRef()
		if err != nil {
			return nil, fmt.Errorf("failed to load bin tree ref: %w", err)
		}

		err = walkShardBinTree(root, cell.BeginCell(), func(key *cell.Cell, value *cell.Slice) error {
			shardID, err := shardFromBinTreeKey(key)
			if err != nil {
				return err
			}

			shard, err := loadShardDesc(int32(workchain), shardID, value)
			if err != nil {
				return err
			}
			shards = append(shards, shard)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return shards, nil
}

func walkShardBinTree(node *cell.Slice, key *cell.Builder, fn func(key *cell.Cell, value *cell.Slice) error) error {
	if node == nil {
		return fmt.Errorf("failed to load BinTree: nil node")
	}
	if node.IsSpecial() {
		return fmt.Errorf("failed to load BinTree node: unexpected special cell type %v", node.BaseCell().GetType())
	}

	typ, err := node.LoadUInt(1)
	if err != nil {
		return fmt.Errorf("failed to load BinTree type flag: %w", err)
	}

	if typ == 0 {
		return fn(key.EndCell(), node)
	}

	if node.BitsLeft() != 0 || node.RefsNum() != 2 {
		return fmt.Errorf("failed to load BinTree fork: invalid shape")
	}

	left, err := node.LoadRef()
	if err != nil {
		return fmt.Errorf("failed to load BinTree left ref: %w", err)
	}
	leftKey := key.Copy()
	if err = leftKey.StoreUInt(0, 1); err != nil {
		return fmt.Errorf("failed to build BinTree left key: %w", err)
	}
	if err = walkShardBinTree(left, leftKey, fn); err != nil {
		return err
	}

	right, err := node.LoadRef()
	if err != nil {
		return fmt.Errorf("failed to load BinTree right ref: %w", err)
	}
	rightKey := key.Copy()
	if err = rightKey.StoreUInt(1, 1); err != nil {
		return fmt.Errorf("failed to build BinTree right key: %w", err)
	}
	return walkShardBinTree(right, rightKey, fn)
}

func loadShardDesc(workchain int32, shardID int64, loader *cell.Slice) (*ton.BlockIDExt, error) {
	tag, err := loader.LoadUInt(4)
	if err != nil {
		return nil, fmt.Errorf("load ShardDesc magic err: %w", err)
	}

	switch tag {
	case 0xa:
		var shardDesc tlb.ShardDesc
		if err = tlb.LoadFromCell(&shardDesc, loader, true); err != nil {
			return nil, fmt.Errorf("load ShardDesc err: %w", err)
		}
		return &ton.BlockIDExt{
			Workchain: workchain,
			Shard:     shardID,
			SeqNo:     shardDesc.SeqNo,
			RootHash:  shardDesc.RootHash,
			FileHash:  shardDesc.FileHash,
		}, nil
	case 0xb:
		var shardDesc tlb.ShardDescB
		if err = tlb.LoadFromCell(&shardDesc, loader, true); err != nil {
			return nil, fmt.Errorf("load ShardDescB err: %w", err)
		}
		return &ton.BlockIDExt{
			Workchain: workchain,
			Shard:     shardID,
			SeqNo:     shardDesc.SeqNo,
			RootHash:  shardDesc.RootHash,
			FileHash:  shardDesc.FileHash,
		}, nil
	default:
		return nil, fmt.Errorf("wrong ShardDesc magic: %x", tag)
	}
}

func shardFromBinTreeKey(key *cell.Cell) (int64, error) {
	shard := tlb.ShardID(uint64(1) << 63)
	loader, err := key.BeginParse()
	if err != nil {
		return 0, fmt.Errorf("failed to load shard key: %w", err)
	}

	for loader.BitsLeft() > 0 {
		if uint64(shard)&1 != 0 {
			return 0, fmt.Errorf("shard key is too deep")
		}

		bit, err := loader.LoadUInt(1)
		if err != nil {
			return 0, fmt.Errorf("failed to load shard key bit: %w", err)
		}
		shard = shard.GetChild(bit == 0)
	}

	return int64(shard), nil
}
