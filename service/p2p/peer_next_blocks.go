package p2p

import (
	"context"
	"errors"
	"fmt"

	"github.com/pierrec/lz4/v4"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	nextBlocksFullMaxCount          = uint32(10)
	nextBlocksFullMaxAggregateBytes = 1 << 20
)

var errNextBlocksFullSizeLimit = errors.New("next blocks full size limit reached")

func init() {
	tl.Register(DownloadNextBlocksFull{}, "tonNode.downloadNextBlocksFull prev_block:tonNode.blockIdExt max_blocks:int = tonNode.NextBlocksFull")
	tl.Register(NextBlocksFull{}, "tonNode.nextBlocksFull blocks:(vector tonNode.DataFull) = tonNode.NextBlocksFull")
}

type DownloadNextBlocksFull struct {
	PrevBlock ton.BlockIDExt `tl:"struct"`
	MaxBlocks int32          `tl:"int"`
}

type NextBlocksFull struct {
	Blocks []any `tl:"vector struct boxed [tonNode.dataFull,tonNode.dataFullCompressed,tonNode.dataFullCompressedV2,tonNode.dataFullEmpty]"`
}

type nextBlocksFullBuilder struct {
	blocks    []any
	totalSize int
}

func newNextBlocksFullBuilder(maxBlocks uint32) *nextBlocksFullBuilder {
	return &nextBlocksFullBuilder{
		blocks: make([]any, 0, maxBlocks),
	}
}

func (b *nextBlocksFullBuilder) add(block DataFullCompressed) error {
	size := dataFullCompressedWireSize(block)
	if len(b.blocks) > 0 &&
		(b.totalSize > nextBlocksFullMaxAggregateBytes ||
			size > nextBlocksFullMaxAggregateBytes-b.totalSize) {
		return errNextBlocksFullSizeLimit
	}

	b.totalSize += size
	b.blocks = append(b.blocks, block)
	return nil
}

func (b *nextBlocksFullBuilder) sizeLimitReached() bool {
	return b.totalSize > nextBlocksFullMaxAggregateBytes
}

func dataFullCompressedWireSize(block DataFullCompressed) int {
	const fixedSize = 4 + 80 + 4 + 4 // constructor + blockIdExt + flags + Bool

	size := 1 + len(block.Compressed)
	if len(block.Compressed) >= 0xfe {
		size += 3
	}
	return fixedSize + (size+3)&^3
}

func (s *overlaySubscription) serveNextBlocksFull(
	ctx context.Context,
	prev ton.BlockIDExt,
	maxBlocks int32,
) (tl.Serializable, error) {
	limit := uint32(maxBlocks)
	if limit > nextBlocksFullMaxCount {
		limit = nextBlocksFullMaxCount
	}

	builder := newNextBlocksFullBuilder(limit)
	if limit == 0 || !isMasterchainBlock(prev) {
		return NextBlocksFull{Blocks: builder.blocks}, nil
	}

	current := prev
	for uint32(len(builder.blocks)) < limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		full, err := s.node.localNextServedBlockFull(ctx, current)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			break
		}
		if full.IsLink || !isMasterchainBlock(full.ID) {
			break
		}

		block, err := compressNextBlockFull(full)
		if err != nil {
			break
		}
		if err = builder.add(block); err != nil {
			break
		}

		current = full.ID
		if builder.sizeLimitReached() {
			break
		}
	}

	return NextBlocksFull{Blocks: builder.blocks}, nil
}

func compressNextBlockFull(full *tnstore.ServedBlockFull) (DataFullCompressed, error) {
	proofRoot, err := cell.FromBOC(full.Proof)
	if err != nil {
		return DataFullCompressed{}, fmt.Errorf("parse block proof: %w", err)
	}
	blockRoot, err := cell.FromBOC(full.Block)
	if err != nil {
		return DataFullCompressed{}, fmt.Errorf("parse block data: %w", err)
	}

	data, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{proofRoot, blockRoot},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		return DataFullCompressed{}, fmt.Errorf("serialize block proof and data: %w", err)
	}

	compressed := make([]byte, lz4.CompressBlockBound(len(data)))
	size, err := lz4.CompressBlock(data, compressed, nil)
	if err != nil {
		return DataFullCompressed{}, fmt.Errorf("compress block proof and data: %w", err)
	}
	if size == 0 {
		return DataFullCompressed{}, errors.New("compress block proof and data: empty result")
	}

	return DataFullCompressed{
		ID:         full.ID,
		Flags:      0,
		Compressed: compressed[:size],
		IsLink:     false,
	}, nil
}
