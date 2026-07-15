package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

// persistentStateCellsHintMax rejects absurd header counts so a corrupt
// previous state file can neither preallocate tens of GB nor fail the run
// inside the tonutils-go hint validation.
const persistentStateCellsHintMax = uint64(1) << 31

var bocGenericMagic = []byte{0xb5, 0xee, 0x9c, 0x72}

// previousPersistentStateCellsCountHint finds the previous epoch's serialized
// state file for the same shard and effective shard and reads its exact cell
// count from the BoC header. States drift by low percents between epochs, so
// the previous count plus small headroom presizes the large-BOC serializer
// almost exactly. Any miss (no previous description, changed shard or split
// layout, unreadable header) returns 0, which keeps today's behavior.
func (s *stateSerializer) previousPersistentStateCellsCountHint(ctx context.Context, master ton.BlockIDExt, target stateSerializationTarget, part state2.PersistentStatePart) uint64 {
	descriptions, err := s.store.PersistentStateDescriptions(ctx)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(target.block)).
			Msg("skipping persistent state cells count hint: descriptions unavailable")
		return 0
	}

	var previous *storage.PersistentStateDescription
	for i := range descriptions {
		desc := &descriptions[i]
		if desc.MasterchainBlock.SeqNo >= master.SeqNo {
			continue
		}
		if previous == nil || desc.MasterchainBlock.SeqNo > previous.MasterchainBlock.SeqNo {
			previous = desc
		}
	}
	if previous == nil {
		return 0
	}

	previousBlock := previous.MasterchainBlock
	if target.block.Workchain != -1 {
		found := false
		for _, shard := range previous.ShardBlocks {
			if shard.Block.Workchain == target.block.Workchain && shard.Block.Shard == target.block.Shard {
				previousBlock = shard.Block
				found = true
				break
			}
		}
		if !found {
			return 0
		}
	}

	file, err := s.store.PersistentStateFile(ctx, previousBlock, previous.MasterchainBlock, part.EffectiveShard)
	if err != nil {
		return 0
	}
	if file.Ref == nil || file.Ref.Size <= 0 {
		return 0
	}

	cells, err := readBOCHeaderCellsCount(file.Ref.Path)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(target.block)).
			Str("path", file.Ref.Path).
			Msg("skipping persistent state cells count hint: previous state header unreadable")
		return 0
	}
	// A consistent header cannot claim more cells than the file can physically
	// hold: every indexed cell costs well over 16 bytes on disk.
	if cells == 0 || cells > uint64(file.Ref.Size)/16 || cells >= persistentStateCellsHintMax {
		return 0
	}

	// Small headroom for inter-epoch state growth: an under-hint resumes the
	// append-growth cascade, while an over-hint only wastes 56B per extra cell.
	return cells + cells/20
}

// readBOCHeaderCellsCount reads the total cells count from a serialized BoC
// file header without loading the offsets index: the layout is magic(4),
// flags byte with ref size in the low 3 bits, offset size byte, then the
// big-endian cells count of ref size bytes.
func readBOCHeaderCellsCount(path string) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()

	var head [10]byte
	if _, err = io.ReadFull(file, head[:]); err != nil {
		return 0, fmt.Errorf("read boc header: %w", err)
	}
	if !bytes.Equal(head[:4], bocGenericMagic) {
		return 0, fmt.Errorf("unexpected boc magic %x", head[:4])
	}

	refSize := int(head[4] & 0b111)
	if refSize < 1 || refSize > 4 {
		return 0, fmt.Errorf("invalid boc ref size %d", refSize)
	}
	if offsetSize := int(head[5]); offsetSize < 1 || offsetSize > 8 {
		return 0, fmt.Errorf("invalid boc offset size %d", offsetSize)
	}

	var cells uint64
	for i := 0; i < refSize; i++ {
		cells = cells<<8 | uint64(head[6+i])
	}
	return cells, nil
}
