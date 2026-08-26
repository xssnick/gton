package collator

import (
	"fmt"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func parseSemanticQueueEntry(
	keyCell *cell.Cell,
	value *cell.Slice,
	extra *cell.Slice,
) (semanticQueueEntry, error) {
	return parseSemanticQueueEntryWithMode(keyCell, value, extra, false, semanticQueueLeafCells{})
}

func parseSemanticNeighborQueueEntry(
	keyCell *cell.Cell,
	value *cell.Slice,
	extra *cell.Slice,
) (semanticQueueEntry, error) {
	return parseSemanticQueueEntryWithMode(keyCell, value, extra, true, semanticQueueLeafCells{})
}

func parseSemanticQueueEntryWithMode(
	keyCell *cell.Cell,
	value *cell.Slice,
	extra *cell.Slice,
	neighborProof bool,
	leaf semanticQueueLeafCells,
) (semanticQueueEntry, error) {
	var keyLoader cell.Slice
	if err := keyCell.BeginParseInto(&keyLoader); err != nil {
		return semanticQueueEntry{}, fmt.Errorf("%w: outbound queue key is malformed", ErrInvalidInput)
	}
	var key msgpool.QueueKey
	err := keyLoader.LoadSliceInto(key[:], 352)
	if err != nil || keyLoader.BitsLeft() != 0 || keyLoader.RefsNum() != 0 {
		return semanticQueueEntry{}, fmt.Errorf("%w: outbound queue key is malformed", ErrInvalidInput)
	}

	return parseSemanticQueueEntryKeyWithMode(key, value, extra, neighborProof, leaf, nil)
}

func semanticQueueLeafBound(key *cell.Cell, lt uint64) (semanticMessageBound, error) {
	if key.BitsSize() != 352 {
		return semanticMessageBound{}, fmt.Errorf("outbound queue leaf key has %d bits", key.BitsSize())
	}
	loader, err := key.BeginParse()
	if err != nil {
		return semanticMessageBound{}, err
	}
	if err = loader.SkipBits(96); err != nil {
		return semanticMessageBound{}, err
	}
	var hash [32]byte
	if err = loader.LoadSliceInto(hash[:], 256); err != nil {
		return semanticMessageBound{}, err
	}

	return semanticMessageBound{lt: lt, hash: hash}, nil
}

// semanticQueuePrefixDirection is the allocation-owning reference used to
// check semanticQueuePrefixDirectionBorrowed in proof-walk tests.
func semanticQueuePrefixDirection(current, target *cell.Cell) (int, error) {
	common := min(current.BitsSize(), target.BitsSize())
	currentLoader := current.MustBeginParse()
	targetLoader := target.MustBeginParse()
	for common > 0 {
		chunk := min(common, uint(64))
		currentBits, err := currentLoader.LoadUInt(chunk)
		if err != nil {
			return 0, err
		}
		targetBits, err := targetLoader.LoadUInt(chunk)
		if err != nil {
			return 0, err
		}
		if currentBits != targetBits {
			return 0, nil
		}
		common -= chunk
	}
	if current.BitsSize() >= target.BitsSize() {
		return 6, nil
	}
	next, err := targetLoader.LoadUInt(1)
	if err != nil {
		return 0, err
	}

	return int(next) + 1, nil
}
