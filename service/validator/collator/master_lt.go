package collator

import "fmt"

const (
	masterMaxLTGrowth = 10*logicalTimeAlignment - 1
	masterMaxLTGap    = 4 * logicalTimeAlignment
)

func nextMasterBlockStartLT(previousLT, shardsMaxEndLT uint64) (uint64, error) {
	if shardsMaxEndLT > previousLT && shardsMaxEndLT-previousLT > masterMaxLTGrowth {
		return 0, fmt.Errorf("%w: accepted shard end lt grows by %d, limit is %d",
			ErrInvalidInput, shardsMaxEndLT-previousLT, masterMaxLTGrowth)
	}
	startLT, err := nextBlockStartLT(previousLT, shardsMaxEndLT)
	if err != nil {
		return 0, err
	}
	if startLT-previousLT > masterMaxLTGrowth {
		return 0, fmt.Errorf("%w: masterchain start lt grows by %d, limit is %d",
			ErrInvalidInput, startLT-previousLT, masterMaxLTGrowth)
	}
	base := max(previousLT, shardsMaxEndLT)
	if startLT-base > masterMaxLTGap {
		return 0, fmt.Errorf("%w: masterchain start lt is too far ahead of its inputs", ErrInvalidInput)
	}
	return startLT, nil
}
