package liveview

import (
	"fmt"
	"math"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const workchainInvalid int32 = -1 << 31

func currentMasterchainInfo(current *storage.CurrentState) (ton.BlockIDExt, []byte, uint32, error) {
	if current == nil {
		return ton.BlockIDExt{}, nil, 0, storage.ErrNotFound
	}

	block := current.Masterchain.Block
	stateRoot := current.Masterchain.StateRootHash
	lastUTime := uint32(0)
	if current.Masterchain.Parsed != nil {
		lastUTime = current.Masterchain.Parsed.GenUTime
	}

	if len(stateRoot) != 32 {
		return ton.BlockIDExt{}, nil, 0, fmt.Errorf("masterchain state root hash is missing")
	}
	return block, stateRoot, lastUTime, nil
}

func isFullBlockID(id *ton.BlockIDExt) bool {
	if id == nil || len(id.RootHash) != 32 || len(id.FileHash) != 32 {
		return false
	}
	if id.Workchain == workchainInvalid || id.Shard == 0 || uint64(id.Shard)&7 != 0 || id.SeqNo > math.MaxInt32 {
		return false
	}
	if id.Workchain == masterchainID && id.Shard != masterchainShard {
		return false
	}
	return !isZeroHash(id.RootHash) && !isZeroHash(id.FileHash)
}

func isZeroHash(hash []byte) bool {
	for _, b := range hash {
		if b != 0 {
			return false
		}
	}
	return true
}
