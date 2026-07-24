package liveview

import (
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

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
