package liteserver

import (
	"bytes"

	"github.com/xssnick/tonutils-go/ton"
)

func cloneBlockID(id ton.BlockIDExt) *ton.BlockIDExt {
	cloned := id
	return &cloned
}

func cloneZeroState(id ton.ZeroStateIDExt) ton.ZeroStateIDExt {
	return id
}

func cloneZeroStatePtr(id ton.ZeroStateIDExt) *ton.ZeroStateIDExt {
	cloned := cloneZeroState(id)
	return &cloned
}

func blockIDEqual(a ton.BlockIDExt, b ton.BlockIDExt) bool {
	return a.Workchain == b.Workchain &&
		a.Shard == b.Shard &&
		a.SeqNo == b.SeqNo &&
		bytes.Equal(a.RootHash, b.RootHash) &&
		bytes.Equal(a.FileHash, b.FileHash)
}
