package liteserver

import (
	"bytes"

	"github.com/xssnick/gton/service/blockproof"

	"github.com/xssnick/tonutils-go/ton"
)

func cloneZeroState(id ton.ZeroStateIDExt) ton.ZeroStateIDExt {
	return ton.ZeroStateIDExt{
		Workchain: id.Workchain,
		RootHash:  bytes.Clone(id.RootHash),
		FileHash:  bytes.Clone(id.FileHash),
	}
}

func cloneZeroStatePtr(id ton.ZeroStateIDExt) *ton.ZeroStateIDExt {
	cloned := cloneZeroState(id)
	return &cloned
}

func cloneBlockID(id ton.BlockIDExt) *ton.BlockIDExt {
	return blockproof.CloneBlockID(id)
}

func blockIDEqual(a ton.BlockIDExt, b ton.BlockIDExt) bool {
	return blockproof.BlockIDEqual(a, b)
}
