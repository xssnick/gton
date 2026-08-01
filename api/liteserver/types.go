package liteserver

import (
	"bytes"

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
