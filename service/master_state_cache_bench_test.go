package service

import (
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkRememberCompressedBlockStateRefresh(b *testing.B) {
	root := cell.BeginCell().EndCell()
	states := make([]storage.BlockState, compressedBlockStateCacheLimit)
	for i := range states {
		states[i] = storage.BlockState{
			Block: testBlockID(0, topShard, uint32(i+1)),
			Cell:  root,
		}
	}

	service := newCompressedStateCacheTestService()
	for i := range states {
		service.RememberCompressedBlockState(&states[i])
	}

	index := 0
	b.ReportAllocs()
	for b.Loop() {
		service.RememberCompressedBlockState(&states[index])
		index++
		if index == len(states) {
			index = 0
		}
	}
}

func BenchmarkRememberMasterStateCacheEviction(b *testing.B) {
	root := cell.BeginCell().EndCell()
	states := make([]storage.BlockState, masterStateCacheLimit*2)
	for i := range states {
		states[i] = storage.BlockState{
			Block: testBlockID(-1, topShard, uint32(i+1)),
			Cell:  root,
		}
	}

	service := newMasterStateCacheTestService(b)
	for i := 0; i < masterStateCacheLimit; i++ {
		service.rememberMasterState(b.Context(), &states[i], nil, nil)
	}

	index := masterStateCacheLimit
	b.ReportAllocs()
	for b.Loop() {
		service.rememberMasterState(b.Context(), &states[index], nil, nil)
		index++
		if index == len(states) {
			index = 0
		}
	}
}
