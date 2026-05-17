package storage

import "github.com/xssnick/tonutils-go/ton"

type PersistentStateSerializerState struct {
	LastBlock             ton.BlockIDExt
	LastWrittenBlock      ton.BlockIDExt
	LastWrittenBlockUTime uint32
}

type PersistentStateSerializerActive struct {
	Block         ton.BlockIDExt
	StartedAtUnix uint64
}

type PersistentStateDescription struct {
	MasterchainBlock ton.BlockIDExt
	ShardBlocks      []PersistentStateDescriptionShard
	StartTime        uint32
	EndTime          uint64
}

type PersistentStateDescriptionShard struct {
	Block      ton.BlockIDExt
	SplitDepth uint32
}
