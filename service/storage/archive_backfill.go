package storage

import "github.com/xssnick/tonutils-go/ton"

type ArchiveBackfillProgress struct {
	OriginPersistentState ton.BlockIDExt
	OriginGenUTime        uint32
	GCCutoffUnix          uint32
	TargetUnix            uint32
	VerifiedFloorSeqno    uint32
	VerifiedFloorUTime    uint32
	UpdatedAtUnix         uint64
}
