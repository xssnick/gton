package p2p

import (
	"crypto/sha256"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func init() {
	// Keep the released Go type serializable without replacing tonutils-go's
	// canonical overlay.broadcast.id registration. The explicit constructor ID
	// is the CRC of the canonical schema, so boxed bytes remain identical.
	tl.Register(OverlayBroadcastID{}, "gton.overlayBroadcastID#51fd789a src:int256 data_hash:int256 flags:int = overlay.broadcast.Id")
	tl.Register(BlockDescriptionEmpty{}, "tonNode.blockDescriptionEmpty = tonNode.BlockDescription")
	tl.Register(BlockDescription{}, "tonNode.blockDescription id:tonNode.blockIdExt = tonNode.BlockDescription")
	tl.Register(GetNextBlockDescription{}, "tonNode.getNextBlockDescription prev_block:tonNode.blockIdExt = tonNode.BlockDescription")
	tl.Register(DownloadNextBlockFull{}, "tonNode.downloadNextBlockFull prev_block:tonNode.blockIdExt = tonNode.DataFull")
	tl.Register(KeyBlocks{}, "tonNode.keyBlocks blocks:(vector tonNode.blockIdExt) incomplete:Bool error:Bool = tonNode.KeyBlocks")
	tl.Register(GetNextKeyBlockIDs{}, "tonNode.getNextKeyBlockIds block:tonNode.blockIdExt max_size:int = tonNode.KeyBlocks")
	tl.Register(DataFullCompressed{}, "tonNode.dataFullCompressed id:tonNode.blockIdExt flags:# compressed:bytes is_link:Bool = tonNode.DataFull")
	tl.Register(DataFullCompressedV2{}, "tonNode.dataFullCompressedV2 id:tonNode.blockIdExt flags:# proof:bytes block_compressed:bytes is_link:Bool = tonNode.DataFull")
	tl.Register(Success{}, "tonNode.success = tonNode.Success")
	tl.Register(ImportedMsgQueueLimits{}, "tonNode.importedMsgQueueLimits max_bytes:int max_msgs:int = ImportedMsgQueueLimits")
	tl.Register(OutMsgQueueProof{}, "tonNode.outMsgQueueProof queue_proofs:bytes block_state_proofs:bytes msg_counts:(vector int) = tonNode.OutMsgQueueProof")
	tl.Register(OutMsgQueueProofEmpty{}, "tonNode.outMsgQueueProofEmpty = tonNode.OutMsgQueueProof")
	tl.Register(GetOutMsgQueueProof{}, "tonNode.getOutMsgQueueProof dst_shard:tonNode.shardId blocks:(vector tonNode.blockIdExt) limits:tonNode.importedMsgQueueLimits = tonNode.OutMsgQueueProof")
	tl.Register(OutMsgQueueProofBroadcast{}, "tonNode.outMsgQueueProofBroadcast dst_shard:tonNode.shardId block:tonNode.blockIdExt limits:ImportedMsgQueueLimits proof:tonNode.OutMsgQueueProof = tonNode.Broadcast")
	tl.Register(SendExtMessage{}, "tonNode.slave.sendExtMessage message:tonNode.externalMessage = tonNode.Success")
	tl.Register(BlockFinalityBroadcast{}, "tonNode.blockFinalityBroadcast id:tonNode.blockIdExt signature_set:tonNode.SignatureSet = tonNode.Broadcast")
	tl.Register(FinalityBroadcastID{}, "tonNode.finalityBroadcastId id:tonNode.blockIdExt = tonNode.FinalityBroadcastId")
}

type OverlayBroadcastID struct {
	Source   []byte `tl:"int256"`
	DataHash []byte `tl:"int256"`
	Flags    int32  `tl:"int"`
}

type BlockDescriptionEmpty struct{}

type BlockDescription struct {
	ID ton.BlockIDExt `tl:"struct"`
}

type GetNextBlockDescription struct {
	PrevBlock ton.BlockIDExt `tl:"struct"`
}

type DownloadNextBlockFull struct {
	PrevBlock ton.BlockIDExt `tl:"struct"`
}

type KeyBlocks struct {
	Blocks     []ton.BlockIDExt `tl:"vector struct"`
	Incomplete bool             `tl:"bool"`
	Error      bool             `tl:"bool"`
}

type GetNextKeyBlockIDs struct {
	Block   ton.BlockIDExt `tl:"struct"`
	MaxSize int32          `tl:"int"`
}

type DataFullCompressed struct {
	ID         ton.BlockIDExt `tl:"struct"`
	Flags      uint32         `tl:"flags"`
	Compressed []byte         `tl:"bytes"`
	IsLink     bool           `tl:"bool"`
}

type DataFullCompressedV2 struct {
	ID              ton.BlockIDExt `tl:"struct"`
	Flags           uint32         `tl:"flags"`
	Proof           []byte         `tl:"bytes"`
	BlockCompressed []byte         `tl:"bytes"`
	IsLink          bool           `tl:"bool"`
}

type Success struct{}

type ImportedMsgQueueLimits struct {
	MaxBytes int32 `tl:"int"`
	MaxMsgs  int32 `tl:"int"`
}

type OutMsgQueueProof struct {
	QueueProofs      []byte  `tl:"bytes"`
	BlockStateProofs []byte  `tl:"bytes"`
	MessageCounts    []int32 `tl:"vector int"`
}

type OutMsgQueueProofEmpty struct{}

type GetOutMsgQueueProof struct {
	DstShard tonnodeapi.ShardID     `tl:"struct"`
	Blocks   []ton.BlockIDExt       `tl:"vector struct"`
	Limits   ImportedMsgQueueLimits `tl:"struct"`
}

type OutMsgQueueProofBroadcast struct {
	DstShard tonnodeapi.ShardID     `tl:"struct"`
	Block    ton.BlockIDExt         `tl:"struct"`
	Limits   ImportedMsgQueueLimits `tl:"struct"`
	Proof    any                    `tl:"struct boxed [tonNode.outMsgQueueProof,tonNode.outMsgQueueProofEmpty]"`
}

type SendExtMessage struct {
	Message tonnodeapi.ExternalMessage `tl:"struct"`
}

type BlockFinalityBroadcast struct {
	ID           ton.BlockIDExt `tl:"struct"`
	SignatureSet any            `tl:"struct boxed [tonNode.signatureSet.ordinary,tonNode.signatureSet.simplex]"`
}

type FinalityBroadcastID struct {
	ID ton.BlockIDExt `tl:"struct"`
}

func hashSimpleBroadcastPayload(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}
