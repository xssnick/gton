package p2p

import (
	"crypto/sha256"
	"flexserver/service/archive"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func init() {
	tl.Register(OverlayBroadcastID{}, "overlay.broadcast.id src:int256 data_hash:int256 flags:int = overlay.broadcast.Id")
	tl.Register(SignatureSetOrdinary{}, "tonNode.signatureSet.ordinary cc_seqno:int validator_set_hash:int signatures:(vector tonNode.blockSignature) = tonNode.SignatureSet")
	tl.Register(SignatureSetSimplex{}, "tonNode.signatureSet.simplex final:Bool cc_seqno:int validator_set_hash:int signatures:(vector tonNode.blockSignature) session_id:int256 slot:int candidate:consensus.CandidateHashData = tonNode.SignatureSet")
	tl.Register(BlockBroadcastCompressed{}, "tonNode.blockBroadcastCompressed id:tonNode.blockIdExt catchain_seqno:int validator_set_hash:int flags:# compressed:bytes = tonNode.Broadcast")
	tl.Register(BlockBroadcastCompressedData{}, "tonNode.blockBroadcastCompressed.data signatures:(vector tonNode.blockSignature) proof_data:bytes = tonNode.blockBroadcaseCompressed.Data")
	tl.Register(BlockBroadcastCompressedV2{}, "tonNode.blockBroadcastCompressedV2 id:tonNode.blockIdExt signature_set:tonNode.SignatureSet flags:# proof:bytes data_compressed:bytes = tonNode.Broadcast")
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
	tl.Register(SendExtMessage{}, "tonNode.slave.sendExtMessage message:tonNode.externalMessage = tonNode.Success")
}

type OverlayBroadcastID struct {
	Source   []byte `tl:"int256"`
	DataHash []byte `tl:"int256"`
	Flags    int32  `tl:"int"`
}

type SignatureSetOrdinary struct {
	CatchainSeqno    int32                       `tl:"int"`
	ValidatorSetHash int32                       `tl:"int"`
	Signatures       []tonnodeapi.BlockSignature `tl:"vector struct"`
}

type SignatureSetSimplex struct {
	Final            bool                        `tl:"bool"`
	CatchainSeqno    int32                       `tl:"int"`
	ValidatorSetHash int32                       `tl:"int"`
	Signatures       []tonnodeapi.BlockSignature `tl:"vector struct"`
	SessionID        []byte                      `tl:"int256"`
	Slot             int32                       `tl:"int"`
	Candidate        any                         `tl:"struct boxed [consensus.candidateHashDataOrdinary,consensus.candidateHashDataEmpty]"`
}

type BlockBroadcastCompressed struct {
	ID               ton.BlockIDExt `tl:"struct"`
	CatchainSeqno    int32          `tl:"int"`
	ValidatorSetHash int32          `tl:"int"`
	Flags            uint32         `tl:"flags"`
	Compressed       []byte         `tl:"bytes"`
}

type BlockBroadcastCompressedData struct {
	Signatures []tonnodeapi.BlockSignature `tl:"vector struct"`
	ProofData  []byte                      `tl:"bytes"`
}

type BlockBroadcastCompressedV2 struct {
	ID             ton.BlockIDExt `tl:"struct"`
	SignatureSet   any            `tl:"struct boxed [tonNode.signatureSet.ordinary,tonNode.signatureSet.simplex]"` // SignatureSetOrdinary or SignatureSetSimplex after TL decode.
	Flags          uint32         `tl:"flags"`
	Proof          []byte         `tl:"bytes"`
	DataCompressed []byte         `tl:"bytes"`
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
	DstShard archive.ShardID        `tl:"struct"`
	Blocks   []ton.BlockIDExt       `tl:"vector struct"`
	Limits   ImportedMsgQueueLimits `tl:"struct"`
}

type SendExtMessage struct {
	Message tonnodeapi.ExternalMessage `tl:"struct"`
}

func checkSimpleBroadcastDate(ts int32) bool {
	now := time.Now()
	at := time.Unix(int64(ts), 0)
	if at.Before(now.Add(-simpleBroadcastSkew)) {
		return false
	}
	if at.After(now.Add(simpleBroadcastSkew)) {
		return false
	}
	return true
}

func hashSimpleBroadcastPayload(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}
