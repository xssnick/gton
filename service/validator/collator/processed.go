package collator

import (
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/tlb"
)

const (
	processedInfoKeyBits = 96
)

// shardEndLTResolver adapts the request's registered-end-lt lookup to the tlb
// coverage callback, which has no way to report failure. An unresolvable shard
// has no safe answer in either direction: treating the message as undelivered
// leaves queue cleanup incomplete, while treating it as delivered re-imports a
// message a previous block already processed, which validation rejects. A
// shard holding a queued message has produced at least one block, so its end lt
// is never zero — the zero an unknown shard yields is latched here and turned
// into a collation error instead of being taken at face value.
//
// The latch is per collation and its calls are sequential, like the rest of the
// replay.
type shardEndLTResolver struct {
	resolve   tlb.ShardEndLTFunc
	missed    bool
	seqno     uint32
	workchain int32
	prefix    uint64
}

func newShardEndLTResolver(resolve tlb.ShardEndLTFunc) *shardEndLTResolver {
	return &shardEndLTResolver{resolve: resolve}
}

func (r *shardEndLTResolver) lookup(mcSeqno uint32, workchain int32, prefix uint64) uint64 {
	endLT := r.resolve(mcSeqno, workchain, prefix)
	if endLT == 0 && !r.missed {
		r.missed = true
		r.seqno, r.workchain, r.prefix = mcSeqno, workchain, prefix
	}

	return endLT
}

// alreadyProcessed reports whether a processed frontier covers one queued
// message, failing closed when the registered end lt of some shard could not be
// resolved.
func (r *shardEndLTResolver) alreadyProcessed(
	records []tlb.ProcessedUptoRecord,
	workchain int32,
	shard uint64,
	descr *tlb.ProcessedMsgDescr,
) (bool, error) {
	if r.resolve == nil {
		return tlb.AlreadyProcessed(records, workchain, shard, descr, nil)
	}

	r.missed = false
	processed, err := tlb.AlreadyProcessed(records, workchain, shard, descr, r.lookup)
	if err != nil {
		return false, err
	}
	if r.missed {
		return false, fmt.Errorf("%w: masterchain block %d registers no shard holding %d:%016x",
			ErrInvalidInput, r.seqno, r.workchain, r.prefix)
	}

	return processed, nil
}

// minRecordedMCSeqno is the oldest masterchain seqno any processed-upto record
// refers to. An empty collection contributes no bound, so it yields the maximum.
func minRecordedMCSeqno(records []tlb.ProcessedUptoRecord) uint32 {
	minimum := uint32(math.MaxUint32)
	for i := range records {
		minimum = min(minimum, records[i].MCSeqno)
	}

	return minimum
}
