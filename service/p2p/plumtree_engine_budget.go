package p2p

import (
	"errors"
	"sync"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

var (
	errPlumtreeClosed         = errors.New("Plumtree engine is closed")
	errPlumtreeMemoryCapacity = errors.New("Plumtree retained-memory capacity exceeded")
)

// plumtreeMemoryBudget bounds retained Plumtree state across every overlay
// owned by one node. Project-owned state and wire bytes are exact; RaptorQ
// storage uses a conservative working-set estimate because the library does
// not expose its private slice capacities, caches, or pooled solver arenas.
type plumtreeMemoryBudget struct {
	mu sync.Mutex

	activeStates uint64
	activeBytes  uint64
}

// plumtreeActiveStateBudgetFactor widens the upstream per-overlay stream limit
// because this budget is shared by every overlay on the node: masterchain plus
// each fast-sync shard draw from the same pool, while the upstream constant
// bounds a single overlay. States are held for the broadcast lifetime, so the
// limit doubles as a broadcast rate ceiling and must leave headroom for the
// busiest shard without starving masterchain.
const plumtreeActiveStateBudgetFactor = 4

// plumtreeMaxActiveStates and plumtreeMaxActiveBytes are the node-wide ceilings
// enforced by reserve.
const (
	plumtreeMaxActiveStates = uint64(overlay.DefaultFECBroadcastMaxActiveStreams) *
		plumtreeActiveStateBudgetFactor
	plumtreeMaxActiveBytes = uint64(overlay.DefaultFECBroadcastMaxActiveBytes)
)

func (b *plumtreeMemoryBudget) reserve(states uint64, bytes uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if states > plumtreeMaxActiveStates-b.activeStates ||
		bytes > plumtreeMaxActiveBytes-b.activeBytes {
		return false
	}

	b.activeStates += states
	b.activeBytes += bytes
	return true
}

func (b *plumtreeMemoryBudget) release(states uint64, bytes uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if states > b.activeStates || bytes > b.activeBytes {
		panic("invalid Plumtree memory budget release")
	}

	b.activeStates -= states
	b.activeBytes -= bytes
}

// plumtreeFECDecoderBufferEstimate mirrors the ordinary FEC receiver's
// conservative accounting: one K-symbol fast buffer, up to two capacities of
// every legal repair symbol, and three K-symbol decode/solver workspaces. The
// fixed allowance covers the bounded K<=30 decoder metadata. It is not a
// measurement of process heap retained by RaptorQ's package-global pools.
func plumtreeFECDecoderBufferEstimate(
	fullDataSize int32,
	partSize int32,
) uint64 {
	fullBytes := uint64(fullDataSize)
	partBytes := uint64(partSize)
	dataParts := (fullBytes + partBytes - 1) / partBytes
	repairParts := uint64(plumtreeFECTotalParts) - dataParts
	const metadataBytes = uint64(4 << 10)

	return (4*dataParts+2*repairParts)*partBytes + metadataBytes
}
