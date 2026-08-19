package collator

import (
	"sync/atomic"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// candidateBlockParses counts eager parses of a candidate block BOC. Each one is
// a full deserialization and hash of every cell of a block — the largest single
// item in the commit sequence before the builder's own derivation was reused —
// so the count is the direct measure of whether that reuse is actually
// happening. A produced block should contribute none.
var candidateBlockParses atomic.Int64

// builtCandidate is the derivation this node's own builder already performed
// for a block it produced. Committing that block needs a parsed block root, the
// successor state, the header's start logical time and the state's generation
// time; finish() holds all four the moment it returns, and every one of them
// used to be thrown away and re-derived from the serialized bytes.
//
// It is not a cache and it has no fallback of its own. A caller either holds a
// capsule — which only finish() can create, because the field on Candidate is
// unexported — or it replays the bytes. The distinction is the type, not a nil
// check on some optional field: a Pipeline implemented outside this package
// physically cannot produce one, so a candidate that did not come from this
// builder cannot take the fast path by accident. This mirrors
// CandidateArtifact.prepared and simplex.PreparedCandidate.
type builtCandidate struct {
	// root is the block cell tree exactly as it was serialized to BlockBOC.
	// serializeBlockBOC walked every reference to write those bytes, so nothing
	// here is unmaterialized and reusing it performs no lazy load.
	root *cell.Cell
	// startLT is BlockInfo.StartLt, the logical time the message-pool delta is
	// derived against.
	startLT uint64
	// genUTime is the successor state's GenUTime, which the masterchain view is
	// resolved as of. state.go and master_state.go both set it from the same
	// header field this records.
	genUTime uint32
	// parents are the level-0 hashes of the predecessor states the Merkle update
	// was built over, in the order the update sees them: one state, or the two
	// merge parents that openPredecessorReadSet combines under a split root.
	// bind refuses a chain that is not those parents, so the reused successor
	// can never belong to a different predecessor than the one being committed.
	parents []cell.Hash
}

// commitDerivation is what committing a candidate needs to know about the block
// beyond its own fields, however that knowledge was obtained: reused from the
// builder, or replayed from the serialized bytes.
type commitDerivation struct {
	// root is the parsed block root. It feeds PreviousBlock.Block — read by a
	// later replay's verifyPredecessor and by the next block's collated previous
	// state proof — and the message-pool delta walk.
	root *cell.Cell
	// state is the successor state root the block's update produces.
	state *cell.Cell
	// startLT is BlockInfo.StartLt.
	startLT uint64
	// genUTime is the successor state's GenUTime.
	genUTime uint32
}

func newBuiltCandidate(
	root *cell.Cell,
	startLT uint64,
	genUTime uint32,
	previous ...*PreviousBlock,
) *builtCandidate {
	parents := make([]cell.Hash, 0, len(previous))
	for _, parent := range previous {
		if parent == nil || parent.State == nil {
			return nil
		}
		parents = append(parents, parent.State.HashKeyAt(0))
	}

	return &builtCandidate{root: root, startLT: startLT, genUTime: genUTime, parents: parents}
}

// bind hands over the builder's derivation for one specific commit, or reports
// that this capsule does not describe it.
//
// Every clause is a cached-hash read: no cell is walked, parsed or allocated.
// Predecessor identity is compared by hash rather than by pointer on purpose —
// resolveChain can re-load an evicted local block cache entry and hand back an
// equal but distinct tree, which is an equally valid tree to have applied the
// update over.
func (b *builtCandidate) bind(
	id ton.BlockIDExt,
	state *cell.Cell,
	previous []PreviousBlock,
) (commitDerivation, bool) {
	if b == nil || b.root == nil || state == nil || len(previous) != len(b.parents) {
		return commitDerivation{}, false
	}
	if len(id.RootHash) != 32 {
		return commitDerivation{}, false
	}
	if b.root.HashKeyAt(0) != cell.Hash(id.RootHash) {
		return commitDerivation{}, false
	}
	for i := range previous {
		if previous[i].State == nil || previous[i].State.HashKeyAt(0) != b.parents[i] {
			return commitDerivation{}, false
		}
	}

	return commitDerivation{
		root:     b.root,
		state:    state,
		startLT:  b.startLT,
		genUTime: b.genUTime,
	}, true
}
