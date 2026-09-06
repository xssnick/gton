package collator

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/xssnick/gton/service/validator/msgpool"
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
// Validating it needs the parsed block root again, and the collated roots
// beside it. Consensus asks every validator including the leader to validate
// this candidate, so the same two BOCs this build wrote were parsed straight
// back a moment later; the roots travel to that call on the emitted artifact.
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
	// collated are the roots CollatedData was serialized from, in wire order.
	// The same materialization argument as root applies: their BOC was written
	// by walking every reference.
	//
	// Both are trace-inert. finish() seals the collation's read set before it
	// returns either of them, and a sealed set stops propagating down the cells
	// it handed out, so the reads this node's own validation performs over these
	// trees record nothing and allocate no wrapper — which is what makes reusing
	// them cheaper than the parse they replace rather than merely different.
	collated []*cell.Cell
	// startLT is BlockInfo.StartLt, the logical time the message-pool delta is
	// derived against.
	startLT uint64
	// genUTime is the successor state's GenUTime, which the masterchain view is
	// resolved as of. state.go and master_state.go both set it from the same
	// header field this records.
	genUTime uint32
	// genUTimeMS is the exact consensus-extra timestamp. The block header keeps
	// only seconds, so it cannot be reconstructed from genUTime without losing
	// the millisecond component state resolution uses for slot pacing.
	genUTimeMS uint64
	// parents are the level-0 hashes of the predecessor states the Merkle update
	// was built over, in the order the update sees them: one state, or the two
	// merge parents that openPredecessorReadSet combines under a split root.
	// bind refuses a chain that is not those parents, so the reused successor
	// can never belong to a different predecessor than the one being committed.
	parents []cell.Hash
	// live binds the built successor to the exact resident predecessor trees.
	live LiveSuccessorState
	// state and stateUpdate bind the successor handed to CommitCandidate to the
	// transition finish() actually built. Both are cached level-0 hashes, so the
	// check is O(1) and does not retain another tree.
	state       cell.Hash
	stateUpdate cell.Hash
	workchain   int32
	shard       int64
	seqno       uint32
	fileHash    [32]byte
	// metadata is installed only when the complete Builder entry point seals
	// the candidate. finish() deliberately leaves the capsule provisional so
	// ready-external wrappers can finish the candidate statistics first.
	metadata *candidateProvenance
}

// candidateProvenance is an immutable seal over the exported Candidate fields
// whose ownership crosses the public Pipeline boundary. It is deliberately
// separate from builtCandidate: a build whose predecessor shape cannot use the
// commit fast path may still reuse its prepared broadcast payload safely.
type candidateProvenance struct {
	workchain        int32
	shard            int64
	seqno            uint32
	rootHash         cell.Hash
	fileHash         [32]byte
	collatedFileHash [32]byte
	stateHash        cell.Hash
	stateUpdateHash  cell.Hash
	stats            Stats
	storageStats     AccountStorageStats
	externals        []msgpool.ExternalFeedback
}

// sealBuiltCandidate closes the canonical Builder ownership boundary after
// every wrapper has finished decorating the candidate. It is deliberately
// one-shot: resealing after the Candidate crossed that boundary would bless a
// mutation the provenance exists to reject.
func sealBuiltCandidate(candidate *Candidate) error {
	if candidate == nil || candidate.built == nil || candidate.built.root == nil {
		return fmt.Errorf("%w: candidate derivation is absent at seal", ErrInvalidInput)
	}
	if candidate.provenance != nil || candidate.built.metadata != nil {
		return fmt.Errorf("%w: candidate is already sealed", ErrInvalidInput)
	}

	provenance := newCandidateProvenance(candidate, candidate.built.root)
	if provenance == nil {
		return fmt.Errorf("%w: candidate is incomplete at seal", ErrInvalidInput)
	}
	candidate.provenance = provenance
	candidate.built.metadata = provenance

	return nil
}

func newCandidateProvenance(
	candidate *Candidate,
	root *cell.Cell,
) *candidateProvenance {
	if candidate == nil || root == nil || candidate.State == nil || candidate.StateUpdate == nil ||
		len(candidate.ID.RootHash) != 32 || len(candidate.ID.FileHash) != 32 {
		return nil
	}
	storageStats := make(AccountStorageStats, len(candidate.StorageStats))
	for hash, stat := range candidate.StorageStats {
		storageStats[hash] = stat
	}

	p := &candidateProvenance{
		workchain:        candidate.ID.Workchain,
		shard:            candidate.ID.Shard,
		seqno:            candidate.ID.SeqNo,
		rootHash:         root.HashKeyAt(0),
		collatedFileHash: candidate.CollatedFileHash,
		stateHash:        candidate.State.HashKeyAt(0),
		stateUpdateHash:  candidate.StateUpdate.HashKeyAt(0),
		stats:            candidate.Stats,
		storageStats:     storageStats,
		externals:        append([]msgpool.ExternalFeedback(nil), candidate.Externals...),
	}
	copy(p.fileHash[:], candidate.ID.FileHash)

	return p
}

func (p *candidateProvenance) binds(candidate *Candidate, fileHash, collatedFileHash [32]byte) bool {
	if p == nil || candidate == nil || candidate.State == nil || candidate.StateUpdate == nil ||
		len(candidate.ID.RootHash) != len(p.rootHash) || len(candidate.ID.FileHash) != len(p.fileHash) {
		return false
	}

	return candidate.ID.Workchain == p.workchain &&
		candidate.ID.Shard == p.shard &&
		candidate.ID.SeqNo == p.seqno &&
		bytes.Equal(candidate.ID.RootHash, p.rootHash[:]) &&
		bytes.Equal(candidate.ID.FileHash, p.fileHash[:]) &&
		fileHash == p.fileHash &&
		collatedFileHash == p.collatedFileHash &&
		candidate.CollatedFileHash == p.collatedFileHash &&
		candidate.State.HashKeyAt(0) == p.stateHash &&
		candidate.StateUpdate.HashKeyAt(0) == p.stateUpdateHash &&
		candidate.Stats == p.stats &&
		equalCandidateStorageStats(candidate.StorageStats, p.storageStats) &&
		equalCandidateExternals(candidate.Externals, p.externals)
}

func equalCandidateStorageStats(left, right AccountStorageStats) bool {
	if len(left) != len(right) {
		return false
	}
	for hash, stat := range left {
		other, exists := right[hash]
		if !exists || other != stat {
			return false
		}
	}

	return true
}

func equalCandidateExternals(left, right []msgpool.ExternalFeedback) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
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
	// The remaining values are local commit metadata. Builder-owned derivations
	// carry the sealed snapshots rather than rereading the mutable public
	// Candidate after it crossed the Pipeline boundary.
	outQueueSize uint64
	storageStats AccountStorageStats
	externals    []msgpool.ExternalFeedback
}

func newBuiltCandidate(
	id ton.BlockIDExt,
	root *cell.Cell,
	collated []*cell.Cell,
	state *cell.Cell,
	stateUpdate *cell.Cell,
	startLT uint64,
	genUTime uint32,
	genUTimeMS uint64,
	previous ...*PreviousBlock,
) *builtCandidate {
	if root == nil || state == nil || stateUpdate == nil || len(id.FileHash) != 32 {
		return nil
	}

	parents := make([]cell.Hash, 0, len(previous))
	liveParents := make([]*cell.Cell, 0, len(previous))
	for _, parent := range previous {
		if parent == nil || parent.State == nil {
			return nil
		}
		parents = append(parents, parent.State.HashKeyAt(0))
		liveParents = append(liveParents, parent.State)
	}

	source, err := stateUpdate.PeekRef(0)
	if err != nil {
		return nil
	}

	built := &builtCandidate{
		live:        LiveSuccessorState{root: state, parents: liveParents, source: source.HashKeyAt(0)},
		root:        root,
		collated:    collated,
		startLT:     startLT,
		genUTime:    genUTime,
		genUTimeMS:  genUTimeMS,
		parents:     parents,
		state:       state.HashKeyAt(0),
		stateUpdate: stateUpdate.HashKeyAt(0),
		workchain:   id.Workchain,
		shard:       id.Shard,
		seqno:       id.SeqNo,
	}
	copy(built.fileHash[:], id.FileHash)

	return built
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
	stateUpdate *cell.Cell,
	previous []PreviousBlock,
) (commitDerivation, bool) {
	if b == nil || b.root == nil || b.metadata == nil || state == nil || stateUpdate == nil ||
		len(previous) != len(b.parents) {
		return commitDerivation{}, false
	}
	if len(id.RootHash) != 32 {
		return commitDerivation{}, false
	}
	if id.Workchain != b.workchain || id.Shard != b.shard || id.SeqNo != b.seqno ||
		len(id.FileHash) != len(b.fileHash) || !bytes.Equal(id.FileHash, b.fileHash[:]) {
		return commitDerivation{}, false
	}
	if b.root.HashKeyAt(0) != cell.Hash(id.RootHash) {
		return commitDerivation{}, false
	}
	if state.HashKeyAt(0) != b.state || stateUpdate.HashKeyAt(0) != b.stateUpdate {
		return commitDerivation{}, false
	}
	for i := range previous {
		if previous[i].State == nil || previous[i].State.HashKeyAt(0) != b.parents[i] {
			return commitDerivation{}, false
		}
	}

	return commitDerivation{
		root:         b.root,
		state:        state,
		startLT:      b.startLT,
		genUTime:     b.genUTime,
		outQueueSize: b.metadata.stats.OutQueueSize,
		storageStats: b.metadata.storageStats,
		externals:    b.metadata.externals,
	}, true
}
