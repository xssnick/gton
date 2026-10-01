package validator

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

// lazyCandidateWire is the canonical wire of a candidate this node received,
// built on first use rather than on receipt.
//
// Building it is a combined BOC serialization of the block and the collated
// data plus an LZ4 pass over the result, and a received candidate used to pay
// that on the decode path, every time, ahead of anything that needed the bytes.
// Measured on the testnet validator it was 3.27 s of CPU per minute — more
// than the node spent collating — and nothing on the receive path reads the
// result: the candidate is stored only when the engine says so
// (sessionRuntime.StoreCandidate), served only when a peer asks, and the
// resolver has no earlier exact wire to compare on a first admission. So the
// bytes are owed to readers that may never come, and this type builds them for
// the first one that does and keeps them for the rest. A prior exact identity
// is the exception and is handled explicitly below.
//
// The identity problem this has to get right. The candidate store is
// content-addressed by wireHash, so the hash handed to it must be the sha256 of
// the canonical bytes and nothing else — a digest of the compressed broadcast,
// or of the decompressed BOC, would make every restart read back its own durable
// index as a conflict. Deferring the bytes therefore defers the hash with them.
// A newly admitted lazy entry has neither until it materializes. An entry that
// already remembers a wireHash cannot accept a lazy replacement: the candidate
// id commits to the payload but not to candidate signature and delegation
// bytes, so the replacement is materialized outside the resolver lock and its
// exact hash is compared first.
//
// A candidate this node produced is never lazy. Its prepared capsule already
// holds the bytes and the digest, and it is stored and broadcast at once.
type lazyCandidateWire struct {
	once sync.Once
	// The roots avoid another parse for an active candidate. The resolver may
	// release them when the candidate goes cold; materialization then rebuilds
	// them from the immutable, already verified BOCs shared with the artifact.
	// An atomic handoff lets cache eviction run without waiting for compression.
	roots        atomic.Pointer[candidateWireRoots]
	candidate    simplex.Candidate
	blockBOC     []byte
	collatedData []byte

	wire []byte
	hash [32]byte
	err  error
}

type candidateWireRoots struct {
	block    *cell.Cell
	collated []*cell.Cell
}

// setRoots initializes the optional parsed cache before the wire is published.
// The roots and their slice must remain immutable after this call.
func (w *lazyCandidateWire) setRoots(block *cell.Cell, collated []*cell.Cell) {
	w.roots.Store(&candidateWireRoots{block: block, collated: collated})
}

// releaseRoots drops only the cache's reference. An in-progress materialization
// owns its roots independently and continues without reparsing or blocking.
func (w *lazyCandidateWire) releaseRoots() {
	w.roots.Store(nil)
}

// materialize builds the canonical wire on the calling goroutine, once. It is
// never called under the resolver lock: it runs the same serialization and
// compression the decode path used to, which is milliseconds of CPU, and the
// consensus goroutine takes that lock.
func (w *lazyCandidateWire) materialize() ([]byte, [32]byte, error) {
	w.once.Do(func() {
		roots := w.roots.Swap(nil)
		if roots == nil {
			// Only the verified decoder creates an unmaterialized lazy wire.
			// Its BOCs are canonical and its digests were derived from those
			// immutable bytes, so do not reserialize each half and rehash it as
			// the external SerializeCandidate boundary must. Parsing is still
			// checked, and PrepareCandidate binds the block root to the identity.
			block, err := cell.FromBOCWithOptions(w.blockBOC, cell.BOCParseOptions{NoCopyPayload: true})
			if err != nil {
				w.err = fmt.Errorf("validator runtime: restore candidate block roots: %w", err)
				return
			}
			collated, err := cell.FromBOCMultiRootWithOptions(w.collatedData, cell.BOCParseOptions{NoCopyPayload: true})
			if err != nil {
				w.err = fmt.Errorf("validator runtime: restore candidate collated roots: %w", err)
				return
			}
			roots = &candidateWireRoots{block: block, collated: collated}
		}

		prepared, err := simplex.PrepareCandidate(
			w.candidate.Block.SeqNo,
			roots.block,
			roots.collated,
			w.fileHash(),
			w.candidate.CollatedFileHash,
			simplex.PayloadCellHint(w.blockBOC, w.collatedData),
		)
		if err == nil {
			w.wire, err = simplex.SerializeCandidatePrepared(w.candidate, prepared)
		}
		if err != nil {
			w.err = err
		} else {
			w.hash = sha256.Sum256(w.wire)
		}
	})

	return w.wire, w.hash, w.err
}

func (w *lazyCandidateWire) fileHash() [32]byte {
	var fileHash [32]byte
	copy(fileHash[:], w.candidate.Block.FileHash)

	return fileHash
}
