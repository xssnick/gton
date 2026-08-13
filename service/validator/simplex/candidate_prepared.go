package simplex

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ErrPreparedCandidateMismatch reports a prepared payload handed over with a
// candidate it was not built for.
var ErrPreparedCandidateMismatch = errors.New("simplex: prepared payload does not match the candidate")

// PreparedCandidate is the compressed payload of one candidate, built from the
// cell roots it is made of rather than from the two BOCs those roots were
// serialized into.
//
// Both producers of a payload already hold those roots: the collator has just
// built the block, and the codec has just decoded a remote candidate. Going
// through the bytes instead makes each of them parse both BOCs again and
// re-serialize them to prove they are canonical — the cost of the payload paid
// twice over, on the path between a finished candidate and its broadcast.
//
// The three hashes are the candidate identity ComputeID covers, so a serializer
// can tell whether a prepared payload belongs to the candidate it was asked to
// serialize without hashing anything. Only the root hash is derived here; the
// two file hashes are declared by the producer, which computed them over the
// very bytes it serialized from these roots.
//
// The zero value is unusable and every field is unexported: the only way to
// obtain one is to hand over the roots, which is what makes a payload that
// reached this type provably a serialization of them.
type PreparedCandidate struct {
	// ready is closed once payload and err are final. It is closed before
	// PrepareCandidate returns, and from the goroutine PrepareCandidateAsync
	// starts.
	ready   chan struct{}
	payload []byte
	err     error

	rootHash         [32]byte
	fileHash         [32]byte
	collatedFileHash [32]byte
}

// PrepareCandidate builds the payload on the calling goroutine.
func PrepareCandidate(
	seqNo uint32,
	blockRoot *cell.Cell,
	collatedRoots []*cell.Cell,
	fileHash [32]byte,
	collatedFileHash [32]byte,
) (*PreparedCandidate, error) {
	prepared, build := newPreparedCandidate(seqNo, blockRoot, collatedRoots, fileHash, collatedFileHash)
	build()
	if prepared.err != nil {
		return nil, prepared.err
	}

	return prepared, nil
}

// PrepareCandidateAsync starts the build on its own goroutine and returns
// immediately. The caller overlaps the compression tail of its candidate with
// whatever it does between building that candidate and serializing it —
// signing, persistence and the scheduled broadcast wait for a collator.
//
// The roots must be final: they are read without synchronisation until the
// build completes. Cell trees are immutable once finalized, and BOC
// serialization never parses a cell, so a concurrent reader of the same tree is
// safe; a producer that still mutates its roots is not.
func PrepareCandidateAsync(
	seqNo uint32,
	blockRoot *cell.Cell,
	collatedRoots []*cell.Cell,
	fileHash [32]byte,
	collatedFileHash [32]byte,
) *PreparedCandidate {
	prepared, build := newPreparedCandidate(seqNo, blockRoot, collatedRoots, fileHash, collatedFileHash)
	go build()

	return prepared
}

func newPreparedCandidate(
	seqNo uint32,
	blockRoot *cell.Cell,
	collatedRoots []*cell.Cell,
	fileHash [32]byte,
	collatedFileHash [32]byte,
) (*PreparedCandidate, func()) {
	prepared := &PreparedCandidate{
		ready:            make(chan struct{}),
		fileHash:         fileHash,
		collatedFileHash: collatedFileHash,
	}
	if blockRoot == nil {
		prepared.err = errors.New("simplex: prepared candidate has no block root")

		return prepared, func() { close(prepared.ready) }
	}
	copy(prepared.rootHash[:], blockRoot.Hash())

	// The reference orders the combined BOC block-root first, then the collated
	// roots, and this slice is the only place that order is applied on the
	// prepared path.
	roots := make([]*cell.Cell, 1, len(collatedRoots)+1)
	roots[0] = blockRoot
	roots = append(roots, collatedRoots...)

	return prepared, func() {
		defer close(prepared.ready)
		prepared.payload, prepared.err = compressCandidatePayload(seqNo, prepared.rootHash[:], roots)
	}
}

// payloadFor returns the payload once it is built, after checking that it
// belongs to candidate. The binding is checked before the wait so a mismatch is
// reported without blocking on work that will be discarded.
func (p *PreparedCandidate) payloadFor(candidate Candidate) ([]byte, error) {
	if err := p.bind(candidate); err != nil {
		return nil, err
	}
	<-p.ready
	if p.err != nil {
		return nil, p.err
	}

	return p.payload, nil
}

func (p *PreparedCandidate) bind(candidate Candidate) error {
	if candidate.Empty {
		return fmt.Errorf("%w: empty candidate carries a payload", ErrPreparedCandidateMismatch)
	}
	if !bytes.Equal(candidate.Block.RootHash, p.rootHash[:]) {
		return fmt.Errorf("%w: block root hash", ErrPreparedCandidateMismatch)
	}
	if !bytes.Equal(candidate.Block.FileHash, p.fileHash[:]) {
		return fmt.Errorf("%w: block file hash", ErrPreparedCandidateMismatch)
	}
	if candidate.CollatedFileHash != p.collatedFileHash {
		return fmt.Errorf("%w: collated file hash", ErrPreparedCandidateMismatch)
	}

	return nil
}
