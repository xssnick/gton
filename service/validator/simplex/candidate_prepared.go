package simplex

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

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
	payload *candidatePayload
	err     error

	// digests is the pair bind checks a candidate against, published separately
	// from the rest of the capsule because the producer that starts the build
	// earliest has not serialized either BOC yet and so cannot have hashed one:
	// a collator launches this the moment its roots are final, several
	// milliseconds before the two component serializations it will declare the
	// hashes of have finished. The atomic store is what orders that publication
	// against the delivery goroutine that later reads it in bind; the build
	// itself never looks at these two values, which is why it can run without
	// them.
	digests atomic.Pointer[preparedDigests]

	rootHash [32]byte
	seqNo    uint32
}

// preparedDigests is one candidate's file-hash pair: sha256 of its block BOC and
// sha256 of its collated data, as the producer computed them over the very bytes
// it serialized from this capsule's roots.
type preparedDigests struct {
	fileHash         [32]byte
	collatedFileHash [32]byte
}

// PrepareCandidate builds the payload on the calling goroutine. cellsHint is
// PayloadCellHint over the two BOCs the caller serialized these roots into; zero
// leaves the serializer at its default sizing.
func PrepareCandidate(
	seqNo uint32,
	blockRoot *cell.Cell,
	collatedRoots []*cell.Cell,
	fileHash [32]byte,
	collatedFileHash [32]byte,
	cellsHint int,
) (*PreparedCandidate, error) {
	prepared, build := newPreparedCandidate(seqNo, blockRoot, collatedRoots, cellsHint)
	prepared.DeclareDigests(fileHash, collatedFileHash)
	build()
	if prepared.err != nil {
		return nil, prepared.err
	}

	return prepared, nil
}

// PrepareCandidateAsync starts the build on its own goroutine and returns
// immediately. The caller overlaps the compression tail of its candidate with
// whatever it does between building that candidate and serializing it — for a
// collator: the rest of its own serialization, signing, persistence and the
// scheduled broadcast wait.
//
// It takes no file hashes because its callers do not have them yet — the point
// of starting here is to start before the BOCs those hashes are taken over
// exist. The producer must call DeclareDigests once it has both, and always
// before it hands the candidate on; a payload asked for without them is refused
// rather than served unchecked.
//
// The roots must be final: they are read without synchronisation until the
// build completes. Cell trees are immutable once finalized, and BOC
// serialization never parses a cell, so a concurrent reader of the same tree is
// safe; a producer that still mutates its roots is not.
func PrepareCandidateAsync(
	seqNo uint32,
	blockRoot *cell.Cell,
	collatedRoots []*cell.Cell,
	cellsHint int,
) *PreparedCandidate {
	prepared, build := newPreparedCandidate(seqNo, blockRoot, collatedRoots, cellsHint)
	go build()

	return prepared
}

// DeclareDigests publishes the two file hashes bind compares a candidate
// against. It must be called before the capsule is reachable by whatever asks
// for the payload — the producer is the only party that can compute these, and
// the ordering between its store and that reader's load is the store itself.
func (p *PreparedCandidate) DeclareDigests(fileHash, collatedFileHash [32]byte) {
	p.digests.Store(&preparedDigests{fileHash: fileHash, collatedFileHash: collatedFileHash})
}

func newPreparedCandidate(
	seqNo uint32,
	blockRoot *cell.Cell,
	collatedRoots []*cell.Cell,
	cellsHint int,
) (*PreparedCandidate, func()) {
	prepared := &PreparedCandidate{
		ready: make(chan struct{}),
		seqNo: seqNo,
	}
	if blockRoot == nil {
		prepared.err = errors.New("simplex: prepared candidate has no block root")

		return prepared, func() { close(prepared.ready) }
	}
	prepared.rootHash = [32]byte(blockRoot.HashKey())

	// The reference orders the combined BOC block-root first, then the collated
	// roots, and this slice is the only place that order is applied on the
	// prepared path.
	roots := make([]*cell.Cell, 1, len(collatedRoots)+1)
	roots[0] = blockRoot
	roots = append(roots, collatedRoots...)

	return prepared, func() {
		defer close(prepared.ready)
		prepared.payload, prepared.err = compressCandidatePayload(seqNo, prepared.rootHash[:], roots, cellsHint)
	}
}

// payloadFor returns the payload once it is built, after checking that it
// belongs to candidate. The binding is checked before the wait so a mismatch is
// reported without blocking on work that will be discarded.
func (p *PreparedCandidate) payloadFor(candidate Candidate) (*candidatePayload, error) {
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
	if candidate.Block.SeqNo != p.seqNo {
		return fmt.Errorf("%w: block sequence number", ErrPreparedCandidateMismatch)
	}
	if !bytes.Equal(candidate.Block.RootHash, p.rootHash[:]) {
		return fmt.Errorf("%w: block root hash", ErrPreparedCandidateMismatch)
	}
	// A capsule started before its producer had hashed anything carries no
	// digests until DeclareDigests runs, which is always before the candidate
	// leaves that producer. Reaching this point without them is a producer that
	// never declared them, so the payload is refused: serving it would be
	// serving bytes nothing has checked belong to this candidate.
	digests := p.digests.Load()
	if digests == nil {
		return fmt.Errorf("%w: file hashes were never declared", ErrPreparedCandidateMismatch)
	}
	if !bytes.Equal(candidate.Block.FileHash, digests.fileHash[:]) {
		return fmt.Errorf("%w: block file hash", ErrPreparedCandidateMismatch)
	}
	if candidate.CollatedFileHash != digests.collatedFileHash {
		return fmt.Errorf("%w: collated file hash", ErrPreparedCandidateMismatch)
	}

	return nil
}
