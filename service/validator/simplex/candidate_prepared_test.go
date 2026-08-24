package simplex

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// preparedCandidateFixture is one candidate in both of the forms a serializer
// accepts: the two BOCs, and the roots they were serialized from.
type preparedCandidateFixture struct {
	candidate     Candidate
	blockRoot     *cell.Cell
	collatedRoots []*cell.Cell
	blockBOC      []byte
	collatedData  []byte
}

func newPreparedCandidateFixture(t *testing.T, delegated bool) preparedCandidateFixture {
	t.Helper()

	blockRoot := cell.BeginCell().MustStoreUInt(0x1234, 16).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell()).EndCell()
	collatedRoots := []*cell.Cell{
		cell.BeginCell().MustStoreUInt(0x5678, 16).EndCell(),
		cell.BeginCell().MustStoreUInt(0x9abc, 16).EndCell(),
	}
	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	collatedData, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}

	fileHash := sha256.Sum256(blockBOC)
	candidate := Candidate{
		Parent: Parent(CandidateID{Slot: 4, Hash: [32]byte{0x44}}),
		Leader: 2,
		Block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     19,
			RootHash:  bytes.Clone(blockRoot.Hash()),
			FileHash:  fileHash[:],
		},
		CollatedFileHash: sha256.Sum256(collatedData),
		Signature:        bytes.Repeat([]byte{0x55}, ed25519.SignatureSize),
	}
	if delegated {
		candidate.Delegation = &Delegation{
			CollatorKey: bytes.Repeat([]byte{0x66}, ed25519.PublicKeySize),
			Signature:   bytes.Repeat([]byte{0x77}, ed25519.SignatureSize),
		}
	}
	candidate.ID = candidate.ComputeID(8)

	return preparedCandidateFixture{
		candidate:     candidate,
		blockRoot:     blockRoot,
		collatedRoots: collatedRoots,
		blockBOC:      blockBOC,
		collatedData:  collatedData,
	}
}

func (f preparedCandidateFixture) prepare(t *testing.T) *PreparedCandidate {
	t.Helper()

	prepared, err := PrepareCandidate(
		f.candidate.Block.SeqNo,
		f.blockRoot,
		f.collatedRoots,
		sha256.Sum256(f.blockBOC),
		f.candidate.CollatedFileHash,
		PayloadCellHint(f.blockBOC, f.collatedData),
	)
	if err != nil {
		t.Fatal(err)
	}

	return prepared
}

// The prepared path exists only to reach the same bytes over a cheaper route.
// Anything else is a wire change, so both serializers are compared for every
// combination of entry point and delegation.
func TestPreparedCandidateWireIsIdenticalToTheFullPath(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		fixture := newPreparedCandidateFixture(t, delegated)

		wire, err := SerializeCandidate(fixture.candidate, fixture.blockBOC, fixture.collatedData)
		if err != nil {
			t.Fatal(err)
		}
		preparedWire, err := SerializeCandidatePrepared(fixture.candidate, fixture.prepare(t))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire, preparedWire) {
			t.Fatalf("delegated=%v: prepared candidate wire differs from the full path", delegated)
		}

		broadcast, err := SerializeCandidateForBroadcast(fixture.candidate, fixture.blockBOC, fixture.collatedData)
		if err != nil {
			t.Fatal(err)
		}
		preparedBroadcast, err := SerializeCandidateForBroadcastPrepared(fixture.candidate, fixture.prepare(t))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(broadcast.Data, preparedBroadcast.Data) {
			t.Fatalf("delegated=%v: prepared broadcast data differs from the full path", delegated)
		}
		if !bytes.Equal(broadcast.Extra, preparedBroadcast.Extra) {
			t.Fatalf("delegated=%v: prepared broadcast extra differs from the full path", delegated)
		}
	}
}

// The asynchronous constructor is the one the collator uses; it must reach the
// same payload as the synchronous one, and its consumer must block until it is
// ready rather than observe a half-built one.
func TestPrepareCandidateAsyncMatchesSynchronousPayload(t *testing.T) {
	fixture := newPreparedCandidateFixture(t, false)

	async := PrepareCandidateAsync(
		fixture.candidate.Block.SeqNo,
		fixture.blockRoot,
		fixture.collatedRoots,
		PayloadCellHint(fixture.blockBOC, fixture.collatedData),
	)
	async.DeclareDigests(sha256.Sum256(fixture.blockBOC), fixture.candidate.CollatedFileHash)
	asyncWire, err := SerializeCandidatePrepared(fixture.candidate, async)
	if err != nil {
		t.Fatal(err)
	}
	syncWire, err := SerializeCandidatePrepared(fixture.candidate, fixture.prepare(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(asyncWire, syncWire) {
		t.Fatal("asynchronously prepared candidate wire differs from the synchronous one")
	}
}

// The asynchronous constructor takes no file hashes, because its caller starts
// the build before the BOCs those hashes are taken over exist. That leaves one
// window in which a capsule is complete and yet nothing has bound it to a
// candidate, and a producer that never closed it must not get its payload
// broadcast unchecked: the two hashes are half of what ComputeID covers, so
// serving the payload without them would put a candidate's identity on the wire
// with a body nothing had matched against it.
func TestPreparedCandidateRefusesUndeclaredDigests(t *testing.T) {
	fixture := newPreparedCandidateFixture(t, false)

	undeclared := PrepareCandidateAsync(
		fixture.candidate.Block.SeqNo,
		fixture.blockRoot,
		fixture.collatedRoots,
		PayloadCellHint(fixture.blockBOC, fixture.collatedData),
	)
	if _, err := SerializeCandidatePrepared(fixture.candidate, undeclared); !errors.Is(err, ErrPreparedCandidateMismatch) {
		t.Fatalf("serialized a candidate from a capsule bound to nothing: %v", err)
	}

	// Declared late is still declared: the same capsule serves the same payload
	// once its producer has closed the window.
	undeclared.DeclareDigests(sha256.Sum256(fixture.blockBOC), fixture.candidate.CollatedFileHash)
	wire, err := SerializeCandidatePrepared(fixture.candidate, undeclared)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := SerializeCandidatePrepared(fixture.candidate, fixture.prepare(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, expected) {
		t.Fatal("a late-declared capsule serialized a different candidate wire")
	}
}

// A payload that does not belong to the candidate it is handed with must be
// refused rather than broadcast: every hash ComputeID covers is checked.
func TestPreparedCandidateRejectsAnotherCandidate(t *testing.T) {
	fixture := newPreparedCandidateFixture(t, false)
	prepared := fixture.prepare(t)

	otherRoot := cell.BeginCell().MustStoreUInt(0x4321, 16).EndCell()
	otherBOC, err := otherRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	otherFileHash := sha256.Sum256(otherBOC)

	for name, mutate := range map[string]func(*Candidate){
		"sequence number": func(c *Candidate) {
			c.Block.SeqNo++
		},
		"root hash": func(c *Candidate) {
			c.Block.RootHash = bytes.Clone(otherRoot.Hash())
		},
		"file hash": func(c *Candidate) {
			c.Block.FileHash = otherFileHash[:]
		},
		"collated file hash": func(c *Candidate) {
			c.CollatedFileHash = sha256.Sum256(otherBOC)
		},
		"empty candidate": func(c *Candidate) {
			c.Empty = true
		},
	} {
		other := fixture.candidate
		mutate(&other)
		other.ID = other.ComputeID(other.ID.Slot)

		if _, err = SerializeCandidatePrepared(other, prepared); !errors.Is(err, ErrPreparedCandidateMismatch) {
			t.Fatalf("%s: serialized a foreign candidate from a prepared payload: %v", name, err)
		}
	}

	if _, err = SerializeCandidatePrepared(fixture.candidate, nil); err == nil ||
		!strings.Contains(err.Error(), "prepared candidate payload is absent") {
		t.Fatalf("missing prepared payload = %v", err)
	}
}
