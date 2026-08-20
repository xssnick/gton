package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// PreparedBlockCandidate is canonical block data derived from one parsed root.
// It crosses the consensus-decoder -> node-cache boundary without making the
// cache parse and hash the same BOC again. Its fields are private so callers
// cannot manufacture the proof that BlockBOC, root hash and file hash came
// from the same serialization.
//
// The BOC is owned privately. Crossing into another asynchronous owner returns
// a copy; otherwise a caller could mutate the slice after the root/hash binding
// was established and make a trusted cache path persist corrupt bytes.
type PreparedBlockCandidate struct {
	id   ton.BlockIDExt
	root *cell.Cell
	boc  []byte
}

// PrepareBlockCandidate takes an independently owned snapshot of the reachable
// eager block graph, serializes that snapshot in the canonical block-file mode
// and derives both hashes from it. Detaching is required because a root parsed
// from a combined candidate BOC otherwise keeps every independent collated-data
// root in the parser's flat cell and payload arenas.
func PrepareBlockCandidate(
	workchain int32,
	shard int64,
	seqno uint32,
	root *cell.Cell,
) (*PreparedBlockCandidate, error) {
	if root == nil {
		return nil, errors.New("prepare block candidate: block root is absent")
	}

	detachedRoot, err := root.CloneDetached()
	if err != nil {
		return nil, fmt.Errorf("prepare block candidate: detach block root: %w", err)
	}
	boc, err := detachedRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		return nil, err
	}
	rootHash := detachedRoot.HashKeyAt(0)
	fileHash := sha256.Sum256(boc)

	return &PreparedBlockCandidate{
		id: ton.BlockIDExt{
			Workchain: workchain,
			Shard:     shard,
			SeqNo:     seqno,
			RootHash:  rootHash[:],
			FileHash:  fileHash[:],
		},
		root: detachedRoot,
		boc:  boc,
	}, nil
}

func (p *PreparedBlockCandidate) ID() ton.BlockIDExt {
	id := p.id
	id.RootHash = append([]byte(nil), p.id.RootHash...)
	id.FileHash = append([]byte(nil), p.id.FileHash...)

	return id
}

func (p *PreparedBlockCandidate) Root() *cell.Cell {
	return p.root
}

func (p *PreparedBlockCandidate) BlockBOC() []byte {
	return bytes.Clone(p.boc)
}
