package service

import (
	"encoding/binary"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// stateCellCacheIndex avoids storing another full hash beside the one already
// owned by each record. Fingerprints select candidates only: every hit checks
// the full hash. Collision links are allocated only when two fingerprints match.
// The owning cache's mutex protects the index and its append-only record positions.
type stateCellCacheIndex struct {
	positions map[uint64]int
	next      map[int]int
}

func newStateCellCacheIndex(capacity int) stateCellCacheIndex {
	return stateCellCacheIndex{positions: make(map[uint64]int, capacity)}
}

func (x *stateCellCacheIndex) indexOf(records []storage.EncodedCellRecord, hash cell.Hash) (int, error) {
	pos, ok := x.positions[binary.LittleEndian.Uint64(hash[:8])]
	if !ok {
		return 0, storage.ErrNotFound
	}
	if records[pos].Hash == hash {
		return pos, nil
	}
	return x.collisionIndexOf(records, hash, pos)
}

func (x *stateCellCacheIndex) collisionIndexOf(records []storage.EncodedCellRecord, hash cell.Hash, pos int) (int, error) {
	for {
		next, ok := x.next[pos]
		if !ok {
			return 0, storage.ErrNotFound
		}
		if records[next].Hash == hash {
			return next, nil
		}
		pos = next
	}
}

// insert adds a hash already known to be absent. Positions stay stable when the
// cache replaces a record's encoding or its backing slices grow.
func (x *stateCellCacheIndex) insert(hash cell.Hash, pos int) {
	fingerprint := binary.LittleEndian.Uint64(hash[:8])
	if previous, ok := x.positions[fingerprint]; ok {
		if x.next == nil {
			x.next = make(map[int]int)
		}
		x.next[pos] = previous
	}
	x.positions[fingerprint] = pos
}
