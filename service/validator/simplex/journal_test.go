package simplex

import (
	"fmt"
	"sort"
	"sync"
)

// MemoryJournal is an in-memory Journal for tests and simulations. It stores
// records in their TL-encoded form (the canonical consensus DB layout) and
// decodes them on Bootstrap, so every test run also exercises
// the record codecs. Completions are invoked inline, which keeps a directly
// driven engine fully synchronous and deterministic.
//
// FailNextWrites and OnBeforeWrite allow fault injection: crash-point tests
// make writes fail or "crash" the node between the persist and the send.
type MemoryJournal struct {
	mu sync.Mutex

	records  map[[32]byte][]byte // key hash -> encoded db.Vote value
	order    [][32]byte          // insertion order of vote/cert records
	pool     []byte              // encoded db.poolState value, nil if absent
	nextSeq  int64
	maxSigs  int
	failNext int
	onWrite  func(kind string) error
}

// NewMemoryJournal creates an empty in-memory journal. maxSigs bounds
// certificate decoding on Bootstrap (pass the validator set size).
func NewMemoryJournal(maxSigs int) *MemoryJournal {
	return &MemoryJournal{records: map[[32]byte][]byte{}, maxSigs: maxSigs}
}

// FailNextWrites makes the next n writes return an injected error.
func (j *MemoryJournal) FailNextWrites(n int) {
	j.mu.Lock()
	j.failNext = n
	j.mu.Unlock()
}

// OnBeforeWrite installs a hook called before each durable write with the
// record kind ("vote", "cert", "pool"); a non-nil result is returned to the
// engine instead of performing the write.
func (j *MemoryJournal) OnBeforeWrite(f func(kind string) error) {
	j.mu.Lock()
	j.onWrite = f
	j.mu.Unlock()
}

func (j *MemoryJournal) gate(kind string) error {
	if j.failNext > 0 {
		j.failNext--
		return fmt.Errorf("simplex: injected journal failure (%s)", kind)
	}
	if j.onWrite != nil {
		if err := j.onWrite(kind); err != nil {
			return err
		}
	}
	return nil
}

func (j *MemoryJournal) SaveOurVote(v Vote, done func(error)) {
	done(j.saveOurVote(v))
}

func (j *MemoryJournal) saveOurVote(v Vote) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	hash := OurVoteRecordHash(v)
	if _, ok := j.records[hash]; ok {
		return ErrAlreadySaved
	}
	if err := j.gate("vote"); err != nil {
		return err
	}
	j.records[hash] = EncodeOurVoteRecord(v, j.nextSeq)
	j.order = append(j.order, hash)
	j.nextSeq++
	return nil
}

func (j *MemoryJournal) SaveCertificate(c *Certificate, done func(error)) {
	done(j.saveCertificate(c))
}

func (j *MemoryJournal) saveCertificate(c *Certificate) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	hash := CertificateRecordHash(c)
	if _, ok := j.records[hash]; ok {
		return ErrAlreadySaved
	}
	if err := j.gate("cert"); err != nil {
		return err
	}
	j.records[hash] = EncodeCertificateRecord(c)
	j.order = append(j.order, hash)
	return nil
}

func (j *MemoryJournal) SaveFirstNonAnnouncedWindow(w uint32, done func(error)) {
	done(j.saveWindow(w))
}

func (j *MemoryJournal) saveWindow(w uint32) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.gate("pool"); err != nil {
		return err
	}
	j.pool = EncodePoolStateRecord(w)
	return nil
}

func (j *MemoryJournal) Bootstrap() (*BootstrapState, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	out := &BootstrapState{}
	if j.pool != nil {
		w, err := DecodePoolStateRecord(j.pool)
		if err != nil {
			return nil, err
		}
		out.FirstNonAnnouncedWindow = w
	}

	type seqVote struct {
		seq int64
		v   Vote
	}
	var votes []seqVote
	// Iterate in map order deliberately: a prefix scan leaves the physical order
	// unspecified, and recovery must not depend on it.
	for _, enc := range j.records {
		rec, err := DecodeVoteJournalRecord(enc, j.maxSigs)
		if err != nil {
			return nil, err
		}
		if rec.Vote != nil {
			votes = append(votes, seqVote{seq: rec.Seqno, v: *rec.Vote})
		} else {
			out.Certificates = append(out.Certificates, rec.Cert)
		}
	}
	sort.Slice(votes, func(a, b int) bool { return votes[a].seq < votes[b].seq })
	for _, sv := range votes {
		out.OurVotes = append(out.OurVotes, sv.v)
	}
	return out, nil
}
