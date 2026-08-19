package pebblestore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// THE CLASSIFICATION, enumerated. Every write in this store is a commitment — a
// record whose purpose is to constrain this node's future behaviour — except the
// candidate wire, which is bulk content the network holds copies of.
//
// Why that line and not another: losing a commitment across a restart lets this
// node produce a CONFLICTING one, which is a safety violation and not a slow
// resync. Losing the candidate wire costs a re-fetch and nothing else, because the
// candidate is broadcast before it is stored and notarization requires 2/3 of
// weight to have voted, which requires those 2/3 to have stored it — so a
// notarized candidate is held by at least 2/3 of the network, and a candidate only
// we hold cannot be notarized.
//
// This test is the gate on that line. The zero value of durabilityClass is already
// the strict one, so nothing becomes unsynced by omission; what this adds is that
// nothing becomes unsynced by a one-word edit either. A new relaxed write has to be
// added to the list below, next to a justification of the same form.
func TestDurabilityClassificationIsExplicit(t *testing.T) {
	// Keys are RECEIVER-QUALIFIED, and that is the whole point of the change that
	// introduced this comment. Under a bare function name
	// ValidatorStore.SaveCandidate (session.go, recoverablePayload) and
	// CollatorStore.SaveCandidate (collator_store.go, commitment by omission)
	// collide on "SaveCandidate", so the single guard against a wrong class was
	// blind on the single colliding pair: an allowlist entry written for one of
	// them would have silently authorized an unsynced write added to the other.
	// See TestDurabilitySitesAreReceiverQualified.
	relaxed := map[string]string{
		"ValidatorStore.SaveCandidate": "the candidate wire: broadcast before it is stored, and held by the " +
			"2/3 of weight whose votes notarization requires",
	}

	named := namedDurabilitySites(t)
	if len(named) == 0 {
		t.Fatal("no durability class is named anywhere in the package, so this test checks nothing")
	}
	for _, site := range sortedSites(named) {
		switch named[site] {
		case "durableCommitment":
			// Naming the strict class is always allowed; it is also the default.
		case "recoverablePayload":
			if _, allowed := relaxed[site]; !allowed {
				t.Errorf(
					"%s commits WITHOUT an fsync and is not in this test's list. A record whose "+
						"purpose is to constrain this node's future behaviour — anything it has "+
						"signed, voted, notarized, finalized or authorized — must stay synced: a "+
						"restart that forgets one can produce a conflicting record, which is a "+
						"safety violation, not a slow resync. If this write really is recoverable "+
						"bulk content, say why here.",
					site,
				)
			}
		default:
			t.Errorf("%s names durability %q, which is not one of the two classes", site, named[site])
		}
	}
	for site := range relaxed {
		if _, found := named[site]; !found {
			t.Errorf("%s is listed as an unsynced write but no longer names a durability class", site)
		}
	}
}

// namedDurabilitySites maps the enclosing function — receiver-qualified, e.g.
// "ValidatorStore.SaveCandidate" — to the durability constant it names in a
// writeRequest literal. Writes that omit the field do not appear: they are
// commitments by the zero value, which is what makes omission safe.
//
// The receiver is part of the key because this package has two stores with
// overlapping method names, in different durability classes. A bare name makes
// the map lossy exactly where the allowlist has to be exact, and a lossy map
// still reports PASS.
func namedDurabilitySites(t *testing.T) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	named := map[string]string{}
	fileSet := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(fileSet, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				literal, isLiteral := node.(*ast.CompositeLit)
				if !isLiteral {
					return true
				}
				if ident, isIdent := literal.Type.(*ast.Ident); !isIdent || ident.Name != "writeRequest" {
					return true
				}
				for _, element := range literal.Elts {
					pair, isPair := element.(*ast.KeyValueExpr)
					if !isPair {
						continue
					}
					if key, isKey := pair.Key.(*ast.Ident); !isKey || key.Name != "durability" {
						continue
					}
					site := durabilitySiteName(function)
					class := "<not a constant>"
					if value, isValue := pair.Value.(*ast.Ident); isValue {
						class = value.Name
					}
					if previous, clash := named[site]; clash && previous != class {
						t.Fatalf("two writeRequest literals in %s name different durability "+
							"classes (%s and %s): this walk keys one class per site and "+
							"cannot describe both", site, previous, class)
					}
					named[site] = class
				}
				return true
			})
		}
	}

	return named
}

// durabilitySiteName qualifies a function by its receiver TYPE, so
// ValidatorStore.SaveCandidate and CollatorStore.SaveCandidate are two sites and
// not one. A free function keeps its bare name.
func durabilitySiteName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	if star, isStar := receiver.(*ast.StarExpr); isStar {
		receiver = star.X
	}
	if ident, isIdent := receiver.(*ast.Ident); isIdent {
		return ident.Name + "." + function.Name.Name
	}

	return function.Name.Name
}

func sortedSites(named map[string]string) []string {
	sites := make([]string, 0, len(named))
	for site := range named {
		sites = append(sites, site)
	}
	sort.Strings(sites)

	return sites
}

// syncCountingFS counts fsyncs, which is the whole question this change is about.
// It wraps the real filesystem rather than a memory one so the write path is the
// production one; only the counting is added.
type syncCountingFS struct {
	vfs.FS
	syncs  atomic.Int64
	mu     sync.Mutex
	synced []string
}

func (f *syncCountingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, category)
	if err != nil {
		return nil, err
	}

	return &syncCountingFile{File: file, fs: f, name: name}, nil
}

func (f *syncCountingFS) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.synced...)
}

type syncCountingFile struct {
	vfs.File
	fs   *syncCountingFS
	name string
}

func (f *syncCountingFile) Sync() error {
	f.fs.syncs.Add(1)
	f.fs.mu.Lock()
	f.fs.synced = append(f.fs.synced, "sync:"+f.name)
	f.fs.mu.Unlock()

	return f.File.Sync()
}

func (f *syncCountingFile) SyncData() error {
	f.fs.syncs.Add(1)
	f.fs.mu.Lock()
	f.fs.synced = append(f.fs.synced, "syncdata:"+f.name)
	f.fs.mu.Unlock()

	return f.File.SyncData()
}

// batchGrouping records which requests shared one pebble batch, observed from
// inside apply: batch.Count() is the number of entries already written into this
// batch, so a zero count is the first request of a new batch. Pointer identity would
// not do — pebble pools batch objects, so a fresh batch can be the same pointer as
// the one just closed, and a grouping keyed on the pointer silently reports every
// batch as one.
type batchGrouping struct {
	mu       sync.Mutex
	observed []observedBatch
}

func newBatchGrouping() *batchGrouping {
	return &batchGrouping{}
}

func (g *batchGrouping) observe(batch *pebble.Batch, class durabilityClass) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if batch.Count() == 0 || len(g.observed) == 0 {
		g.observed = append(g.observed, observedBatch{class: class, count: 1})

		return
	}
	g.observed[len(g.observed)-1].count++
}

func (g *batchGrouping) batches() []observedBatch {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]observedBatch(nil), g.observed...)
}

type observedBatch struct {
	class durabilityClass
	count int
}

// The writer must never mix the classes in one batch — a batch commits with one
// pebble.WriteOptions — and must still coalesce WITHIN a class, which is the
// group-commit property the single queue exists for.
//
// The fsync count is asserted alongside the grouping, because grouping alone would
// not notice a payload batch that still went to the disk.
func TestWriterPartitionsBatchesByDurabilityClass(t *testing.T) {
	counting := &syncCountingFS{FS: vfs.Default}
	db, err := pebble.Open(t.TempDir(), &pebble.Options{FS: counting})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})

	grouping := newBatchGrouping()
	store := &Store{db: db, queue: make(chan writeRequest, 16), writerDone: make(chan struct{})}
	request := func(class durabilityClass, key string) writeRequest {
		return writeRequest{
			durability: class,
			apply: func(batch *pebble.Batch) error {
				grouping.observe(batch, class)

				return batch.Set([]byte(key), []byte{1}, nil)
			},
			done: func(error) {},
		}
	}

	// Two commitments, two payloads, one commitment: three batches, with the pairs
	// coalescing inside their class.
	for _, req := range []writeRequest{
		request(durableCommitment, "c1"),
		request(durableCommitment, "c2"),
		request(recoverablePayload, "p1"),
		request(recoverablePayload, "p2"),
		request(durableCommitment, "c3"),
	} {
		store.queue <- req
	}
	close(store.queue)

	before := counting.syncs.Load()
	store.runWriter()
	store.callbackWG.Wait()
	syncs := counting.syncs.Load() - before

	batches := grouping.batches()
	want := []observedBatch{
		{class: durableCommitment, count: 2},
		{class: recoverablePayload, count: 2},
		{class: durableCommitment, count: 1},
	}
	if len(batches) != len(want) {
		t.Fatalf("batches = %+v, want three: commitments, payloads, commitment", batches)
	}
	for i := range want {
		if batches[i] != want[i] {
			t.Errorf("batch %d = %+v, want %+v", i, batches[i], want[i])
		}
	}
	// Two commitment batches, two fsyncs; the payload batch adds none. Asserted as a
	// bound rather than an equality because pebble may sync its own bookkeeping.
	if syncs < 2 {
		t.Errorf("fsyncs = %d, want at least one per commitment batch", syncs)
	}
	if syncs > 3 {
		t.Errorf("fsyncs = %d, want the payload batch to add none to the two commitment batches", syncs)
	}
}

// And the same question asked over a run of writes, through the writer: a stream of
// candidate wires costs strictly fewer fsyncs than the same stream of commitments.
//
// Not zero, and that is worth stating precisely. pebble.NoSync removes the fsync the
// COMMIT waits for; it does not stop the log from being flushed when it rotates, and
// a 760 KB record fills a log quickly. Measured here, twenty 760 KB payload commits
// cost about half the fsyncs of twenty synced ones, and the latency the payload
// commit waits for drops by an order of magnitude — see the split measurement.
func TestCandidateWritesCostFewerFsyncsThanCommitments(t *testing.T) {
	const rounds = 20

	run := func(class durabilityClass) int64 {
		counting := &syncCountingFS{FS: vfs.Default}
		db, err := pebble.Open(t.TempDir(), &pebble.Options{FS: counting})
		if err != nil {
			t.Fatal(err)
		}
		store := &Store{db: db, queue: make(chan writeRequest, 4), writerDone: make(chan struct{})}
		go store.runWriter()

		before := counting.syncs.Load()
		wire := make([]byte, 760<<10)
		for i := 0; i < rounds; i++ {
			done := make(chan error, 1)
			key := []byte{byte(i)}
			if submitErr := store.submit(writeRequest{
				durability: class,
				sizeHint:   len(wire),
				apply: func(batch *pebble.Batch) error {
					return batch.Set(key, wire, nil)
				},
				done: func(err error) { done <- err },
			}); submitErr != nil {
				t.Fatal(submitErr)
			}
			if err = receiveTestResult(t, done); err != nil {
				t.Fatalf("commit %d: %v", i, err)
			}
		}
		syncs := counting.syncs.Load() - before
		close(store.queue)
		<-store.writerDone
		store.callbackWG.Wait()
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}

		return syncs
	}

	payload := run(recoverablePayload)
	commitment := run(durableCommitment)
	t.Logf("fsyncs over %d 760 KB writes: payload=%d commitment=%d", rounds, payload, commitment)
	if payload >= commitment {
		t.Errorf(
			"the candidate class cost %d fsyncs against the commitment class %d: the notarize "+
				"vote is still waiting for the disk",
			payload,
			commitment,
		)
	}
	// And the premise: the commitment class really is paying an fsync per commit.
	// Not asserted as exactly one per write — pebble skips a redundant fsync when
	// nothing new reached the log — but it must be of that order rather than of the
	// payload class's.
	if commitment < rounds*3/4 {
		t.Errorf(
			"commitments cost %d fsyncs over %d writes, which is too few to be one per commit: "+
				"a commitment record is no longer durable before use",
			commitment,
			rounds,
		)
	}
}

func TestDurabilityClassWriteOptions(t *testing.T) {
	if durableCommitment.writeOptions() != pebble.Sync {
		t.Error("a commitment is not fsynced")
	}
	if recoverablePayload.writeOptions() != pebble.NoSync {
		t.Error("a recoverable payload waits for an fsync")
	}
	// The zero value is the strict one: a write that says nothing is synced.
	var unset durabilityClass
	if unset != durableCommitment || unset.writeOptions() != pebble.Sync {
		t.Fatal("the zero durability class is not the strict one, so a new write could default to unsynced")
	}
}

// TestDurabilitySitesAreReceiverQualified is the regression for the key the walk
// above builds. It pins two things a bare function name cannot express:
//
//  1. the two SaveCandidate methods appear as two distinct sites, and
//  2. the one on CollatorStore is a COMMITMENT — it names no class, so it is not
//     in the map at all, and the allowlist entry that exempts
//     ValidatorStore.SaveCandidate does not reach it.
//
// Keyed by bare name the second property was unverifiable: the two collapsed
// into one entry, so an unsynced write introduced in CollatorStore.SaveCandidate
// would have been covered by the existing "SaveCandidate" exemption and the
// guard would still have reported PASS. Those two writes are in different
// durability classes — a candidate wire the network already holds against a
// record that constrains this node's future behaviour — which is the one pair
// where getting it wrong is a safety violation rather than a slow resync.
func TestDurabilitySitesAreReceiverQualified(t *testing.T) {
	named := namedDurabilitySites(t)

	if class, found := named["ValidatorStore.SaveCandidate"]; !found || class != "recoverablePayload" {
		t.Fatalf("ValidatorStore.SaveCandidate = %q (found=%v), want recoverablePayload: the "+
			"receiver-qualified key is not reaching the site the allowlist exempts", class, found)
	}
	if class, found := named["CollatorStore.SaveCandidate"]; found {
		t.Fatalf("CollatorStore.SaveCandidate now names durability %q. It was a commitment by "+
			"omission, and it is NOT covered by the ValidatorStore exemption: classify it "+
			"deliberately and, if it really is recoverable, add its own allowlist entry with the "+
			"reason", class)
	}
	if _, found := named["SaveCandidate"]; found {
		t.Fatal("a bare \"SaveCandidate\" key is back in the walk: the two stores' methods collide " +
			"under it and the allowlist stops being exact")
	}

	// And the guard must be non-vacuous: at least one site really is
	// receiver-qualified, so a regression to bare names cannot pass by producing
	// an empty map.
	qualified := 0
	for site := range named {
		if strings.Contains(site, ".") {
			qualified++
		}
	}
	if qualified == 0 {
		t.Fatal("no durability site is receiver-qualified, so this test is checking nothing")
	}
}
