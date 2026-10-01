package pebblestore

import (
	"path/filepath"
	"testing"
)

func TestOpenNormalizesRelativeStoreDirForArchiveJournal(t *testing.T) {
	dir := t.TempDir()
	workingDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	relativeDir, err := filepath.Rel(workingDir, dir)
	if err != nil {
		t.Fatal(err)
	}

	store, err := Open(Options{Dir: relativeDir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if store.dir != dir {
		t.Fatalf("store dir = %q, want absolute %q", store.dir, dir)
	}
	path := store.archivePackPath(0, -1, topShard)
	journalPath, err := store.packJournalPath(path)
	if err != nil {
		t.Fatalf("archive pack journal path: %v", err)
	}
	if journalPath != filepath.Join("archive", "packages", "arch0000", "archive.00000.pack") {
		t.Fatalf("journal path = %q", journalPath)
	}
}
