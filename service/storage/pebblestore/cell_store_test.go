package pebblestore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/rs/zerolog"
)

type closeBarrierFS struct {
	vfs.FS
	active  atomic.Bool
	mu      sync.Mutex
	targets map[string]struct{}
	blocked map[string]struct{}
	started chan string
	release chan struct{}
}

type closeBarrierFile struct {
	vfs.File
	fs   *closeBarrierFS
	name string
}

func newCloseBarrierFS(fs vfs.FS, targets []string) *closeBarrierFS {
	targetSet := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		targetSet[target] = struct{}{}
	}
	return &closeBarrierFS{
		FS:      fs,
		targets: targetSet,
		blocked: make(map[string]struct{}, len(targets)),
		started: make(chan string, len(targets)),
		release: make(chan struct{}),
	}
}

func (f *closeBarrierFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &closeBarrierFile{File: file, fs: f, name: name}, nil
}

func (f *closeBarrierFS) blockClose(name string) bool {
	if !f.active.Load() {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.targets[name]; !ok {
		return false
	}
	if _, ok := f.blocked[name]; ok {
		return false
	}
	f.blocked[name] = struct{}{}
	return true
}

func (f *closeBarrierFile) Close() error {
	if f.fs.blockClose(f.name) {
		f.fs.started <- f.name
		<-f.fs.release
	}
	return f.File.Close()
}

func TestCellStoreCloseClosesShardsInParallel(t *testing.T) {
	const generation = uint64(1)

	dir := t.TempDir()
	targets := make([]string, cellDBShardCount)
	for i := range targets {
		targets[i] = cellGenerationShardDir(dir, generation, i)
	}
	fs := newCloseBarrierFS(vfs.Default, targets)
	cells, err := openCellStore(dir, generation, fs, 1<<20, 1<<20, 2, 0, false, zerolog.Nop())
	if err != nil {
		t.Fatalf("open cell store: %v", err)
	}

	fs.active.Store(true)
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- cells.close()
	}()

	observed := make(map[string]struct{}, cellDBShardCount)
	timer := time.NewTimer(5 * time.Second)
	var observationErr error
collect:
	for len(observed) < cellDBShardCount {
		select {
		case name := <-fs.started:
			observed[name] = struct{}{}
		case <-timer.C:
			observationErr = fmt.Errorf("only %d of %d shard closes started in parallel", len(observed), cellDBShardCount)
			break collect
		}
	}
	timer.Stop()
	close(fs.release)

	select {
	case err = <-closeDone:
		if err != nil {
			t.Fatalf("close cell store: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cell store close did not finish after releasing shards")
	}
	if observationErr != nil {
		t.Fatal(observationErr)
	}
}
