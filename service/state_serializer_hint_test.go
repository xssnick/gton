package service

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	state2 "github.com/xssnick/gton/service/state"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func testHintStateBOC(tb testing.TB) ([]byte, uint64) {
	tb.Helper()

	branches := make([]*cell.Cell, 0, 4)
	for b := uint64(0); b < 4; b++ {
		builder := cell.BeginCell().MustStoreUInt(b, 64)
		for l := uint64(0); l < 4; l++ {
			leaf := cell.BeginCell().
				MustStoreUInt(b, 64).MustStoreUInt(l, 64).
				MustStoreUInt(^b, 64).MustStoreUInt(^l, 64).
				EndCell()
			builder.MustStoreRef(leaf)
		}
		branches = append(branches, builder.EndCell())
	}
	root := cell.BeginCell().MustStoreUInt(0xAA, 8)
	for _, branch := range branches {
		root.MustStoreRef(branch)
	}

	// 1 root + 4 branches + 16 distinct leaves
	return cell.ToBOCWithOptions([]*cell.Cell{root.EndCell()}, persistentStateBOCOptions()), 21
}

func TestReadBOCHeaderCellsCount(t *testing.T) {
	data, wantCells := testHintStateBOC(t)

	path := filepath.Join(t.TempDir(), "state.boc")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write state boc: %v", err)
	}

	cells, err := readBOCHeaderCellsCount(path)
	if err != nil {
		t.Fatalf("read boc header cells count: %v", err)
	}
	if cells != wantCells {
		t.Fatalf("cells count mismatch: got=%d want=%d", cells, wantCells)
	}
}

func TestReadBOCHeaderCellsCountRejectsInvalid(t *testing.T) {
	dir := t.TempDir()

	badMagic := filepath.Join(dir, "bad_magic.boc")
	if err := os.WriteFile(badMagic, []byte("not a boc file at all"), 0o644); err != nil {
		t.Fatalf("write bad magic file: %v", err)
	}
	if _, err := readBOCHeaderCellsCount(badMagic); err == nil {
		t.Fatal("expected bad magic error")
	}

	truncated := filepath.Join(dir, "truncated.boc")
	if err := os.WriteFile(truncated, []byte{0xb5, 0xee, 0x9c, 0x72, 0x01}, 0o644); err != nil {
		t.Fatalf("write truncated file: %v", err)
	}
	if _, err := readBOCHeaderCellsCount(truncated); err == nil {
		t.Fatal("expected truncated header error")
	}
}

func saveTestPersistentStateFile(t *testing.T, store interface {
	StateFilesDir() string
	SavePersistentStateFile(file *tnstore.PersistentStateFile) error
}, block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64, data []byte) {
	t.Helper()

	name, err := tnstore.PersistentStateFileName(block, master, effectiveShard)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	if err = os.MkdirAll(store.StateFilesDir(), 0o755); err != nil {
		t.Fatalf("create state files dir: %v", err)
	}
	path := filepath.Join(store.StateFilesDir(), name)
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write persistent state file: %v", err)
	}

	fileHash := sha256.Sum256(data)
	rootHash := sha256.Sum256(append([]byte("root"), data...))
	if err = store.SavePersistentStateFile(&tnstore.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   effectiveShard,
		Ref: &tnstore.ArtifactRef{
			Path: path,
			Size: int64(len(data)),
		},
		FileHash:      fileHash[:],
		StateRootHash: rootHash[:],
	}); err != nil {
		t.Fatalf("register persistent state file: %v", err)
	}
}

func TestPreviousPersistentStateCellsCountHint(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	serializer := newStateSerializer(zerolog.Nop(), store, t.TempDir(), false, 0, false)

	prevMaster := testBlockID(-1, topShard, 90)
	prevShard := testBlockID(0, topShard, 80)
	curMaster := testBlockID(-1, topShard, 100)
	curShard := testBlockID(0, topShard, 95)

	part := state2.PersistentStatePart{Kind: state2.PersistentStatePartUnsplit, EffectiveShard: 0}
	shardTarget := stateSerializationTarget{block: curShard, kind: "basechain"}
	masterTarget := stateSerializationTarget{block: curMaster, kind: "masterchain"}

	// No descriptions at all yet.
	if hint := serializer.previousPersistentStateCellsCountHint(ctx, curMaster, shardTarget, part); hint != 0 {
		t.Fatalf("expected zero hint without descriptions, got %d", hint)
	}

	endTime := uint64(time.Now().Unix()) + 3600
	if err := store.SavePersistentStateDescription(ctx, &tnstore.PersistentStateDescription{
		MasterchainBlock: prevMaster,
		ShardBlocks:      []tnstore.PersistentStateDescriptionShard{{Block: prevShard}},
		StartTime:        1,
		EndTime:          endTime,
	}); err != nil {
		t.Fatalf("save previous description: %v", err)
	}
	// The current epoch description is stored before the run starts, so the
	// lookup must skip it and use the strictly older one.
	if err := store.SavePersistentStateDescription(ctx, &tnstore.PersistentStateDescription{
		MasterchainBlock: curMaster,
		ShardBlocks:      []tnstore.PersistentStateDescriptionShard{{Block: curShard}},
		StartTime:        2,
		EndTime:          endTime,
	}); err != nil {
		t.Fatalf("save current description: %v", err)
	}

	// Description exists but the previous file is not registered.
	if hint := serializer.previousPersistentStateCellsCountHint(ctx, curMaster, shardTarget, part); hint != 0 {
		t.Fatalf("expected zero hint without previous file, got %d", hint)
	}

	data, cells := testHintStateBOC(t)
	saveTestPersistentStateFile(t, store, prevShard, prevMaster, 0, data)
	saveTestPersistentStateFile(t, store, prevMaster, prevMaster, 0, data)

	wantHint := cells + cells/20
	if hint := serializer.previousPersistentStateCellsCountHint(ctx, curMaster, shardTarget, part); hint != wantHint {
		t.Fatalf("shard hint mismatch: got=%d want=%d", hint, wantHint)
	}
	if hint := serializer.previousPersistentStateCellsCountHint(ctx, curMaster, masterTarget, part); hint != wantHint {
		t.Fatalf("masterchain hint mismatch: got=%d want=%d", hint, wantHint)
	}

	// Unknown shard in the previous epoch (e.g. split/merge changed topology).
	otherTarget := stateSerializationTarget{block: testBlockID(0, topShard>>1, 95), kind: "basechain"}
	if hint := serializer.previousPersistentStateCellsCountHint(ctx, curMaster, otherTarget, part); hint != 0 {
		t.Fatalf("expected zero hint for unknown shard, got %d", hint)
	}

	// Unknown effective shard part (e.g. split depth changed between epochs).
	splitPart := state2.PersistentStatePart{Kind: state2.PersistentStatePartSplitAccount, EffectiveShard: 1 << 62}
	if hint := serializer.previousPersistentStateCellsCountHint(ctx, curMaster, shardTarget, splitPart); hint != 0 {
		t.Fatalf("expected zero hint for unknown effective shard, got %d", hint)
	}

	// Serializing the oldest known master must not pick its own description.
	if hint := serializer.previousPersistentStateCellsCountHint(ctx, prevMaster, shardTarget, part); hint != 0 {
		t.Fatalf("expected zero hint for oldest master, got %d", hint)
	}
}
