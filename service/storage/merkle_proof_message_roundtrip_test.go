package storage

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// An internal message taken from the load stand (block 0:e000000000000000
// seqno 87642379): its body carries a Merkle proof of a dictionary, so the
// state that enqueues it holds level-1 cells — pruned siblings and the ordinary
// cells above them — which no other part of a shard state does. Collating the
// block that imported it failed 65 times with "loaded lazy ref does not match
// placeholder: depth mismatch at level 1" before the slot went out empty.
const stateMessageWithMerkleProofBOC = "te6cckECJAEABEcAA69oAN0j+6IT0rSH37trt8nsETo9MwXHuEXZ/IHqIzNWxsJHADK8w99HyMQe3wkW9S+e6hPWlJ/78SnO/m8zFoXIwS8cTwfNYAYPs9QAAKtIDdQdhNUzDr0bAQIDCEICsK+TL6FfBycCE2aoyY+V9xaAF6g6tugahSex8zXxM6IAg4ACozkpZnDej7fFLCLaXxyihMS8rScngWgepFzwGePS8ALCfCzugI5WRtYwb96kQ6f6JsP4DytR3M8UdbiyFD4q8QKdQlAqsyLaIfMMixESgA3SP7ohPStIffu2u3yewROj0zBce4Rdn8geojM1bGwkcAZBa3+XkoHIdQFjZ0t6xX44gktFhTn10Yzsn4nUTJ+W0gQFCUYDvsrTXs885YLttN9OG8Vv312xFXWpO8t8wJ02x8ar2B0ACwYClbCfCzugI5WRtYwb96kQ6f6JsP4DytR3M8UdbiyFD4q84AO8fCNucovlJEDN0JhF5/VYrkow5JGvy7yHj/ZGzELuPFK3SmXvx82PQBcYIgEgBwgiASAJCihIAQGomWC11JeHPK8UFMd65eQ2hoVEHj3nnrUAGihxubAZuwAKKEgBAcvWOA98OSMU99zlzSQLdeXNCaDzPVdxTJcJwRWRzIKcAAkiASALDCIBIA0OKEgBAar1z1FCOBJOVLHHo9vxC7YKoyrAiro87ivQ7iJuqLxrAAgoSAEBQtDtURMNYqCsFZpz8cw9CAecgZP3RE+ebHEMIc8wqOkABCIBIA8QKEgBAb71vkdnzQHsTQEgSeR/Xu0754BdC7Pky33HoQ1z5YykAAIiASAREihIAQHAamC2yi3T4Ihf/qV6nLGEuJ0/3fGGwdeu8KPcKx6lowADIgEgExQoSAEB4pxkX1i6ZudQz2b8Fs0/36INIOi7sTdK2EsaVUwVCaEAAgIBIBUWAEG+BWJt9F0rhL2Vqs+31lW7gUcfSCQomWW3RXIN2Z1n5zAAQb4Ci5D4m7/V1Szu7CYj3ZLIi/K5/cETYFz35cY3PM6G8AED0AgZAgEgHB0BU4AyC1v8vJQOQ6gLGzpb1ivxxBJaLCnProxnZPxOomT8toAAAAAGqfxnqBoBC5zEtAAIAhsAgCA438Ii2iHzDIsRErCfCzugI5WRtYwb96kQ6f6JsP4DytR3M8UdbiyFD4q8DcXkp+/k15NxyIRTvIruW7j7TxICASAeHwIBICIjAEO/o/soGhH5GZOSoh7Yo2NVJh562UFATS9gGWeQym5WVFLAAgFIICEAQb8MnrGI1ZgSFV2cG6woioVdhW9yoV3kAGKTjnhNimxf/wBBvyoaCFw34uqxB8XxI9YIttOMImGxMiat9LgNQhhKrZ0HAEO/rqTxZs+Rclxi4JIInfaxkjJpan9RKoJiPGPvkW8BdGbAAEO/gPW4NOd81jca5ga1KTVbXQ81oNDafmxyUCxdhJBzKDjADkKIfQ=="

func stateMessageWithMerkleProof(t *testing.T) *cell.Cell {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(stateMessageWithMerkleProofBOC)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := cell.FromBOC(raw)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

// walkLockstep parses the lazily loaded tree next to the original one and
// reports the first place a hash or a depth differs, or a ref fails to load.
func walkLockstep(t *testing.T, path string, want, got *cell.Cell) error {
	t.Helper()
	wantMask, gotMask := want.LevelMask(), got.LevelMask()
	if wantMask != gotMask {
		return fmt.Errorf("%s: level mask %03b, want %03b", path, gotMask.Mask, wantMask.Mask)
	}
	for level := 0; level <= wantMask.GetLevel(); level++ {
		if string(want.Hash(level)) != string(got.Hash(level)) {
			return fmt.Errorf("%s: hash at level %d differs", path, level)
		}
		if want.Depth(level) != got.Depth(level) {
			return fmt.Errorf("%s: depth at level %d = %d, want %d", path, level, got.Depth(level), want.Depth(level))
		}
	}
	if want.RefsNum() != got.RefsNum() {
		return fmt.Errorf("%s: refs %d, want %d", path, got.RefsNum(), want.RefsNum())
	}
	gotSlice, err := got.BeginParse()
	if err != nil {
		return fmt.Errorf("%s: parse: %w", path, err)
	}
	wantSlice, err := want.BeginParse()
	if err != nil {
		return fmt.Errorf("%s: parse original: %w", path, err)
	}
	for i := 0; i < int(want.RefsNum()); i++ {
		wantRef, err := wantSlice.LoadRef()
		if err != nil {
			return fmt.Errorf("%s: load original ref %d: %w", path, i, err)
		}
		gotRef, err := gotSlice.LoadRef()
		if err != nil {
			return fmt.Errorf("%s: load ref %d: %w", path, i, err)
		}
		if err = walkLockstep(t, fmt.Sprintf("%s/%d", path, i), wantRef.MustToCell(), gotRef.MustToCell()); err != nil {
			return err
		}
	}
	return nil
}

func lazyLoaderOver(records map[cell.Hash][]byte) cell.LazyCellLoader {
	var loader cell.LazyCellLoader
	loader = func(hash cell.Hash) (*cell.Cell, error) {
		encoded, ok := records[hash]
		if !ok {
			return nil, fmt.Errorf("cell %x is not in the store", hash[:6])
		}
		return DecodeLazyCellRecordTrusted(hash[:], encoded, loader)
	}
	return loader
}

// The sync pipeline writes the cells of a block's state update through
// PrepareStateUpdateCells; the validator later reads the same cells back
// lazily, one placeholder at a time, and every placeholder is checked against
// the cell it loads. A state carrying a Merkle proof must survive that round
// trip exactly like any other state.
func TestStateUpdateCellsRoundTripAMessageBodyWithAMerkleProof(t *testing.T) {
	msg := stateMessageWithMerkleProof(t)
	old := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	next := cell.BeginCell().MustStoreUInt(2, 8).MustStoreRef(msg).EndCell()
	update, err := cell.CreateMerkleUpdate(old, next)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareStateUpdateCells(update)
	if err != nil {
		t.Fatal(err)
	}
	records := make(map[cell.Hash][]byte)
	if err = prepared.ForEach(func(record EncodedCellRecord) error {
		records[record.Hash] = record.Data
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	loader := lazyLoaderOver(records)
	root, err := loader(next.HashKey())
	if err != nil {
		t.Fatalf("load the state root back: %v", err)
	}
	if err = walkLockstep(t, "state", next, root); err != nil {
		t.Fatalf("state update cells do not round-trip: %v", err)
	}
}

// The metadata encoder is the other writer of state cells — the applied-state
// cache's root record and PrepareReachableStateCells — and it takes its refs
// from tonutils' GetMetadata, which decides a child's level by the same rule.
func TestReachableStateCellsRoundTripAMessageBodyWithAMerkleProof(t *testing.T) {
	msg := stateMessageWithMerkleProof(t)
	next := cell.BeginCell().MustStoreUInt(2, 8).MustStoreRef(msg).EndCell()
	prepared, err := PrepareReachableStateCells(next)
	if err != nil {
		t.Fatal(err)
	}
	records := make(map[cell.Hash][]byte)
	if err = prepared.ForEach(func(record EncodedCellRecord) error {
		records[record.Hash] = record.Data
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	loader := lazyLoaderOver(records)
	root, err := loader(next.HashKey())
	if err != nil {
		t.Fatalf("load the state root back: %v", err)
	}
	if err = walkLockstep(t, "state", next, root); err != nil {
		t.Fatalf("reachable state cell records do not round-trip: %v", err)
	}
}
