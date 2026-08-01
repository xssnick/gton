package pebblestore

import (
	"bytes"
	"testing"
)

func TestOverlayPeerCacheRoundtripAndOverwrite(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	overlayID := bytes.Repeat([]byte{0xAB}, 32)
	otherID := bytes.Repeat([]byte{0xCD}, 32)

	if raw, err := store.LoadOverlayPeerCache(overlayID); err != nil || raw != nil {
		t.Fatalf("missing snapshot must load as nil, got %v %v", raw, err)
	}

	first := []byte("snapshot-v1")
	if err := store.SaveOverlayPeerCache(overlayID, first); err != nil {
		t.Fatalf("save: %v", err)
	}
	if raw, err := store.LoadOverlayPeerCache(overlayID); err != nil || !bytes.Equal(raw, first) {
		t.Fatalf("load = %q %v, want %q", raw, err, first)
	}

	second := []byte("snapshot-v2-replacing")
	if err := store.SaveOverlayPeerCache(overlayID, second); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if raw, err := store.LoadOverlayPeerCache(overlayID); err != nil || !bytes.Equal(raw, second) {
		t.Fatalf("overwrite load = %q %v, want %q", raw, err, second)
	}

	// Overlays are isolated by key.
	if raw, err := store.LoadOverlayPeerCache(otherID); err != nil || raw != nil {
		t.Fatalf("other overlay must stay empty, got %v %v", raw, err)
	}
}
