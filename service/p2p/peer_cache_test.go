package p2p

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"
)

func testCacheEntry(seed byte, addr string, score uint16, lastSeen time.Time) peerCacheEntry {
	pub := bytes.Repeat([]byte{seed}, ed25519.PublicKeySize)
	return peerCacheEntry{
		pub:      pub,
		addr:     addr,
		lastSeen: uint32(lastSeen.Unix()),
		srcScore: score,
	}
}

func TestPeerCacheSnapshotCodecRoundtrip(t *testing.T) {
	now := time.Now()
	entries := []peerCacheEntry{
		testCacheEntry(1, "10.0.0.1:30303", 7, now),
		{
			pub:      bytes.Repeat([]byte{2}, ed25519.PublicKeySize),
			addr:     "10.0.0.2:30303",
			quicAddr: "10.0.0.2:31303",
			lastSeen: uint32(now.Unix()),
			srcScore: 0xFFFF,
		},
	}

	decoded, err := decodePeerCacheSnapshot(encodePeerCacheSnapshot(entries))
	if err != nil {
		t.Fatalf("roundtrip failed: %v", err)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("roundtrip count = %d, want %d", len(decoded), len(entries))
	}
	for i := range entries {
		if !bytes.Equal(decoded[i].pub, entries[i].pub) ||
			decoded[i].addr != entries[i].addr ||
			decoded[i].quicAddr != entries[i].quicAddr ||
			decoded[i].lastSeen != entries[i].lastSeen ||
			decoded[i].srcScore != entries[i].srcScore {
			t.Fatalf("entry %d mismatch: %+v vs %+v", i, decoded[i], entries[i])
		}
	}
}

func TestPeerCacheSnapshotCodecRejectsGarbage(t *testing.T) {
	if entries, err := decodePeerCacheSnapshot(nil); err != nil || entries != nil {
		t.Fatalf("empty snapshot must decode to nothing, got %v %v", entries, err)
	}
	valid := encodePeerCacheSnapshot([]peerCacheEntry{testCacheEntry(1, "10.0.0.1:1", 1, time.Now())})
	for _, corrupt := range [][]byte{
		{0xFF},               // unknown version
		valid[:len(valid)-3], // truncated tail
		append(valid, 0x01),  // trailing junk is tolerated only inside declared bounds
		{peerCacheFormatVersion, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, // absurd count
	} {
		if _, err := decodePeerCacheSnapshot(corrupt); err == nil && corrupt[0] != peerCacheFormatVersion {
			t.Fatalf("corrupt snapshot %x must not decode", corrupt)
		}
	}
	if _, err := decodePeerCacheSnapshot(valid[:len(valid)-3]); err == nil {
		t.Fatalf("truncated snapshot must fail")
	}
}

func TestSelectPeerCacheEntriesCapsAndSlots(t *testing.T) {
	now := time.Now()
	var candidates []peerCacheEntry
	// A Sybil farm: many identities on one IP and one /24.
	for i := 0; i < 40; i++ {
		candidates = append(candidates, testCacheEntry(byte(i+1), "192.168.1.9:30303", 0, now))
	}
	for i := 0; i < 40; i++ {
		candidates = append(candidates, testCacheEntry(byte(i+50), fmt.Sprintf("192.168.2.%d:30303", i+1), 0, now))
	}
	// Diverse feeders with scores.
	for i := 0; i < 10; i++ {
		candidates = append(candidates, testCacheEntry(byte(i+120), fmt.Sprintf("10.%d.0.1:30303", i), uint16(i+1), now))
	}

	selected := selectPeerCacheEntries(candidates)

	perIP := map[string]int{}
	perSubnet := map[string]int{}
	scoreSeen := 0
	for _, entry := range selected {
		ip, subnet := peerCacheSubnet(entry.addr)
		perIP[ip]++
		perSubnet[subnet]++
		if entry.srcScore > 0 {
			scoreSeen++
		}
	}
	if perIP["192.168.1.9"] > peerCacheMaxPerIP {
		t.Fatalf("per-IP cap violated: %d", perIP["192.168.1.9"])
	}
	if perSubnet["192.168.2.0"] > peerCacheMaxPerSubnet {
		t.Fatalf("per-subnet cap violated: %d", perSubnet["192.168.2.0"])
	}
	if scoreSeen != 10 {
		t.Fatalf("all 10 scored feeders must be selected, got %d", scoreSeen)
	}
	if len(selected) > peerCacheMaxEntries {
		t.Fatalf("selection exceeds cap: %d", len(selected))
	}
	// Best feeder must rank first.
	if selected[0].srcScore != 10 {
		t.Fatalf("selection must lead with the best source, got score %d", selected[0].srcScore)
	}
}

func TestPeerCacheEntryUsableFilters(t *testing.T) {
	now := time.Now()
	good := testCacheEntry(1, "10.0.0.1:1", 0, now.Add(-time.Hour))
	if !peerCacheEntryUsable(good, now) {
		t.Fatalf("fresh entry must be usable")
	}

	old := good
	old.lastSeen = uint32(now.Add(-peerCacheMaxEntryAge - time.Hour).Unix())
	if peerCacheEntryUsable(old, now) {
		t.Fatalf("ancient entry must be dropped")
	}

	noAddr := good
	noAddr.addr = ""
	if peerCacheEntryUsable(noAddr, now) {
		t.Fatalf("entry without addr must be dropped")
	}
}

func TestDecayedPeerCacheScore(t *testing.T) {
	if got := decayedPeerCacheScore(100, 0); got != 100 {
		t.Fatalf("no idle no decay, got %d", got)
	}
	if got := decayedPeerCacheScore(100, peerCacheScoreHalfLife); got != 50 {
		t.Fatalf("one half-life must halve, got %d", got)
	}
	if got := decayedPeerCacheScore(1<<20, 0); got != 0xFFFF {
		t.Fatalf("score must clamp to u16, got %d", got)
	}
	if got := decayedPeerCacheScore(0xFFFF, 30*24*time.Hour); got != 0 {
		t.Fatalf("month-idle score must decay to zero, got %d", got)
	}
}
