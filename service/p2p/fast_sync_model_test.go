package p2p

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"slices"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
)

func TestNewFastSyncOverlayIdentity(t *testing.T) {
	t.Parallel()

	var zeroStateFileHash FastSyncFileHash
	for i := range zeroStateFileHash {
		zeroStateFileHash[i] = byte(i)
	}

	identity := NewFastSyncOverlayIdentity(
		zeroStateFileHash,
		FastSyncShard{Workchain: 0, Shard: topShard},
	)

	if got := hex.EncodeToString(identity.FullID[:]); got != "04f0cd245e1793e3e459998c7932cc171c612787d7042af187074a6d69115aa1" {
		t.Fatalf("fast sync full overlay id = %s", got)
	}
	if got := hex.EncodeToString(identity.ShortID[:]); got != "9a1196e821a8f5fe3f833e40769bba86a854b4110f2dd903548e15f6a8085471" {
		t.Fatalf("fast sync short overlay id = %s", got)
	}
	if identity.FullID == FastSyncOverlayFullID(identity.ShortID) {
		t.Fatal("fast sync full and short overlay ids are equal")
	}

	other := NewFastSyncOverlayIdentity(
		zeroStateFileHash,
		FastSyncShard{Workchain: -1, Shard: topShard},
	)
	if other == identity {
		t.Fatal("fast sync overlay identity did not include workchain")
	}
}

func TestNewFastSyncValidatorRoster(t *testing.T) {
	t.Parallel()

	sharedADNL := fastSyncTestPeerID(0x30)
	previous := []FastSyncValidator{
		{
			PublicKey: fastSyncTestPublicKey(0x11),
		},
		{
			PublicKey: fastSyncTestPublicKey(0x22),
			ADNLID:    sharedADNL,
		},
	}
	current := []FastSyncValidator{
		previous[0],
		{
			PublicKey: fastSyncTestPublicKey(0x33),
			ADNLID:    sharedADNL,
		},
	}
	next := []FastSyncValidator{
		{
			PublicKey: fastSyncTestPublicKey(0x44),
			ADNLID:    fastSyncTestPeerID(0x10),
		},
		previous[1],
	}

	roster := NewFastSyncValidatorRoster(previous, current, next)

	fallbackID := fastSyncTestValidatorID(t, previous[0].PublicKey)
	expectedADNL := []PeerID{
		fallbackID,
		sharedADNL,
		fastSyncTestPeerID(0x10),
	}
	expectedADNL = sortUniqueFastSyncPeerIDs(expectedADNL)
	if got := roster.ADNLIDs(); !slices.Equal(got, expectedADNL) {
		t.Fatalf("fast sync adnl roster = %v, want %v", got, expectedADNL)
	}
	if roster.Len() != len(expectedADNL) {
		t.Fatalf("fast sync adnl roster len = %d, want %d", roster.Len(), len(expectedADNL))
	}
	if !roster.ContainsADNL(fallbackID) {
		t.Fatal("fast sync adnl roster omitted zero-adnl public key fallback")
	}
	if roster.ContainsADNL(fastSyncTestPeerID(0xff)) {
		t.Fatal("fast sync adnl roster contains unknown id")
	}

	expectedValidators := []PeerID{
		fastSyncTestValidatorID(t, previous[0].PublicKey),
		fastSyncTestValidatorID(t, previous[1].PublicKey),
		fastSyncTestValidatorID(t, current[1].PublicKey),
		fastSyncTestValidatorID(t, next[0].PublicKey),
	}
	expectedValidators = sortUniqueFastSyncPeerIDs(expectedValidators)
	if got := roster.RootPublicKeyIDs(); !slices.Equal(got, expectedValidators) {
		t.Fatalf("fast sync validator ids = %v, want %v", got, expectedValidators)
	}

	previous[0].PublicKey[0] ^= 0xff
	previous[0].ADNLID = fastSyncTestPeerID(0xee)
	got := roster.ADNLIDs()
	got[0] = fastSyncTestPeerID(0xdd)
	if current := roster.ADNLIDs(); !slices.Equal(current, expectedADNL) {
		t.Fatalf("mutated immutable fast sync roster: %v", current)
	}
	got = roster.RootPublicKeyIDs()
	got[0] = fastSyncTestPeerID(0xcc)
	if current := roster.RootPublicKeyIDs(); !slices.Equal(current, expectedValidators) {
		t.Fatalf("mutated immutable fast sync root public key roster: %v", current)
	}
}

func TestFastSyncShardSetSelection(t *testing.T) {
	t.Parallel()

	requested := FastSyncShard{
		Workchain: 0,
		Shard:     testShardAncestor(t, 0x1234567890abcdef, 4),
	}
	parent := FastSyncShard{
		Workchain: requested.Workchain,
		Shard:     testShardAncestor(t, requested.Shard, 2),
	}
	top := FastSyncShard{Workchain: requested.Workchain, Shard: topShard}
	input := []FastSyncShard{top, parent, requested, parent}
	set := NewFastSyncShardSet(input)

	selected, err := set.Select(requested)
	if err != nil {
		t.Fatalf("select exact fast sync shard: %v", err)
	}
	if selected != requested {
		t.Fatalf("selected exact shard = %+v, want %+v", selected, requested)
	}

	parentSet := NewFastSyncShardSet([]FastSyncShard{top, parent})
	selected, err = parentSet.Select(requested)
	if err != nil {
		t.Fatalf("select ancestor fast sync shard: %v", err)
	}
	if selected != parent {
		t.Fatalf("selected ancestor shard = %+v, want nearest %+v", selected, parent)
	}

	otherWorkchain := requested
	otherWorkchain.Workchain = 1
	if _, err = set.Select(otherWorkchain); !errors.Is(err, ErrFastSyncNotFound) {
		t.Fatalf("other-workchain selection error = %v, want %v", err, ErrFastSyncNotFound)
	}
	if _, err = NewFastSyncShardSet(nil).Select(top); !errors.Is(err, ErrFastSyncNotFound) {
		t.Fatalf("missing top shard error = %v, want %v", err, ErrFastSyncNotFound)
	}
	invalid := FastSyncShard{Workchain: 0, Shard: 0}
	if _, err = NewFastSyncShardSet([]FastSyncShard{invalid}).Select(invalid); err == nil {
		t.Fatal("zero fast-sync shard should be rejected even when present in the set")
	}

	input[0] = FastSyncShard{Workchain: 9, Shard: topShard}
	got := set.Shards()
	got[0] = FastSyncShard{Workchain: 8, Shard: topShard}
	if current := set.Shards(); slices.Contains(current, got[0]) {
		t.Fatalf("mutated immutable fast sync shard set: %v", current)
	}
}

func fastSyncTestPeerID(value byte) PeerID {
	var id PeerID
	id[31] = value
	return id
}

func fastSyncTestPublicKey(value byte) FastSyncValidatorPublicKey {
	var key FastSyncValidatorPublicKey
	for i := range key {
		key[i] = value
	}
	return key
}

func fastSyncTestValidatorID(
	t *testing.T,
	publicKey FastSyncValidatorPublicKey,
) PeerID {
	t.Helper()

	raw, err := tl.Hash(keys.PublicKeyED25519{
		Key: ed25519.PublicKey(publicKey[:]),
	})
	if err != nil {
		t.Fatalf("build validator short id: %v", err)
	}
	id, err := NewPeerID(raw)
	if err != nil {
		t.Fatalf("parse validator short id: %v", err)
	}
	return id
}
