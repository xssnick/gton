package keyring_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/validator/keyring"
	"github.com/xssnick/gton/service/validator/pebblestore"
)

func TestPersistentKeyringLifecycle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	firstStore := openValidatorStore(t, dir)
	first, err := keyring.Open(t.Context(), firstStore.Validator())
	if err != nil {
		t.Fatal(err)
	}
	if entries := first.Entries(); len(entries) != 0 {
		t.Fatalf("empty keyring entries = %d, want 0", len(entries))
	}
	if ids := first.KeyIDs(); len(ids) != 0 {
		t.Fatalf("empty keyring ids = %d, want 0", len(ids))
	}

	keyID, err := first.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := first.PublicKeyFor(keyID)
	if err != nil {
		t.Fatal(err)
	}
	if ids := first.KeyIDs(); len(ids) != 0 {
		t.Fatalf("generated key ids = %x, want no permanent ids", ids)
	}
	payload := []byte("generated key payload")
	signature, err := first.Sign(keyID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(publicKey, payload, signature) {
		t.Fatal("generated key signature does not verify")
	}
	if err = firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	secondStore := openValidatorStore(t, dir)
	second, err := keyring.Open(t.Context(), secondStore.Validator())
	if err != nil {
		t.Fatal(err)
	}
	reopenedPublicKey, err := second.PublicKeyFor(keyID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reopenedPublicKey, publicKey) {
		t.Fatalf("reopened public key = %x, want %x", reopenedPublicKey, publicKey)
	}
	if ids := second.KeyIDs(); len(ids) != 0 {
		t.Fatalf("reopened generated key ids = %x, want no permanent ids", ids)
	}
	if err = second.AddTemp(t.Context(), keyID, keyID, 300); err == nil {
		t.Fatal("temporary metadata accepted before permanent activation")
	}

	const (
		electionDate = uint32(100)
		permanentTTL = uint32(200)
		tempTTL      = uint32(300)
		adnlTTL      = uint32(400)
	)
	var adnlID [32]byte
	for i := range adnlID {
		adnlID[i] = byte(i + 1)
	}
	if err = second.AddPermanent(t.Context(), keyID, electionDate, permanentTTL); err != nil {
		t.Fatal(err)
	}
	if err = second.AddTemp(t.Context(), keyID, keyID, tempTTL); err != nil {
		t.Fatal(err)
	}
	if err = second.AddADNL(t.Context(), keyID, adnlID, adnlTTL); err != nil {
		t.Fatal(err)
	}
	assertPersistentMetadata(t, second, keyID, adnlID)
	if err = secondStore.Close(); err != nil {
		t.Fatal(err)
	}

	thirdStore := openValidatorStore(t, dir)
	third, err := keyring.Open(t.Context(), thirdStore.Validator())
	if err != nil {
		t.Fatal(err)
	}
	assertPersistentMetadata(t, third, keyID, adnlID)
	signature, err = third.Sign(keyID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(publicKey, payload, signature) {
		t.Fatal("reopened permanent key signature does not verify")
	}
}

func TestPersistentKeyringPublishesOnlyAfterSave(t *testing.T) {
	t.Parallel()

	storage := &memoryKeyStorage{}
	keys, err := keyring.Open(t.Context(), storage)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := keys.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	storage.failNext(errors.New("disk unavailable"))
	if err = keys.AddPermanent(t.Context(), keyID, 10, 20); err == nil {
		t.Fatal("permanent activation succeeded despite storage failure")
	}
	if ids := keys.KeyIDs(); len(ids) != 0 {
		t.Fatalf("ids after failed save = %x, want none", ids)
	}
	entries := keys.Entries()
	if len(entries) != 1 || entries[0].Permanent {
		t.Fatalf("entry after failed save = %+v, want generated-only key", entries)
	}
}

func TestPersistentKeyringRejectsStoredIDMismatch(t *testing.T) {
	t.Parallel()

	seed := bytes.Repeat([]byte{0x73}, ed25519.SeedSize)
	static, err := keyring.New(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatal(err)
	}
	stored := keyring.StoredKey{
		ID:   static.KeyIDs()[0],
		Seed: [ed25519.SeedSize]byte(seed),
	}
	stored.ID[0] ^= 0xff
	storage := &memoryKeyStorage{keys: []keyring.StoredKey{stored}}

	if _, err = keyring.Open(t.Context(), storage); err == nil {
		t.Fatal("stored key with mismatched id was accepted")
	}
}

func TestPersistentKeyringRequiresMatchingTempID(t *testing.T) {
	t.Parallel()

	storage := &memoryKeyStorage{}
	keys, err := keyring.Open(t.Context(), storage)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := keys.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = keys.AddPermanent(t.Context(), keyID, 10, 20); err != nil {
		t.Fatal(err)
	}

	tempID := keyID
	tempID[0] ^= 0xff
	if err = keys.AddTemp(t.Context(), keyID, tempID, 30); err == nil {
		t.Fatal("distinct temporary key id was accepted")
	}
}

func TestPersistentKeyringElectionAndAssociationUpdates(t *testing.T) {
	t.Parallel()

	storage := &memoryKeyStorage{}
	keys, err := keyring.Open(t.Context(), storage)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := keys.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := keys.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if err = keys.AddPermanent(t.Context(), firstID, 100, 200); err != nil {
		t.Fatal(err)
	}
	if err = keys.AddPermanent(t.Context(), firstID, 100, 200); err != nil {
		t.Fatalf("idempotent permanent update: %v", err)
	}
	if err = keys.AddPermanent(t.Context(), firstID, 101, 200); err == nil {
		t.Fatal("same key was assigned to another election")
	}
	if err = keys.AddPermanent(t.Context(), secondID, 100, 200); err == nil {
		t.Fatal("another key was assigned to the same election")
	}
	if err = keys.AddPermanent(t.Context(), firstID, 100, 250); err != nil {
		t.Fatalf("update permanent expiry: %v", err)
	}

	if err = keys.AddTemp(t.Context(), firstID, firstID, 300); err != nil {
		t.Fatal(err)
	}
	if err = keys.AddTemp(t.Context(), firstID, firstID, 300); err != nil {
		t.Fatalf("idempotent temporary update: %v", err)
	}
	if err = keys.AddTemp(t.Context(), firstID, firstID, 350); err != nil {
		t.Fatalf("update temporary expiry: %v", err)
	}

	firstADNL := [32]byte{0xa1}
	if err = keys.AddADNL(t.Context(), firstID, firstADNL, 400); err != nil {
		t.Fatal(err)
	}
	if err = keys.AddADNL(t.Context(), firstID, firstADNL, 400); err != nil {
		t.Fatalf("idempotent adnl update: %v", err)
	}
	if err = keys.AddADNL(t.Context(), firstID, firstADNL, 450); err != nil {
		t.Fatalf("update adnl expiry: %v", err)
	}
	if err = keys.AddADNL(t.Context(), firstID, [32]byte{0xa2}, 450); err == nil {
		t.Fatal("permanent key was assigned another adnl address")
	}

	entries := keys.Entries()
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	if entries[0] != (keyring.KeyInfo{
		ID:                firstID,
		Permanent:         true,
		ElectionDate:      100,
		PermanentExpireAt: 250,
		TempExpireAt:      350,
		HasADNL:           true,
		ADNLID:            firstADNL,
		ADNLExpireAt:      450,
	}) {
		t.Fatalf("first entry = %+v", entries[0])
	}
	if entries[1] != (keyring.KeyInfo{ID: secondID}) {
		t.Fatalf("second entry = %+v, want generated-only key", entries[1])
	}
}

func TestPersistentKeyringRejectsDuplicateStoredElection(t *testing.T) {
	t.Parallel()

	storage := &memoryKeyStorage{}
	keys, err := keyring.Open(t.Context(), storage)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := keys.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := keys.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	storage.mu.Lock()
	for i := range storage.keys {
		if storage.keys[i].ID == firstID || storage.keys[i].ID == secondID {
			storage.keys[i].Permanent = true
			storage.keys[i].ElectionDate = 100
			storage.keys[i].PermanentExpireAt = 200
		}
	}
	storage.mu.Unlock()

	if _, err = keyring.Open(t.Context(), storage); err == nil {
		t.Fatal("duplicate stored election was accepted")
	}
}

func TestPersistentKeyringConcurrentReadsAndUpdates(t *testing.T) {
	t.Parallel()

	storage := &memoryKeyStorage{}
	keys, err := keyring.Open(t.Context(), storage)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := keys.Generate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = keys.AddPermanent(t.Context(), keyID, 10, 20); err != nil {
		t.Fatal(err)
	}
	publicKey, err := keys.PublicKeyFor(keyID)
	if err != nil {
		t.Fatal(err)
	}

	var adnlID [32]byte
	adnlID[0] = 0x91
	errorsFound := make(chan error, 16)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 100 {
				payload := []byte("concurrent payload")
				signature, signErr := keys.Sign(keyID, payload)
				if signErr != nil {
					errorsFound <- signErr

					return
				}
				if !ed25519.Verify(publicKey, payload, signature) {
					errorsFound <- errors.New("concurrent signature does not verify")

					return
				}
				_ = keys.KeyIDs()
				_ = keys.Entries()
			}
		})
	}
	for worker := range 4 {
		workers.Go(func() {
			for iteration := range 25 {
				ttl := uint32(1000 + worker*25 + iteration)
				if updateErr := keys.AddTemp(t.Context(), keyID, keyID, ttl); updateErr != nil {
					errorsFound <- updateErr

					return
				}
				if updateErr := keys.AddADNL(t.Context(), keyID, adnlID, ttl); updateErr != nil {
					errorsFound <- updateErr

					return
				}
			}
		})
	}
	workers.Wait()
	close(errorsFound)
	for workerErr := range errorsFound {
		t.Error(workerErr)
	}
}

func openValidatorStore(t *testing.T, dir string) *pebblestore.Store {
	t.Helper()

	store, err := pebblestore.Open(pebblestore.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close validator store: %v", closeErr)
		}
	})

	return store
}

func assertPersistentMetadata(t *testing.T, keys *keyring.Keyring, keyID, adnlID [32]byte) {
	t.Helper()

	ids := keys.KeyIDs()
	if len(ids) != 1 || ids[0] != keyID {
		t.Fatalf("permanent ids = %x, want %x", ids, keyID)
	}
	entries := keys.Entries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	want := keyring.KeyInfo{
		ID:                keyID,
		Permanent:         true,
		ElectionDate:      100,
		PermanentExpireAt: 200,
		TempExpireAt:      300,
		HasADNL:           true,
		ADNLID:            adnlID,
		ADNLExpireAt:      400,
	}
	if entries[0] != want {
		t.Fatalf("entry = %+v, want %+v", entries[0], want)
	}
}

type memoryKeyStorage struct {
	mu        sync.Mutex
	keys      []keyring.StoredKey
	nextError error
}

func (s *memoryKeyStorage) LoadValidatorKeys(ctx context.Context) ([]keyring.StoredKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]keyring.StoredKey(nil), s.keys...), nil
}

func (s *memoryKeyStorage) SaveValidatorKey(ctx context.Context, key keyring.StoredKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.nextError != nil {
		err := s.nextError
		s.nextError = nil

		return err
	}
	for i := range s.keys {
		if s.keys[i].ID == key.ID {
			s.keys[i] = key

			return nil
		}
	}
	s.keys = append(s.keys, key)

	return nil
}

func (s *memoryKeyStorage) failNext(err error) {
	s.mu.Lock()
	s.nextError = err
	s.mu.Unlock()
}
