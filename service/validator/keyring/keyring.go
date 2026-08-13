// Package keyring owns validator private keys and the signing primitive.
// Protocol packages construct their domain-separated payloads and depend on
// narrow signer interfaces satisfied by Keyring.
package keyring

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
)

var (
	// ErrNotFound reports that the requested validator signing key is absent.
	ErrNotFound = errors.New("keyring: key not found")
	// ErrReadOnly reports that a keyring made with New cannot be changed.
	ErrReadOnly = errors.New("keyring: persistent storage is not configured")
)

// StoredKey is the durable representation of one validator key. Seed is
// fixed-size so persistence implementations cannot retain caller-owned key
// buffers.
type StoredKey struct {
	ID                [32]byte
	Seed              [ed25519.SeedSize]byte
	Permanent         bool
	ElectionDate      uint32
	PermanentExpireAt uint32
	TempExpireAt      uint32
	HasADNL           bool
	ADNLID            [32]byte
	ADNLExpireAt      uint32
}

// KeyInfo is the public, non-secret metadata of one locally stored key.
type KeyInfo struct {
	ID                [32]byte
	Permanent         bool
	ElectionDate      uint32
	PermanentExpireAt uint32
	TempExpireAt      uint32
	HasADNL           bool
	ADNLID            [32]byte
	ADNLExpireAt      uint32
}

// Storage is the durable key contract consumed by a dynamic Keyring.
type Storage interface {
	// LoadValidatorKeys transfers ownership of the returned records, including
	// their seed bytes, to the caller.
	LoadValidatorKeys(ctx context.Context) ([]StoredKey, error)
	SaveValidatorKey(ctx context.Context, key StoredKey) error
}

type signingKey struct {
	private ed25519.PrivateKey
	public  ed25519.PublicKey
	info    KeyInfo
}

// Keyring owns current and future validator Ed25519 key pairs. New creates a
// static keyring; Open creates a dynamically managed keyring backed by
// durable storage. Both forms are safe for concurrent use.
type Keyring struct {
	storage Storage

	writeMu sync.Mutex
	mu      sync.RWMutex
	keys    map[[32]byte]signingKey
	order   [][32]byte
}

// New copies and owns all supplied Ed25519 validator private keys. Static
// keys are permanent immediately, preserving the original constructor's
// semantics.
func New(privateKeys ...ed25519.PrivateKey) (*Keyring, error) {
	if len(privateKeys) == 0 {
		return nil, errors.New("keyring: at least one private key is required")
	}

	keyring := newKeyring(nil, len(privateKeys))
	for i, privateKey := range privateKeys {
		if len(privateKey) != ed25519.PrivateKeySize {
			keyring.clearOwned()

			return nil, fmt.Errorf(
				"keyring: private key %d must be %d bytes, got %d",
				i,
				ed25519.PrivateKeySize,
				len(privateKey),
			)
		}

		ownedPrivateKey := append(ed25519.PrivateKey(nil), privateKey...)
		publicKey := ownedPrivateKey.Public().(ed25519.PublicKey)
		keyID, err := publicKeyID(publicKey)
		if err != nil {
			clear(ownedPrivateKey)
			keyring.clearOwned()

			return nil, err
		}
		if _, duplicate := keyring.keys[keyID]; duplicate {
			clear(ownedPrivateKey)
			keyring.clearOwned()

			return nil, fmt.Errorf("keyring: duplicate key %x", keyID)
		}

		keyring.keys[keyID] = signingKey{
			private: ownedPrivateKey,
			public:  append(ed25519.PublicKey(nil), publicKey...),
			info: KeyInfo{
				ID:        keyID,
				Permanent: true,
			},
		}
		keyring.order = append(keyring.order, keyID)
	}

	return keyring, nil
}

// Open loads every persisted key. An empty database yields an empty keyring;
// keys become available through Generate and become validator identities only
// after AddPermanent.
func Open(ctx context.Context, storage Storage) (*Keyring, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	storedKeys, err := storage.LoadValidatorKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("keyring: load validator keys: %w", err)
	}
	defer func() {
		for i := range storedKeys {
			clear(storedKeys[i].Seed[:])
		}
	}()

	keyring := newKeyring(storage, len(storedKeys))
	elections := make(map[uint32][32]byte, len(storedKeys))
	for i := range storedKeys {
		record, recordErr := signingKeyFromStored(storedKeys[i])
		if recordErr != nil {
			keyring.clearOwned()

			return nil, fmt.Errorf("keyring: load validator key %d: %w", i, recordErr)
		}
		if _, duplicate := keyring.keys[record.info.ID]; duplicate {
			clear(record.private)
			keyring.clearOwned()

			return nil, fmt.Errorf("keyring: duplicate persisted key %x", record.info.ID)
		}
		if record.info.Permanent {
			if existingID, duplicate := elections[record.info.ElectionDate]; duplicate {
				clear(record.private)
				keyring.clearOwned()

				return nil, fmt.Errorf(
					"keyring: election date %d belongs to both %x and %x",
					record.info.ElectionDate,
					existingID,
					record.info.ID,
				)
			}
			elections[record.info.ElectionDate] = record.info.ID
		}

		keyring.keys[record.info.ID] = record
		keyring.order = append(keyring.order, record.info.ID)
	}

	return keyring, nil
}

// Generate creates and durably stores a key before making it available for
// signing and public-key export.
func (k *Keyring) Generate(ctx context.Context) ([32]byte, error) {
	k.writeMu.Lock()
	defer k.writeMu.Unlock()

	if k.storage == nil {
		return [32]byte{}, ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return [32]byte{}, err
	}

	for {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return [32]byte{}, fmt.Errorf("keyring: generate validator key: %w", err)
		}
		keyID, err := publicKeyID(publicKey)
		if err != nil {
			clear(privateKey)

			return [32]byte{}, err
		}

		k.mu.RLock()
		_, duplicate := k.keys[keyID]
		k.mu.RUnlock()
		if duplicate {
			clear(privateKey)

			continue
		}

		record := signingKey{
			private: privateKey,
			public:  append(ed25519.PublicKey(nil), publicKey...),
			info:    KeyInfo{ID: keyID},
		}
		if err = k.save(ctx, record); err != nil {
			clear(privateKey)

			return [32]byte{}, err
		}

		k.mu.Lock()
		k.keys[keyID] = record
		k.order = append(k.order, keyID)
		k.mu.Unlock()

		return keyID, nil
	}
}

// AddPermanent activates a generated key as a validator identity and records
// its election interval metadata.
func (k *Keyring) AddPermanent(
	ctx context.Context,
	keyID [32]byte,
	electionDate uint32,
	expireAt uint32,
) error {
	k.writeMu.Lock()
	defer k.writeMu.Unlock()

	if k.storage == nil {
		return ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	k.mu.RLock()
	record, exists := k.keys[keyID]
	if exists {
		for otherID, other := range k.keys {
			if otherID != keyID && other.info.Permanent && other.info.ElectionDate == electionDate {
				k.mu.RUnlock()

				return fmt.Errorf(
					"keyring: election date %d already belongs to key %x",
					electionDate,
					otherID,
				)
			}
		}
	}
	k.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %x", ErrNotFound, keyID)
	}
	if record.info.Permanent && record.info.ElectionDate != electionDate {
		return fmt.Errorf(
			"keyring: permanent key %x already belongs to election date %d",
			keyID,
			record.info.ElectionDate,
		)
	}
	if record.info.Permanent && record.info.PermanentExpireAt == expireAt {
		return nil
	}

	record.info.Permanent = true
	record.info.ElectionDate = electionDate
	record.info.PermanentExpireAt = expireAt
	if err := k.save(ctx, record); err != nil {
		return err
	}

	k.mu.Lock()
	k.keys[keyID] = record
	k.mu.Unlock()

	return nil
}

// AddTemp binds the temporary-key lifetime expected by validator-engine. This
// implementation intentionally uses the permanent key itself as the temporary
// key, so both identifiers must match.
func (k *Keyring) AddTemp(
	ctx context.Context,
	permanentKeyID [32]byte,
	keyID [32]byte,
	expireAt uint32,
) error {
	if permanentKeyID != keyID {
		return errors.New("keyring: temporary key must match permanent key")
	}

	return k.update(ctx, permanentKeyID, func(record *signingKey) error {
		if !record.info.Permanent {
			return errors.New("keyring: temporary key requires a permanent key")
		}
		record.info.TempExpireAt = expireAt

		return nil
	})
}

// AddADNL associates the validator identity with its externally managed ADNL
// address. The ADNL private key remains owned by the node network stack.
func (k *Keyring) AddADNL(
	ctx context.Context,
	permanentKeyID [32]byte,
	adnlID [32]byte,
	expireAt uint32,
) error {
	return k.update(ctx, permanentKeyID, func(record *signingKey) error {
		if !record.info.Permanent {
			return errors.New("keyring: adnl address requires a permanent key")
		}
		if record.info.HasADNL && record.info.ADNLID != adnlID {
			return errors.New("keyring: permanent key already has another adnl address")
		}
		record.info.HasADNL = true
		record.info.ADNLID = adnlID
		record.info.ADNLExpireAt = expireAt

		return nil
	})
}

// Entries returns an ownership copy of every generated key's public metadata.
func (k *Keyring) Entries() []KeyInfo {
	k.mu.RLock()
	defer k.mu.RUnlock()

	entries := make([]KeyInfo, 0, len(k.order))
	for _, id := range k.order {
		entries = append(entries, k.keys[id].info)
	}

	return entries
}

// KeyIDs returns only permanent validator key identifiers.
func (k *Keyring) KeyIDs() [][32]byte {
	k.mu.RLock()
	defer k.mu.RUnlock()

	ids := make([][32]byte, 0, len(k.order))
	for _, id := range k.order {
		if k.keys[id].info.Permanent {
			ids = append(ids, id)
		}
	}

	return ids
}

// Sign signs an already domain-separated protocol payload with keyID. It does
// not retain or modify payload.
func (k *Keyring) Sign(keyID [32]byte, payload []byte) ([]byte, error) {
	k.mu.RLock()
	key, exists := k.keys[keyID]
	if !exists {
		k.mu.RUnlock()

		return nil, fmt.Errorf("%w: %x", ErrNotFound, keyID)
	}
	signature := ed25519.Sign(key.private, payload)
	k.mu.RUnlock()

	return signature, nil
}

// PublicKeyFor returns an ownership copy of the public key identified by
// keyID. Keeping lookup on the keyring lets protocol services bind signatures
// to a public identity without receiving private key material.
func (k *Keyring) PublicKeyFor(keyID [32]byte) (ed25519.PublicKey, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	key, exists := k.keys[keyID]
	if !exists {
		return nil, fmt.Errorf("%w: %x", ErrNotFound, keyID)
	}

	return append(ed25519.PublicKey(nil), key.public...), nil
}

func newKeyring(storage Storage, capacity int) *Keyring {
	return &Keyring{
		storage: storage,
		keys:    make(map[[32]byte]signingKey, capacity),
		order:   make([][32]byte, 0, capacity),
	}
}

func (k *Keyring) update(
	ctx context.Context,
	keyID [32]byte,
	apply func(*signingKey) error,
) error {
	k.writeMu.Lock()
	defer k.writeMu.Unlock()

	if k.storage == nil {
		return ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	k.mu.RLock()
	record, exists := k.keys[keyID]
	k.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %x", ErrNotFound, keyID)
	}
	previousInfo := record.info
	if err := apply(&record); err != nil {
		return err
	}
	if record.info == previousInfo {
		return nil
	}
	if err := k.save(ctx, record); err != nil {
		return err
	}

	k.mu.Lock()
	k.keys[keyID] = record
	k.mu.Unlock()

	return nil
}

func (k *Keyring) save(ctx context.Context, record signingKey) error {
	stored := storedKeyFromSigning(record)
	defer clear(stored.Seed[:])

	if err := k.storage.SaveValidatorKey(ctx, stored); err != nil {
		return fmt.Errorf("keyring: save validator key %x: %w", record.info.ID, err)
	}

	return nil
}

func signingKeyFromStored(stored StoredKey) (signingKey, error) {
	if !stored.Permanent {
		hasPermanentMetadata := stored.ElectionDate != 0 ||
			stored.PermanentExpireAt != 0 ||
			stored.TempExpireAt != 0 ||
			stored.HasADNL
		if hasPermanentMetadata {
			return signingKey{}, errors.New("non-permanent key has validator metadata")
		}
	}
	if !stored.HasADNL && (stored.ADNLID != [32]byte{} || stored.ADNLExpireAt != 0) {
		return signingKey{}, errors.New("key without adnl address has adnl metadata")
	}

	privateKey := ed25519.NewKeyFromSeed(stored.Seed[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID, err := publicKeyID(publicKey)
	if err != nil {
		clear(privateKey)

		return signingKey{}, err
	}
	if keyID != stored.ID {
		clear(privateKey)

		return signingKey{}, fmt.Errorf("stored key id %x does not match seed id %x", stored.ID, keyID)
	}

	return signingKey{
		private: privateKey,
		public:  append(ed25519.PublicKey(nil), publicKey...),
		info: KeyInfo{
			ID:                stored.ID,
			Permanent:         stored.Permanent,
			ElectionDate:      stored.ElectionDate,
			PermanentExpireAt: stored.PermanentExpireAt,
			TempExpireAt:      stored.TempExpireAt,
			HasADNL:           stored.HasADNL,
			ADNLID:            stored.ADNLID,
			ADNLExpireAt:      stored.ADNLExpireAt,
		},
	}, nil
}

func storedKeyFromSigning(record signingKey) StoredKey {
	stored := StoredKey{
		ID:                record.info.ID,
		Permanent:         record.info.Permanent,
		ElectionDate:      record.info.ElectionDate,
		PermanentExpireAt: record.info.PermanentExpireAt,
		TempExpireAt:      record.info.TempExpireAt,
		HasADNL:           record.info.HasADNL,
		ADNLID:            record.info.ADNLID,
		ADNLExpireAt:      record.info.ADNLExpireAt,
	}
	copy(stored.Seed[:], record.private.Seed())

	return stored
}

func (k *Keyring) clearOwned() {
	for id, key := range k.keys {
		clear(key.private)
		delete(k.keys, id)
	}
	k.order = k.order[:0]
}

func publicKeyID(publicKey ed25519.PublicKey) ([32]byte, error) {
	hash, err := tl.Hash(keys.PublicKeyED25519{Key: publicKey})
	if err != nil {
		return [32]byte{}, fmt.Errorf("keyring: hash validator public key: %w", err)
	}

	var keyID [32]byte
	copy(keyID[:], hash)

	return keyID, nil
}
