package keyring_test

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"strconv"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/keyring"
)

func TestKeyringSignsConsumerPayload(t *testing.T) {
	t.Parallel()

	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	keys, err := keyring.New(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("domain-separated validator payload")
	keyIDs := keys.KeyIDs()
	if len(keyIDs) != 1 {
		t.Fatalf("key ids = %d, want 1", len(keyIDs))
	}
	publicKey, err := keys.PublicKeyFor(keyIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	clear(privateKey)

	signature, err := keys.Sign(keyIDs[0], payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(publicKey, payload, signature) {
		t.Fatal("signature does not verify with the keyring public key")
	}
}

func TestKeyringRejectsUnknownKey(t *testing.T) {
	t.Parallel()

	seed := bytes.Repeat([]byte{0x35}, ed25519.SeedSize)
	keys, err := keyring.New(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatal(err)
	}

	unknown := keys.KeyIDs()[0]
	unknown[0] ^= 0xff
	if _, err = keys.Sign(unknown, []byte("payload")); !errors.Is(err, keyring.ErrNotFound) {
		t.Fatalf("sign error = %v, want ErrNotFound", err)
	}
}

func TestKeyringKeyIDMatchesValidatorRoster(t *testing.T) {
	t.Parallel()

	seed := bytes.Repeat([]byte{0x61}, ed25519.SeedSize)
	keys, err := keyring.New(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatal(err)
	}

	var publicKey [32]byte
	resolved, err := keys.PublicKeyFor(keys.KeyIDs()[0])
	if err != nil {
		t.Fatal(err)
	}
	copy(publicKey[:], resolved)
	want, err := groups.PublicKeyHash(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := keys.KeyIDs()[0]; got != want {
		t.Fatalf("key id = %x, want %x", got, want)
	}
}

func TestKeyringOwnsPublicKey(t *testing.T) {
	t.Parallel()

	seed := bytes.Repeat([]byte{0x24}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	keys, err := keyring.New(privateKey)
	if err != nil {
		t.Fatal(err)
	}

	keyID := keys.KeyIDs()[0]
	first, err := keys.PublicKeyFor(keyID)
	if err != nil {
		t.Fatal(err)
	}
	first[0] ^= 0xff
	second, err := keys.PublicKeyFor(keyID)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("public key mutation escaped into the keyring")
	}

	firstID := keys.KeyIDs()
	firstID[0][0] ^= 0xff
	secondID := keys.KeyIDs()
	if firstID[0] == secondID[0] {
		t.Fatal("key id mutation did not change the caller-owned value")
	}
}

func TestKeyringPublicKeyFor(t *testing.T) {
	seed := bytes.Repeat([]byte{0x52}, ed25519.SeedSize)
	keys, err := keyring.New(ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatal(err)
	}
	keyID := keys.KeyIDs()[0]

	publicKey, err := keys.PublicKeyFor(keyID)
	if err != nil {
		t.Fatalf("lookup public key: %v", err)
	}
	want, err := keys.PublicKeyFor(keyID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(publicKey, want) {
		t.Fatalf("public key = %x, want %x", publicKey, want)
	}

	publicKey[0] ^= 0xff
	again, err := keys.PublicKeyFor(keyID)
	if err != nil {
		t.Fatalf("lookup public key again: %v", err)
	}
	if bytes.Equal(publicKey, again) {
		t.Fatal("public key lookup returned keyring-owned storage")
	}

	unknown := keyID
	unknown[0] ^= 0xff
	if _, err = keys.PublicKeyFor(unknown); !errors.Is(err, keyring.ErrNotFound) {
		t.Fatalf("unknown public key error = %v, want ErrNotFound", err)
	}
}

func TestKeyringRejectsInvalidPrivateKey(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, ed25519.PrivateKeySize - 1, ed25519.PrivateKeySize + 1} {
		size := size
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()

			if _, err := keyring.New(make([]byte, size)); err == nil {
				t.Fatalf("accepted %d-byte private key", size)
			}
		})
	}
}

func TestKeyringSupportsCurrentAndFutureKeys(t *testing.T) {
	t.Parallel()

	first := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x11}, ed25519.SeedSize))
	second := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x22}, ed25519.SeedSize))
	keys, err := keyring.New(first, second)
	if err != nil {
		t.Fatal(err)
	}
	clear(first)
	clear(second)

	ids := keys.KeyIDs()
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("key ids = %x, want two distinct ids", ids)
	}
	for _, id := range ids {
		publicKey, err := keys.PublicKeyFor(id)
		if err != nil {
			t.Fatal(err)
		}
		signature, err := keys.Sign(id, []byte("rotation payload"))
		if err != nil {
			t.Fatal(err)
		}
		if !ed25519.Verify(publicKey, []byte("rotation payload"), signature) {
			t.Fatalf("signature for %x does not verify", id)
		}
	}
}

func TestKeyringRejectsDuplicateKeys(t *testing.T) {
	t.Parallel()

	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x33}, ed25519.SeedSize))
	defer clear(privateKey)
	if _, err := keyring.New(privateKey, privateKey); err == nil {
		t.Fatal("duplicate key was accepted")
	}
}
