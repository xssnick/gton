package pebblestore

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/validator/keyring"
)

func TestValidatorKeyStorageRoundTrip(t *testing.T) {
	t.Parallel()

	store := openTestStore(t, t.TempDir())
	record := testStoredValidatorKey(0x31)
	if err := store.Validator().SaveValidatorKey(t.Context(), record); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Validator().LoadValidatorKeys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0] != record {
		t.Fatalf("loaded keys = %+v, want %+v", loaded, record)
	}

	record.Permanent = true
	record.ElectionDate = 123
	record.PermanentExpireAt = 456
	record.TempExpireAt = 457
	record.HasADNL = true
	record.ADNLExpireAt = 458
	for i := range record.ADNLID {
		record.ADNLID[i] = byte(0xa0 + i)
	}
	if err = store.Validator().SaveValidatorKey(t.Context(), record); err != nil {
		t.Fatal(err)
	}

	loaded, err = store.Validator().LoadValidatorKeys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0] != record {
		t.Fatalf("updated keys = %+v, want %+v", loaded, record)
	}
}

func TestValidatorKeyStorageRejectsSeedReplacement(t *testing.T) {
	t.Parallel()

	store := openTestStore(t, t.TempDir())
	record := testStoredValidatorKey(0x41)
	if err := store.Validator().SaveValidatorKey(t.Context(), record); err != nil {
		t.Fatal(err)
	}

	replacement := record
	replacement.Seed[0] ^= 0xff
	if err := store.Validator().SaveValidatorKey(t.Context(), replacement); err == nil {
		t.Fatal("validator key seed replacement was accepted")
	}
	loaded, err := store.Validator().LoadValidatorKeys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0] != record {
		t.Fatalf("loaded keys after replacement = %+v, want %+v", loaded, record)
	}
}

func TestValidatorKeyStorageReportsCorruption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value []byte
	}{
		{
			name:  "short value",
			value: []byte{validatorKeyVersion},
		},
		{
			name: "unknown version",
			value: func() []byte {
				value := make([]byte, validatorKeyValueSize)
				value[0] = validatorKeyVersion + 1

				return value
			}(),
		},
		{
			name: "unknown flags",
			value: func() []byte {
				value := make([]byte, validatorKeyValueSize)
				value[0] = validatorKeyVersion
				value[1] = 0x80

				return value
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := openTestStore(t, t.TempDir())
			id := [32]byte{0x51}
			if err := store.db.Set(validatorKeyKey(id), tt.value, pebble.Sync); err != nil {
				t.Fatal(err)
			}

			if _, err := store.Validator().LoadValidatorKeys(t.Context()); err == nil {
				t.Fatal("corrupt validator key was accepted")
			}
		})
	}
}

func TestValidatorKeyStorageHonorsCancelledAdmission(t *testing.T) {
	t.Parallel()

	store := openTestStore(t, t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := store.Validator().SaveValidatorKey(ctx, testStoredValidatorKey(0x61))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("save error = %v, want context.Canceled", err)
	}
	if _, err = store.Validator().LoadValidatorKeys(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("load error = %v, want context.Canceled", err)
	}
	loaded, err := store.Validator().LoadValidatorKeys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("loaded keys = %d, want 0", len(loaded))
	}
}

func testStoredValidatorKey(fill byte) keyring.StoredKey {
	key := keyring.StoredKey{ID: [32]byte{fill}}
	for i := range key.Seed {
		key.Seed[i] = fill + byte(i)
	}

	return key
}
