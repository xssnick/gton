package pebblestore

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

type startupMigrations struct {
	activeOrigin ton.BlockIDExt
}

func runStartupMigrations(db *pebble.DB, manifest cellGenerationManifest) (cellGenerationManifest, startupMigrations, error) {
	var applied startupMigrations

	origin, err := migrateActiveOriginFromCurrentState(db, manifest)
	if err != nil {
		return manifest, startupMigrations{}, err
	}
	if !isEmptyBlockID(origin) {
		manifest.activeOrigin = origin
		applied.activeOrigin = origin
	}

	return manifest, applied, nil
}

func migrateActiveOriginFromCurrentState(db *pebble.DB, manifest cellGenerationManifest) (ton.BlockIDExt, error) {
	if !isEmptyBlockID(manifest.activeOrigin) {
		return ton.BlockIDExt{}, nil
	}

	current, err := loadHotCurrentState(db)
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, nil
	}
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load current state for active origin migration: %w", err)
	}

	origin := current.Masterchain.Block
	if isEmptyBlockID(origin) {
		return ton.BlockIDExt{}, nil
	}

	manifest.activeOrigin = origin
	if err = db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("persist active origin migration: %w", err)
	}
	return origin, nil
}

func loadHotCurrentState(db *pebble.DB) (*storage.CurrentState, error) {
	raw, closer, err := pebbleReaderGet(db, hotKeyCurrentState())
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return decodeCurrentState(raw)
}
