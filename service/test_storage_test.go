package service

import "github.com/xssnick/gton/service/storage"

// testStorage keeps broad fake embedding out of production contracts. Runtime
// components use their consumer-owned stores; tests that override one or two
// methods may embed this aggregate and fail immediately if an unexpected
// method is actually called.
type testStorage interface {
	storage.PeerServingStorage
	storage.PeerServingStorageWriter
	storage.PersistentStateFileStorage
	storage.StateStorage
	storage.BlockMetaStorage
	storage.CellStorage
	storage.MaintenanceStorage
	storage.PersistentStateSerializationStorage
	storage.CellGenerationStorage
}
