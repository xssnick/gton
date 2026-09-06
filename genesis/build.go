package genesis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type Paths struct {
	Genesis      string
	Data         string
	GlobalConfig string
	Lock         string
}

type Result struct {
	Created     bool
	GenesisTime uint32
	Masterchain ton.BlockIDExt
	Basechain   ton.BlockIDExt
	Wallet      string
}

type Lock struct {
	FormatVersion int               `json:"format_version"`
	SourceSHA256  string            `json:"source_sha256"`
	GenesisTime   uint32            `json:"genesis_time"`
	GlobalID      int32             `json:"global_id"`
	Masterchain   LockedBlock       `json:"masterchain"`
	Basechain     LockedBlock       `json:"basechain"`
	Addresses     LockedAddresses   `json:"addresses"`
	Validators    []LockedValidator `json:"validators"`
}

type LockedBlock struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
}

type LockedAddresses struct {
	Wallet            string `json:"wallet"`
	Elector           string `json:"elector"`
	Config            string `json:"config"`
	ValidatorRegistry string `json:"validator_registry,omitempty"`
}

type LockedValidator struct {
	PublicKey string `json:"public_key"`
	ADNLID    string `json:"adnl_id"`
	KeyID     string `json:"key_id"`
	Weight    uint64 `json:"weight"`
}

func DefaultPaths() Paths {
	return Paths{
		Genesis:      DefaultGenesisPath,
		Data:         DefaultDataPath,
		GlobalConfig: DefaultGlobalConfigPath,
		Lock:         DefaultLockPath,
	}
}

func Generate(ctx context.Context, paths Paths, logger zerolog.Logger) (Result, error) {
	paths = normalizePaths(paths)
	if err := validatePaths(paths); err != nil {
		return Result{}, err
	}
	spec, _, err := LoadSpec(paths.Genesis)
	if err != nil {
		return Result{}, fmt.Errorf("load %s: %w", paths.Genesis, err)
	}
	validated, err := validateSpec(spec)
	if err != nil {
		return Result{}, err
	}
	canonical, err := json.Marshal(spec)
	if err != nil {
		return Result{}, err
	}
	sourceHash := sha256.Sum256(canonical)

	existingLock, lockExists, err := readLock(paths.Lock)
	if err != nil {
		return Result{}, err
	}
	genesisTime := spec.GenesisTime
	if lockExists {
		if existingLock.FormatVersion != FormatVersion {
			return Result{}, fmt.Errorf("%s has unsupported format_version %d", paths.Lock, existingLock.FormatVersion)
		}
		if existingLock.SourceSHA256 != hex.EncodeToString(sourceHash[:]) {
			return Result{}, fmt.Errorf("%s belongs to a different genesis.json", paths.Lock)
		}
		if genesisTime != 0 && genesisTime != existingLock.GenesisTime {
			return Result{}, fmt.Errorf("%s genesis_time differs from genesis.json", paths.Lock)
		}
		genesisTime = existingLock.GenesisTime
	}
	if genesisTime == 0 {
		now := time.Now().Unix()
		if now <= 0 || now > int64(^uint32(0)) {
			return Result{}, fmt.Errorf("current unix time %d does not fit uint32", now)
		}
		genesisTime, err = automaticGenesisTime(uint32(now), validated.spec.Consensus.MasterGroupLifetime)
		if err != nil {
			return Result{}, err
		}
	}
	if genesisTime > uint32(time.Now().Add(time.Minute).Unix()) {
		return Result{}, fmt.Errorf("genesis_time %d is more than one minute in the future", genesisTime)
	}

	built, err := buildStates(validated, genesisTime)
	if err != nil {
		return Result{}, err
	}
	lock, err := buildLock(validated, sourceHash, genesisTime, built)
	if err != nil {
		return Result{}, err
	}
	lockData, err := marshalJSON(lock)
	if err != nil {
		return Result{}, err
	}
	globalData, err := marshalJSON(buildGlobalConfig(spec, built.master.block))
	if err != nil {
		return Result{}, err
	}

	if lockExists && !locksEqual(existingLock, lock) {
		return Result{}, fmt.Errorf("%s does not match the generated state", paths.Lock)
	}
	globalExists, err := verifyExistingFile(paths.GlobalConfig, globalData)
	if err != nil {
		return Result{}, err
	}
	dataExists, err := verifyExistingData(ctx, paths.Data, built)
	if err != nil {
		return Result{}, err
	}

	created := false
	// The lock resolves an omitted genesis_time. Publish it first so an
	// interrupted first run can deterministically complete on the next run.
	if !lockExists {
		if err = writeAtomicFile(paths.Lock, lockData, 0o644); err != nil {
			return Result{}, fmt.Errorf("write genesis lock: %w", err)
		}
		created = true
	}
	if !dataExists {
		logger.Info().Str("data", paths.Data).Msg("building genesis node database")
		if err = createDataAtomically(ctx, paths.Data, built); err != nil {
			return Result{}, err
		}
		created = true
	}
	if !globalExists {
		if err = writeAtomicFile(paths.GlobalConfig, globalData, 0o644); err != nil {
			return Result{}, fmt.Errorf("write global config: %w", err)
		}
		created = true
	}

	return Result{
		Created:     created,
		GenesisTime: genesisTime,
		Masterchain: built.master.block,
		Basechain:   built.base.block,
		Wallet:      lock.Addresses.Wallet,
	}, nil
}

// automaticGenesisTime places an automatically generated zero state at the
// beginning of the active masterchain validator-set lifetime. This gives a
// freshly started network the maximum remaining interval before its first
// catchain rotation while keeping the zero state valid at node start.
//
// Explicit genesis times are protocol input and intentionally bypass this
// policy. A future zero state needs a runtime startup barrier; Generate does
// not manufacture one, so automatic generation never moves time forward.
func automaticGenesisTime(now, lifetime uint32) (uint32, error) {
	if lifetime == 0 {
		return 0, errors.New("automatic genesis time requires a masterchain group lifetime")
	}
	return now - now%lifetime, nil
}

func normalizePaths(paths Paths) Paths {
	defaults := DefaultPaths()
	if paths.Genesis == "" {
		paths.Genesis = defaults.Genesis
	}
	if paths.Data == "" {
		paths.Data = defaults.Data
	}
	if paths.GlobalConfig == "" {
		paths.GlobalConfig = defaults.GlobalConfig
	}
	if paths.Lock == "" {
		paths.Lock = defaults.Lock
	}
	paths.Genesis = filepath.Clean(paths.Genesis)
	paths.Data = filepath.Clean(paths.Data)
	paths.GlobalConfig = filepath.Clean(paths.GlobalConfig)
	paths.Lock = filepath.Clean(paths.Lock)
	return paths
}

func validatePaths(paths Paths) error {
	files := []struct {
		name string
		path string
	}{
		{name: "genesis", path: paths.Genesis},
		{name: "global config", path: paths.GlobalConfig},
		{name: "lock", path: paths.Lock},
	}
	absFiles := make([]string, len(files))
	for i := range files {
		path, err := filepath.Abs(files[i].path)
		if err != nil {
			return fmt.Errorf("resolve %s path: %w", files[i].name, err)
		}
		absFiles[i] = filepath.Clean(path)
	}
	data, err := filepath.Abs(paths.Data)
	if err != nil {
		return fmt.Errorf("resolve data path: %w", err)
	}
	data = filepath.Clean(data)

	for i := range absFiles {
		for j := i + 1; j < len(absFiles); j++ {
			if absFiles[i] == absFiles[j] {
				return fmt.Errorf("%s and %s paths must differ", files[i].name, files[j].name)
			}
		}
		if pathsOverlap(data, absFiles[i]) {
			return fmt.Errorf("data and %s paths must not contain one another", files[i].name)
		}
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	if left == right {
		return true
	}
	leftToRight, err := filepath.Rel(left, right)
	if err == nil && leftToRight != ".." && !strings.HasPrefix(leftToRight, ".."+string(filepath.Separator)) {
		return true
	}
	rightToLeft, err := filepath.Rel(right, left)
	return err == nil && rightToLeft != ".." && !strings.HasPrefix(rightToLeft, ".."+string(filepath.Separator))
}

func buildGlobalConfig(spec Spec, master ton.BlockIDExt) liteclient.GlobalConfig {
	return liteclient.GlobalConfig{
		Type: "config.global",
		DHT: liteclient.DHTConfig{
			Type:        "dht.config.global",
			K:           6,
			A:           3,
			StaticNodes: liteclient.DHTNodes{Type: "dht.nodes", Nodes: spec.DHTNodes},
		},
		Liteservers: spec.Liteservers,
		Validator: liteclient.ValidatorConfig{
			Type:      "validator.config.global",
			ZeroState: configBlock(master),
			InitBlock: configBlock(master),
			Hardforks: []liteclient.ConfigBlock{},
		},
	}
}

func configBlock(block ton.BlockIDExt) liteclient.ConfigBlock {
	return liteclient.ConfigBlock{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo,
		RootHash:  block.RootHash,
		FileHash:  block.FileHash,
	}
}

func buildLock(spec validatedSpec, sourceHash [32]byte, genesisTime uint32, built builtGenesis) (Lock, error) {
	validators := make([]LockedValidator, len(spec.validators))
	for i, validator := range spec.validators {
		keyID, err := tl.Hash(keys.PublicKeyED25519{Key: validator.publicKey[:]})
		if err != nil {
			return Lock{}, err
		}
		validators[i] = LockedValidator{
			PublicKey: base64.StdEncoding.EncodeToString(validator.publicKey[:]),
			ADNLID:    base64.StdEncoding.EncodeToString(validator.adnlID[:]),
			KeyID:     base64.StdEncoding.EncodeToString(keyID),
			Weight:    validator.weight,
		}
	}
	registryAddress := ""
	if built.validatorRegistry != nil {
		registryAddress = built.validatorRegistry.Bounce(true).Testnet(true).String()
	}
	return Lock{
		FormatVersion: FormatVersion,
		SourceSHA256:  hex.EncodeToString(sourceHash[:]),
		GenesisTime:   genesisTime,
		GlobalID:      spec.spec.GlobalID,
		Masterchain:   lockedBlock(built.master.block),
		Basechain:     lockedBlock(built.base.block),
		Addresses: LockedAddresses{
			Wallet:            built.wallet.Bounce(true).Testnet(true).String(),
			Elector:           friendlyAddress(electorAddress),
			Config:            friendlyAddress(configAddress),
			ValidatorRegistry: registryAddress,
		},
		Validators: validators,
	}, nil
}

func lockedBlock(block ton.BlockIDExt) LockedBlock {
	return LockedBlock{
		Workchain: block.Workchain,
		Shard:     fmt.Sprintf("%016x", uint64(block.Shard)),
		Seqno:     block.SeqNo,
		RootHash:  hex.EncodeToString(block.RootHash),
		FileHash:  hex.EncodeToString(block.FileHash),
	}
}

func friendlyAddress(data [32]byte) string {
	return address.NewAddress(0, 0xff, data[:]).Bounce(true).Testnet(true).String()
}

func readLock(path string) (Lock, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Lock{}, false, nil
	}
	if err != nil {
		return Lock{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	var lock Lock
	if err = json.Unmarshal(raw, &lock); err != nil {
		return Lock{}, false, fmt.Errorf("decode %s: %w", path, err)
	}
	return lock, true, nil
}

func locksEqual(left, right Lock) bool {
	return left.FormatVersion == right.FormatVersion &&
		left.SourceSHA256 == right.SourceSHA256 &&
		left.GenesisTime == right.GenesisTime &&
		left.GlobalID == right.GlobalID &&
		left.Masterchain == right.Masterchain &&
		left.Basechain == right.Basechain &&
		left.Addresses == right.Addresses &&
		slices.Equal(left.Validators, right.Validators)
}

func verifyExistingFile(path string, expected []byte) (bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !bytes.Equal(raw, expected) {
		return false, fmt.Errorf("existing %s does not match this genesis", path)
	}
	return true, nil
}

func verifyExistingData(ctx context.Context, path string, built builtGenesis) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("data path %s is not a directory", path)
	}
	if err = verifyData(ctx, path, built); err != nil {
		return false, fmt.Errorf("existing data directory %s does not match this genesis: %w", path, err)
	}
	return true, nil
}

func createDataAtomically(ctx context.Context, target string, built builtGenesis) error {
	abs, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	parent := filepath.Dir(abs)
	if err = os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, "."+filepath.Base(abs)+".genesis-")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(stage)
		}
	}()

	// Pebble emits detailed per-database recovery and compaction logs. They are
	// useful for a long-running node but drown the genesis command's one-shot
	// progress and final hashes, so keep storage internals silent here.
	store, err := openGenesisStore(stage, false, zerolog.Nop())
	if err != nil {
		return err
	}
	closeStore := func() error {
		if store == nil {
			return nil
		}
		err := store.Close()
		store = nil
		return err
	}
	defer func() { _ = closeStore() }()

	for _, artifact := range []stateArtifact{built.master, built.base} {
		if err = store.SaveZeroState(artifact.block, artifact.boc, nil); err != nil {
			return fmt.Errorf("save zerostate %s: %w", storage.FormatBlockRef(artifact.block), err)
		}
	}
	master, err := storage.ParseStateBOC(&built.master.block, built.master.boc, built.master.block.RootHash, built.master.block.FileHash)
	if err != nil {
		return err
	}
	base, err := storage.ParseStateBOC(&built.base.block, built.base.boc, built.base.block.RootHash, built.base.block.FileHash)
	if err != nil {
		return err
	}
	current := &storage.CurrentState{
		SyncedAt:    time.Unix(int64(master.Parsed.GenUTime), 0).UTC(),
		Masterchain: *master,
		Shards:      make(map[storage.ShardKey]storage.BlockState),
	}
	if err = store.SaveZeroStateCheckpoint(ctx, []*storage.BlockState{master, base}, current); err != nil {
		return fmt.Errorf("save genesis checkpoint: %w", err)
	}
	if err = closeStore(); err != nil {
		return fmt.Errorf("close staged genesis database: %w", err)
	}
	if err = verifyData(ctx, stage, built); err != nil {
		return fmt.Errorf("verify staged genesis database: %w", err)
	}
	if err = os.Rename(stage, abs); err != nil {
		return fmt.Errorf("publish genesis database: %w", err)
	}
	if err = syncDirectory(parent); err != nil {
		return fmt.Errorf("sync genesis database parent: %w", err)
	}
	committed = true
	return nil
}

func verifyData(ctx context.Context, path string, built builtGenesis) error {
	store, err := openGenesisStore(path, true, zerolog.Nop())
	if err != nil {
		return err
	}
	defer store.Close()

	blocks, err := store.StoredZeroStateBlocks(ctx)
	if err != nil {
		return err
	}
	if len(blocks) != 2 || !containsBlock(blocks, built.master.block) || !containsBlock(blocks, built.base.block) {
		return fmt.Errorf("stored zerostate set is not the generated masterchain and basechain pair")
	}
	for _, artifact := range []stateArtifact{built.master, built.base} {
		raw, err := store.ZeroState(ctx, artifact.block)
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, artifact.boc) {
			return fmt.Errorf("zerostate artifact %s differs", storage.FormatBlockRef(artifact.block))
		}
		state, err := store.BlockState(ctx, artifact.block)
		if err != nil {
			return err
		}
		if !bytes.Equal(state.StateRootHash, artifact.block.RootHash) || !bytes.Equal(state.StateFileHash, artifact.block.FileHash) {
			return fmt.Errorf("zerostate metadata %s differs", storage.FormatBlockRef(artifact.block))
		}
	}
	current, err := store.CurrentState(ctx)
	if err != nil {
		return err
	}
	if !current.Masterchain.Block.Equals(&built.master.block) || len(current.Shards) != 0 {
		return errors.New("current state is not the generated masterchain zerostate")
	}
	master, err := store.BlockState(ctx, built.master.block)
	if err != nil {
		return fmt.Errorf("load generated masterchain state: %w", err)
	}
	parsed, err := groups.ParseState(groups.StateInput{Block: built.master.block, Root: master.Cell})
	if err != nil {
		return fmt.Errorf("parse persisted generated masterchain state: %w", err)
	}
	if _, err = groups.ParseConfig(parsed.ConfigRoot); err != nil {
		return fmt.Errorf("parse persisted generated validator config: %w", err)
	}
	lazyRoot, err := store.LoadStateCellTree(ctx, built.master.block, master.StateRootHash)
	if err != nil {
		return fmt.Errorf("load generated masterchain state cell tree: %w", err)
	}
	lazyState, err := groups.ParseState(groups.StateInput{Block: built.master.block, Root: lazyRoot})
	if err != nil {
		return fmt.Errorf("parse generated masterchain state cell tree: %w", err)
	}
	if _, err = groups.ParseConfig(lazyState.ConfigRoot); err != nil {
		return fmt.Errorf("parse generated validator config cell tree: %w", err)
	}
	return nil
}

func openGenesisStore(path string, readOnly bool, logger zerolog.Logger) (*pebblestore.Store, error) {
	return pebblestore.Open(pebblestore.Options{
		Dir:                   path,
		Logger:                &logger,
		ReadOnly:              readOnly,
		MetaCacheSize:         8 << 20,
		CellCacheSize:         16 << 20,
		MetaMemTableSize:      1 << 20,
		CellShardMemTableSize: 1 << 20,
	})
}

func containsBlock(blocks []ton.BlockIDExt, want ton.BlockIDExt) bool {
	for i := range blocks {
		if blocks[i].Equals(&want) {
			return true
		}
	}
	return false
}

func marshalJSON(value any) ([]byte, error) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func writeNewFile(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(filepath.Clean(path)), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	written := false
	defer func() {
		_ = file.Close()
		if !written {
			_ = os.Remove(path)
		}
	}()
	if _, err = file.Write(data); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	written = true
	return syncDirectory(filepath.Dir(filepath.Clean(path)))
}

func writeAtomicFile(path string, data []byte, mode os.FileMode) error {
	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()
	if err = temp.Chmod(mode); err != nil {
		return err
	}
	if _, err = io.Copy(temp, bytes.NewReader(data)); err != nil {
		return err
	}
	if err = temp.Sync(); err != nil {
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	// Link publishes the already-synced inode atomically and refuses to replace a
	// file created by another process between verification and publication.
	if err = os.Link(tempPath, path); err != nil {
		return err
	}
	if err = os.Remove(tempPath); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
