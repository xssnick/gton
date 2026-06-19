package liteserver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	liveAccountsRootPrewarmDepth      = 5
	liveConfigRootPrewarmDepth        = 3
	liveGlobalLibrariesPrewarmDepth   = 2
	liveMasterInfoPrewarmDepth        = 2
	liveMasterShardHashesPrewarmDepth = 2
	liveAccountProofCacheLimit        = 64
	liveShardHashesProofCacheLimit    = 64
)

type liveBlockFragments struct {
	mu sync.Mutex

	block               ton.BlockIDExt
	stateRoot           *cell.Cell
	blockStateRootProof *cell.Cell
	accountsRoot        *cell.Cell
	shardHeader         runMethodShardHeader

	masterExtra        *tlb.McStateExtra
	accountProofs      map[accountProofKey]accountProofValue
	shardHashesProofs  map[shardHashesProofKey]*cell.Cell
	baseConfig         *runMethodBaseConfig
	globalLibs         *cell.Dictionary
	librariesLoaded    bool
	extMsgLimits       externalMessageSizeLimits
	extMsgLimitsLoaded bool
	lazyLoad           liveLoadGroup[liveBlockFragmentLoadKey]
}

type accountProofKey struct {
	accountID [32]byte
}

type accountProofValue struct {
	proof             []*cell.Cell
	fullState         *cell.Cell
	fullStateLoaded   bool
	prunedState       *cell.Cell
	prunedStateLoaded bool
}

type accountProofResult struct {
	proof []*cell.Cell
	state *cell.Cell
}

type shardHashesProofKey struct {
	workchain int32
	shard     int64
	exact     bool
}

type liveBlockFragmentLoadKind uint8

const (
	liveBlockFragmentLoadMcExtra liveBlockFragmentLoadKind = iota
	liveBlockFragmentLoadAccountProof
	liveBlockFragmentLoadShardHashes
	liveBlockFragmentLoadBaseConfig
	liveBlockFragmentLoadGlobalLibraries
	liveBlockFragmentLoadExternalMessageLimits
)

type liveBlockFragmentLoadKey struct {
	kind    liveBlockFragmentLoadKind
	account accountProofKey
	shard   shardHashesProofKey
	pruned  bool
}

func buildLiveBlockFragments(block ton.BlockIDExt, blockRoot *cell.Cell, stateRoot *cell.Cell) (*liveBlockFragments, error) {
	blockProof, err := blockStateRootProof(blockRoot)
	if err != nil {
		return nil, fmt.Errorf("build block state root proof: %w", err)
	}

	accountsRoot, err := accountsDictRoot(stateRoot)
	if errors.Is(err, storage.ErrNotFound) {
		accountsRoot = nil
	} else if err != nil {
		return nil, fmt.Errorf("load accounts dict root: %w", err)
	}
	if accountsRoot != nil && block.Workchain != masterchainID {
		accountsRoot, err = prewarmCachedCell(accountsRoot, liveAccountsRootPrewarmDepth)
		if err != nil {
			return nil, fmt.Errorf("prewarm accounts dict root: %w", err)
		}
	}

	header, err := runMethodShardStateHeader(stateRoot)
	if err != nil {
		return nil, fmt.Errorf("load shard state header: %w", err)
	}

	return &liveBlockFragments{
		block:               *cloneBlockID(block),
		stateRoot:           stateRoot,
		blockStateRootProof: blockProof,
		accountsRoot:        accountsRoot,
		shardHeader:         header,
	}, nil
}

func (f *liveBlockFragments) prewarmForLiteServer() {
	if f.block.Workchain != masterchainID {
		return
	}

	// Master config and global libraries are runMethod hot-path data. Prewarm is
	// opportunistic because publish accepts hash-valid partial artifacts as well.
	_, _ = f.runMethodBaseConfig()
	_, _ = f.globalLibraries()
}

func (f *liveBlockFragments) accountCell(accountID []byte) (*cell.Cell, error) {
	return accountCellFromAccountsRoot(f.accountsRoot, accountID)
}

func (f *liveBlockFragments) accountProof(accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
	var key accountProofKey
	if len(accountID) == len(key.accountID) {
		copy(key.accountID[:], accountID)

		proof, state, err := f.accountFullProof(accountID, key)
		if err != nil {
			return nil, nil, err
		}
		if pruned {
			state, err = f.accountPrunedState(key, proof, state)
			if err != nil {
				return nil, nil, err
			}
		}
		return proof, state, nil
	}

	return f.buildAccountProof(accountID, pruned)
}

func (f *liveBlockFragments) accountFullProof(accountID []byte, key accountProofKey) ([]*cell.Cell, *cell.Cell, error) {
	f.mu.Lock()
	if result, ok := f.cachedAccountProofResultLocked(key, false); ok {
		f.mu.Unlock()
		return result.proof, result.state, nil
	}
	f.mu.Unlock()

	loadKey := liveBlockFragmentLoadKey{kind: liveBlockFragmentLoadAccountProof, account: key}
	value, err := f.lazyLoad.do(context.Background(), loadKey, func() (any, error) {
		f.mu.Lock()
		if result, ok := f.cachedAccountProofResultLocked(key, false); ok {
			f.mu.Unlock()
			return result, nil
		}
		f.mu.Unlock()

		proof, state, err := f.buildAccountProof(accountID, false)
		if err != nil {
			return accountProofResult{}, err
		}

		result := f.rememberAccountProofState(key, proof, state, false)
		return result, nil
	})
	if err != nil {
		return nil, nil, err
	}
	result, ok := value.(accountProofResult)
	if !ok {
		return nil, nil, errors.New("invalid account proof cache value")
	}
	return result.proof, result.state, nil
}

func (f *liveBlockFragments) accountPrunedState(key accountProofKey, proof []*cell.Cell, fullState *cell.Cell) (*cell.Cell, error) {
	f.mu.Lock()
	if result, ok := f.cachedAccountProofResultLocked(key, true); ok {
		f.mu.Unlock()
		return result.state, nil
	}
	f.mu.Unlock()

	loadKey := liveBlockFragmentLoadKey{kind: liveBlockFragmentLoadAccountProof, account: key, pruned: true}
	value, err := f.lazyLoad.do(context.Background(), loadKey, func() (any, error) {
		f.mu.Lock()
		if result, ok := f.cachedAccountProofResultLocked(key, true); ok {
			f.mu.Unlock()
			return result, nil
		}
		f.mu.Unlock()

		var prunedState *cell.Cell
		if fullState != nil {
			var err error
			prunedState, err = accountPrunedProof(fullState)
			if err != nil {
				return accountProofResult{}, err
			}
		}

		return f.rememberAccountProofState(key, proof, prunedState, true), nil
	})
	if err != nil {
		return nil, err
	}
	result, ok := value.(accountProofResult)
	if !ok {
		return nil, errors.New("invalid pruned account proof cache value")
	}
	return result.state, nil
}

func (f *liveBlockFragments) cachedAccountProofResultLocked(key accountProofKey, pruned bool) (accountProofResult, bool) {
	cached, ok := f.accountProofs[key]
	if !ok || cached.proof == nil {
		return accountProofResult{}, false
	}
	if pruned {
		if !cached.prunedStateLoaded {
			return accountProofResult{}, false
		}
		return accountProofResult{proof: cached.proof, state: cached.prunedState}, true
	}
	if !cached.fullStateLoaded {
		return accountProofResult{}, false
	}
	return accountProofResult{proof: cached.proof, state: cached.fullState}, true
}

func (f *liveBlockFragments) rememberAccountProofState(key accountProofKey, proof []*cell.Cell, state *cell.Cell, pruned bool) accountProofResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.accountProofs == nil {
		f.accountProofs = map[accountProofKey]accountProofValue{}
	}
	cached, ok := f.accountProofs[key]
	if !ok {
		if len(f.accountProofs) >= liveAccountProofCacheLimit {
			for evict := range f.accountProofs {
				delete(f.accountProofs, evict)
				break
			}
		}
	}
	if cached.proof == nil {
		cached.proof = proof
	}
	if pruned {
		if !cached.prunedStateLoaded {
			cached.prunedState = state
			cached.prunedStateLoaded = true
		}
		state = cached.prunedState
	} else {
		if !cached.fullStateLoaded {
			cached.fullState = state
			cached.fullStateLoaded = true
		}
		state = cached.fullState
	}
	f.accountProofs[key] = cached

	return accountProofResult{proof: cached.proof, state: state}
}

func (f *liveBlockFragments) buildAccountProof(accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
	stateProof, state, err := accountStateProofAndCell(f.stateRoot, accountID)
	if err != nil {
		return nil, nil, err
	}

	if state != nil && pruned {
		stateProof, err := accountPrunedProof(state)
		if err != nil {
			return nil, nil, err
		}
		state = stateProof
	}

	return []*cell.Cell{f.blockStateRootProof, stateProof}, state, nil
}

func (f *liveBlockFragments) mcStateExtra() (*tlb.McStateExtra, error) {
	f.mu.Lock()
	if f.masterExtra != nil {
		extra := f.masterExtra
		f.mu.Unlock()
		return extra, nil
	}
	f.mu.Unlock()

	value, err := f.lazyLoad.do(context.Background(), liveBlockFragmentLoadKey{kind: liveBlockFragmentLoadMcExtra}, func() (any, error) {
		f.mu.Lock()
		if f.masterExtra != nil {
			extra := f.masterExtra
			f.mu.Unlock()
			return extra, nil
		}
		f.mu.Unlock()

		extra, err := mcStateExtra(f.stateRoot)
		if err != nil {
			return nil, err
		}
		extra, err = prewarmMcStateExtra(extra)
		if err != nil {
			return nil, err
		}

		f.mu.Lock()
		if f.masterExtra == nil {
			f.masterExtra = extra
		} else {
			extra = f.masterExtra
		}
		f.mu.Unlock()
		return extra, nil
	})
	if err != nil {
		return nil, err
	}
	extra, ok := value.(*tlb.McStateExtra)
	if !ok {
		return nil, errors.New("invalid masterchain state extra cache value")
	}
	return extra, nil
}

func (f *liveBlockFragments) shardHashesProof(workchain int32, shard int64, exact bool) (*cell.Cell, error) {
	key := shardHashesProofKey{workchain: workchain, shard: shard, exact: exact}

	f.mu.Lock()
	if proof := f.shardHashesProofs[key]; proof != nil {
		f.mu.Unlock()
		return proof, nil
	}
	f.mu.Unlock()

	loadKey := liveBlockFragmentLoadKey{kind: liveBlockFragmentLoadShardHashes, shard: key}
	value, err := f.lazyLoad.do(context.Background(), loadKey, func() (any, error) {
		f.mu.Lock()
		if proof := f.shardHashesProofs[key]; proof != nil {
			f.mu.Unlock()
			return proof, nil
		}
		f.mu.Unlock()

		proof, err := shardHashesProof(f.stateRoot, workchain, shard, exact)
		if err != nil {
			return nil, err
		}

		f.mu.Lock()
		if f.shardHashesProofs == nil {
			f.shardHashesProofs = map[shardHashesProofKey]*cell.Cell{}
		}
		if cached := f.shardHashesProofs[key]; cached != nil {
			proof = cached
		} else {
			if len(f.shardHashesProofs) >= liveShardHashesProofCacheLimit {
				for evict := range f.shardHashesProofs {
					delete(f.shardHashesProofs, evict)
					break
				}
			}
			f.shardHashesProofs[key] = proof
		}
		f.mu.Unlock()
		return proof, nil
	})
	if err != nil {
		return nil, err
	}
	proof, ok := value.(*cell.Cell)
	if !ok {
		return nil, errors.New("invalid shard hashes proof cache value")
	}
	return proof, nil
}

func (f *liveBlockFragments) runMethodConfig(now uint32, code *cell.Cell) (runMethodConfigInfo, error) {
	base, err := f.runMethodBaseConfig()
	if err != nil {
		return runMethodConfigInfo{}, err
	}
	return runMethodConfigFromBase(base, now, code)
}

func (f *liveBlockFragments) runMethodBaseConfig() (*runMethodBaseConfig, error) {
	f.mu.Lock()
	if f.baseConfig != nil {
		config := f.baseConfig
		f.mu.Unlock()
		return config, nil
	}
	f.mu.Unlock()

	value, err := f.lazyLoad.do(context.Background(), liveBlockFragmentLoadKey{kind: liveBlockFragmentLoadBaseConfig}, func() (any, error) {
		f.mu.Lock()
		if f.baseConfig != nil {
			config := f.baseConfig
			f.mu.Unlock()
			return config, nil
		}
		f.mu.Unlock()

		extra, err := f.mcStateExtra()
		if err != nil {
			return nil, err
		}

		config, err := buildRunMethodBaseConfig(f.block, extra)
		if err != nil {
			return nil, err
		}

		f.mu.Lock()
		if f.baseConfig == nil {
			f.baseConfig = config
		} else {
			config = f.baseConfig
		}
		f.mu.Unlock()
		return config, nil
	})
	if err != nil {
		return nil, err
	}
	config, ok := value.(*runMethodBaseConfig)
	if !ok {
		return nil, errors.New("invalid run method base config cache value")
	}
	return config, nil
}

func (f *liveBlockFragments) runMethodLibraries(accountLibs *cell.Dictionary) ([]*cell.Cell, error) {
	globalLibs, err := f.globalLibraries()
	if err != nil {
		return nil, err
	}
	return runMethodLibrariesFromGlobal(globalLibs, accountLibs), nil
}

func (f *liveBlockFragments) globalLibraries() (*cell.Dictionary, error) {
	f.mu.Lock()
	if f.librariesLoaded {
		globalLibs := f.globalLibs
		f.mu.Unlock()
		return globalLibs, nil
	}
	f.mu.Unlock()

	value, err := f.lazyLoad.do(context.Background(), liveBlockFragmentLoadKey{kind: liveBlockFragmentLoadGlobalLibraries}, func() (any, error) {
		f.mu.Lock()
		if f.librariesLoaded {
			globalLibs := f.globalLibs
			f.mu.Unlock()
			return globalLibs, nil
		}
		f.mu.Unlock()

		globalLibs, err := librariesDict(f.stateRoot)
		if err != nil {
			return nil, err
		}
		globalLibs, err = prewarmCachedDict(globalLibs, 256, liveGlobalLibrariesPrewarmDepth)
		if err != nil {
			return nil, err
		}

		f.mu.Lock()
		if !f.librariesLoaded {
			f.globalLibs = globalLibs
			f.librariesLoaded = true
		} else {
			globalLibs = f.globalLibs
		}
		f.mu.Unlock()
		return globalLibs, nil
	})
	if err != nil {
		return nil, err
	}
	globalLibs, ok := value.(*cell.Dictionary)
	if !ok {
		return nil, errors.New("invalid global libraries cache value")
	}
	return globalLibs, nil
}

func prewarmMcStateExtra(extra *tlb.McStateExtra) (*tlb.McStateExtra, error) {
	var err error
	if extra.Info != nil {
		extra.Info, err = prewarmCachedCell(extra.Info, liveMasterInfoPrewarmDepth)
		if err != nil {
			return nil, err
		}
	}

	extra.ShardHashes, err = prewarmCachedDict(extra.ShardHashes, 32, liveMasterShardHashesPrewarmDepth)
	if err != nil {
		return nil, err
	}
	return extra, nil
}

func prewarmCachedDict(dict *cell.Dictionary, keySize uint, depth int) (*cell.Dictionary, error) {
	if dict == nil || dict.IsEmpty() {
		return dict, nil
	}

	root, err := prewarmCachedCell(dict.AsCell(), depth)
	if err != nil {
		return nil, err
	}
	return root.AsDict(keySize), nil
}

func prewarmCachedCell(root *cell.Cell, depth int) (*cell.Cell, error) {
	if root == nil {
		return nil, nil
	}
	return root.PrewarmRecursive(depth)
}
