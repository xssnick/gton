package liveview

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
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

type BlockView struct {
	mu sync.Mutex

	block               ton.BlockIDExt
	stateRoot           *cell.Cell
	blockStateRootProof *cell.Cell
	accountsRoot        *cell.Cell
	shardHeader         RunMethodShardHeader

	masterExtra         *tlb.McStateExtra
	accountProofs       map[accountProofKey]accountProofValue
	shardHashesProofs   map[shardHashesProofKey]*cell.Cell
	retainCurrentCaches bool
	baseConfig          *runMethodBaseConfig
	globalLibs          *cell.Dictionary
	librariesLoaded     bool
	externalMsgAccounts map[externalMessageAccountKey]externalMessageAccountValue
	extMsgLimits        ExternalMessageSizeLimits
	extMsgLimitsLoaded  bool
	mcExtraLoad         liveLoadGroup[struct{}, *tlb.McStateExtra]
	accountProofLoad    liveLoadGroup[accountProofLoadKey, accountProofResult]
	shardHashesLoad     liveLoadGroup[shardHashesProofKey, *cell.Cell]
	baseConfigLoad      liveLoadGroup[struct{}, *runMethodBaseConfig]
	globalLibsLoad      liveLoadGroup[struct{}, *cell.Dictionary]
	extMsgLimitsLoad    liveLoadGroup[struct{}, ExternalMessageSizeLimits]
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

type accountProofLoadKey struct {
	account accountProofKey
	pruned  bool
}

// lazyLoadFragment owns the lazy-load sequence shared by the BlockView
// fragment getters: a mu-guarded fast-path check, a singleflight-collapsed
// build with the same re-check inside, and a mu-guarded publish where a
// concurrently stored value wins. get and put are called with view.mu held;
// put stores the built value or adopts an existing one and returns the
// canonical value. build runs without the lock.
func lazyLoadFragment[K comparable, V any](view *BlockView, group *liveLoadGroup[K, V], key K, get func() (V, error), put func(V) V, build func() (V, error)) (V, error) {
	view.mu.Lock()
	cached, err := get()
	view.mu.Unlock()
	if err == nil {
		return cached, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		var zero V
		return zero, err
	}

	value, err := group.do(context.Background(), key, func() (V, error) {
		view.mu.Lock()
		cached, err := get()
		view.mu.Unlock()
		if err == nil {
			return cached, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			var zero V
			return zero, err
		}

		built, err := build()
		if err != nil {
			var zero V
			return zero, err
		}

		view.mu.Lock()
		built = put(built)
		view.mu.Unlock()
		return built, nil
	})
	if err != nil {
		var zero V
		return zero, err
	}
	return value, nil
}

func NewBlockView(block ton.BlockIDExt, blockRoot *cell.Cell, stateRoot *cell.Cell) (*BlockView, error) {
	blockProof, err := blockproof.BlockStateRootProof(blockRoot)
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
		accountsRoot, err = accountsRoot.PrewarmRecursive(liveAccountsRootPrewarmDepth)
		if err != nil {
			return nil, fmt.Errorf("prewarm accounts dict root: %w", err)
		}
	}

	header, err := runMethodShardStateHeader(stateRoot)
	if err != nil {
		return nil, fmt.Errorf("load shard state header: %w", err)
	}

	return &BlockView{
		block:               cloneBlockID(block),
		stateRoot:           stateRoot,
		blockStateRootProof: blockProof,
		accountsRoot:        accountsRoot,
		shardHeader:         header,
		retainCurrentCaches: true,
	}, nil
}

func (f *BlockView) releaseCurrentCaches() {
	f.mu.Lock()
	f.retainCurrentCaches = false
	f.accountProofs = nil
	f.shardHashesProofs = nil
	f.externalMsgAccounts = nil
	f.mu.Unlock()
}

func (f *BlockView) prewarmHotPath() {
	if f.block.Workchain != masterchainID {
		return
	}

	// Master config and global libraries are runMethod hot-path data. Prewarm is
	// opportunistic because publish accepts hash-valid partial artifacts as well.
	_, _ = f.runMethodBaseConfig()
	_, _ = f.GlobalLibraries()
}

func (f *BlockView) Block() ton.BlockIDExt {
	return cloneBlockID(f.block)
}

func (f *BlockView) StateRoot() *cell.Cell {
	return f.stateRoot
}

func (f *BlockView) BlockStateRootProof() *cell.Cell {
	return f.blockStateRootProof
}

func (f *BlockView) Header() RunMethodShardHeader {
	return f.shardHeader
}

// BlockContext builds an immutable tvm execution context for this block's
// master config epoch: the prepared blockchain config, prev-blocks tuple and the global
// libraries. Account-scoped libraries come from the prepared account, so only
// the global library collection is attached here. The returned context is
// safe to share and reuse across accounts and messages of the same block.
func (f *BlockView) BlockContext(now uint32, blockLT uint64) (*tvm.BlockContext, error) {
	base, err := f.runMethodBaseConfig()
	if err != nil {
		return nil, err
	}
	libraries, err := f.RunMethodLibraries(nil)
	if err != nil {
		return nil, err
	}
	return base.epoch.prepared.NewBlockContext(tvm.BlockOptions{
		Now:        now,
		BlockLT:    int64(blockLT),
		PrevBlocks: base.prevBlocks,
		Libraries:  libraries,
	})
}

func (f *BlockView) AccountCell(accountID []byte) (*cell.Cell, error) {
	return accountCellFromAccountsRoot(f.accountsRoot, accountID)
}

func (f *BlockView) AccountProof(accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
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

func (f *BlockView) accountFullProof(accountID []byte, key accountProofKey) ([]*cell.Cell, *cell.Cell, error) {
	result, err := lazyLoadFragment(f, &f.accountProofLoad,
		accountProofLoadKey{account: key},
		func() (accountProofResult, error) { return f.cachedAccountProofResultLocked(key, false) },
		func(result accountProofResult) accountProofResult {
			return f.rememberAccountProofStateLocked(key, result.proof, result.state, false)
		},
		func() (accountProofResult, error) {
			proof, state, err := f.buildAccountProof(accountID, false)
			if err != nil {
				return accountProofResult{}, err
			}
			return accountProofResult{proof: proof, state: state}, nil
		},
	)
	if err != nil {
		return nil, nil, err
	}
	return result.proof, result.state, nil
}

func (f *BlockView) accountPrunedState(key accountProofKey, proof []*cell.Cell, fullState *cell.Cell) (*cell.Cell, error) {
	result, err := lazyLoadFragment(f, &f.accountProofLoad,
		accountProofLoadKey{account: key, pruned: true},
		func() (accountProofResult, error) { return f.cachedAccountProofResultLocked(key, true) },
		func(result accountProofResult) accountProofResult {
			return f.rememberAccountProofStateLocked(key, result.proof, result.state, true)
		},
		func() (accountProofResult, error) {
			var prunedState *cell.Cell
			if fullState != nil {
				var err error
				prunedState, err = accountPrunedProof(fullState)
				if err != nil {
					return accountProofResult{}, err
				}
			}
			return accountProofResult{proof: proof, state: prunedState}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return result.state, nil
}

func (f *BlockView) cachedAccountProofResultLocked(key accountProofKey, pruned bool) (accountProofResult, error) {
	cached, ok := f.accountProofs[key]
	if !ok || cached.proof == nil {
		return accountProofResult{}, storage.ErrNotFound
	}
	if pruned {
		if !cached.prunedStateLoaded {
			return accountProofResult{}, storage.ErrNotFound
		}
		return accountProofResult{proof: cached.proof, state: cached.prunedState}, nil
	}
	if !cached.fullStateLoaded {
		return accountProofResult{}, storage.ErrNotFound
	}
	return accountProofResult{proof: cached.proof, state: cached.fullState}, nil
}

// rememberAccountProofStateLocked publishes a built proof/state pair, adopting
// any concurrently stored parts. The caller must hold f.mu.
func (f *BlockView) rememberAccountProofStateLocked(key accountProofKey, proof []*cell.Cell, state *cell.Cell, pruned bool) accountProofResult {
	if !f.retainCurrentCaches {
		return accountProofResult{proof: proof, state: state}
	}

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

func (f *BlockView) buildAccountProof(accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
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

func (f *BlockView) McStateExtra() (*tlb.McStateExtra, error) {
	return lazyLoadFragment(f, &f.mcExtraLoad, struct{}{},
		func() (*tlb.McStateExtra, error) {
			if f.masterExtra == nil {
				return nil, storage.ErrNotFound
			}
			return f.masterExtra, nil
		},
		func(extra *tlb.McStateExtra) *tlb.McStateExtra {
			if f.masterExtra == nil {
				f.masterExtra = extra
			}
			return f.masterExtra
		},
		func() (*tlb.McStateExtra, error) {
			extra, err := mcStateExtra(f.stateRoot)
			if err != nil {
				return nil, err
			}
			return prewarmMcStateExtra(extra)
		},
	)
}

func (f *BlockView) ShardHashesProof(workchain int32, shard int64, exact bool) (*cell.Cell, error) {
	key := shardHashesProofKey{workchain: workchain, shard: shard, exact: exact}
	return lazyLoadFragment(f, &f.shardHashesLoad, key,
		func() (*cell.Cell, error) {
			proof := f.shardHashesProofs[key]
			if proof == nil {
				return nil, storage.ErrNotFound
			}
			return proof, nil
		},
		func(proof *cell.Cell) *cell.Cell {
			if !f.retainCurrentCaches {
				return proof
			}
			if cached := f.shardHashesProofs[key]; cached != nil {
				return cached
			}
			if f.shardHashesProofs == nil {
				f.shardHashesProofs = map[shardHashesProofKey]*cell.Cell{}
			}
			if len(f.shardHashesProofs) >= liveShardHashesProofCacheLimit {
				for evict := range f.shardHashesProofs {
					delete(f.shardHashesProofs, evict)
					break
				}
			}
			f.shardHashesProofs[key] = proof
			return proof
		},
		func() (*cell.Cell, error) {
			return blockproof.ShardHashesProof(f.stateRoot, workchain, shard, exact)
		},
	)
}

func (f *BlockView) RunMethodConfig(now uint32, code *cell.Cell) (RunMethodConfigInfo, error) {
	base, err := f.runMethodBaseConfig()
	if err != nil {
		return RunMethodConfigInfo{}, err
	}
	return runMethodConfigFromBase(base, now, code)
}

func (f *BlockView) runMethodBaseConfig() (*runMethodBaseConfig, error) {
	return lazyLoadFragment(f, &f.baseConfigLoad, struct{}{},
		func() (*runMethodBaseConfig, error) {
			if f.baseConfig == nil {
				return nil, storage.ErrNotFound
			}
			return f.baseConfig, nil
		},
		func(config *runMethodBaseConfig) *runMethodBaseConfig {
			if f.baseConfig == nil {
				f.baseConfig = config
			}
			return f.baseConfig
		},
		func() (*runMethodBaseConfig, error) {
			extra, err := f.McStateExtra()
			if err != nil {
				return nil, err
			}
			return buildRunMethodBaseConfig(f.block, extra)
		},
	)
}

func (f *BlockView) RunMethodLibraries(accountLibs *cell.Dictionary) ([]*cell.Cell, error) {
	globalLibs, err := f.GlobalLibraries()
	if err != nil {
		return nil, err
	}
	return runMethodLibrariesFromGlobal(globalLibs, accountLibs), nil
}

func (f *BlockView) GlobalLibraries() (*cell.Dictionary, error) {
	return lazyLoadFragment(f, &f.globalLibsLoad, struct{}{},
		func() (*cell.Dictionary, error) {
			if !f.librariesLoaded {
				return nil, storage.ErrNotFound
			}
			return f.globalLibs, nil
		},
		func(globalLibs *cell.Dictionary) *cell.Dictionary {
			if !f.librariesLoaded {
				f.globalLibs = globalLibs
				f.librariesLoaded = true
			}
			return f.globalLibs
		},
		func() (*cell.Dictionary, error) {
			globalLibs, err := librariesDict(f.stateRoot)
			if err != nil {
				return nil, err
			}
			return prewarmCachedDict(globalLibs, 256, liveGlobalLibrariesPrewarmDepth)
		},
	)
}

func prewarmMcStateExtra(extra *tlb.McStateExtra) (*tlb.McStateExtra, error) {
	var err error
	if extra.Info != nil {
		extra.Info, err = extra.Info.PrewarmRecursive(liveMasterInfoPrewarmDepth)
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

	root, err := dict.AsCell().PrewarmRecursive(depth)
	if err != nil {
		return nil, err
	}
	return root.AsDict(keySize), nil
}
