package liteserver

import (
	"context"
	"errors"
	"strconv"
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
)

type liveBlockFragments struct {
	mu sync.Mutex

	block               ton.BlockIDExt
	stateRoot           *cell.Cell
	blockStateRootProof *cell.Cell
	accountsRoot        *cell.Cell
	shardHeader         runMethodShardHeader

	masterExtra       *tlb.McStateExtra
	shardHashesProofs map[int32]*cell.Cell
	baseConfig        *runMethodBaseConfig
	globalLibs        *cell.Dictionary
	librariesLoaded   bool
	lazyLoad          liveBlockLoadGroup
}

func buildLiveBlockFragments(block ton.BlockIDExt, blockRoot *cell.Cell, stateRoot *cell.Cell) (*liveBlockFragments, error) {
	blockProof, err := blockStateRootProof(blockRoot)
	if err != nil {
		return nil, err
	}

	accountsRoot, err := accountsDictRoot(stateRoot)
	if errors.Is(err, storage.ErrNotFound) {
		accountsRoot = nil
	} else if err != nil {
		return nil, err
	}
	if accountsRoot != nil && block.Workchain != masterchainID {
		accountsRoot, err = prewarmCachedCell(accountsRoot, liveAccountsRootPrewarmDepth)
		if err != nil {
			return nil, err
		}
	}

	header, err := runMethodShardStateHeader(stateRoot)
	if err != nil {
		return nil, err
	}

	return &liveBlockFragments{
		block:               *cloneBlockID(block),
		stateRoot:           stateRoot,
		blockStateRootProof: blockProof,
		accountsRoot:        accountsRoot,
		shardHeader:         header,
	}, nil
}

func (f *liveBlockFragments) accountCell(accountID []byte) (*cell.Cell, error) {
	return accountCellFromAccountsRoot(f.accountsRoot, accountID)
}

func (f *liveBlockFragments) accountProof(accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
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

	value, err := f.lazyLoad.do(context.Background(), "mc-extra", func() (any, error) {
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

func (f *liveBlockFragments) shardHashesProof(workchain int32) (*cell.Cell, error) {
	f.mu.Lock()
	if proof := f.shardHashesProofs[workchain]; proof != nil {
		f.mu.Unlock()
		return proof, nil
	}
	f.mu.Unlock()

	value, err := f.lazyLoad.do(context.Background(), "shard-hashes:"+strconv.FormatInt(int64(workchain), 10), func() (any, error) {
		f.mu.Lock()
		if proof := f.shardHashesProofs[workchain]; proof != nil {
			f.mu.Unlock()
			return proof, nil
		}
		f.mu.Unlock()

		proof, err := shardHashesProof(f.stateRoot, workchain)
		if err != nil {
			return nil, err
		}

		f.mu.Lock()
		if f.shardHashesProofs == nil {
			f.shardHashesProofs = map[int32]*cell.Cell{}
		}
		if cached := f.shardHashesProofs[workchain]; cached != nil {
			proof = cached
		} else {
			f.shardHashesProofs[workchain] = proof
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

	value, err := f.lazyLoad.do(context.Background(), "base-config", func() (any, error) {
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

	value, err := f.lazyLoad.do(context.Background(), "global-libraries", func() (any, error) {
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
