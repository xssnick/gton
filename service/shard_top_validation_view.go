package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errShardTopValidationViewConflict = errors.New("shard top validation view conflicts at the same masterchain height")

// shardTopValidationView is the single immutable masterchain epoch used for
// both TopBlockDescr validator-set authentication and state-aware validation.
// The state is cloned once on publication; its cell tree and parsed projection
// are immutable and shared with in-flight validations.
type shardTopValidationView struct {
	masterchain      *storage.BlockState
	stateRootHash    cell.Hash
	config           broadcastValidatorConfig
	validatorContext *blockproof.ShardTopValidatorContext

	validatorMu   sync.Mutex
	validatorSets map[shardTopValidatorCacheKey]*blockproof.PreparedValidatorSet
}

type shardTopValidatorCacheKey struct {
	workchain     int32
	shard         int64
	catchainSeqno uint32
}

func newShardTopValidationView(state *storage.BlockState) (*shardTopValidationView, error) {
	if state == nil {
		return nil, errors.New("shard top validation view has no masterchain state")
	}
	masterchain := storage.CloneBlockState(state)
	if masterchain.Block.Workchain != -1 || masterchain.Block.Shard != topShard {
		return nil, fmt.Errorf("shard top validation view block %s is not masterchain", storage.FormatBlockRef(masterchain.Block))
	}
	if err := storage.ValidateBlockIDHashes(masterchain.Block); err != nil {
		return nil, fmt.Errorf("shard top validation view block: %w", err)
	}
	if masterchain.Cell == nil {
		return nil, errors.New("shard top validation view has no masterchain state root")
	}
	stateRootHash := masterchain.Cell.HashKey()
	if len(masterchain.StateRootHash) != 0 {
		if len(masterchain.StateRootHash) != len(stateRootHash) {
			return nil, fmt.Errorf(
				"shard top validation view state root metadata for %s has invalid length %d",
				storage.FormatBlockRef(masterchain.Block),
				len(masterchain.StateRootHash),
			)
		}
		if !bytes.Equal(masterchain.StateRootHash, stateRootHash[:]) {
			return nil, fmt.Errorf("shard top validation view state root differs from metadata for %s", storage.FormatBlockRef(masterchain.Block))
		}
	}

	validatorContext, err := blockproof.NewShardTopValidatorContext(masterchain)
	if err != nil {
		return nil, fmt.Errorf("load shard top validator context from %s: %w", storage.FormatBlockRef(masterchain.Block), err)
	}
	config, err := broadcastValidatorConfigForMasterchain(masterchain.Block, validatorContext.BlockchainConfig())
	if err != nil {
		return nil, err
	}

	return &shardTopValidationView{
		masterchain:      masterchain,
		stateRootHash:    stateRootHash,
		config:           config,
		validatorContext: validatorContext,
	}, nil
}

func (v *shardTopValidationView) validatorSet(
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) (*blockproof.PreparedValidatorSet, error) {
	key := shardTopValidatorCacheKey{
		workchain:     block.Workchain,
		shard:         block.Shard,
		catchainSeqno: catchainSeqno,
	}

	v.validatorMu.Lock()
	set := v.validatorSets[key]
	v.validatorMu.Unlock()
	if set == nil {
		validators, err := v.validatorContext.ValidatorsForCatchain(block, catchainSeqno)
		if err != nil {
			return nil, fmt.Errorf("load shard top validators for %s: %w", storage.FormatBlockRef(block), err)
		}
		set, err = blockproof.PrepareValidatorSet(catchainSeqno, validators)
		if err != nil {
			return nil, fmt.Errorf("prepare shard top validators for %s: %w", storage.FormatBlockRef(block), err)
		}

		v.validatorMu.Lock()
		if v.validatorSets == nil {
			v.validatorSets = make(map[shardTopValidatorCacheKey]*blockproof.PreparedValidatorSet)
		}
		if cached := v.validatorSets[key]; cached != nil {
			set = cached
		} else {
			v.validatorSets[key] = set
		}
		v.validatorMu.Unlock()
	}
	if set.Hash() != validatorSetHash {
		return nil, fmt.Errorf(
			"validator set %08x for %s is not available: %w",
			validatorSetHash,
			storage.FormatBlockRef(block),
			storage.ErrNotFound,
		)
	}

	return set, nil
}

func (c *broadcastValidatorCache) getShardTopView() (*shardTopValidationView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.shardTopView == nil {
		return nil, storage.ErrNotFound
	}

	return c.shardTopView, nil
}

func (c *broadcastValidatorCache) putShardTopView(next *shardTopValidationView) (*shardTopValidationView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.shardTopView
	if current != nil {
		currentBlock := &current.masterchain.Block
		nextBlock := &next.masterchain.Block
		if nextBlock.SeqNo < currentBlock.SeqNo {
			return current, nil
		}
		if nextBlock.SeqNo == currentBlock.SeqNo {
			if !nextBlock.Equals(currentBlock) || next.stateRootHash != current.stateRootHash ||
				next.config.rootHash != current.config.rootHash {
				return nil, fmt.Errorf(
					"%w: current=%s next=%s",
					errShardTopValidationViewConflict,
					storage.FormatBlockRef(*currentBlock),
					storage.FormatBlockRef(*nextBlock),
				)
			}

			return current, nil
		}
	}

	c.shardTopView = next
	return next, nil
}

func (s *SyncCoordinator) publishShardTopValidationView(state *storage.BlockState) error {
	next, err := newShardTopValidationView(state)
	if err != nil {
		return err
	}

	_, err = s.broadcastValidatorCache.putShardTopView(next)
	return err
}

func (s *SyncCoordinator) currentShardTopValidationView(ctx context.Context) (*shardTopValidationView, error) {
	view, err := s.broadcastValidatorCache.getShardTopView()
	if err == nil {
		return view, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	current, err := s.status.currentStateSnapshot(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("%w: load current masterchain for shard top validation: %v", p2p.ErrBroadcastSignatureRetryable, err)
		}
		return nil, fmt.Errorf("load current masterchain for shard top validation: %w", err)
	}
	state, err := s.loadMasterStateForConsensus(ctx, current.Masterchain.Block)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf(
				"%w: load masterchain state %s for shard top validation: %v",
				p2p.ErrBroadcastSignatureRetryable,
				storage.FormatBlockRef(current.Masterchain.Block),
				err,
			)
		}
		return nil, fmt.Errorf("load masterchain state %s for shard top validation: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	next, err := newShardTopValidationView(state)
	if err != nil {
		return nil, err
	}

	return s.broadcastValidatorCache.putShardTopView(next)
}
