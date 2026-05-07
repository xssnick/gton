package service

import (
	"sort"

	"flexserver/service/storage"
)

func cloneBlockStateSlice(states []*storage.BlockState) []*storage.BlockState {
	if len(states) == 0 {
		return nil
	}
	cloned := make([]*storage.BlockState, 0, len(states))
	for _, state := range states {
		cloned = append(cloned, storage.CloneBlockState(state))
	}
	return cloned
}

type appliedStateSet struct {
	states map[string]*storage.BlockState
}

func (s *appliedStateSet) remember(state *storage.BlockState) {
	if state == nil {
		return
	}
	if s.states == nil {
		s.states = map[string]*storage.BlockState{}
	}
	s.states[storage.BlockKey(state.Block)] = storage.CloneBlockState(state)
}

func (s *appliedStateSet) rememberAll(states []*storage.BlockState) {
	for _, state := range states {
		s.remember(state)
	}
}

func (s *appliedStateSet) rememberCurrent(current *storage.CurrentState) {
	if current == nil {
		return
	}
	s.remember(&current.Masterchain)
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		s.remember(&shard)
	}
}

func (s *appliedStateSet) takeWithCurrent(current *storage.CurrentState) []*storage.BlockState {
	s.rememberCurrent(current)

	if len(s.states) == 0 {
		return nil
	}
	states := make([]*storage.BlockState, 0, len(s.states))
	keys := make([]string, 0, len(s.states))
	for key := range s.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		states = append(states, storage.CloneBlockState(s.states[key]))
	}
	s.states = nil
	return states
}
