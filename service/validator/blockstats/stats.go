// Package blockstats keeps the validator-engine-compatible block counters.
package blockstats

import "sync/atomic"

// Counter is one terminal outcome pair for a chain and operation.
type Counter struct {
	OK    uint64
	Error uint64
}

// ChainStats separates masterchain and shardchain counters.
type ChainStats struct {
	Master Counter
	Shard  Counter
}

// Snapshot is a consistent-enough point-in-time view for status reporting.
// Each counter is loaded atomically and may advance independently afterwards.
type Snapshot struct {
	Collated  ChainStats
	Validated ChainStats
}

// Accumulator owns process-lifetime block counters. It is safe for concurrent
// observations and snapshots.
type Accumulator struct {
	collatedMasterOK     atomic.Uint64
	collatedMasterError  atomic.Uint64
	collatedShardOK      atomic.Uint64
	collatedShardError   atomic.Uint64
	validatedMasterOK    atomic.Uint64
	validatedMasterError atomic.Uint64
	validatedShardOK     atomic.Uint64
	validatedShardError  atomic.Uint64
}

// New creates an empty process-lifetime accumulator.
func New() *Accumulator {
	return &Accumulator{}
}

// ObserveCollation records one terminal collation build. masterchain selects
// the masterchain counter; every other chain is recorded as a shardchain.
func (a *Accumulator) ObserveCollation(masterchain bool, success bool) {
	if masterchain {
		if success {
			a.collatedMasterOK.Add(1)
		} else {
			a.collatedMasterError.Add(1)
		}

		return
	}
	if success {
		a.collatedShardOK.Add(1)
	} else {
		a.collatedShardError.Add(1)
	}
}

// ObserveValidation records one terminal non-empty candidate validation.
// masterchain selects the masterchain counter; every other chain is recorded
// as a shardchain.
func (a *Accumulator) ObserveValidation(masterchain bool, success bool) {
	if masterchain {
		if success {
			a.validatedMasterOK.Add(1)
		} else {
			a.validatedMasterError.Add(1)
		}

		return
	}
	if success {
		a.validatedShardOK.Add(1)
	} else {
		a.validatedShardError.Add(1)
	}
}

// BlockStats returns the current counters without resetting them.
func (a *Accumulator) BlockStats() Snapshot {
	return Snapshot{
		Collated: ChainStats{
			Master: Counter{
				OK:    a.collatedMasterOK.Load(),
				Error: a.collatedMasterError.Load(),
			},
			Shard: Counter{
				OK:    a.collatedShardOK.Load(),
				Error: a.collatedShardError.Load(),
			},
		},
		Validated: ChainStats{
			Master: Counter{
				OK:    a.validatedMasterOK.Load(),
				Error: a.validatedMasterError.Load(),
			},
			Shard: Counter{
				OK:    a.validatedShardOK.Load(),
				Error: a.validatedShardError.Load(),
			},
		},
	}
}
