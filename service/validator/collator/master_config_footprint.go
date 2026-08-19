package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
)

// configFootprintCells sizes the capture recorder. Mainnet reads 2792 of the
// configuration's 2929 cells; this is a sizing hint and nothing else.
const configFootprintCells = 4096

// masterConfigParse is everything one configuration epoch is parsed into. It
// exists so the capture below and the collation fresh branch cannot drift
// apart: a parse added here joins the recorded footprint by construction.
type masterConfigParse struct {
	execution *tvm.PreparedBlockchainConfig
	config    *Config
	groups    *groups.Config
}

func parseMasterConfigEpoch(root *cell.Cell) (masterConfigParse, error) {
	execution, err := tvm.PrepareBlockchainConfig(root)
	if err != nil {
		return masterConfigParse{}, fmt.Errorf("%w: prepare blockchain config: %v", ErrInvalidInput, err)
	}
	config, err := PrepareConfig(execution)
	if err != nil {
		return masterConfigParse{}, err
	}
	groupConfig, err := groups.ParseConfig(root)
	if err != nil {
		return masterConfigParse{}, fmt.Errorf("%w: prepare validator groups config: %v", ErrInvalidInput, err)
	}
	return masterConfigParse{execution: execution, config: config, groups: groupConfig}, nil
}

// configFootprint is the set of predecessor cells the configuration parses of
// one config epoch read.
//
// Master collation replays it when it skips those parses: the Merkle update
// descends only through recorded cells, and the collated-size estimate answers
// membership out of the same record, so the block is the block the parse would
// have produced only if the replayed set is the set the parse read.
//
// That is the promise, and it is kept exactly. A wrong footprint is not
// observable on the one masterchain fixture this package has — its whole
// non-configuration state is twenty cells — but that is a fact about the
// fixture and not a structural guarantee. Reuse only fires when the
// configuration the candidate installs is the root the predecessor state
// already holds, so on that fixture the update prunes the whole configuration
// onto one boundary and a proof over it stops there too, and a short, empty or
// padded footprint produces byte-identical output. Membership in the read set
// is answered by hash, wherever that hash occurs, so a configuration cell that
// also appears elsewhere in a real state is named by the destination and moves
// the produced bytes: one such cell is enough, and it has been reproduced.
//
// The set is therefore maintained against the record, which is the checkable
// thing on every state rather than only on states where the bytes happen to
// see it: TestMasterConfigFootprintMutationsAreDetected is the gate, and
// TestMasterConfigFootprintDoesNotReachTheProducedBlock is the canary over the
// fixture — it fails the day a new consumer selects cells out of the read set,
// which is what shard collation already does for its predecessor state proof,
// or the day this fixture holds a configuration cell that occurs outside the
// configuration too.
type configFootprint struct {
	root  cell.Hash
	cells []*cell.Cell
}

// captureConfigFootprint materializes the configuration and records what
// parsing it reads. It returns the materialized tree alongside the footprint,
// and the caller must parse that tree rather than the one it passed in: the
// materialization here is the only pass over the configuration either of them
// needs, and repeating the caller's parse over the original — paged-in — root
// would page the whole configuration in a second time.
//
// The parse runs a second time here and its result is discarded.
// PreparedBlockchainConfig keeps the exact root it was handed, so a config
// prepared through the recording trace would carry that trace into every
// transaction of every later block; the objects that leave prepare must come
// from an untraced parse of the returned tree.
//
// Nothing here reports an error and nothing returns a partial footprint: a nil
// footprint costs one fresh parse per block, which is what the collator pays
// today, while a short one would cost a wrong block.
func captureConfigFootprint(root *cell.Cell) (*cell.Cell, *configFootprint) {
	if root == nil || root.IsVirtualized() {
		// A virtualized root came out of a proof, so its cells are not the
		// predecessor's own bodies and must never stand in for them.
		return nil, nil
	}
	// Materializing first is what makes the capture complete: the recorder keeps
	// the resolved body of a lazy placeholder but not the placeholder's own
	// unread references, so a footprint taken over a paged-in tree holds cells
	// that still resolve through the state snapshot's loader — which the
	// footprint, and the prepared configuration built from it, outlive.
	resident, err := root.PrewarmRecursive(0)
	if err != nil {
		return nil, nil
	}
	usage := cell.NewReadSetSized(resident, configFootprintCells)
	if _, err = parseMasterConfigEpoch(usage.Root()); err != nil {
		return resident, nil
	}

	recorded := usage.Cells()
	cells := make([]*cell.Cell, 0, len(recorded))
	for _, c := range recorded {
		if c.IsLazy() || c.IsVirtualized() || c.Level() != 0 {
			return resident, nil
		}
		cells = append(cells, c.WithoutTrace())
	}
	if len(cells) == 0 {
		return resident, nil
	}
	return resident, &configFootprint{root: root.HashKey(), cells: cells}
}

// covers reports whether f was captured from this configuration root.
func (f *configFootprint) covers(root *cell.Cell) bool {
	return f != nil && root != nil && f.root == root.HashKey()
}

// replay records f into usage. usage is nil on the verification path, which
// records nothing at all.
func (f *configFootprint) replay(usage *cell.ReadSet) {
	if f == nil || usage == nil {
		return
	}
	for _, c := range f.cells {
		usage.Record(c)
	}
}
