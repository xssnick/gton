package collator

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/vmerr"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// ErrSemanticExecution marks a local transaction-emulator failure. It is not
// proof that the candidate is invalid and must remain distinct from
// ErrInvalidInput at the consensus/runtime boundary.
var ErrSemanticExecution = errors.New("collator: semantic execution failed")

// SemanticVerifier deterministically replays the state transition after the
// structural verifier has authenticated the candidate, predecessor and
// auxiliary state views. A verifier is stateless and can validate independent
// account lanes concurrently at the call-site level.
type SemanticVerifier struct {
	machine *tvm.TVM
	// onStorageStatRecompute observes every replayed transaction whose bound
	// account storage-stat proof fell short of this replay's own update walk,
	// making the executor recompute the stat from state (see
	// tvm.TransactionExecutionResult.StorageStatRecomputed). The candidate
	// still validates; the observation is the only signal that a producer's
	// proof shape and our walk disagreed. Replay lanes run concurrently, so
	// the callback must be safe to call from multiple goroutines.
	onStorageStatRecompute func(MetricChain, [32]byte)
}

// SetStorageStatRecomputeObserver installs the storage-stat recompute
// observer; see the field for the contract. Set once, before the verifier
// starts serving.
func (v *SemanticVerifier) SetStorageStatRecomputeObserver(fn func(MetricChain, [32]byte)) {
	v.onStorageStatRecompute = fn
}

// NewSemanticVerifier constructs the production candidate transition
// verifier backed by the transaction executor used by Builder.
func NewSemanticVerifier(machine *tvm.TVM) *SemanticVerifier {
	return &SemanticVerifier{machine: machine}
}

// VerifyCandidateTransition replays all builder-supported semantic phases.
// CandidateTransition values are produced by VerifyShardCandidate and
// VerifyMasterCandidate after their structural checks; constructing one by
// hand cannot provide the private decoded transition and is rejected.
func (v *SemanticVerifier) VerifyCandidateTransition(ctx context.Context, transition CandidateTransition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if transition.prepared == nil || transition.prepared.candidate == nil || transition.prepared.previous == nil {
		return fmt.Errorf("%w: semantic transition was not prepared by the structural verifier", ErrInvalidInput)
	}

	replay, err := newSemanticReplay(ctx, v, transition)
	if err != nil {
		return err
	}
	// The account precheck is a pure reader — it scans the predecessor accounts
	// against the candidate's and looks entries up in the account blocks — and
	// writes nothing the queue preparation reads, so the two run together and
	// the cheaper of them stops costing wall time.
	//
	// It joins before verifyAccounts because a candidate that is structurally
	// invalid and also trips the local emulator has to keep being rejected with
	// the precheck's ErrInvalidInput. Letting verifyAccounts answer first would
	// turn a rejection into ErrSemanticExecution, which is documented above as
	// "retry", not "invalid".
	var (
		precheckErr error
		precheck    sync.WaitGroup
	)
	precheck.Add(1)
	go func() {
		defer precheck.Done()
		precheckErr = replay.precheckAccountUpdates()
	}()

	queues, queuesErr := replay.prepareQueueValidation()
	precheck.Wait()
	if precheckErr != nil {
		return precheckErr
	}
	if queuesErr != nil {
		return queuesErr
	}
	if err = replay.verifyAccounts(); err != nil {
		return err
	}
	if err = queues.verifyAfterReplay(); err != nil {
		return err
	}
	if err = replay.verifyMasterSemantics(); err != nil {
		return err
	}
	// Only an accepted candidate has a shape worth reporting, so this is the
	// last thing the transition does. The counts came from the walks above; the
	// block is not read again for them.
	transition.prepared.candidate.shape = replay.shape

	return nil
}

// semanticEnvelopeCache deduplicates envelope parses within one validation.
// The same message envelope reaches the replay through up to five doors — the
// inbound descriptor, the outbound descriptor, the candidate queue entry, the
// predecessor queue entry and the dequeue check — and each used to parse it
// from scratch: measured on the mainnet workload, 4 047 parses for 855
// distinct envelopes. The parse is a pure function of the cell's content,
// every downstream comparison is by hash (equalCell, HashKey), and the entries
// die with the validation, so caching by content hash changes nothing
// observable but the work. Reject parity survives too: within one candidate,
// equal content is one node of the proof DAG, so the first parse's resolution
// of that node is exactly the residency check every later door would repeat.
//
// Scoped to one replay on purpose: a process-wide cache would retain each
// envelope's root and, through its refs, the whole proof DAG of a candidate
// long past its validation — live-set growth, which is the one memory currency
// this tree treats as expensive.
type semanticEnvelopeCache struct {
	entries map[cell.Hash]*semanticEnvelope
}

// parse returns the cached envelope for the cell's content or parses and
// caches it. Queue preparation fills the cache before the account lanes start;
// lanes read only the parsed descriptor bindings. Queue checks resume after
// the lanes join, so cache access is sequential. A nil receiver parses without
// caching for callers outside the replay.
func (c *semanticEnvelopeCache) parse(root *cell.Cell) (*semanticEnvelope, error) {
	if c == nil {
		return parseSemanticEnvelope(root)
	}
	key := root.HashKey()
	if cached := c.entries[key]; cached != nil {
		return cached, nil
	}
	envelope, err := parseSemanticEnvelope(root)
	if err != nil {
		return nil, err
	}
	if c.entries == nil {
		c.entries = make(map[cell.Hash]*semanticEnvelope)
	}
	c.entries[key] = envelope

	return envelope, nil
}

type semanticReplay struct {
	ctx        context.Context
	envelopes  semanticEnvelopeCache
	verifier   *SemanticVerifier
	transition CandidateTransition
	candidate  *verifiedCandidate
	previous   *tlb.ShardStateUnsplit

	blockContext     *tvm.BlockContext
	inMessages       *tlb.InMsgDescrAugDict
	outMessages      *tlb.OutMsgDescrAugDict
	accountBlocks    *tlb.ShardAccountBlocksAugDict
	replayedAccounts *tlb.ShardAccountsAugDict

	// accountBlockIndex is the structural pass's decode of every AccountBlock.
	// It is frozen when that pass returns, so the precheck may read it from its
	// own goroutine while the queue preparation runs, and the lanes may read it
	// from GOMAXPROCS goroutines at once. Nothing in this replay writes it.
	accountBlockIndex *accountBlockIndex

	// shape accumulates what the candidate contains, from the walks this replay
	// already makes. It is handed to the candidate once, after the replay has
	// accepted the block, so a rejected candidate reports nothing.
	shape CandidateShape

	accounts          map[[32]byte]*semanticAccountResult
	consumedIn        map[cell.Hash]struct{}
	consumedOut       map[cell.Hash]struct{}
	specialTxs        semanticSpecialTransactions
	parsedIn          map[cell.Hash]*semanticInDescriptor
	parsedOut         map[cell.Hash]*semanticOutDescriptor
	parsedInOrder     []semanticInDescriptorEntry
	parsedOutOrder    []semanticOutDescriptorEntry
	messageProcessing []semanticMessageProcessing

	// specials is the epoch's masterchain special-account identities, shared with
	// Config.specials and with every other replay of the same config epoch. It is
	// read-only: a write here would be a write into the shared epoch data. It
	// stays zero on the shard path, where membership answers false.
	specials        masterSpecials
	blackholeBurned tlb.CurrencyCollection
	normalGasUsed   uint64
	specialGasUsed  uint64
	normalGasLimit  uint64
	specialGasLimit uint64
}

type semanticAccountResult struct {
	key        [32]byte
	original   *tlb.ShardAccount
	final      *tvm.PreparedAccount
	sequence   []semanticTransactionResult
	outputTags [][]uint8
}

// semanticSpecialTransactions is the complete masterchain special-message
// transaction set: fee recovery and mint, with duplicate roots collapsed.
// Keeping the protocol's fixed cardinality in the representation avoids a map
// allocation on every semantic replay.
type semanticSpecialTransactions struct {
	hashes [2]cell.Hash
	count  int
}

func (s *semanticSpecialTransactions) add(hash cell.Hash) {
	if s.contains(hash) || s.count == len(s.hashes) {
		return
	}
	s.hashes[s.count] = hash
	s.count++
}

func (s *semanticSpecialTransactions) contains(hash cell.Hash) bool {
	for i := 0; i < s.count; i++ {
		if s.hashes[i] == hash {
			return true
		}
	}

	return false
}

func newSemanticReplay(
	ctx context.Context,
	verifier *SemanticVerifier,
	transition CandidateTransition,
) (*semanticReplay, error) {
	prepared := transition.prepared
	candidate := prepared.candidate
	if transition.Config == nil || transition.Config.execution == nil {
		return nil, fmt.Errorf("%w: semantic verification config is absent", ErrInvalidInput)
	}

	// The structural verifier already parsed and augmentation-checked the three
	// block dictionaries once (see verifiedCandidate); reuse them instead of a
	// second ValidateAll walk per dictionary.
	inMessages := candidate.inMessages
	outMessages := candidate.outMessages
	if inMessages == nil || outMessages == nil {
		return nil, fmt.Errorf("%w: semantic descriptor dictionaries are absent", ErrInvalidInput)
	}
	if candidate.accountBlocks == nil || candidate.accountBlocks.AugmentedDictionary == nil {
		return nil, fmt.Errorf("%w: semantic account block dictionary is absent", ErrInvalidInput)
	}

	replay := &semanticReplay{
		ctx:               ctx,
		verifier:          verifier,
		transition:        transition,
		candidate:         candidate,
		previous:          prepared.previous,
		inMessages:        inMessages,
		outMessages:       outMessages,
		accountBlocks:     candidate.accountBlocks,
		accountBlockIndex: candidate.accountBlockIndex,
		replayedAccounts: &tlb.ShardAccountsAugDict{
			AugmentedDictionary: prepared.previous.Accounts.ShardAccounts.Copy(),
		},
		accounts:    make(map[[32]byte]*semanticAccountResult),
		consumedIn:  make(map[cell.Hash]struct{}),
		consumedOut: make(map[cell.Hash]struct{}),
	}
	if err := replay.loadMasterSpecialTransactions(); err != nil {
		return nil, err
	}
	if err := replay.prepareGasAccounting(); err != nil {
		return nil, err
	}
	blockContext, err := replay.newBlockContext()
	if err != nil {
		return nil, err
	}
	replay.blockContext = blockContext

	return replay, nil
}

// prepareGasAccounting binds the epoch gas allowance and, on the masterchain,
// the special-account set to this replay. Both are pure functions of the
// configuration root and are derived once per config epoch by PrepareConfig; all
// that is left here is choosing the chain and surfacing a carried rejection at
// the site that used to raise it.
func (r *semanticReplay) prepareGasAccounting() error {
	masterchain := !r.candidate.block.BlockInfo.NotMaster
	config := r.transition.Config

	gas := config.gas[0]
	if masterchain {
		gas = config.gas[1]
	}
	if gas.err != nil {
		return gas.err
	}
	r.normalGasLimit = gas.normal
	r.specialGasLimit = gas.special
	if !masterchain {
		return nil
	}
	if config.specials.err != nil {
		return config.specials.err
	}
	r.specials = config.specials

	return nil
}

func semanticGasLimit(hard, transaction uint64) (uint64, error) {
	if transaction > math.MaxUint64-hard {
		return 0, fmt.Errorf("%w: semantic gas limit overflow", ErrInvalidInput)
	}
	return hard + transaction, nil
}

func (r *semanticReplay) loadMasterSpecialTransactions() error {
	if r.candidate.block.BlockInfo.NotMaster || r.candidate.block.Extra.Custom == nil {
		return nil
	}

	details := r.candidate.block.Extra.Custom.Details
	for _, special := range [...]struct {
		name string
		root *cell.Cell
	}{
		{name: "fee recovery", root: details.RecoverCreateMsg},
		{name: "mint", root: details.MintMsg},
	} {
		if special.root == nil {
			continue
		}
		_, transactionRoot, err := semanticSpecialImportDescriptor(special.name, special.root)
		if err != nil {
			return err
		}
		r.specialTxs.add(transactionRoot.HashKey())
	}

	return nil
}

func (r *semanticReplay) newBlockContext() (*tvm.BlockContext, error) {
	header := &r.candidate.block.BlockInfo
	options := tvm.BlockOptions{
		Now:      header.GenUtime,
		BlockLT:  int64(header.StartLt),
		RandSeed: r.candidate.block.Extra.RandSeed,
		GlobalID: r.previous.GlobalID,
	}
	if header.NotMaster {
		if r.transition.Masterchain == nil {
			return nil, fmt.Errorf("%w: shard semantic transition has no masterchain context", ErrInvalidInput)
		}
		options.PrevBlocks = r.transition.Masterchain.PrevBlocks
		options.Libraries = r.transition.Masterchain.Libraries
	} else {
		prepared := r.transition.prepared
		if prepared.previousStats == nil || prepared.previousInfo == nil {
			return nil, fmt.Errorf("%w: masterchain predecessor views are absent", ErrInvalidInput)
		}
		prevBlocks, err := masterPrevBlocksTuple(
			r.transition.Previous.ID,
			prepared.previousInfo,
			r.transition.Config.globalVersion,
		)
		if err != nil {
			return nil, fmt.Errorf("build semantic masterchain previous-block tuple: %w", err)
		}
		options.PrevBlocks = prevBlocks
		libraries, err := masterExecutionLibraries(prepared.previousStats.Libraries)
		if err != nil {
			return nil, err
		}
		options.Libraries = libraries
	}

	blockContext, err := r.transition.Config.execution.NewBlockContext(options)
	if err != nil {
		return nil, fmt.Errorf("prepare semantic transaction context: %w", err)
	}

	return blockContext, nil
}

// semanticAccountLane is one account's slice of the replay. Everything a lane
// produces is returned here instead of being written into semanticReplay, so
// the replay itself stays read-only while lanes run and every cross-account
// effect is applied by a single goroutine in ascending account-key order.
type semanticAccountLane struct {
	key [32]byte
	// entry is the structural pass's decode of this account's block, shared and
	// immutable. Lane i owns lanes[i] and reads entries[i]; two lanes never hold
	// the same entry, so the AccountTransactionsAugDict inside it is read by one
	// goroutine — and even if it were not, aug-dict iteration allocates its own
	// cursor and mutates nothing, over cells whose hashes the BOC parse already
	// computed.
	entry *accountBlockEntry
	err   error

	result  *semanticAccountResult
	account accountLane
	// transactions is this account's share of the block's transaction count,
	// taken from the walk the lane already makes. Lanes run concurrently, so it
	// is summed by mergeAccountLanes rather than into a shared counter here.
	transactions uint32
	normalGas    uint64
	specialGas   uint64
	burned       tlb.CurrencyCollection
	processing   []semanticMessageProcessing
	consumedIn   []cell.Hash
	consumedOut  []cell.Hash
	inSeen       map[cell.Hash]struct{}
	outSeen      map[cell.Hash]struct{}
}

// precheckAccountUpdates mirrors ValidateQuery::precheck_account_updates. The
// structural diff is consensus-visible: in addition to changed leaves it
// validates every changed augmentation in the candidate trie. Replaying only
// the AccountBlocks and comparing the final root does not prove that all cells
// required by the reference validator were supplied in FullCollatedData.
func (r *semanticReplay) precheckAccountUpdates() error {
	oldAccounts := r.previous.Accounts.ShardAccounts
	newAccounts := r.candidate.state.Accounts.ShardAccounts
	if oldAccounts == nil || oldAccounts.AugmentedDictionary == nil ||
		newAccounts == nil || newAccounts.AugmentedDictionary == nil {
		return fmt.Errorf("%w: shard account dictionary is absent", ErrInvalidInput)
	}

	err := oldAccounts.ScanDiffRaw(newAccounts.AugmentedDictionary, true, func(view cell.AugDictDiffRawView) error {
		if err := r.ctx.Err(); err != nil {
			return err
		}

		var key [32]byte
		if view.KeyBits != 256 || len(view.Key) != len(key) {
			return fmt.Errorf("%w: changed account key is malformed", ErrInvalidInput)
		}
		copy(key[:], view.Key)

		// The structural pass had to decode every entry to reach its transaction
		// augmentation, so this is a binary search over what it recorded rather
		// than a second decode. The keyed descent below runs only when the index
		// cannot answer: a replay assembled without the structural pass, a trie
		// whose walk was not strictly ascending, or an account the candidate
		// changed without supplying an AccountBlock — which is the failure the
		// message names, reported by the same LoadValue that reported it before.
		entry, indexed := r.accountBlockIndex.find(key)
		if !indexed {
			var loadErr error
			if entry, loadErr = r.loadChangedAccountBlock(key); loadErr != nil {
				return fmt.Errorf("%w: changed account %x has no AccountBlock: %v", ErrInvalidInput, key, loadErr)
			}
		}
		if entry.exactErr != nil {
			return fmt.Errorf("%w: decode AccountBlock for changed account %x: %v", ErrInvalidInput, key, entry.exactErr)
		}
		if !bytes.Equal(entry.block.Addr, key[:]) {
			return fmt.Errorf("%w: AccountBlock address differs from changed account %x", ErrInvalidInput, key)
		}

		oldAccount, err := semanticDiffAccount(view.OldValueExtra, view.HasOld)
		if err != nil {
			return fmt.Errorf("%w: decode predecessor ShardAccount %x: %v", ErrInvalidInput, key, err)
		}
		newAccount, err := semanticDiffAccount(view.NewValueExtra, view.HasNew)
		if err != nil {
			return fmt.Errorf("%w: decode candidate ShardAccount %x: %v", ErrInvalidInput, key, err)
		}
		if view.HasNew {
			addr := address.NewAddress(0, byte(r.candidate.block.BlockInfo.Shard.WorkchainID), key[:])
			if _, err = tvm.PrepareAccount(newAccount, addr); err != nil {
				return fmt.Errorf("%w: validate candidate ShardAccount %x: %v", ErrInvalidInput, key, err)
			}
		}

		if entry.updateErr != nil {
			return fmt.Errorf("%w: decode AccountBlock state update %x: %v", ErrInvalidInput, key, entry.updateErr)
		}
		accountUpdate := &entry.update
		if !equalCellHashBytes(oldAccount.Account, accountUpdate.OldHash) {
			return fmt.Errorf("%w: AccountBlock %x old hash differs from predecessor state", ErrInvalidInput, key)
		}
		if !equalCellHashBytes(newAccount.Account, accountUpdate.NewHash) {
			return fmt.Errorf("%w: AccountBlock %x new hash differs from candidate state", ErrInvalidInput, key)
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, ErrInvalidInput) {
			return err
		}
		return fmt.Errorf("%w: invalid ShardAccounts dictionary difference: %v", ErrInvalidInput, err)
	}

	return nil
}

// loadChangedAccountBlock is the keyed descent precheckAccountUpdates made
// before the structural pass started keeping its decode. It is reached only when
// the index cannot serve a key, and it fills the same two deferred verdicts the
// index carries so the caller reports them from one place. Both are recorded
// rather than raised here: exactErr is reported before the address match and
// updateErr after it, which is the order the two rejections had.
func (r *semanticReplay) loadChangedAccountBlock(key [32]byte) (*accountBlockEntry, error) {
	var blockValue cell.Slice
	if err := r.accountBlocks.LoadValueByBytesKeyInto(key[:], &blockValue); err != nil {
		return nil, err
	}
	entry := &accountBlockEntry{}
	entry.exactErr = loadExactSlice(&entry.block, &blockValue)
	entry.updateErr = parseExact(&entry.update, entry.block.StateUpdate)

	return entry, nil
}

func semanticDiffAccount(valueExtra cell.Slice, present bool) (*tlb.ShardAccount, error) {
	if !present {
		return &tlb.ShardAccount{
			Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
			LastTransHash: make([]byte, 32),
		}, nil
	}

	value := valueExtra
	if err := (tlb.AugShardAccounts{}).SkipExtra(&value); err != nil {
		return nil, err
	}
	var account tlb.ShardAccount
	if err := loadExactSlice(&account, &value); err != nil {
		return nil, err
	}
	return &account, nil
}

func equalCellHashBytes(root *cell.Cell, expected []byte) bool {
	if len(expected) != 32 {
		return false
	}
	var hash cell.Hash
	copy(hash[:], expected)

	return root.HashKey() == hash
}

// verifyAccounts replays every account block. Accounts are independent — a lane
// reads only inputs that are already final when the loop starts — so the replay
// itself runs concurrently. Decoding stays serial
// because the dictionary iterator is stateful, and the merge is serial so the
// rejection reason never depends on scheduling.
func (r *semanticReplay) verifyAccounts() error {
	lanes, err := r.decodeAccountLanes()
	if err != nil {
		return err
	}
	r.replayAccountLanes(lanes)
	if err = r.mergeAccountLanes(lanes); err != nil {
		return err
	}

	if !equalCell(r.replayedAccounts.RootCell(), r.candidate.state.Accounts.ShardAccounts.RootCell()) {
		return fmt.Errorf("%w: replayed account dictionary differs from candidate state", ErrInvalidInput)
	}

	return nil
}

// decodeAccountLanes projects the structural pass's account-block index onto one
// lane per entry. The projection is positional — lane i is entry i is the i-th
// ascending account key — which is what makes mergeAccountLanes' "lowest key
// wins" ranking mechanical rather than re-derived. A malformed entry is recorded
// on its own lane and stops the projection rather than returning: keys ascend,
// so the sequential replay would never have reached a later account either, and
// deferring the error keeps it ranked by account key like any replay failure.
func (r *semanticReplay) decodeAccountLanes() ([]semanticAccountLane, error) {
	index := r.accountBlockIndex
	if index == nil {
		// Only a replay assembled without the structural verifier reaches this;
		// the production constructor sets accountBlocks and accountBlockIndex
		// from one verifyBlockDictionaries result, and a nil accountBlocks is
		// already rejected above it. Rebuilding rather than failing keeps a
		// hand-built replay's verdict the one it had — which is why the rebuild
		// defers the structural verdicts instead of raising them: raising would
		// replace a lane failure ranked by account key with an unranked
		// structural rejection in different words.
		var err error
		if index, err = buildAccountBlockIndexDeferred(r.accountBlocks); err != nil {
			return nil, fmt.Errorf("%w: iterate semantic account blocks: %v", ErrInvalidInput, err)
		}
	}
	// The duplicate check needs a set only when the walk was not proven strictly
	// ascending: a strictly ascending sequence is pairwise distinct, so for a
	// keyed index the set answers "no duplicate" for every entry and building it
	// is N insertions for a constant.
	var seen map[[32]byte]struct{}
	if !index.keyed {
		seen = make(map[[32]byte]struct{}, len(index.entries))
	}
	lanes := make([]semanticAccountLane, 0, len(index.entries))
	for i := range index.entries {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		entry := &index.entries[i]
		if entry.keyMalformed {
			return nil, fmt.Errorf("%w: semantic account block key is malformed", ErrInvalidInput)
		}
		lane := semanticAccountLane{key: entry.key, entry: entry}
		switch {
		case entry.decodeErr != nil, entry.exactErr != nil:
			lane.err = fmt.Errorf("%w: decode semantic account block %x", ErrInvalidInput, lane.key)
		case !bytes.Equal(entry.block.Addr, lane.key[:]):
			lane.err = fmt.Errorf("%w: semantic account block %x address differs from its key", ErrInvalidInput, lane.key)
		default:
			if _, duplicate := seen[lane.key]; duplicate {
				lane.err = fmt.Errorf("%w: duplicate semantic account block %x", ErrInvalidInput, lane.key)
			}
		}
		if seen != nil {
			seen[lane.key] = struct{}{}
		}
		lanes = append(lanes, lane)
		if lane.err != nil {
			return lanes, nil
		}
	}

	return lanes, nil
}

// replayAccountLanes runs the lanes concurrently and abandons only lanes with a
// strictly larger index than the first failure, which the sequential replay
// would not have reached either.
func (r *semanticReplay) replayAccountLanes(lanes []semanticAccountLane) {
	workers := min(collationParallelism, runtime.GOMAXPROCS(0), len(lanes))
	if workers < 2 {
		for i := range lanes {
			if lanes[i].err != nil {
				return
			}
			if lanes[i].err = r.replayAccount(&lanes[i]); lanes[i].err != nil {
				return
			}
		}

		return
	}

	var wg sync.WaitGroup
	var next atomic.Int64
	var firstFailed atomic.Int64
	firstFailed.Store(int64(len(lanes)))
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				i := next.Add(1) - 1
				if i >= int64(len(lanes)) || i >= firstFailed.Load() {
					return
				}
				lane := &lanes[i]
				if lane.err == nil {
					lane.err = r.replayAccount(lane)
				}
				if lane.err == nil {
					continue
				}
				for {
					current := firstFailed.Load()
					if i >= current || firstFailed.CompareAndSwap(current, i) {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
}

// mergeAccountLanes applies every cross-account effect in ascending account-key
// order, which is the only order the sequential replay ever had.
func (r *semanticReplay) mergeAccountLanes(lanes []semanticAccountLane) error {
	// A cancelled replay must not be reported as an invalid candidate: the
	// runtime retries a deadline but treats ErrInvalidInput as a consensus
	// reject, so cancellation wins over any lane verdict.
	if err := r.ctx.Err(); err != nil {
		return err
	}
	replayed := make([]cell.AugmentedEntry, 0, len(lanes))
	for i := range lanes {
		lane := &lanes[i]
		if lane.err != nil {
			return lane.err
		}
		for _, hash := range lane.consumedIn {
			if _, duplicate := r.consumedIn[hash]; duplicate {
				return fmt.Errorf("%w: inbound message %x is processed by more than one transaction", ErrInvalidInput, hash)
			}
			r.consumedIn[hash] = struct{}{}
		}
		for _, hash := range lane.consumedOut {
			if _, duplicate := r.consumedOut[hash]; duplicate {
				return fmt.Errorf("%w: outbound message %x is emitted more than once", ErrInvalidInput, hash)
			}
			r.consumedOut[hash] = struct{}{}
		}
		burned, err := r.blackholeBurned.Add(lane.burned)
		if err != nil {
			return fmt.Errorf("%w: aggregate blackhole burn: %v", ErrInvalidInput, err)
		}
		r.blackholeBurned = burned
		if err = addAggregateGas(&r.normalGasUsed, lane.normalGas, r.normalGasLimit); err != nil {
			return err
		}
		if err = addAggregateGas(&r.specialGasUsed, lane.specialGas, r.specialGasLimit); err != nil {
			return err
		}
		entry, deleted, err := accountDictionaryEntry(&lane.account)
		if err != nil {
			return fmt.Errorf("apply replayed account %x: %w", lane.key, err)
		}
		if deleted {
			if err = r.replayedAccounts.Delete(entry.Key); err != nil {
				return fmt.Errorf("apply replayed account %x: %w", lane.key, err)
			}
		} else if entry.Value != nil {
			replayed = append(replayed, entry)
		}
		r.accounts[lane.key] = lane.result
		r.shape.Transactions += lane.transactions
		r.messageProcessing = append(r.messageProcessing, lane.processing...)
	}

	// One descent for the whole replay: the accounts are already in ascending
	// key order, and the resulting dictionary is the one repeated writes build.
	if err := r.replayedAccounts.SetMany(replayed, collationParallelism); err != nil {
		return fmt.Errorf("%w: apply replayed accounts: %v", ErrInvalidInput, err)
	}
	return nil
}

// addAggregateGas holds the block-wide allowance. The counter only grows, so a
// counter already over the allowance proves the block is over: charging here
// aborts at the crossing transaction instead of replaying the rest of a block
// that is already invalid, and stops a later failure in the same lane from
// masking the gas verdict.
func addAggregateGas(counter *uint64, used, limit uint64) error {
	if used > math.MaxUint64-*counter {
		return fmt.Errorf("%w: aggregate gas counter overflow", ErrInvalidInput)
	}
	*counter += used
	if *counter > limit {
		return fmt.Errorf("%w: aggregate gas usage %d exceeds validator allowance %d", ErrInvalidInput, *counter, limit)
	}

	return nil
}

// replayAccount replays one account block. It reads only inputs that are final
// before the lanes start and writes exclusively through lane.
func (r *semanticReplay) replayAccount(lane *semanticAccountLane) error {
	// Not a candidate check: decodeAccountLanes sets entry on every lane it
	// produces, and nothing else feeds this. It is here because a nil dereference
	// on the validation path takes the node down, and a rejected candidate does
	// not.
	if lane.entry == nil {
		return fmt.Errorf("%w: semantic account lane %x carries no account block", ErrInvalidInput, lane.key)
	}
	key, entry := lane.key, lane.entry
	block := &entry.block
	header := &r.candidate.block.BlockInfo
	owner := msgpool.ShardIdent{
		Workchain: header.Shard.WorkchainID,
		Shard:     uint64(header.Shard.GetShardID()),
	}
	if !owner.Contains(header.Shard.WorkchainID, binary.BigEndian.Uint64(key[:8])) {
		return fmt.Errorf("%w: account block %x is outside the candidate shard", ErrInvalidInput, key)
	}

	// The structural diff above authenticates the predecessor leaf but is not
	// an execution view. Load the account from the predecessor dictionary just
	// before replay, matching ValidateQuery::CheckAccountTxs::unpack_account in
	// the reference validator. In particular, this avoids handing TVM a
	// traced/virtualized value borrowed from the diff walk over a lazy state.
	original, originallyExists, err := semanticLoadAccount(r.previous.Accounts.ShardAccounts, key)
	if err != nil {
		return err
	}
	addr := address.NewAddress(0, byte(r.candidate.block.BlockInfo.Shard.WorkchainID), key[:])
	current, err := tvm.PrepareAccount(original, addr)
	if err != nil {
		return fmt.Errorf("%w: prepare semantic account %x: %v", ErrInvalidInput, key, err)
	}
	if originallyExists && current.State().LastTransactionLT >= header.StartLt {
		return fmt.Errorf("%w: account %x has a predecessor transaction at or after block start", ErrInvalidInput, key)
	}

	// The structural pass parsed this same cell; the verdict is re-worded here
	// because that is the message this rejection has always carried.
	if entry.updateErr != nil {
		return fmt.Errorf("%w: decode account block state update %x: %v", ErrInvalidInput, key, entry.updateErr)
	}
	accountUpdate := &entry.update
	if !equalCellHashBytes(original.Account, accountUpdate.OldHash) {
		return fmt.Errorf("%w: account block %x old hash differs from predecessor state", ErrInvalidInput, key)
	}

	if err = r.bindInitialStorageStat(current); err != nil {
		return fmt.Errorf("%w: account %x storage proof: %v", ErrInvalidInput, key, err)
	}
	if block.Transactions == nil || block.Transactions.AugmentedDictionary == nil {
		return fmt.Errorf("%w: account block %x transaction dictionary is absent", ErrInvalidInput, key)
	}
	iterator, err := block.Transactions.IteratorExtra(false, false)
	if err != nil {
		return fmt.Errorf("%w: iterate transactions of account %x: %v", ErrInvalidInput, key, err)
	}

	var transactions uint32
	var sequence []semanticTransactionResult
	var outputTags [][]uint8
	var transaction tlb.TransactionLean
	nextTransactionLT := current.State().LastTransactionLT + 1
	for iterator.Next() {
		if err = r.ctx.Err(); err != nil {
			return err
		}
		item := iterator.View()
		lt, keyErr := item.Key.LoadUInt(64)
		if keyErr != nil || item.Key.BitsLeft() != 0 || item.Key.RefsNum() != 0 {
			return fmt.Errorf("%w: transaction key of account %x is malformed", ErrInvalidInput, key)
		}
		value := item.Value
		root, loadErr := value.LoadRefCell()
		if loadErr != nil || value.BitsLeft() != 0 || value.RefsNum() != 0 {
			return fmt.Errorf("%w: transaction value of account %x is malformed", ErrInvalidInput, key)
		}
		if err = parseSemanticTransaction(&transaction, root); err != nil {
			return fmt.Errorf("%w: decode transaction %x:%d: %v", ErrInvalidInput, key, lt, err)
		}
		if transaction.LT != lt || transaction.AccountAddr != key {
			return fmt.Errorf("%w: transaction %x:%d identity differs from its dictionary key", ErrInvalidInput, key, lt)
		}
		if transaction.Now != header.GenUtime {
			return fmt.Errorf("%w: transaction %x:%d time differs from block time", ErrInvalidInput, key, lt)
		}
		ltLength := uint64(transaction.OutMsgCount) + 1
		if transaction.LT <= header.StartLt || transaction.LT >= header.EndLt ||
			ltLength > header.EndLt-transaction.LT {
			return fmt.Errorf("%w: transaction %x:%d lies outside the block logical-time interval", ErrInvalidInput, key, lt)
		}
		if transaction.LT < nextTransactionLT {
			return fmt.Errorf("%w: transaction %x:%d overlaps the previous transaction", ErrInvalidInput, key, lt)
		}
		currentShardAccount := current.ShardAccount()
		if transaction.PrevTxLT != currentShardAccount.LastTransLT ||
			!bytes.Equal(transaction.PrevTxHash[:], currentShardAccount.LastTransHash) {
			return fmt.Errorf("%w: transaction %x:%d previous transaction pointer mismatch", ErrInvalidInput, key, lt)
		}
		if currentShardAccount.Account.HashKey() != cell.Hash(transaction.OldHash) {
			return fmt.Errorf("%w: transaction %x:%d old account hash mismatch", ErrInvalidInput, key, lt)
		}
		inboundRoot := transaction.InMsg

		beforeActive, tickEnabled, tockEnabled := semanticAccountTickTock(current.State())
		isTickTock := transaction.Kind == tlb.TransactionKindTickTock
		transactionResult := semanticTransactionResult{
			lt:           transaction.LT,
			tickTock:     isTickTock,
			isTock:       isTickTock && transaction.IsTock,
			beforeActive: beforeActive,
			tickEnabled:  tickEnabled,
			tockEnabled:  tockEnabled,
		}

		result, inboundMessage, replayErr := r.replayTransaction(current, inboundRoot, &transaction)
		if replayErr != nil {
			return fmt.Errorf("replay transaction %x:%d: %w", key, lt, replayErr)
		}
		if result.StorageStatRecomputed && r.verifier.onStorageStatRecompute != nil {
			chain := MetricChainShardchain
			if !header.NotMaster {
				chain = MetricChainMasterchain
			}
			r.verifier.onStorageStatRecompute(chain, key)
		}
		transactionResult.afterActive = result.NextAccount.State().Status == tlb.AccountStatusActive
		if result.TransactionCell.HashKey() != root.HashKey() {
			missingLibrary := "none"
			if result.MissingLibrary != nil {
				missingLibrary = fmt.Sprintf("%x", *result.MissingLibrary)
			}
			return fmt.Errorf(
				"%w: transaction %x:%d hash differs from deterministic replay: missing_library=%s, expected %s, got %s",
				ErrInvalidInput,
				key,
				lt,
				missingLibrary,
				semanticTransactionFingerprint(root),
				semanticTransactionFingerprint(result.TransactionCell),
			)
		}
		if result.StartLT != transaction.LT || result.EndLT != transaction.LT+1+uint64(transaction.OutMsgCount) {
			return fmt.Errorf("%w: transaction %x:%d logical time range differs from replay", ErrInvalidInput, key, lt)
		}
		if len(result.OutMessages) != int(transaction.OutMsgCount) {
			return fmt.Errorf("%w: transaction %x:%d output count differs from replay", ErrInvalidInput, key, lt)
		}
		if result.NextAccount.ShardAccount().Account.HashKey() != cell.Hash(transaction.NewHash) {
			return fmt.Errorf("%w: transaction %x:%d new account hash differs from replay", ErrInvalidInput, key, lt)
		}
		if err = r.recordTransactionGas(lane, root, &transaction, result); err != nil {
			return fmt.Errorf("transaction %x:%d gas: %w", key, lt, err)
		}
		tags, descriptorErr := r.verifyTransactionDescriptors(lane, root, inboundRoot, current, &transaction, inboundMessage, result)
		if descriptorErr != nil {
			err = descriptorErr
			return fmt.Errorf("transaction %x:%d descriptors: %w", key, lt, err)
		}

		sequence = append(sequence, transactionResult)
		outputTags = append(outputTags, tags)
		current = result.NextAccount
		nextTransactionLT = transaction.LT + ltLength
		if transactions == math.MaxUint32 {
			return fmt.Errorf("%w: account %x transaction count overflow", ErrInvalidInput, key)
		}
		transactions++
	}
	if err = iterator.Err(); err != nil {
		return fmt.Errorf("%w: iterate transactions of account %x: %v", ErrInvalidInput, key, err)
	}
	if transactions == 0 {
		return fmt.Errorf("%w: account block %x has no transactions", ErrInvalidInput, key)
	}
	if !equalCellHashBytes(current.ShardAccount().Account, accountUpdate.NewHash) {
		return fmt.Errorf("%w: account block %x new hash differs from replay", ErrInvalidInput, key)
	}

	lane.account = accountLane{
		key:              key,
		original:         original,
		current:          current,
		originallyExists: originallyExists,
	}
	lane.transactions = transactions
	lane.result = &semanticAccountResult{
		key:        key,
		original:   original,
		final:      current,
		sequence:   sequence,
		outputTags: outputTags,
	}

	return nil
}

func (r *semanticReplay) recordTransactionGas(
	lane *semanticAccountLane,
	transactionRoot *cell.Cell,
	transaction *tlb.TransactionLean,
	result *tvm.TransactionExecutionResult,
) error {
	account := lane.key
	burned, err := lane.burned.Add(result.Burned)
	if err != nil {
		return fmt.Errorf("%w: aggregate blackhole burn: %v", ErrInvalidInput, err)
	}
	lane.burned = burned

	if result.GasUsed < 0 {
		return fmt.Errorf("%w: deterministic replay returned negative gas usage", ErrInvalidInput)
	}
	if transaction.Kind != tlb.TransactionKindOrdinary {
		return nil
	}
	if r.specialTxs.contains(transactionRoot.HashKey()) {
		return nil
	}
	_, specialAccount := r.specials.set[account]
	if !specialAccount && semanticGasLimitOverridden(
		r.candidate.block.BlockInfo.Shard.WorkchainID,
		account,
		r.transition.Config.globalVersion,
		r.candidate.block.BlockInfo.GenUtime,
	) {
		return nil
	}

	// A lane charges against its own share of the block allowance; blocks that
	// only cross it once the lanes are summed are caught by mergeAccountLanes.
	if specialAccount {
		return addAggregateGas(&lane.specialGas, uint64(result.GasUsed), r.specialGasLimit)
	}

	return addAggregateGas(&lane.normalGas, uint64(result.GasUsed), r.normalGasLimit)
}

type semanticGasOverride struct {
	account     *address.Address
	fromVersion uint32
	until       uint32
}

// These expired mainnet exceptions are consensus-visible when historical
// blocks are replayed: their transactions are omitted from block gas totals.
var semanticGasOverrides = [...]semanticGasOverride{
	{
		account:     address.MustParseRawAddr("0:FFBFD8F5AE5B2E1C7C3614885CB02145483DFAEE575F0DD08A72C366369211CD"),
		fromVersion: 5,
		until:       1_709_164_800,
	},
	{
		account:     address.MustParseRawAddr("0:5E4A5F9DBA638789E6770C990D2959237ACA3BC19D15A734782C26CB19343CC6"),
		fromVersion: 9,
		until:       1_740_787_200,
	},
	{
		account:     address.MustParseRawAddr("0:B755C43EE37925C30F547E2991E7C4C18C1CE4EC63EEA5743708DBAD868369FA"),
		fromVersion: 9,
		until:       1_740_787_200,
	},
	{
		account:     address.MustParseRawAddr("0:61C016FC8EFA241AF7EB787451A1E571236DFB3EB389832AEC0212C0FB8AC10B"),
		fromVersion: 9,
		until:       1_740_787_200,
	},
	{
		account:     address.MustParseRawAddr("0:A4A11A78384F92154A0C12761F2F7BC5E374F703335F5BC8F24C2E32CE4F1C26"),
		fromVersion: 9,
		until:       1_740_787_200,
	},
	{
		account:     address.MustParseRawAddr("0:4DE480AB6ACEFD53C158126EF5C2CDF89FE64D210D0B44DA5C90E52C215DCE79"),
		fromVersion: 9,
		until:       1_740_787_200,
	},
	{
		account:     address.MustParseRawAddr("0:436A76C2794A88E3FBFEC6B9C0374FC8DB046F10868B835420D9937973A665D4"),
		fromVersion: 9,
		until:       1_740_787_200,
	},
}

func semanticGasLimitOverridden(workchain int32, account [32]byte, version, now uint32) bool {
	if workchain != 0 || now >= 1_740_787_200 {
		return false
	}
	for _, override := range semanticGasOverrides {
		if version >= override.fromVersion && now < override.until && bytes.Equal(account[:], override.account.Data()) {
			return true
		}
	}

	return false
}

func semanticLoadAccount(
	accounts *tlb.ShardAccountsAugDict,
	key [32]byte,
) (*tlb.ShardAccount, bool, error) {
	var value cell.Slice
	err := accounts.LoadValueByBytesKeyInto(key[:], &value)
	if isMissingKey(err) {
		return &tlb.ShardAccount{
			Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
			LastTransHash: make([]byte, 32),
		}, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("%w: load predecessor account %x: %v", ErrInvalidInput, key, err)
	}

	var account tlb.ShardAccount
	if err = loadExactSlice(&account, &value); err != nil {
		return nil, false, fmt.Errorf("%w: decode predecessor account %x: %v", ErrInvalidInput, key, err)
	}

	return &account, true, nil
}

func (r *semanticReplay) bindInitialStorageStat(account *tvm.PreparedAccount) error {
	extra, ok := account.State().StorageInfo.StorageExtra.(tlb.StorageExtraInfo)
	if !ok || len(extra.DictHash) != 32 || !r.transition.FullCollatedData {
		return nil
	}
	var hash cell.Hash
	copy(hash[:], extra.DictHash)
	root, err := r.transition.CollatedProofs.AccountStorageRoot(hash)
	if errors.Is(err, ErrCollatedRootNotFound) {
		// This proof is only a cache optimization: its absence is valid and makes
		// the executor calculate the storage stat directly.
		return nil
	}
	if err != nil {
		return err
	}

	return r.blockContext.BindAccountStorageStat(account, root)
}

func (r *semanticReplay) replayTransaction(
	account *tvm.PreparedAccount,
	inboundRoot *cell.Cell,
	transaction *tlb.TransactionLean,
) (*tvm.TransactionExecutionResult, *tlb.Message, error) {
	if transaction.LT > math.MaxInt64 {
		return nil, nil, fmt.Errorf("%w: transaction logical time exceeds signed TVM range", ErrInvalidInput)
	}
	options := tvm.TransactionOptions{LogicalTime: int64(transaction.LT)}

	// The inbound message is parsed once, by PrepareMessage, and that parse is
	// handed back to the caller; the recorded transaction carries the same cell,
	// so a separate parse of it would decode identical bytes a second time.
	var inboundMessage *tlb.Message
	var result *tvm.TransactionExecutionResult
	var err error
	switch transaction.Kind {
	case tlb.TransactionKindOrdinary:
		if transaction.InMsg == nil || inboundRoot == nil {
			return nil, nil, fmt.Errorf("%w: ordinary transaction has no inbound message", ErrInvalidInput)
		}
		message, prepareErr := tvm.PrepareMessage(inboundRoot)
		if prepareErr != nil {
			return nil, nil, fmt.Errorf("%w: prepare inbound message: %v", ErrInvalidInput, prepareErr)
		}
		inboundMessage = message.Message()
		result, err = r.verifier.machine.EmulateTransaction(r.blockContext, account, message, options)
	case tlb.TransactionKindTickTock:
		if transaction.InMsg != nil {
			return nil, nil, fmt.Errorf("%w: tick/tock transaction has an inbound message", ErrInvalidInput)
		}
		if r.candidate.block.BlockInfo.NotMaster {
			return nil, nil, fmt.Errorf("%w: shard tick/tock transaction is not produced by Builder", ErrUnsupported)
		}
		result, err = r.verifier.machine.EmulateTickTockTransaction(
			r.blockContext,
			account,
			transaction.IsTock,
			options,
		)
	default:
		return nil, nil, fmt.Errorf(
			"%w: transaction type %s is not produced by Builder",
			ErrUnsupported,
			transaction.Kind,
		)
	}
	if err != nil {
		return nil, nil, classifySemanticExecutionError(err)
	}
	if inboundMessage != nil && inboundMessage.MsgType == tlb.MsgTypeExternalIn &&
		(result == nil || !result.Accepted) {
		return nil, nil, fmt.Errorf("%w: external message was not accepted", ErrInvalidInput)
	}
	if result == nil || result.TransactionCell == nil || result.NextAccount == nil {
		return nil, nil, fmt.Errorf("%w: TVM returned an incomplete transaction result", ErrInvalidInput)
	}
	return result, inboundMessage, nil
}

func semanticTransactionFingerprint(root *cell.Cell) string {
	refs := make([]cell.Hash, root.RefsNum())
	for i := range refs {
		refs[i] = root.MustRefHashAt(i)
	}

	return fmt.Sprintf("hash=%x bits=%s refs=%x", root.Hash(), root.DumpBits(), refs)
}

func classifySemanticExecutionError(err error) error {
	if code, ok := vmerr.ErrorCode(err); ok && code == vmerr.CodeVirtualization {
		return fmt.Errorf("%w: candidate proof contains an unresolved pruned branch: %v", ErrInvalidInput, err)
	}

	return fmt.Errorf("%w: TVM execution failed: %v", ErrSemanticExecution, err)
}

func (r *semanticReplay) verifyTransactionDescriptors(
	lane *semanticAccountLane,
	transactionRoot *cell.Cell,
	inboundRoot *cell.Cell,
	previousAccount *tvm.PreparedAccount,
	transaction *tlb.TransactionLean,
	inboundMessage *tlb.Message,
	result *tvm.TransactionExecutionResult,
) ([]uint8, error) {
	account := lane.key
	var inboundMetadata *tlb.MsgMetadata
	moneyImported := tlb.CurrencyCollection{}
	moneyExported := tlb.CurrencyCollection{}
	externalInitiated := false
	specialTransaction := r.specialTxs.contains(transactionRoot.HashKey())
	if inboundMessage != nil {
		binding, err := r.verifyInboundTransactionDescriptor(lane, inboundRoot, transactionRoot)
		if err != nil {
			return nil, err
		}
		switch inboundMessage.MsgType {
		case tlb.MsgTypeExternalIn:
			if binding.tag != 0b000 || binding.envelope != nil {
				return nil, fmt.Errorf("%w: inbound external message uses a non-external descriptor", ErrInvalidInput)
			}
			if err = semanticMessageAccount(inboundMessage.Msg.DestAddr(), account, r.candidate.block.BlockInfo.Shard.WorkchainID); err != nil {
				return nil, fmt.Errorf("inbound destination: %w", err)
			}
			externalInitiated = true
		case tlb.MsgTypeInternal:
			if binding.envelope == nil {
				return nil, fmt.Errorf("%w: inbound internal message descriptor has no envelope", ErrInvalidInput)
			}
			internal := inboundMessage.AsInternal()
			moneyImported, err = semanticInternalMessageValue(
				internal,
				tlb.Coins{},
				r.transition.Config.globalVersion,
			)
			if err != nil {
				return nil, fmt.Errorf("%w: calculate imported message value: %v", ErrInvalidInput, err)
			}
			if err = semanticMessageAccount(internal.DstAddr, account, r.candidate.block.BlockInfo.Shard.WorkchainID); err != nil {
				return nil, fmt.Errorf("inbound destination: %w", err)
			}
			emittedLT := internal.CreatedLT
			if binding.envelope.value.EmittedLT != nil {
				emittedLT = *binding.envelope.value.EmittedLT
			}
			if emittedLT >= transaction.LT {
				return nil, fmt.Errorf("%w: inbound message emitted at %d is not earlier than transaction %d", ErrInvalidInput, emittedLT, transaction.LT)
			}
			if internal.CreatedLT != r.candidate.block.BlockInfo.StartLt || !specialTransaction {
				lane.processing = append(lane.processing, semanticMessageProcessing{
					account:       account,
					transactionLT: transaction.LT,
					messageLT:     emittedLT,
				})
			}
			inboundMetadata = binding.envelope.value.Metadata
		default:
			return nil, fmt.Errorf("%w: transaction has unsupported inbound message type %s", ErrInvalidInput, inboundMessage.MsgType)
		}
	} else if transaction.Kind == tlb.TransactionKindTickTock {
		externalInitiated = true
	}
	if specialTransaction {
		externalInitiated = true
	}

	expectedMetadata := r.transactionOutputMetadata(account, transaction.LT, inboundMetadata, externalInitiated)
	tags := make([]uint8, 0, len(result.OutMessages))
	for i := range result.OutMessages {
		output := &result.OutMessages[i]
		binding, err := r.verifyOutboundTransactionDescriptor(lane, output.Cell, transactionRoot)
		if err != nil {
			return nil, fmt.Errorf("output %d: %w", i, err)
		}
		tags = append(tags, uint8(binding.tag))

		switch output.Msg.MsgType {
		case tlb.MsgTypeExternalOut:
			if binding.tag != 0b000 || binding.envelope != nil {
				return nil, fmt.Errorf("output %d: %w: outbound external message uses a non-external descriptor", i, ErrInvalidInput)
			}
			if err = semanticMessageAccount(output.Msg.Msg.SenderAddr(), account, r.candidate.block.BlockInfo.Shard.WorkchainID); err != nil {
				return nil, fmt.Errorf("output %d source: %w", i, err)
			}
		case tlb.MsgTypeInternal:
			if binding.envelope == nil {
				return nil, fmt.Errorf("output %d: %w: outbound internal message descriptor has no envelope", i, ErrInvalidInput)
			}
			if err = semanticMessageAccount(output.Msg.Msg.SenderAddr(), account, r.candidate.block.BlockInfo.Shard.WorkchainID); err != nil {
				return nil, fmt.Errorf("output %d source: %w", i, err)
			}
			value, valueErr := semanticInternalMessageValue(
				output.Msg.AsInternal(),
				binding.envelope.value.FwdFeeRemaining,
				r.transition.Config.globalVersion,
			)
			if valueErr != nil {
				return nil, fmt.Errorf("output %d: %w: calculate exported message value: %v", i, ErrInvalidInput, valueErr)
			}
			moneyExported, valueErr = moneyExported.Add(value)
			if valueErr != nil {
				return nil, fmt.Errorf("output %d: %w: aggregate exported message value: %v", i, ErrInvalidInput, valueErr)
			}
			if !semanticMetadataEqual(binding.envelope.value.Metadata, expectedMetadata) {
				return nil, fmt.Errorf("output %d: %w: outbound message metadata differs from deterministic chain", i, ErrInvalidInput)
			}
		default:
			return nil, fmt.Errorf("output %d: %w: transaction emitted unsupported message type %s", i, ErrInvalidInput, output.Msg.MsgType)
		}
	}
	if err := verifySemanticTransactionValueFlow(
		semanticPreparedAccountBalance(previousAccount),
		semanticPreparedAccountBalance(result.NextAccount),
		moneyImported,
		moneyExported,
		transaction.TotalFees,
		result.Burned,
	); err != nil {
		return nil, err
	}

	return tags, nil
}

func semanticInternalMessageValue(
	message *tlb.InternalMessage,
	envelopeFee tlb.Coins,
	globalVersion uint32,
) (tlb.CurrencyCollection, error) {
	value := tlb.CurrencyCollection{
		Coins:           message.Amount,
		ExtraCurrencies: message.ExtraCurrencies,
	}
	var err error
	if globalVersion < 12 {
		value, err = value.Add(tlb.CurrencyCollection{Coins: message.IHRFee})
		if err != nil {
			return tlb.CurrencyCollection{}, err
		}
	}

	return value.Add(tlb.CurrencyCollection{Coins: envelopeFee})
}

func verifySemanticTransactionValueFlow(
	previousBalance tlb.CurrencyCollection,
	nextBalance tlb.CurrencyCollection,
	imported tlb.CurrencyCollection,
	exported tlb.CurrencyCollection,
	totalFees tlb.CurrencyCollection,
	burned tlb.CurrencyCollection,
) error {
	left, err := previousBalance.Add(imported)
	if err != nil {
		return fmt.Errorf("%w: add transaction imported value: %v", ErrInvalidInput, err)
	}
	right, err := nextBalance.Add(exported)
	if err != nil {
		return fmt.Errorf("%w: add transaction exported value: %v", ErrInvalidInput, err)
	}
	right, err = right.Add(totalFees)
	if err != nil {
		return fmt.Errorf("%w: add transaction fees: %v", ErrInvalidInput, err)
	}
	right, err = right.Add(burned)
	if err != nil {
		return fmt.Errorf("%w: add transaction burned value: %v", ErrInvalidInput, err)
	}
	if !left.Equals(right) {
		return fmt.Errorf(
			"%w: transaction currency flow mismatch: previous %s + imported %s != next %s + exported %s + fees %s + burned %s",
			ErrInvalidInput,
			previousBalance.Coins.String(),
			imported.Coins.String(),
			nextBalance.Coins.String(),
			exported.Coins.String(),
			totalFees.Coins.String(),
			burned.Coins.String(),
		)
	}

	return nil
}

func semanticPreparedAccountBalance(account *tvm.PreparedAccount) tlb.CurrencyCollection {
	state := account.State()
	return tlb.CurrencyCollection{
		Coins:           state.Balance,
		ExtraCurrencies: state.ExtraCurrencies,
	}
}

func semanticTransactionInbound(root *cell.Cell, expected bool) (*cell.Cell, error) {
	io, err := root.PeekRef(0)
	if err != nil {
		return nil, fmt.Errorf("load transaction IO: %v", err)
	}
	var loader cell.Slice
	err = io.BeginParseInto(&loader)
	if err != nil {
		return nil, fmt.Errorf("parse transaction IO: %v", err)
	}
	present, err := loader.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load transaction inbound flag: %v", err)
	}
	if present != expected {
		return nil, errors.New("parsed and exact inbound message presence differs")
	}
	if !present {
		return nil, nil
	}
	message, err := loader.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load exact inbound message: %v", err)
	}

	return message, nil
}

func (r *semanticReplay) transactionOutputMetadata(
	account [32]byte,
	transactionLT uint64,
	inbound *tlb.MsgMetadata,
	externalInitiated bool,
) *tlb.MsgMetadata {
	if r.transition.Config.capabilities&capMsgMetadata == 0 {
		return nil
	}
	if externalInitiated {
		return &tlb.MsgMetadata{
			Initiator:   address.NewAddress(0, byte(r.candidate.block.BlockInfo.Shard.WorkchainID), account[:]),
			InitiatorLT: transactionLT,
		}
	}
	if inbound == nil {
		return nil
	}

	metadata := *inbound
	metadata.Depth++
	return &metadata
}

func semanticMetadataEqual(left, right *tlb.MsgMetadata) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.Initiator == nil || right.Initiator == nil {
		return left.Initiator == nil && right.Initiator == nil &&
			left.Depth == right.Depth && left.InitiatorLT == right.InitiatorLT
	}

	return left.Depth == right.Depth && left.InitiatorLT == right.InitiatorLT &&
		left.Initiator.Equals(right.Initiator)
}

func semanticMessageAccount(addr *address.Address, account [32]byte, workchain int32) error {
	if addr == nil || addr.Workchain() != workchain {
		return fmt.Errorf("%w: message account workchain differs from candidate shard", ErrInvalidInput)
	}
	actual, err := semanticAccountIDFromAddress(addr)
	if err != nil {
		return err
	}
	if actual != account {
		return fmt.Errorf("%w: message account differs from transaction account", ErrInvalidInput)
	}

	return nil
}

// semanticAccountIDFromAddress extracts the standard account id, with rewriting
// enabled. Both 256-bit addr_std and addr_var are valid on the wire.
func semanticAccountIDFromAddress(addr *address.Address) ([32]byte, error) {
	if addr == nil || addr.BitsLen() != 256 || len(addr.Data()) != 32 ||
		(addr.Type() != address.StdAddress && addr.Type() != address.VarAddress) {
		return [32]byte{}, fmt.Errorf("%w: account address is not 256-bit std or var", ErrInvalidInput)
	}

	var account [32]byte
	copy(account[:], addr.Data())
	if err := msgpool.RewriteAnycast(account[:], addr); err != nil {
		return [32]byte{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	return account, nil
}

// verifyInboundTransactionDescriptor resolves the descriptor the queue phase
// already decoded. prepareQueueValidation fills r.parsedIn before the lanes
// start, so the decoded map is the single source of descriptor semantics: a
// second parser here would be free to disagree with it on trailing data, on
// error classification, and on the tag it reports for deferred imports.
func (r *semanticReplay) verifyInboundTransactionDescriptor(
	lane *semanticAccountLane,
	message, transaction *cell.Cell,
) (*semanticInDescriptor, error) {
	hash := message.HashKey()
	// A lane sees only its own account; a message claimed by two accounts is
	// caught by mergeAccountLanes, which walks lanes in account-key order.
	if _, consumed := lane.inSeen[hash]; consumed {
		return nil, fmt.Errorf("%w: inbound message %x is processed by more than one transaction", ErrInvalidInput, hash)
	}
	binding := r.parsedIn[hash]
	if binding == nil {
		return nil, fmt.Errorf("%w: inbound message %x has no descriptor", ErrInvalidInput, hash)
	}
	if binding.message == nil || binding.transaction == nil ||
		binding.message.HashKey() != hash || binding.transaction.HashKey() != transaction.HashKey() {
		return nil, fmt.Errorf("%w: inbound message %x descriptor binding mismatch", ErrInvalidInput, hash)
	}
	if lane.inSeen == nil {
		lane.inSeen = make(map[cell.Hash]struct{})
	}
	lane.inSeen[hash] = struct{}{}
	lane.consumedIn = append(lane.consumedIn, hash)

	return binding, nil
}

func (r *semanticReplay) verifyOutboundTransactionDescriptor(
	lane *semanticAccountLane,
	message, transaction *cell.Cell,
) (*semanticOutDescriptor, error) {
	hash := message.HashKey()
	if _, consumed := lane.outSeen[hash]; consumed {
		return nil, fmt.Errorf("%w: outbound message %x is emitted more than once", ErrInvalidInput, hash)
	}
	binding := r.parsedOut[hash]
	if binding == nil {
		return nil, fmt.Errorf("%w: outbound message %x has no descriptor", ErrInvalidInput, hash)
	}
	if binding.message == nil || binding.transaction == nil ||
		binding.message.HashKey() != hash || binding.transaction.HashKey() != transaction.HashKey() {
		return nil, fmt.Errorf("%w: outbound message %x descriptor binding mismatch", ErrInvalidInput, hash)
	}
	if lane.outSeen == nil {
		lane.outSeen = make(map[cell.Hash]struct{})
	}
	lane.outSeen[hash] = struct{}{}
	lane.consumedOut = append(lane.consumedOut, hash)

	return binding, nil
}
