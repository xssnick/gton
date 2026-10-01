package collator

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type semanticTransactionResult struct {
	lt           uint64
	tickTock     bool
	isTock       bool
	beforeActive bool
	afterActive  bool
	tickEnabled  bool
	tockEnabled  bool
}

type semanticMessageProcessing struct {
	account       [32]byte
	transactionLT uint64
	messageLT     uint64
}

type semanticMessageEmission struct {
	source    [32]byte
	createdLT uint64
	emittedLT uint64
}

func (r *semanticReplay) verifyMasterSemantics() error {
	if err := r.verifyMessageProcessingOrder(); err != nil {
		return err
	}
	if r.candidate.block.BlockInfo.NotMaster {
		return nil
	}
	if err := r.verifyMasterTickTock(); err != nil {
		return err
	}
	if err := r.verifyMasterSpecialMessages(); err != nil {
		return err
	}
	if err := r.verifyMasterPublicLibraries(); err != nil {
		return err
	}
	if err := r.verifyMasterBurnedValue(); err != nil {
		return err
	}

	return nil
}

func (r *semanticReplay) verifyMessageProcessingOrder() error {
	processed, emitted, err := r.loadSemanticMessageOrder()
	if err != nil {
		return err
	}

	return checkSemanticMessageOrder(processed, emitted)
}

func (r *semanticReplay) loadSemanticMessageOrder() (
	[]semanticMessageProcessing,
	[]semanticMessageEmission,
	error,
) {
	if r.parsedIn == nil || r.parsedOut == nil {
		return nil, nil, fmt.Errorf("%w: parsed message descriptors are absent", ErrInvalidInput)
	}

	processed := r.messageProcessing
	var emitted []semanticMessageEmission

	for _, entry := range r.parsedInOrder {
		hash, descriptor := entry.hash, entry.descriptor
		if err := r.ctx.Err(); err != nil {
			return nil, nil, err
		}
		if descriptor.tag == semanticInDeferredFinal {
			source, sourceErr := semanticAccountIDFromAddress(descriptor.envelope.internal.SrcAddr)
			if sourceErr != nil {
				return nil, nil, fmt.Errorf("deferred inbound message %x source: %w", hash, sourceErr)
			}
			emitted = append(emitted, semanticMessageEmission{
				source:    source,
				createdLT: descriptor.envelope.internal.CreatedLT,
				emittedLT: descriptor.envelope.bound.lt,
			})
		}
	}

	for _, entry := range r.parsedOutOrder {
		hash, descriptor := entry.hash, entry.descriptor
		if err := r.ctx.Err(); err != nil {
			return nil, nil, err
		}
		switch descriptor.tag {
		case semanticOutImmediate, semanticOutDequeueImmediate, semanticOutNew, semanticOutDeferredTransit:
		default:
			continue
		}
		if descriptor.envelope.internal.SrcAddr.Workchain() != r.candidate.block.BlockInfo.Shard.WorkchainID {
			continue
		}
		if descriptor.tag == semanticOutImmediate && r.isMasterSpecialDescriptor(descriptor.reimport) {
			continue
		}
		source, sourceErr := semanticAccountIDFromAddress(descriptor.envelope.internal.SrcAddr)
		if sourceErr != nil {
			return nil, nil, fmt.Errorf("outbound message %x source: %w", hash, sourceErr)
		}
		emitted = append(emitted, semanticMessageEmission{
			source:    source,
			createdLT: descriptor.envelope.internal.CreatedLT,
			emittedLT: descriptor.envelope.bound.lt,
		})
	}

	return processed, emitted, nil
}

func (r *semanticReplay) isMasterSpecialDescriptor(descriptor *cell.Cell) bool {
	if descriptor == nil || r.candidate.block.Extra.Custom == nil {
		return false
	}
	details := r.candidate.block.Extra.Custom.Details
	return equalCell(descriptor, details.RecoverCreateMsg) || equalCell(descriptor, details.MintMsg)
}

func checkSemanticMessageOrder(processed []semanticMessageProcessing, emitted []semanticMessageEmission) error {
	slices.SortFunc(processed, func(left, right semanticMessageProcessing) int {
		if order := bytes.Compare(left.account[:], right.account[:]); order != 0 {
			return order
		}
		if left.transactionLT < right.transactionLT {
			return -1
		}
		if left.transactionLT > right.transactionLT {
			return 1
		}
		if left.messageLT < right.messageLT {
			return -1
		}
		if left.messageLT > right.messageLT {
			return 1
		}
		return 0
	})
	for i := 1; i < len(processed); i++ {
		previous := processed[i-1]
		current := processed[i]
		if previous.account == current.account && previous.messageLT > current.messageLT {
			return fmt.Errorf(
				"%w: transaction %x:%d processes message lt %d before later transaction %d processes earlier message lt %d",
				ErrInvalidInput,
				previous.account,
				previous.transactionLT,
				previous.messageLT,
				current.transactionLT,
				current.messageLT,
			)
		}
	}

	slices.SortFunc(emitted, func(left, right semanticMessageEmission) int {
		if order := bytes.Compare(left.source[:], right.source[:]); order != 0 {
			return order
		}
		if left.createdLT < right.createdLT {
			return -1
		}
		if left.createdLT > right.createdLT {
			return 1
		}
		if left.emittedLT < right.emittedLT {
			return -1
		}
		if left.emittedLT > right.emittedLT {
			return 1
		}
		return 0
	})
	for i := 1; i < len(emitted); i++ {
		previous := emitted[i-1]
		current := emitted[i]
		if previous.source == current.source && previous.emittedLT >= current.emittedLT {
			return fmt.Errorf(
				"%w: source %x emits message lt %d at %d before message lt %d at non-increasing lt %d",
				ErrInvalidInput,
				previous.source,
				previous.createdLT,
				previous.emittedLT,
				current.createdLT,
				current.emittedLT,
			)
		}
	}

	return nil
}

func (r *semanticReplay) verifyMasterTickTock() error {
	// Ascending account id, which is the order this check always imposed on
	// itself; PrepareConfig sorts the epoch's identities once instead of every
	// masterchain validation rebuilding the slice out of the map.
	for _, account := range r.specials.sorted {
		original, _, err := semanticLoadAccount(r.previous.Accounts.ShardAccounts, account)
		if err != nil {
			return err
		}
		var state tlb.AccountState
		if err = parseExact(&state, original.Account); err != nil {
			return fmt.Errorf("%w: decode special account %x for tick/tock: %v", ErrInvalidInput, account, err)
		}
		_, tick, tock := semanticAccountTickTock(&state)
		if (tick || tock) && r.accounts[account] == nil {
			return fmt.Errorf("%w: special tick/tock account %x has no account block", ErrInvalidInput, account)
		}
	}

	accounts := make([][32]byte, 0, len(r.accounts))
	for account := range r.accounts {
		accounts = append(accounts, account)
	}
	slices.SortFunc(accounts, func(left, right [32]byte) int {
		return bytes.Compare(left[:], right[:])
	})
	for _, account := range accounts {
		if err := r.verifyMasterAccountTickTock(account, r.accounts[account]); err != nil {
			return err
		}
	}

	return nil
}

func (r *semanticReplay) verifyMasterAccountTickTock(account [32]byte, result *semanticAccountResult) error {
	_, special := r.specials.set[account]
	for i, transaction := range result.sequence {
		if !transaction.tickTock {
			continue
		}
		if !special {
			return fmt.Errorf("%w: non-special account %x has a tick/tock transaction", ErrInvalidInput, account)
		}
		if !transaction.isTock {
			if i != 0 {
				return fmt.Errorf("%w: tick transaction of account %x is not first", ErrInvalidInput, account)
			}
			if r.candidate.block.BlockInfo.StartLt == math.MaxUint64 ||
				transaction.lt != r.candidate.block.BlockInfo.StartLt+1 {
				return fmt.Errorf("%w: tick transaction of account %x does not start at block lt + 1", ErrInvalidInput, account)
			}
			if !transaction.tickEnabled {
				return fmt.Errorf("%w: account %x has not enabled tick transactions", ErrInvalidInput, account)
			}
			continue
		}
		if i != len(result.sequence)-1 {
			return fmt.Errorf("%w: tock transaction of account %x is not last", ErrInvalidInput, account)
		}
		if !transaction.tockEnabled {
			return fmt.Errorf("%w: account %x has not enabled tock transactions", ErrInvalidInput, account)
		}
	}

	first := result.sequence[0]
	if special && first.beforeActive && first.tickEnabled && (!first.tickTock || first.isTock) {
		return fmt.Errorf("%w: special tick account %x does not begin with tick", ErrInvalidInput, account)
	}
	last := result.sequence[len(result.sequence)-1]
	if special && last.tockEnabled && last.afterActive && (!last.tickTock || !last.isTock) {
		return fmt.Errorf("%w: special tock account %x does not end with tock", ErrInvalidInput, account)
	}

	return nil
}

func semanticAccountTickTock(state *tlb.AccountState) (active, tick, tock bool) {
	if state.Status != tlb.AccountStatusActive || state.StateInit == nil {
		return false, false, false
	}
	if state.StateInit.TickTock == nil {
		return true, false, false
	}

	return true, state.StateInit.TickTock.Tick, state.StateInit.TickTock.Tock
}

func (r *semanticReplay) verifyMasterSpecialMessages() error {
	extra := r.candidate.block.Extra.Custom
	if extra == nil {
		return fmt.Errorf("%w: masterchain block has no custom extra", ErrInvalidInput)
	}
	fees := r.transition.Config.fees
	for _, special := range [...]struct {
		name        string
		descriptor  *cell.Cell
		amount      tlb.CurrencyCollection
		destination feeDestination
	}{
		{
			name:        "fee recovery",
			descriptor:  extra.Details.RecoverCreateMsg,
			amount:      r.candidate.flow.Recovered,
			destination: fees.collector,
		},
		{
			name:        "mint",
			descriptor:  extra.Details.MintMsg,
			amount:      r.candidate.flow.Minted,
			destination: fees.minter,
		},
	} {
		if (special.descriptor == nil) != currencyZero(special.amount) {
			return fmt.Errorf("%w: %s message presence differs from value flow", ErrInvalidInput, special.name)
		}
		if special.descriptor == nil {
			continue
		}
		if !special.destination.ok {
			return fmt.Errorf("%w: %s destination is malformed: %v",
				ErrInvalidInput, special.name, special.destination.err)
		}
		destination := special.destination.addr
		if err := r.verifyMasterSpecialMessage(special.name, special.descriptor, special.amount, destination[:]); err != nil {
			return err
		}
	}

	return nil
}

// semanticSpecialImportDescriptor decodes the fee recovery and mint descriptors,
// which the masterchain collator emits as msg_import_imm$011 with a zero fee.
// It is deliberately narrower than parseSemanticInDescriptor: that one also
// accepts transit constructors, which carry no transaction, and every caller
// here dereferences the transaction reference.
func semanticSpecialImportDescriptor(name string, root *cell.Cell) (*cell.Cell, *cell.Cell, error) {
	var loader cell.Slice
	err := root.BeginParseInto(&loader)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse %s descriptor: %v", ErrInvalidInput, name, err)
	}
	tag, err := loader.LoadUInt(3)
	if err != nil || tag != semanticInImmediate {
		return nil, nil, fmt.Errorf("%w: %s descriptor is not msg_import_imm", ErrInvalidInput, name)
	}
	envelopeRoot, transactionRoot, err := semanticLoadTwoRefs(&loader)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: decode %s descriptor references: %v", ErrInvalidInput, name, err)
	}
	fee, err := semanticLoadCoins(&loader)
	if err != nil || !fee.IsZero() || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, nil, fmt.Errorf("%w: %s descriptor has a non-zero fee or trailing data", ErrInvalidInput, name)
	}

	return envelopeRoot, transactionRoot, nil
}

func (r *semanticReplay) verifyMasterSpecialMessage(
	name string,
	descriptorRoot *cell.Cell,
	amount tlb.CurrencyCollection,
	destination []byte,
) error {
	envelopeRoot, transactionRoot, err := semanticSpecialImportDescriptor(name, descriptorRoot)
	if err != nil {
		return err
	}

	envelope, err := verifyMasterSpecialEnvelope(
		envelopeRoot,
		amount,
		destination,
		msgpool.ShardIdent{
			Workchain: r.candidate.block.BlockInfo.Shard.WorkchainID,
			Shard:     uint64(r.candidate.block.BlockInfo.Shard.GetShardID()),
		},
		r.candidate.block.BlockInfo.StartLt,
		r.candidate.block.BlockInfo.GenUtime,
	)
	if err != nil {
		return fmt.Errorf("%w: %s message: %v", ErrInvalidInput, name, err)
	}

	var transaction tlb.Transaction
	if err = parseExact(&transaction, transactionRoot); err != nil {
		return fmt.Errorf("%w: decode %s transaction: %v", ErrInvalidInput, name, err)
	}
	if len(transaction.AccountAddr) != 32 || !bytes.Equal(transaction.AccountAddr, destination) ||
		transaction.Now != r.candidate.block.BlockInfo.GenUtime {
		return fmt.Errorf("%w: %s transaction is not bound to its destination and block time", ErrInvalidInput, name)
	}
	if _, ordinary := transaction.Description.(tlb.TransactionDescriptionOrdinary); !ordinary {
		return fmt.Errorf("%w: %s transaction is not ordinary", ErrInvalidInput, name)
	}
	inbound, err := semanticTransactionInbound(transactionRoot, true)
	if err != nil || !equalCell(inbound, envelope.message) {
		return fmt.Errorf("%w: %s transaction does not process the system message", ErrInvalidInput, name)
	}

	messageHash := envelope.message.HashKey()
	stored := r.parsedIn[messageHash]
	if stored == nil {
		return fmt.Errorf("%w: %s message is absent from inbound descriptors", ErrInvalidInput, name)
	}
	if !equalCell(stored.root, descriptorRoot) {
		return fmt.Errorf("%w: stored %s descriptor differs from masterchain extra", ErrInvalidInput, name)
	}
	if _, consumed := r.consumedIn[messageHash]; !consumed {
		return fmt.Errorf("%w: %s message is not processed by an account transaction", ErrInvalidInput, name)
	}
	if !r.specialTxs.contains(transactionRoot.HashKey()) {
		return fmt.Errorf("%w: %s transaction is not registered as special", ErrInvalidInput, name)
	}

	return nil
}

func verifyMasterSpecialEnvelope(
	root *cell.Cell,
	amount tlb.CurrencyCollection,
	destination []byte,
	target msgpool.ShardIdent,
	startLT uint64,
	genUtime uint32,
) (*semanticEnvelope, error) {
	envelope, err := parseSemanticEnvelope(root)
	if err != nil {
		return nil, fmt.Errorf("decode envelope: %v", err)
	}
	if envelope.current != envelope.destination || envelope.next != envelope.destination {
		return nil, errors.New("message is not routed to its final destination")
	}
	if !target.ContainsPrefix(envelope.source) || !target.ContainsPrefix(envelope.destination) {
		return nil, errors.New("message source or destination is outside the masterchain shard")
	}
	if !envelope.value.FwdFeeRemaining.IsZero() || !envelope.internal.FwdFee.IsZero() ||
		!envelope.internal.IHRFee.IsZero() {
		return nil, errors.New("message has a non-zero forwarding fee, IHR fee, or extra flags")
	}
	// Metadata is legitimate on ordinary immediate messages -- a transit rewrite
	// even has to preserve it (validate-query.cpp:4399) -- so the prohibition
	// belongs here, where check_special_message states it
	// (validate-query.cpp:6574). emitted_lt is not checked here on purpose: it is
	// forbidden on every msg_import_imm, and verifyInboundEnvelope already holds
	// this descriptor to that unconditional rule. The msg_envelope_v2 wire tag
	// itself stays legal, exactly as it is for the reference decoder.
	if envelope.value.Metadata != nil {
		return nil, errors.New("message envelope carries metadata")
	}
	// check_special_message pins the flags and the creation stamps of the system
	// message to the block it is generated in (validate-query.cpp:6600-6608).
	if !envelope.internal.IHRDisabled || !envelope.internal.Bounce || envelope.internal.Bounced {
		return nil, errors.New("message has invalid flags")
	}
	if envelope.internal.CreatedLT != startLT {
		return nil, errors.New("message was not created at the block start logical time")
	}
	if envelope.internal.CreatedAt != genUtime {
		return nil, errors.New("message was not created at the block generation time")
	}
	value := tlb.CurrencyCollection{
		Coins:           envelope.internal.Amount,
		ExtraCurrencies: envelope.internal.ExtraCurrencies,
	}
	if !value.Equals(amount) {
		return nil, errors.New("message value differs from masterchain value flow")
	}

	var zero [32]byte
	source, err := semanticAccountIDFromAddress(envelope.internal.SrcAddr)
	if err != nil || envelope.internal.SrcAddr.Workchain() != address.MasterchainID || source != zero {
		return nil, errors.New("message source is not the zero masterchain address")
	}
	targetAccount, err := semanticAccountIDFromAddress(envelope.internal.DstAddr)
	if err != nil || envelope.internal.DstAddr.Workchain() != address.MasterchainID ||
		!bytes.Equal(targetAccount[:], destination) {
		return nil, errors.New("message destination differs from the configured masterchain address")
	}
	if envelope.internal.StateInit != nil || envelope.internal.Body == nil ||
		envelope.internal.Body.BitsSize() != 0 || envelope.internal.Body.RefsNum() != 0 {
		return nil, errors.New("message has a non-empty body or state init")
	}
	// The two post-info bits have to be exactly `00`: no StateInit and an inline
	// empty body. Re-serialization preserves every other wire field.
	canonical, err := envelope.internal.ToCell()
	if err != nil || !equalCell(canonical, envelope.message) {
		return nil, errors.New("message body is not the inline empty form")
	}

	return envelope, nil
}

func (r *semanticReplay) verifyMasterPublicLibraries() error {
	prepared := r.transition.prepared
	if prepared == nil || prepared.previousStats == nil {
		return fmt.Errorf("%w: masterchain predecessor statistics are absent", ErrInvalidInput)
	}
	previousStats := prepared.previousStats
	// The dictionary is shared with the structural pass now, and this function
	// mutates it through addLibraryPublisher/removeLibraryPublisher, so the copy
	// below is what keeps that sharing safe.
	libraries := previousStats.Libraries
	if libraries == nil {
		libraries = cell.NewDict(256)
	} else {
		if libraries.GetKeySize() != 256 {
			return fmt.Errorf("%w: previous public library dictionary has key size %d", ErrInvalidInput, libraries.GetKeySize())
		}
		libraries = libraries.Copy()
	}

	accounts := make([]*semanticAccountResult, 0, len(r.accounts))
	for _, account := range r.accounts {
		accounts = append(accounts, account)
	}
	slices.SortFunc(accounts, func(left, right *semanticAccountResult) int {
		return bytes.Compare(left.key[:], right.key[:])
	})
	for _, account := range accounts {
		var original tlb.AccountState
		if err := parseExact(&original, account.original.Account); err != nil {
			return fmt.Errorf("%w: decode original account %x for public libraries: %v", ErrInvalidInput, account.key, err)
		}
		before, err := accountPublicLibraries(&original)
		if err != nil {
			return fmt.Errorf("%w: decode original public libraries of %x: %v", ErrInvalidInput, account.key, err)
		}
		after, err := accountPublicLibraries(account.final.State())
		if err != nil {
			return fmt.Errorf("%w: decode resulting public libraries of %x: %v", ErrInvalidInput, account.key, err)
		}
		for _, key := range publicLibraryDiffKeys(before, after) {
			oldLibrary, wasPublic := before[key]
			newLibrary, isPublic := after[key]
			switch {
			case wasPublic && !isPublic:
				if err = removeLibraryPublisher(libraries, key, account.key, oldLibrary.root); err != nil {
					return err
				}
			case !wasPublic && isPublic:
				if err = addLibraryPublisher(libraries, key, account.key, newLibrary.root); err != nil {
					return err
				}
			}
		}
	}

	candidate := r.candidate.stats.Libraries
	if candidate != nil && candidate.GetKeySize() != 256 {
		return fmt.Errorf("%w: candidate public library dictionary has key size %d", ErrInvalidInput, candidate.GetKeySize())
	}
	if !equalCell(semanticDictionaryRoot(libraries), semanticDictionaryRoot(candidate)) {
		return fmt.Errorf("%w: public library publisher delta differs from replayed account states", ErrInvalidInput)
	}

	return nil
}

func semanticDictionaryRoot(dictionary *cell.Dictionary) *cell.Cell {
	if dictionary == nil || dictionary.IsEmpty() {
		return nil
	}

	return dictionary.AsCell()
}

func (r *semanticReplay) verifyMasterBurnedValue() error {
	expected, err := r.transition.prepared.minimumBurned.Add(r.blackholeBurned)
	if err != nil {
		return fmt.Errorf("%w: add protocol and blackhole burn: %v", ErrInvalidInput, err)
	}
	if !r.candidate.flow.Burned.Equals(expected) {
		return fmt.Errorf("%w: burned value differs from protocol fees and replayed blackhole burns", ErrInvalidInput)
	}

	return nil
}
