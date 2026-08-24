package msgpool

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const shardStateMagic = 0x9023afe2

// ErrQueueSizeNotStored means the state predates capStoreOutMsgQueueSize.
var ErrQueueSizeNotStored = errors.New("msgpool: state stores no out-queue size")

func routedDeltasFromBlockRoot(
	root *cell.Cell,
	startLT uint64,
	source ShardIdent,
	ref SourceRef,
	snapshot *internalsSnapshot,
) ([]RoutedDelta, error) {
	outDescr, err := blockOutMsgDescr(root)
	if err != nil {
		return nil, err
	}
	loader, err := outDescr.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse OutMsgDescr: %w", err)
	}
	dict, err := loader.LoadAugDict(256, tlb.AugOutMsgDescr{}, false)
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot load OutMsgDescr dictionary: %w", err)
	}

	deltas := make([]RoutedDelta, len(snapshot.destinations))
	values := make([]InternalsDelta, len(snapshot.destinations))
	for index := range deltas {
		deltas[index] = RoutedDelta{
			Destination: snapshot.destinations[index].shard,
			Delta:       &values[index],
		}
	}
	sink := routedSink{
		snapshot: snapshot,
		deltas:   deltas,
		source:   source,
		seqno:    ref.Seqno,
		startLT:  startLT,
	}
	// Borrowed: this walk runs on every applied block and reads nothing past
	// the callback, so the materializing form's owned key cell and value slice
	// per leaf were pure waste. item.Value is already this callback's own copy.
	err = dict.ForEachBorrowed(false, false, func(item cell.AugDictItemView) error {
		v := item.Value
		tag, err := v.LoadUInt(3)
		if err != nil {
			return fmt.Errorf("cannot load OutMsg tag: %w", err)
		}
		switch tag {
		case 0b000, 0b010:
			// msg_export_ext leaves the chain, msg_export_imm was delivered
			// inside the block: no queue effect.
			return nil

		case 0b001, 0b011: // msg_export_new$001 / msg_export_tr$011
			envCell, err := v.LoadRefCell()
			if err != nil {
				return fmt.Errorf("cannot unpack queued export: %w", err)
			}
			placement := queueAtMessageLT
			if tag == 0b011 {
				placement = queueAtBlockStart
			}
			return sink.addEnvelope(envCell, placement)

		case 0b101: // msg_export_new_defer$10100 / msg_export_deferred_tr$10101
			deferredTag, err := v.LoadUInt(2)
			if err != nil {
				return fmt.Errorf("cannot load deferred export tag: %w", err)
			}
			switch deferredTag {
			case 0b00:
				// msg_export_new_defer stays in DispatchQueue.
				return nil
			case 0b01:
				envCell, err := v.LoadRefCell()
				if err != nil {
					return fmt.Errorf("cannot unpack msg_export_deferred_tr: %w", err)
				}
				return sink.addEnvelope(envCell, queueAtEmissionLT)
			default:
				return fmt.Errorf("unknown OutMsg tag 101%02b", deferredTag)
			}

		case 0b100: // msg_export_deq_imm$100 out_msg:^MsgEnvelope reimport:^InMsg
			return sink.removeEnvelopeFrom(&v)

		case 0b110: // msg_export_deq$1100 / msg_export_deq_short$1101
			short, err := v.LoadBoolBit()
			if err != nil {
				return fmt.Errorf("cannot load dequeue tag bit: %w", err)
			}
			if !short {
				return sink.removeEnvelopeFrom(&v)
			}
			// msg_export_deq_short$1101 msg_env_hash:bits256
			// next_workchain:int32 next_addr_pfx:uint64 import_block_lt:uint64
			envHash, err := v.LoadSlice(256)
			if err != nil {
				return fmt.Errorf("cannot load dequeued envelope hash: %w", err)
			}
			workchain, err := v.LoadInt(32)
			if err != nil {
				return fmt.Errorf("cannot load dequeue next workchain: %w", err)
			}
			prefix, err := v.LoadUInt(64)
			if err != nil {
				return fmt.Errorf("cannot load dequeue next prefix: %w", err)
			}
			var hash [32]byte
			copy(hash[:], envHash)
			sink.removeByEnvHash(AccountPrefix{Workchain: int32(workchain), Prefix: prefix}, hash)
			return nil

		case 0b111: // msg_export_tr_req$111: dequeue old envelope and enqueue rewritten one
			envCell, err := v.LoadRefCell()
			if err != nil {
				return fmt.Errorf("cannot unpack msg_export_tr_req envelope: %w", err)
			}
			if err := sink.addEnvelope(envCell, queueAtBlockStart); err != nil {
				return err
			}

			imported, err := v.LoadRefCell()
			if err != nil {
				return fmt.Errorf("cannot unpack msg_export_tr_req import: %w", err)
			}
			oldEnv, err := transitInputEnvelope(imported)
			if err != nil {
				return err
			}
			return sink.removeEnvelopeCell(oldEnv)
		}

		return fmt.Errorf("unknown OutMsg tag %03b", tag)
	})
	if err != nil {
		return nil, fmt.Errorf("msgpool: failed to iterate OutMsgDescr: %w", err)
	}

	for index := range deltas {
		delta := deltas[index].Delta
		delta.AddedTotal = sink.addedTotal
		delta.RemovedTotal = sink.removedTotal
		added := delta.Added
		slices.SortFunc(added, CompareLtHash)
	}
	return deltas, nil
}

// routedSink routes one parsed operation through the immutable prefix trie;
// full-queue totals remain source invariants and therefore reach every view.
type routedSink struct {
	snapshot     *internalsSnapshot
	deltas       []RoutedDelta
	source       ShardIdent
	seqno        uint32
	startLT      uint64
	addedTotal   int
	removedTotal int
}

type queuePlacement uint8

const (
	queueAtMessageLT queuePlacement = iota
	queueAtBlockStart
	queueAtEmissionLT
)

func (s *routedSink) addEnvelope(envCell *cell.Cell, placement queuePlacement) error {
	s.addedTotal++
	msg, err := InternalMessageFromEnvelope(envCell)
	if err != nil {
		return err
	}
	switch placement {
	case queueAtMessageLT:
	case queueAtBlockStart:
		// An ordinary transit re-enqueue enters the queue at the relaying
		// block's start_lt, not at the message creation lt.
		msg.QueueLT = s.startLT
	case queueAtEmissionLT:
		// A message released from DispatchQueue is emitted inside this block.
		// C++ stores that emitted_lt as EnqueuedMsg.enqueued_lt; start_lt is
		// necessarily earlier and would make the queue entry invalid.
		msg.QueueLT = msg.EnqueuedLT
	}
	msg.Source = s.source
	msg.SourceSeqno = s.seqno
	hop := msg.Key.NextHop()
	s.snapshot.router.walk(hop, func(index int) {
		delta := s.deltas[index].Delta
		delta.Added = append(delta.Added, msg)
	})
	return nil
}

// removeEnvelopeFrom resolves a dequeue record carrying the full envelope
// into its queue key.
func (s *routedSink) removeEnvelopeFrom(v *cell.Slice) error {
	envCell, err := v.LoadRefCell()
	if err != nil {
		return fmt.Errorf("cannot unpack dequeue record: %w", err)
	}
	return s.removeEnvelopeCell(envCell)
}

func (s *routedSink) removeEnvelopeCell(envCell *cell.Cell) error {
	s.removedTotal++
	msg, err := InternalMessageFromEnvelope(envCell)
	if err != nil {
		return err
	}
	hop := msg.Key.NextHop()
	s.snapshot.router.walk(hop, func(index int) {
		delta := s.deltas[index].Delta
		delta.RemovedKeys = append(delta.RemovedKeys, msg.Key)
	})
	return nil
}

func (s *routedSink) removeByEnvHash(hop AccountPrefix, envHash [32]byte) {
	s.removedTotal++
	s.snapshot.router.walk(hop, func(index int) {
		delta := s.deltas[index].Delta
		delta.RemovedEnvHashes = append(delta.RemovedEnvHashes, envHash)
	})
}

// transitInputEnvelope extracts the old queued envelope from
// msg_import_tr$101, the reimport record carried by msg_export_tr_req.
func transitInputEnvelope(imported *cell.Cell) (*cell.Cell, error) {
	v, err := imported.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("cannot parse msg_export_tr_req import: %w", err)
	}
	tag, err := v.LoadUInt(3)
	if err != nil {
		return nil, fmt.Errorf("cannot load msg_export_tr_req import tag: %w", err)
	}
	if tag != 0b101 {
		return nil, fmt.Errorf("msg_export_tr_req import has tag %03b, want msg_import_tr$101", tag)
	}
	oldEnv, err := v.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("cannot unpack msg_import_tr input envelope: %w", err)
	}
	return oldEnv, nil
}

// seedWalkWorkers bounds the goroutines a queue seed spreads over, and
// seedWalkPrefixBits is how far past the workchain the split prefixes reach:
// six bits give up to 64 subtrees per live workchain, enough to keep the
// workers busy on a queue of thousands of entries.
const (
	seedWalkWorkers    = 16
	seedWalkPrefixBits = 6
	seedWalkMaxTasks   = 512
)

// queueKeyWorkchainBits is the width of the workchain that leads every
// out-queue key; see MakeQueueKey.
const queueKeyWorkchainBits = 32

func routedSeedsFromStateRoot(
	stateRoot *cell.Cell,
	source ShardIdent,
	top SourceRef,
	snapshot *internalsSnapshot,
) ([]RoutedSeed, uint64, error) {
	return routedSeedsFromStateRootWith(stateRoot, source, top, snapshot, true)
}

func routedSeedsFromStateRootWith(
	stateRoot *cell.Cell,
	source ShardIdent,
	top SourceRef,
	snapshot *internalsSnapshot,
	split bool,
) ([]RoutedSeed, uint64, error) {
	queueInfo, err := StateOutMsgQueueInfo(stateRoot)
	if err != nil {
		return nil, 0, err
	}

	seeds := make([]RoutedSeed, len(snapshot.destinations))
	for index := range seeds {
		seeds[index].Destination = snapshot.destinations[index].shard
	}

	// The walk is split by key prefix and run on workers when the trie has
	// enough live prefixes to split. Seeding a fresh branch from a queue
	// thousands of entries deep is the single largest cost of the first build
	// of a leader window — 40-100 ms of walking a lazily loaded trie, on the
	// one build that has no slack — and it is trivially parallel: every entry
	// is routed by its own key and decoded on its own, and the runs are sorted
	// afterwards, so the order the workers produce them in is immaterial.
	// Anything that fails to split, or a queue small enough not to need it,
	// takes the sequential walk below.
	if !split {
		// The test arm: the sequential walk below, unconditionally.
	} else if total, ok, err := routedSeedsFromQueueParallel(queueInfo.OutQueue, source, top, snapshot, seeds, -1); ok {
		if err != nil {
			return nil, 0, err
		}
		return seeds, total, nil
	}

	var total uint64
	err = queueInfo.OutQueue.ForEachBorrowed(false, false, func(item cell.AugDictItemView) error {
		total++
		queueKey, err := queueKeyFromView(item.Key)
		if err != nil {
			return err
		}
		canonicalLT, err := item.Extra.LoadUInt(64)
		if err != nil {
			return fmt.Errorf("cannot load queue leaf augmentation lt: %w", err)
		}

		msg, err := decodeRoutedQueueEntry(&item.Value, queueKey, canonicalLT, source, top)
		if err != nil {
			return err
		}
		snapshot.router.walk(queueKey.NextHop(), func(index int) {
			seeds[index].Messages = append(seeds[index].Messages, msg)
		})
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("msgpool: failed to walk the out-queue: %w", err)
	}

	for index := range seeds {
		messages := seeds[index].Messages
		slices.SortFunc(messages, CompareLtHash)
	}

	return seeds, total, nil
}

// routedSeedsFromQueueParallel is the split walk behind routedSeedsFromStateRoot.
// It reports ok=false when it declined to run — too few prefixes, or the
// prefix enumeration hit its limit — in which case nothing was written and the
// caller walks sequentially. When it ran, seeds hold every routed entry, sorted
// per destination exactly as the sequential walk leaves them.

// filterSeedPrefixes drops the key-prefix subtrees that cannot hold a single
// entry for the wanted destination. A prefix that cannot be decoded is kept: the
// walk is the authority on what a key means, and a filter that guessed wrong
// would silently drop messages rather than merely fail to skip them.
func filterSeedPrefixes(prefixes []*cell.Cell, router destinationRouter, only int) []*cell.Cell {
	kept := prefixes[:0]
	for _, prefix := range prefixes {
		route, bits, ok := seedPrefixRoute(prefix)
		if !ok || router.routesUnder(route, bits, only) {
			kept = append(kept, prefix)
		}
	}

	return kept
}

// seedPrefixRoute reads the workchain and however many account-prefix bits a
// key prefix carries. It reports ok=false for anything shorter than the
// workchain, which cannot be narrowed at all.
func seedPrefixRoute(prefix *cell.Cell) (AccountPrefix, int, bool) {
	slice, err := prefix.BeginParse()
	if err != nil {
		return AccountPrefix{}, 0, false
	}
	if slice.BitsLeft() < queueKeyWorkchainBits {
		return AccountPrefix{}, 0, false
	}
	workchain, err := slice.LoadUInt(queueKeyWorkchainBits)
	if err != nil {
		return AccountPrefix{}, 0, false
	}
	bits := int(min(slice.BitsLeft(), 64))
	route := AccountPrefix{Workchain: int32(uint32(workchain))}
	if bits > 0 {
		value, loadErr := slice.LoadUInt(uint(bits))
		if loadErr != nil {
			return AccountPrefix{}, 0, false
		}
		route.Prefix = value << (64 - uint(bits))
	}

	return route, bits, true
}

func routedSeedsFromQueueParallel(
	queue *tlb.OutMsgQueueAugDict,
	source ShardIdent,
	top SourceRef,
	snapshot *internalsSnapshot,
	seeds []RoutedSeed,
	only int,
) (uint64, bool, error) {
	prefixes, err := queue.KeyPrefixes(queueKeyWorkchainBits+seedWalkPrefixBits, seedWalkMaxTasks)
	if err != nil || len(prefixes) < 2 {
		return 0, false, nil
	}
	// With one destination wanted, subtrees that cannot reach it are dropped
	// before anything is walked. The per-entry routing check below still has to
	// parse a key to ask its question; asked once per subtree it costs nothing
	// and removes the entries entirely — every message bound for another
	// workchain today, and half the queue the moment this shard splits and the
	// sibling's traffic starts sharing the source.
	//
	// The filter is a pure narrowing: whatever it keeps, the per-entry check
	// still decides. What it must never do is drop a subtree that holds a
	// wanted entry, and on an unsplit shard — where the destination owns the
	// whole workchain — that mistake would drop the entire queue and produce
	// empty blocks that look perfectly valid. TestSeedPrefixFilterNeverChanges
	// TheSeededSet is what stands between that and production.
	if only >= 0 {
		prefixes = filterSeedPrefixes(prefixes, snapshot.router, only)
		if len(prefixes) == 0 {
			return 0, true, nil
		}
	}
	// With one destination wanted, subtrees that cannot reach it are dropped
	// before anything is walked. The per-entry routing check below still has to
	// parse a key to ask its question; asked once per subtree it costs nothing
	// and removes the entries entirely — every message bound for another
	// workchain today, and half the queue the moment this shard splits and the
	// sibling's traffic starts sharing the source.

	type seedTask struct {
		runs  [][]*InternalMessage
		total uint64
		err   error
	}
	tasks := make([]seedTask, len(prefixes))
	next := make(chan int, len(prefixes))
	for i := range prefixes {
		next <- i
	}
	close(next)
	var wait sync.WaitGroup
	for range min(seedWalkWorkers, len(prefixes)) {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for i := range next {
				task := &tasks[i]
				task.runs = make([][]*InternalMessage, len(seeds))
				sub := &tlb.OutMsgQueueAugDict{AugmentedDictionary: queue.Copy()}
				cut, cutErr := sub.CutPrefixSubdict(prefixes[i], false)
				if cutErr != nil || !cut {
					task.err = fmt.Errorf("msgpool: cut out-queue prefix: ok=%t err=%v", cut, cutErr)
					continue
				}
				walkErr := sub.ForEachBorrowed(false, false, func(item cell.AugDictItemView) error {
					task.total++
					queueKey, err := queueKeyFromView(item.Key)
					if err != nil {
						return err
					}
					// With a single destination wanted, the routing decision —
					// a function of the key alone — is taken before the decode,
					// which is the larger half of an entry's cost.
					if only >= 0 && !snapshot.router.routes(queueKey.NextHop(), only) {
						return nil
					}
					canonicalLT, err := item.Extra.LoadUInt(64)
					if err != nil {
						return fmt.Errorf("cannot load queue leaf augmentation lt: %w", err)
					}
					msg, err := decodeRoutedQueueEntry(&item.Value, queueKey, canonicalLT, source, top)
					if err != nil {
						return err
					}
					if only >= 0 {
						task.runs[only] = append(task.runs[only], msg)
						return nil
					}
					snapshot.router.walk(queueKey.NextHop(), func(index int) {
						task.runs[index] = append(task.runs[index], msg)
					})
					return nil
				})
				if walkErr != nil {
					task.err = fmt.Errorf("msgpool: failed to walk the out-queue: %w", walkErr)
				}
			}
		}()
	}
	wait.Wait()

	var total uint64
	for i := range tasks {
		if tasks[i].err != nil {
			return 0, true, tasks[i].err
		}
		total += tasks[i].total
		for index := range seeds {
			seeds[index].Messages = append(seeds[index].Messages, tasks[i].runs[index]...)
		}
	}
	for index := range seeds {
		slices.SortFunc(seeds[index].Messages, CompareLtHash)
	}
	return total, true, nil
}

// queueKeyFromView reads a queue key out of a borrowed iterator item.
//
// It takes the key by value, which is the whole point of the borrowed walk: the
// materializing form built an owning key cell per leaf — a Builder, its buffer,
// a Slice to parse it back out of — and at ten thousand entries that was five
// heap objects an entry for 352 bits the iterator already had in front of it.
// The view's Key is a Slice over the iterator's scratch, so it is consumed here
// and never retained.
func queueKeyFromView(key cell.Slice) (QueueKey, error) {
	var queueKey QueueKey
	if err := key.LoadSliceInto(queueKey[:], 352); err != nil {
		return queueKey, fmt.Errorf("cannot load queue key bits: %w", err)
	}

	return queueKey, nil
}

// decodeQueueEntry turns one out-queue leaf into the message the pool carries.
// It is the expensive half of a queue walk — an envelope parse and a message
// parse per entry — and it is a function rather than inline code so a caller
// that already knows it does not want the entry can decline to pay for it.
func decodeRoutedQueueEntry(
	value *cell.Slice,
	queueKey QueueKey,
	canonicalLT uint64,
	source ShardIdent,
	top SourceRef,
) (*InternalMessage, error) {
	var enqueued tlb.EnqueuedMsg
	if err := tlb.LoadFromCell(&enqueued, value.Copy()); err != nil {
		return nil, fmt.Errorf("cannot decode enqueued message: %w", err)
	}
	msg, err := InternalMessageFromEnvelope(enqueued.Msg)
	if err != nil {
		return nil, err
	}
	if msg.Key.MsgHash() != queueKey.MsgHash() {
		return nil, fmt.Errorf("%w: queue key %x does not match its envelope", ErrApplyDisorder, queueKey)
	}
	// AugOutMsgQueue's leaf augmentation is the canonical import-order
	// lt: MsgEnvelope.emitted_lt for v2 envelopes, otherwise
	// Message.created_lt. EnqueuedMsg.enqueued_lt records when the
	// envelope entered this particular queue and may differ in transit.
	msg.EnqueuedLT = canonicalLT
	msg.QueueLT = enqueued.EnqueuedLT
	msg.Key = queueKey
	msg.Source = source
	msg.SourceSeqno = top.Seqno

	return msg, nil
}

// routedSeedsForDestination walks one source out-queue and returns only the run
// bound for destination.
//
// It exists because the caller that dominates this walk wants exactly one
// destination and the shared form gives it every destination in the topology.
// The routing decision is a function of the KEY — the next-hop prefix occupies
// the first 96 bits of it — so it can be taken before the entry is decoded,
// while the decode is an envelope parse plus a message parse and is by far the
// larger half. Deciding first and decoding second is the whole difference:
// entries bound elsewhere cost a key parse and a trie descent instead of a full
// materialization.
//
// The measured shape it was written for: on the testnet validator a masterchain
// build spent 38.3 ms of its 52.6 ms in acquire_inputs, walking every entry of
// every shard top's queue to collect the few bound for the masterchain.
//
// Unlike the shared form this returns no queue total, because it no longer has
// to be complete to be correct and a caller that needs the count should ask for
// it separately. Seeds keep the same order the shared form produces.
func routedSeedsForDestination(
	stateRoot *cell.Cell,
	source ShardIdent,
	top SourceRef,
	snapshot *internalsSnapshot,
	destination ShardIdent,
) ([]*InternalMessage, error) {
	index := -1
	for i := range snapshot.destinations {
		if snapshot.destinations[i].shard == destination {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, ErrNotFound
	}

	queueInfo, err := StateOutMsgQueueInfo(stateRoot)
	if err != nil {
		return nil, err
	}

	// The split walk, narrowed to the one destination, when the trie splits;
	// the sequential narrowed walk below otherwise.
	seeds := make([]RoutedSeed, len(snapshot.destinations))
	if _, ok, err := routedSeedsFromQueueParallel(queueInfo.OutQueue, source, top, snapshot, seeds, index); ok {
		if err != nil {
			return nil, err
		}
		return seeds[index].Messages, nil
	}

	var messages []*InternalMessage
	err = queueInfo.OutQueue.ForEachBorrowed(false, false, func(item cell.AugDictItemView) error {
		queueKey, err := queueKeyFromView(item.Key)
		if err != nil {
			return err
		}
		if !snapshot.router.routes(queueKey.NextHop(), index) {
			return nil
		}
		canonicalLT, err := item.Extra.LoadUInt(64)
		if err != nil {
			return fmt.Errorf("cannot load queue leaf augmentation lt: %w", err)
		}
		msg, err := decodeRoutedQueueEntry(&item.Value, queueKey, canonicalLT, source, top)
		if err != nil {
			return err
		}
		messages = append(messages, msg)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("msgpool: failed to walk the out-queue: %w", err)
	}
	slices.SortFunc(messages, CompareLtHash)

	return messages, nil
}

// QueueSizeFromStateRoot reads the out-queue size stored by
// capStoreOutMsgQueueSize.
func QueueSizeFromStateRoot(stateRoot *cell.Cell) (uint64, error) {
	queueInfo, err := StateOutMsgQueueInfo(stateRoot)
	if err != nil {
		return 0, err
	}
	if queueInfo.Extra == nil || queueInfo.Extra.OutQueueSize == nil {
		return 0, ErrQueueSizeNotStored
	}
	return *queueInfo.Extra.OutQueueSize, nil
}

// StateOutMsgQueueInfo walks a ShardStateUnsplit root to its parsed
// OutMsgQueueInfo, touching only the state magic and the first reference.
func StateOutMsgQueueInfo(stateRoot *cell.Cell) (*tlb.OutMsgQueueInfo, error) {
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse state root: %w", err)
	}
	magic, err := loader.LoadUInt(32)
	if err != nil || magic != shardStateMagic {
		return nil, errors.New("msgpool: cell is not a shard state root")
	}
	// shard_state#9023afe2 … out_msg_queue_info:^OutMsgQueueInfo is the
	// first reference of the state root.
	infoCell, err := stateRoot.PeekRef(0)
	if err != nil {
		return nil, fmt.Errorf("msgpool: state root has no out-queue info: %w", err)
	}
	infoLoader, err := infoCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse out-queue info: %w", err)
	}
	var queueInfo tlb.OutMsgQueueInfo
	if err = tlb.LoadFromCell(&queueInfo, infoLoader); err != nil {
		return nil, fmt.Errorf("msgpool: cannot decode out-queue info: %w", err)
	}
	return &queueInfo, nil
}

// InternalMessageFromEnvelope builds the pooled view of one queued envelope:
// the parsed envelope plus the queue key derived from the next-hop address.
//
// Of int_msg_info only the header the derivation reads is decoded — both
// addresses and created_lt. value, the extra-currency dictionary, both fees,
// the state init and the body are skipped and never materialized: no field of
// InternalMessage carries them, and the reference node walks the same queue
// through unpack_cell_inexact (block.cpp EnqueuedMsgDescr::unpack), a
// header-only decode as well. The addresses still go through
// cell.Slice.LoadAddr, exactly the routine tlb's `addr` tag calls, so the
// addr_std/addr_var split and the anycast rewrite behind every derived key
// stay bit-identical.
func InternalMessageFromEnvelope(envCell *cell.Cell) (*InternalMessage, error) {
	envLoader, err := envCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("cannot parse message envelope: %w", err)
	}
	var env tlb.MsgEnvelope
	if err = env.LoadFromCell(envLoader); err != nil {
		return nil, fmt.Errorf("cannot decode message envelope: %w", err)
	}

	msgLoader, err := env.Msg.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("cannot parse enveloped message: %w", err)
	}
	// int_msg_info$0 ihr_disabled:Bool bounce:Bool bounced:Bool
	header, err := msgLoader.LoadUInt(4)
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message header: %w", err)
	}
	if header&0b1000 != 0 {
		return nil, errors.New("enveloped message is not int_msg_info$0")
	}
	srcAddr, err := msgLoader.LoadAddr()
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message source: %w", err)
	}
	dstAddr, err := msgLoader.LoadAddr()
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message destination: %w", err)
	}
	destinationWorkchain := dstAddr.Workchain()
	var destinationAccount [32]byte
	destinationPrewarmable := dstAddr.Type() == address.StdAddress
	if destinationPrewarmable {
		if dstAddr.BitsLen() != 256 || len(dstAddr.Data()) != len(destinationAccount) {
			return nil, errors.New("envelope destination: malformed standard account address")
		}
		copy(destinationAccount[:], dstAddr.Data())
		if err = RewriteAnycast(destinationAccount[:], dstAddr); err != nil {
			return nil, fmt.Errorf("envelope destination: %w", err)
		}
	}
	// value:CurrencyCollection — grams, then the extra-currency HashmapE,
	// whose root is a reference and therefore costs a single bit here.
	if err = skipGrams(msgLoader); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message value: %w", err)
	}
	if _, err = msgLoader.LoadBoolBit(); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message extra currencies: %w", err)
	}
	if err = skipGrams(msgLoader); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message ihr fee: %w", err)
	}
	if err = skipGrams(msgLoader); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message forward fee: %w", err)
	}
	createdLT, err := msgLoader.LoadUInt(64)
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message created lt: %w", err)
	}

	var hop AccountPrefix
	switch env.NextAddr.Type {
	case tlb.IntermediateAddressRegular:
		src, err := AccountPrefixFromAddress(srcAddr)
		if err != nil {
			return nil, fmt.Errorf("envelope source: %w", err)
		}
		dst, err := AccountPrefixFromAddress(dstAddr)
		if err != nil {
			return nil, fmt.Errorf("envelope destination: %w", err)
		}
		hop = InterpolatePrefix(src, dst, int(env.NextAddr.UseDestBits))
	case tlb.IntermediateAddressSimple, tlb.IntermediateAddressExt:
		hop = AccountPrefix{Workchain: env.NextAddr.Workchain, Prefix: env.NextAddr.AddrPfx}
	default:
		return nil, fmt.Errorf("unknown envelope next-hop type %d", env.NextAddr.Type)
	}

	// The queue lt rule of AugOutMsgQueue: a v2 envelope's emitted lt takes
	// precedence over the message creation lt.
	enqueuedLT := createdLT
	if env.EmittedLT != nil {
		enqueuedLT = *env.EmittedLT
	}

	return &InternalMessage{
		Key:                    MakeQueueKey(hop, env.Msg.HashKey()),
		EnqueuedLT:             enqueuedLT,
		QueueLT:                createdLT,
		EnvHash:                envCell.HashKey(),
		Envelope:               env,
		EnvelopeCell:           envCell,
		Root:                   env.Msg,
		DestinationWorkchain:   destinationWorkchain,
		DestinationAccount:     destinationAccount,
		DestinationPrewarmable: destinationPrewarmable,
	}, nil
}

// skipGrams advances past one Grams (VarUInteger 16) field: a 4-bit byte
// length followed by that many bytes.
func skipGrams(loader *cell.Slice) error {
	length, err := loader.LoadUInt(4)
	if err != nil {
		return err
	}
	return loader.SkipBits(uint(length) * 8)
}

// blockOutMsgDescr walks a block root to its OutMsgDescr cell, touching
// only the header magics.
func blockOutMsgDescr(root *cell.Cell) (*cell.Cell, error) {
	extra, err := blockExtra(root)
	if err != nil {
		return nil, err
	}
	// block_extra in_msg_descr:^InMsgDescr out_msg_descr:^OutMsgDescr ...
	outMsg, err := extra.PeekRef(1)
	if err != nil {
		return nil, fmt.Errorf("msgpool: block extra has no OutMsgDescr: %w", err)
	}
	return outMsg, nil
}
