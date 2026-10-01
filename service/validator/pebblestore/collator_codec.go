package pebblestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	collatorSessionRecordVersion = byte(3)
	// Version 3 dropped the block and collated payloads: the record is now only
	// the signature marker that keeps a restarted producer from signing a slot
	// twice. It also records whether the validator signed directly or authorized
	// a delegated collator. This in-progress format has no released predecessor.
	collatorCandidateRecordVersion = byte(3)
	collatorBlockIDSize            = 80
	collatorValidatorSize          = 72
	collatorCandidateFixedSize     = 232
)

type collatorRecordEncoder struct {
	value []byte
	err   error
}

func (e *collatorRecordEncoder) addByte(value byte) {
	if e.err == nil {
		e.value = append(e.value, value)
	}
}

func (e *collatorRecordEncoder) addBool(value bool) {
	if value {
		e.addByte(1)
	} else {
		e.addByte(0)
	}
}

func (e *collatorRecordEncoder) addUint32(value uint32) {
	if e.err == nil {
		e.value = binary.LittleEndian.AppendUint32(e.value, value)
	}
}

func (e *collatorRecordEncoder) addInt32(value int32) {
	e.addUint32(uint32(value))
}

func (e *collatorRecordEncoder) addUint64(value uint64) {
	if e.err == nil {
		e.value = binary.LittleEndian.AppendUint64(e.value, value)
	}
}

func (e *collatorRecordEncoder) addInt64(value int64) {
	e.addUint64(uint64(value))
}

func (e *collatorRecordEncoder) addFixed(value []byte) {
	if e.err == nil {
		e.value = append(e.value, value...)
	}
}

func (e *collatorRecordEncoder) addBytes(name string, value []byte) {
	if e.err != nil {
		return
	}
	if uint64(len(value)) > math.MaxUint32 {
		e.err = fmt.Errorf("%s is too large", name)

		return
	}

	e.addUint32(uint32(len(value)))
	e.addFixed(value)
}

func (e *collatorRecordEncoder) addCount(name string, count int) {
	if e.err != nil {
		return
	}
	if uint64(count) > math.MaxUint32 {
		e.err = fmt.Errorf("%s count is too large", name)

		return
	}

	e.addUint32(uint32(count))
}

func (e *collatorRecordEncoder) addBlockID(name string, block ton.BlockIDExt) {
	if e.err != nil {
		return
	}
	if len(block.RootHash) != 32 {
		e.err = fmt.Errorf("%s root hash length %d", name, len(block.RootHash))

		return
	}
	if len(block.FileHash) != 32 {
		e.err = fmt.Errorf("%s file hash length %d", name, len(block.FileHash))

		return
	}

	e.addInt32(block.Workchain)
	e.addInt64(block.Shard)
	e.addUint32(block.SeqNo)
	e.addFixed(block.RootHash)
	e.addFixed(block.FileHash)
}

func (e *collatorRecordEncoder) addShard(shard groups.ShardID) {
	e.addInt32(shard.Workchain)
	e.addInt64(shard.Shard)
}

func (e *collatorRecordEncoder) addParent(parent simplex.ParentID) {
	e.addBool(parent.Exists)
	if parent.Exists {
		e.addCandidateID(parent.ID)
	} else if parent.ID != (simplex.CandidateID{}) && e.err == nil {
		e.err = errors.New("absent parent has a candidate ID")
	}
}

func (e *collatorRecordEncoder) addCandidateID(id simplex.CandidateID) {
	e.addUint32(id.Slot)
	e.addFixed(id.Hash[:])
}

type collatorRecordDecoder struct {
	value []byte
	pos   int
	err   error
}

func (d *collatorRecordDecoder) take(size int, name string) []byte {
	if d.err != nil {
		return nil
	}
	if size < 0 || size > len(d.value)-d.pos {
		d.err = fmt.Errorf("truncated %s", name)

		return nil
	}

	value := d.value[d.pos : d.pos+size]
	d.pos += size

	return value
}

func (d *collatorRecordDecoder) byte(name string) byte {
	value := d.take(1, name)
	if d.err != nil {
		return 0
	}

	return value[0]
}

func (d *collatorRecordDecoder) boolean(name string) bool {
	value := d.byte(name)
	if d.err != nil {
		return false
	}
	switch value {
	case 0:
		return false
	case 1:
		return true
	default:
		d.err = fmt.Errorf("invalid %s marker %d", name, value)

		return false
	}
}

func (d *collatorRecordDecoder) uint32(name string) uint32 {
	value := d.take(4, name)
	if d.err != nil {
		return 0
	}

	return binary.LittleEndian.Uint32(value)
}

func (d *collatorRecordDecoder) int32(name string) int32 {
	return int32(d.uint32(name))
}

func (d *collatorRecordDecoder) uint64(name string) uint64 {
	value := d.take(8, name)
	if d.err != nil {
		return 0
	}

	return binary.LittleEndian.Uint64(value)
}

func (d *collatorRecordDecoder) int64(name string) int64 {
	return int64(d.uint64(name))
}

func (d *collatorRecordDecoder) fixed(size int, name string) []byte {
	value := d.take(size, name)
	if d.err != nil {
		return nil
	}

	return append([]byte(nil), value...)
}

func (d *collatorRecordDecoder) bytes(name string) []byte {
	size := d.uint32(name + " length")
	if d.err != nil {
		return nil
	}
	if uint64(size) > uint64(len(d.value)-d.pos) {
		d.err = fmt.Errorf("truncated %s", name)

		return nil
	}

	return d.fixed(int(size), name)
}

func (d *collatorRecordDecoder) count(name string, minimumRecordSize int) int {
	count := d.uint32(name + " count")
	if d.err != nil {
		return 0
	}
	if minimumRecordSize > 0 && uint64(count) > uint64(len(d.value)-d.pos)/uint64(minimumRecordSize) {
		d.err = fmt.Errorf("invalid %s count %d", name, count)

		return 0
	}

	return int(count)
}

func (d *collatorRecordDecoder) blockID(name string) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: d.int32(name + " workchain"),
		Shard:     d.int64(name + " shard"),
		SeqNo:     d.uint32(name + " seqno"),
		RootHash:  d.fixed(32, name+" root hash"),
		FileHash:  d.fixed(32, name+" file hash"),
	}
}

func (d *collatorRecordDecoder) shard(name string) groups.ShardID {
	return groups.ShardID{
		Workchain: d.int32(name + " workchain"),
		Shard:     d.int64(name + " shard"),
	}
}

func (d *collatorRecordDecoder) candidateID(name string) simplex.CandidateID {
	var id simplex.CandidateID
	id.Slot = d.uint32(name + " slot")
	copy(id.Hash[:], d.take(32, name+" hash"))

	return id
}

func (d *collatorRecordDecoder) parent(name string) simplex.ParentID {
	if !d.boolean(name + " present") {
		return simplex.ParentID{}
	}

	return simplex.Parent(d.candidateID(name))
}

func (d *collatorRecordDecoder) finish(name string) error {
	if d.err != nil {
		return fmt.Errorf("validator pebblestore: decode %s: %w", name, d.err)
	}
	if d.pos != len(d.value) {
		return fmt.Errorf("validator pebblestore: decode %s: %d trailing bytes", name, len(d.value)-d.pos)
	}

	return nil
}

func encodeCollatorSessionRecord(record collator.SessionRecord) ([]byte, error) {
	var encoder collatorRecordEncoder
	encoder.addByte(collatorSessionRecordVersion)
	encodeCollatorSession(&encoder, record.Session)
	encoder.addBool(record.Activation != nil)
	if record.Activation != nil {
		encodeCollatorSessionActivation(&encoder, *record.Activation)
	}
	encodeCollatorSessionUpdate(&encoder, record.Update)
	if encoder.err != nil {
		return nil, fmt.Errorf("validator pebblestore: encode collator session: %w", encoder.err)
	}

	return encoder.value, nil
}

func decodeCollatorSessionRecord(value []byte) (collator.SessionRecord, error) {
	decoder := collatorRecordDecoder{value: value}
	if version := decoder.byte("version"); version != collatorSessionRecordVersion && decoder.err == nil {
		return collator.SessionRecord{}, fmt.Errorf("validator pebblestore: collator session version %d", version)
	}

	record := collator.SessionRecord{Session: decodeCollatorSession(&decoder)}
	if decoder.boolean("session activation") {
		activation := decodeCollatorSessionActivation(&decoder, record.Session.ID)
		record.Activation = &activation
	}
	record.Update = decodeCollatorSessionUpdate(&decoder)
	if err := decoder.finish("collator session"); err != nil {
		return collator.SessionRecord{}, err
	}
	if record.Update.SessionID != record.Session.ID {
		return collator.SessionRecord{}, errors.New("validator pebblestore: collator session update ID mismatch")
	}

	return record, nil
}

func encodeCollatorSession(encoder *collatorRecordEncoder, session collator.Session) {
	encoder.addFixed(session.ID[:])
	encoder.addShard(session.Shard)
	encoder.addUint32(session.CatchainSeqno)
	encoder.addUint32(session.ValidatorSetHash)
	encoder.addByte(session.ConsensusVersion)
	encoder.addByte(session.ConsensusFlags)
	encoder.addByte(session.ProtocolVersion)
	encoder.addBool(session.UseQUIC)
	encoder.addUint32(session.SlotsPerLeaderWindow)
	encoder.addCount("validators", len(session.Validators))
	for i := range session.Validators {
		validator := session.Validators[i]
		encoder.addFixed(validator.PublicKey[:])
		encoder.addFixed(validator.ADNLID[:])
		encoder.addUint64(validator.Weight)
	}
}

func decodeCollatorSession(decoder *collatorRecordDecoder) collator.Session {
	var session collator.Session
	copy(session.ID[:], decoder.take(32, "session ID"))
	session.Shard = decoder.shard("session shard")
	session.CatchainSeqno = decoder.uint32("catchain seqno")
	session.ValidatorSetHash = decoder.uint32("validator set hash")
	session.ConsensusVersion = decoder.byte("consensus version")
	session.ConsensusFlags = decoder.byte("consensus flags")
	session.ProtocolVersion = decoder.byte("protocol version")
	session.UseQUIC = decoder.boolean("use QUIC")
	session.SlotsPerLeaderWindow = decoder.uint32("slots per leader window")

	count := decoder.count("validators", collatorValidatorSize)
	session.Validators = make([]collator.SessionValidator, count)
	for i := range session.Validators {
		copy(session.Validators[i].PublicKey[:], decoder.take(32, "validator public key"))
		copy(session.Validators[i].ADNLID[:], decoder.take(32, "validator ADNL ID"))
		session.Validators[i].Weight = decoder.uint64("validator weight")
	}

	return session
}

func encodeCollatorSessionActivation(
	encoder *collatorRecordEncoder,
	activation collator.SessionActivation,
) {
	encoder.addCount("genesis blocks", len(activation.Genesis))
	for i := range activation.Genesis {
		encoder.addBlockID("genesis block", activation.Genesis[i])
	}
	encoder.addBlockID("minimum masterchain block", activation.MinMasterchain)
}

func decodeCollatorSessionActivation(
	decoder *collatorRecordDecoder,
	sessionID [32]byte,
) collator.SessionActivation {
	activation := collator.SessionActivation{SessionID: sessionID}
	count := decoder.count("genesis blocks", collatorBlockIDSize)
	activation.Genesis = make([]ton.BlockIDExt, count)
	for i := range activation.Genesis {
		activation.Genesis[i] = decoder.blockID("genesis block")
	}
	activation.MinMasterchain = decoder.blockID("minimum masterchain block")

	return activation
}

func encodeCollatorSessionUpdate(encoder *collatorRecordEncoder, update collator.SessionUpdate) {
	if update.TargetRate <= 0 && encoder.err == nil {
		encoder.err = errors.New("target rate must be positive")
	}
	if !update.HasFinalizedBlock && !isZeroCollatorBlockID(update.FinalizedBlock) && encoder.err == nil {
		encoder.err = errors.New("absent finalized block has an ID")
	}
	if !update.HasCurrentWindow &&
		(update.CurrentWindowStart != 0 || update.CurrentWindowObservedSlot != 0 ||
			!update.CurrentWindowStartAt.IsZero()) && encoder.err == nil {
		encoder.err = errors.New("absent current window has timing data")
	}

	encoder.addFixed(update.SessionID[:])
	encoder.addInt64(int64(update.TargetRate))
	encoder.addInt64(int64(update.NoEmptyBlocksOnErrTimeout))
	encoder.addBlockID("masterchain block", update.MasterchainBlock)
	encoder.addBool(update.HasFinalizedBlock)
	if update.HasFinalizedBlock {
		encoder.addBlockID("finalized block", update.FinalizedBlock)
	}
	encoder.addCount("registered shards", len(update.Registered))
	for i := range update.Registered {
		registered := update.Registered[i]
		encoder.addShard(registered.Shard)
		encoder.addBlockID("registered block", registered.Block)
		encoder.addUint32(registered.NextCatchainSeqno)
		encoder.addBool(registered.BeforeSplit)
		encoder.addBool(registered.BeforeMerge)
		encoder.addByte(byte(registered.FSM.Kind))
		encoder.addUint32(registered.FSM.UTime)
		encoder.addUint32(registered.FSM.Interval)
	}
	encoder.addBool(update.HasCurrentWindow)
	if update.HasCurrentWindow {
		encoder.addUint32(update.CurrentWindowStart)
		encoder.addUint32(update.CurrentWindowObservedSlot)
		encoder.addInt64(update.CurrentWindowStartAt.Unix())
		encoder.addInt32(int32(update.CurrentWindowStartAt.Nanosecond()))
	}
	encoder.addParent(update.CurrentBase)
}

func decodeCollatorSessionUpdate(decoder *collatorRecordDecoder) collator.SessionUpdate {
	var update collator.SessionUpdate
	copy(update.SessionID[:], decoder.take(32, "update session ID"))
	update.TargetRate = time.Duration(decoder.int64("target rate"))
	if update.TargetRate <= 0 && decoder.err == nil {
		decoder.err = errors.New("target rate must be positive")
	}
	update.NoEmptyBlocksOnErrTimeout = time.Duration(decoder.int64("no-empty-blocks timeout"))
	if update.NoEmptyBlocksOnErrTimeout < 0 && decoder.err == nil {
		decoder.err = errors.New("no-empty-blocks timeout must not be negative")
	}
	update.MasterchainBlock = decoder.blockID("masterchain block")
	update.HasFinalizedBlock = decoder.boolean("finalized block")
	if update.HasFinalizedBlock {
		update.FinalizedBlock = decoder.blockID("finalized block")
	}

	count := decoder.count("registered shards", 12+collatorBlockIDSize+15)
	update.Registered = make([]groups.ShardDescription, count)
	for i := range update.Registered {
		registered := &update.Registered[i]
		registered.Shard = decoder.shard("registered shard")
		registered.Block = decoder.blockID("registered block")
		registered.NextCatchainSeqno = decoder.uint32("registered catchain seqno")
		registered.BeforeSplit = decoder.boolean("registered before split")
		registered.BeforeMerge = decoder.boolean("registered before merge")
		registered.FSM.Kind = groups.ShardFSMKind(decoder.byte("registered FSM kind"))
		registered.FSM.UTime = decoder.uint32("registered FSM time")
		registered.FSM.Interval = decoder.uint32("registered FSM interval")
		if registered.FSM.Kind > groups.ShardFSMMerge && decoder.err == nil {
			decoder.err = fmt.Errorf("invalid registered FSM kind %d", registered.FSM.Kind)
		}
	}
	update.HasCurrentWindow = decoder.boolean("current window")
	if update.HasCurrentWindow {
		update.CurrentWindowStart = decoder.uint32("current window start")
		update.CurrentWindowObservedSlot = decoder.uint32("current window observed slot")
		seconds := decoder.int64("current window start seconds")
		nanos := decoder.int32("current window start nanoseconds")
		if (nanos < 0 || nanos >= int32(time.Second)) && decoder.err == nil {
			decoder.err = fmt.Errorf("invalid current window nanoseconds %d", nanos)
		} else {
			update.CurrentWindowStartAt = time.Unix(seconds, int64(nanos)).UTC()
		}
	}
	update.CurrentBase = decoder.parent("current base")

	return update
}

func encodeCollatorCandidateRecord(record collator.CandidateRecord) ([]byte, error) {
	if !validCandidateAuthority(record.Authority) {
		return nil, fmt.Errorf(
			"validator pebblestore: encode collator candidate: invalid authority %d",
			record.Authority,
		)
	}
	capacity, err := collatorCandidateRecordCapacity(record)
	if err != nil {
		return nil, fmt.Errorf("validator pebblestore: encode collator candidate: %w", err)
	}
	encoder := collatorRecordEncoder{value: make([]byte, 0, capacity)}
	encoder.addByte(collatorCandidateRecordVersion)
	encoder.addFixed(record.WindowID.SessionID[:])
	encoder.addUint32(record.WindowID.StartSlot)
	encoder.addCandidateID(record.ID)
	encoder.addParent(record.Parent)
	encoder.addUint32(record.Leader)
	encoder.addBool(record.Empty)
	encoder.addBlockID("candidate block", record.Block)
	encoder.addFixed(record.CollatedFileHash[:])
	encoder.addBytes("candidate signature", record.Signature)
	encoder.addFixed(record.DelegationKey[:])
	encoder.addBytes("delegation signature", record.DelegationSignature)
	encoder.addByte(byte(record.Authority))
	if encoder.err != nil {
		return nil, fmt.Errorf("validator pebblestore: encode collator candidate: %w", encoder.err)
	}

	return encoder.value, nil
}

func collatorCandidateRecordCapacity(record collator.CandidateRecord) (int, error) {
	if !record.Parent.Exists && record.Parent.ID != (simplex.CandidateID{}) {
		return 0, errors.New("absent parent has a candidate ID")
	}
	if len(record.Block.RootHash) != 32 {
		return 0, fmt.Errorf("candidate block root hash length %d", len(record.Block.RootHash))
	}
	if len(record.Block.FileHash) != 32 {
		return 0, fmt.Errorf("candidate block file hash length %d", len(record.Block.FileHash))
	}

	size := uint64(collatorCandidateFixedSize)
	if record.Parent.Exists {
		size += 36
	}

	var err error
	if size, err = addCollatorRecordBytesSize(size, "candidate signature", record.Signature); err != nil {
		return 0, err
	}
	if size, err = addCollatorRecordBytesSize(size, "delegation signature", record.DelegationSignature); err != nil {
		return 0, err
	}
	if size > uint64(math.MaxInt) {
		return 0, errors.New("candidate record is too large")
	}

	return int(size), nil
}

func addCollatorRecordBytesSize(size uint64, name string, value []byte) (uint64, error) {
	if uint64(len(value)) > math.MaxUint32 {
		return 0, fmt.Errorf("%s is too large", name)
	}

	return size + uint64(len(value)), nil
}

// decodeCollatorCandidateRecord takes ownership of value. The record is a
// bounded signature marker, so every field it returns is copied out and the
// caller may reuse value afterwards.
func decodeCollatorCandidateRecord(value []byte) (collator.CandidateRecord, error) {
	decoder := collatorRecordDecoder{value: value}
	version := decoder.byte("version")
	if version != collatorCandidateRecordVersion && decoder.err == nil {
		return collator.CandidateRecord{}, fmt.Errorf("validator pebblestore: collator candidate version %d", version)
	}

	var record collator.CandidateRecord
	copy(record.WindowID.SessionID[:], decoder.take(32, "session ID"))
	record.WindowID.StartSlot = decoder.uint32("window start slot")
	record.ID = decoder.candidateID("candidate ID")
	record.Parent = decoder.parent("parent")
	record.Leader = decoder.uint32("leader")
	record.Empty = decoder.boolean("empty candidate")
	record.Block = decoder.blockID("candidate block")
	copy(record.CollatedFileHash[:], decoder.take(32, "collated file hash"))
	record.Signature = decoder.bytes("candidate signature")
	copy(record.DelegationKey[:], decoder.take(32, "delegation key"))
	record.DelegationSignature = decoder.bytes("delegation signature")
	record.Authority = collator.CandidateAuthority(decoder.byte("authority"))
	if !validCandidateAuthority(record.Authority) && decoder.err == nil {
		decoder.err = fmt.Errorf("invalid authority %d", record.Authority)
	}
	if err := decoder.finish("collator candidate"); err != nil {
		return collator.CandidateRecord{}, err
	}

	return record, nil
}

func validCandidateAuthority(authority collator.CandidateAuthority) bool {
	return authority == collator.CandidateAuthoritySelf ||
		authority == collator.CandidateAuthorityDelegated
}

func isZeroCollatorBlockID(block ton.BlockIDExt) bool {
	return block.Workchain == 0 && block.Shard == 0 && block.SeqNo == 0 &&
		len(block.RootHash) == 0 && len(block.FileHash) == 0
}
