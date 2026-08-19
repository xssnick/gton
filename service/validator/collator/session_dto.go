package collator

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func validateSessionRecord(record SessionRecord) error {
	if err := validateSession(record.Session); err != nil {
		return err
	}
	if record.Activation != nil {
		if err := validateSessionActivation(record.Session, *record.Activation); err != nil {
			return err
		}
	}
	return validateSessionUpdate(record.Session, record.Update)
}

func validateSession(session Session) error {
	if session.ID == ([32]byte{}) {
		return errors.New("collator runtime: session id is zero")
	}
	if !session.Shard.IsValid() {
		return errors.New("collator runtime: session shard is invalid")
	}
	if session.ConsensusVersion != 2 {
		return errors.New("collator runtime: unsupported simplex version")
	}
	if session.ProtocolVersion > simplex.MaxProtocolVersion {
		return errors.New("collator runtime: unsupported simplex protocol version")
	}
	if session.SlotsPerLeaderWindow == 0 {
		return errors.New("collator runtime: slots per leader window is zero")
	}
	if len(session.Validators) == 0 {
		return errors.New("collator runtime: validator roster is empty")
	}
	seenADNL := make(map[[32]byte]struct{}, len(session.Validators))
	var total uint64
	for i := range session.Validators {
		validator := session.Validators[i]
		if validator.PublicKey == ([ed25519.PublicKeySize]byte{}) || validator.ADNLID == ([32]byte{}) || validator.Weight == 0 {
			return fmt.Errorf("collator runtime: validator %d is invalid", i)
		}
		if _, duplicate := seenADNL[validator.ADNLID]; duplicate {
			return fmt.Errorf("collator runtime: duplicate validator ADNL ID at index %d", i)
		}
		seenADNL[validator.ADNLID] = struct{}{}
		if total > math.MaxUint64-validator.Weight {
			return errors.New("collator runtime: validator weight overflows")
		}
		total += validator.Weight
	}
	return nil
}

func validateActivatedSession(session ActivatedSession) error {
	if err := validateSession(session.Session); err != nil {
		return err
	}
	if len(session.Genesis) < 1 || len(session.Genesis) > 2 {
		return errors.New("collator runtime: session genesis must contain one or two blocks")
	}
	for i := range session.Genesis {
		if err := validateBlockID(session.Genesis[i]); err != nil {
			return fmt.Errorf("collator runtime: genesis block %d is invalid", i)
		}
	}
	if err := validateBlockID(session.MinMasterchain); err != nil ||
		session.MinMasterchain.Workchain != -1 || session.MinMasterchain.Shard != -1<<63 {
		return errors.New("collator runtime: minimum masterchain block is invalid")
	}
	return nil
}

func validateSessionActivation(session Session, activation SessionActivation) error {
	if activation.SessionID != session.ID {
		return ErrSessionConflict
	}

	return validateActivatedSession(activatedSession(session, activation))
}

func validateSessionUpdate(session Session, update SessionUpdate) error {
	if update.SessionID != session.ID {
		return ErrSessionConflict
	}
	if update.TargetRate <= 0 {
		return errors.New("collator runtime: target rate must be positive")
	}
	if update.NoEmptyBlocksOnErrTimeout < 0 {
		return errors.New("collator runtime: no-empty-blocks timeout must not be negative")
	}
	if err := validateBlockID(update.MasterchainBlock); err != nil ||
		update.MasterchainBlock.Workchain != -1 || update.MasterchainBlock.Shard != -1<<63 {
		return errors.New("collator runtime: masterchain block id is invalid")
	}
	if !update.HasFinalizedBlock {
		if !isZeroBlockID(update.FinalizedBlock) {
			return errors.New("collator runtime: finalized block is set without presence flag")
		}
	} else {
		if err := validateBlockID(update.FinalizedBlock); err != nil ||
			update.FinalizedBlock.Workchain != session.Shard.Workchain ||
			update.FinalizedBlock.Shard != session.Shard.Shard {
			return errors.New("collator runtime: finalized block id is invalid")
		}
	}
	registered := make(map[groups.ShardID]struct{}, len(update.Registered))
	for i := range update.Registered {
		description := update.Registered[i]
		if !description.Shard.IsValid() || description.Block.Workchain != description.Shard.Workchain ||
			description.Block.Shard != description.Shard.Shard || validateBlockID(description.Block) != nil {
			return fmt.Errorf("collator runtime: registered shard %d block id is invalid", i)
		}
		if _, duplicate := registered[description.Shard]; duplicate {
			return fmt.Errorf("collator runtime: registered shard %d is duplicated", i)
		}
		registered[description.Shard] = struct{}{}
	}
	if !update.HasCurrentWindow {
		if update.CurrentWindowStart != 0 || update.CurrentWindowObservedSlot != 0 ||
			!update.CurrentWindowStartAt.IsZero() ||
			update.CurrentBase.Exists || update.CurrentBase.ID != (simplex.CandidateID{}) {
			return errors.New("collator runtime: current window fields are set without a current window")
		}
		return nil
	}
	if update.CurrentWindowStart%session.SlotsPerLeaderWindow != 0 {
		return errors.New("collator runtime: current window is not aligned")
	}
	if update.CurrentWindowObservedSlot == update.CurrentWindowStart && update.CurrentWindowStartAt.IsZero() {
		return errors.New("collator runtime: current window start time is zero")
	}
	windowEnd := update.CurrentWindowStart + session.SlotsPerLeaderWindow
	if windowEnd < update.CurrentWindowStart ||
		update.CurrentWindowObservedSlot < update.CurrentWindowStart ||
		update.CurrentWindowObservedSlot >= windowEnd {
		return errors.New("collator runtime: observed slot is outside the current window")
	}
	if update.CurrentBase.Exists && update.CurrentBase.ID.Slot >= update.CurrentWindowObservedSlot {
		return errors.New("collator runtime: current base is not before its window")
	}
	if !update.CurrentBase.Exists && update.CurrentBase.ID != (simplex.CandidateID{}) {
		return errors.New("collator runtime: absent current base carries an id")
	}
	return nil
}

func validateBlockID(id ton.BlockIDExt) error {
	if !(groups.ShardID{Workchain: id.Workchain, Shard: id.Shard}).IsValid() {
		return errors.New("invalid shard")
	}
	if len(id.RootHash) != sha256.Size || len(id.FileHash) != sha256.Size {
		return errors.New("invalid hashes")
	}
	return nil
}

func isZeroBlockID(id ton.BlockIDExt) bool {
	return id.Workchain == 0 && id.Shard == 0 && id.SeqNo == 0 && len(id.RootHash) == 0 && len(id.FileHash) == 0
}

func sameActivatedSession(a, b ActivatedSession) bool {
	return a.Session.Equal(b.Session) && equalBlockIDs(a.Genesis, b.Genesis) &&
		sameBlockID(a.MinMasterchain, b.MinMasterchain)
}

// Equal reports whether two activations name the same session and chain start.
func (a SessionActivation) Equal(other SessionActivation) bool {
	return a.SessionID == other.SessionID && equalBlockIDs(a.Genesis, other.Genesis) &&
		sameBlockID(a.MinMasterchain, other.MinMasterchain)
}

func equalBlockIDs(left, right []ton.BlockIDExt) bool {
	return slices.EqualFunc(left, right, sameBlockID)
}

// Equal reports whether two updates carry the same chain state and window. It
// gates ErrSessionConflict, so it compares every field a session is pinned by.
func (a SessionUpdate) Equal(other SessionUpdate) bool {
	b := other
	if !sameSessionChainUpdate(a, b) ||
		a.HasCurrentWindow != b.HasCurrentWindow || a.CurrentWindowStart != b.CurrentWindowStart ||
		a.CurrentWindowObservedSlot != b.CurrentWindowObservedSlot ||
		!a.CurrentWindowStartAt.Equal(b.CurrentWindowStartAt) || a.CurrentBase != b.CurrentBase {
		return false
	}

	return true
}

// sameSessionChainUpdate stays unexported on purpose: it is the deliberate
// partial comparator the restore branch uses, which tolerates a moved window.
func sameSessionChainUpdate(a, b SessionUpdate) bool {
	return a.SessionID == b.SessionID && a.TargetRate == b.TargetRate &&
		a.NoEmptyBlocksOnErrTimeout == b.NoEmptyBlocksOnErrTimeout &&
		sameBlockID(a.MasterchainBlock, b.MasterchainBlock) &&
		a.HasFinalizedBlock == b.HasFinalizedBlock && sameBlockID(a.FinalizedBlock, b.FinalizedBlock) &&
		slices.EqualFunc(a.Registered, b.Registered, sameShardDescription)
}

func sameShardDescription(x, y groups.ShardDescription) bool {
	return x.Shard == y.Shard && sameBlockID(x.Block, y.Block) &&
		x.NextCatchainSeqno == y.NextCatchainSeqno && x.BeforeSplit == y.BeforeSplit &&
		x.BeforeMerge == y.BeforeMerge && x.FSM == y.FSM
}

func sameBlockID(a, b ton.BlockIDExt) bool {
	return a.Workchain == b.Workchain && a.Shard == b.Shard && a.SeqNo == b.SeqNo &&
		bytes.Equal(a.RootHash, b.RootHash) && bytes.Equal(a.FileHash, b.FileHash)
}

func cloneSessionRecord(record SessionRecord) SessionRecord {
	cloned := SessionRecord{Session: cloneSession(record.Session), Update: cloneSessionUpdate(record.Update)}
	if record.Activation != nil {
		activation := cloneSessionActivation(*record.Activation)
		cloned.Activation = &activation
	}
	return cloned
}

func cloneSession(session Session) Session {
	session.Validators = append([]SessionValidator(nil), session.Validators...)
	return session
}

func cloneSessionActivation(activation SessionActivation) SessionActivation {
	activation.Genesis = cloneBlockIDs(activation.Genesis)
	activation.MinMasterchain = cloneBlockID(activation.MinMasterchain)
	return activation
}

func activatedSession(session Session, activation SessionActivation) ActivatedSession {
	return ActivatedSession{
		Session:        session,
		Genesis:        activation.Genesis,
		MinMasterchain: activation.MinMasterchain,
	}
}

func cloneBlockIDs(ids []ton.BlockIDExt) []ton.BlockIDExt {
	cloned := make([]ton.BlockIDExt, len(ids))
	for i := range ids {
		cloned[i] = cloneBlockID(ids[i])
	}

	return cloned
}

func cloneSessionUpdate(update SessionUpdate) SessionUpdate {
	update.MasterchainBlock = cloneBlockID(update.MasterchainBlock)
	update.FinalizedBlock = cloneBlockID(update.FinalizedBlock)
	update.Registered = append([]groups.ShardDescription(nil), update.Registered...)
	for i := range update.Registered {
		update.Registered[i].Block = cloneBlockID(update.Registered[i].Block)
	}
	return update
}

func cloneBlockID(id ton.BlockIDExt) ton.BlockIDExt {
	id.RootHash = append([]byte(nil), id.RootHash...)
	id.FileHash = append([]byte(nil), id.FileHash...)
	return id
}

func recordFromArtifact(artifact CandidateArtifact) CandidateRecord {
	candidate := artifact.Candidate
	authority := CandidateAuthoritySelf
	var delegationKey [ed25519.PublicKeySize]byte
	var delegationSignature []byte
	if candidate.Delegation != nil {
		authority = CandidateAuthorityDelegated
		copy(delegationKey[:], candidate.Delegation.CollatorKey)
		delegationSignature = candidate.Delegation.Signature
	}
	return CandidateRecord{
		WindowID:            artifact.WindowID,
		Authority:           authority,
		ID:                  candidate.ID,
		Parent:              candidate.Parent,
		Leader:              candidate.Leader,
		Empty:               candidate.Empty,
		Block:               candidate.Block,
		CollatedFileHash:    candidate.CollatedFileHash,
		Signature:           candidate.Signature,
		DelegationKey:       delegationKey,
		DelegationSignature: delegationSignature,
	}
}
