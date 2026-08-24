package simplex

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// Wire types of the collator delegation protocol:
// a leader probes with consensus.pleaseCollatePrepare and delegates its
// window with consensus.pleaseCollate, the collator signs candidates with its
// own key and attaches consensus.delegation, and candidate containers carry
// the delegation either in the broadcast extra (consensus.BroadcastExtra) or
// in the consensus.candidate wrapper (db and resolver paths).
//
// Scheme strings are exact lines of the experimental collators branch of
// ton-blockchain/ton; golden constructor ids are pinned in tl_test.go. The
// flag-bearing constructors are hand-coded — the reflection loader has no
// conditional fields.
const (
	schemeDelegationToSign     = "consensus.delegationToSign window_start_slot:int collator_id:int256 = consensus.DelegationToSign"
	schemeDelegation           = "consensus.delegation collator_key:PublicKey signature:bytes = consensus.Delegation"
	schemePleaseCollatePrepare = "consensus.pleaseCollatePrepare window_start_slot:int = tonNode.Success"
	schemePleaseCollate        = "consensus.pleaseCollate window_start_slot:int signature:bytes = tonNode.Success"
	schemeCandidateBlock       = "consensus.block slot:int parent:consensus.CandidateParent candidate:bytes signature:bytes = consensus.CandidateData"
	schemeCandidateEmpty       = "consensus.empty slot:int parent:consensus.CandidateId block:tonNode.blockIdExt signature:bytes = consensus.CandidateData"

	schemeCandidateWrapped = "consensus.candidate flags:# data:consensus.CandidateData delegation:flags.0?consensus.Delegation = consensus.Candidate"
	schemeBroadcastExtra   = "consensus.broadcastExtra flags:# slot:int delegation:flags.0?consensus.Delegation = consensus.BroadcastExtra"

	// This constructor is deliberately emitted for non-delegated validator
	// candidates. Despite its name it remains part of
	// the live delegation wire format.
	idBroadcastExtraLegacy = uint32(0x921297fa)
)

// ConsensusDelegationToSign is consensus.delegationToSign — the payload a
// leader signs through consensus.dataToSign to delegate one window.
type ConsensusDelegationToSign struct {
	WindowStartSlot int32  `tl:"int"`
	CollatorID      []byte `tl:"int256"`
}

// ConsensusDelegation is consensus.delegation.
type ConsensusDelegation struct {
	CollatorKey any    `tl:"struct boxed [pub.ed25519]"`
	Signature   []byte `tl:"bytes"`
}

// ConsensusPleaseCollatePrepare probes a collator one leader window before
// the delegated window. It reserves no state.
type ConsensusPleaseCollatePrepare struct {
	WindowStartSlot int32 `tl:"int"`
}

// ConsensusPleaseCollate commits one leader-signed delegation.
type ConsensusPleaseCollate struct {
	WindowStartSlot int32  `tl:"int"`
	Signature       []byte `tl:"bytes"`
}

// ConsensusBlockData is consensus.block — an ordinary candidate payload.
type ConsensusBlockData struct {
	Slot      int32  `tl:"int"`
	Parent    any    `tl:"struct boxed [consensus.candidateParent,consensus.candidateWithoutParents]"`
	Candidate []byte `tl:"bytes"`
	Signature []byte `tl:"bytes"`
}

// ConsensusEmptyData is consensus.empty — an empty candidate payload.
type ConsensusEmptyData struct {
	Slot      int32          `tl:"int"`
	Parent    any            `tl:"struct boxed [consensus.candidateId]"`
	Block     ton.BlockIDExt `tl:"struct"`
	Signature []byte         `tl:"bytes"`
}

func init() {
	tl.Register(ConsensusDelegationToSign{}, schemeDelegationToSign)
	tl.Register(ConsensusDelegation{}, schemeDelegation)
	tl.Register(ConsensusPleaseCollatePrepare{}, schemePleaseCollatePrepare)
	tl.Register(ConsensusPleaseCollate{}, schemePleaseCollate)
	tl.Register(ConsensusBlockData{}, schemeCandidateBlock)
	tl.Register(ConsensusEmptyData{}, schemeCandidateEmpty)
}

var (
	idBroadcastExtra   = tl.CRC(schemeBroadcastExtra)
	idCandidateWrapped = tl.CRC(schemeCandidateWrapped)
)

// BroadcastExtra is the parsed consensus.BroadcastExtra of a candidate
// broadcast: the slot plus the optional collator delegation.
type BroadcastExtra struct {
	Slot       uint32
	Delegation *Delegation
}

// Serialize matches the live wire format: ordinary validator candidates use
// broadcastExtraLegacy, delegated candidates use the flags
// constructor and carry consensus.delegation.
func (b *BroadcastExtra) Serialize() ([]byte, error) {
	if b.Delegation == nil {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint32(buf[:4], idBroadcastExtraLegacy)
		binary.LittleEndian.PutUint32(buf[4:], b.Slot)
		return buf, nil
	}

	deleg, err := tl.Serialize(delegationToTL(b.Delegation), true)
	if err != nil {
		return nil, fmt.Errorf("simplex/tl: serialize delegation: %w", err)
	}
	buf := make([]byte, 12, 12+len(deleg))
	binary.LittleEndian.PutUint32(buf[:4], idBroadcastExtra)
	binary.LittleEndian.PutUint32(buf[4:8], 1)
	binary.LittleEndian.PutUint32(buf[8:], b.Slot)
	return append(buf, deleg...), nil
}

// ParseBroadcastExtra decodes both constructors currently in use. The flags
// constructor with flags=0 is accepted as well, even though broadcasters emit
// the legacy form.
func ParseBroadcastExtra(data []byte) (*BroadcastExtra, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("simplex/tl: broadcast extra is too short")
	}
	id := binary.LittleEndian.Uint32(data[:4])
	if id == idBroadcastExtraLegacy {
		if len(data) != 8 {
			return nil, fmt.Errorf("simplex/tl: invalid legacy broadcast extra size %d", len(data))
		}
		return &BroadcastExtra{Slot: binary.LittleEndian.Uint32(data[4:])}, nil
	}
	if id != idBroadcastExtra {
		return nil, fmt.Errorf("simplex/tl: unexpected broadcast extra constructor %#x", id)
	}
	if len(data) < 12 {
		return nil, fmt.Errorf("simplex/tl: broadcast extra is too short")
	}
	flags := binary.LittleEndian.Uint32(data[4:8])
	if flags&^uint32(1) != 0 {
		return nil, fmt.Errorf("simplex/tl: unknown broadcast extra flags %#x", flags)
	}
	out := &BroadcastExtra{Slot: binary.LittleEndian.Uint32(data[8:12])}
	rest := data[12:]
	if flags&1 != 0 {
		var wire ConsensusDelegation
		left, err := tl.Parse(&wire, rest, true)
		if err != nil {
			return nil, fmt.Errorf("simplex/tl: parse delegation: %w", err)
		}
		if out.Delegation, err = delegationFromTL(wire); err != nil {
			return nil, err
		}
		rest = left
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("simplex/tl: trailing bytes in broadcast extra")
	}
	return out, nil
}

// ConsensusCandidateWrapped is the storage/resolver representation of a
// consensus candidate. Data is a ConsensusBlockData or ConsensusEmptyData.
// Protocol v3 wraps it in consensus.candidate only when delegation is present.
type ConsensusCandidateWrapped struct {
	Data       any
	Delegation *Delegation
}

// Serialize emits the candidate wire form: non-delegated
// candidates are emitted as bare boxed consensus.block/consensus.empty, while
// delegated candidates use the consensus.candidate wrapper.
func (c *ConsensusCandidateWrapped) Serialize() ([]byte, error) {
	data, err := tl.Serialize(c.Data, true)
	if err != nil {
		return nil, fmt.Errorf("simplex/tl: serialize candidate data: %w", err)
	}

	return wrapCandidateData(data, c.Delegation)
}

// ParseCandidateWrapped decodes both the v3 bare non-delegated form and the
// delegation-aware consensus.candidate wrapper.
func ParseCandidateWrapped(data []byte) (*ConsensusCandidateWrapped, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("simplex/tl: candidate data is too short")
	}
	if binary.LittleEndian.Uint32(data[:4]) != idCandidateWrapped {
		inner, rest, err := parseCandidateData(data)
		if err != nil {
			return nil, err
		}
		if len(rest) != 0 {
			return nil, fmt.Errorf("simplex/tl: trailing bytes in candidate data")
		}
		return &ConsensusCandidateWrapped{Data: inner}, nil
	}
	if len(data) < 8 {
		return nil, fmt.Errorf("simplex/tl: candidate wrapper is too short")
	}

	flags := binary.LittleEndian.Uint32(data[4:8])
	if flags&^uint32(1) != 0 {
		return nil, fmt.Errorf("simplex/tl: unknown candidate wrapper flags %#x", flags)
	}

	inner, rest, err := parseCandidateData(data[8:])
	if err != nil {
		return nil, err
	}
	out := &ConsensusCandidateWrapped{Data: inner}
	if flags&1 != 0 {
		var wire ConsensusDelegation
		if rest, err = tl.Parse(&wire, rest, true); err != nil {
			return nil, fmt.Errorf("simplex/tl: parse delegation: %w", err)
		}
		if out.Delegation, err = delegationFromTL(wire); err != nil {
			return nil, err
		}
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("simplex/tl: trailing bytes in candidate wrapper")
	}
	return out, nil
}

// ParseCandidateData decodes one bare consensus.block or consensus.empty.
// Private-overlay broadcasts carry this form and keep an optional delegation
// in BroadcastExtra; storage and resolver wires may instead use the wrapper
// accepted by ParseCandidateWrapped.
func ParseCandidateData(data []byte) (tl.Serializable, error) {
	inner, rest, err := parseCandidateData(data)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("simplex/tl: trailing bytes in candidate data")
	}

	return inner, nil
}

func parseCandidateData(data []byte) (tl.Serializable, []byte, error) {
	var inner tl.Serializable
	rest, err := tl.Parse(&inner, data, true)
	if err != nil {
		return nil, nil, fmt.Errorf("simplex/tl: parse candidate data: %w", err)
	}
	switch inner.(type) {
	case ConsensusBlockData, *ConsensusBlockData, ConsensusEmptyData, *ConsensusEmptyData:
		return inner, rest, nil
	default:
		return nil, nil, fmt.Errorf("simplex/tl: unexpected candidate data type %T", inner)
	}
}

// delegationToTL converts the engine delegation to its wire form.
func delegationToTL(d *Delegation) ConsensusDelegation {
	return ConsensusDelegation{
		CollatorKey: keys.PublicKeyED25519{Key: d.CollatorKey},
		Signature:   d.Signature,
	}
}

// delegationFromTL converts a wire delegation back; only pub.ed25519
// collator keys are representable.
func delegationFromTL(wire ConsensusDelegation) (*Delegation, error) {
	var key keys.PublicKeyED25519
	switch x := wire.CollatorKey.(type) {
	case keys.PublicKeyED25519:
		key = x
	case *keys.PublicKeyED25519:
		key = *x
	default:
		return nil, fmt.Errorf("simplex/tl: unexpected collator key type %T", wire.CollatorKey)
	}
	if len(key.Key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("simplex/tl: invalid collator key length %d", len(key.Key))
	}
	return &Delegation{CollatorKey: key.Key, Signature: wire.Signature}, nil
}
