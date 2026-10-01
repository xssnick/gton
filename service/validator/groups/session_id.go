package groups

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/tl"
)

const (
	groupMemberTLSchema      = "validator.groupMember public_key_hash:int256 adnl:int256 weight:long = engine.validator.GroupMember"
	groupNewTLSchema         = "validator.groupNew workchain:int shard:long vertical_seqno:int last_key_block_seqno:int catchain_seqno:int config_hash:int256 members:(vector validator.groupMember) = validator.Group"
	publicKeyED25519TLSchema = "pub.ed25519 key:int256 = PublicKey"
)

// publicKeyTLID is the boxed pub.ed25519 constructor id (4813b4c6, matching
// the C++ node). It is derived once here rather than per call: tl.CRC rewrites
// the schema string twice and runs a CRC over it, which dominates the hash it
// prefixes.
var publicKeyTLID = tl.CRC(publicKeyED25519TLSchema)

// GroupMemberTL is the exact validator.groupMember TL representation.
// It is public because it participates in TL registration.
type GroupMemberTL struct {
	PublicKeyHash []byte `tl:"int256"`
	ADNL          []byte `tl:"int256"`
	Weight        uint64 `tl:"long"`
}

// GroupNewTL is the exact validator.groupNew TL representation hashed into a
// validator session identifier.
type GroupNewTL struct {
	Workchain         int32           `tl:"int"`
	Shard             int64           `tl:"long"`
	VerticalSeqno     uint32          `tl:"int"`
	LastKeyBlockSeqno uint32          `tl:"int"`
	CatchainSeqno     uint32          `tl:"int"`
	ConfigHash        []byte          `tl:"int256"`
	Members           []GroupMemberTL `tl:"vector struct"`
}

// Member contains the values exported by a selected validator set. Member
// order is part of the session identifier and is preserved by SessionID.
type Member struct {
	PublicKeyHash [32]byte
	ADNL          [32]byte
	Weight        uint64
}

// UnsafeRotation selects an emergency rotation namespace. ID zero disables it
// and uses the normal consensus configuration hash.
type UnsafeRotation struct {
	ID uint32
}

// SessionIDInput contains every value hashed by validator.groupNew.
type SessionIDInput struct {
	Workchain            int32
	Shard                int64
	MaximalVerticalSeqno uint32
	LastKeyBlockSeqno    uint32
	CatchainSeqno        uint32
	UnsafeRotation       UnsafeRotation
	Members              []Member
}

func init() {
	tl.Register(GroupMemberTL{}, groupMemberTLSchema)
	tl.Register(GroupNewTL{}, groupNewTLSchema)
}

// PublicKeyHash returns the ADNL short identifier of an Ed25519 public key.
//
// This is byte for byte what tl.Hash over keys.PublicKeyED25519 produces: the
// boxed form of pub.ed25519 is the little-endian constructor id followed by the
// raw 32 key bytes, and nothing else. Building those 36 bytes by hand avoids
// reflecting over the struct into a bytes.Buffer, which cost 6 allocations per
// validator on every config parse and session construction. The error is kept
// because the input is a fixed-size array and can never fail, but a dozen call
// sites already plumb it.
func PublicKeyHash(publicKey [32]byte) ([32]byte, error) {
	var boxed [4 + 32]byte
	binary.LittleEndian.PutUint32(boxed[:4], publicKeyTLID)
	copy(boxed[4:], publicKey[:])

	return sha256.Sum256(boxed[:]), nil
}

// SessionID computes the validator.groupNew wire hash.
func SessionID(input SessionIDInput) ([32]byte, error) {
	members := make([]GroupMemberTL, len(input.Members))
	for i := range input.Members {
		member := &input.Members[i]
		if member.Weight > math.MaxInt64 {
			return [32]byte{}, fmt.Errorf("member %d weight %d exceeds TL long", i, member.Weight)
		}

		members[i] = GroupMemberTL{
			PublicKeyHash: member.PublicKeyHash[:],
			ADNL:          member.ADNL[:],
			Weight:        member.Weight,
		}
	}

	configHash := sessionConfigHash()
	if input.UnsafeRotation.ID != 0 {
		configHash = [32]byte{}
		binary.LittleEndian.PutUint32(configHash[:4], input.UnsafeRotation.ID)
	}

	hash, err := tl.Hash(GroupNewTL{
		Workchain:         input.Workchain,
		Shard:             input.Shard,
		VerticalSeqno:     input.MaximalVerticalSeqno,
		LastKeyBlockSeqno: input.LastKeyBlockSeqno,
		CatchainSeqno:     input.CatchainSeqno,
		ConfigHash:        configHash[:],
		Members:           members,
	})
	if err != nil {
		return [32]byte{}, fmt.Errorf("hash validator.groupNew: %w", err)
	}

	var result [32]byte
	copy(result[:], hash)
	return result, nil
}

func sessionConfigHash() [32]byte {
	return [32]byte{
		0x0a, 0x5b, 0xf2, 0x39, 0x9f, 0x17, 0x2f, 0xee,
		0x5a, 0x8e, 0x78, 0x6f, 0x55, 0xa9, 0xd2, 0x71,
		0x49, 0xd1, 0xed, 0x33, 0xe6, 0xb8, 0xe0, 0xcc,
		0x81, 0xef, 0x45, 0xfa, 0x3b, 0x8c, 0xb8, 0xd7,
	}
}
