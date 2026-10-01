package pebblestore

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	descriptorVersion                 = byte(1)
	descriptorSize                    = 126
	leaderVersion                     = byte(1)
	leaderRecordSize                  = 54
	delegationAuthorizationVersion    = byte(1)
	delegationAuthorizationRecordSize = 1 + 4 + 32 + ed25519.SignatureSize
)

// Validator records use the same little-endian primitives as the collator
// records, so they are built on the shared encoder/decoder rather than on
// hand-written byte offsets. The fixed sizes stay as capacity hints:
// encodeSessionID runs on every ensureSession.
func encodeSessionID(id validator.SessionStorageID) []byte {
	e := collatorRecordEncoder{value: make([]byte, 0, descriptorSize)}
	e.addByte(descriptorVersion)
	e.addFixed(id.SessionID[:])
	e.addShard(id.Shard)
	e.addUint32(id.CatchainSeqno)
	e.addBool(id.IsValidator)
	e.addFixed(id.ValidatorKeyID[:])
	e.addFixed(id.LocalADNLID[:])
	e.addInt32(int32(id.ValidatorIndex))
	e.addByte(id.Protocol.Version)
	e.addByte(id.Protocol.Flags)
	e.addByte(id.Protocol.ProtocolVersion)
	e.addBool(id.Protocol.UseQUIC)
	e.addUint32(id.Protocol.SlotsPerLeaderWindow)

	return e.value
}

func decodeSessionID(value []byte) (validator.SessionStorageID, error) {
	d := collatorRecordDecoder{value: value}
	if version := d.byte("session descriptor version"); d.err == nil && version != descriptorVersion {
		return validator.SessionStorageID{}, fmt.Errorf("validator pebblestore: session descriptor version %d", version)
	}

	var id validator.SessionStorageID
	copy(id.SessionID[:], d.fixed(32, "session id"))
	id.Shard = d.shard("session")
	id.CatchainSeqno = d.uint32("session catchain seqno")
	id.IsValidator = d.boolean("session role")
	copy(id.ValidatorKeyID[:], d.fixed(32, "session validator key id"))
	copy(id.LocalADNLID[:], d.fixed(32, "session adnl id"))
	id.ValidatorIndex = int(d.int32("session validator index"))
	id.Protocol.Version = d.byte("session protocol version")
	id.Protocol.Flags = d.byte("session protocol flags")
	id.Protocol.ProtocolVersion = d.byte("session wire protocol version")
	id.Protocol.UseQUIC = d.boolean("session QUIC marker")
	id.Protocol.SlotsPerLeaderWindow = d.uint32("session slots per leader window")
	// Structure before semantics: a truncated record must not be reported as an
	// invalid descriptor.
	if err := d.finish("session descriptor"); err != nil {
		return validator.SessionStorageID{}, err
	}
	if err := id.Validate(); err != nil {
		return validator.SessionStorageID{}, fmt.Errorf("validator pebblestore: invalid session descriptor: %w", err)
	}

	return id, nil
}

func encodeLeaderWindow(window validator.LeaderWindowRecord) []byte {
	e := collatorRecordEncoder{value: make([]byte, 0, leaderRecordSize)}
	e.addByte(leaderVersion)
	e.addBool(window.Base.Exists)
	e.addUint32(window.Base.ID.Slot)
	e.addFixed(window.Base.ID.Hash[:])
	e.addUint32(window.StartSlot)
	e.addUint32(window.EndSlot)
	e.addInt64(window.ObservedAt.UnixNano())

	return e.value
}

func decodeLeaderWindow(value []byte) (validator.LeaderWindowRecord, error) {
	d := collatorRecordDecoder{value: value}
	if version := d.byte("leader window version"); d.err == nil && version != leaderVersion {
		return validator.LeaderWindowRecord{}, fmt.Errorf("validator pebblestore: leader window version %d", version)
	}

	var window validator.LeaderWindowRecord
	window.Base.Exists = d.boolean("leader parent marker")
	window.Base.ID.Slot = d.uint32("leader parent slot")
	copy(window.Base.ID.Hash[:], d.fixed(32, "leader parent hash"))
	window.StartSlot = d.uint32("leader window start")
	window.EndSlot = d.uint32("leader window end")
	window.ObservedAt = time.Unix(0, d.int64("leader window observed at")).UTC()
	if err := d.finish("leader window"); err != nil {
		return validator.LeaderWindowRecord{}, err
	}
	if err := validateLeaderWindow(window); err != nil {
		return validator.LeaderWindowRecord{}, err
	}

	return window, nil
}

func validateLeaderWindow(window validator.LeaderWindowRecord) error {
	if window.EndSlot <= window.StartSlot {
		return fmt.Errorf("validator pebblestore: leader window [%d,%d) is invalid", window.StartSlot, window.EndSlot)
	}
	if !window.Base.Exists && window.Base.ID != (simplex.CandidateID{}) {
		return errors.New("validator pebblestore: genesis leader window has a parent id")
	}

	return nil
}

func encodeDelegationAuthorization(authorization validator.DelegationAuthorization) ([]byte, error) {
	if err := validateDelegationAuthorization(authorization); err != nil {
		return nil, err
	}

	e := collatorRecordEncoder{value: make([]byte, 0, delegationAuthorizationRecordSize)}
	e.addByte(delegationAuthorizationVersion)
	e.addUint32(authorization.StartSlot)
	e.addFixed(authorization.Collator[:])
	e.addFixed(authorization.Signature)

	return e.value, nil
}

func decodeDelegationAuthorization(value []byte) (validator.DelegationAuthorization, error) {
	d := collatorRecordDecoder{value: value}
	if version := d.byte("delegation authorization version"); d.err == nil && version != delegationAuthorizationVersion {
		return validator.DelegationAuthorization{}, fmt.Errorf(
			"validator pebblestore: delegation authorization version %d",
			version,
		)
	}

	var authorization validator.DelegationAuthorization
	authorization.StartSlot = d.uint32("delegation authorization start slot")
	copy(authorization.Collator[:], d.fixed(32, "delegation authorization collator id"))
	authorization.Signature = d.fixed(ed25519.SignatureSize, "delegation authorization signature")
	if err := d.finish("delegation authorization"); err != nil {
		return validator.DelegationAuthorization{}, err
	}
	if err := validateDelegationAuthorization(authorization); err != nil {
		return validator.DelegationAuthorization{}, err
	}

	return authorization, nil
}

func validateDelegationAuthorization(authorization validator.DelegationAuthorization) error {
	if authorization.Collator == ([32]byte{}) {
		return errors.New("validator pebblestore: delegation authorization collator id is zero")
	}
	if len(authorization.Signature) != ed25519.SignatureSize {
		return fmt.Errorf(
			"validator pebblestore: delegation authorization signature length %d, want %d",
			len(authorization.Signature),
			ed25519.SignatureSize,
		)
	}

	return nil
}
