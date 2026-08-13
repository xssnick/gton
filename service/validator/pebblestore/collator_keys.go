package pebblestore

import (
	"encoding/binary"

	"github.com/xssnick/gton/service/validator/collator"
)

const collatorKeyspace byte = 0x80

const (
	collatorKeySession byte = iota + 1
	collatorKeyCandidate
)

func collatorRecordPrefix(kind byte) []byte {
	return []byte{collatorKeyspace, kind}
}

func collatorSessionKey(sessionID [32]byte) []byte {
	return append(collatorRecordPrefix(collatorKeySession), sessionID[:]...)
}

func collatorCandidatePrefix(id collator.WindowID) []byte {
	key := append(collatorRecordPrefix(collatorKeyCandidate), id.SessionID[:]...)

	return binary.BigEndian.AppendUint32(key, id.StartSlot)
}

func collatorCandidateSessionPrefix(sessionID [32]byte) []byte {
	return append(collatorRecordPrefix(collatorKeyCandidate), sessionID[:]...)
}

func collatorCandidateKey(id collator.WindowID, slot uint32) []byte {
	return binary.BigEndian.AppendUint32(collatorCandidatePrefix(id), slot)
}
