package pebblestore

import (
	"encoding/binary"
	"fmt"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	keySchema byte = iota
	keySession
	keyVote
	keyPoolState
	keyCandidateIndex
	keyCandidateContent
	keyFinalized
	keyLeaderWindow
	keySummary
	// keyAcceptedProof is retired. Accepted shard proof links are no longer
	// cached here: the shard top description reads its predecessor links from
	// the node's own block storage and waits for them to appear. The constant
	// stays reserved so the prefixes above keep their on-disk values, and
	// DeleteSession still sweeps it so rows an older build wrote are removed
	// with their namespace instead of outliving the descriptor that names them.
	keyAcceptedProof
	keyDelegationAuthorization
	keyValidatorKey
)

var schemaKey = []byte{keySchema}

// storageNamespace is the single-Pebble equivalent of the canonical path
// workchain.shard.catchain-seqno.<consensus.dbId hash>.
type storageNamespace [48]byte

func namespaceForSession(id validator.SessionStorageID) (storageNamespace, error) {
	dbID, err := id.Namespace()
	if err != nil {
		return storageNamespace{}, err
	}

	var namespace storageNamespace
	binary.LittleEndian.PutUint32(namespace[0:4], uint32(id.Shard.Workchain))
	binary.LittleEndian.PutUint64(namespace[4:12], uint64(id.Shard.Shard))
	binary.LittleEndian.PutUint32(namespace[12:16], id.CatchainSeqno)
	copy(namespace[16:], dbID[:])

	return namespace, nil
}

func sessionKey(namespace storageNamespace) []byte {
	return namespacedPrefix(keySession, namespace)
}

func summaryKey(namespace storageNamespace) []byte {
	return namespacedPrefix(keySummary, namespace)
}

func votePrefix(namespace storageNamespace) []byte {
	return namespacedPrefix(keyVote, namespace)
}

func voteKey(namespace storageNamespace, recordKey []byte) []byte {
	return append(votePrefix(namespace), recordKey...)
}

func poolStateKey(namespace storageNamespace) []byte {
	return namespacedPrefix(keyPoolState, namespace)
}

func candidateIndexPrefix(namespace storageNamespace) []byte {
	return namespacedPrefix(keyCandidateIndex, namespace)
}

func candidateIndexKey(namespace storageNamespace, id simplex.CandidateID) []byte {
	return appendCandidateID(candidateIndexPrefix(namespace), id)
}

func candidateContentPrefix(namespace storageNamespace) []byte {
	return namespacedPrefix(keyCandidateContent, namespace)
}

func candidateContentKey(namespace storageNamespace, hash [32]byte) []byte {
	return append(candidateContentPrefix(namespace), hash[:]...)
}

func finalizedPrefix(namespace storageNamespace) []byte {
	return namespacedPrefix(keyFinalized, namespace)
}

func finalizedKey(namespace storageNamespace, id simplex.CandidateID) []byte {
	return appendCandidateID(finalizedPrefix(namespace), id)
}

func leaderWindowPrefix(namespace storageNamespace) []byte {
	return namespacedPrefix(keyLeaderWindow, namespace)
}

func leaderWindowKey(namespace storageNamespace, startSlot uint32) []byte {
	return binary.BigEndian.AppendUint32(leaderWindowPrefix(namespace), startSlot)
}

// acceptedProofPrefix addresses the retired kind. Nothing writes under it; only
// the namespace sweep reads it, so a database written before it was retired is
// cleaned up by ordinary teardown rather than needing a migration.
func acceptedProofPrefix(namespace storageNamespace) []byte {
	return namespacedPrefix(keyAcceptedProof, namespace)
}

func delegationAuthorizationPrefix(namespace storageNamespace) []byte {
	return namespacedPrefix(keyDelegationAuthorization, namespace)
}

func delegationAuthorizationKey(namespace storageNamespace, startSlot uint32) []byte {
	return binary.BigEndian.AppendUint32(delegationAuthorizationPrefix(namespace), startSlot)
}

func validatorKeyPrefix() []byte {
	return []byte{keyValidatorKey}
}

func validatorKeyKey(id [32]byte) []byte {
	return append(validatorKeyPrefix(), id[:]...)
}

func validatorKeyIDFromKey(key []byte) ([32]byte, error) {
	prefix := validatorKeyPrefix()
	if len(key) != len(prefix)+32 {
		return [32]byte{}, fmt.Errorf("validator pebblestore: validator key length %d", len(key))
	}

	var id [32]byte
	copy(id[:], key[len(prefix):])

	return id, nil
}

func namespacedPrefix(kind byte, namespace storageNamespace) []byte {
	key := make([]byte, 1, 1+len(namespace))
	key[0] = kind

	return append(key, namespace[:]...)
}

func appendCandidateID(dst []byte, id simplex.CandidateID) []byte {
	dst = binary.BigEndian.AppendUint32(dst, id.Slot)

	return append(dst, id.Hash[:]...)
}

func candidateIDFromKey(key, prefix []byte) (simplex.CandidateID, error) {
	if len(key) != len(prefix)+36 {
		return simplex.CandidateID{}, fmt.Errorf("validator pebblestore: candidate key length %d", len(key))
	}

	id := simplex.CandidateID{Slot: binary.BigEndian.Uint32(key[len(prefix):])}
	copy(id.Hash[:], key[len(prefix)+4:])

	return id, nil
}

func prefixBounds(prefix []byte) ([]byte, []byte) {
	lower := append([]byte(nil), prefix...)
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++

			return lower, upper[:i+1]
		}
	}

	return lower, nil
}
