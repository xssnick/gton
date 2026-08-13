package network

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/adnl/overlay"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

const supportedProtocolVersion = 3

func (m *Manager) validatorSessionSpec(config validator.SessionConfig) (sessionSpec, error) {
	identity := config.Identity.Validator
	if identity == nil || identity.Signer == nil {
		return sessionSpec{}, errors.New("validator network: local validator identity is required")
	}
	if identity.Index >= uint32(len(config.Validators)) {
		return sessionSpec{}, fmt.Errorf(
			"validator network: local validator index %d is out of range",
			identity.Index,
		)
	}

	spec, err := m.validatorOwnedSessionSpec(config)
	if err != nil {
		return sessionSpec{}, err
	}
	localValidator := config.Validators[identity.Index]
	if identity.KeyID != localValidator.PublicKeyHash {
		return sessionSpec{}, errors.New("validator network: local validator key differs from roster")
	}
	localValidatorADNL := groups.ValidatorADNL(localValidator)
	if localValidatorADNL != m.localADNLID {
		return sessionSpec{}, fmt.Errorf(
			"%w: local validator roster requires %x, node has %x",
			ErrLocalADNLUnavailable,
			localValidatorADNL,
			m.localADNLID,
		)
	}
	spec.signer = validatorBroadcastSigner{
		publicKey: ed25519.PublicKey(localValidator.PublicKey[:]),
		signer:    identity.Signer,
	}

	return spec, nil
}

func (m *Manager) persistentObserverSessionSpec(config validator.SessionConfig) (sessionSpec, error) {
	if config.Identity.Validator != nil {
		return sessionSpec{}, errors.New("validator network: persistent observer has a validator identity")
	}

	spec, err := m.validatorOwnedSessionSpec(config)
	if err != nil {
		return sessionSpec{}, err
	}
	spec.role = collator.OverlayRoleObserver

	return spec, nil
}

func (m *Manager) validatorOwnedSessionSpec(config validator.SessionConfig) (sessionSpec, error) {
	if config.Protocol.ProtocolVersion != supportedProtocolVersion {
		return sessionSpec{}, fmt.Errorf(
			"validator network: protocol version %d is unsupported, require exactly %d",
			config.Protocol.ProtocolVersion,
			supportedProtocolVersion,
		)
	}
	if config.Identity.ADNLID != m.localADNLID {
		return sessionSpec{}, fmt.Errorf(
			"%w: session %x requires %x, node has %x",
			ErrLocalADNLUnavailable,
			config.SessionID,
			config.Identity.ADNLID,
			m.localADNLID,
		)
	}

	keyIDs, validatorByADNL, err := validatorRoster(config.Validators)
	if err != nil {
		return sessionSpec{}, err
	}
	members, err := overlayMembers(config.OverlayMembers, config.Validators, m.localADNLID)
	if err != nil {
		return sessionSpec{}, err
	}
	maxReplyBytes, err := maximumCandidateBytes(
		config.CandidateLimits.MaxBlockBytes,
		config.CandidateLimits.MaxCollatedDataBytes,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	authorized, candidateADNL, err := authorizedCandidateSources(
		keyIDs,
		config.Validators,
		config.CollatorsByValidator,
		maxReplyBytes,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	identitySeed, err := BuildConsensusOverlayIdentity(config.SessionID, keyIDs)
	if err != nil {
		return sessionSpec{}, err
	}

	return sessionSpec{
		id:               config.SessionID,
		kind:             sessionKindValidator,
		workchain:        config.Shard.Workchain,
		shard:            config.Shard.Shard,
		fullOverlayID:    identitySeed.FullID,
		members:          members,
		peers:            remoteMembers(members, m.localADNLID),
		validatorByADNL:  validatorByADNL,
		validatorCount:   len(config.Validators),
		catchainSeqno:    config.CatchainSeqno,
		validatorSetHash: config.ValidatorSetHash,
		maxReplyBytes:    maxReplyBytes,
		authorized:       authorized,
		candidateADNL:    candidateADNL,
		validatorSource:  validatorSourceSet(keyIDs),
	}, nil
}

func (m *Manager) observerSessionSpec(descriptor collator.OverlaySession) (sessionSpec, error) {
	if descriptor.Session.ProtocolVersion != supportedProtocolVersion {
		return sessionSpec{}, fmt.Errorf(
			"validator network: protocol version %d is unsupported, require exactly %d",
			descriptor.Session.ProtocolVersion,
			supportedProtocolVersion,
		)
	}
	if descriptor.Role != collator.OverlayRoleObserver && descriptor.Role != collator.OverlayRoleCollator {
		return sessionSpec{}, fmt.Errorf("validator network: invalid overlay role %d", descriptor.Role)
	}

	validators, keyIDs, validatorByADNL, err := observerRoster(descriptor.Session.Validators)
	if err != nil {
		return sessionSpec{}, err
	}
	memberIDs := descriptor.AllOverlayNodes
	if !descriptor.ObserversInPrivateOverlay {
		memberIDs = make([][32]byte, len(validators))
		for i := range validators {
			memberIDs[i] = groups.ValidatorADNL(validators[i])
		}
	}
	members, err := overlayMembers(memberIDs, validators, m.localADNLID)
	if err != nil {
		return sessionSpec{}, err
	}
	maxReplyBytes, err := maximumCandidateBytes(
		descriptor.MaxBlockSize,
		descriptor.MaxCollatedDataSize,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	authorized, candidateADNL, err := authorizedCandidateSources(
		keyIDs,
		validators,
		descriptor.CollatorsByValidator,
		maxReplyBytes,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	if descriptor.Role == collator.OverlayRoleCollator {
		if _, ok := authorized[p2p.PeerID(m.localADNLID)]; !ok {
			return sessionSpec{}, errors.New("validator network: local collator is not authorized for session")
		}
	}
	identitySeed, err := BuildConsensusOverlayIdentity(descriptor.Session.ID, keyIDs)
	if err != nil {
		return sessionSpec{}, err
	}

	return sessionSpec{
		id:               descriptor.Session.ID,
		kind:             sessionKindObserver,
		role:             descriptor.Role,
		workchain:        descriptor.Session.Shard.Workchain,
		shard:            descriptor.Session.Shard.Shard,
		fullOverlayID:    identitySeed.FullID,
		members:          members,
		peers:            remoteMembers(members, m.localADNLID),
		validatorByADNL:  validatorByADNL,
		validatorCount:   len(validators),
		catchainSeqno:    descriptor.Session.CatchainSeqno,
		validatorSetHash: descriptor.Session.ValidatorSetHash,
		maxReplyBytes:    maxReplyBytes,
		authorized:       authorized,
		candidateADNL:    candidateADNL,
		validatorSource:  validatorSourceSet(keyIDs),
		// A nil signer deliberately binds this handle to the private-overlay
		// registry's node ADNL key without exposing that key to extensions.
		signer: nil,
	}, nil
}

func validatorRoster(
	validators []groups.Validator,
) ([][32]byte, map[p2p.PeerID]int, error) {
	if len(validators) == 0 {
		return nil, nil, errors.New("validator network: empty validator roster")
	}

	keyIDs := make([][32]byte, len(validators))
	byADNL := make(map[p2p.PeerID]int, len(validators))
	for i := range validators {
		validator := validators[i]
		keyID, err := groups.PublicKeyHash(validator.PublicKey)
		if err != nil {
			return nil, nil, err
		}
		if keyID != validator.PublicKeyHash {
			return nil, nil, fmt.Errorf("validator network: validator %d public key hash mismatch", i)
		}
		peer := p2p.PeerID(groups.ValidatorADNL(validator))
		if _, duplicate := byADNL[peer]; duplicate {
			return nil, nil, fmt.Errorf("validator network: validator %d has duplicate ADNL ID", i)
		}

		keyIDs[i] = keyID
		byADNL[peer] = i
	}

	return keyIDs, byADNL, nil
}

func observerRoster(
	validators []collator.SessionValidator,
) ([]groups.Validator, [][32]byte, map[p2p.PeerID]int, error) {
	if len(validators) == 0 {
		return nil, nil, nil, errors.New("validator network: empty validator roster")
	}

	result := make([]groups.Validator, len(validators))
	keyIDs := make([][32]byte, len(validators))
	byADNL := make(map[p2p.PeerID]int, len(validators))
	for i := range validators {
		validator := validators[i]
		keyID, err := groups.PublicKeyHash(validator.PublicKey)
		if err != nil {
			return nil, nil, nil, err
		}
		resolved := groups.Validator{
			PublicKey:     validator.PublicKey,
			PublicKeyHash: keyID,
			ADNL:          validator.ADNLID,
			Weight:        validator.Weight,
		}
		resolved.ADNL = groups.ValidatorADNL(resolved)
		peer := p2p.PeerID(resolved.ADNL)
		if _, duplicate := byADNL[peer]; duplicate {
			return nil, nil, nil, fmt.Errorf("validator network: validator %d has duplicate ADNL ID", i)
		}

		result[i] = resolved
		keyIDs[i] = keyID
		byADNL[peer] = i
	}

	return result, keyIDs, byADNL, nil
}

func overlayMembers(
	input [][32]byte,
	validators []groups.Validator,
	localADNLID [32]byte,
) ([]p2p.PeerID, error) {
	if len(input) == 0 {
		return nil, errors.New("validator network: empty overlay membership")
	}

	members := make([]p2p.PeerID, 0, len(input))
	seen := make(map[p2p.PeerID]struct{}, len(input))
	for i, raw := range input {
		peer := p2p.PeerID(raw)
		if peer.IsZero() {
			return nil, fmt.Errorf("validator network: overlay member %d is zero", i)
		}
		if _, duplicate := seen[peer]; duplicate {
			continue
		}

		seen[peer] = struct{}{}
		members = append(members, peer)
	}
	if _, ok := seen[p2p.PeerID(localADNLID)]; !ok {
		return nil, fmt.Errorf("%w: session overlay does not contain %x", ErrLocalADNLUnavailable, localADNLID)
	}
	for i := range validators {
		validatorADNL := groups.ValidatorADNL(validators[i])
		if _, ok := seen[p2p.PeerID(validatorADNL)]; !ok {
			return nil, fmt.Errorf("validator network: overlay omits validator %d ADNL ID", i)
		}
	}

	return members, nil
}

func remoteMembers(members []p2p.PeerID, localADNLID [32]byte) []p2p.PeerID {
	local := p2p.PeerID(localADNLID)
	peers := make([]p2p.PeerID, 0, len(members)-1)
	for _, member := range members {
		if member != local {
			peers = append(peers, member)
		}
	}

	return peers
}

func maximumCandidateBytes(maxBlockBytes, maxCollatedDataBytes uint32) (uint32, error) {
	if maxBlockBytes == 0 || maxCollatedDataBytes == 0 {
		return 0, errors.New("validator network: candidate size limits must be positive")
	}
	total := uint64(maxBlockBytes) + uint64(maxCollatedDataBytes) + 1<<20
	if total > math.MaxUint32 {
		return 0, errors.New("validator network: maximum candidate size overflows uint32")
	}

	return uint32(total), nil
}

func authorizedCandidateSources(
	validatorKeyIDs [][32]byte,
	validators []groups.Validator,
	registry []groups.CollatorRegistryEntry,
	maxCandidateBytes uint32,
) (map[p2p.PeerID]uint32, map[p2p.PeerID]p2p.PeerID, error) {
	if len(validators) != len(validatorKeyIDs) {
		return nil, nil, errors.New("validator network: validator source mapping size mismatch")
	}
	authorized := make(map[p2p.PeerID]uint32, len(validatorKeyIDs)+len(registry))
	candidateADNL := make(map[p2p.PeerID]p2p.PeerID, len(validatorKeyIDs)+len(registry))
	validatorKeys := make(map[[32]byte]struct{}, len(validatorKeyIDs))
	for i, keyID := range validatorKeyIDs {
		if keyID == ([32]byte{}) {
			return nil, nil, fmt.Errorf("validator network: validator %d key ID is zero", i)
		}
		if _, duplicate := validatorKeys[keyID]; duplicate {
			return nil, nil, fmt.Errorf("validator network: validator %d key ID is duplicated", i)
		}
		validatorKeys[keyID] = struct{}{}
		source := p2p.PeerID(keyID)
		authorized[source] = maxCandidateBytes
		candidateADNL[source] = p2p.PeerID(groups.ValidatorADNL(validators[i]))
	}
	for i := range registry {
		entry := registry[i]
		if _, ok := validatorKeys[entry.ValidatorKeyID]; !ok {
			return nil, nil, fmt.Errorf("validator network: collator registry entry %d has an unknown validator", i)
		}
		for j, id := range entry.CollatorADNLIDs {
			peer := p2p.PeerID(id)
			if peer.IsZero() {
				return nil, nil, fmt.Errorf("validator network: collator registry entry %d ID %d is zero", i, j)
			}
			if _, alreadyAuthorized := authorized[peer]; alreadyAuthorized {
				continue
			}
			authorized[peer] = maxCandidateBytes
			candidateADNL[peer] = peer
		}
	}

	return authorized, candidateADNL, nil
}

func publicKeyID(publicKey ed25519.PublicKey) ([32]byte, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return [32]byte{}, errors.New("validator network: invalid Ed25519 public key")
	}

	var key [ed25519.PublicKeySize]byte
	copy(key[:], publicKey)
	return groups.PublicKeyHash(key)
}

func validatorSourceSet(keyIDs [][32]byte) map[p2p.PeerID]struct{} {
	result := make(map[p2p.PeerID]struct{}, len(keyIDs))
	for _, id := range keyIDs {
		result[p2p.PeerID(id)] = struct{}{}
	}
	return result
}

type validatorBroadcastSigner struct {
	publicKey ed25519.PublicKey
	signer    simplex.Signer
}

var _ overlay.BroadcastSigner = validatorBroadcastSigner{}

func (s validatorBroadcastSigner) PublicKey() ed25519.PublicKey {
	return s.publicKey
}

func (s validatorBroadcastSigner) Sign(payload []byte) ([]byte, error) {
	return s.signer.Sign(payload)
}
