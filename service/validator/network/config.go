package network

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/xssnick/tonutils-go/adnl/overlay"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

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
	if config.Protocol.ProtocolVersion == 0 {
		return sessionSpec{}, errors.New("validator network: protocol version 0 has no persistent observer role")
	}

	spec, err := m.validatorOwnedSessionSpec(config)
	if err != nil {
		return sessionSpec{}, err
	}
	spec.role = collator.OverlayRoleObserver

	return spec, nil
}

func (m *Manager) validatorOwnedSessionSpec(config validator.SessionConfig) (sessionSpec, error) {
	protocolVersion := config.Protocol.ProtocolVersion
	if protocolVersion > simplex.MaxProtocolVersion {
		return sessionSpec{}, fmt.Errorf(
			"validator network: protocol version %d is unsupported, maximum is %d",
			protocolVersion,
			simplex.MaxProtocolVersion,
		)
	}
	if config.Protocol.Version != 2 {
		return sessionSpec{}, fmt.Errorf(
			"validator network: consensus version %d is unsupported, require 2",
			config.Protocol.Version,
		)
	}
	if config.Protocol.SlotsPerLeaderWindow == 0 {
		return sessionSpec{}, errors.New("validator network: slots per leader window must be positive")
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
	twoStepMembers, err := twoStepIntermediateMembers(config.AllCurrentValidators)
	if err != nil {
		return sessionSpec{}, err
	}
	if err = validateCollatorRegistry(keyIDs, config.CollatorsByValidator); err != nil {
		return sessionSpec{}, err
	}
	consensusCollators := config.AllCollators
	if config.Shard.IsMasterchain() {
		consensusCollators = nil
	}
	consensusMemberIDs := config.OverlayMembers
	if !observersInPrivateOverlay(protocolVersion) {
		consensusMemberIDs = privateConsensusMemberIDs(config.Validators, consensusCollators)
	}
	members, err := validatedOverlayMembers(consensusMemberIDs, config.Validators)
	if err != nil {
		return sessionSpec{}, err
	}
	if err = requireOverlayMembers(members, consensusCollators, "consensus"); err != nil {
		return sessionSpec{}, err
	}
	localConsensusMember := slices.Contains(members, p2p.PeerID(m.localADNLID))
	openConsensus := localConsensusMember
	if protocolVersion == 1 && config.Identity.Validator == nil {
		openConsensus = false
	}
	if !localConsensusMember && !(protocolVersion == 1 && config.Identity.Validator == nil) {
		return sessionSpec{}, fmt.Errorf(
			"%w: session consensus overlay does not contain %x",
			ErrLocalADNLUnavailable,
			m.localADNLID,
		)
	}
	maxReplyBytes, err := maximumCandidateBytes(
		config.CandidateLimits.MaxBlockBytes,
		config.CandidateLimits.MaxCollatedDataBytes,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	candidateCollators := consensusCollators
	if protocolVersion == 1 {
		candidateCollators = config.AllCollators
	}
	authorized, candidateADNL, err := authorizedCandidateSources(
		protocolVersion,
		keyIDs,
		config.Validators,
		candidateCollators,
		maxReplyBytes,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	consensusAuthorized := authorized
	if protocolVersion == 1 {
		consensusAuthorized, _, err = authorizedCandidateSources(
			0,
			keyIDs,
			config.Validators,
			consensusCollators,
			maxReplyBytes,
		)
		if err != nil {
			return sessionSpec{}, err
		}
	}
	identitySeed, err := BuildConsensusOverlayIdentity(config.SessionID, keyIDs)
	if err != nil {
		return sessionSpec{}, err
	}
	var blockSyncIdentity OverlayIdentity
	var blockSyncMembers []p2p.PeerID
	if protocolVersion == 1 {
		blockSyncIdentity, err = BuildBlockSyncOverlayIdentity(config.SessionID)
		if err != nil {
			return sessionSpec{}, err
		}
		blockSyncMembers, err = overlayMembers(config.OverlayMembers, config.Validators, m.localADNLID)
		if err != nil {
			return sessionSpec{}, err
		}
		if err = requireOverlayMembers(blockSyncMembers, config.AllCollators, "block-sync"); err != nil {
			return sessionSpec{}, err
		}
	}

	return sessionSpec{
		id:                   config.SessionID,
		kind:                 sessionKindValidator,
		protocolVersion:      protocolVersion,
		useQUIC:              config.Protocol.UseQUIC,
		slotsPerLeaderWindow: config.Protocol.SlotsPerLeaderWindow,
		openConsensus:        openConsensus,
		workchain:            config.Shard.Workchain,
		shard:                config.Shard.Shard,
		fullOverlayID:        identitySeed.FullID,
		members:              members,
		peers:                remoteMembers(members, m.localADNLID),
		blockSyncFullID:      blockSyncIdentity.FullID,
		blockSyncMembers:     blockSyncMembers,
		twoStepMembers:       twoStepMembers,
		validatorByADNL:      validatorByADNL,
		validatorKeys:        validatorPublicKeys(config.Validators),
		validatorCount:       len(config.Validators),
		catchainSeqno:        config.CatchainSeqno,
		validatorSetHash:     config.ValidatorSetHash,
		maxReplyBytes:        maxReplyBytes,
		consensusAuthorized:  consensusAuthorized,
		authorized:           authorized,
		candidateADNL:        candidateADNL,
		validatorSource:      validatorSourceSet(protocolVersion, keyIDs, config.Validators),
	}, nil
}

func (m *Manager) observerSessionSpec(descriptor collator.OverlaySession) (sessionSpec, error) {
	protocolVersion := descriptor.Session.ProtocolVersion
	if protocolVersion > simplex.MaxProtocolVersion {
		return sessionSpec{}, fmt.Errorf(
			"validator network: protocol version %d is unsupported, maximum is %d",
			protocolVersion,
			simplex.MaxProtocolVersion,
		)
	}
	if descriptor.Session.ConsensusVersion != 2 {
		return sessionSpec{}, fmt.Errorf(
			"validator network: consensus version %d is unsupported, require 2",
			descriptor.Session.ConsensusVersion,
		)
	}
	if descriptor.Session.SlotsPerLeaderWindow == 0 {
		return sessionSpec{}, errors.New("validator network: slots per leader window must be positive")
	}
	if protocolVersion == 0 {
		return sessionSpec{}, errors.New("validator network: protocol version 0 has no standalone overlay role")
	}
	if descriptor.Role != collator.OverlayRoleObserver && descriptor.Role != collator.OverlayRoleCollator {
		return sessionSpec{}, fmt.Errorf("validator network: invalid overlay role %d", descriptor.Role)
	}
	if descriptor.Session.Shard.IsMasterchain() && descriptor.Role == collator.OverlayRoleCollator {
		return sessionSpec{}, errors.New("validator network: masterchain session has no collator role")
	}
	if !descriptor.Session.Shard.IsMasterchain() {
		registered := slices.Contains(descriptor.AllCollators, m.localADNLID)
		if registered != (descriptor.Role == collator.OverlayRoleCollator) {
			return sessionSpec{}, errors.New(
				"validator network: non-masterchain overlay role differs from global collator registration",
			)
		}
	}
	expectedBroadcastMode := collator.CandidateBroadcastPrivateOverlay
	if protocolVersion == 1 {
		expectedBroadcastMode = collator.CandidateBroadcastBlockSyncOverlay
	}
	if descriptor.BroadcastMode != expectedBroadcastMode {
		return sessionSpec{}, fmt.Errorf(
			"validator network: protocol version %d requires candidate broadcast mode %d",
			protocolVersion,
			expectedBroadcastMode,
		)
	}
	expectedObservers := observersInPrivateOverlay(protocolVersion)
	if descriptor.ObserversInPrivateOverlay != expectedObservers {
		return sessionSpec{}, fmt.Errorf(
			"validator network: protocol version %d private-overlay observer policy mismatch",
			protocolVersion,
		)
	}

	validators, keyIDs, validatorByADNL, err := observerRoster(descriptor.Session.Validators)
	if err != nil {
		return sessionSpec{}, err
	}
	twoStepMembers, err := twoStepIntermediateMembers(descriptor.AllCurrentValidators)
	if err != nil {
		return sessionSpec{}, err
	}
	if err = validateCollatorRegistry(keyIDs, descriptor.CollatorsByValidator); err != nil {
		return sessionSpec{}, err
	}
	consensusCollators := descriptor.AllCollators
	if descriptor.Session.Shard.IsMasterchain() {
		consensusCollators = nil
	}
	memberIDs := descriptor.AllOverlayNodes
	if !expectedObservers {
		memberIDs = privateConsensusMemberIDs(validators, consensusCollators)
	}
	members, err := validatedOverlayMembers(memberIDs, validators)
	if err != nil {
		return sessionSpec{}, err
	}
	if err = requireOverlayMembers(members, consensusCollators, "consensus"); err != nil {
		return sessionSpec{}, err
	}
	localConsensusMember := slices.Contains(members, p2p.PeerID(m.localADNLID))
	openConsensus := localConsensusMember
	if protocolVersion == 1 && descriptor.Role == collator.OverlayRoleObserver {
		openConsensus = false
	}
	if !localConsensusMember && !(protocolVersion == 1 && descriptor.Role == collator.OverlayRoleObserver) {
		return sessionSpec{}, fmt.Errorf(
			"%w: session consensus overlay does not contain %x",
			ErrLocalADNLUnavailable,
			m.localADNLID,
		)
	}
	maxReplyBytes, err := maximumCandidateBytes(
		descriptor.MaxBlockSize,
		descriptor.MaxCollatedDataSize,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	candidateCollators := consensusCollators
	if protocolVersion == 1 {
		candidateCollators = descriptor.AllCollators
	}
	authorized, candidateADNL, err := authorizedCandidateSources(
		protocolVersion,
		keyIDs,
		validators,
		candidateCollators,
		maxReplyBytes,
	)
	if err != nil {
		return sessionSpec{}, err
	}
	consensusAuthorized := authorized
	if protocolVersion == 1 {
		consensusAuthorized, _, err = authorizedCandidateSources(
			0,
			keyIDs,
			validators,
			consensusCollators,
			maxReplyBytes,
		)
		if err != nil {
			return sessionSpec{}, err
		}
	}
	var blockSyncIdentity OverlayIdentity
	var blockSyncMembers []p2p.PeerID
	if protocolVersion == 1 {
		blockSyncIdentity, err = BuildBlockSyncOverlayIdentity(descriptor.Session.ID)
		if err != nil {
			return sessionSpec{}, err
		}
		blockSyncMembers, err = overlayMembers(descriptor.AllOverlayNodes, validators, m.localADNLID)
		if err != nil {
			return sessionSpec{}, err
		}
		if err = requireOverlayMembers(blockSyncMembers, descriptor.AllCollators, "block-sync"); err != nil {
			return sessionSpec{}, err
		}
	}
	identitySeed, err := BuildConsensusOverlayIdentity(descriptor.Session.ID, keyIDs)
	if err != nil {
		return sessionSpec{}, err
	}

	return sessionSpec{
		id:                   descriptor.Session.ID,
		kind:                 sessionKindObserver,
		role:                 descriptor.Role,
		protocolVersion:      protocolVersion,
		useQUIC:              descriptor.Session.UseQUIC,
		slotsPerLeaderWindow: descriptor.Session.SlotsPerLeaderWindow,
		openConsensus:        openConsensus,
		workchain:            descriptor.Session.Shard.Workchain,
		shard:                descriptor.Session.Shard.Shard,
		fullOverlayID:        identitySeed.FullID,
		members:              members,
		peers:                remoteMembers(members, m.localADNLID),
		blockSyncFullID:      blockSyncIdentity.FullID,
		blockSyncMembers:     blockSyncMembers,
		twoStepMembers:       twoStepMembers,
		validatorByADNL:      validatorByADNL,
		validatorKeys:        validatorPublicKeys(validators),
		validatorCount:       len(validators),
		catchainSeqno:        descriptor.Session.CatchainSeqno,
		validatorSetHash:     descriptor.Session.ValidatorSetHash,
		maxReplyBytes:        maxReplyBytes,
		consensusAuthorized:  consensusAuthorized,
		authorized:           authorized,
		candidateADNL:        candidateADNL,
		validatorSource:      validatorSourceSet(protocolVersion, keyIDs, validators),
		// A nil signer deliberately binds this handle to the private-overlay
		// registry's node ADNL key without exposing that key to extensions.
		signer: nil,
	}, nil
}

func observersInPrivateOverlay(protocolVersion uint8) bool {
	return protocolVersion >= 2
}

func privateConsensusMemberIDs(
	validators []groups.Validator,
	allCollators [][32]byte,
) [][32]byte {
	members := make([][32]byte, 0, len(validators)+len(allCollators))
	for i := range validators {
		members = append(members, groups.ValidatorADNL(validators[i]))
	}
	members = append(members, allCollators...)

	return members
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

func validatorPublicKeys(validators []groups.Validator) [][32]byte {
	keys := make([][32]byte, len(validators))
	for i := range validators {
		keys[i] = validators[i].PublicKey
	}

	return keys
}

func twoStepIntermediateMembers(input [][32]byte) ([]p2p.PeerID, error) {
	if len(input) == 0 {
		return nil, errors.New("validator network: empty current-validator intermediate roster")
	}

	members := make([]p2p.PeerID, len(input))
	for i := range input {
		members[i] = p2p.PeerID(input[i])
		if members[i].IsZero() {
			return nil, fmt.Errorf("validator network: current-validator intermediate %d is zero", i)
		}
	}
	slices.SortFunc(members, func(left, right p2p.PeerID) int {
		return bytes.Compare(left[:], right[:])
	})

	return slices.Compact(members), nil
}

func overlayMembers(
	input [][32]byte,
	validators []groups.Validator,
	localADNLID [32]byte,
) ([]p2p.PeerID, error) {
	members, err := validatedOverlayMembers(input, validators)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(members, p2p.PeerID(localADNLID)) {
		return nil, fmt.Errorf("%w: session overlay does not contain %x", ErrLocalADNLUnavailable, localADNLID)
	}

	return members, nil
}

func validatedOverlayMembers(
	input [][32]byte,
	validators []groups.Validator,
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
	for i := range validators {
		validatorADNL := groups.ValidatorADNL(validators[i])
		if _, ok := seen[p2p.PeerID(validatorADNL)]; !ok {
			return nil, fmt.Errorf("validator network: overlay omits validator %d ADNL ID", i)
		}
	}

	return members, nil
}

func requireOverlayMembers(
	members []p2p.PeerID,
	required [][32]byte,
	name string,
) error {
	for i, id := range required {
		if !slices.Contains(members, p2p.PeerID(id)) {
			return fmt.Errorf("validator network: %s overlay omits collator ADNL ID %d", name, i)
		}
	}

	return nil
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
	protocolVersion uint8,
	validatorKeyIDs [][32]byte,
	validators []groups.Validator,
	allCollators [][32]byte,
	maxCandidateBytes uint32,
) (map[p2p.PeerID]uint32, map[p2p.PeerID]p2p.PeerID, error) {
	if len(validators) != len(validatorKeyIDs) {
		return nil, nil, errors.New("validator network: validator source mapping size mismatch")
	}
	authorized := make(map[p2p.PeerID]uint32, len(validatorKeyIDs)+len(allCollators))
	candidateADNL := make(map[p2p.PeerID]p2p.PeerID, len(validatorKeyIDs)+len(allCollators))
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
		if protocolVersion == 1 {
			source = p2p.PeerID(groups.ValidatorADNL(validators[i]))
		}
		authorized[source] = maxCandidateBytes
		candidateADNL[source] = p2p.PeerID(groups.ValidatorADNL(validators[i]))
	}
	for i, id := range allCollators {
		peer := p2p.PeerID(id)
		if peer.IsZero() {
			return nil, nil, fmt.Errorf("validator network: collator ADNL ID %d is zero", i)
		}
		if _, alreadyAuthorized := authorized[peer]; alreadyAuthorized {
			continue
		}
		authorized[peer] = maxCandidateBytes
		candidateADNL[peer] = peer
	}

	return authorized, candidateADNL, nil
}

func validateCollatorRegistry(
	validatorKeyIDs [][32]byte,
	registry []groups.CollatorRegistryEntry,
) error {
	validatorKeys := make(map[[32]byte]struct{}, len(validatorKeyIDs))
	for _, keyID := range validatorKeyIDs {
		validatorKeys[keyID] = struct{}{}
	}
	for i := range registry {
		entry := registry[i]
		if _, ok := validatorKeys[entry.ValidatorKeyID]; !ok {
			return fmt.Errorf("validator network: collator registry entry %d has an unknown validator", i)
		}
		for j, id := range entry.CollatorADNLIDs {
			if p2p.PeerID(id).IsZero() {
				return fmt.Errorf("validator network: collator registry entry %d ID %d is zero", i, j)
			}
		}
	}

	return nil
}

func publicKeyID(publicKey ed25519.PublicKey) ([32]byte, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return [32]byte{}, errors.New("validator network: invalid Ed25519 public key")
	}

	var key [ed25519.PublicKeySize]byte
	copy(key[:], publicKey)
	return groups.PublicKeyHash(key)
}

func validatorSourceSet(
	protocolVersion uint8,
	keyIDs [][32]byte,
	validators []groups.Validator,
) map[p2p.PeerID]int {
	result := make(map[p2p.PeerID]int, len(keyIDs))
	for i, id := range keyIDs {
		source := p2p.PeerID(id)
		if protocolVersion == 1 {
			source = p2p.PeerID(groups.ValidatorADNL(validators[i]))
		}
		result[source] = i
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
