package collator

import (
	"errors"
	"fmt"
	"slices"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type sessionProjectionPolicy struct {
	localADNLID [32]byte
}

type projectedCollatorSession struct {
	session    Session
	activation *SessionActivation
	update     SessionUpdate
	overlay    OverlaySession
	params     simplex.Params
}

func (s projectedCollatorSession) hasBackend() bool {
	return s.overlay.Role == OverlayRoleCollator
}

func (s projectedCollatorSession) active() bool {
	return s.activation != nil
}

func newSessionProjectionPolicy(options ControllerOptions) (sessionProjectionPolicy, error) {
	localADNLID := options.Observer.LocalADNLID()
	if localADNLID == ([32]byte{}) {
		return sessionProjectionPolicy{}, errors.New("collator controller: local ADNL ID is zero")
	}

	return sessionProjectionPolicy{localADNLID: localADNLID}, nil
}

func projectCollatorSessions(
	snapshot *groups.Snapshot,
	policy sessionProjectionPolicy,
) (map[[32]byte]projectedCollatorSession, error) {
	if snapshot.Config == nil {
		return nil, errors.New("collator controller: validator config is absent")
	}
	registeredLocalCollator := false
	for i := range snapshot.CollatorsByValidator {
		if slices.Contains(snapshot.CollatorsByValidator[i].CollatorADNLIDs, policy.localADNLID) {
			registeredLocalCollator = true
			break
		}
	}
	if !registeredLocalCollator {
		return map[[32]byte]projectedCollatorSession{}, nil
	}

	allOverlayNodes := make([][32]byte, len(snapshot.PersistentOverlay))
	for i := range snapshot.PersistentOverlay {
		allOverlayNodes[i] = snapshot.PersistentOverlay[i].ADNL
	}

	projected := make(map[[32]byte]projectedCollatorSession, len(snapshot.Active)+len(snapshot.Future))
	add := func(group groups.Session, active bool) error {
		role, registry := projectedCollatorRole(snapshot, group, policy)
		consensus := snapshot.Config.NewConsensus.Shard
		if group.Shard.IsMasterchain() {
			consensus = snapshot.Config.NewConsensus.Masterchain
		}
		if consensus == nil {
			if role == OverlayRoleObserver {
				return nil
			}

			return fmt.Errorf("collator controller: session %x has no simplex config", group.ID)
		}
		if !consensus.SupportedProtocol() {
			if role == OverlayRoleObserver {
				return nil
			}

			return fmt.Errorf(
				"collator controller: session %x requires simplex v2 protocol version 3 or newer",
				group.ID,
			)
		}

		session := projectCollatorSession(
			snapshot,
			group,
			allOverlayNodes,
			*consensus,
			role,
			registry,
			active,
		)
		if _, duplicate := projected[session.session.ID]; duplicate {
			return fmt.Errorf("collator controller: duplicate session id %x", session.session.ID)
		}
		projected[session.session.ID] = session

		return nil
	}
	for i := range snapshot.Future {
		if err := add(snapshot.Future[i], false); err != nil {
			return nil, err
		}
	}
	for i := range snapshot.Active {
		if err := add(snapshot.Active[i], true); err != nil {
			return nil, err
		}
	}

	return projected, nil
}

func projectCollatorSession(
	snapshot *groups.Snapshot,
	group groups.Session,
	allOverlayNodes [][32]byte,
	consensus groups.SimplexConfig,
	role OverlayRole,
	registry []groups.CollatorRegistryEntry,
	active bool,
) projectedCollatorSession {
	validators := make([]SessionValidator, len(group.Validators))
	for i := range group.Validators {
		validator := group.Validators[i]
		validators[i] = SessionValidator{
			PublicKey: validator.PublicKey,
			ADNLID:    groups.ValidatorADNL(validator),
			Weight:    validator.Weight,
		}
	}

	// Session renames the protocol fields, so this projection cannot be a type
	// conversion the way the supervisor's is; reading them off SimplexProtocol
	// still keeps one definition of which fields the descriptor carries.
	protocol := consensus.Protocol()
	session := Session{
		ID:                   group.ID,
		Shard:                group.Shard,
		CatchainSeqno:        group.CatchainSeqno,
		ValidatorSetHash:     group.ValidatorSetHash,
		ConsensusVersion:     protocol.Version,
		ConsensusFlags:       protocol.Flags,
		ProtocolVersion:      protocol.ProtocolVersion,
		UseQUIC:              protocol.UseQUIC,
		SlotsPerLeaderWindow: protocol.SlotsPerLeaderWindow,
		Validators:           validators,
	}

	var activation *SessionActivation
	if active {
		value := SessionActivation{
			SessionID:      group.ID,
			Genesis:        cloneBlockIDs(group.Genesis),
			MinMasterchain: cloneBlockID(group.MinMasterchain),
		}
		activation = &value
	}

	params := consensus.SimplexParams()
	update := SessionUpdate{
		SessionID:                 group.ID,
		TargetRate:                params.TargetRate,
		NoEmptyBlocksOnErrTimeout: params.NoEmptyBlocksOnErrTimeout,
		MasterchainBlock:          cloneBlockID(snapshot.MasterchainBlock),
		Registered:                append([]groups.ShardDescription(nil), group.Registered...),
	}
	for i := range update.Registered {
		update.Registered[i].Block = cloneBlockID(update.Registered[i].Block)
	}
	if group.FinalizedBlock != nil {
		update.HasFinalizedBlock = true
		update.FinalizedBlock = cloneBlockID(*group.FinalizedBlock)
	}

	overlay := OverlaySession{
		Session:                   session,
		Role:                      role,
		CollatorsByValidator:      registry,
		AllOverlayNodes:           allOverlayNodes,
		MaxBlockSize:              snapshot.Config.MaxBlockSize,
		MaxCollatedDataSize:       snapshot.Config.MaxCollatedDataSize,
		BroadcastMode:             CandidateBroadcastPrivateOverlay,
		ObserversInPrivateOverlay: true,
	}

	return projectedCollatorSession{
		session:    session,
		activation: activation,
		update:     update,
		overlay:    overlay,
		params:     params,
	}
}

func projectedCollatorRole(
	snapshot *groups.Snapshot,
	group groups.Session,
	policy sessionProjectionPolicy,
) (OverlayRole, []groups.CollatorRegistryEntry) {
	if group.Shard.IsMasterchain() {
		return OverlayRoleObserver, nil
	}

	rosterKeys := make(map[[32]byte]struct{}, len(group.Validators))
	for i := range group.Validators {
		rosterKeys[group.Validators[i].PublicKeyHash] = struct{}{}
	}

	registry := make([]groups.CollatorRegistryEntry, 0, len(group.Validators))
	role := OverlayRoleObserver
	for i := range snapshot.CollatorsByValidator {
		entry := snapshot.CollatorsByValidator[i]
		if _, inRoster := rosterKeys[entry.ValidatorKeyID]; !inRoster {
			continue
		}
		registry = append(registry, entry)
		if slices.Contains(entry.CollatorADNLIDs, policy.localADNLID) {
			role = OverlayRoleCollator
		}
	}

	return role, registry
}

func (s projectedCollatorSession) observerSession() ConsensusObserverSession {
	return ConsensusObserverSession{
		Overlay: s.overlay,
		Update:  s.update,
		Params:  s.params,
	}
}

// Equal reports whether two overlay projections describe the same session.
func (o OverlaySession) Equal(other OverlaySession) bool {
	left, right := o, other

	return left.Session.Equal(right.Session) && left.Role == right.Role &&
		slices.EqualFunc(left.CollatorsByValidator, right.CollatorsByValidator, sameCollatorRegistryEntry) &&
		slices.Equal(left.AllOverlayNodes, right.AllOverlayNodes) &&
		left.MaxBlockSize == right.MaxBlockSize &&
		left.MaxCollatedDataSize == right.MaxCollatedDataSize &&
		left.BroadcastMode == right.BroadcastMode &&
		left.ObserversInPrivateOverlay == right.ObserversInPrivateOverlay
}

func sameCollatorRegistryEntry(left, right groups.CollatorRegistryEntry) bool {
	return left.ValidatorKeyID == right.ValidatorKeyID && slices.Equal(left.CollatorADNLIDs, right.CollatorADNLIDs)
}
