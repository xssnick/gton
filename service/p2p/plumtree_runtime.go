package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/tl"
)

const plumtreeOutboundTimeout = 5 * time.Second

type plumtreeRuntime struct {
	sub          *overlaySubscription
	policySource plumtreePolicySource
	engine       *plumtreeEngine
	origin       plumtreeLocalOrigin
	stats        *plumtreeStats
	wake         chan struct{}
	outbound     chan plumtreeWireBatch

	statsTimingMu          sync.Mutex
	statsTimingInitialized bool
	statsTimingEpoch       int64
	statsJitter            plumtreeStatsJitter
}

type plumtreeMessageResult struct {
	actions      plumtreeActions
	statsForward plumtreeStatsForward
}

func newPlumtreeRuntime(sub *overlaySubscription) (*plumtreeRuntime, error) {
	overlayID, err := NewPeerID(sub.spec.ShortID)
	if err != nil {
		return nil, fmt.Errorf("parse overlay id: %w", err)
	}

	policySource := plumtreePolicySource(
		nodePlumtreePolicySource{node: sub.node},
	)
	if sub.fastSync != nil {
		policySource = &fixedPlumtreePolicySource{
			policy: NewPlumtreePolicy(sub.spec.FixedNodes),
		}
	}

	stats := &plumtreeStats{}
	engine, err := newPlumtreeEngine(
		plumtreeEngineConfig{
			LocalID: sub.node.localID,
			IsOriginalSender: sub.fastSync != nil &&
				sub.fastSync.spec.localValidator,
			Now: time.Now,
		},
		sub.node.plumtreeBudget,
		sub,
		newPlumtreeSignatureVerifier(
			policySource,
			overlayID,
		),
		stats,
	)
	if err != nil {
		return nil, err
	}
	origin, err := newPlumtreeLocalOrigin(
		sub.node.privKey,
		sub.quicEnvelope,
	)
	if err != nil {
		return nil, err
	}

	return &plumtreeRuntime{
		sub:          sub,
		policySource: policySource,
		engine:       engine,
		origin:       origin,
		stats:        stats,
		wake:         make(chan struct{}, 1),
		outbound:     make(chan plumtreeWireBatch, plumtreeOutboundBatchLimit),
	}, nil
}

func (r *plumtreeRuntime) HandleMessage(
	ctx context.Context,
	from PeerID,
	wire []byte,
	body []byte,
) error {
	if !r.sub.isActive() {
		return errOverlayInactive
	}

	result, err := r.processMessage(ctx, from, wire, body)
	r.notifyAlarmChanged()
	r.sendStatsForward(result.statsForward)
	deliveryErr := r.applyActions(result.actions)
	if err != nil {
		return err
	}
	return deliveryErr
}

func (r *plumtreeRuntime) processMessage(
	ctx context.Context,
	from PeerID,
	wire []byte,
	body []byte,
) (plumtreeMessageResult, error) {
	if len(body) < 4 {
		return plumtreeMessageResult{}, fmt.Errorf("Plumtree broadcast is too short")
	}

	now := time.Now()
	var result plumtreeMessageResult
	var err error
	switch binary.LittleEndian.Uint32(body[:4]) {
	case broadcastPlumtreeSimpleConstructorID:
		var message BroadcastPlumtreeSimple
		if err = plumtreeParsed(message.ParseNoCopy(body[4:])); err != nil {
			return plumtreeMessageResult{}, err
		}
		r.stats.telemetry.noteMessageReceived(plumtreeMessageSimple)
		result.actions, err = r.engine.HandleSimple(ctx, now, from, plumtreeSimpleEnvelope{
			Message: message,
			Wire: plumtreePayloadWire{
				Message: wire,
				Body:    body,
			},
		})
	case broadcastPlumtreeFECConstructorID:
		var message BroadcastPlumtreeFEC
		if err = plumtreeParsed(message.ParseNoCopy(body[4:])); err != nil {
			return plumtreeMessageResult{}, err
		}
		r.stats.telemetry.noteMessageReceived(plumtreeMessageFEC)
		result.actions, err = r.engine.HandleFEC(ctx, now, from, plumtreeFECEnvelope{
			Message: message,
			Wire: plumtreePayloadWire{
				Message: wire,
				Body:    body,
			},
		})
	case broadcastPlumtreeIHaveConstructorID:
		var message BroadcastPlumtreeIHave
		if err = plumtreeParsed(message.ParseNoCopy(body[4:])); err != nil {
			return plumtreeMessageResult{}, err
		}
		r.stats.telemetry.noteMessageReceived(plumtreeMessageIHave)
		result.actions, err = r.engine.HandleIHave(ctx, now, from, message)
	case broadcastPlumtreePruneConstructorID:
		var message BroadcastPlumtreePrune
		if err = plumtreeParsed(message.ParseNoCopy(body[4:])); err != nil {
			return plumtreeMessageResult{}, err
		}
		r.stats.telemetry.noteMessageReceived(plumtreeMessagePrune)
		err = r.engine.HandlePrune(now, from, message)
	case broadcastPlumtreeUsefulConstructorID:
		var message BroadcastPlumtreeUseful
		if err = plumtreeParsed(message.ParseNoCopy(body[4:])); err != nil {
			return plumtreeMessageResult{}, err
		}
		r.stats.telemetry.noteMessageReceived(plumtreeMessageUseful)
		err = r.engine.HandleUseful(now, from, message)
	case plumtreeStatsPushConstructorID:
		var message PlumtreeStatsPush
		if err = parsePlumtreeMessage(&message, body); err != nil {
			return plumtreeMessageResult{}, err
		}
		r.stats.telemetry.noteMessageReceived(plumtreeMessageStatsPush)
		eager := r.engine.statsEagerPeers(plumtreeSimpleTree)
		var pushResult plumtreeStatsPushResult
		pushResult, err = r.stats.HandlePush(
			now,
			r.statsJitterAt(now),
			r.sub.node.localID,
			from,
			&eager,
			message,
		)
		if err == nil {
			return plumtreeMessageResult{
				statsForward: pushResult.forward,
			}, nil
		}
	default:
		err = fmt.Errorf("unexpected Plumtree broadcast constructor")
	}
	return result, err
}

func (r *plumtreeRuntime) OriginateSimple(
	flags int32,
	broadcastID [sha256.Size]byte,
	data []byte,
) error {
	if !r.sub.isActive() {
		return errOverlayInactive
	}

	actions, err := r.engine.OriginateSimple(
		r.origin,
		flags,
		broadcastID,
		data,
	)
	r.notifyAlarmChanged()
	if err != nil {
		return err
	}
	return r.applyActions(actions)
}

func (r *plumtreeRuntime) OriginateFEC(
	flags int32,
	data []byte,
) error {
	if !r.sub.isActive() {
		return errOverlayInactive
	}

	actions, err := r.engine.OriginateFEC(r.origin, flags, data)
	r.notifyAlarmChanged()
	if err != nil {
		return err
	}
	return r.applyActions(actions)
}

func (r *plumtreeRuntime) HandleRepairQuery(
	_ context.Context,
	from PeerID,
	request RepairPlumtreePart,
) ([]byte, error) {
	if !r.sub.isActive() {
		return nil, errOverlayInactive
	}
	r.stats.telemetry.noteMessageReceived(plumtreeMessageRepairQuery)
	return r.engine.HandleRepairQuery(time.Now(), from, request)
}

func (r *plumtreeRuntime) applyActions(
	actions plumtreeActions,
) error {
	r.enqueueOutbounds(r.prepareOutboundBatch(actions.Outbounds))
	r.startVerifiedRepairs(actions.Candidates)
	for _, delivery := range actions.Deliveries {
		if err := r.deliver(delivery); err != nil {
			return err
		}
	}
	return nil
}

func plumtreeOutboundMessage(action *plumtreeOutboundAction) tl.Serializable {
	control := action.Control
	key := control.key
	switch action.Kind {
	case plumtreeOutboundIHave:
		part := control.part
		return BroadcastPlumtreeIHave{
			BroadcastID:      key.broadcastID[:],
			Timestamp:        control.timestamp,
			PartIndex:        key.partIndex,
			TreeIndex:        key.treeIndex,
			Source:           part.sourceKey,
			Certificate:      part.certificate.protocolValue(),
			PayloadTimestamp: part.timestamp,
			DataSize:         part.dataSize,
			DataHash:         part.dataHash[:],
			Signature:        part.signature,
		}
	case plumtreeOutboundPrune:
		return BroadcastPlumtreePrune{
			BroadcastID: key.broadcastID[:],
			Timestamp:   control.timestamp,
			PartIndex:   key.partIndex,
			TreeIndex:   key.treeIndex,
		}
	case plumtreeOutboundUseful:
		return BroadcastPlumtreeUseful{
			BroadcastID: key.broadcastID[:],
			Timestamp:   control.timestamp,
			PartIndex:   key.partIndex,
			TreeIndex:   key.treeIndex,
		}
	default:
		panic("invalid Plumtree outbound control kind")
	}
}

func (r *plumtreeRuntime) deliver(delivery plumtreeDelivery) error {
	message, err := parseOneQUICOverlayObject(delivery.Data)
	if err != nil {
		return fmt.Errorf("parse Plumtree payload: %w", err)
	}

	r.sub.handleOverlayBroadcastPayload(
		nil,
		message,
		newKnownIdentifiedBroadcastPayload(delivery.Data, nil),
		DeliveryPlumtree,
		true,
		delivery.Source,
	)
	return nil
}

// Checks the signature a buffered announcer deferred, then asks the survivors for
// the part. Serial and on the caller's goroutine: candidates for one part carry
// the same source signature over the same preimage, so only the first pays key
// math and the rest hit the verifier's tuple cache.
//
// A candidate that fails is dropped and its peer banned for 5s. Nothing reaches
// the network before the signature holds.
func (r *plumtreeRuntime) startVerifiedRepairs(candidates []plumtreeRepairCandidate) {
	for _, candidate := range candidates {
		now := time.Now()
		if err := validatePlumtreeTimestamp(candidate.PayloadTimestamp, now); err != nil {
			continue
		}

		plumtreeIHaveVerifiableTotal.Add(1)
		if _, err := r.engine.verifier.VerifyPlumtree(
			r.sub.node.runCtx,
			plumtreeVerification{
				From:        candidate.Peer,
				Source:      candidate.Source,
				Certificate: candidate.Certificate,
				DataSize:    uint32(candidate.DataSize),
				SignedData:  candidate.CandidateSignedData(),
				Signature:   candidate.Signature,
			},
		); err != nil {
			if errors.Is(err, errPlumtreeSignatureInvalid) {
				r.engine.DropPeerAnnouncements(candidate.Peer)
			}
			continue
		}

		if action, ok := r.engine.StartVerifiedRepair(now, candidate); ok {
			r.startRepair(action)
		}
	}
}

func (r *plumtreeRuntime) startRepair(action plumtreeRepairAction) {
	r.sub.node.runAsync(func() {
		if err := r.runRepair(action); err != nil {
			r.sub.log.Debug().
				Err(err).
				Str("peer_id", action.To.String()).
				Msg("Plumtree repair failed")
		}
	})
}

func (r *plumtreeRuntime) runRepair(action plumtreeRepairAction) error {
	// The successful path consumes the attempt inside HandleRepair*; every other
	// exit releases the slot here, as a harmless unknown-attempt no-op.
	defer func() {
		_ = r.engine.FinishRepair(action.ID)
	}()

	path, err := r.sub.quicPeerPath(action.To)
	if err != nil {
		return err
	}
	if !path.route.quicReady(time.Now()) {
		return nil
	}

	ctx, cancel := context.WithTimeout(r.sub.node.runCtx, action.Timeout)
	defer cancel()

	peer, err := path.dialGated(ctx)
	if err != nil {
		if errors.Is(err, errQUICDialDeferred) {
			return nil
		}
		return err
	}
	payload, err := r.sub.quicEnvelope.Query(action.Request)
	if err != nil {
		return fmt.Errorf("serialize Plumtree repair query: %w", err)
	}
	answer, err := peer.QueryOutbound(ctx, payload, int64(action.MaxAnswerSize))
	if err != nil {
		return fmt.Errorf("query Plumtree repair: %w", err)
	}

	actions, err := r.processRepairAnswer(ctx, action, answer)
	r.notifyAlarmChanged()
	deliveryErr := r.applyActions(actions)
	if err != nil {
		return err
	}
	return deliveryErr
}

func (r *plumtreeRuntime) processRepairAnswer(
	ctx context.Context,
	action plumtreeRepairAction,
	answer []byte,
) (plumtreeActions, error) {
	if len(answer) < 4 {
		return plumtreeActions{}, fmt.Errorf("Plumtree repair answer is too short")
	}

	now := time.Now()
	switch binary.LittleEndian.Uint32(answer[:4]) {
	case broadcastNotFoundConstructorID:
		var message BroadcastNotFound
		if err := parsePlumtreeMessage(&message, answer); err != nil {
			return plumtreeActions{}, err
		}
		return plumtreeActions{}, nil
	case broadcastPlumtreeSimpleConstructorID:
		wire, body := r.sub.quicEnvelope.WrapMessageBody(answer)
		var message BroadcastPlumtreeSimple
		if err := parsePlumtreeMessage(&message, body); err != nil {
			return plumtreeActions{}, err
		}
		return r.engine.HandleRepairSimple(
			ctx,
			now,
			action.ID,
			plumtreeSimpleEnvelope{
				Message: message,
				Wire: plumtreePayloadWire{
					Message: wire,
					Body:    body,
				},
			},
		)
	case broadcastPlumtreeFECConstructorID:
		wire, body := r.sub.quicEnvelope.WrapMessageBody(answer)
		var message BroadcastPlumtreeFEC
		if err := parsePlumtreeMessage(&message, body); err != nil {
			return plumtreeActions{}, err
		}
		return r.engine.HandleRepairFEC(
			ctx,
			now,
			action.ID,
			plumtreeFECEnvelope{
				Message: message,
				Wire: plumtreePayloadWire{
					Message: wire,
					Body:    body,
				},
			},
		)
	default:
		return plumtreeActions{}, fmt.Errorf(
			"unexpected Plumtree repair answer constructor",
		)
	}
}

func (r *plumtreeRuntime) notifyAlarmChanged() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *plumtreeRuntime) run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	queue := newPlumtreeOutboundQueue()
	jobs := make(chan plumtreeOutboundJob)
	done := make(chan PeerID, plumtreeOutboundParallelism)
	var workers sync.WaitGroup
	workerCount := 0
	startWorker := func() {
		workerCount++
		workers.Add(1)
		go func() {
			defer workers.Done()
			r.runOutboundWorker(ctx, jobs, done)
		}()
	}
	defer func() {
		close(jobs)
		workers.Wait()
	}()

	activeSends := 0
	dropLogEnabled := r.sub.log.Debug().Enabled()
	droppedSinceLog := 0
	var nextDropLogAt time.Time

	for {
		for activeSends < plumtreeOutboundParallelism {
			peer, sends, ok := queue.take()
			if !ok {
				break
			}
			if activeSends == workerCount {
				startWorker()
			}
			job := plumtreeOutboundJob{
				peer:  peer,
				wires: sends,
			}
			select {
			case jobs <- job:
				activeSends++
			case <-ctx.Done():
				return
			}
		}

		now := time.Now()
		next, scheduled := r.nextAlarm(now)
		var alarm <-chan time.Time
		if scheduled {
			delay := time.Until(next)
			if delay < 0 {
				delay = 0
			}
			timer.Reset(delay)
			alarm = timer.C
		}

		select {
		case <-ctx.Done():
			return
		case <-r.wake:
			stopPlumtreeTimer(timer, scheduled)
		case batch := <-r.outbound:
			stopPlumtreeTimer(timer, scheduled)
			dropped := queue.add(batch)
			if dropped > 0 && dropLogEnabled {
				droppedSinceLog += dropped
				now := time.Now()
				if !now.Before(nextDropLogAt) {
					r.sub.log.Debug().
						Int("messages", droppedSinceLog).
						Msg("dropping Plumtree messages queued behind a stalled peer")
					droppedSinceLog = 0
					nextDropLogAt = now.Add(plumtreeOutboundDropLogInterval)
				}
			}
		case peer := <-done:
			stopPlumtreeTimer(timer, scheduled)
			queue.finish(peer)
			activeSends--
		case <-alarm:
			handledAt := time.Now()
			r.tickStats(ctx, handledAt)
			actions := r.engine.Alarm(handledAt)
			_ = r.applyActions(actions)
		}
	}
}

func (r *plumtreeRuntime) nextAlarm(now time.Time) (time.Time, bool) {
	next, scheduled := r.engine.NextAlarm()
	statsAt := r.stats.NextAt(now, r.statsJitterAt(now))
	if !scheduled || statsAt.Before(next) {
		return statsAt, true
	}
	return next, true
}

func (r *plumtreeRuntime) tickStats(ctx context.Context, now time.Time) {
	jitter := r.statsJitterAt(now)
	schedule := r.stats.Schedule(now, jitter)
	eager := r.engine.statsEagerSnapshot(schedule.rotatingTree)
	result := r.stats.Tick(
		now,
		jitter,
		r.sub.node.localID,
		&eager,
	)
	r.sendStatsForward(result.forward)
}

func (r *plumtreeRuntime) sendStatsForward(
	forward plumtreeStatsForward,
) {
	if !forward.ready || forward.count == 0 {
		return
	}

	payload, err := r.sub.quicEnvelope.Message(
		PlumtreeStatsPush{Record: forward.record},
	)
	if err != nil {
		r.sub.log.Error().
			Err(err).
			Msg("failed to serialize Plumtree stats")
		return
	}

	batch := plumtreeWireBatch{
		sends: make([]plumtreeWireSend, 0, forward.count),
	}
	for i := range int(forward.count) {
		batch.sends = append(batch.sends, plumtreeWireSend{
			to:   forward.to[i],
			wire: payload,
		})
	}
	r.enqueueOutbounds(batch)
}

func (r *plumtreeRuntime) statsJitterAt(now time.Time) plumtreeStatsJitter {
	epoch := now.Unix() / int64(plumtreeStatsEpochDuration/time.Second)

	r.statsTimingMu.Lock()
	defer r.statsTimingMu.Unlock()

	if r.statsTimingInitialized && epoch <= r.statsTimingEpoch {
		return r.statsJitter
	}
	if !r.statsTimingInitialized || r.statsTimingEpoch < epoch {
		r.statsTimingInitialized = true
		r.statsTimingEpoch = epoch
		step := rand.Uint32N(plumtreeStatsMaximumJitterStep + 1)
		r.statsJitter.delay = time.Duration(step) *
			plumtreeStatsJitterStepDuration
	}
	return r.statsJitter
}

func stopPlumtreeTimer(timer *time.Timer, running bool) {
	if !running || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func plumtreeBroadcastConstructor(body []byte) (uint32, bool) {
	if len(body) < 4 {
		return 0, false
	}

	constructor := binary.LittleEndian.Uint32(body)
	switch constructor {
	case broadcastPlumtreeSimpleConstructorID,
		broadcastPlumtreeFECConstructorID,
		broadcastPlumtreeIHaveConstructorID,
		broadcastPlumtreePruneConstructorID,
		broadcastPlumtreeUsefulConstructorID,
		plumtreeStatsPushConstructorID:
		return constructor, true
	default:
		return 0, false
	}
}

func (s *overlaySubscription) PlumtreeActivePeers(limit int) []PeerID {
	peers := s.broadcastTargetsSnapshot().plumtree
	if limit > 0 && len(peers) > limit {
		return peers[:limit]
	}
	return peers
}

// The peers plumtree may push to right now. Their QUIC paths must survive the
// idle sweep: closing the path of a mesh member is how this node fell out of
// other nodes' broadcast trees before.
func (s *overlaySubscription) plumtreePinnedPeers() []PeerID {
	if s.plumtree == nil {
		return nil
	}
	return s.PlumtreeActivePeers(plumtreeActiveNeighbourLimit)
}

func (s *overlaySubscription) PlumtreePeerReceivesBroadcasts(peer PeerID) bool {
	s.mx.Lock()
	_, rosterPeer := s.peers[peer]
	receives := !s.removed && !s.inactive && rosterPeer
	s.mx.Unlock()
	if receives && s.fastSync != nil {
		return s.fastSync.peerReceivesBroadcasts(peer)
	}
	return receives
}

func (s *overlaySubscription) PlumtreePeersReceiveBroadcasts(
	peers []PeerID,
	out []bool,
) {
	s.mx.Lock()
	alive := !s.removed && !s.inactive
	for i, peer := range peers {
		_, rosterPeer := s.peers[peer]
		out[i] = alive && rosterPeer
	}
	s.mx.Unlock()

	if s.fastSync == nil {
		return
	}
	for i, peer := range peers {
		if out[i] {
			out[i] = s.fastSync.peerReceivesBroadcasts(peer)
		}
	}
}

func (n *Node) removePlumtreePeer(peer PeerID) {
	for _, entry := range n.subscriptionEntriesSnapshot() {
		if entry.sub.plumtree == nil {
			continue
		}
		entry.sub.plumtree.engine.RemovePeer(peer)
		entry.sub.plumtree.notifyAlarmChanged()
	}
}
