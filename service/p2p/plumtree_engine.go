package p2p

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/xssnick/raptorq"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	plumtreeDeliveredLimit       = 1000
	plumtreeMissingLimit         = 1024
	plumtreeActiveRepairLimit    = 512
	plumtreeRepairTargetLimit    = 5
	plumtreeActiveNeighbourLimit = 20
	plumtreeIHaveFanout          = 8

	plumtreeOriginalEagerLimit = 1
	plumtreeRegularEagerLimit  = 4

	plumtreePendingFeedbackTTL = 5 * time.Second
	// How long a silent eager neighbour keeps its slot. Well under
	// plumtreeBroadcastLifetime: the slot suppresses repairs for that part, so
	// waiting too long means losing the broadcast rather than repairing it.
	plumtreeEagerInactivityTTL = 10 * time.Second
	plumtreeRepairDelay        = 200 * time.Millisecond
	plumtreeRepairTimeout      = 5 * time.Second
	plumtreeRepairMTUOverhead  = 4096

	plumtreeMaxSentWithoutActivity = 50
)

// plumtreePeerSet is the required read-only view of QUIC broadcast routes.
type plumtreePeerSet interface {
	PlumtreeActivePeers(limit int) []PeerID
	PlumtreePeerReceivesBroadcasts(peer PeerID) bool
	// PlumtreePeersReceiveBroadcasts answers PlumtreePeerReceivesBroadcasts for
	// every candidate with one roster-lock acquisition, writing into out
	// (len(out) == len(peers)).
	PlumtreePeersReceiveBroadcasts(peers []PeerID, out []bool)
}

// One batched roster check, so a forward decision asks the peer set once instead
// of once per candidate. Built under e.mu from the same eager slots it answers
// for, so every looked-up peer is present by construction.
type plumtreeReceiveVerdicts struct {
	peers    []PeerID
	receives []bool
}

func (v *plumtreeReceiveVerdicts) receivesBroadcasts(peer PeerID) bool {
	for i, candidate := range v.peers {
		if candidate == peer {
			return v.receives[i]
		}
	}
	return false
}

func (e *plumtreeEngine) receiveVerdicts(peers []PeerID) plumtreeReceiveVerdicts {
	verdicts := plumtreeReceiveVerdicts{peers: peers}
	if len(peers) == 0 {
		return verdicts
	}
	verdicts.receives = make([]bool, len(peers))
	e.peers.PlumtreePeersReceiveBroadcasts(peers, verdicts.receives)
	return verdicts
}

func (e *plumtreeEngine) slotReceiveVerdictsLocked(
	slot *plumtreeSlot,
) plumtreeReceiveVerdicts {
	return e.receiveVerdicts(slices.Clone(slot.eager[:slot.eagerCount]))
}

// The unique eager peers across every FEC slot, so OriginateFEC resolves the
// roster once for all 45 parts.
func (e *plumtreeEngine) fecEagerReceiveVerdictsLocked() plumtreeReceiveVerdicts {
	var unique []PeerID
	for treeIndex := plumtreeFECTreeOffset; treeIndex < plumtreeTreeSlots; treeIndex++ {
		slot := &e.slots[treeIndex]
		for i := range int(slot.eagerCount) {
			if !slices.Contains(unique, slot.eager[i]) {
				unique = append(unique, slot.eager[i])
			}
		}
	}
	return e.receiveVerdicts(unique)
}

type plumtreeVerifier interface {
	VerifyPlumtree(ctx context.Context, request plumtreeVerification) (PeerID, error)
}

type plumtreeVerification struct {
	From        PeerID
	Source      keys.PublicKeyED25519
	Certificate plumtreeCertificate
	DataSize    uint32
	SignedData  []byte
	Signature   []byte
}

type plumtreeCertificateKind uint8

const (
	plumtreeCertificateUnknown plumtreeCertificateKind = iota
	plumtreeCertificateEmpty
	plumtreeCertificateLegacy
	plumtreeCertificateV2
)

type plumtreeCertificate struct {
	kind   plumtreeCertificateKind
	legacy overlay.Certificate
	v2     overlay.CertificateV2
}

func decodePlumtreeIdentity(
	sourceValue any,
	certificateValue any,
) (keys.PublicKeyED25519, plumtreeCertificate, error) {
	source, ok := sourceValue.(keys.PublicKeyED25519)
	if !ok {
		return keys.PublicKeyED25519{}, plumtreeCertificate{}, fmt.Errorf(
			"unsupported Plumtree source type %T",
			sourceValue,
		)
	}

	var certificate plumtreeCertificate
	switch value := certificateValue.(type) {
	case overlay.CertificateEmpty:
		certificate.kind = plumtreeCertificateEmpty
	case overlay.Certificate:
		certificate.kind = plumtreeCertificateLegacy
		certificate.legacy = value
	case overlay.CertificateV2:
		certificate.kind = plumtreeCertificateV2
		certificate.v2 = value
	default:
		return keys.PublicKeyED25519{}, plumtreeCertificate{}, fmt.Errorf(
			"unsupported Plumtree certificate type %T",
			certificateValue,
		)
	}

	return source, certificate, nil
}

func (c plumtreeCertificate) protocolValue() any {
	switch c.kind {
	case plumtreeCertificateEmpty:
		return overlay.CertificateEmpty{}
	case plumtreeCertificateLegacy:
		return c.legacy
	case plumtreeCertificateV2:
		return c.v2
	default:
		panic("invalid Plumtree certificate")
	}
}

// Ownership of both slices moves to the engine. Message is the one retained
// protocol buffer; Body and decoded byte fields backed by it are immutable
// views. Wire accounting therefore counts len(Message) exactly once.
type plumtreePayloadWire struct {
	Message []byte
	Body    []byte
}

type plumtreeSimpleEnvelope struct {
	Message BroadcastPlumtreeSimple
	Wire    plumtreePayloadWire
}

type plumtreeFECEnvelope struct {
	Message BroadcastPlumtreeFEC
	Wire    plumtreePayloadWire
}

type plumtreeOutboundKind uint8

const (
	plumtreeOutboundPayload plumtreeOutboundKind = iota + 1
	plumtreeOutboundIHave
	plumtreeOutboundPrune
	plumtreeOutboundUseful
)

type plumtreeOutboundControl struct {
	key       plumtreePartKey
	timestamp float64
	// IHAVE references protocol metadata that is immutable after verification.
	part *plumtreePartState
}

type plumtreeOutboundAction struct {
	Kind    plumtreeOutboundKind
	To      PeerID
	Wire    []byte
	Control plumtreeOutboundControl
}

type plumtreeRepairAction struct {
	ID            uint64
	To            PeerID
	Request       RepairPlumtreePart
	Timeout       time.Duration
	MaxAnswerSize uint64
}

type plumtreeDelivery struct {
	Source PeerID
	Data   []byte
}

type plumtreeActions struct {
	Outbounds  []plumtreeOutboundAction
	Repairs    []plumtreeRepairAction
	Deliveries []plumtreeDelivery
	// Candidates are buffered announcers whose signature still has to be
	// verified before they can be asked for a part. The caller verifies them off
	// the engine lock and feeds the survivors back through StartVerifiedRepair.
	Candidates []plumtreeRepairCandidate
}

type plumtreeEngineConfig struct {
	LocalID          PeerID
	IsOriginalSender bool
	Now              func() time.Time
}

type plumtreePartKey struct {
	broadcastID [sha256.Size]byte
	partIndex   int32
	treeIndex   int32
}

type plumtreePayloadContext struct {
	from        PeerID
	expectedKey plumtreePartKey
}

type plumtreePendingPeer struct {
	id   PeerID
	sent time.Time
}

type plumtreeSlot struct {
	eager        [plumtreeRegularEagerLimit]PeerID
	eagerCount   uint8
	pending      [plumtreeRegularEagerLimit]plumtreePendingPeer
	pendingCount uint8
}

type plumtreePeerActivity struct {
	references      uint16
	lastActive      time.Time
	sentSinceActive uint32
}

type plumtreePartState struct {
	partIndex   int32
	treeIndex   int32
	timestamp   float64
	dataSize    int32
	dataHash    [sha256.Size]byte
	sourceKey   keys.PublicKeyED25519
	certificate plumtreeCertificate
	signature   []byte
	wire        plumtreePayloadWire

	fullSentTo    [plumtreeRegularEagerLimit]PeerID
	fullSentCount uint8
	// Each part is forwarded once against one active-neighbour snapshot.
	advertisedTo    [plumtreeIHaveFanout]PeerID
	advertisedCount uint8
}

type plumtreeFECState struct {
	broadcastID           [sha256.Size]byte
	firstSource           PeerID
	fullDataHash          [sha256.Size]byte
	firstSeen             time.Time
	flags                 int32
	fullDataSize          int32
	partSize              int32
	isDelivered           bool
	hasDecodeFailed       bool
	partCount             uint8
	decoderPartCount      uint8
	retainedWireBytes     uint64
	decoderEstimatedBytes uint64

	// decodeMu serialises RaptorQ work for this broadcast outside the engine lock.
	// Lock order is decodeMu -> mu. decoder, decodeBuffer and decoderPartCount
	// belong to decodeMu; clearing decoder/decodeBuffer and setting decoderReleased
	// additionally require mu. The decoder is built lazily on the first symbol, so
	// "completed or failed" is decoderReleased, not a nil decoder.
	decodeMu        sync.Mutex
	decoder         *raptorq.Decoder
	decoderReleased bool
	decodeBuffer    []byte
	parts           [plumtreeFECTotalParts]*plumtreePartState

	// Per part, measured from that part's earliest announcement; zero means nothing
	// pending. A single per-broadcast minimum would repair a late-announced part
	// early and prune the healthy eager peer still pushing it.
	repairAt [plumtreeFECTotalParts]time.Time

	previous *plumtreeFECState
	next     *plumtreeFECState
}

type plumtreeSimpleState struct {
	broadcastID       [sha256.Size]byte
	source            PeerID
	data              []byte
	firstSeen         time.Time
	part              plumtreePartState
	retainedWireBytes uint64

	previous *plumtreeSimpleState
	next     *plumtreeSimpleState
}

type plumtreeMissingPart struct {
	key         plumtreePartKey
	dataSize    int32
	targets     [plumtreeRepairTargetLimit]PeerID
	targetCount uint8
	sentTargets uint8
	repairAt    time.Time

	previous *plumtreeMissingPart
	next     *plumtreeMissingPart
}

type plumtreeRepairAttempt struct {
	from PeerID
	key  plumtreePartKey
}

type plumtreeEngine struct {
	mu sync.Mutex

	budget           *plumtreeMemoryBudget
	localID          PeerID
	isOriginalSender bool
	localEagerLimit  int
	now              func() time.Time
	peers            plumtreePeerSet
	verifier         plumtreeVerifier
	stats            *plumtreeStats

	slots      [plumtreeTreeSlots]plumtreeSlot
	activities map[PeerID]plumtreePeerActivity

	fecStates    map[[sha256.Size]byte]*plumtreeFECState
	fecOldest    *plumtreeFECState
	fecNewest    *plumtreeFECState
	simpleStates map[[sha256.Size]byte]*plumtreeSimpleState
	simpleOldest *plumtreeSimpleState
	simpleNewest *plumtreeSimpleState

	missing       map[plumtreePartKey]*plumtreeMissingPart
	missingOldest *plumtreeMissingPart
	missingNewest *plumtreeMissingPart

	// Unverified IHAVEs, bucketed per announcing peer. See
	// plumtree_announcements.go.
	announcements map[PeerID]*plumtreePeerAnnouncements
	// Earliest repairAt and earliest retention deadline, cached so NextAlarm stays
	// an O(1) read instead of a scan on every run-loop turn.
	fecRepairNext     time.Time
	stateExpireNext   time.Time
	pendingExpireNext time.Time

	repairs      map[uint64]plumtreeRepairAttempt
	nextRepairID uint64

	delivered    plumtreeRingSet[[sha256.Size]byte]
	notFoundBody []byte
	closed       bool
}

// newPlumtreeEngine requires the node-owned budget shared by all its engines.
func newPlumtreeEngine(
	config plumtreeEngineConfig,
	budget *plumtreeMemoryBudget,
	peers plumtreePeerSet,
	verifier plumtreeVerifier,
	stats *plumtreeStats,
) (*plumtreeEngine, error) {
	notFoundBody, err := tl.Serialize(BroadcastNotFound{}, true)
	if err != nil {
		return nil, fmt.Errorf("serialize Plumtree not-found answer: %w", err)
	}

	localEagerLimit := plumtreeRegularEagerLimit
	if config.IsOriginalSender {
		localEagerLimit = plumtreeOriginalEagerLimit
	}

	return &plumtreeEngine{
		budget:           budget,
		localID:          config.LocalID,
		isOriginalSender: config.IsOriginalSender,
		localEagerLimit:  localEagerLimit,
		now:              config.Now,
		peers:            peers,
		verifier:         verifier,
		stats:            stats,
		activities:       make(map[PeerID]plumtreePeerActivity),
		fecStates:        make(map[[sha256.Size]byte]*plumtreeFECState),
		simpleStates:     make(map[[sha256.Size]byte]*plumtreeSimpleState),
		missing:          make(map[plumtreePartKey]*plumtreeMissingPart),
		announcements:    make(map[PeerID]*plumtreePeerAnnouncements),
		repairs:          make(map[uint64]plumtreeRepairAttempt),
		delivered:        newPlumtreeRingSet[[sha256.Size]byte](plumtreeDeliveredLimit),
		notFoundBody:     notFoundBody,
	}, nil
}

func (e *plumtreeEngine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return
	}
	e.closed = true

	for e.fecOldest != nil {
		e.removeFECStateLocked(e.fecOldest)
	}
	for e.simpleOldest != nil {
		e.removeSimpleStateLocked(e.simpleOldest)
	}

	e.slots = [plumtreeTreeSlots]plumtreeSlot{}
	e.activities = nil
	e.fecStates = nil
	e.simpleStates = nil
	e.missing = nil
	e.missingOldest = nil
	e.missingNewest = nil
	e.announcements = nil
	e.repairs = nil
	e.delivered = plumtreeRingSet[[sha256.Size]byte]{}
	e.notFoundBody = nil
}

func (s *plumtreeSlot) hasEager(peer PeerID) bool {
	for i := range int(s.eagerCount) {
		if s.eager[i] == peer {
			return true
		}
	}
	return false
}

func (s *plumtreeSlot) eagerIndex(peer PeerID) int {
	for i := range int(s.eagerCount) {
		if s.eager[i] == peer {
			return i
		}
	}
	return -1
}

func (s *plumtreeSlot) pendingIndex(peer PeerID) int {
	for i := range int(s.pendingCount) {
		if s.pending[i].id == peer {
			return i
		}
	}
	return -1
}

func (s *plumtreeSlot) removePendingAt(index int) {
	last := int(s.pendingCount) - 1
	s.pending[index] = s.pending[last]
	s.pending[last] = plumtreePendingPeer{}
	s.pendingCount--
}

func (s *plumtreeSlot) removePending(peer PeerID) bool {
	index := s.pendingIndex(peer)
	if index < 0 {
		return false
	}
	s.removePendingAt(index)
	return true
}

func (s *plumtreeSlot) expirePending(now time.Time) {
	expiredBefore := now.Add(-plumtreePendingFeedbackTTL)
	for i := 0; i < int(s.pendingCount); {
		if !s.pending[i].sent.After(expiredBefore) {
			s.removePendingAt(i)
			continue
		}
		i++
	}
}

func (s *plumtreeSlot) reservePending(peer PeerID, now time.Time) {
	if index := s.pendingIndex(peer); index >= 0 {
		s.pending[index].sent = now
		return
	}

	s.pending[s.pendingCount] = plumtreePendingPeer{
		id:   peer,
		sent: now,
	}
	s.pendingCount++
}

func (p *plumtreePartState) wasSentTo(peer PeerID) bool {
	for i := range int(p.fullSentCount) {
		if p.fullSentTo[i] == peer {
			return true
		}
	}
	return false
}

func (p *plumtreePartState) addFullSent(peer PeerID) {
	p.fullSentTo[p.fullSentCount] = peer
	p.fullSentCount++
}

func (p *plumtreePartState) wasAdvertisedTo(peer PeerID) bool {
	for i := range int(p.advertisedCount) {
		if p.advertisedTo[i] == peer {
			return true
		}
	}
	return false
}

func (p *plumtreePartState) addAdvertised(peer PeerID) {
	p.advertisedTo[p.advertisedCount] = peer
	p.advertisedCount++
}

func (e *plumtreeEngine) canReserveFeedbackLocked(
	slot *plumtreeSlot,
	peer PeerID,
	now time.Time,
) bool {
	slot.expirePending(now)
	if slot.hasEager(peer) || slot.pendingIndex(peer) >= 0 {
		return true
	}
	return int(slot.eagerCount)+int(slot.pendingCount) < e.localEagerLimit
}

func (e *plumtreeEngine) addEagerReferenceLocked(peer PeerID, now time.Time) {
	activity := e.activities[peer]
	activity.references++
	if activity.references == 1 {
		activity.lastActive = now
	}
	e.activities[peer] = activity
}

func (e *plumtreeEngine) removeEagerReferenceLocked(peer PeerID) {
	activity := e.activities[peer]
	activity.references--
	if activity.references == 0 {
		delete(e.activities, peer)
		return
	}
	e.activities[peer] = activity
}

func (e *plumtreeEngine) noteActiveLocked(peer PeerID, now time.Time) {
	activity, ok := e.activities[peer]
	if !ok {
		return
	}
	activity.lastActive = now
	activity.sentSinceActive = 0
	e.activities[peer] = activity
}

func (e *plumtreeEngine) removeEagerAtLocked(slot *plumtreeSlot, index int) {
	peer := slot.eager[index]
	last := int(slot.eagerCount) - 1
	slot.eager[index] = slot.eager[last]
	slot.eager[last] = PeerID{}
	slot.eagerCount--
	e.removeEagerReferenceLocked(peer)
}

func (e *plumtreeEngine) removeEagerLocked(slot *plumtreeSlot, peer PeerID) {
	if index := slot.eagerIndex(peer); index >= 0 {
		e.removeEagerAtLocked(slot, index)
	}
	slot.removePending(peer)
}

func (e *plumtreeEngine) promoteEagerLocked(
	slot *plumtreeSlot,
	peer PeerID,
	force bool,
	now time.Time,
) {
	slot.expirePending(now)
	slot.removePending(peer)
	if peer.IsZero() || slot.hasEager(peer) {
		return
	}
	if !force && int(slot.eagerCount) >= e.localEagerLimit {
		return
	}
	if force && int(slot.eagerCount) >= e.localEagerLimit {
		e.removeEagerAtLocked(slot, rand.IntN(int(slot.eagerCount)))
	}

	slot.eager[slot.eagerCount] = peer
	slot.eagerCount++
	e.addEagerReferenceLocked(peer, now)
}

// Resolves the roster once for all slots rather than per slot. The verdicts must
// cover the simple tree too: receivesBroadcasts reports false for a peer it was
// not asked about, which would read as "gone" for a healthy one.
func (e *plumtreeEngine) expireInactiveEagerLocked(now time.Time) {
	var unique []PeerID
	for i := range e.slots {
		slot := &e.slots[i]
		for j := range int(slot.eagerCount) {
			if !slices.Contains(unique, slot.eager[j]) {
				unique = append(unique, slot.eager[j])
			}
		}
	}
	if len(unique) == 0 {
		return
	}

	verdicts := e.receiveVerdicts(unique)
	for i := range e.slots {
		e.removeInactiveEagerLocked(&e.slots[i], now, &verdicts)
	}
}

func (e *plumtreeEngine) removeInactiveEagerLocked(
	slot *plumtreeSlot,
	now time.Time,
	verdicts *plumtreeReceiveVerdicts,
) {
	inactiveBefore := now.Add(-plumtreeEagerInactivityTTL)
	for i := 0; i < int(slot.eagerCount); {
		peer := slot.eager[i]
		activity, ok := e.activities[peer]
		if !ok || activity.lastActive.After(inactiveBefore) {
			i++
			continue
		}

		isInactive := !verdicts.receivesBroadcasts(peer) ||
			activity.sentSinceActive > plumtreeMaxSentWithoutActivity
		if !isInactive {
			i++
			continue
		}

		slot.removePending(peer)
		e.removeEagerAtLocked(slot, i)
	}
}

func (e *plumtreeEngine) registerFullSendLocked(
	slot *plumtreeSlot,
	part *plumtreePartState,
	peer PeerID,
	now time.Time,
) {
	if slot.hasEager(peer) {
		slot.removePending(peer)
		activity := e.activities[peer]
		activity.sentSinceActive++
		e.activities[peer] = activity
	} else {
		slot.reservePending(peer, now)
		e.notePendingDeadlineLocked(now.Add(plumtreePendingFeedbackTTL))
	}
	part.addFullSent(peer)
}

func (e *plumtreeEngine) RemovePeer(peer PeerID) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return
	}

	for i := range e.slots {
		e.removeEagerLocked(&e.slots[i], peer)
	}
	delete(e.announcements, peer)
}

func (e *plumtreeEngine) statsEagerPeers(
	treeIndex int32,
) plumtreeStatsEagerPeers {
	e.mu.Lock()
	defer e.mu.Unlock()

	slot := &e.slots[treeIndex]
	var peers plumtreeStatsEagerPeers
	peers.count = slot.eagerCount
	copy(peers.peers[:], slot.eager[:slot.eagerCount])
	return peers
}

func (e *plumtreeEngine) statsEagerSnapshot(
	rotatingTree int32,
) plumtreeStatsEagerSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	var snapshot plumtreeStatsEagerSnapshot
	simple := &e.slots[plumtreeSimpleTree]
	snapshot.simple.count = simple.eagerCount
	copy(
		snapshot.simple.peers[:],
		simple.eager[:simple.eagerCount],
	)

	rotating := &e.slots[rotatingTree]
	snapshot.rotating.count = rotating.eagerCount
	copy(
		snapshot.rotating.peers[:],
		rotating.eager[:rotating.eagerCount],
	)
	return snapshot
}
