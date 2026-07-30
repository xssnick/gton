package p2p

import (
	"context"
	"math/rand/v2"
	"time"
)

// This file is the only place outside the three overlaySpec builders that names
// an overlayKind. Everywhere else asks a named behavioural question instead.
//
// The point is not to hide the enum. It is that a behaviour two subsystems must
// agree on — the two-step wiring in newOverlaySubscription and the two-step
// drain worker in custom_overlay.go, the pending flag set on attach and the
// pending-warmup retry, the receive-side and send-side halves of the IHR rule —
// used to be stated once per file as an open-coded kind compare, and drifted
// silently when only one of them was edited. Here each of those is one named
// fact with one body.
//
// Two rules keep this file honest:
//
//  1. A predicate reads spec.Kind when it is called. Nothing is precomputed
//     into a field, nothing is installed by a constructor, and spec is never
//     written. That is load-bearing, not stylistic: most subscriptions in this
//     package's tests are hand-built as raw &overlaySubscription{spec:
//     overlaySpec{Kind: ...}} literals that never reach
//     newOverlaySubscription, and two of them assign sub.spec.Kind after
//     construction and then call an inner function directly. A call-time read
//     is the only shape that is correct for all of them.
//
//  2. `s.fastSync != nil` is not a kind question and must not be rewritten into
//     one. It is the runtime question "does this subscription own a membership
//     object", and it is the right form wherever s.fastSync is about to be
//     dereferenced. Use the predicates below for policy; use the nil check for
//     dereferences.
//
// Receivers are pointers because overlaySpec is 208 bytes and a value receiver
// would copy it on every call the inliner declined. With a pointer receiver on
// a spec reached through a *overlaySubscription each predicate inlines to the
// same byte compare the call site used to contain (inline cost 5 against a
// budget of 80), which is what makes them usable on the per-inbound-message and
// per-broadcast paths.

// ---------------------------------------------------------------------------
// membership, discovery, peer lifecycle
// ---------------------------------------------------------------------------

// seedsFromFixedNodes reports whether peers are dialled from a configured
// roster instead of crawled out of the DHT by overlay id. Both roster kinds
// know every member's ADNL id up front, so discovery would only rediscover what
// the config already says.
func (spec *overlaySpec) seedsFromFixedNodes() bool {
	return spec.Kind == overlayKindCustomFixed ||
		spec.Kind == overlayKindFastSync
}

// restrictsPeerIDs reports whether roster admission is limited to the ids in
// FixedNodeIDs. FastSync answers the same question from its live roster instead
// and is handled before this in acceptsPeerID.
func (spec *overlaySpec) restrictsPeerIDs() bool {
	return spec.Kind == overlayKindCustomFixed
}

// adoptsInboundPeers reports whether a raw inbound transport joins the roster
// directly. Public overlays serve unlisted ingress through the
// broadcast-receiver resolver instead, so an inbound stranger never becomes a
// member there.
func (spec *overlaySpec) adoptsInboundPeers() bool {
	return spec.Kind == overlayKindCustomFixed
}

// membersArePermanent is the per-overlay default for overlayPeer.fixedMember:
// never evicted for capacity, starts alive=false, pinned in the pool. FastSync
// overrides it per peer from its roster, so this is only the default.
func (spec *overlaySpec) membersArePermanent() bool {
	return spec.Kind == overlayKindCustomFixed
}

// peersStartPending reports whether a freshly attached peer is untrusted until
// warmup proves it answers. A configured roster member is a member from the
// start — there is nothing to prove and no alternative peer to prefer.
func (spec *overlaySpec) peersStartPending() bool {
	return spec.Kind == overlayKindPublicShard
}

// hasDirectoryTier reports whether the overlay splits knowledge (the directory)
// from transports (the live tier), and therefore has a live tier to top up.
// Fixed-membership overlays hold a transport for every member, so the split
// would be empty on one side.
func (spec *overlaySpec) hasDirectoryTier() bool {
	return spec.Kind == overlayKindPublicShard
}

// shufflesSeedOrder randomises the dial order of FixedNodes so a large
// validator set is not dialled in the same order by every node, which would
// concentrate the first wave of handshakes on the same few validators.
func (spec *overlaySpec) shufflesSeedOrder() bool {
	return spec.Kind == overlayKindFastSync
}

// rosterSizesPeerLimit lets a configured roster larger than maxPeersPerOverlay
// raise the transport budget to its own size: every member of a custom overlay
// is expected to be reachable, so the generic cap would silently drop members.
func (spec *overlaySpec) rosterSizesPeerLimit() bool {
	return spec.Kind == overlayKindCustomFixed
}

// usesDedicatedQueryPeers reports whether this overlay routes queries through
// its own configured roster instead of the public overlay peer set. Such an
// overlay has no DHT presence, so the archive pool must neither crawl for peers
// on it nor own their transports.
func (spec *overlaySpec) usesDedicatedQueryPeers() bool {
	return spec.Kind == overlayKindCustomFixed
}

// usesV1PeerGossip reports whether overlay.getRandomPeers (v1 NodesList) is
// exchanged with unattached directory rows. The kind term is what excludes
// FastSync: it also sets RandomPeers, but speaks getRandomPeersV2 only.
func (spec *overlaySpec) usesV1PeerGossip() bool {
	return spec.RandomPeers && spec.Kind == overlayKindPublicShard
}

// usesFastSyncPeerGossip selects the getRandomPeersV2 exchange, which carries
// the member certificates FastSync enrolment needs and which v1 cannot express.
func (spec *overlaySpec) usesFastSyncPeerGossip() bool {
	return spec.Kind == overlayKindFastSync
}

// runsFixedPeerProbes reports whether the overlay.Ping watchdog and its ADNL
// soft/hard transport recovery run here. A roster peer may carry no organic
// outbound traffic for hours, so nothing else would ever notice its pair died.
func (spec *overlaySpec) runsFixedPeerProbes() bool {
	return spec.Kind == overlayKindCustomFixed
}

// statusListsWholeRoster reports whether the status table renders every roster
// member rather than only the neighbours: a member frozen from the start is
// never promoted into neighbours and would otherwise be invisible exactly when
// its diagnostics matter most. Deliberately distinct from runsFixedPeerProbes
// even though they agree today — status formatting must not be coupled to probe
// policy.
func (spec *overlaySpec) statusListsWholeRoster() bool {
	return spec.Kind == overlayKindCustomFixed
}

// followsShardLifecycle reports whether the overlay is torn down by the
// TTL-based active-shard reconciliation. Custom overlays live as long as their
// config; FastSync overlays are reconciled by reconcileFastSyncOverlays.
func (spec *overlaySpec) followsShardLifecycle() bool {
	return spec.Kind == overlayKindPublicShard
}

// persistsPeerCache reports whether the roster is worth writing to disk. Only a
// DHT-discovered roster is expensive to rebuild after a restart; a configured
// one is already on disk in the config.
func (spec *overlaySpec) persistsPeerCache() bool {
	return spec.Kind == overlayKindPublicShard
}

// usesFastSyncRoster reports whether membership comes from a FastSync validator
// roster. newOverlaySubscription uses it to require the sub-spec before
// anything reads it; do not use it as a stand-in for `s.fastSync != nil` on a
// path that dereferences the runtime.
func (spec *overlaySpec) usesFastSyncRoster() bool {
	return spec.Kind == overlayKindFastSync
}

// ---------------------------------------------------------------------------
// queries
// ---------------------------------------------------------------------------

// probesPeersPeriodically reports whether the run loop arms the query-probe
// timer at all. Public and FastSync overlays arm it through QueryCapabilities
// and then take their own row of overlayProbeCadence; the kind term exists only
// because a custom overlay sets QueryCapabilities false and would otherwise
// never arm, however its operator configured SendQueries.
func (spec *overlaySpec) probesPeersPeriodically() bool {
	return spec.QueryCapabilities || spec.Kind == overlayKindCustomFixed && spec.SendQueries
}

// preferredAsQuerySource reports that this overlay outranks FastSync and the
// public pool as a download source. The kind term is load-bearing: FastSync
// also sets SendQueries, and selecting it here would bypass the FastSync
// readiness check in readyFastSyncQuerySubscription.
func (spec *overlaySpec) preferredAsQuerySource() bool {
	return spec.Kind == overlayKindCustomFixed && spec.SendQueries
}

// privatePeerRoster reports that the roster must not be disclosed through
// overlay.getRandomPeers.
func (spec *overlaySpec) privatePeerRoster() bool {
	return spec.Kind == overlayKindCustomFixed
}

// enforcesAcceptQueries reports whether spec.AcceptQueries gates non-ping
// queries. Only a fixed roster has the concept of the local node being a
// configured acceptor, so the flag is consulted nowhere else.
func (spec *overlaySpec) enforcesAcceptQueries() bool {
	return spec.Kind == overlayKindCustomFixed
}

// newInboundQueryLimiters builds one limiter per overlay kind. The reference
// node does the same — three RateLimiter instances, public, fast-sync and
// custom, with identical defaults — because the limit is per traffic source,
// not per overlay feature: a public overlay is the one every stranger can
// reach, so it needs the cap at least as much as a certified FastSync member
// does.
func newInboundQueryLimiters() [3]inboundQueryLimiter {
	return [3]inboundQueryLimiter{
		overlayKindPublicShard: newInboundQueryLimiter(),
		overlayKindCustomFixed: newInboundQueryLimiter(),
		overlayKindFastSync:    newInboundQueryLimiter(),
	}
}

// inboundQueryLimiter selects this overlay's share of the per-TL-type inbound
// query budget. Each kind gets its own window so a flood on one overlay cannot
// starve queries arriving on another.
func (s *overlaySubscription) inboundQueryLimiter() *inboundQueryLimiter {
	return &s.node.inboundQueryLimiters[s.spec.Kind]
}

// probeDrivenQueryReadiness reports that peer readiness is owned by the probe
// loop, so an application-query outcome must park or drop the peer rather than
// feed unreliability, failedQueries and RTT. On a fixed roster eviction is
// meaningless — there is no replacement to promote.
func (spec *overlaySpec) probeDrivenQueryReadiness() bool {
	return spec.Kind == overlayKindCustomFixed
}

// preservesQueryCandidateOrder reports that a hedged draw takes the roster
// prefix as-is instead of re-mixing neighbours with preferred peers: a
// configured roster is already the intended preference order.
func (spec *overlaySpec) preservesQueryCandidateOrder() bool {
	return spec.Kind == overlayKindCustomFixed
}

// pingsPeerOnQueryTimeout kicks an out-of-band liveness ping so the membership
// runtime learns a validator is dead before the next single-validator draw
// picks it again.
func (spec *overlaySpec) pingsPeerOnQueryTimeout() bool {
	return spec.Kind == overlayKindFastSync
}

// drawsRandomQueryFallback enables C++'s random verified-alive peer draw after
// the neighbour roster is exhausted. A fixed roster has no random tail to draw
// from.
func (spec *overlaySpec) drawsRandomQueryFallback() bool {
	return spec.Kind == overlayKindPublicShard
}

// overlayProbeCadence is the per-kind query-probe interval. It is a table
// rather than a chain of predicates because the two durations are paired data:
// every arm must populate both, and a zero jitter here would either panic in
// rand.Int64N or spin the ping timer. TestOverlayProbeCadenceIsArmed pins that.
var overlayProbeCadence = [3]struct {
	min    time.Duration
	jitter time.Duration
}{
	overlayKindPublicShard: {min: peerPingMinDelay, jitter: peerPingJitter},
	overlayKindCustomFixed: {min: customQueryProbeMinDelay, jitter: customQueryProbeJitter},
	overlayKindFastSync:    {min: fastSyncPingMinDelay, jitter: fastSyncPingJitter},
}

func (spec *overlaySpec) nextQueryProbeDelay() time.Duration {
	cadence := overlayProbeCadence[spec.Kind]
	if cadence.jitter <= 0 {
		return cadence.min
	}
	return cadence.min + time.Duration(rand.Int64N(int64(cadence.jitter)))
}

// ---------------------------------------------------------------------------
// broadcast
// ---------------------------------------------------------------------------

// restrictsBroadcastKinds admits only the block family, dropping externals and
// IHR before any type switch runs. A FastSync overlay exists to move blocks
// between validators; anything else on it is either a bug or an attack.
func (spec *overlaySpec) restrictsBroadcastKinds() bool {
	return spec.Kind == overlayKindFastSync
}

// dropsSelfSourcedBroadcasts drops our own payload echoed back by the two-step
// relay, which happens because we are also a listed sender on this overlay.
func (spec *overlaySpec) dropsSelfSourcedBroadcasts() bool {
	return spec.Kind == overlayKindCustomFixed
}

// dropsIHRBroadcasts reports that tonNode.ihrMessageBroadcast has no place on
// this overlay. Asked on both the receive and the send side, so the two halves
// of the rule cannot drift apart.
func (spec *overlaySpec) dropsIHRBroadcasts() bool {
	return spec.Kind == overlayKindCustomFixed
}

// authorizesBroadcastSenders reports that broadcasts carry per-sender roles:
// MsgSenders for messages, BlockSenders for blocks. Do not weaken this to
// len(BlockSenders) > 0 — a custom overlay with no configured block senders is
// legal and must reject every block, not accept every block.
func (spec *overlaySpec) authorizesBroadcastSenders() bool {
	return spec.Kind == overlayKindCustomFixed
}

// relaysFECBroadcasts reports that the transport itself accepts and forwards
// FEC parts. Its consequence at the application layer is that an inbound FEC
// broadcast must not additionally be enqueued for overlay rebroadcast, or every
// FEC broadcast is sent twice.
func (spec *overlaySpec) relaysFECBroadcasts() bool {
	return spec.Kind != overlayKindCustomFixed
}

// usesPlumtree reports whether a Plumtree runtime is built for this overlay.
// Kept separate from relaysFECBroadcasts even though they agree today: they are
// independent switches on the receiver and only happen to coincide for the
// current three kinds.
func (spec *overlaySpec) usesPlumtree() bool {
	return spec.Kind != overlayKindCustomFixed
}

// usesTwoStepDelivery reports whether whole-roster overlay.broadcastTwostep is
// available. The receiver wiring and startTwoStepRebroadcastWorker must agree
// on this or requests queue forever, or the queue is never drained.
func (spec *overlaySpec) usesTwoStepDelivery() bool {
	return spec.Kind == overlayKindCustomFixed ||
		spec.Kind == overlayKindFastSync
}

// alwaysTwoStepFEC reports that an FEC rebroadcast takes the two-step queue
// whatever the plan flags say, so planCustomRebroadcast's flags never have to
// be audited against this path.
func (spec *overlaySpec) alwaysTwoStepFEC() bool {
	return spec.Kind == overlayKindCustomFixed
}

// twoStepFECUnlessPlanOptsOut reports that FEC goes two-step unless the plan
// stamped overlay.BroadcastFlagNoTwoStep, which planFastSyncRebroadcast does
// for shard descriptions.
func (spec *overlaySpec) twoStepFECUnlessPlanOptsOut() bool {
	return spec.Kind == overlayKindFastSync
}

// floodsWholeRoster reports that a rebroadcast goes to every member rather than
// a gossip fanout: a private overlay is a fixed delivery set, not a mesh, so a
// partial fanout would simply lose deliveries.
func (spec *overlaySpec) floodsWholeRoster() bool {
	return spec.Kind == overlayKindCustomFixed
}

// pacesFECBursts inserts an inter-burst sleep every DefaultBroadcastFECBurstSize
// symbols instead of blasting all parts back to back. The pace itself is a Node
// option, so only the gate lives here.
func (spec *overlaySpec) pacesFECBursts() bool {
	return spec.Kind == overlayKindFastSync
}

// originatesLocalBroadcasts reports whether this overlay is a fanout sink for
// traffic we originate or accepted elsewhere. Public overlays are covered by
// their own relay and Plumtree; FastSync goes through fastSyncFanoutTarget.
func (spec *overlaySpec) originatesLocalBroadcasts() bool {
	return spec.Kind == overlayKindCustomFixed
}

// servesPublicIngress reports whether this overlay's BroadcastReceiver is
// resolvable by overlay id from raw unauthenticated ADNL ingress.
func (spec *overlaySpec) servesPublicIngress() bool {
	return spec.Kind == overlayKindPublicShard
}

// unauthorizedBroadcastLimit caps broadcasts from senders outside the overlay's
// authorized keys. A FastSync overlay only ever carries traffic from its
// validator roster, so nothing outside it is acceptable at any size: the
// reference node builds this overlay with OverlayPrivacyRules{0, 0, keys}
// (full-node-fast-sync-overlays.cpp), where a non-zero size from an
// unauthorized sender is Forbidden outright. Without the cap a certified
// non-validator member could push a full-size FEC broadcast that only the block
// signature check would eventually reject.
//
// Keeps the overlayKind parameter: TestUnauthorizedBroadcastLimitIsZeroOnlyForFastSync
// drives it as a table over all three kinds.
func unauthorizedBroadcastLimit(kind overlayKind) uint32 {
	if kind == overlayKindFastSync {
		return 0
	}
	return maxOverlayPayloadSize
}

func (spec *overlaySpec) unauthorizedBroadcastLimit() uint32 {
	return unauthorizedBroadcastLimit(spec.Kind)
}

// ---------------------------------------------------------------------------
// dispatchers
//
// These are the behaviours that are genuinely three-way. They stay methods with
// a switch rather than function fields on a policy value: a method cannot be
// nil for a hand-built test subscription, it is not an inlining barrier, and it
// is not opaque to escape analysis the way an indirect call through a struct
// field is.
// ---------------------------------------------------------------------------

// rebroadcastPlan selects the wire-format policy for one outbound kind. It is
// evaluated several times per accepted broadcast (once on enqueue, then per
// target peer), which is why it is a switch and not a dispatch through a value.
func (s *overlaySubscription) rebroadcastPlan(kind string, payloadLen int) rebroadcastPlan {
	switch s.spec.Kind {
	case overlayKindCustomFixed:
		return planCustomRebroadcast(kind, payloadLen)
	case overlayKindFastSync:
		return planFastSyncRebroadcast(kind, payloadLen)
	default:
		return planRebroadcast(kind, payloadLen)
	}
}

// warmupPeer is the first thing said to a freshly attached peer. The three arms
// are different protocols, not variations of one: a custom-fixed peer gets a
// bare ADNL nop (it may not answer overlay queries at all), a FastSync peer
// gets the enrolment exchange, and a public peer gets capabilities plus gossip.
func (s *overlaySubscription) warmupPeer(ctx context.Context, peer *overlayPeer) {
	switch s.spec.Kind {
	case overlayKindCustomFixed:
		s.warmupCustomFixedPeer(ctx, peer)
	case overlayKindFastSync:
		s.warmupFastSyncPeer(ctx, peer)
	default:
		s.warmupPublicPeer(ctx, peer)
	}
}

// pingPeers is the periodic liveness sweep. Each arm owns its own activity and
// target-selection guards, so this function deliberately has none.
func (s *overlaySubscription) pingPeers(ctx context.Context) {
	switch s.spec.Kind {
	case overlayKindCustomFixed:
		s.probeCustomQueryPeers(ctx)
	case overlayKindFastSync:
		s.pingFastSyncValidators(ctx)
	default:
		s.pingPublicPeers(ctx)
	}
}

// queryCandidates ranks the peers a query may be sent to.
func (s *overlaySubscription) queryCandidates(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	switch s.spec.Kind {
	case overlayKindCustomFixed:
		return s.customQueryCandidates(requiredVersionMajor, requiredVersionMinor)
	case overlayKindFastSync:
		return s.fastSyncQueryCandidates(
			requiredVersionMajor,
			requiredVersionMinor,
		)
	default:
		return s.publicQueryCandidates(requiredVersionMajor, requiredVersionMinor)
	}
}
