package p2p

import (
	"context"
	"errors"
	"net"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// TwoStepSendFault classifies why one recipient of a two-step fan-out did not
// take its part. The distinction carries two decisions. First, only a genuine
// transport fault says anything about the peer, so only that class may feed
// peer health: a budget this node imposed on itself, a dial this node deferred
// and a connection this node has not opened yet are all local state. Second,
// the ratio of delivered to missing recipients is what separates a harmless
// miss - two-step FEC reconstructs from (n-1)/2 parts, so one absent peer of
// fifteen costs nothing - from a candidate that reached nobody.
type TwoStepSendFault uint8

const (
	// TwoStepSendFaultOffline: no live outbound connection when the part was
	// due. A background dial has been asked for; the part is dropped.
	TwoStepSendFaultOffline TwoStepSendFault = iota
	// TwoStepSendFaultDialDeferred: the peer's route is inside its retry
	// window, so no dial was attempted at all.
	TwoStepSendFaultDialDeferred
	// TwoStepSendFaultWriteDeadline: the transport's own write deadline fired.
	TwoStepSendFaultWriteDeadline
	// TwoStepSendFaultContextDeadline: the caller's budget expired.
	TwoStepSendFaultContextDeadline
	// TwoStepSendFaultCanceled: the fan-out was cancelled - node shutdown or a
	// retired session.
	TwoStepSendFaultCanceled
	// TwoStepSendFaultOther: an error the peer is answerable for.
	TwoStepSendFaultOther
	TwoStepSendFaultCount
)

// TwoStepSendOutcome reports one fan-out by recipient. Attempted counts the
// recipients the broadcast was addressed to, Sent the ones that took their
// part, and Faults says why the rest did not.
type TwoStepSendOutcome struct {
	BroadcastID []byte
	Attempted   int
	Sent        int
	// Pending counts recipients whose send had not finished when the fan-out
	// returned at quorum. They are neither delivered nor failed, so a caller
	// judging delivery must not count them against it.
	Pending int
	Faults  [TwoStepSendFaultCount]int
}

// Failed is the number of recipients that did not take their part. Recipients
// still in flight when the fan-out was released at quorum are not among them.
func (o TwoStepSendOutcome) Failed() int {
	failed := o.Attempted - o.Sent - o.Pending
	if failed < 0 {
		return 0
	}

	return failed
}

// twoStepSendOutcome folds a tonutils fan-out result into the per-recipient
// classification and feeds peer health only the faults the peer is answerable
// for. Reporting our own deadline, our own deferred dial or a not-yet-open
// connection as a peer fault used to evict a validator - handlePeerQueryFailure
// removes a peer with no open connection - from the very private overlay this
// node is required to reach.
func (s *overlaySubscription) twoStepSendOutcome(
	result overlay.BroadcastTwoStepSendResult,
) TwoStepSendOutcome {
	outcome := TwoStepSendOutcome{
		BroadcastID: result.BroadcastID,
		Attempted:   result.Attempted,
		Sent:        result.Sent,
		Pending:     result.Pending,
	}
	for _, peerErr := range result.Failed {
		fault := classifyTwoStepSendFault(peerErr.Err)
		outcome.Faults[fault]++
		if fault != TwoStepSendFaultOther {
			continue
		}
		id, err := NewPeerID(peerErr.PeerID)
		if err != nil {
			continue
		}
		peer := s.peerByID(id)
		if peer == nil {
			continue
		}
		s.handlePeerQueryFailure(peer, peerErr.Err)
	}

	return outcome
}

func classifyTwoStepSendFault(err error) TwoStepSendFault {
	switch {
	case err == nil:
		return TwoStepSendFaultOther
	case errors.Is(err, errQUICPeerOffline),
		errors.Is(err, errQUICRouteMissing),
		errors.Is(err, errAuthenticatedQUICPeerNotFound):
		return TwoStepSendFaultOffline
	case errors.Is(err, errQUICDialDeferred):
		return TwoStepSendFaultDialDeferred
	case errors.Is(err, context.Canceled):
		return TwoStepSendFaultCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return TwoStepSendFaultContextDeadline
	}

	// A stream deadline surfaces as a net.Error timeout rather than a context
	// error, because tonutils arms the QUIC stream from the context deadline
	// (adnl/quic/transport.go sendMessageParts) and the stream reports first.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return TwoStepSendFaultWriteDeadline
	}

	return TwoStepSendFaultOther
}
