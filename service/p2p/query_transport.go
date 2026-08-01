package p2p

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync/atomic"

	adnloverlay "github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type peerQueryTransport interface {
	Query(ctx context.Context, maxAnswerSize uint64, req tl.Serializable, result tl.Serializable) error
	QueryRaw(ctx context.Context, maxAnswerSize uint64, req tl.Serializable) ([]byte, error)
}

type quicOverlayEnvelope struct {
	overlayID PeerID
	state     atomic.Pointer[quicOverlayEnvelopeState]
}

type quicOverlayEnvelopeState struct {
	queryPrefix   []byte
	messagePrefix []byte
	plain         bool
}

type rldpPeerQueryTransport struct {
	overlay   *adnloverlay.RLDPOverlayWrapper
	overlayID []byte
}

type quicPeerQueryTransport struct {
	peer     *overlayPeer
	envelope *quicOverlayEnvelope
}

var _ peerQueryTransport = rldpPeerQueryTransport{}
var _ peerQueryTransport = quicPeerQueryTransport{}

func newQUICOverlayEnvelope(
	overlayID []byte,
	certificate *adnloverlay.MemberCertificate,
) (*quicOverlayEnvelope, error) {
	id, err := NewPeerID(overlayID)
	if err != nil {
		return nil, fmt.Errorf("parse QUIC overlay id: %w", err)
	}

	envelope := &quicOverlayEnvelope{overlayID: id}
	state, err := envelope.prepareCertificate(certificate)
	if err != nil {
		return nil, err
	}
	envelope.state.Store(state)
	return envelope, nil
}

func (e *quicOverlayEnvelope) prepareCertificate(
	certificate *adnloverlay.MemberCertificate,
) (*quicOverlayEnvelopeState, error) {
	query := tl.Serializable(adnloverlay.Query{Overlay: e.overlayID[:]})
	message := tl.Serializable(adnloverlay.Message{Overlay: e.overlayID[:]})
	plain := certificate == nil
	if certificate != nil {
		extra := adnloverlay.MessageExtra{
			Flags:       1,
			Certificate: *certificate,
		}
		query = adnloverlay.QueryWithExtra{
			Overlay: e.overlayID[:],
			Extra:   extra,
		}
		message = adnloverlay.MessageWithExtra{
			Overlay: e.overlayID[:],
			Extra:   extra,
		}
	}

	queryPrefix, err := tl.Serialize(query, true)
	if err != nil {
		return nil, fmt.Errorf("serialize QUIC query envelope: %w", err)
	}
	messagePrefix, err := tl.Serialize(message, true)
	if err != nil {
		return nil, fmt.Errorf("serialize QUIC message envelope: %w", err)
	}
	return &quicOverlayEnvelopeState{
		queryPrefix:   queryPrefix,
		messagePrefix: messagePrefix,
		plain:         plain,
	}, nil
}

func (e *quicOverlayEnvelope) Query(
	req tl.Serializable,
) ([]byte, error) {
	state := e.state.Load()
	payload, err := appendQUICOverlayBody(state.queryPrefix, req)
	if err != nil {
		return nil, fmt.Errorf("serialize QUIC overlay query: %w", err)
	}
	return payload, nil
}

func (e *quicOverlayEnvelope) Message(
	message tl.Serializable,
) ([]byte, error) {
	wire, err := appendQUICOverlayBody(e.state.Load().messagePrefix, message)
	if err != nil {
		return nil, fmt.Errorf("serialize QUIC overlay message: %w", err)
	}
	return wire, nil
}

func (e *quicOverlayEnvelope) messageBuffer(
	bodyCapacity int,
) ([]byte, int) {
	prefix := e.state.Load().messagePrefix
	wire := make([]byte, len(prefix), len(prefix)+bodyCapacity)
	copy(wire, prefix)
	return wire, len(prefix)
}

func (e *quicOverlayEnvelope) WrapMessageBody(
	body []byte,
) ([]byte, []byte) {
	prefix := e.state.Load().messagePrefix
	wire := make([]byte, len(prefix)+len(body))
	copy(wire, prefix)
	bodyOffset := len(prefix)
	copy(wire[bodyOffset:], body)
	return wire, wire[bodyOffset:]
}

func (e *quicOverlayEnvelope) CanReuseMessage(
	header quicOverlayHeader,
) bool {
	state := e.state.Load()
	return state.plain &&
		header.certificateKind == quicMembershipCertificateOmitted
}

func appendQUICOverlayBody(
	prefix []byte,
	body tl.Serializable,
) ([]byte, error) {
	payload := make([]byte, 0, len(prefix)+tl.DefaultSerializeBufferSize)
	payload = append(payload, prefix...)
	return tl.Append(payload, body, true)
}

func (t rldpPeerQueryTransport) Query(
	ctx context.Context,
	maxAnswerSize uint64,
	req tl.Serializable,
	result tl.Serializable,
) error {
	answer, err := t.QueryRaw(ctx, maxAnswerSize, req)
	if err != nil {
		return err
	}
	return parsePeerQueryAnswer(result, answer)
}

func (t rldpPeerQueryTransport) QueryRaw(
	ctx context.Context,
	maxAnswerSize uint64,
	req tl.Serializable,
) ([]byte, error) {
	var queryID [32]byte
	if _, err := rand.Read(queryID[:]); err != nil {
		return nil, fmt.Errorf("generate RLDP query id: %w", err)
	}

	result := make(chan rldp.AsyncQueryResult, 1)
	err := t.overlay.DoQueryAsync(
		ctx,
		maxAnswerSize,
		queryID[:],
		adnloverlay.WrapQuery(t.overlayID, req),
		result,
	)
	if err != nil {
		return nil, err
	}

	select {
	case answer := <-result:
		return answer.ResultBytes, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("response deadline exceeded: %w", ctx.Err())
	}
}

func (t quicPeerQueryTransport) Query(
	ctx context.Context,
	maxAnswerSize uint64,
	req tl.Serializable,
	result tl.Serializable,
) error {
	answer, err := t.QueryRaw(ctx, maxAnswerSize, req)
	if err != nil {
		return err
	}
	return parsePeerQueryAnswer(result, answer)
}

func (t quicPeerQueryTransport) QueryRaw(
	ctx context.Context,
	maxAnswerSize uint64,
	req tl.Serializable,
) ([]byte, error) {
	peer, err := t.peer.dialQUIC(ctx)
	if err != nil {
		return nil, err
	}

	payload, err := t.envelope.Query(req)
	if err != nil {
		return nil, err
	}
	answer, err := peer.QueryOutbound(ctx, payload, int64(maxAnswerSize))
	if err != nil {
		return nil, fmt.Errorf("query quic overlay: %w", err)
	}
	return answer, nil
}

func parsePeerQueryAnswer(result tl.Serializable, answer []byte) error {
	rest, err := tl.ParseNoCopy(result, answer, true)
	if err != nil {
		return fmt.Errorf("parse overlay query answer: %w", err)
	}
	if len(rest) != 0 {
		return fmt.Errorf("parse overlay query answer: %d trailing bytes", len(rest))
	}
	return nil
}
