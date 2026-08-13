package network

import (
	"io"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type blockBroadcastPublisher interface {
	TryPublishAccepted(p2p.AcceptedBlockPublication) bool
	TryRelayCandidate(p2p.BlockCandidatePublication) bool
	RegisterPlumtreeProducer() io.Closer
}

// PublishAcceptedBlock transfers an immutable accepted block to the node-owned
// bounded publication worker. Queue pressure is best-effort by contract.
func (m *Manager) PublishAcceptedBlock(publication validator.AcceptedBlockPublication) {
	var certificateSigner overlay.BroadcastSigner
	if session, err := m.session(publication.SessionID); err == nil {
		if spec, specErr := session.contribution(sessionKindValidator); specErr == nil {
			certificateSigner = spec.signer
		}
	}

	if m.broadcasts.TryPublishAccepted(p2p.AcceptedBlockPublication{
		Block:             publication.Block,
		Signatures:        publication.Signatures,
		Public:            publication.Public,
		CertificateSigner: certificateSigner,
	}) {
		return
	}

	m.log.Warn().Str("publication", "accepted").Msg("dropping block publication because the queue is unavailable")
}

func (m *Manager) publishCandidate(spec sessionSpec, artifact validator.CandidateArtifact) {
	if artifact.Candidate.Empty {
		return
	}
	if m.broadcasts.TryRelayCandidate(p2p.BlockCandidatePublication{
		Block:             artifact.Candidate.Block,
		BlockBOC:          artifact.BlockBOC,
		CatchainSeqno:     spec.catchainSeqno,
		ValidatorSetHash:  spec.validatorSetHash,
		CertificateSigner: spec.signer,
	}) {
		return
	}

	m.log.Warn().
		Hex("session_id", spec.id[:]).
		Str("publication", "candidate").
		Msg("dropping candidate relay because the queue is unavailable")
}
