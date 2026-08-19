package network

import (
	"io"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
)

type blockBroadcastPublisher interface {
	TryPublishAccepted(p2p.AcceptedBlockPublication) bool
	TryRelayCandidate(p2p.BlockCandidatePublication) bool
	TryCacheCandidate(p2p.TrustedBlockCandidate) bool
	RegisterPlumtreeProducer() io.Closer
}

// PublishAcceptedBlock transfers an immutable accepted block to the node-owned
// bounded publication worker. Queue pressure is best-effort by contract.
func (m *Manager) PublishAcceptedBlock(publication validator.AcceptedBlockPublication) {
	session, err := m.session(publication.SessionID)
	if err != nil {
		m.log.Warn().
			Err(err).
			Hex("session_id", publication.SessionID[:]).
			Str("publication", "accepted").
			Msg("dropping block publication without an active validator session")
		return
	}
	spec, err := session.contribution(sessionKindValidator)
	if err != nil || spec.signer == nil {
		m.log.Warn().
			Err(err).
			Hex("session_id", publication.SessionID[:]).
			Str("publication", "accepted").
			Msg("dropping block publication without a signed validator contribution")
		return
	}

	blockMode := p2p.BlockBroadcastModeCustom
	var finalityMode p2p.BlockBroadcastMode
	plumtree := spec.protocolVersion >= 2
	if publication.Public {
		blockMode |= p2p.BlockBroadcastModePublic
		if !plumtree {
			blockMode |= p2p.BlockBroadcastModeFastSync
		}
	}
	if plumtree {
		finalityMode = p2p.BlockBroadcastModePublic |
			p2p.BlockBroadcastModeFastSync |
			p2p.BlockBroadcastModeCustom
	}
	var certificateSigner = spec.signer
	if !plumtree {
		certificateSigner = nil
	}

	if m.broadcasts.TryPublishAccepted(p2p.AcceptedBlockPublication{
		Block:             publication.Block,
		Signatures:        publication.Signatures,
		BlockMode:         blockMode,
		FinalityMode:      finalityMode,
		Plumtree:          plumtree,
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
	if spec.workchain == -1 && spec.protocolVersion < 2 {
		return
	}

	mode := p2p.BlockBroadcastModeCustom
	plumtree := spec.protocolVersion >= 2
	if plumtree && spec.signer != nil {
		mode |= p2p.BlockBroadcastModePublic | p2p.BlockBroadcastModeFastSync
	}
	if !plumtree {
		mode |= p2p.BlockBroadcastModeFastSync
	}
	var certificateSigner = spec.signer
	if !plumtree {
		certificateSigner = nil
	}
	if m.broadcasts.TryRelayCandidate(p2p.BlockCandidatePublication{
		Block:             artifact.Candidate.Block,
		BlockBOC:          artifact.BlockBOC,
		CatchainSeqno:     spec.catchainSeqno,
		ValidatorSetHash:  spec.validatorSetHash,
		Mode:              mode,
		Plumtree:          plumtree,
		CertificateSigner: certificateSigner,
	}) {
		return
	}

	m.log.Warn().
		Hex("session_id", spec.id[:]).
		Str("publication", "candidate").
		Msg("dropping candidate relay because the queue is unavailable")
}

func (m *Manager) cacheCandidate(spec sessionSpec, artifact validator.CandidateArtifact) {
	if artifact.Candidate.Empty {
		return
	}
	if m.broadcasts.TryCacheCandidate(p2p.TrustedBlockCandidate{
		Block:    artifact.Candidate.Block,
		BlockBOC: artifact.BlockBOC,
	}) {
		return
	}

	m.log.Warn().
		Hex("session_id", spec.id[:]).
		Str("publication", "candidate_cache").
		Msg("dropping trusted candidate because the cache queue is unavailable")
}
