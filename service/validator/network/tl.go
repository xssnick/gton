package network

import (
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// These are exact constructor declarations from the testnet ton_api.tl. The
// consensus.overlayId constructor is already registered by tonutils-go's
// adnl/node package and is deliberately not registered a second time here.
const (
	schemeConsensusOverlayID          = "consensus.overlayId session_id:int256 nodes:(vector int256) = consensus.OverlayId"
	schemeConsensusBlockSyncOverlayID = "consensus.blockSyncOverlayId session_id:int256 = consensus.BlockOverlayId"
	schemeRequestError                = "consensus.requestError = consensus.RequestError"
	schemeCandidateAndCert            = "consensus.simplex.candidateAndCert candidate:bytes notar:bytes = consensus.simplex.CandidateAndCert"
	schemeRequestCandidate            = "consensus.simplex.requestCandidate id:consensus.CandidateId want_candidate:Bool want_notar:Bool = consensus.simplex.CandidateAndCert"
	schemePleaseCollatePrepare        = "consensus.pleaseCollatePrepare window_start_slot:int = tonNode.Success"
	schemePleaseCollate               = "consensus.pleaseCollate window_start_slot:int signature:bytes = tonNode.Success"
)

// ConsensusBlockSyncOverlayID is consensus.blockSyncOverlayId. Unlike the
// consensus overlay name, the block-sync overlay name contains no validator
// roster and is stable for the whole session.
type ConsensusBlockSyncOverlayID struct {
	SessionID []byte `tl:"int256"`
}

// ConsensusSimplexCandidateAndCert is
// consensus.simplex.candidateAndCert. An empty field means that the peer does
// not have, or was not asked for, that part. Notar contains a boxed
// consensus.simplex.voteSignatureSet, not a full certificate.
type ConsensusSimplexCandidateAndCert struct {
	Candidate []byte `tl:"bytes"`
	Notar     []byte `tl:"bytes"`
}

// ConsensusSimplexRequestCandidate is
// consensus.simplex.requestCandidate.
type ConsensusSimplexRequestCandidate struct {
	ID            ton.ConsensusCandidateID `tl:"struct boxed"`
	WantCandidate bool                     `tl:"bool"`
	WantNotar     bool                     `tl:"bool"`
}

func init() {
	tl.Register(ConsensusBlockSyncOverlayID{}, schemeConsensusBlockSyncOverlayID)
	tl.Register(ConsensusSimplexCandidateAndCert{}, schemeCandidateAndCert)
	tl.Register(ConsensusSimplexRequestCandidate{}, schemeRequestCandidate)
}

var (
	idRequestError         = tl.CRC(schemeRequestError)
	idRequestCandidate     = tl.CRC(schemeRequestCandidate)
	idBoolTrue             = tl.CRC("boolTrue = Bool")
	idBoolFalse            = tl.CRC("boolFalse = Bool")
	idPleaseCollatePrepare = tl.CRC(schemePleaseCollatePrepare)
	idPleaseCollate        = tl.CRC(schemePleaseCollate)
)
