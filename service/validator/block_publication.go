package validator

import (
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator/groups"
)

// BlockAcceptanceView is the exact masterchain-derived shard registry used to
// bind a newly accepted shard block to its registered ancestors.
type BlockAcceptanceView struct {
	MasterchainBlock ton.BlockIDExt
	Registered       []groups.ShardDescription
}

// AcceptedBlockPublication is the immutable material required to publish an
// accepted block and its logical Simplex certificate. The publisher owns any
// queueing, deduplication, and transport-specific serialization policy.
type AcceptedBlockPublication struct {
	SessionID  [32]byte
	Block      p2p.DownloadedBlock
	Signatures *blockproof.ValidatorSignatureSet
	// Public is true only for the scheduled validator that produced the
	// candidate. Every validator may publish into its configured private/custom
	// overlays, while the public shard overlay has exactly one full-block
	// originator for the slot.
	Public bool
}

// BlockPublisher is the network boundary of block acceptance. Calls only
// transfer immutable objects to implementation-owned bounded queues and never
// wait for network delivery. It may be omitted while networking is not
// composed; production validators need it as recovery for best-effort local
// submission.
type BlockPublisher interface {
	PublishAcceptedBlock(AcceptedBlockPublication)
}
