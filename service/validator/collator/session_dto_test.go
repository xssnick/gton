package collator

import (
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestValidateSessionSupportsEveryConfig22Protocol(t *testing.T) {
	base := Session{
		ID:                   [32]byte{1},
		Shard:                groups.ShardID{Workchain: 0, Shard: -1 << 63},
		ConsensusVersion:     2,
		SlotsPerLeaderWindow: 4,
		Validators: []SessionValidator{{
			PublicKey: [32]byte{2},
			ADNLID:    [32]byte{3},
			Weight:    1,
		}},
	}
	for protocolVersion := uint8(0); protocolVersion <= simplex.MaxProtocolVersion; protocolVersion++ {
		session := base
		session.ProtocolVersion = protocolVersion
		if err := validateSession(session); err != nil {
			t.Fatalf("protocol version %d: %v", protocolVersion, err)
		}
	}

	unsupportedVersion := base
	unsupportedVersion.ConsensusVersion = 1
	if err := validateSession(unsupportedVersion); err == nil {
		t.Fatal("simplex config version 1 was accepted")
	}
	unsupportedProtocol := base
	unsupportedProtocol.ProtocolVersion = simplex.MaxProtocolVersion + 1
	if err := validateSession(unsupportedProtocol); err == nil {
		t.Fatal("unrepresentable protocol version was accepted")
	}
}
