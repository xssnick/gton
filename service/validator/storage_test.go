package validator

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/tonutils-go/tl"
)

var benchmarkSessionNamespace [32]byte

func TestSessionStorageConstructorID(t *testing.T) {
	if actual := tl.CRC(consensusDBIDSchema); actual != consensusDBIDConstructorID {
		t.Fatalf("constructor id = 0x%08x, want 0x%08x", actual, consensusDBIDConstructorID)
	}
}

func TestSessionStorageNamespaceWireParity(t *testing.T) {
	id := testSessionStorageID()

	wire := make([]byte, 0, 104)
	wire = binary.LittleEndian.AppendUint32(wire, tl.CRC(consensusDBIDSchema))
	wire = append(wire, id.SessionID[:]...)
	wire = tl.AppendBool(wire, id.IsValidator)
	wire = append(wire, id.ValidatorKeyID[:]...)
	wire = append(wire, id.LocalADNLID[:]...)
	want := sha256.Sum256(wire)

	actual, err := id.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	if actual != want {
		t.Fatalf("namespace = %x, want canonical consensus.dbId hash %x", actual, want)
	}
}

func BenchmarkSessionStorageIDNamespace(b *testing.B) {
	id := testSessionStorageID()

	b.ReportAllocs()
	for b.Loop() {
		var err error
		benchmarkSessionNamespace, err = id.Namespace()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func testSessionStorageID() SessionStorageID {
	return SessionStorageID{
		SessionID:      [32]byte{1},
		Shard:          groups.ShardID{Workchain: -1, Shard: math.MinInt64},
		CatchainSeqno:  1,
		IsValidator:    true,
		ValidatorKeyID: [32]byte{2},
		LocalADNLID:    [32]byte{3},
		ValidatorIndex: 0,
		Protocol:       SessionProtocol{Version: 1, ProtocolVersion: 3, UseQUIC: true},
	}
}
