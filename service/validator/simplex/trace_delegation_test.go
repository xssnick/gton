package simplex

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
)

func TestCandidateReceivedTraceCollatorIdentity(t *testing.T) {
	type traceCase struct {
		name      string
		local     int
		delegated bool
		wantTrace bool
	}
	cases := []traceCase{
		{name: "own_self_collation", local: 0},
		{name: "remote_self_collation", local: 1, wantTrace: true},
		{name: "own_delegated_window", local: 0, delegated: true, wantTrace: true},
		{name: "remote_delegated_window", local: 1, delegated: true, wantTrace: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tracer := &recTracer{}
			env := newTestEnv(t, withLocal(tc.local), func(_ *testEnv, cfg *Config) {
				cfg.Tracer = tracer
			})
			env.start()
			candidate := env.makeCandidate(0, Genesis(), 0x73)
			var collatorID [32]byte
			if tc.delegated {
				key := testKey(73)
				pub := key.Public().(ed25519.PublicKey)
				collatorID = KeyNodeIDShort(pub)
				env.delegate(candidate, pub, key)
			}
			if err := env.eng.SubmitCandidate(candidate); err != nil {
				t.Fatal(err)
			}

			var count int
			for _, event := range tracer.events {
				received, ok := event.(TraceCandidateReceived)
				if !ok {
					continue
				}
				count++
				if received.CollatorNodeID != collatorID || received.IsCollator || received.ID != candidate.ID {
					t.Fatalf("candidate received event = %+v, want collator %x", received, collatorID)
				}
				wire := received.Serialize()
				// ton_api.tl at b8c167a: candidateReceived includes collator_node_id.
				if binary.LittleEndian.Uint32(wire[:4]) != 0xdd4bc811 {
					t.Fatalf("candidateReceived constructor = %x", wire[:4])
				}
				var decoded ConsensusStatsCandidateReceived
				rest, err := tl.Parse(&decoded, wire, true)
				if err != nil || len(rest) != 0 || !bytes.Equal(decoded.CollatorNodeID, collatorID[:]) {
					t.Fatalf("decode event: %+v, trailing=%d, err=%v", decoded, len(rest), err)
				}
			}
			want := 0
			if tc.wantTrace {
				want = 1
			}
			if count != want {
				t.Fatalf("candidate received event count = %d, want %d", count, want)
			}
		})
	}
}
