package simplex

import (
	"crypto/ed25519"
	"fmt"
	"testing"
)

func benchEngine(b *testing.B, n int) (*Engine, *Certificate) {
	b.Helper()
	vals, keys := testValidators(n)
	var session [32]byte
	session[0] = 0x42
	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           ObserverIndex,
		SlotsPerLeaderWindow: 4,
		Journal:              NewMemoryJournal(n),
		Transport:            &fakeTransport{},
		Hooks:                acceptingHooks{},
	})
	if err != nil {
		b.Fatal(err)
	}
	v := NotarizeVote(candID(1, 0x5f))
	cert := &Certificate{Vote: v}
	payload := DataToSign(session, VoteBytes(v))
	for i := 0; i < n; i++ {
		cert.Signatures = append(cert.Signatures, VoteSignature{
			ValidatorIndex: uint32(i),
			Signature:      ed25519.Sign(keys[i], payload),
		})
	}
	return eng, cert
}

// BenchmarkVerifyCertificate measures the untrusted-quorum verification that
// sits on the certificate ingestion path (parallel above
// parallelVerifyMinSigs signatures).
func BenchmarkVerifyCertificate(b *testing.B) {
	for _, n := range []int{7, 32, 100, 300} {
		b.Run(fmt.Sprintf("validators=%d", n), func(b *testing.B) {
			eng, cert := benchEngine(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := eng.verifyCertificate(cert); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSignedVoteSerializeParse(b *testing.B) {
	sv := &SignedVote{ValidatorIndex: 3, Vote: NotarizeVote(candID(5, 0xee)), Signature: make([]byte, 64)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		wire := sv.Serialize()
		if _, _, err := parseSignedVote(wire); err != nil {
			b.Fatal(err)
		}
	}
}
