package groups

import "testing"

var benchmarkPublicKeyHash [32]byte

func BenchmarkPublicKeyHash(b *testing.B) {
	publicKey := sequentialBytes(0x00)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		hash, err := PublicKeyHash(publicKey)
		if err != nil {
			b.Fatalf("PublicKeyHash: %v", err)
		}
		benchmarkPublicKeyHash = hash
	}
}
