package collator

import (
	"math"
	"testing"
)

func TestNextMasterBlockStartLT(t *testing.T) {
	tests := []struct {
		name       string
		previous   uint64
		shardEndLT uint64
		want       uint64
		wantErr    bool
	}{
		{name: "aligned predecessor", previous: 10_000_000, shardEndLT: 9_000_001, want: 11_000_000},
		{name: "round shard end", previous: 10_000_000, shardEndLT: 11_000_001, want: 12_000_000},
		{name: "maximum shard growth", previous: 10_000_000, shardEndLT: 19_999_999, want: 20_000_000, wantErr: true},
		{name: "shard growth exceeded", previous: 10_000_000, shardEndLT: 20_000_000, wantErr: true},
		{name: "start growth exceeded after rounding", previous: 10_500_000, shardEndLT: 20_499_999, wantErr: true},
		{name: "alignment overflow", previous: math.MaxUint64 - 1, shardEndLT: 0, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := nextMasterBlockStartLT(test.previous, test.shardEndLT)
			if (err != nil) != test.wantErr {
				t.Fatalf("nextMasterBlockStartLT() error = %v, wantErr %t", err, test.wantErr)
			}
			if err == nil && got != test.want {
				t.Fatalf("nextMasterBlockStartLT() = %d, want %d", got, test.want)
			}
		})
	}
}
