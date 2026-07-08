package service

import (
	"testing"
	"time"
)

func TestDownloadElapsedExcludingInlinePrepare(t *testing.T) {
	tests := []struct {
		name    string
		elapsed time.Duration
		prepare time.Duration
		want    time.Duration
	}{
		{
			name:    "no inline prepare",
			elapsed: 20 * time.Millisecond,
			want:    20 * time.Millisecond,
		},
		{
			name:    "subtract inline prepare",
			elapsed: 20 * time.Millisecond,
			prepare: 7 * time.Millisecond,
			want:    13 * time.Millisecond,
		},
		{
			name:    "clamp negative duration",
			elapsed: 5 * time.Millisecond,
			prepare: 7 * time.Millisecond,
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := downloadElapsedExcludingInlinePrepare(tt.elapsed, tt.prepare)
			if got != tt.want {
				t.Fatalf("downloadElapsedExcludingInlinePrepare(%s, %s) = %s, want %s", tt.elapsed, tt.prepare, got, tt.want)
			}
		})
	}
}
