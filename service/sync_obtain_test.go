package service

import (
	"testing"
	"time"
)

func TestShardObtainRecorderMergesParallelIntervals(t *testing.T) {
	recorder := newShardObtainRecorder()
	base := time.Unix(100, 0)

	recorder.observe(base, base.Add(5*time.Second))
	recorder.observe(base.Add(time.Second), base.Add(3*time.Second))
	recorder.observe(base.Add(6*time.Second), base.Add(8*time.Second))

	if got := recorder.count(); got != 3 {
		t.Fatalf("interval count = %d, want 3", got)
	}
	if got := recorder.duration(); got != 7*time.Second {
		t.Fatalf("duration = %s, want 7s", got)
	}
}
