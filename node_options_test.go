package gton

import (
	"testing"

	"github.com/xssnick/gton/service"
)

func TestRunNodeRejectsNegativeArchivePrefetchWindows(t *testing.T) {
	err := RunNode(t.Context(), NodeOptions{ArchivePrefetchWindows: -1})
	if err == nil {
		t.Fatal("expected negative archive prefetch windows error")
	}
	if err.Error() != "archive prefetch windows cannot be negative: -1" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyNodeOptionDefaultsUsesArchivePrefetchDefault(t *testing.T) {
	opts := applyNodeOptionDefaults(NodeOptions{})

	if opts.ArchivePrefetchWindows != service.DefaultArchiveCatchUpPrefetchWindows {
		t.Fatalf("archive prefetch windows = %d, want %d", opts.ArchivePrefetchWindows, service.DefaultArchiveCatchUpPrefetchWindows)
	}
}
