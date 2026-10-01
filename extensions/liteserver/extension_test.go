package liteserver

import (
	"testing"

	"github.com/xssnick/gton/internal/metrics"
)

func TestExtensionQueryObserverKeepsUntypedCompatibility(t *testing.T) {
	var typedNil *metrics.Metrics
	for name, capability := range map[string]any{
		"nil":         nil,
		"typed nil":   typedNil,
		"unsupported": struct{}{},
	} {
		t.Run(name, func(t *testing.T) {
			observer, err := extensionQueryObserver(capability)
			if err != nil {
				t.Fatalf("create extension query observer: %v", err)
			}
			if observer != nil {
				t.Fatal("expected no observer for unsupported extension metrics capability")
			}
		})
	}
}
