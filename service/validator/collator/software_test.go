package collator

import "testing"

func TestSupportedSoftwareMatchesReferenceCollator(t *testing.T) {
	software := SupportedSoftware()
	if software.Version != 15 {
		t.Fatalf("supported version = %d, want 15", software.Version)
	}
	if software.Capabilities != 0x3ee {
		t.Fatalf("supported capabilities = %#x, want 0x3ee", software.Capabilities)
	}
}
