package service

import "testing"

func TestFormatByteSize(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{bytes: 0, want: "0 B"},
		{bytes: 1023, want: "1023 B"},
		{bytes: 1024, want: "1.0 KiB"},
		{bytes: 1536, want: "1.5 KiB"},
		{bytes: 10 << 20, want: "10 MiB"},
	}

	for _, test := range tests {
		if got := formatByteSize(test.bytes); got != test.want {
			t.Fatalf("formatByteSize(%d) = %q, want %q", test.bytes, got, test.want)
		}
	}
}
