package p2p

import (
	"strings"
	"testing"
)

func TestArchivePackDownloadLimitRejectsEndlessFullSlices(t *testing.T) {
	var offset int64
	fullSlices := int(archivePackMaxBytes / archiveSliceSize)
	for i := 0; i < fullSlices; i++ {
		if err := checkArchivePackDownloadSize(offset, archiveSliceSize); err != nil {
			t.Fatalf("slice %d rejected before limit: %v", i, err)
		}
		offset += archiveSliceSize
	}
	if offset != archivePackMaxBytes {
		t.Fatalf("test setup reached offset=%d want=%d", offset, archivePackMaxBytes)
	}

	err := checkArchivePackDownloadSize(offset, archiveSliceSize)
	if err == nil || !strings.Contains(err.Error(), "exceeds max size") {
		t.Fatalf("next full slice error = %v, want max size rejection", err)
	}
}
