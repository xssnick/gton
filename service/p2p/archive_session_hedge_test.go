package p2p

import (
	"context"
	"testing"
)

func TestArchiveSessionLimitsConcurrentFullHedges(t *testing.T) {
	session := (&Node{runCtx: context.Background()}).BeginArchiveSession()
	defer session.Close()

	releases := make([]func(), 0, archiveConcurrentHedgeLimit)
	for range archiveConcurrentHedgeLimit {
		release := session.tryAcquireArchiveHedge()
		if release == nil {
			t.Fatal("archive hedge budget exhausted before its limit")
		}
		releases = append(releases, release)
	}
	if release := session.tryAcquireArchiveHedge(); release != nil {
		release()
		t.Fatal("archive hedge budget allowed an attempt above its limit")
	}

	releases[0]()
	if release := session.tryAcquireArchiveHedge(); release == nil {
		t.Fatal("released archive hedge slot was not reusable")
	} else {
		release()
	}
	for _, release := range releases[1:] {
		release()
	}
}
