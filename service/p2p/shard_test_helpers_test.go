package p2p

import (
	"testing"

	sharddomain "github.com/xssnick/gton/service/shard"
)

func testShardAncestor(t testing.TB, id int64, depth uint32) int64 {
	t.Helper()
	ancestor, err := sharddomain.Ancestor(id, depth)
	if err != nil {
		t.Fatalf("shard ancestor %016x at depth %d: %v", uint64(id), depth, err)
	}
	return ancestor
}
