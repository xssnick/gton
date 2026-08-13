package msgpool

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestParseBOCBasics(t *testing.T) {
	addr := testAddr(0x11)
	body := bodyWithTag(7)
	m := buildExtMsg(t, 0, addr, body, msgOpts{importFee: 5})

	msg, err := newExternalMessage(len(m.raw), m.root, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, msg.Workchain, int32(0), "workchain")
	requireEqual(t, msg.Addr, addr, "addr")
	requireEqual(t, msg.AddrPrefix, uint64(0x1111111111111111), "prefix")
	if !bytes.Equal(msg.Hash[:], msg.Root.Hash()) {
		t.Fatal("raw hash must be the root hash")
	}
	if msg.Hash == msg.HashNorm {
		t.Fatal("normalized hash must differ from raw hash for a message with import fee")
	}
}

func TestExternalMessageRoutingUsesAnycastRewrite(t *testing.T) {
	rawAddr := testAddr(0x11)
	destination := addrOf(0, rawAddr).WithAnycast(address.NewAnycast(13, []byte{0xab, 0xc8}))
	root := cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreUInt(0b00, 2).
		MustStoreAddr(destination).
		MustStoreCoins(0).
		MustStoreUInt(0, 1).
		MustStoreUInt(1, 1).
		MustStoreRef(bodyWithTag(1)).
		EndCell()

	message, err := newExternalMessage(0, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := rawAddr
	if err = RewriteAnycast(want[:], destination); err != nil {
		t.Fatal(err)
	}
	if message.Addr != want {
		t.Fatalf("routing address = %x, want rewritten %x", message.Addr, want)
	}
}

// TestNormalizedHashInvariance: messages differing only in import fee or
// body placement (inline vs ref) normalize to the same hash, while dest or
// body changes produce different hashes.
func TestNormalizedHashInvariance(t *testing.T) {
	addr := testAddr(0x22)
	body := bodyWithTag(99)

	base := assembleMsg(t, buildExtMsg(t, 0, addr, body, msgOpts{}))
	withFee := assembleMsg(t, buildExtMsg(t, 0, addr, body, msgOpts{importFee: 1000}))
	inline := assembleMsg(t, buildExtMsg(t, 0, addr, body, msgOpts{inlineBody: true}))

	if base.Hash == withFee.Hash || base.Hash == inline.Hash {
		t.Fatal("raw hashes must differ")
	}
	requireEqual(t, withFee.HashNorm, base.HashNorm, "import fee must not affect the normalized hash")
	requireEqual(t, inline.HashNorm, base.HashNorm, "body placement must not affect the normalized hash")

	otherBody := assembleMsg(t, buildExtMsg(t, 0, addr, bodyWithTag(100), msgOpts{}))
	otherAddr := assembleMsg(t, buildExtMsg(t, 0, testAddr(0x23), body, msgOpts{}))
	if otherBody.HashNorm == base.HashNorm || otherAddr.HashNorm == base.HashNorm {
		t.Fatal("body/dest changes must change the normalized hash")
	}

	// Structural cross-check of the normalized layout.
	manual := cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreUInt(0b00, 2).
		MustStoreAddr(addrOf(0, addr)).
		MustStoreCoins(0).
		MustStoreUInt(0b01, 2).
		MustStoreRef(body).
		EndCell()
	var want [32]byte
	copy(want[:], manual.Hash())
	requireEqual(t, base.HashNorm, want, "normalized layout")
}

func TestAssembleRejects(t *testing.T) {
	addr := testAddr(0x31)
	// Internal message header must be rejected.
	intMsg := cell.BeginCell().
		MustStoreUInt(0b0, 1). // int_msg_info$0...
		MustStoreUInt(0, 3).
		MustStoreAddr(addrOf(0, addr)).
		MustStoreAddr(addrOf(0, addr)).
		MustStoreCoins(1).
		EndCell()
	if _, err := newExternalMessage(0, intMsg, nil); err == nil {
		t.Fatal("int_msg_info must fail")
	}
}

// TestEstimatedSizeTracksTheBOC pins the structural estimate used when the
// caller has no received length — which is what both production callers do.
// The budget it feeds is a heuristic, so the estimate only has to stay close
// to and never above the real serialization: it counts cell payloads and skips
// the fixed BOC header.
func TestEstimatedSizeTracksTheBOC(t *testing.T) {
	for _, inline := range []bool{false, true} {
		m := buildExtMsg(t, 0, testAddr(0x41), bodyWithTag(3), msgOpts{inlineBody: inline})
		estimated := estimateBOCSize(m.root)
		if estimated <= 0 || estimated > len(m.raw) || estimated < len(m.raw)-32 {
			t.Fatalf("inline=%v: estimate %d is not close under the BOC length %d", inline, estimated, len(m.raw))
		}
	}

	// A shared subtree is charged once, exactly as the BOC stores it. The two
	// trees below differ only in that the second references the same body
	// twice, so the whole difference must be the one byte of the extra
	// reference index — the body payload itself may not be charged again.
	// Asserting the exact delta is what pins the dedup: without it the second
	// tree would additionally carry the body's own 2+len(body) bytes, and a
	// bound like "estimate <= len(BOC)" would still pass.
	body := bodyWithTag(9)
	once := cell.BeginCell().MustStoreRef(body).EndCell()
	twice := cell.BeginCell().MustStoreRef(body).MustStoreRef(body).EndCell()

	delta := estimateBOCSize(twice) - estimateBOCSize(once)
	if delta != 1 {
		t.Fatalf("shared body charged twice: estimate grew by %d bytes for one extra reference to the same cell, want 1", delta)
	}
}

// TestBytesBudgetWithoutReceivedLength covers the production shape of
// AddExternal: no raw buffer, so the byte budget runs on the structural
// estimate alone.
func TestBytesBudgetWithoutReceivedLength(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) { c.MempoolBytesLimit = 100 })
	for _, fill := range []byte{0x51, 0x52, 0x53} {
		m := buildExtMsg(t, 0, testAddr(fill), bodyWithTag(1), msgOpts{})
		if err := env.pool.AddExternal(nil, m.root, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	st := env.pool.Stats()
	if st.PooledBytes <= 0 {
		t.Fatal("pooling without a received length must still account bytes")
	}
	if st.OverflowBytes == 0 || st.PooledBytes > 100 {
		t.Fatalf("bytes budget did not hold: pooled=%d overflow=%d", st.PooledBytes, st.OverflowBytes)
	}
}

func TestShardContains(t *testing.T) {
	all := ShardIdent{Workchain: 0, Shard: ShardAll}
	left := ShardIdent{Workchain: 0, Shard: 0x4000000000000000}  // prefixes 0xxxxxxx...
	right := ShardIdent{Workchain: 0, Shard: 0xC000000000000000} // prefixes 1xxxxxxx...
	mc := ShardIdent{Workchain: -1, Shard: ShardAll}

	loPrefix := uint64(0x1111111111111111)
	hiPrefix := uint64(0x9111111111111111)

	requireEqual(t, all.Contains(0, loPrefix), true, "all lo")
	requireEqual(t, all.Contains(0, hiPrefix), true, "all hi")
	requireEqual(t, all.Contains(-1, loPrefix), false, "workchain mismatch")

	requireEqual(t, left.Contains(0, loPrefix), true, "left lo")
	requireEqual(t, left.Contains(0, hiPrefix), false, "left hi")
	requireEqual(t, right.Contains(0, loPrefix), false, "right lo")
	requireEqual(t, right.Contains(0, hiPrefix), true, "right hi")

	requireEqual(t, mc.Contains(-1, loPrefix), true, "mc")
	requireEqual(t, mc.Contains(0, loPrefix), false, "mc wc")
}
