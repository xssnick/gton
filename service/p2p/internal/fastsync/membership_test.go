package fastsync

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type fastSyncMembershipTestIssuer struct {
	public  keys.PublicKeyED25519
	private ed25519.PrivateKey
	id      ID
}

func fastSyncTestPeerID(value byte) ID {
	var id ID
	id[len(id)-1] = value
	return id
}

type testRoster struct {
	roots     []ID
	permanent []ID
}

type testMembership struct {
	*Membership
}

func newTestMembership(
	roster testRoster,
	permanentFlags uint32,
) *testMembership {
	return &testMembership{
		Membership: NewMembership(
			roster.roots,
			roster.permanent,
			permanentFlags,
		),
	}
}

func (m *testMembership) UpdateRoster(
	roster testRoster,
	permanentFlags uint32,
	now time.Time,
) {
	m.Membership.updateRoster(
		roster.roots,
		roster.permanent,
		permanentFlags,
		now,
	)
}

type fastSyncMembershipTester interface {
	Helper()
	Fatalf(format string, arguments ...any)
}

var (
	fastSyncMembershipCountSink MembershipCounts
	fastSyncMembershipErrorSink error
)

func TestFastSyncMembershipSlotArbitration(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_000, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x11)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	).Membership

	firstNode := fastSyncTestPeerID(0x40)
	first := fastSyncMembershipTestCertificate(
		t,
		issuer,
		firstNode,
		0,
		7,
		int32(now.Add(100*time.Second).Unix()),
	)
	if err := membership.AuthorizeMember(firstNode, first, now); err != nil {
		t.Fatalf("authorize first slot winner: %v", err)
	}

	olderNode := fastSyncTestPeerID(0x50)
	older := fastSyncMembershipTestCertificate(
		t,
		issuer,
		olderNode,
		0,
		8,
		int32(now.Add(99*time.Second).Unix()),
	)
	if err := membership.AuthorizeMember(olderNode, older, now); !errors.Is(
		err,
		errFastSyncMemberCertificateSuperseded,
	) {
		t.Fatalf("older certificate error = %v", err)
	}

	lowerNode := fastSyncTestPeerID(0x30)
	lower := fastSyncMembershipTestCertificate(
		t,
		issuer,
		lowerNode,
		0,
		9,
		first.ExpireAt,
	)
	if err := membership.AuthorizeMember(lowerNode, lower, now); !errors.Is(
		err,
		errFastSyncMemberCertificateSuperseded,
	) {
		t.Fatalf("lexicographically lower certificate error = %v", err)
	}

	higherNode := fastSyncTestPeerID(0x50)
	higher := fastSyncMembershipTestCertificate(
		t,
		issuer,
		higherNode,
		0,
		10,
		first.ExpireAt,
	)
	if err := membership.AuthorizeMember(higherNode, higher, now); err != nil {
		t.Fatalf("authorize lexicographically higher certificate: %v", err)
	}
	if err := membership.AuthorizeMember(higherNode, higher, now); err != nil {
		t.Fatalf("authorize equal cached winner: %v", err)
	}

	counts := membership.counts()
	if counts.CachedCertificates != 2 {
		t.Fatalf(
			"cached certificates = %d, want 2",
			counts.CachedCertificates,
		)
	}
}

func TestFastSyncMembershipCachedOmittedCertificate(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_100, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x22)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	).Membership
	node := fastSyncTestPeerID(0x21)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		1,
		0xabcdef01,
		int32(now.Add(time.Hour).Unix()),
	)

	const nodeFlags = uint32(0x12345678)
	if err := membership.enrollCertifiedNode(
		node,
		nodeFlags,
		certificate,
		now,
	); err != nil {
		t.Fatalf("enroll certified node: %v", err)
	}
	if !membership.Contains(node, now) {
		t.Fatal("enrolled node is absent")
	}
	if membership.IsPermanent(node) {
		t.Fatal("certified node became permanent")
	}

	flags, err := membership.PeerFlags(node)
	if err != nil {
		t.Fatalf("read node flags: %v", err)
	}
	if flags != nodeFlags {
		t.Fatalf("node flags = %#x, want %#x", flags, nodeFlags)
	}
	if flags == certificate.Flags {
		t.Fatal("node flags were replaced with certificate flags")
	}

	if err = membership.AuthorizeOmitted(node, now); err != nil {
		t.Fatalf("authorize omitted cached certificate: %v", err)
	}

	certificate.Signature[0] ^= 0xff
	issuer.public.Key[0] ^= 0xff

	membership.mu.Lock()
	root := membership.roots[issuer.id]
	root.certificates = newMemberCertificateCache()
	membership.cachedCertificates = 0
	membership.mu.Unlock()

	if err = membership.AuthorizeOmitted(node, now); err != nil {
		t.Fatalf("authorize owned certificate after cache eviction: %v", err)
	}
}

func TestFastSyncMembershipStoredCertificateOnlyAdvances(t *testing.T) {
	now := time.Unix(1_800_000_150, 0)
	firstIssuer := newFastSyncMembershipTestIssuer(t, 0x23)
	incomingIssuer := newFastSyncMembershipTestIssuer(t, 0x24)

	for _, test := range []struct {
		name         string
		expiryChange int32
		wantRetained bool
	}{
		{
			name:         "lower_expiry",
			expiryChange: -1,
			wantRetained: true,
		},
		{
			name:         "equal_expiry",
			wantRetained: true,
		},
		{
			name:         "higher_expiry",
			expiryChange: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := fastSyncTestPeerID(0x25)
			membership := newTestMembership(
				fastSyncMembershipTestRoster(
					[]ID{firstIssuer.id, incomingIssuer.id},
					nil,
				),
				0,
			)
			first := fastSyncMembershipTestCertificate(
				t,
				firstIssuer,
				node,
				0,
				1,
				int32(now.Add(time.Hour).Unix()),
			)
			if err := membership.enrollCertifiedNode(
				node,
				0,
				first,
				now,
			); err != nil {
				t.Fatalf("enroll first certificate: %v", err)
			}

			incoming := fastSyncMembershipTestCertificate(
				t,
				incomingIssuer,
				node,
				0,
				2,
				first.ExpireAt+test.expiryChange,
			)
			if err := membership.AuthorizeMember(
				node,
				incoming,
				now,
			); err != nil {
				t.Fatalf("authorize incoming certificate: %v", err)
			}

			membership.UpdateRoster(
				fastSyncMembershipTestRoster(
					[]ID{firstIssuer.id},
					nil,
				),
				0,
				now,
			)
			if retained := membership.Contains(node, now); retained != test.wantRetained {
				t.Fatalf(
					"node retained = %t, want %t",
					retained,
					test.wantRetained,
				)
			}
			if test.wantRetained {
				if err := membership.AuthorizeOmitted(node, now); err != nil {
					t.Fatalf("authorize retained certificate: %v", err)
				}
			}
		})
	}
}

func TestFastSyncMembershipUnknownValidCertificateDoesNotEnroll(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_200, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x33)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0x31)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		2,
		1,
		int32(now.Add(time.Hour).Unix()),
	)

	if err := membership.AuthorizeMember(node, certificate, now); err != nil {
		t.Fatalf("authorize unknown certified source: %v", err)
	}
	if membership.Contains(node, now) {
		t.Fatal("inbound authorization auto-enrolled an unknown node")
	}
	if err := membership.AuthorizeOmitted(node, now); !errors.Is(
		err,
		errFastSyncMemberCertificateRequired,
	) {
		t.Fatalf("omitted unknown certificate error = %v", err)
	}
	if counts := membership.counts(); counts.Enrolled != 0 {
		t.Fatalf("enrolled nodes = %d, want 0", counts.Enrolled)
	}
}

func TestFastSyncMembershipRetainsRootState(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_300, 0)
	retainedIssuer := newFastSyncMembershipTestIssuer(t, 0x41)
	removedIssuer := newFastSyncMembershipTestIssuer(t, 0x42)
	addedIssuer := newFastSyncMembershipTestIssuer(t, 0x43)
	retainedPermanent := fastSyncTestPeerID(0x61)
	addedPermanent := fastSyncTestPeerID(0x62)
	membership := newTestMembership(
		fastSyncMembershipTestRoster(
			[]ID{retainedIssuer.id, removedIssuer.id},
			[]ID{retainedPermanent},
		),
		0x10,
	)
	if err := membership.updatePermanentNode(
		retainedPermanent,
		0x20,
	); err != nil {
		t.Fatalf("update retained permanent flags: %v", err)
	}

	retainedNode := fastSyncTestPeerID(0x71)
	retainedCertificate := fastSyncMembershipTestCertificate(
		t,
		retainedIssuer,
		retainedNode,
		0,
		1,
		int32(now.Add(time.Hour).Unix()),
	)
	if err := membership.enrollCertifiedNode(
		retainedNode,
		0x31,
		retainedCertificate,
		now,
	); err != nil {
		t.Fatalf("enroll retained-root node: %v", err)
	}

	removedNode := fastSyncTestPeerID(0x72)
	removedCertificate := fastSyncMembershipTestCertificate(
		t,
		removedIssuer,
		removedNode,
		1,
		2,
		int32(now.Add(time.Hour).Unix()),
	)
	if err := membership.enrollCertifiedNode(
		removedNode,
		0x32,
		removedCertificate,
		now,
	); err != nil {
		t.Fatalf("enroll removed-root node: %v", err)
	}

	transitionNode := fastSyncTestPeerID(0x73)
	transitionCertificate := fastSyncMembershipTestCertificate(
		t,
		removedIssuer,
		transitionNode,
		2,
		3,
		int32(now.Add(time.Hour).Unix()),
	)
	if err := membership.enrollCertifiedNode(
		transitionNode,
		0x33,
		transitionCertificate,
		now,
	); err != nil {
		t.Fatalf("enroll future permanent node: %v", err)
	}

	membership.UpdateRoster(
		fastSyncMembershipTestRoster(
			[]ID{retainedIssuer.id, addedIssuer.id},
			[]ID{
				retainedPermanent,
				addedPermanent,
				transitionNode,
			},
		),
		0x40,
		now,
	)

	if !membership.Contains(retainedNode, now) {
		t.Fatal("retained-root node was evicted")
	}
	if membership.Contains(removedNode, now) {
		t.Fatal("removed-root node was retained")
	}
	retainedFlags, err := membership.PeerFlags(retainedPermanent)
	if err != nil {
		t.Fatalf("read retained permanent flags: %v", err)
	}
	if retainedFlags != 0x20 {
		t.Fatalf("retained permanent flags = %#x, want %#x", retainedFlags, 0x20)
	}
	addedFlags, err := membership.PeerFlags(addedPermanent)
	if err != nil {
		t.Fatalf("read added permanent flags: %v", err)
	}
	if addedFlags != 0x40 {
		t.Fatalf("added permanent flags = %#x, want %#x", addedFlags, 0x40)
	}
	transitionFlags, err := membership.PeerFlags(transitionNode)
	if err != nil {
		t.Fatalf("read transitioned permanent flags: %v", err)
	}
	if transitionFlags != 0x33 || !membership.IsPermanent(transitionNode) {
		t.Fatalf(
			"transitioned permanent = %t, flags %#x",
			membership.IsPermanent(transitionNode),
			transitionFlags,
		)
	}

	older := fastSyncMembershipTestCertificate(
		t,
		retainedIssuer,
		fastSyncTestPeerID(0x7f),
		0,
		3,
		retainedCertificate.ExpireAt-1,
	)
	if err = membership.AuthorizeMember(
		fastSyncTestPeerID(0x7f),
		older,
		now,
	); !errors.Is(err, errFastSyncMemberCertificateSuperseded) {
		t.Fatalf("retained slot winner error = %v", err)
	}

	counts := membership.counts()
	if counts.Roots != 2 ||
		counts.Permanent != 3 ||
		counts.Enrolled != 1 ||
		counts.CachedCertificates != 1 {
		t.Fatalf("membership counts after roster update = %+v", counts)
	}
}

func TestFastSyncMembershipPermanentDemotion(t *testing.T) {
	now := time.Unix(1_800_000_350, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x44)
	node := fastSyncTestPeerID(0x74)
	roster := fastSyncMembershipTestRoster(
		[]ID{issuer.id},
		[]ID{node},
	)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		0,
		1,
		int32(now.Add(time.Hour).Unix()),
	)

	t.Run("retains_received_certificate", func(t *testing.T) {
		membership := newTestMembership(roster, 0x10)
		if err := membership.updatePermanentNode(node, 0x20); err != nil {
			t.Fatalf("update permanent flags: %v", err)
		}
		if err := membership.AuthorizeMember(
			node,
			certificate,
			now,
		); err != nil {
			t.Fatalf("authorize permanent certificate: %v", err)
		}

		membership.UpdateRoster(
			fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
			0,
			now,
		)

		if !membership.Contains(node, now) || membership.IsPermanent(node) {
			t.Fatalf(
				"demoted node present = %t, permanent = %t",
				membership.Contains(node, now),
				membership.IsPermanent(node),
			)
		}
		flags, err := membership.PeerFlags(node)
		if err != nil {
			t.Fatalf("read demoted node flags: %v", err)
		}
		if flags != 0x20 {
			t.Fatalf("demoted node flags = %#x, want %#x", flags, 0x20)
		}
		if err = membership.AuthorizeOmitted(node, now); err != nil {
			t.Fatalf("authorize demoted node certificate: %v", err)
		}
	})

	t.Run("roster_refresh_clears_permanent_certificate", func(t *testing.T) {
		membership := newTestMembership(roster, 0)
		if err := membership.AuthorizeMember(
			node,
			certificate,
			now,
		); err != nil {
			t.Fatalf("authorize permanent certificate: %v", err)
		}

		membership.UpdateRoster(roster, 0, now)
		membership.UpdateRoster(
			fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
			0,
			now,
		)

		if membership.Contains(node, now) {
			t.Fatal("demoted node retained a certificate cleared by roster refresh")
		}
	})
}

func TestFastSyncMembershipRateAndLRU(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_400, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x51)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0x51)
	expireAt := int32(now.Add(2 * time.Hour).Unix())

	var firstCertificate overlay.MemberCertificate
	var firstKey memberCertificateCacheKey
	for i := range fastSyncMemberCertificateCheckLimit {
		certificate := fastSyncMembershipTestCertificate(
			t,
			issuer,
			node,
			0,
			uint32(i),
			expireAt,
		)
		if err := membership.AuthorizeMember(
			node,
			certificate,
			now,
		); err != nil {
			t.Fatalf("authorize certificate %d: %v", i, err)
		}
		if i == 0 {
			firstCertificate = certificate
			firstKey = fastSyncMembershipTestCertificateCacheKey(
				t,
				node,
				certificate,
			)
		}
	}

	blockedByRate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		0,
		fastSyncMemberCertificateCheckLimit,
		expireAt,
	)
	if err := membership.AuthorizeMember(
		node,
		blockedByRate,
		now,
	); !errors.Is(err, errFastSyncMemberCertificateRateLimited) {
		t.Fatalf("sixty-first certificate error = %v", err)
	}
	if err := membership.AuthorizeMember(
		node,
		blockedByRate,
		now.Add(fastSyncMemberCertificateCheckWindow),
	); !errors.Is(err, errFastSyncMemberCertificateRateLimited) {
		t.Fatalf("exact-window certificate error = %v", err)
	}
	if err := membership.AuthorizeMember(
		node,
		firstCertificate,
		now,
	); err != nil {
		t.Fatalf("authorize cached oldest certificate: %v", err)
	}

	secondWindow := now.Add(
		fastSyncMemberCertificateCheckWindow + time.Nanosecond,
	)
	for i := fastSyncMemberCertificateCheckLimit; i <= fastSyncMemberCertificateCacheLimit; i++ {
		certificate := fastSyncMembershipTestCertificate(
			t,
			issuer,
			node,
			0,
			uint32(i),
			expireAt,
		)
		if err := membership.AuthorizeMember(
			node,
			certificate,
			secondWindow,
		); err != nil {
			t.Fatalf("authorize second-window certificate %d: %v", i, err)
		}
	}

	counts := membership.counts()
	if counts.CachedCertificates != fastSyncMemberCertificateCacheLimit {
		t.Fatalf(
			"cached certificates = %d, want %d",
			counts.CachedCertificates,
			fastSyncMemberCertificateCacheLimit,
		)
	}

	membership.mu.Lock()
	_, firstCached := membership.roots[issuer.id].certificates.entries[firstKey]
	membership.mu.Unlock()
	if firstCached {
		t.Fatal("oldest certificate survived the bounded lru")
	}
}

func TestFastSyncMembershipBadDirectSourceCooldown(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_500, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x61)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0x81)
	valid := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		3,
		1,
		int32(now.Add(time.Hour).Unix()),
	)
	invalid := valid
	invalid.Signature = bytes.Repeat([]byte{0xff}, ed25519.SignatureSize)

	if err := membership.AuthorizeMember(node, invalid, now); err == nil {
		t.Fatal("bad direct signature was accepted")
	}
	if err := membership.AuthorizeMember(
		node,
		valid,
		now.Add(time.Second),
	); !errors.Is(err, errFastSyncSignatureSourceBlocked) {
		t.Fatalf("blocked direct source error = %v", err)
	}
	if err := membership.AuthorizeMember(
		node,
		valid,
		now.Add(fastSyncBadSignatureSourceCooldown),
	); err != nil {
		t.Fatalf("authorize source after cooldown: %v", err)
	}
	membership.mu.Lock()
	blockedSources := len(membership.blockedSources)
	blockedDeadlines := len(membership.blockedDeadlines)
	membership.mu.Unlock()
	if blockedSources != 0 || blockedDeadlines != 0 {
		t.Fatalf(
			"expired source block state = sources %d, deadlines %d",
			blockedSources,
			blockedDeadlines,
		)
	}

	nodeV2Source := fastSyncTestPeerID(0x82)
	nodeV2Valid := fastSyncMembershipTestCertificate(
		t,
		issuer,
		nodeV2Source,
		4,
		2,
		int32(now.Add(time.Hour).Unix()),
	)
	nodeV2Invalid := nodeV2Valid
	nodeV2Invalid.Signature = bytes.Repeat(
		[]byte{0xee},
		ed25519.SignatureSize,
	)
	if err := membership.enrollCertifiedNode(
		nodeV2Source,
		0,
		nodeV2Invalid,
		now,
	); err == nil {
		t.Fatal("bad forwarded node signature was accepted")
	}
	if err := membership.AuthorizeMember(
		nodeV2Source,
		nodeV2Valid,
		now,
	); err != nil {
		t.Fatalf("forwarded bad signature blocked direct source: %v", err)
	}
}

func TestFastSyncMembershipRejectsPointerIssuer(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_600, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x71)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0x91)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		0,
		1,
		int32(now.Add(time.Hour).Unix()),
	)
	certificate.IssuedBy = &issuer.public

	err := membership.AuthorizeMember(node, certificate, now)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("pointer issuer error = %v", err)
	}
}

func TestFastSyncMembershipCertificateExpiryGrace(t *testing.T) {
	t.Parallel()

	expireAt := int32(1_800_000_700)
	issuer := newFastSyncMembershipTestIssuer(t, 0x72)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0x92)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		0,
		1,
		expireAt,
	)
	graceBoundary := time.Unix(int64(expireAt), 0).Add(3 * time.Second)

	if err := membership.AuthorizeMember(
		node,
		certificate,
		graceBoundary,
	); err != nil {
		t.Fatalf("authorize at expiry grace boundary: %v", err)
	}
	if err := membership.AuthorizeMember(
		node,
		certificate,
		graceBoundary.Add(time.Nanosecond),
	); err == nil {
		t.Fatal("certificate survived beyond expiry grace")
	}
}

func TestFastSyncMembershipPermanentOmittedCertificate(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_700, 0)
	permanent := fastSyncTestPeerID(0xa1)
	unknown := fastSyncTestPeerID(0xa2)
	membership := newTestMembership(
		fastSyncMembershipTestRoster(nil, []ID{permanent}),
		0x11,
	)

	if err := membership.AuthorizeOmitted(permanent, now); err != nil {
		t.Fatalf("authorize permanent omitted certificate: %v", err)
	}
	if err := membership.AuthorizeOmitted(
		unknown,
		now,
	); !errors.Is(err, errFastSyncMemberCertificateRequired) {
		t.Fatalf("unknown omitted certificate error = %v", err)
	}
	if err := membership.updatePermanentNode(
		unknown,
		0x22,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown permanent enrollment error = %v", err)
	}
}

func TestFastSyncMembershipCountsAllocations(t *testing.T) {
	membership := newTestMembership(
		fastSyncMembershipTestRoster(
			nil,
			[]ID{fastSyncTestPeerID(0xb1)},
		),
		0,
	)

	allocations := testing.AllocsPerRun(1_000, func() {
		fastSyncMembershipCountSink = membership.counts()
	})
	if allocations != 0 {
		t.Fatalf("membership count allocations = %f, want 0", allocations)
	}
}

func TestFastSyncMembershipCachedAuthorizationAllocations(t *testing.T) {
	now := time.Unix(1_800_000_800, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x73)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0xb2)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		0,
		1,
		int32(now.Add(time.Hour).Unix()),
	)
	if err := membership.enrollCertifiedNode(
		node,
		0,
		certificate,
		now,
	); err != nil {
		t.Fatalf("enroll cached allocation node: %v", err)
	}

	allocations := testing.AllocsPerRun(1_000, func() {
		fastSyncMembershipErrorSink = membership.AuthorizeOmitted(node, now)
	})
	if allocations != 0 {
		t.Fatalf(
			"cached membership authorization allocations = %f, want 0",
			allocations,
		)
	}
	if fastSyncMembershipErrorSink != nil {
		t.Fatalf(
			"cached membership authorization error: %v",
			fastSyncMembershipErrorSink,
		)
	}
}

func BenchmarkFastSyncMembershipAuthorizeOmitted(b *testing.B) {
	now := time.Unix(1_800_000_900, 0)
	issuer := newFastSyncMembershipTestIssuer(b, 0x74)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	).Membership
	node := fastSyncTestPeerID(0xb3)
	certificate := fastSyncMembershipTestCertificate(
		b,
		issuer,
		node,
		0,
		1,
		int32(now.Add(time.Hour).Unix()),
	)
	if err := membership.enrollCertifiedNode(
		node,
		0,
		certificate,
		now,
	); err != nil {
		b.Fatalf("enroll benchmark node: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		fastSyncMembershipErrorSink = membership.AuthorizeOmitted(node, now)
	}
	if fastSyncMembershipErrorSink != nil {
		b.Fatalf(
			"cached membership authorization error: %v",
			fastSyncMembershipErrorSink,
		)
	}
}

// Senders attach their certificate to every packet, so this is the hot path for
// any peer that is not using the omitted form.
func BenchmarkFastSyncMembershipAuthorizeMember(b *testing.B) {
	now := time.Unix(1_800_000_900, 0)
	issuer := newFastSyncMembershipTestIssuer(b, 0x75)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	).Membership
	node := fastSyncTestPeerID(0xb4)
	certificate := fastSyncMembershipTestCertificate(
		b,
		issuer,
		node,
		0,
		1,
		int32(now.Add(time.Hour).Unix()),
	)
	if err := membership.enrollCertifiedNode(
		node,
		0,
		certificate,
		now,
	); err != nil {
		b.Fatalf("enroll benchmark node: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		fastSyncMembershipErrorSink = membership.AuthorizeMember(
			node,
			certificate,
			now,
		)
	}
	if fastSyncMembershipErrorSink != nil {
		b.Fatalf(
			"member authorization error: %v",
			fastSyncMembershipErrorSink,
		)
	}
}

func newFastSyncMembershipTestIssuer(
	t fastSyncMembershipTester,
	value byte,
) fastSyncMembershipTestIssuer {
	t.Helper()

	private := ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{value}, ed25519.SeedSize),
	)
	public := keys.PublicKeyED25519{
		Key: ed25519.PublicKey(
			slices.Clone([]byte(private[ed25519.SeedSize:])),
		),
	}
	rawID, err := tl.Hash(public)
	if err != nil {
		t.Fatalf("hash membership test issuer: %v", err)
	}
	id, err := idFromBytes(rawID)
	if err != nil {
		t.Fatalf("parse membership test issuer id: %v", err)
	}

	return fastSyncMembershipTestIssuer{
		public:  public,
		private: private,
		id:      id,
	}
}

func fastSyncMembershipTestRoster(
	roots,
	permanent []ID,
) testRoster {
	return testRoster{
		roots:     slices.Clone(roots),
		permanent: slices.Clone(permanent),
	}
}

func fastSyncMembershipTestCertificate(
	t fastSyncMembershipTester,
	issuer fastSyncMembershipTestIssuer,
	node ID,
	slot int32,
	flags uint32,
	expireAt int32,
) overlay.MemberCertificate {
	t.Helper()

	certificate := overlay.MemberCertificate{
		IssuedBy: issuer.public,
		Flags:    flags,
		Slot:     slot,
		ExpireAt: expireAt,
	}
	toSign, err := tl.Serialize(overlay.MemberCertificateID{
		Node:     node[:],
		Flags:    certificate.Flags,
		Slot:     certificate.Slot,
		ExpireAt: certificate.ExpireAt,
	}, true)
	if err != nil {
		t.Fatalf("serialize membership test certificate: %v", err)
	}
	certificate.Signature = ed25519.Sign(issuer.private, toSign)
	return certificate
}

func fastSyncMembershipTestCertificateCacheKey(
	t fastSyncMembershipTester,
	node ID,
	certificate overlay.MemberCertificate,
) memberCertificateCacheKey {
	t.Helper()

	encoded, err := tl.Serialize(certificate, true)
	if err != nil {
		t.Fatalf("serialize membership test certificate key: %v", err)
	}
	return memberCertificateCacheKey{
		node: node,
		hash: sha256.Sum256(encoded),
	}
}

// Enrollment is not authorization forever: the QUIC path already rejects a peer
// whose certificate expired, and Contains gates the ADNL/RLDP path, so answering
// "member" from the map alone kept serving a de-authorized node until the next
// roster update - hours, on a stable roster.
func TestFastSyncMembershipDropsExpiredEnrollment(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_900, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x31)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0x32)
	expireAt := now.Add(time.Hour)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		1,
		0,
		int32(expireAt.Unix()),
	)

	if err := membership.enrollCertifiedNode(node, 0, certificate, now); err != nil {
		t.Fatalf("enroll certified node: %v", err)
	}
	if !membership.Contains(node, now) {
		t.Fatal("freshly enrolled node is absent")
	}

	// Past the expiry and past overlay's expiry grace.
	after := expireAt.Add(time.Minute)
	if membership.Contains(node, after) {
		t.Fatal("expired enrollment still counts as membership")
	}
	// The QUIC path must agree, so the two doors cannot disagree about a peer.
	if err := membership.AuthorizeOmitted(node, after); err == nil {
		t.Fatal("expired enrollment passed the certificate check")
	}
}

// A permanent member has no certificate to expire.
func TestFastSyncMembershipPermanentIgnoresExpiry(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_950, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x33)
	membership := newTestMembership(
		fastSyncMembershipTestRoster([]ID{issuer.id}, nil),
		0,
	)
	node := fastSyncTestPeerID(0x34)
	membership.UpdateRoster(
		fastSyncMembershipTestRoster([]ID{issuer.id}, []ID{node}),
		0,
		now,
	)
	if err := membership.updatePermanentNode(node, 0); err != nil {
		t.Fatalf("enroll permanent node: %v", err)
	}

	if !membership.Contains(node, now.Add(1000*time.Hour)) {
		t.Fatal("permanent member expired")
	}
}
