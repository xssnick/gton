package p2p

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type fastSyncCertificateTestStorage struct {
	mx        sync.Mutex
	snapshot  []byte
	loadErr   error
	saveErr   error
	deleteErr error
	saves     int
	deletes   int
}

func (s *fastSyncCertificateTestStorage) SaveFastSyncCertificateSnapshot(
	ctx context.Context,
	snapshot []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mx.Lock()
	defer s.mx.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.snapshot = bytes.Clone(snapshot)
	s.saves++
	return nil
}

func (s *fastSyncCertificateTestStorage) FastSyncCertificateSnapshot(
	ctx context.Context,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mx.Lock()
	defer s.mx.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	if s.snapshot == nil {
		return nil, storage.ErrNotFound
	}
	return bytes.Clone(s.snapshot), nil
}

func (s *fastSyncCertificateTestStorage) DeleteFastSyncCertificateSnapshot(
	ctx context.Context,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mx.Lock()
	defer s.mx.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.snapshot = nil
	s.deletes++
	return nil
}

func TestFastSyncCertificateSnapshotCodec(t *testing.T) {
	now := time.Unix(1_800_010_000, 0)
	node := fastSyncTestPeerID(0x41)
	firstIssuer := newFastSyncMembershipTestIssuer(t, 0x42)
	secondIssuer := newFastSyncMembershipTestIssuer(t, 0x43)
	certificates := []overlay.MemberCertificate{
		fastSyncMembershipTestCertificate(
			t,
			firstIssuer,
			node,
			1,
			0,
			int32(now.Add(time.Hour).Unix()),
		),
		fastSyncMembershipTestCertificate(
			t,
			secondIssuer,
			node,
			2,
			0,
			int32(now.Add(2*time.Hour).Unix()),
		),
	}

	raw, err := encodeFastSyncCertificateSnapshot(certificates)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	decoded, err := decodeFastSyncCertificateSnapshot(raw)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if len(decoded) != len(certificates) {
		t.Fatalf("decoded certificates = %d, want %d", len(decoded), len(certificates))
	}
	for i := range certificates {
		if decoded[i].ExpireAt != certificates[i].ExpireAt ||
			decoded[i].Slot != certificates[i].Slot {
			t.Fatalf("decoded certificate %d = %+v, want %+v", i, decoded[i], certificates[i])
		}
	}
}

func TestFastSyncCertificateSnapshotRejectsMalformedData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty"},
		{name: "version", data: []byte{fastSyncCertificateSnapshotVersion + 1}},
		{name: "missing count", data: []byte{fastSyncCertificateSnapshotVersion}},
		{name: "zero count", data: []byte{fastSyncCertificateSnapshotVersion, 0}},
		{name: "missing entry", data: []byte{fastSyncCertificateSnapshotVersion, 1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeFastSyncCertificateSnapshot(test.data); err == nil {
				t.Fatal("malformed snapshot decoded successfully")
			}
		})
	}
}

func TestFastSyncCertificatePersistsAndLoadsAfterRestart(t *testing.T) {
	now := time.Unix(1_800_011_000, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x51)
	node := fastSyncCertificateTestNode(t, issuer)
	store := &fastSyncCertificateTestStorage{}
	node.fastSyncCertificateStorage = store

	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node.localID,
		1,
		0,
		int32(now.Add(time.Hour).Unix()),
	)
	if err := node.importFastSyncCertificate(certificate, now); err != nil {
		t.Fatalf("import certificate: %v", err)
	}

	loaded, err := loadFastSyncCertificates(
		context.Background(),
		store,
		node.localID,
		now,
	)
	if err != nil {
		t.Fatalf("load persisted certificates: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded certificates = %d, want 1", len(loaded))
	}
	if loaded[0].ExpireAt != certificate.ExpireAt {
		t.Fatalf("loaded expiry = %d, want %d", loaded[0].ExpireAt, certificate.ExpireAt)
	}
	if store.saves != 1 {
		t.Fatalf("snapshot saves = %d, want 1", store.saves)
	}
}

func TestLoadFastSyncCertificatesCompactsInvalidEntries(t *testing.T) {
	now := time.Unix(1_800_012_000, 0)
	localID := fastSyncTestPeerID(0x61)
	otherID := fastSyncTestPeerID(0x62)
	validIssuer := newFastSyncMembershipTestIssuer(t, 0x63)
	expiredIssuer := newFastSyncMembershipTestIssuer(t, 0x64)
	wrongNodeIssuer := newFastSyncMembershipTestIssuer(t, 0x65)
	certificates := []overlay.MemberCertificate{
		fastSyncMembershipTestCertificate(
			t,
			validIssuer,
			localID,
			1,
			0,
			int32(now.Add(time.Hour).Unix()),
		),
		fastSyncMembershipTestCertificate(
			t,
			expiredIssuer,
			localID,
			2,
			0,
			int32(now.Add(-10*time.Second).Unix()),
		),
		fastSyncMembershipTestCertificate(
			t,
			wrongNodeIssuer,
			otherID,
			3,
			0,
			int32(now.Add(time.Hour).Unix()),
		),
	}
	raw, err := encodeFastSyncCertificateSnapshot(certificates)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	store := &fastSyncCertificateTestStorage{snapshot: raw}

	loaded, err := loadFastSyncCertificates(
		context.Background(),
		store,
		localID,
		now,
	)
	if err != nil {
		t.Fatalf("load certificates: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded certificates = %d, want 1", len(loaded))
	}
	if loaded[0].ExpireAt != certificates[0].ExpireAt {
		t.Fatalf("loaded expiry = %d, want %d", loaded[0].ExpireAt, certificates[0].ExpireAt)
	}
	if store.saves != 1 {
		t.Fatalf("compacted snapshot saves = %d, want 1", store.saves)
	}

	compacted, err := decodeFastSyncCertificateSnapshot(store.snapshot)
	if err != nil {
		t.Fatalf("decode compacted snapshot: %v", err)
	}
	if len(compacted) != 1 {
		t.Fatalf("compacted certificates = %d, want 1", len(compacted))
	}
}

func TestLoadFastSyncCertificatesDeletesExpiredSnapshot(t *testing.T) {
	now := time.Unix(1_800_013_000, 0)
	localID := fastSyncTestPeerID(0x71)
	issuer := newFastSyncMembershipTestIssuer(t, 0x72)
	raw, err := encodeFastSyncCertificateSnapshot([]overlay.MemberCertificate{
		fastSyncMembershipTestCertificate(
			t,
			issuer,
			localID,
			1,
			0,
			int32(now.Add(-10*time.Second).Unix()),
		),
	})
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	store := &fastSyncCertificateTestStorage{snapshot: raw}

	loaded, err := loadFastSyncCertificates(
		context.Background(),
		store,
		localID,
		now,
	)
	if err != nil {
		t.Fatalf("load certificates: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("loaded certificates = %d, want 0", len(loaded))
	}
	if store.deletes != 1 {
		t.Fatalf("snapshot deletes = %d, want 1", store.deletes)
	}
}

func TestFastSyncCertificatePersistenceFailureKeepsMemoryUnchanged(t *testing.T) {
	now := time.Unix(1_800_014_000, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0x81)
	node := fastSyncCertificateTestNode(t, issuer)
	node.fastSyncCertificateStorage = &fastSyncCertificateTestStorage{
		saveErr: errors.New("write failed"),
	}
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node.localID,
		1,
		0,
		int32(now.Add(time.Hour).Unix()),
	)

	if err := node.importFastSyncCertificate(certificate, now); err == nil {
		t.Fatal("import succeeded despite persistence failure")
	}
	if got := len(node.fastSyncCertificateSnapshot()); got != 0 {
		t.Fatalf("in-memory certificates = %d, want 0", got)
	}
}
