package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	fastSyncCertificateSnapshotVersion = 1
	fastSyncCertificateMaxEncodedSize  = 4 << 10
)

func encodeFastSyncCertificateSnapshot(
	certificates []overlay.MemberCertificate,
) ([]byte, error) {
	if len(certificates) == 0 {
		return nil, nil
	}
	if len(certificates) > fastSyncCertificateLimit {
		return nil, fmt.Errorf(
			"too many fast sync certificates: %d",
			len(certificates),
		)
	}

	data := []byte{fastSyncCertificateSnapshotVersion}
	data = binary.AppendUvarint(data, uint64(len(certificates)))
	for i := range certificates {
		encoded, err := tl.Serialize(certificates[i], true)
		if err != nil {
			return nil, fmt.Errorf(
				"serialize fast sync certificate %d: %w",
				i,
				err,
			)
		}
		if len(encoded) > fastSyncCertificateMaxEncodedSize {
			return nil, fmt.Errorf(
				"fast sync certificate %d is too large: %d",
				i,
				len(encoded),
			)
		}

		data = binary.AppendUvarint(data, uint64(len(encoded)))
		data = append(data, encoded...)
	}
	return data, nil
}

func decodeFastSyncCertificateSnapshot(
	data []byte,
) ([]overlay.MemberCertificate, error) {
	if len(data) == 0 {
		return nil, errors.New("empty fast sync certificate snapshot")
	}
	if data[0] != fastSyncCertificateSnapshotVersion {
		return nil, fmt.Errorf(
			"unsupported fast sync certificate snapshot version %d",
			data[0],
		)
	}
	data = data[1:]

	count, n := binary.Uvarint(data)
	if n <= 0 || count == 0 || count > fastSyncCertificateLimit {
		return nil, errors.New("malformed fast sync certificate snapshot header")
	}
	data = data[n:]

	certificates := make([]overlay.MemberCertificate, 0, count)
	for i := uint64(0); i < count; i++ {
		size, sizeBytes := binary.Uvarint(data)
		if sizeBytes <= 0 ||
			size == 0 ||
			size > fastSyncCertificateMaxEncodedSize ||
			uint64(len(data)-sizeBytes) < size {
			return nil, fmt.Errorf(
				"malformed fast sync certificate snapshot entry %d",
				i,
			)
		}
		data = data[sizeBytes:]
		encoded := data[:size]
		data = data[size:]

		var certificate overlay.MemberCertificate
		rest, err := tl.Parse(&certificate, encoded, true)
		if err != nil {
			return nil, fmt.Errorf(
				"parse fast sync certificate %d: %w",
				i,
				err,
			)
		}
		if len(rest) != 0 {
			return nil, fmt.Errorf(
				"fast sync certificate %d has %d trailing bytes",
				i,
				len(rest),
			)
		}
		certificates = append(certificates, certificate)
	}
	if len(data) != 0 {
		return nil, fmt.Errorf(
			"fast sync certificate snapshot has %d trailing bytes",
			len(data),
		)
	}
	return certificates, nil
}

func loadFastSyncCertificates(
	ctx context.Context,
	store storage.FastSyncCertificateStorage,
	localID PeerID,
	now time.Time,
) ([]overlay.MemberCertificate, error) {
	if store == nil {
		return nil, nil
	}

	raw, err := store.FastSyncCertificateSnapshot(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load fast sync certificate snapshot: %w", err)
	}

	loaded, err := decodeFastSyncCertificateSnapshot(raw)
	if err != nil {
		return nil, fmt.Errorf("decode fast sync certificate snapshot: %w", err)
	}

	changed := false
	prepared := make([]overlay.MemberCertificate, 0, len(loaded))
	byIssuer := make(map[PeerID]int, len(loaded))
	for i := range loaded {
		certificate := loaded[i]
		if certificate.IsExpired(now) {
			changed = true
			continue
		}
		if err = certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
			changed = true
			continue
		}
		if err = certificate.CheckSignature(localID[:]); err != nil {
			changed = true
			continue
		}

		issuerRaw, issuerErr := certificate.IssuerID()
		if issuerErr != nil {
			changed = true
			continue
		}
		issuer, issuerErr := NewPeerID(issuerRaw)
		if issuerErr != nil {
			changed = true
			continue
		}

		cloned, cloneErr := cloneFastSyncCertificate(certificate)
		if cloneErr != nil {
			changed = true
			continue
		}
		if existing, ok := byIssuer[issuer]; ok {
			changed = true
			if prepared[existing].ExpireAt < cloned.ExpireAt {
				prepared[existing] = cloned
			}
			continue
		}

		byIssuer[issuer] = len(prepared)
		prepared = append(prepared, cloned)
	}

	if changed {
		if err = persistFastSyncCertificateSnapshot(ctx, store, prepared); err != nil {
			return nil, fmt.Errorf("compact fast sync certificate snapshot: %w", err)
		}
	}
	return prepared, nil
}

func (n *Node) persistFastSyncCertificates(
	ctx context.Context,
	certificates []overlay.MemberCertificate,
) error {
	return persistFastSyncCertificateSnapshot(
		ctx,
		n.fastSyncCertificateStorage,
		certificates,
	)
}

func persistFastSyncCertificateSnapshot(
	ctx context.Context,
	store storage.FastSyncCertificateStorage,
	certificates []overlay.MemberCertificate,
) error {
	if store == nil {
		return nil
	}
	if len(certificates) == 0 {
		if err := store.DeleteFastSyncCertificateSnapshot(ctx); err != nil {
			return fmt.Errorf("delete fast sync certificate snapshot: %w", err)
		}
		return nil
	}

	raw, err := encodeFastSyncCertificateSnapshot(certificates)
	if err != nil {
		return err
	}
	if err = store.SaveFastSyncCertificateSnapshot(ctx, raw); err != nil {
		return fmt.Errorf("save fast sync certificate snapshot: %w", err)
	}
	return nil
}
