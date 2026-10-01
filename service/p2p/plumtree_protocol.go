package p2p

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/tonutils-go/tl"
)

const (
	plumtreeMaxPayloadSize int32 = 16 << 20
	plumtreeFECDataParts   int32 = 30
	plumtreeFECTotalParts  int32 = 45
	plumtreeTreeSlots      int32 = 46
	plumtreeSimpleTree     int32 = 0
	plumtreeFECTreeOffset  int32 = 1

	// Timestamp acceptance window. Must stay at the upstream value
	// (PLUMTREE_BROADCAST_LIFETIME = 20.0 in overlay/broadcast-plumtree.cpp): it is
	// the clock-skew tolerance, so shrinking it silently rejects broadcasts every
	// C++ node accepts. State retention is separate - see the two TTLs below.
	plumtreeBroadcastLifetime = 20 * time.Second
	// How long an assembled broadcast keeps its parts to answer repair queries. A
	// neighbour asks within its own repair delay, and plumtreeRepairTimeout is 5s.
	plumtreeDeliveredStateTTL = 5 * time.Second
	// Longer than the delivered case: an unassembled broadcast still has repairs to
	// attempt, and each may take plumtreeRepairTimeout.
	plumtreePendingStateTTL = 10 * time.Second

	broadcastNotFoundSchema  = "overlay.broadcastNotFound = overlay.Broadcast"
	repairPlumtreePartSchema = "overlay.repairPlumtreePart broadcast_id:int256 timestamp:double " +
		"part_index:int tree_index:int = overlay.Broadcast"
	broadcastPlumtreeSimpleSchema = "overlay.broadcastPlumtreeSimple flags:int timestamp:double " +
		"src:PublicKey certificate:overlay.Certificate broadcast_id:int256 tree_index:int data:bytes " +
		"signature:bytes = overlay.Broadcast"
	broadcastPlumtreeFECSchema = "overlay.broadcastPlumtreeFec flags:int timestamp:double src:PublicKey " +
		"certificate:overlay.Certificate full_data_hash:int256 full_data_size:int part_index:int " +
		"tree_index:int data:bytes signature:bytes = overlay.Broadcast"
	broadcastPlumtreeIHaveSchema = "overlay.broadcastPlumtreeIHave broadcast_id:int256 timestamp:double " +
		"part_index:int tree_index:int src:PublicKey certificate:overlay.Certificate " +
		"payload_timestamp:double data_size:int data_hash:int256 signature:bytes = overlay.Broadcast"
	broadcastPlumtreePruneSchema = "overlay.broadcastPlumtreePrune broadcast_id:int256 timestamp:double " +
		"part_index:int tree_index:int = overlay.Broadcast"
	broadcastPlumtreeUsefulSchema = "overlay.broadcastPlumtreeUseful broadcast_id:int256 timestamp:double " +
		"part_index:int tree_index:int = overlay.Broadcast"
	plumtreeStatsRecordSchema = "overlay.plumtreeStatsRecord src:int256 epoch:int tree_index:int " +
		"eager_0:(vector long) eager_index:(vector long) delivered_bcsts:int parts_in_p99_bcsts:int " +
		"useful_avg_ms:int useful_max_ms:int useful_p99_ms:int = overlay.PlumtreeStatsRecord"
	plumtreeStatsPushSchema = "overlay.plumtreeStatsPush record:overlay.plumtreeStatsRecord = overlay.Broadcast"
	plumtreeFECIDSchema     = "overlay.broadcastPlumtreeFec.id flags:int full_data_hash:int256 " +
		"full_data_size:int part_size:int = overlay.broadcastPlumtreeFec.Id"
	broadcastPlumtreeSimpleToSignSchema = "overlay.broadcastPlumtreeSimple.toSign id:int256 " +
		"timestamp:double tree_index:int data_size:int data_hash:int256 = overlay.broadcastPlumtreeSimple.ToSign"
	broadcastPlumtreeFECToSignSchema = "overlay.broadcastPlumtreeFec.toSign id:int256 timestamp:double " +
		"part_index:int tree_index:int data_size:int data_hash:int256 = overlay.broadcastPlumtreeFec.ToSign"
)

var (
	broadcastNotFoundConstructorID       uint32
	repairPlumtreePartConstructorID      uint32
	broadcastPlumtreeSimpleConstructorID uint32
	broadcastPlumtreeFECConstructorID    uint32
	broadcastPlumtreeIHaveConstructorID  uint32
	broadcastPlumtreePruneConstructorID  uint32
	broadcastPlumtreeUsefulConstructorID uint32
	plumtreeStatsRecordConstructorID     uint32
	plumtreeStatsPushConstructorID       uint32
)

func init() {
	broadcastNotFoundConstructorID = tl.Register(BroadcastNotFound{}, broadcastNotFoundSchema)
	repairPlumtreePartConstructorID = tl.Register(RepairPlumtreePart{}, repairPlumtreePartSchema)
	broadcastPlumtreeSimpleConstructorID = tl.Register(
		BroadcastPlumtreeSimple{},
		broadcastPlumtreeSimpleSchema,
	)
	broadcastPlumtreeFECConstructorID = tl.Register(BroadcastPlumtreeFEC{}, broadcastPlumtreeFECSchema)
	broadcastPlumtreeIHaveConstructorID = tl.Register(
		BroadcastPlumtreeIHave{},
		broadcastPlumtreeIHaveSchema,
	)
	broadcastPlumtreePruneConstructorID = tl.Register(
		BroadcastPlumtreePrune{},
		broadcastPlumtreePruneSchema,
	)
	broadcastPlumtreeUsefulConstructorID = tl.Register(
		BroadcastPlumtreeUseful{},
		broadcastPlumtreeUsefulSchema,
	)
	plumtreeStatsRecordConstructorID = tl.Register(PlumtreeStatsRecord{}, plumtreeStatsRecordSchema)
	plumtreeStatsPushConstructorID = tl.Register(PlumtreeStatsPush{}, plumtreeStatsPushSchema)
	tl.Register(PlumtreeFECID{}, plumtreeFECIDSchema)
	tl.Register(BroadcastPlumtreeSimpleToSign{}, broadcastPlumtreeSimpleToSignSchema)
	tl.Register(BroadcastPlumtreeFECToSign{}, broadcastPlumtreeFECToSignSchema)
}

type BroadcastNotFound struct{}

type RepairPlumtreePart struct {
	BroadcastID []byte  `tl:"int256"`
	Timestamp   float64 `tl:"double"`
	PartIndex   int32   `tl:"int"`
	TreeIndex   int32   `tl:"int"`
}

type BroadcastPlumtreeSimple struct {
	Flags       int32   `tl:"int"`
	Timestamp   float64 `tl:"double"`
	Source      any     `tl:"struct boxed [pub.ed25519]"`
	Certificate any     `tl:"struct boxed [overlay.emptyCertificate,overlay.certificate,overlay.certificateV2]"`
	BroadcastID []byte  `tl:"int256"`
	TreeIndex   int32   `tl:"int"`
	Data        []byte  `tl:"bytes"`
	Signature   []byte  `tl:"bytes"`
}

type BroadcastPlumtreeFEC struct {
	Flags        int32   `tl:"int"`
	Timestamp    float64 `tl:"double"`
	Source       any     `tl:"struct boxed [pub.ed25519]"`
	Certificate  any     `tl:"struct boxed [overlay.emptyCertificate,overlay.certificate,overlay.certificateV2]"`
	FullDataHash []byte  `tl:"int256"`
	FullDataSize int32   `tl:"int"`
	PartIndex    int32   `tl:"int"`
	TreeIndex    int32   `tl:"int"`
	Data         []byte  `tl:"bytes"`
	Signature    []byte  `tl:"bytes"`
}

type BroadcastPlumtreeIHave struct {
	BroadcastID      []byte  `tl:"int256"`
	Timestamp        float64 `tl:"double"`
	PartIndex        int32   `tl:"int"`
	TreeIndex        int32   `tl:"int"`
	Source           any     `tl:"struct boxed [pub.ed25519]"`
	Certificate      any     `tl:"struct boxed [overlay.emptyCertificate,overlay.certificate,overlay.certificateV2]"`
	PayloadTimestamp float64 `tl:"double"`
	DataSize         int32   `tl:"int"`
	DataHash         []byte  `tl:"int256"`
	Signature        []byte  `tl:"bytes"`
}

type BroadcastPlumtreePrune struct {
	BroadcastID []byte  `tl:"int256"`
	Timestamp   float64 `tl:"double"`
	PartIndex   int32   `tl:"int"`
	TreeIndex   int32   `tl:"int"`
}

type BroadcastPlumtreeUseful struct {
	BroadcastID []byte  `tl:"int256"`
	Timestamp   float64 `tl:"double"`
	PartIndex   int32   `tl:"int"`
	TreeIndex   int32   `tl:"int"`
}

type PlumtreeStatsRecord struct {
	Source             []byte  `tl:"int256"`
	Epoch              int32   `tl:"int"`
	TreeIndex          int32   `tl:"int"`
	EagerSimple        []int64 `tl:"vector long"`
	EagerRotating      []int64 `tl:"vector long"`
	DeliveredBroadcast int32   `tl:"int"`
	PartsInP99         int32   `tl:"int"`
	UsefulAverageMS    int32   `tl:"int"`
	UsefulMaximumMS    int32   `tl:"int"`
	UsefulP99MS        int32   `tl:"int"`
}

type PlumtreeStatsPush struct {
	Record PlumtreeStatsRecord `tl:"struct boxed"`
}

type PlumtreeFECID struct {
	Flags        int32  `tl:"int"`
	FullDataHash []byte `tl:"int256"`
	FullDataSize int32  `tl:"int"`
	PartSize     int32  `tl:"int"`
}

type BroadcastPlumtreeSimpleToSign struct {
	ID        []byte  `tl:"int256"`
	Timestamp float64 `tl:"double"`
	TreeIndex int32   `tl:"int"`
	DataSize  int32   `tl:"int"`
	DataHash  []byte  `tl:"int256"`
}

type BroadcastPlumtreeFECToSign struct {
	ID        []byte  `tl:"int256"`
	Timestamp float64 `tl:"double"`
	PartIndex int32   `tl:"int"`
	TreeIndex int32   `tl:"int"`
	DataSize  int32   `tl:"int"`
	DataHash  []byte  `tl:"int256"`
}

func validateBroadcastPlumtreeSimple(message BroadcastPlumtreeSimple, now time.Time) error {
	// validatePlumtreeControl only relates the two indices, and part 0 relates to
	// FEC tree 1 as legitimately as to the simple tree. Without this pin a simple
	// payload could take an FEC slot, where getPartLocked would never find it again
	// and the IHAVE we forward would be signed against the wrong preimage.
	if message.TreeIndex != plumtreeSimpleTree {
		return fmt.Errorf("Plumtree simple payload uses tree %d", message.TreeIndex)
	}
	if err := validatePlumtreeControl(
		message.BroadcastID,
		message.Timestamp,
		0,
		message.TreeIndex,
		now,
	); err != nil {
		return err
	}
	if len(message.Data) == 0 || len(message.Data) > int(plumtreeMaxPayloadSize) {
		return fmt.Errorf("invalid Plumtree simple payload size %d", len(message.Data))
	}
	return nil
}

func validateBroadcastPlumtreeFEC(message BroadcastPlumtreeFEC, now time.Time) error {
	if err := validatePlumtreeTimestamp(message.Timestamp, now); err != nil {
		return err
	}
	return validatePlumtreeFECFields(
		message.FullDataHash,
		message.FullDataSize,
		message.PartIndex,
		message.TreeIndex,
		message.Data,
	)
}

func validateBroadcastPlumtreeIHave(message BroadcastPlumtreeIHave, now time.Time) error {
	if err := validatePlumtreeControl(
		message.BroadcastID,
		message.Timestamp,
		message.PartIndex,
		message.TreeIndex,
		now,
	); err != nil {
		return err
	}
	if err := validatePlumtreeTimestamp(message.PayloadTimestamp, now); err != nil {
		return fmt.Errorf("invalid payload timestamp: %w", err)
	}
	if err := validatePlumtreeInt256(message.DataHash, "data hash"); err != nil {
		return err
	}
	// An IHAVE is buffered before its signature is checked, so a signature that can
	// never verify must be refused at the door rather than stored.
	if len(message.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("invalid Plumtree signature length %d", len(message.Signature))
	}

	maxSize := plumtreeMaxPayloadSize
	if message.TreeIndex != plumtreeSimpleTree {
		maxSize = plumtreeFECPartSize(plumtreeMaxPayloadSize)
	}
	if message.DataSize <= 0 || message.DataSize > maxSize {
		return fmt.Errorf("invalid Plumtree advertised data size %d", message.DataSize)
	}
	return nil
}

func validateRepairPlumtreePart(message RepairPlumtreePart, now time.Time) error {
	return validatePlumtreeControl(
		message.BroadcastID,
		message.Timestamp,
		message.PartIndex,
		message.TreeIndex,
		now,
	)
}

func validateBroadcastPlumtreePrune(message BroadcastPlumtreePrune, now time.Time) error {
	return validatePlumtreeControl(
		message.BroadcastID,
		message.Timestamp,
		message.PartIndex,
		message.TreeIndex,
		now,
	)
}

func validateBroadcastPlumtreeUseful(message BroadcastPlumtreeUseful, now time.Time) error {
	return validatePlumtreeControl(
		message.BroadcastID,
		message.Timestamp,
		message.PartIndex,
		message.TreeIndex,
		now,
	)
}

func validatePlumtreeFECFields(
	fullDataHash []byte,
	fullDataSize int32,
	partIndex int32,
	treeIndex int32,
	data []byte,
) error {
	if err := validatePlumtreeInt256(fullDataHash, "full data hash"); err != nil {
		return err
	}
	if err := validatePlumtreePayloadSize(fullDataSize); err != nil {
		return err
	}
	if partIndex < 0 || partIndex >= plumtreeFECTotalParts {
		return fmt.Errorf("invalid Plumtree FEC part index %d", partIndex)
	}
	if treeIndex != partIndex+plumtreeFECTreeOffset {
		return fmt.Errorf("Plumtree FEC part %d uses tree %d", partIndex, treeIndex)
	}

	expectedSize := plumtreeFECPartSize(fullDataSize)
	if int64(len(data)) != int64(expectedSize) {
		return fmt.Errorf("invalid Plumtree FEC symbol size %d, expected %d", len(data), expectedSize)
	}
	return nil
}

func validatePlumtreeControl(
	broadcastID []byte,
	timestamp float64,
	partIndex int32,
	treeIndex int32,
	now time.Time,
) error {
	if err := validatePlumtreeInt256(broadcastID, "broadcast id"); err != nil {
		return err
	}
	if plumtreeInt256IsZero(broadcastID) {
		return fmt.Errorf("empty Plumtree broadcast id")
	}
	if err := validatePlumtreeTimestamp(timestamp, now); err != nil {
		return err
	}
	if partIndex < 0 || treeIndex < 0 || treeIndex >= plumtreeTreeSlots {
		return fmt.Errorf(
			"invalid Plumtree part or tree index %d/%d",
			partIndex,
			treeIndex,
		)
	}
	if treeIndex == plumtreeSimpleTree {
		if partIndex != 0 {
			return fmt.Errorf("invalid Plumtree simple part index %d", partIndex)
		}
		return nil
	}
	if partIndex >= plumtreeFECTotalParts || treeIndex != partIndex+plumtreeFECTreeOffset {
		return fmt.Errorf(
			"Plumtree FEC part %d does not match tree %d",
			partIndex,
			treeIndex,
		)
	}
	return nil
}

func validatePlumtreeTimestamp(timestamp float64, now time.Time) error {
	if math.IsNaN(timestamp) || math.IsInf(timestamp, 0) {
		return fmt.Errorf("invalid Plumtree timestamp")
	}

	nowSeconds := float64(now.Unix()) + float64(now.Nanosecond())/float64(time.Second)
	window := plumtreeBroadcastLifetime.Seconds()
	if timestamp < nowSeconds-window {
		return fmt.Errorf("too old Plumtree timestamp")
	}
	if timestamp > nowSeconds+window {
		return fmt.Errorf("too new Plumtree timestamp")
	}
	return nil
}

func validatePlumtreePayloadSize(size int32) error {
	if size <= 0 || size > plumtreeMaxPayloadSize {
		return fmt.Errorf("invalid Plumtree payload size %d", size)
	}
	return nil
}

func validatePlumtreeInt256(value []byte, field string) error {
	if len(value) != sha256.Size {
		return fmt.Errorf("invalid Plumtree %s length %d", field, len(value))
	}
	return nil
}

func plumtreeInt256IsZero(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}

// Takes the decoder's two return values rather than the message itself: that
// keeps the message off an interface, and so off the heap.
func plumtreeParsed(rest []byte, err error) error {
	if err != nil {
		return fmt.Errorf("parse Plumtree object: %w", err)
	}
	if len(rest) != 0 {
		return fmt.Errorf("unexpected trailing Plumtree object data")
	}
	return nil
}

// The generic path, for the rare messages with no hand-written decoder.
func parsePlumtreeMessage(message any, data []byte) error {
	rest, err := tl.ParseNoCopy(message, data, true)
	if err != nil {
		return fmt.Errorf("parse Plumtree object: %w", err)
	}
	if len(rest) != 0 {
		return fmt.Errorf("unexpected trailing Plumtree object data")
	}
	return nil
}

func plumtreeFECPartSize(fullDataSize int32) int32 {
	return (fullDataSize + plumtreeFECDataParts - 1) / plumtreeFECDataParts
}
