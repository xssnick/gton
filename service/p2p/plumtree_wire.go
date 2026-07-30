package p2p

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// Hand-written decoders for the hot-path Plumtree messages, exposed through tl's
// ParseableTL interface so tl still does the boxed type-id check. tl's reflection
// decoder recursed deeply enough that stack growth alone outweighed the parsing:
// runtime.newstack was 8.3% of the node, 97.7% of it under tl.executeParse.
//
// ParseableTL only sets manualParse; the struct tags stay live for serialization.
// plumtree_wire_diff_test.go pins these against tag-identical twin types that
// keep the reflection path.

var (
	overlayCertificateConstructorID = tl.CRC(
		"overlay.certificate issued_by:PublicKey expire_at:int max_size:int " +
			"signature:bytes = overlay.Certificate",
	)
	overlayCertificateV2ConstructorID = tl.CRC(
		"overlay.certificateV2 issued_by:PublicKey expire_at:int max_size:int " +
			"flags:int signature:bytes = overlay.Certificate",
	)
	overlayCertificateEmptyConstructorID = tl.CRC(
		"overlay.emptyCertificate = overlay.Certificate",
	)

	errPlumtreeWireShort = errors.New("truncated Plumtree object")
)

// Walks a boxed body whose type id tl already consumed. copyPayload mirrors tl's
// split: Parse copies every field, ParseNoCopy aliases the bulk ones.
type plumtreeWireCursor struct {
	buf         []byte
	copyPayload bool
}

func (c *plumtreeWireCursor) readUint32() (uint32, error) {
	if len(c.buf) < 4 {
		return 0, errPlumtreeWireShort
	}
	value := binary.LittleEndian.Uint32(c.buf)
	c.buf = c.buf[4:]
	return value, nil
}

func (c *plumtreeWireCursor) readInt32() (int32, error) {
	value, err := c.readUint32()
	return int32(value), err
}

func (c *plumtreeWireCursor) readDouble() (float64, error) {
	if len(c.buf) < 8 {
		return 0, errPlumtreeWireShort
	}
	value := math.Float64frombits(binary.LittleEndian.Uint64(c.buf))
	c.buf = c.buf[8:]
	return value, nil
}

func (c *plumtreeWireCursor) readInt256() ([]byte, error) {
	if len(c.buf) < 32 {
		return nil, errPlumtreeWireShort
	}
	value := c.result(c.buf[:32])
	c.buf = c.buf[32:]
	return value, nil
}

// Defers to tl's length-prefix reader so the short/0xFE/0xFF forms and their
// padding rules stay defined in one place.
func (c *plumtreeWireCursor) readBytes() ([]byte, error) {
	var value []byte
	var err error
	if c.copyPayload {
		value, c.buf, err = tl.FromBytes(c.buf)
	} else {
		value, c.buf, err = tl.FromBytesNoCopy(c.buf)
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Caps capacity on the aliasing path so a later append cannot reach the wire
// bytes that follow the field.
func (c *plumtreeWireCursor) result(value []byte) []byte {
	if !c.copyPayload {
		return value[:len(value):len(value)]
	}
	copied := make([]byte, len(value))
	copy(copied, value)
	return copied
}

// Hand-written: pub.ed25519 is a type id plus a fixed 32 bytes, no layout that
// could drift. The copy matches keys.PublicKeyED25519.Parse, which copies even
// under ParseNoCopy.
func (c *plumtreeWireCursor) readSourceKey() (keys.PublicKeyED25519, error) {
	constructor, err := c.readUint32()
	if err != nil {
		return keys.PublicKeyED25519{}, err
	}
	if constructor != ed25519PublicKeyConstructorID {
		return keys.PublicKeyED25519{}, fmt.Errorf(
			"unsupported Plumtree public key type %08x",
			constructor,
		)
	}
	if len(c.buf) < ed25519.PublicKeySize {
		return keys.PublicKeyED25519{}, errPlumtreeWireShort
	}

	key := make([]byte, ed25519.PublicKeySize)
	copy(key, c.buf)
	c.buf = c.buf[ed25519.PublicKeySize:]
	return keys.PublicKeyED25519{Key: key}, nil
}

// Certificates are common on this network and are decoded on every inbound
// message, before the IHAVE dedup, so handing them to tl cost a measured 2.2% of
// node CPU. These layouts belong to tonutils-go and could gain a field without
// this code noticing; TestPlumtreeWireCertificateLayoutIsPinned makes that loud.
func (c *plumtreeWireCursor) readCertificate() (any, error) {
	constructor, err := c.readUint32()
	if err != nil {
		return nil, err
	}

	switch constructor {
	case overlayCertificateEmptyConstructorID:
		return overlay.CertificateEmpty{}, nil
	case overlayCertificateConstructorID:
		var certificate overlay.Certificate
		if certificate.IssuedBy, err = c.readSourceKey(); err != nil {
			return nil, err
		}
		if certificate.ExpireAt, err = c.readUint32(); err != nil {
			return nil, err
		}
		if certificate.MaxSize, err = c.readUint32(); err != nil {
			return nil, err
		}
		if certificate.Signature, err = c.readBytes(); err != nil {
			return nil, err
		}
		return certificate, nil
	case overlayCertificateV2ConstructorID:
		var certificate overlay.CertificateV2
		if certificate.IssuedBy, err = c.readSourceKey(); err != nil {
			return nil, err
		}
		if certificate.ExpireAt, err = c.readUint32(); err != nil {
			return nil, err
		}
		if certificate.MaxSize, err = c.readUint32(); err != nil {
			return nil, err
		}
		if certificate.Flags, err = c.readInt32(); err != nil {
			return nil, err
		}
		if certificate.Signature, err = c.readBytes(); err != nil {
			return nil, err
		}
		return certificate, nil
	default:
		return nil, fmt.Errorf(
			"unsupported Plumtree certificate type %08x",
			constructor,
		)
	}
}

func (m *BroadcastPlumtreeIHave) Parse(data []byte) ([]byte, error) {
	return m.parse(data, true)
}

func (m *BroadcastPlumtreeIHave) ParseNoCopy(data []byte) ([]byte, error) {
	return m.parse(data, false)
}

func (m *BroadcastPlumtreeIHave) parse(data []byte, copyPayload bool) ([]byte, error) {
	cursor := plumtreeWireCursor{buf: data, copyPayload: copyPayload}

	var err error
	if m.BroadcastID, err = cursor.readInt256(); err != nil {
		return nil, err
	}
	if m.Timestamp, err = cursor.readDouble(); err != nil {
		return nil, err
	}
	if m.PartIndex, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.TreeIndex, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	source, err := cursor.readSourceKey()
	if err != nil {
		return nil, err
	}
	m.Source = source
	if m.Certificate, err = cursor.readCertificate(); err != nil {
		return nil, err
	}
	if m.PayloadTimestamp, err = cursor.readDouble(); err != nil {
		return nil, err
	}
	if m.DataSize, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.DataHash, err = cursor.readInt256(); err != nil {
		return nil, err
	}
	if m.Signature, err = cursor.readBytes(); err != nil {
		return nil, err
	}
	return cursor.buf, nil
}

func (m *BroadcastPlumtreeFEC) Parse(data []byte) ([]byte, error) {
	return m.parse(data, true)
}

func (m *BroadcastPlumtreeFEC) ParseNoCopy(data []byte) ([]byte, error) {
	return m.parse(data, false)
}

func (m *BroadcastPlumtreeFEC) parse(data []byte, copyPayload bool) ([]byte, error) {
	cursor := plumtreeWireCursor{buf: data, copyPayload: copyPayload}

	var err error
	if m.Flags, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.Timestamp, err = cursor.readDouble(); err != nil {
		return nil, err
	}
	source, err := cursor.readSourceKey()
	if err != nil {
		return nil, err
	}
	m.Source = source
	if m.Certificate, err = cursor.readCertificate(); err != nil {
		return nil, err
	}
	if m.FullDataHash, err = cursor.readInt256(); err != nil {
		return nil, err
	}
	if m.FullDataSize, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.PartIndex, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.TreeIndex, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.Data, err = cursor.readBytes(); err != nil {
		return nil, err
	}
	if m.Signature, err = cursor.readBytes(); err != nil {
		return nil, err
	}
	return cursor.buf, nil
}

func (m *BroadcastPlumtreeSimple) Parse(data []byte) ([]byte, error) {
	return m.parse(data, true)
}

func (m *BroadcastPlumtreeSimple) ParseNoCopy(data []byte) ([]byte, error) {
	return m.parse(data, false)
}

func (m *BroadcastPlumtreeSimple) parse(data []byte, copyPayload bool) ([]byte, error) {
	cursor := plumtreeWireCursor{buf: data, copyPayload: copyPayload}

	var err error
	if m.Flags, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.Timestamp, err = cursor.readDouble(); err != nil {
		return nil, err
	}
	source, err := cursor.readSourceKey()
	if err != nil {
		return nil, err
	}
	m.Source = source
	if m.Certificate, err = cursor.readCertificate(); err != nil {
		return nil, err
	}
	if m.BroadcastID, err = cursor.readInt256(); err != nil {
		return nil, err
	}
	if m.TreeIndex, err = cursor.readInt32(); err != nil {
		return nil, err
	}
	if m.Data, err = cursor.readBytes(); err != nil {
		return nil, err
	}
	if m.Signature, err = cursor.readBytes(); err != nil {
		return nil, err
	}
	return cursor.buf, nil
}

// Prune, useful and the repair request carry nothing but the part coordinates.
func parsePlumtreeControlWire(
	data []byte,
	copyPayload bool,
) (id []byte, timestamp float64, partIndex, treeIndex int32, rest []byte, err error) {
	cursor := plumtreeWireCursor{buf: data, copyPayload: copyPayload}
	if id, err = cursor.readInt256(); err != nil {
		return nil, 0, 0, 0, nil, err
	}
	if timestamp, err = cursor.readDouble(); err != nil {
		return nil, 0, 0, 0, nil, err
	}
	if partIndex, err = cursor.readInt32(); err != nil {
		return nil, 0, 0, 0, nil, err
	}
	if treeIndex, err = cursor.readInt32(); err != nil {
		return nil, 0, 0, 0, nil, err
	}
	return id, timestamp, partIndex, treeIndex, cursor.buf, nil
}

func (m *BroadcastPlumtreePrune) Parse(data []byte) ([]byte, error) {
	return m.parse(data, true)
}

func (m *BroadcastPlumtreePrune) ParseNoCopy(data []byte) ([]byte, error) {
	return m.parse(data, false)
}

func (m *BroadcastPlumtreePrune) parse(data []byte, copyPayload bool) ([]byte, error) {
	id, timestamp, partIndex, treeIndex, rest, err := parsePlumtreeControlWire(
		data,
		copyPayload,
	)
	if err != nil {
		return nil, err
	}
	m.BroadcastID, m.Timestamp = id, timestamp
	m.PartIndex, m.TreeIndex = partIndex, treeIndex
	return rest, nil
}

func (m *BroadcastPlumtreeUseful) Parse(data []byte) ([]byte, error) {
	return m.parse(data, true)
}

func (m *BroadcastPlumtreeUseful) ParseNoCopy(data []byte) ([]byte, error) {
	return m.parse(data, false)
}

func (m *BroadcastPlumtreeUseful) parse(data []byte, copyPayload bool) ([]byte, error) {
	id, timestamp, partIndex, treeIndex, rest, err := parsePlumtreeControlWire(
		data,
		copyPayload,
	)
	if err != nil {
		return nil, err
	}
	m.BroadcastID, m.Timestamp = id, timestamp
	m.PartIndex, m.TreeIndex = partIndex, treeIndex
	return rest, nil
}

func (m *RepairPlumtreePart) Parse(data []byte) ([]byte, error) {
	return m.parse(data, true)
}

func (m *RepairPlumtreePart) ParseNoCopy(data []byte) ([]byte, error) {
	return m.parse(data, false)
}

func (m *RepairPlumtreePart) parse(data []byte, copyPayload bool) ([]byte, error) {
	id, timestamp, partIndex, treeIndex, rest, err := parsePlumtreeControlWire(
		data,
		copyPayload,
	)
	if err != nil {
		return nil, err
	}
	m.BroadcastID, m.Timestamp = id, timestamp
	m.PartIndex, m.TreeIndex = partIndex, treeIndex
	return rest, nil
}
