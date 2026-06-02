package packfile

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	PackageMagic = 0xae8fdd01
	EntryMagic   = 0x1e8b

	HeaderSize      = 4
	EntryHeaderSize = 8
	MaxDataSize     = (1 << 31) - 1
	MaxNameSize     = (1 << 16) - 1

	KindBlock     = "block"
	KindProof     = "proof"
	KindProofLink = "prooflink"
	KindZeroState = "zerostate"
)

type Entry struct {
	Name        string
	Data        []byte
	EntryOffset int64
	DataOffset  int64
	DataSize    int64
}

type Pointer struct {
	Path   string
	Offset int64
	Size   int64
}

type Appender struct {
	file   *os.File
	offset int64
}

func EntryName(kind string, id ton.BlockIDExt) string {
	return fmt.Sprintf(
		"%s_(%d,%016x,%d):%x:%x",
		kind,
		id.Workchain,
		uint64(id.Shard),
		id.SeqNo,
		id.RootHash,
		id.FileHash,
	)
}

func Read(ctx context.Context, r io.Reader, handle func(Entry) error) error {
	var magic [HeaderSize]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return fmt.Errorf("read archive magic: %w", err)
	}
	got := binary.LittleEndian.Uint32(magic[:])
	if got != PackageMagic {
		return fmt.Errorf("archive package magic mismatch: got=%08x want=%08x", got, PackageMagic)
	}

	offset := int64(HeaderSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var header [EntryHeaderSize]byte
		n, err := io.ReadFull(r, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) && n == 0 {
				return nil
			}
			return fmt.Errorf("read archive entry header: %w", err)
		}

		header0 := binary.LittleEndian.Uint32(header[:4])
		if header0&0xffff != EntryMagic {
			return fmt.Errorf("archive entry magic mismatch")
		}

		nameLen := int(header0 >> 16)
		dataLen := int(binary.LittleEndian.Uint32(header[4:]))
		if dataLen > MaxDataSize {
			return fmt.Errorf("archive entry %d bytes exceeds limit %d", dataLen, MaxDataSize)
		}

		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return fmt.Errorf("read archive entry name: %w", err)
		}

		dataOffset := offset + EntryHeaderSize + int64(nameLen)
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return fmt.Errorf("read archive entry data: %w", err)
		}

		if err := handle(Entry{
			Name:        string(name),
			Data:        data,
			EntryOffset: offset,
			DataOffset:  dataOffset,
			DataSize:    int64(dataLen),
		}); err != nil {
			return err
		}

		offset = dataOffset + int64(dataLen)
	}
}

func ensureWithSize(file *os.File) (int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if stat.Size() == 0 {
		var magic [HeaderSize]byte
		binary.LittleEndian.PutUint32(magic[:], PackageMagic)
		if _, err = file.WriteAt(magic[:], 0); err != nil {
			return 0, err
		}
		return HeaderSize, nil
	}
	if stat.Size() < HeaderSize {
		return 0, fmt.Errorf("archive package is too short")
	}

	var magic [HeaderSize]byte
	if _, err = file.ReadAt(magic[:], 0); err != nil {
		return 0, err
	}
	if binary.LittleEndian.Uint32(magic[:]) != PackageMagic {
		return 0, fmt.Errorf("archive package magic mismatch")
	}
	return stat.Size(), nil
}

func NewAppender(file *os.File) (*Appender, error) {
	offset, err := ensureWithSize(file)
	if err != nil {
		return nil, err
	}
	return &Appender{
		file:   file,
		offset: offset,
	}, nil
}

func (a *Appender) Append(name string, data []byte) (Pointer, error) {
	if len(name) > MaxNameSize {
		return Pointer{}, fmt.Errorf("archive entry name too large: %d", len(name))
	}
	if len(data) > MaxDataSize {
		return Pointer{}, fmt.Errorf("archive entry data too large: %d", len(data))
	}

	entryOffset := a.offset
	dataOffset := entryOffset + EntryHeaderSize + int64(len(name))

	var header [EntryHeaderSize]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(EntryMagic)|uint32(len(name))<<16)
	binary.LittleEndian.PutUint32(header[4:], uint32(len(data)))

	if _, err := a.file.WriteAt(header[:], entryOffset); err != nil {
		return Pointer{}, rollbackAppend(a.file, entryOffset, err)
	}
	if _, err := a.file.WriteAt([]byte(name), entryOffset+EntryHeaderSize); err != nil {
		return Pointer{}, rollbackAppend(a.file, entryOffset, err)
	}
	if _, err := a.file.WriteAt(data, dataOffset); err != nil {
		return Pointer{}, rollbackAppend(a.file, entryOffset, err)
	}

	a.offset = dataOffset + int64(len(data))
	return Pointer{
		Offset: dataOffset,
		Size:   int64(len(data)),
	}, nil
}

func (a *Appender) Size() int64 {
	return a.offset
}

func rollbackAppend(file *os.File, offset int64, err error) error {
	if truncateErr := file.Truncate(offset); truncateErr != nil {
		return errors.Join(err, fmt.Errorf("rollback archive append to %d: %w", offset, truncateErr))
	}
	return err
}
