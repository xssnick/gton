package pebblestore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/xssnick/gton/service/storage"
)

const (
	candidatePackDirectoryName = "candidate-packs"
	candidatePackFileSuffix    = ".pack"
	candidatePackPointerSize   = 1 + 4 + 8 + 4 + sha256.Size
	candidatePackPointerV1     = byte(1)
	candidatePackRecordHeader  = 4
	defaultCandidatePackSize   = int64(1 << 30)
)

var candidatePackMagic = [8]byte{'G', 'T', 'O', 'N', 'C', 'P', 'K', 1}

type candidatePackPointer struct {
	segment uint32
	offset  uint64
	size    uint32
	hash    [sha256.Size]byte
}

type candidatePackFile struct {
	segment uint32
	size    int64
	file    *os.File
}

// candidatePackStore is owned by the validator writer goroutine. Candidate
// reads open the immutable segment named by their pointer directly, so the hot
// append path needs neither a lock nor a second queue.
type candidatePackStore struct {
	root    string
	maxSize int64
	current map[storageNamespace]*candidatePackFile
}

func newCandidatePackStore(databaseDir string) *candidatePackStore {
	return &candidatePackStore{
		root:    filepath.Join(databaseDir, candidatePackDirectoryName),
		maxSize: defaultCandidatePackSize,
		current: make(map[storageNamespace]*candidatePackFile),
	}
}

func (s *candidatePackStore) append(
	namespace storageNamespace,
	wire []byte,
	hash [sha256.Size]byte,
) (candidatePackPointer, error) {
	if uint64(len(wire)) > math.MaxUint32 {
		return candidatePackPointer{}, errors.New("validator pebblestore: candidate wire exceeds pack format")
	}

	recordSize := int64(candidatePackRecordHeader) + int64(len(wire))
	pack := s.current[namespace]
	if pack == nil || (pack.size > int64(len(candidatePackMagic)) && pack.size+recordSize > s.maxSize) {
		var err error
		pack, err = s.rotate(namespace, pack)
		if err != nil {
			return candidatePackPointer{}, err
		}
	}

	recordOffset := pack.size
	var header [candidatePackRecordHeader]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(wire)))
	if err := writeCandidatePackAt(pack.file, header[:], recordOffset); err != nil {
		return candidatePackPointer{}, s.abandon(namespace, pack, err)
	}
	if err := writeCandidatePackAt(pack.file, wire, recordOffset+candidatePackRecordHeader); err != nil {
		return candidatePackPointer{}, s.abandon(namespace, pack, err)
	}
	pack.size += recordSize

	return candidatePackPointer{
		segment: pack.segment,
		offset:  uint64(recordOffset),
		size:    uint32(len(wire)),
		hash:    hash,
	}, nil
}

func (s *candidatePackStore) read(
	namespace storageNamespace,
	pointer candidatePackPointer,
) ([]byte, error) {
	path := s.packPath(namespace, pointer.segment)
	file, err := os.Open(path)
	if err != nil {
		return nil, candidatePackReadError("open", err)
	}
	defer file.Close()

	var magic [len(candidatePackMagic)]byte
	if _, err = file.ReadAt(magic[:], 0); err != nil {
		return nil, candidatePackReadError("read header", err)
	}
	if magic != candidatePackMagic {
		return nil, candidatePackNotFound("read header", errors.New("invalid magic"))
	}
	if pointer.offset < uint64(len(candidatePackMagic)) || pointer.offset > math.MaxInt64 {
		return nil, candidatePackNotFound("read record", errors.New("invalid offset"))
	}

	record := make([]byte, candidatePackRecordHeader+int(pointer.size))
	if _, err = file.ReadAt(record, int64(pointer.offset)); err != nil {
		return nil, candidatePackReadError("read record", err)
	}
	if binary.LittleEndian.Uint32(record[:candidatePackRecordHeader]) != pointer.size {
		return nil, candidatePackNotFound("read record", errors.New("length mismatch"))
	}

	wire := record[candidatePackRecordHeader:]
	if sha256.Sum256(wire) != pointer.hash {
		return nil, candidatePackNotFound("read record", errors.New("hash mismatch"))
	}

	return wire, nil
}

func (s *candidatePackStore) rotate(
	namespace storageNamespace,
	previous *candidatePackFile,
) (*candidatePackFile, error) {
	if previous != nil {
		delete(s.current, namespace)
		if err := previous.file.Close(); err != nil {
			return nil, fmt.Errorf("validator pebblestore: close candidate pack: %w", err)
		}
	}

	dir := s.sessionDir(namespace)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("validator pebblestore: create candidate pack dir: %w", err)
	}
	segment, err := nextCandidatePackSegment(dir)
	if err != nil {
		return nil, err
	}
	path := s.packPath(namespace, segment)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("validator pebblestore: create candidate pack: %w", err)
	}
	if err = writeCandidatePackAt(file, candidatePackMagic[:], 0); err != nil {
		closeErr := file.Close()
		removeErr := os.Remove(path)

		return nil, errors.Join(
			fmt.Errorf("validator pebblestore: initialize candidate pack: %w", err),
			closeErr,
			removeErr,
		)
	}

	pack := &candidatePackFile{segment: segment, size: int64(len(candidatePackMagic)), file: file}
	s.current[namespace] = pack

	return pack, nil
}

func (s *candidatePackStore) abandon(
	namespace storageNamespace,
	pack *candidatePackFile,
	writeErr error,
) error {
	delete(s.current, namespace)

	return errors.Join(
		fmt.Errorf("validator pebblestore: append candidate pack: %w", writeErr),
		pack.file.Close(),
	)
}

func (s *candidatePackStore) delete(namespace storageNamespace) error {
	var result error
	if pack := s.current[namespace]; pack != nil {
		if err := pack.file.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("validator pebblestore: close candidate pack: %w", err))
		}
		delete(s.current, namespace)
	}
	if err := os.RemoveAll(s.sessionDir(namespace)); err != nil {
		result = errors.Join(result, fmt.Errorf("validator pebblestore: delete candidate packs: %w", err))
	}

	return result
}

func (s *candidatePackStore) close() error {
	var result error
	for namespace, pack := range s.current {
		if err := pack.file.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("validator pebblestore: close candidate pack: %w", err))
		}
		delete(s.current, namespace)
	}

	return result
}

func (s *candidatePackStore) sessionDir(namespace storageNamespace) string {
	return filepath.Join(s.root, fmt.Sprintf("%x", namespace[:]))
}

func (s *candidatePackStore) packPath(namespace storageNamespace, segment uint32) string {
	return filepath.Join(s.sessionDir(namespace), candidatePackFileName(segment))
}

func candidatePackFileName(segment uint32) string {
	return fmt.Sprintf("%08x%s", segment, candidatePackFileSuffix)
}

func nextCandidatePackSegment(dir string) (uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("validator pebblestore: list candidate packs: %w", err)
	}

	var highest uint32
	found := false
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || len(name) != 8+len(candidatePackFileSuffix) ||
			name[8:] != candidatePackFileSuffix {
			continue
		}
		value, parseErr := strconv.ParseUint(name[:8], 16, 32)
		if parseErr != nil {
			continue
		}
		segment := uint32(value)
		if !found || segment > highest {
			highest = segment
			found = true
		}
	}
	if !found {
		return 0, nil
	}
	if highest == math.MaxUint32 {
		return 0, errors.New("validator pebblestore: candidate pack segment exhausted")
	}

	return highest + 1, nil
}

func encodeCandidatePackPointer(pointer candidatePackPointer) []byte {
	value := make([]byte, candidatePackPointerSize)
	value[0] = candidatePackPointerV1
	binary.LittleEndian.PutUint32(value[1:5], pointer.segment)
	binary.LittleEndian.PutUint64(value[5:13], pointer.offset)
	binary.LittleEndian.PutUint32(value[13:17], pointer.size)
	copy(value[17:], pointer.hash[:])

	return value
}

func decodeCandidatePackPointer(value []byte) (candidatePackPointer, error) {
	if len(value) != candidatePackPointerSize {
		return candidatePackPointer{}, fmt.Errorf(
			"validator pebblestore: candidate pointer length %d",
			len(value),
		)
	}
	if value[0] != candidatePackPointerV1 {
		return candidatePackPointer{}, fmt.Errorf(
			"validator pebblestore: candidate pointer version %d",
			value[0],
		)
	}

	pointer := candidatePackPointer{
		segment: binary.LittleEndian.Uint32(value[1:5]),
		offset:  binary.LittleEndian.Uint64(value[5:13]),
		size:    binary.LittleEndian.Uint32(value[13:17]),
	}
	copy(pointer.hash[:], value[17:])

	return pointer, nil
}

func writeCandidatePackAt(file *os.File, data []byte, offset int64) error {
	for len(data) > 0 {
		written, err := file.WriteAt(data, offset)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
		offset += int64(written)
	}

	return nil
}

func candidatePackNotFound(operation string, cause error) error {
	return fmt.Errorf("validator pebblestore: candidate pack %s: %w: %v", operation, storage.ErrNotFound, cause)
}

func candidatePackReadError(operation string, cause error) error {
	if errors.Is(cause, os.ErrNotExist) || errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) {
		return candidatePackNotFound(operation, cause)
	}

	return fmt.Errorf("validator pebblestore: candidate pack %s: %w", operation, cause)
}
