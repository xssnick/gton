package localnet

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	logAnchorBytes      = 4 << 10
	logBackupTimeFormat = "2006-01-02T15-04-05.000"
	logCaptureAttempts  = 4
	logOffsetScanBytes  = 64 << 10
)

type logGeneration struct {
	name       string
	path       string
	compressed bool
}

type generationPaths struct {
	name       string
	timestamp  time.Time
	plain      string
	compressed string
}

type logGenerationReader struct {
	io.Reader
	seeker  io.Seeker
	closers []io.Closer
}

func (r *logGenerationReader) Close() error {
	var err error
	for _, closer := range r.closers {
		err = errors.Join(err, closer.Close())
	}
	return err
}

func (r *logGenerationReader) skip(offset int64) error {
	if offset == 0 {
		return nil
	}
	if r.seeker != nil {
		position, err := r.seeker.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}
		if position != offset {
			return fmt.Errorf("seek stopped at %d", position)
		}
		return nil
	}
	_, err := io.CopyN(io.Discard, r, offset)
	return err
}

func captureLogPosition(node NodeConfig) (LogPosition, error) {
	var lastErr error
	for range logCaptureAttempts {
		previousBackup, err := latestLogBackup(node.LogPath)
		if err != nil {
			return LogPosition{}, fmt.Errorf("inspect log backups %q: %w", node.Name, err)
		}

		file, err := os.Open(node.LogPath)
		if err != nil {
			lastErr = err
			continue
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			lastErr = err
			continue
		}
		fileID, err := logFileID(info)
		if err != nil {
			_ = file.Close()
			return LogPosition{}, fmt.Errorf("identify log %q: %w", node.Name, err)
		}
		observedSize := info.Size()
		offset, err := completeLogOffset(file, observedSize)
		if err != nil {
			_ = file.Close()
			lastErr = err
			continue
		}
		anchor, err := logAnchor(file, offset)
		if err != nil {
			_ = file.Close()
			lastErr = err
			continue
		}
		finalInfo, err := file.Stat()
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil || finalInfo.Size() < observedSize || !os.SameFile(info, finalInfo) {
			if err != nil {
				lastErr = err
			} else {
				lastErr = errors.New("active file identity or size changed")
			}
			continue
		}

		activeInfo, err := os.Stat(node.LogPath)
		if err != nil || !os.SameFile(finalInfo, activeInfo) {
			if err != nil {
				lastErr = err
			} else {
				lastErr = errors.New("active path changed")
			}
			continue
		}
		finalPreviousBackup, err := latestLogBackup(node.LogPath)
		if err != nil {
			return LogPosition{}, fmt.Errorf("inspect log backups %q: %w", node.Name, err)
		}
		if previousBackup != finalPreviousBackup {
			lastErr = errors.New("backup set changed")
			continue
		}

		return LogPosition{
			CapturedAt:     time.Now().UTC(),
			FileID:         fileID,
			PreviousBackup: previousBackup,
			Offset:         offset,
			Anchor:         anchor,
		}, nil
	}
	return LogPosition{}, fmt.Errorf("capture log position %q: active log changed during capture: %w", node.Name, lastErr)
}

func completeLogOffset(file *os.File, size int64) (int64, error) {
	if size == 0 {
		return 0, nil
	}
	limit := max(int64(0), size-int64(maxLogLineBytes)-1)
	buffer := make([]byte, logOffsetScanBytes)
	end := size
	for end > limit {
		start := max(limit, end-int64(len(buffer)))
		chunk := buffer[:end-start]
		if _, err := file.ReadAt(chunk, start); err != nil {
			return 0, err
		}
		if newline := bytes.LastIndexByte(chunk, '\n'); newline >= 0 {
			return start + int64(newline) + 1, nil
		}
		end = start
	}
	if limit == 0 {
		return 0, nil
	}
	return 0, fmt.Errorf("active log has no complete line in its last %d bytes", size-limit)
}

func readLogRange(node NodeConfig, start, end LogPosition) ([]Event, logStats, error) {
	var lastErr error
	for range logCaptureAttempts {
		events, stats, err := readLogRangeOnce(node, start, end)
		if err == nil {
			return events, stats, nil
		}
		lastErr = err
	}
	return nil, logStats{}, lastErr
}

func readLogRangeOnce(node NodeConfig, start, end LogPosition) ([]Event, logStats, error) {
	generations, err := listLogGenerations(node.LogPath)
	if err != nil {
		return nil, logStats{}, fmt.Errorf("list log generations %q: %w", node.Name, err)
	}
	startIndex, err := resolveLogPosition(generations, start)
	if err != nil {
		return nil, logStats{}, fmt.Errorf("resolve start of log %q: %w", node.Name, err)
	}
	endIndex, err := resolveLogPosition(generations, end)
	if err != nil {
		return nil, logStats{}, fmt.Errorf("resolve end of log %q: %w", node.Name, err)
	}
	if startIndex > endIndex || startIndex == endIndex && start.Offset > end.Offset {
		return nil, logStats{}, fmt.Errorf("log %q range is reversed", node.Name)
	}

	events := make([]Event, 0, 256)
	var stats logStats
	for index := startIndex; index <= endIndex; index++ {
		from := int64(0)
		if index == startIndex {
			from = start.Offset
		}
		to := int64(-1)
		if index == endIndex {
			to = end.Offset
		}
		generationEvents, err := readLogGeneration(node, generations[index], from, to)
		if err != nil {
			return nil, logStats{}, err
		}
		for _, event := range generationEvents {
			events = append(events, event)
			stats.add(event)
		}
	}
	return events, stats, nil
}

func readLogGeneration(node NodeConfig, generation logGeneration, start, end int64) ([]Event, error) {
	if start < 0 || end >= 0 && end < start {
		return nil, fmt.Errorf("invalid log generation range %d..%d", start, end)
	}
	reader, err := openLogGeneration(generation)
	if err != nil {
		return nil, fmt.Errorf("open log generation %q: %w", generation.path, err)
	}
	defer reader.Close()

	if err = reader.skip(start); err != nil {
		return nil, fmt.Errorf("seek log generation %q to %d: %w", generation.path, start, err)
	}
	var source io.Reader = reader
	var limited *io.LimitedReader
	if end >= 0 {
		limited = &io.LimitedReader{R: reader, N: end - start}
		source = limited
	}

	events := make([]Event, 0, 256)
	buffered := bufio.NewReaderSize(source, 128<<10)
	for {
		line, readErr := buffered.ReadBytes('\n')
		if len(line) > 0 && len(line) <= maxLogLineBytes {
			if event, parsed := parseLogLine(node, line); parsed {
				events = append(events, event)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("read log generation %q: %w", generation.path, readErr)
		}
	}
	if limited != nil && limited.N != 0 {
		return nil, fmt.Errorf("log generation %q ends before offset %d", generation.path, end)
	}
	return events, nil
}

func resolveLogPosition(generations []logGeneration, position LogPosition) (int, error) {
	if position.FileID == "" || position.Offset < 0 {
		return 0, errors.New("invalid log position")
	}
	for index, generation := range generations {
		if generation.compressed {
			continue
		}
		info, err := os.Stat(generation.path)
		if err != nil {
			continue
		}
		fileID, err := logFileID(info)
		if err != nil || fileID != position.FileID {
			continue
		}
		matches, err := logGenerationMatches(generation, position)
		if err == nil && matches {
			return index, nil
		}
	}

	lineageIndex := 0
	lineageAvailable := position.PreviousBackup == ""
	if position.PreviousBackup != "" {
		for index, generation := range generations {
			if generation.name == position.PreviousBackup {
				lineageIndex = index + 1
				lineageAvailable = true
				break
			}
		}
	}
	if lineageAvailable && lineageIndex < len(generations) {
		matches, err := logGenerationMatches(generations[lineageIndex], position)
		if err == nil && matches {
			return lineageIndex, nil
		}
	}

	matchedIndex := -1
	for index, generation := range generations {
		matches, err := logGenerationMatches(generation, position)
		if err != nil || !matches {
			continue
		}
		if matchedIndex >= 0 {
			return 0, errors.New("log position anchor matches multiple generations")
		}
		matchedIndex = index
	}
	if matchedIndex >= 0 {
		return matchedIndex, nil
	}
	return 0, fmt.Errorf("log generation %s at offset %d is no longer available", position.FileID, position.Offset)
}

func logGenerationMatches(generation logGeneration, position LogPosition) (bool, error) {
	reader, err := openLogGeneration(generation)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	anchorSize := min(position.Offset, int64(logAnchorBytes))
	if position.Offset == 0 {
		return position.Anchor == "", nil
	}
	if position.Anchor == "" {
		return false, nil
	}
	if err = reader.skip(position.Offset - anchorSize); err != nil {
		return false, nil
	}
	data := make([]byte, anchorSize)
	if _, err = io.ReadFull(reader, data); err != nil {
		return false, nil
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]) == position.Anchor, nil
}

func logAnchor(file *os.File, offset int64) (string, error) {
	if offset == 0 {
		return "", nil
	}
	size := min(offset, int64(logAnchorBytes))
	data := make([]byte, size)
	if _, err := file.ReadAt(data, offset-size); err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func listLogGenerations(activePath string) ([]logGeneration, error) {
	backups, err := listLogBackups(activePath)
	if err != nil {
		return nil, err
	}
	generations := make([]logGeneration, 0, len(backups)+1)
	for _, backup := range backups {
		generation := logGeneration{name: backup.name}
		if backup.plain != "" {
			generation.path = backup.plain
		} else {
			generation.path = backup.compressed
			generation.compressed = true
		}
		generations = append(generations, generation)
	}
	if _, err = os.Stat(activePath); err != nil {
		return nil, err
	}
	generations = append(generations, logGeneration{path: activePath})
	return generations, nil
}

func latestLogBackup(activePath string) (string, error) {
	backups, err := listLogBackups(activePath)
	if err != nil || len(backups) == 0 {
		return "", err
	}
	return backups[len(backups)-1].name, nil
}

func listLogBackups(activePath string) ([]generationPaths, error) {
	directory := filepath.Dir(activePath)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	base := filepath.Base(activePath)
	extension := filepath.Ext(base)
	prefix := strings.TrimSuffix(base, extension) + "-"
	byName := make(map[string]*generationPaths)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		plainName := strings.TrimSuffix(name, ".gz")
		if filepath.Ext(plainName) != extension {
			continue
		}
		stem := strings.TrimSuffix(plainName, extension)
		if !strings.HasPrefix(stem, prefix) {
			continue
		}
		timestamp, parseErr := time.Parse(logBackupTimeFormat, strings.TrimPrefix(stem, prefix))
		if parseErr != nil {
			continue
		}
		paths := byName[plainName]
		if paths == nil {
			paths = &generationPaths{name: plainName, timestamp: timestamp}
			byName[plainName] = paths
		}
		path := filepath.Join(directory, name)
		if strings.HasSuffix(name, ".gz") {
			paths.compressed = path
		} else {
			paths.plain = path
		}
	}
	backups := make([]generationPaths, 0, len(byName))
	for _, paths := range byName {
		backups = append(backups, *paths)
	}
	sort.Slice(backups, func(i, j int) bool {
		if backups[i].timestamp.Equal(backups[j].timestamp) {
			return backups[i].name < backups[j].name
		}
		return backups[i].timestamp.Before(backups[j].timestamp)
	})
	return backups, nil
}

func openLogGeneration(generation logGeneration) (*logGenerationReader, error) {
	file, err := os.Open(generation.path)
	if err != nil {
		return nil, err
	}
	if !generation.compressed {
		return &logGenerationReader{Reader: file, seeker: file, closers: []io.Closer{file}}, nil
	}
	compressed, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &logGenerationReader{Reader: compressed, closers: []io.Closer{compressed, file}}, nil
}

func logFileID(info os.FileInfo) (string, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("filesystem does not expose device and inode")
	}
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino), nil
}
