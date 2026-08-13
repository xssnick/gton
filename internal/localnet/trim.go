package localnet

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type TrimResult struct {
	LogPath       string   `json:"log_path"`
	Nodes         []string `json:"nodes"`
	OriginalSize  int64    `json:"original_size"`
	ArchivedBytes int64    `json:"archived_bytes"`
	ArchivePath   string   `json:"archive_path"`
	Applied       bool     `json:"applied"`
}

func TrimLogs(cfg Config, keepBytes int64, apply bool) ([]TrimResult, error) {
	if keepBytes < 0 {
		return nil, fmt.Errorf("keep-bytes cannot be negative")
	}
	if apply {
		if _, err := os.Stat(filepath.Join(cfg.RunRoot, ".gton-lab.lock")); err == nil {
			return nil, fmt.Errorf("cannot trim logs while a gton-lab run is active")
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("check active run lock: %w", err)
		}
	}

	nodesByPath := make(map[string][]string, len(cfg.Nodes))
	paths := make([]string, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		if _, exists := nodesByPath[node.LogPath]; !exists {
			paths = append(paths, node.LogPath)
		}
		nodesByPath[node.LogPath] = append(nodesByPath[node.LogPath], node.Name)
	}

	timestamp := time.Now().UTC().Format("20060102T150405Z")
	archiveDirectory := filepath.Join(cfg.RunRoot, "log-archives", timestamp)
	results := make([]TrimResult, 0, len(paths))
	fileInfo := make([]os.FileInfo, 0, len(paths))
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("inspect log %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("log %q is not a regular non-symlink file", path)
		}
		archiveBytes := min(info.Size(), keepBytes)
		digest := sha256.Sum256([]byte(path))
		archiveName := safeArchiveName(nodesByPath[path][0]) + "-" + hex.EncodeToString(digest[:4]) + ".tail.gz"
		archivePath := filepath.Join(archiveDirectory, archiveName)
		result := TrimResult{LogPath: path, Nodes: nodesByPath[path], OriginalSize: info.Size(), ArchivedBytes: archiveBytes, ArchivePath: archivePath}
		results = append(results, result)
		fileInfo = append(fileInfo, info)
	}
	if apply {
		for i := range results {
			if err := archiveLogTail(results[i].LogPath, results[i].ArchivePath, fileInfo[i], results[i].ArchivedBytes); err != nil {
				return nil, err
			}
			results[i].Applied = true
		}
	}
	return results, nil
}

func archiveLogTail(path, archivePath string, original os.FileInfo, bytesToKeep int64) error {
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o755); err != nil {
		return fmt.Errorf("create log archive directory: %w", err)
	}
	input, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open log %q: %w", path, err)
	}
	defer input.Close()
	if _, err = input.Seek(original.Size()-bytesToKeep, io.SeekStart); err != nil {
		return fmt.Errorf("seek log %q: %w", path, err)
	}

	output, err := os.OpenFile(archivePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create log archive %q: %w", archivePath, err)
	}
	archiveReady := false
	defer func() {
		if !archiveReady {
			_ = os.Remove(archivePath)
		}
	}()
	compressed := gzip.NewWriter(output)
	if _, err = io.Copy(compressed, io.LimitReader(input, bytesToKeep)); err != nil {
		_ = compressed.Close()
		_ = output.Close()
		return fmt.Errorf("archive log %q: %w", path, err)
	}
	if err = compressed.Close(); err != nil {
		_ = output.Close()
		return fmt.Errorf("finish log archive %q: %w", path, err)
	}
	if err = output.Sync(); err != nil {
		_ = output.Close()
		return fmt.Errorf("sync log archive %q: %w", path, err)
	}
	if err = output.Close(); err != nil {
		return fmt.Errorf("close log archive %q: %w", path, err)
	}
	archiveReady = true

	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("recheck log %q: %w", path, err)
	}
	if !os.SameFile(original, current) {
		return fmt.Errorf("log %q rotated while it was archived; refusing to truncate", path)
	}
	if err = os.Truncate(path, 0); err != nil {
		return fmt.Errorf("truncate log %q: %w", path, err)
	}
	return nil
}

func safeArchiveName(name string) string {
	name = safeName(name)
	name = strings.Trim(name, "._-")
	if name == "" {
		return "log"
	}
	return name
}
