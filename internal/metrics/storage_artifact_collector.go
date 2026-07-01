package metrics

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type storageArtifactStatus struct {
	ArchivePackageBytes  uint64
	PersistentStateBytes uint64
}

type storageArtifactStatusReader struct {
	mu     sync.RWMutex
	status storageArtifactStatus
}

type storageArtifactCollector struct {
	metrics *Metrics

	archivePackageBytes  *prometheus.Desc
	persistentStateBytes *prometheus.Desc
}

func newStorageArtifactStatusReader(status storageArtifactStatus) *storageArtifactStatusReader {
	return &storageArtifactStatusReader{
		status: status,
	}
}

func newStorageArtifactCollector(metrics *Metrics, namespace string) prometheus.Collector {
	return &storageArtifactCollector{
		metrics: metrics,
		archivePackageBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "archive_package_bytes"),
			"Total bytes used by archive package files on disk.",
			nil,
			nil,
		),
		persistentStateBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "persistent_state_bytes"),
			"Total bytes used by persistent state files on disk.",
			nil,
			nil,
		),
	}
}

func (c *storageArtifactCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.archivePackageBytes
	ch <- c.persistentStateBytes
}

func (c *storageArtifactCollector) Collect(ch chan<- prometheus.Metric) {
	status := c.metrics.storageArtifactStatus()

	ch <- prometheus.MustNewConstMetric(c.archivePackageBytes, prometheus.GaugeValue, float64(status.ArchivePackageBytes))
	ch <- prometheus.MustNewConstMetric(c.persistentStateBytes, prometheus.GaugeValue, float64(status.PersistentStateBytes))
}

func (r *storageArtifactStatusReader) Status() storageArtifactStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.status
}

func (r *storageArtifactStatusReader) AddArchivePackageBytes(delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.status.ArchivePackageBytes = addStorageArtifactBytes(r.status.ArchivePackageBytes, delta)
}

func (r *storageArtifactStatusReader) AddPersistentStateBytes(delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.status.PersistentStateBytes = addStorageArtifactBytes(r.status.PersistentStateBytes, delta)
}

func addStorageArtifactBytes(current uint64, delta int64) uint64 {
	if delta >= 0 {
		return current + uint64(delta)
	}

	removed := uint64(-delta)
	if removed > current {
		// Storage metrics reflect external files. Manual file changes or crash recovery
		// can make a delete event disagree with the startup scan; keep the gauge valid.
		return 0
	}
	return current - removed
}

func scanStorageArtifacts(ctx context.Context, archivePackagesDir string, stateFilesDir string) (storageArtifactStatus, error) {
	var status storageArtifactStatus

	archivePackageBytes, err := scanArchivePackages(ctx, archivePackagesDir)
	if err != nil {
		return storageArtifactStatus{}, err
	}
	persistentStateBytes, err := scanPersistentStates(ctx, stateFilesDir)
	if err != nil {
		return storageArtifactStatus{}, err
	}

	status.ArchivePackageBytes = archivePackageBytes
	status.PersistentStateBytes = persistentStateBytes
	return status, nil
}

func scanArchivePackages(ctx context.Context, dir string) (uint64, error) {
	var bytes uint64

	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		if strings.HasSuffix(path, ".pack") {
			if info.Size() > 0 {
				bytes += uint64(info.Size())
			}
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	return bytes, err
}

func scanPersistentStates(ctx context.Context, dir string) (uint64, error) {
	var bytes uint64

	err := filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		if isPersistentStateFileName(entry.Name()) {
			if info.Size() > 0 {
				bytes += uint64(info.Size())
			}
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	return bytes, err
}

func isPersistentStateFileName(name string) bool {
	switch {
	case strings.HasPrefix(name, "state_"):
		return hasPersistentStateMaster(name[len("state_"):])
	case strings.HasPrefix(name, "statesplit_"):
		return hasPersistentStateMaster(name[len("statesplit_"):])
	case strings.HasPrefix(name, "stateaccount_"):
		return hasPersistentStateMaster(name[len("stateaccount_"):])
	default:
		return false
	}
}

func hasPersistentStateMaster(s string) bool {
	idx := strings.IndexByte(s, '_')
	if idx <= 0 {
		return false
	}
	_, err := strconv.ParseUint(s[:idx], 10, 32)
	return err == nil
}
