package metrics

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	storageArtifactRefreshInterval = time.Minute
	storageArtifactRetryInterval   = 10 * time.Second
)

type storageArtifactStatus struct {
	ArchivePackageBytes  uint64
	PersistentStateBytes uint64
}

type storageArtifactStatusReader struct {
	archivePackagesDir string
	stateFilesDir      string

	mu          sync.Mutex
	nextRefresh time.Time
	status      storageArtifactStatus
}

type storageArtifactCollector struct {
	metrics *Metrics

	archivePackageBytes  *prometheus.Desc
	persistentStateBytes *prometheus.Desc
}

func newStorageArtifactStatusReader(archivePackagesDir string, stateFilesDir string) *storageArtifactStatusReader {
	return &storageArtifactStatusReader{
		archivePackagesDir: archivePackagesDir,
		stateFilesDir:      stateFilesDir,
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	status, err := c.metrics.storageArtifactStatus(ctx)
	if err != nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(c.archivePackageBytes, prometheus.GaugeValue, float64(status.ArchivePackageBytes))
	ch <- prometheus.MustNewConstMetric(c.persistentStateBytes, prometheus.GaugeValue, float64(status.PersistentStateBytes))
}

func (r *storageArtifactStatusReader) Status(ctx context.Context) (storageArtifactStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if now.Before(r.nextRefresh) {
		return r.status, nil
	}

	status, err := scanStorageArtifacts(ctx, r.archivePackagesDir, r.stateFilesDir)
	if err != nil {
		r.nextRefresh = now.Add(storageArtifactRetryInterval)
		return r.status, err
	}
	r.status = status
	r.nextRefresh = now.Add(storageArtifactRefreshInterval)
	return status, nil
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
