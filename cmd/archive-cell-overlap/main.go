package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	defaultCheckpointBytes         = 512 << 20
	defaultArchiveCheckpointBlocks = 2000
	cellKeyBytes                   = 32
)

var archivePackName = regexp.MustCompile(`^archive\.(\d+)(?:\..+)?\.pack$`)

type options struct {
	packagesDir  string
	configPath   string
	windowBytes  uint64
	windowBlocks uint64
	windows      int
	startSeqno   uint64
}

type configFile struct {
	TON struct {
		CheckpointBytes         uint64 `json:"checkpoint_bytes"`
		ArchiveCheckpointBlocks uint64 `json:"archive_checkpoint_blocks"`
	} `json:"ton"`
}

type checkpointLimits struct {
	bytes  uint64
	blocks uint64
}

type packGroup struct {
	seqno uint64
	paths []string
}

type cellAggregate struct {
	size        uint32
	occurrences uint32
}

type cellStat struct {
	hash        cell.Hash
	size        uint32
	occurrences uint32
}

type sliceStats struct {
	seqno                  uint64
	firstMasterSeqno       uint32
	lastMasterSeqno        uint32
	blocks                 uint64
	masterBlocks           uint64
	artifactBytes          uint64
	rawPreparedCells       uint64
	rawPreparedCellBytes   uint64
	logicalCells           uint64
	logicalCellBytes       uint64
	cells                  map[cell.Hash]uint32
	duplicateBlocksSkipped uint64
}

type windowBuilder struct {
	number               int
	limits               checkpointLimits
	firstSliceSeqno      uint64
	lastSliceSeqno       uint64
	firstMasterSeqno     uint32
	lastMasterSeqno      uint32
	slices               uint64
	blocks               uint64
	masterBlocks         uint64
	artifactBytes        uint64
	rawPreparedCells     uint64
	rawPreparedCellBytes uint64
	logicalCells         uint64
	logicalCellBytes     uint64
	cells                map[cell.Hash]cellAggregate
}

type completedWindow struct {
	report windowReport
	cells  []cellStat
}

type windowReport struct {
	Number                         int     `json:"number"`
	FirstSliceSeqno                uint64  `json:"first_slice_seqno"`
	LastSliceSeqno                 uint64  `json:"last_slice_seqno"`
	FirstMasterSeqno               uint32  `json:"first_master_seqno"`
	LastMasterSeqno                uint32  `json:"last_master_seqno"`
	Slices                         uint64  `json:"slices"`
	Blocks                         uint64  `json:"blocks"`
	MasterBlocks                   uint64  `json:"master_blocks"`
	TargetBytes                    uint64  `json:"target_bytes"`
	TargetMasterBlocks             uint64  `json:"target_master_blocks"`
	CompletionReason               string  `json:"completion_reason"`
	PendingBytes                   uint64  `json:"pending_bytes"`
	ArtifactBytes                  uint64  `json:"artifact_bytes"`
	RawPreparedCells               uint64  `json:"raw_prepared_cells"`
	RawPreparedCellBytes           uint64  `json:"raw_prepared_cell_bytes"`
	LogicalCellWrites              uint64  `json:"logical_cell_writes"`
	LogicalCellValueBytes          uint64  `json:"logical_cell_value_bytes"`
	LogicalCellRecordBytes         uint64  `json:"logical_cell_record_bytes"`
	DistinctCells                  uint64  `json:"distinct_cells"`
	DistinctCellValueBytes         uint64  `json:"distinct_cell_value_bytes"`
	RepeatedWritesWithinWindow     uint64  `json:"repeated_writes_within_window"`
	RepeatedValueBytesWithinWindow uint64  `json:"repeated_value_bytes_within_window"`
	RepeatedWritePercent           float64 `json:"repeated_write_percent"`
	RepeatedValueBytePercent       float64 `json:"repeated_value_byte_percent"`
}

type overlapReport struct {
	KnownWindows               []int   `json:"known_windows"`
	TargetWindow               int     `json:"target_window"`
	DistinctIntersectionCells  uint64  `json:"distinct_intersection_cells"`
	DistinctIntersectionBytes  uint64  `json:"distinct_intersection_value_bytes"`
	AvoidableTargetWrites      uint64  `json:"avoidable_target_writes"`
	AvoidableTargetValueBytes  uint64  `json:"avoidable_target_value_bytes"`
	AvoidableTargetRecordBytes uint64  `json:"avoidable_target_record_bytes"`
	TargetWritePercent         float64 `json:"target_write_percent"`
	TargetValueBytePercent     float64 `json:"target_value_byte_percent"`
	TargetRecordBytePercent    float64 `json:"target_record_byte_percent"`
}

type report struct {
	PackagesDir             string          `json:"packages_dir"`
	ConfigPath              string          `json:"config_path"`
	CheckpointBytes         uint64          `json:"checkpoint_bytes"`
	ArchiveCheckpointBlocks uint64          `json:"archive_checkpoint_blocks"`
	RequestedWindows        int             `json:"requested_windows"`
	StartedAt               time.Time       `json:"started_at"`
	Elapsed                 time.Duration   `json:"elapsed"`
	DuplicateBlocksSkipped  uint64          `json:"duplicate_blocks_skipped"`
	Windows                 []windowReport  `json:"windows"`
	Pairwise                []overlapReport `json:"pairwise"`
	Cumulative              []overlapReport `json:"cumulative"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	opts := parseFlags()
	limits, err := loadCheckpointLimits(opts)
	if err != nil {
		return err
	}
	groups, err := collectPackGroups(opts.packagesDir, opts.startSeqno)
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		return fmt.Errorf("no archive packs found in %s", opts.packagesDir)
	}

	started := time.Now()
	ctx := context.Background()
	windows := make([]completedWindow, 0, opts.windows)
	current := newWindowBuilder(1, limits)
	seenBlocks := make(map[storage.BlockRootHash]struct{})
	var duplicateBlocksSkipped uint64

	for _, group := range groups {
		slice, err := readSlice(ctx, group, seenBlocks)
		if err != nil {
			return fmt.Errorf("read archive slice %d: %w", group.seqno, err)
		}
		duplicateBlocksSkipped += slice.duplicateBlocksSkipped
		if err = current.addSlice(slice); err != nil {
			return fmt.Errorf("add archive slice %d: %w", group.seqno, err)
		}

		fmt.Fprintf(
			os.Stderr,
			"window=%d slice=%d files=%d master_blocks=%d/%d pending=%d/%d logical_cells=%d distinct_cells=%d elapsed=%s\n",
			current.number,
			group.seqno,
			len(group.paths),
			current.masterBlocksSpan(),
			current.limits.blocks,
			current.pendingBytes(),
			current.limits.bytes,
			current.logicalCells,
			len(current.cells),
			time.Since(started).Round(time.Second),
		)

		if current.completionReason() == "" {
			continue
		}

		windows = append(windows, current.complete())
		fmt.Fprintf(os.Stderr, "completed window=%d pending=%d distinct_cells=%d\n", current.number, current.pendingBytes(), len(windows[len(windows)-1].cells))
		if len(windows) == opts.windows {
			break
		}
		current = newWindowBuilder(len(windows)+1, limits)
		runtime.GC()
	}
	if len(windows) != opts.windows {
		return fmt.Errorf("collected %d complete windows, want %d", len(windows), opts.windows)
	}

	result, err := buildReport(opts, limits, started, windows, duplicateBlocksSkipped)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(result); err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return nil
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.packagesDir, "packages", "", "archive package directory to scan")
	flag.StringVar(&opts.configPath, "config", "", "node JSON config used for archive checkpoint limits")
	flag.Uint64Var(&opts.windowBytes, "window-bytes", 0, "override checkpoint window size")
	flag.Uint64Var(&opts.windowBlocks, "window-blocks", 0, "override archive checkpoint masterchain block count")
	flag.IntVar(&opts.windows, "windows", 4, "number of consecutive windows")
	flag.Uint64Var(&opts.startSeqno, "start-seqno", 0, "skip archive slices before this masterchain seqno")
	flag.Parse()

	if opts.packagesDir == "" {
		fmt.Fprintln(os.Stderr, "-packages is required")
		flag.Usage()
		os.Exit(2)
	}
	if opts.windows < 2 {
		fmt.Fprintln(os.Stderr, "-windows should be at least 2")
		os.Exit(2)
	}
	return opts
}

func loadCheckpointLimits(opts options) (checkpointLimits, error) {
	limits := checkpointLimits{
		bytes:  defaultCheckpointBytes,
		blocks: defaultArchiveCheckpointBlocks,
	}
	if opts.configPath != "" {
		data, err := os.ReadFile(opts.configPath)
		if err != nil {
			return checkpointLimits{}, fmt.Errorf("read config: %w", err)
		}
		var cfg configFile
		if err = json.Unmarshal(data, &cfg); err != nil {
			return checkpointLimits{}, fmt.Errorf("decode config: %w", err)
		}
		if cfg.TON.CheckpointBytes > 0 {
			limits.bytes = cfg.TON.CheckpointBytes
		}
		if cfg.TON.ArchiveCheckpointBlocks > 0 {
			limits.blocks = cfg.TON.ArchiveCheckpointBlocks
		}
	}
	if opts.windowBytes > 0 {
		limits.bytes = opts.windowBytes
	}
	if opts.windowBlocks > 0 {
		limits.blocks = opts.windowBlocks
	}
	return limits, nil
}

func collectPackGroups(root string, startSeqno uint64) ([]packGroup, error) {
	groups := make(map[uint64][]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}

		matches := archivePackName.FindStringSubmatch(entry.Name())
		if matches == nil {
			return nil
		}
		seqno, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return fmt.Errorf("parse archive seqno from %s: %w", entry.Name(), err)
		}
		if seqno < startSeqno {
			return nil
		}
		groups[seqno] = append(groups[seqno], path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk package directory: %w", err)
	}

	seqnos := make([]uint64, 0, len(groups))
	for seqno := range groups {
		seqnos = append(seqnos, seqno)
	}
	sort.Slice(seqnos, func(i, j int) bool { return seqnos[i] < seqnos[j] })

	result := make([]packGroup, 0, len(seqnos))
	for _, seqno := range seqnos {
		paths := groups[seqno]
		sort.Strings(paths)
		result = append(result, packGroup{seqno: seqno, paths: paths})
	}
	return result, nil
}

func readSlice(ctx context.Context, group packGroup, seenBlocks map[storage.BlockRootHash]struct{}) (sliceStats, error) {
	result := sliceStats{
		seqno: group.seqno,
		cells: make(map[cell.Hash]uint32),
	}
	for _, path := range group.paths {
		file, err := os.Open(path)
		if err != nil {
			return sliceStats{}, fmt.Errorf("open %s: %w", path, err)
		}
		imported, importErr := archive.ImportStream(ctx, &archive.Downloaded{MasterchainSeqno: uint32(group.seqno)}, file)
		closeErr := file.Close()
		if importErr != nil {
			return sliceStats{}, fmt.Errorf("import %s: %w", path, importErr)
		}
		if closeErr != nil {
			return sliceStats{}, fmt.Errorf("close %s: %w", path, closeErr)
		}

		for _, full := range imported.FullBlocks {
			key := storage.BlockKey(full.ID)
			if _, ok := seenBlocks[key]; ok {
				result.duplicateBlocksSkipped++
				continue
			}
			seenBlocks[key] = struct{}{}

			prepared, ok := imported.PreparedBlocks[key]
			if !ok {
				return sliceStats{}, fmt.Errorf("prepared block %s is missing", storage.FormatBlockRef(full.ID))
			}
			result.blocks++
			result.artifactBytes += uint64(len(full.Block)) + uint64(len(full.Proof))
			if full.ID.Workchain == -1 {
				result.masterBlocks++
				if result.firstMasterSeqno == 0 || full.ID.SeqNo < result.firstMasterSeqno {
					result.firstMasterSeqno = full.ID.SeqNo
				}
				if full.ID.SeqNo > result.lastMasterSeqno {
					result.lastMasterSeqno = full.ID.SeqNo
				}
			}

			records := prepared.StateUpdateToCells
			result.rawPreparedCells += uint64(records.Len())
			result.rawPreparedCellBytes += records.ByteSize()
			if err = records.ForEach(func(record storage.EncodedCellRecord) error {
				if len(record.Data) > math.MaxUint32 {
					return fmt.Errorf("cell %x record is too large: %d", record.Hash, len(record.Data))
				}
				size := uint32(len(record.Data))
				if previous, exists := result.cells[record.Hash]; exists {
					if previous != size {
						return fmt.Errorf("cell %x has record sizes %d and %d", record.Hash, previous, size)
					}
					return nil
				}
				result.cells[record.Hash] = size
				result.logicalCells++
				result.logicalCellBytes += uint64(size)
				return nil
			}); err != nil {
				return sliceStats{}, err
			}
		}
	}
	return result, nil
}

func newWindowBuilder(number int, limits checkpointLimits) *windowBuilder {
	return &windowBuilder{
		number: number,
		limits: limits,
		cells:  make(map[cell.Hash]cellAggregate),
	}
}

func (w *windowBuilder) addSlice(slice sliceStats) error {
	if w.slices == 0 {
		w.firstSliceSeqno = slice.seqno
		w.firstMasterSeqno = slice.firstMasterSeqno
	}
	w.lastSliceSeqno = slice.seqno
	if slice.firstMasterSeqno > 0 && (w.firstMasterSeqno == 0 || slice.firstMasterSeqno < w.firstMasterSeqno) {
		w.firstMasterSeqno = slice.firstMasterSeqno
	}
	if slice.lastMasterSeqno > w.lastMasterSeqno {
		w.lastMasterSeqno = slice.lastMasterSeqno
	}
	w.slices++
	w.blocks += slice.blocks
	w.masterBlocks += slice.masterBlocks
	w.artifactBytes += slice.artifactBytes
	w.rawPreparedCells += slice.rawPreparedCells
	w.rawPreparedCellBytes += slice.rawPreparedCellBytes
	w.logicalCells += slice.logicalCells
	w.logicalCellBytes += slice.logicalCellBytes

	for hash, size := range slice.cells {
		aggregate, exists := w.cells[hash]
		if !exists {
			aggregate.size = size
		} else if aggregate.size != size {
			return fmt.Errorf("cell %x has record sizes %d and %d", hash, aggregate.size, size)
		}
		aggregate.occurrences++
		w.cells[hash] = aggregate
	}
	return nil
}

func (w *windowBuilder) pendingBytes() uint64 {
	return w.logicalCellBytes + w.artifactBytes
}

func (w *windowBuilder) masterBlocksSpan() uint64 {
	if w.firstMasterSeqno == 0 || w.lastMasterSeqno < w.firstMasterSeqno {
		return 0
	}
	return uint64(w.lastMasterSeqno-w.firstMasterSeqno) + 1
}

// completionReason returns "" while the window is still collecting.
func (w *windowBuilder) completionReason() string {
	if w.limits.blocks > 0 && w.masterBlocksSpan() >= w.limits.blocks {
		return "master_blocks"
	}
	if w.limits.bytes > 0 && w.pendingBytes() >= w.limits.bytes {
		return "bytes"
	}
	return ""
}

func (w *windowBuilder) complete() completedWindow {
	cells := make([]cellStat, 0, len(w.cells))
	var distinctBytes uint64
	for hash, aggregate := range w.cells {
		cells = append(cells, cellStat{
			hash:        hash,
			size:        aggregate.size,
			occurrences: aggregate.occurrences,
		})
		distinctBytes += uint64(aggregate.size)
	}
	sort.Slice(cells, func(i, j int) bool {
		return bytes.Compare(cells[i].hash[:], cells[j].hash[:]) < 0
	})

	repeatedWrites := w.logicalCells - uint64(len(cells))
	repeatedBytes := w.logicalCellBytes - distinctBytes
	return completedWindow{
		report: windowReport{
			Number:                         w.number,
			FirstSliceSeqno:                w.firstSliceSeqno,
			LastSliceSeqno:                 w.lastSliceSeqno,
			FirstMasterSeqno:               w.firstMasterSeqno,
			LastMasterSeqno:                w.lastMasterSeqno,
			Slices:                         w.slices,
			Blocks:                         w.blocks,
			MasterBlocks:                   w.masterBlocks,
			TargetBytes:                    w.limits.bytes,
			TargetMasterBlocks:             w.limits.blocks,
			CompletionReason:               w.completionReason(),
			PendingBytes:                   w.pendingBytes(),
			ArtifactBytes:                  w.artifactBytes,
			RawPreparedCells:               w.rawPreparedCells,
			RawPreparedCellBytes:           w.rawPreparedCellBytes,
			LogicalCellWrites:              w.logicalCells,
			LogicalCellValueBytes:          w.logicalCellBytes,
			LogicalCellRecordBytes:         w.logicalCellBytes + cellKeyBytes*w.logicalCells,
			DistinctCells:                  uint64(len(cells)),
			DistinctCellValueBytes:         distinctBytes,
			RepeatedWritesWithinWindow:     repeatedWrites,
			RepeatedValueBytesWithinWindow: repeatedBytes,
			RepeatedWritePercent:           percentage(repeatedWrites, w.logicalCells),
			RepeatedValueBytePercent:       percentage(repeatedBytes, w.logicalCellBytes),
		},
		cells: cells,
	}
}

func buildReport(opts options, limits checkpointLimits, started time.Time, windows []completedWindow, duplicateBlocksSkipped uint64) (report, error) {
	result := report{
		PackagesDir:             opts.packagesDir,
		ConfigPath:              opts.configPath,
		CheckpointBytes:         limits.bytes,
		ArchiveCheckpointBlocks: limits.blocks,
		RequestedWindows:        opts.windows,
		StartedAt:               started,
		Elapsed:                 time.Since(started),
		DuplicateBlocksSkipped:  duplicateBlocksSkipped,
		Windows:                 make([]windowReport, 0, len(windows)),
	}
	for _, window := range windows {
		result.Windows = append(result.Windows, window.report)
	}

	for known := 0; known < len(windows)-1; known++ {
		for target := known + 1; target < len(windows); target++ {
			overlap, err := calculateOverlap([]int{known + 1}, target+1, windows[known].cells, windows[target])
			if err != nil {
				return report{}, err
			}
			result.Pairwise = append(result.Pairwise, overlap)
		}
	}

	knownUnion := windows[0].cells
	knownNumbers := []int{1}
	for target := 1; target < len(windows); target++ {
		overlap, err := calculateOverlap(append([]int(nil), knownNumbers...), target+1, knownUnion, windows[target])
		if err != nil {
			return report{}, err
		}
		result.Cumulative = append(result.Cumulative, overlap)

		knownUnion, err = unionCells(knownUnion, windows[target].cells)
		if err != nil {
			return report{}, err
		}
		knownNumbers = append(knownNumbers, target+1)
	}
	return result, nil
}

func calculateOverlap(knownNumbers []int, targetNumber int, known []cellStat, target completedWindow) (overlapReport, error) {
	result := overlapReport{
		KnownWindows: knownNumbers,
		TargetWindow: targetNumber,
	}
	i, j := 0, 0
	for i < len(known) && j < len(target.cells) {
		cmp := bytes.Compare(known[i].hash[:], target.cells[j].hash[:])
		switch {
		case cmp < 0:
			i++
		case cmp > 0:
			j++
		default:
			if known[i].size != target.cells[j].size {
				return overlapReport{}, fmt.Errorf("cell %x has record sizes %d and %d", known[i].hash, known[i].size, target.cells[j].size)
			}
			occurrences := uint64(target.cells[j].occurrences)
			size := uint64(target.cells[j].size)
			result.DistinctIntersectionCells++
			result.DistinctIntersectionBytes += size
			result.AvoidableTargetWrites += occurrences
			result.AvoidableTargetValueBytes += size * occurrences
			result.AvoidableTargetRecordBytes += (size + cellKeyBytes) * occurrences
			i++
			j++
		}
	}
	result.TargetWritePercent = percentage(result.AvoidableTargetWrites, target.report.LogicalCellWrites)
	result.TargetValueBytePercent = percentage(result.AvoidableTargetValueBytes, target.report.LogicalCellValueBytes)
	result.TargetRecordBytePercent = percentage(result.AvoidableTargetRecordBytes, target.report.LogicalCellRecordBytes)
	return result, nil
}

func unionCells(left, right []cellStat) ([]cellStat, error) {
	result := make([]cellStat, 0, len(left)+len(right))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		cmp := bytes.Compare(left[i].hash[:], right[j].hash[:])
		switch {
		case cmp < 0:
			result = append(result, left[i])
			i++
		case cmp > 0:
			result = append(result, right[j])
			j++
		default:
			if left[i].size != right[j].size {
				return nil, fmt.Errorf("cell %x has record sizes %d and %d", left[i].hash, left[i].size, right[j].size)
			}
			result = append(result, left[i])
			i++
			j++
		}
	}
	result = append(result, left[i:]...)
	result = append(result, right[j:]...)
	return result, nil
}

func percentage(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) * 100 / float64(total)
}
