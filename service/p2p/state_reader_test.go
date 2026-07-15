package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

var _ io.WriterTo = (*persistentStateChunkReader)(nil)

// newChunkReaderForTest builds a reader whose first chunk is pre-seeded (like
// the probe chunk) and whose remaining chunks arrive through the results
// channel out of order.
func newChunkReaderForTest(t *testing.T, chunks [][]byte, size int64) *persistentStateChunkReader {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r := &persistentStateChunkReader{
		ctx:          ctx,
		cancel:       cancel,
		node:         &Node{log: zerolog.Nop()},
		peer:         &overlayPeer{addr: "test-peer"},
		blockRef:     "test-block",
		size:         size,
		chunkCount:   int64(len(chunks)),
		chunks:       make(map[int64]bufferedPersistentStateChunk),
		window:       make(chan struct{}, len(chunks)),
		hash:         sha256.New(),
		lastProgress: time.Now(),
	}
	if err := r.addDownloadedChunk(stateChunkResult{offset: 0, data: chunks[0]}, false); err != nil {
		t.Fatalf("seed first chunk: %v", err)
	}

	results := make(chan stateChunkResult, len(chunks))
	for i := len(chunks) - 1; i >= 1; i-- {
		r.window <- struct{}{}
		results <- stateChunkResult{offset: int64(i) * persistentStateChunkSize, data: chunks[i]}
	}
	close(results)
	r.results = results
	return r
}

func testChunkBytes(seed byte, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = seed + byte(i%251)
	}
	return data
}

func TestPersistentStateChunkReaderWriteToMatchesRead(t *testing.T) {
	chunks := [][]byte{
		testChunkBytes(0x11, 64<<10),
		testChunkBytes(0x22, 32<<10),
		testChunkBytes(0x33, 5),
	}
	var want []byte
	for _, chunk := range chunks {
		want = append(want, chunk...)
	}
	size := int64(len(want))

	readReader := newChunkReaderForTest(t, chunks, size)
	readOut, err := io.ReadAll(readReader)
	if err != nil {
		t.Fatalf("sequential Read failed: %v", err)
	}
	if !bytes.Equal(readOut, want) {
		t.Fatalf("sequential Read produced %d bytes, want %d matching bytes", len(readOut), len(want))
	}

	writeReader := newChunkReaderForTest(t, chunks, size)
	var writeOut bytes.Buffer
	written, err := writeReader.WriteTo(&writeOut)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if written != size {
		t.Fatalf("WriteTo wrote %d bytes, want %d", written, size)
	}
	if !bytes.Equal(writeOut.Bytes(), readOut) {
		t.Fatal("WriteTo output differs from sequential Read output")
	}
	if !bytes.Equal(writeReader.FileHash(), readReader.FileHash()) {
		t.Fatal("WriteTo and Read produced different file hashes")
	}
	if !bytes.Equal(writeReader.Prefix(), readReader.Prefix()) {
		t.Fatal("WriteTo and Read captured different prefixes")
	}

	if n, err := writeReader.WriteTo(io.Discard); err != nil || n != 0 {
		t.Fatalf("drained WriteTo = (%d, %v), want (0, nil)", n, err)
	}
	if _, err := writeReader.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("Read after WriteTo drain = %v, want io.EOF", err)
	}
}

func TestPersistentStateChunkReaderWriteToViaIOCopy(t *testing.T) {
	chunks := [][]byte{
		testChunkBytes(0x41, 4<<10),
		testChunkBytes(0x42, 4<<10),
	}
	var want []byte
	for _, chunk := range chunks {
		want = append(want, chunk...)
	}
	size := int64(len(want))

	reader := newChunkReaderForTest(t, chunks, size)
	var out bytes.Buffer
	copied, err := io.Copy(&out, reader)
	if err != nil {
		t.Fatalf("io.Copy failed: %v", err)
	}
	if copied != size {
		t.Fatalf("io.Copy copied %d bytes, want %d", copied, size)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatal("io.Copy output does not match expected chunk bytes")
	}
}

func TestPersistentStateChunkCount(t *testing.T) {
	tests := []struct {
		name string
		size int64
		want int64
	}{
		{name: "zero", size: 0},
		{name: "negative", size: -1},
		{name: "single byte", size: 1, want: 1},
		{name: "one chunk", size: persistentStateChunkSize, want: 1},
		{name: "one chunk plus one", size: persistentStateChunkSize + 1, want: 2},
		{name: "large", size: 1 << 40, want: 1 << 19},
		{name: "max int64", size: math.MaxInt64, want: 1 << 42},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := persistentStateChunkCount(test.size)
			if test.size <= 0 {
				if err == nil {
					t.Fatalf("persistentStateChunkCount(%d) succeeded, want error", test.size)
				}
				return
			}
			if err != nil {
				t.Fatalf("persistentStateChunkCount(%d): %v", test.size, err)
			}
			if got != test.want {
				t.Fatalf("persistentStateChunkCount(%d) = %d, want %d", test.size, got, test.want)
			}
		})
	}
}

func TestPreparePersistentStateCandidateValidatesAdvertisedSize(t *testing.T) {
	tests := []struct {
		name           string
		size           int64
		wantErr        bool
		wantChunkCount int64
		wantWorkers    int
	}{
		{name: "negative", size: -1, wantErr: true},
		{name: "zero", size: 0, wantErr: true},
		{name: "one byte", size: 1, wantChunkCount: 1, wantWorkers: 1},
		{name: "max int64", size: math.MaxInt64, wantChunkCount: 1 << 42, wantWorkers: persistentStateChunkWorkers},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rldpClient := &testArchiveRLDP{
				adnl:        newTestOverlayADNL(),
				queryResult: PersistentStateSize{Size: test.size},
			}
			downloader, peer, _ := testPersistentStateChunkDownloader(rldpClient)
			peer.overlay = &overlay.ADNLOverlayWrapper{}

			candidate, err := downloader.preparePersistentStateCandidate(context.Background(), peer, topShard)
			if test.wantErr {
				if err == nil {
					t.Fatalf("advertised size %d accepted, want error", test.size)
				}
				return
			}
			if err != nil {
				t.Fatalf("prepare candidate for advertised size %d: %v", test.size, err)
			}
			if candidate.chunkCount != test.wantChunkCount {
				t.Fatalf("chunk count = %d, want %d", candidate.chunkCount, test.wantChunkCount)
			}
			if candidate.workers != test.wantWorkers {
				t.Fatalf("workers = %d, want %d", candidate.workers, test.wantWorkers)
			}
		})
	}
}

func TestPersistentStateChunkReaderBoundsConcurrentBuffering(t *testing.T) {
	const (
		workers = 4
		chunks  = 16
	)

	rldpClient := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		asyncResult: bytes.Repeat([]byte{0x5a}, persistentStateChunkSize),
		asyncDelays: map[int64]time.Duration{
			persistentStateChunkSize: 50 * time.Millisecond,
		},
	}
	downloader, peer, id := testPersistentStateChunkDownloader(rldpClient)
	reader, err := downloader.newPersistentStateChunkReader(
		context.Background(),
		peer,
		id,
		chunks*persistentStateChunkSize,
		workers,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("create persistent state chunk reader: %v", err)
	}
	defer reader.Close()
	wantWindow := workers * persistentStateChunkWindowScale

	deadline := time.Now().Add(time.Second)
	for len(rldpClient.snapshot().asyncQueries) < wantWindow+1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)

	snapshot := rldpClient.snapshot()
	if got := len(snapshot.asyncQueries); got != wantWindow+1 {
		t.Fatalf("queries before consuming = %d, want first probe + %d-chunk window", got, wantWindow)
	}
	if got := len(reader.window); got != wantWindow {
		t.Fatalf("scheduled chunks before consuming = %d, want bounded window %d", got, wantWindow)
	}
	if cap(reader.window) != wantWindow {
		t.Fatalf("reorder window capacity = %d, want %d", cap(reader.window), wantWindow)
	}

	written, err := io.Copy(io.Discard, reader)
	if err != nil {
		t.Fatalf("stream persistent state: %v", err)
	}
	wantSize := int64(chunks * persistentStateChunkSize)
	if written != wantSize {
		t.Fatalf("streamed bytes = %d, want %d", written, wantSize)
	}

	snapshot = rldpClient.snapshot()
	if len(snapshot.asyncQueries) != chunks {
		t.Fatalf("total chunk queries = %d, want %d", len(snapshot.asyncQueries), chunks)
	}
	if snapshot.asyncMaxActive > workers+1 {
		t.Fatalf("max concurrent chunk queries including the initial probe = %d, want <= %d", snapshot.asyncMaxActive, workers+1)
	}
	if len(snapshot.asyncCompleted) < workers+1 || snapshot.asyncCompleted[1] == persistentStateChunkSize {
		t.Fatalf("test did not exercise out-of-order completion: %v", snapshot.asyncCompleted)
	}
}

func TestPersistentStateChunkReaderMaxInt64UsesBoundedState(t *testing.T) {
	const workers = 2

	rldpClient := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		asyncResult: bytes.Repeat([]byte{0xa5}, persistentStateChunkSize),
		asyncWaitForCancel: map[int64]bool{
			persistentStateChunkSize:     true,
			2 * persistentStateChunkSize: true,
		},
	}
	downloader, peer, id := testPersistentStateChunkDownloader(rldpClient)
	reader, err := downloader.newPersistentStateChunkReader(
		context.Background(),
		peer,
		id,
		math.MaxInt64,
		workers,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("create max-size persistent state chunk reader: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for len(rldpClient.snapshot().asyncQueries) < workers+1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	snapshot := rldpClient.snapshot()
	if got := len(snapshot.asyncQueries); got != workers+1 {
		reader.Close()
		t.Fatalf("max-size reader scheduled %d queries, want bounded %d", got, workers+1)
	}
	if reader.chunkCount != 1<<42 {
		reader.Close()
		t.Fatalf("max-size chunk count = %d, want %d", reader.chunkCount, int64(1<<42))
	}
	if cap(reader.window) != workers*persistentStateChunkWindowScale {
		reader.Close()
		t.Fatalf("max-size reorder window capacity = %d, want %d", cap(reader.window), workers*persistentStateChunkWindowScale)
	}
	if snapshot.asyncActive != workers {
		reader.Close()
		t.Fatalf("active max-size queries before Close = %d, want %d", snapshot.asyncActive, workers)
	}
	queriesBeforeClose := len(snapshot.asyncQueries)
	reader.Close()

	deadline = time.Now().Add(time.Second)
	for rldpClient.snapshot().asyncActive != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	snapshot = rldpClient.snapshot()
	if snapshot.asyncActive != 0 {
		t.Fatalf("max-size reader left %d active queries after Close", snapshot.asyncActive)
	}
	if len(snapshot.asyncQueries) != queriesBeforeClose {
		t.Fatalf("max-size reader scheduled queries after Close: before=%d after=%d", queriesBeforeClose, len(snapshot.asyncQueries))
	}
}

func TestPersistentStateDownloadRejectsSizeAboveDiskBudget(t *testing.T) {
	node := &Node{
		log:           zerolog.Nop(),
		stateFilesDir: t.TempDir(),
	}

	release, err := node.reservePersistentStateDownload(math.MaxInt64)
	if err == nil {
		release(false)
		t.Fatal("max-int64 persistent state size was accepted by the disk budget")
	}
	if !errors.Is(err, errPersistentStateDownloadDiskBudget) {
		t.Fatalf("max-int64 persistent state size failed with %v, want disk budget error", err)
	}
	if node.stateDownloadReserved != 0 {
		t.Fatalf("rejected persistent state left %d reserved bytes", node.stateDownloadReserved)
	}
}

func TestPersistentStateDownloadReservationKeepsRetainedBytesDuringOverlap(t *testing.T) {
	node := &Node{
		log:           zerolog.Nop(),
		stateFilesDir: t.TempDir(),
	}

	releaseFirst, err := node.reservePersistentStateDownload(1)
	if err != nil {
		t.Fatalf("reserve first persistent state download: %v", err)
	}
	budget := node.stateDownloadBudget
	if budget < 2 || budget > math.MaxInt64 {
		releaseFirst(false)
		t.Skipf("filesystem budget %d cannot exercise overlap accounting", budget)
	}
	releaseSecond, err := node.reservePersistentStateDownload(int64(budget - 1))
	if err != nil {
		releaseFirst(false)
		t.Fatalf("reserve remaining persistent state budget: %v", err)
	}

	releaseFirst(true)
	if release, reserveErr := node.reservePersistentStateDownload(1); !errors.Is(reserveErr, errPersistentStateDownloadDiskBudget) {
		if reserveErr == nil {
			release(false)
		}
		releaseSecond(false)
		t.Fatalf("reserve beyond retained epoch budget = %v, want disk budget error", reserveErr)
	}

	releaseSecond(false)
	if node.stateDownloadActive != 0 || node.stateDownloadReserved != 0 || node.stateDownloadBudget != 0 {
		t.Fatalf("released download epoch left active=%d reserved=%d budget=%d", node.stateDownloadActive, node.stateDownloadReserved, node.stateDownloadBudget)
	}
}

func testPersistentStateChunkDownloader(rldpClient *testArchiveRLDP) (persistentStateSnapshotDownloader, *overlayPeer, PersistentStateIDV2) {
	block := testBlockID(-1, topShard, 42)
	node := &Node{log: zerolog.Nop()}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  zerolog.Nop(),
		node: node,
		spec: overlaySpec{ShortID: []byte{0x01}},
	})
	peer := &overlayPeer{
		addr:        "state-reader-peer",
		rldpOverlay: overlay.CreateExtendedRLDP(rldpClient).CreateOverlay([]byte{0x01}),
	}
	id := PersistentStateIDV2{
		Block:            block,
		MasterchainBlock: block,
		EffectiveShard:   topShard,
	}
	return persistentStateSnapshotDownloader{
		node:   node,
		sub:    sub,
		block:  block,
		master: block,
	}, peer, id
}
