package state

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/xssnick/tonutils-go/ton"
)

type proofChainDownloadSource struct {
	Source
	download func(context.Context, ton.BlockIDExt, bool) ([]byte, error)
}

func (s *proofChainDownloadSource) MasterchainProof(ctx context.Context, block ton.BlockIDExt, requireKey bool) ([]byte, error) {
	return s.download(ctx, block, requireKey)
}

func TestSyncerDownloadKeyBlockProofBatchUsesFourWorkersAndKeepsOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocks := make([]ton.BlockIDExt, keyBlockLookupLimit)
		for idx := range blocks {
			blocks[idx] = testStateBlock(-1, topShard, uint32(idx+1))
		}

		started := make(chan ton.BlockIDExt, len(blocks))
		release := make(chan struct{})
		source := &proofChainDownloadSource{
			download: func(ctx context.Context, block ton.BlockIDExt, requireKey bool) ([]byte, error) {
				if !requireKey {
					return nil, errors.New("key block proof download did not require a key block")
				}
				started <- block

				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
				}

				return []byte{byte(block.SeqNo)}, nil
			},
		}
		syncer := NewSyncer(source, nil, SyncerOptions{})

		done := make(chan []keyBlockProofDownload, 1)
		go func() {
			done <- syncer.downloadKeyBlockProofBatch(context.Background(), blocks)
		}()

		synctest.Wait()
		if got := len(started); got != keyBlockProofDownloadWorkers {
			t.Fatalf("concurrent proof downloads = %d, want %d", got, keyBlockProofDownloadWorkers)
		}

		close(release)
		synctest.Wait()

		downloads := <-done
		if got := len(started); got != len(blocks) {
			t.Fatalf("proof downloads = %d, want %d", got, len(blocks))
		}
		for idx, download := range downloads {
			if download.err != nil {
				t.Fatalf("download proof %d: %v", idx, download.err)
			}
			if len(download.data) != 1 || download.data[0] != byte(blocks[idx].SeqNo) {
				t.Fatalf("download proof %d data = %x, want seqno %d", idx, download.data, blocks[idx].SeqNo)
			}
		}
	})
}

func TestSyncerDownloadKeyBlockProofBatchStopsAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocks := make([]ton.BlockIDExt, keyBlockLookupLimit)
		for idx := range blocks {
			blocks[idx] = testStateBlock(-1, topShard, uint32(idx+1))
		}

		started := make(chan struct{}, len(blocks))
		source := &proofChainDownloadSource{
			download: func(ctx context.Context, _ ton.BlockIDExt, _ bool) ([]byte, error) {
				started <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		syncer := NewSyncer(source, nil, SyncerOptions{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan []keyBlockProofDownload, 1)
		go func() {
			done <- syncer.downloadKeyBlockProofBatch(ctx, blocks)
		}()

		synctest.Wait()
		if got := len(started); got != keyBlockProofDownloadWorkers {
			t.Fatalf("proof downloads before cancellation = %d, want %d", got, keyBlockProofDownloadWorkers)
		}

		cancel()
		synctest.Wait()

		downloads := <-done
		if got := len(started); got != keyBlockProofDownloadWorkers {
			t.Fatalf("proof source calls after cancellation = %d, want %d", got, keyBlockProofDownloadWorkers)
		}
		for idx, download := range downloads {
			if !errors.Is(download.err, context.Canceled) {
				t.Fatalf("download proof %d error = %v, want context canceled", idx, download.err)
			}
		}
	})
}

func TestSyncerVerifyKeyBlockBatchHandlesDownloadResultsInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := testStateBlock(-1, topShard, 10)
		second := testStateBlock(-1, topShard, 20)
		firstRelease := make(chan struct{})
		secondReturned := make(chan struct{})
		firstErr := errors.New("first proof unavailable")
		secondErr := errors.New("second proof unavailable")

		source := &proofChainDownloadSource{
			download: func(_ context.Context, block ton.BlockIDExt, _ bool) ([]byte, error) {
				switch block.SeqNo {
				case first.SeqNo:
					<-firstRelease
					return nil, firstErr
				case second.SeqNo:
					close(secondReturned)
					return nil, secondErr
				default:
					return nil, errors.New("unexpected key block")
				}
			},
		}
		syncer := NewSyncer(source, nil, SyncerOptions{})
		trusted := trustedKeyBlock{block: testStateBlock(-1, topShard, 0)}
		candidates := []keyBlockCandidate{}
		verified := 0

		type verifyResult struct {
			advanced bool
			err      error
		}
		done := make(chan verifyResult, 1)
		go func() {
			advanced, err := syncer.verifyKeyBlockBatch(
				context.Background(),
				&trusted,
				&candidates,
				[]ton.BlockIDExt{first, second},
				&verified,
			)
			done <- verifyResult{advanced: advanced, err: err}
		}()

		synctest.Wait()
		select {
		case <-secondReturned:
		default:
			t.Fatal("second proof download did not finish while the first was pending")
		}
		select {
		case result := <-done:
			t.Fatalf("verification returned before the first proof download: %v", result.err)
		default:
		}

		close(firstRelease)
		synctest.Wait()

		result := <-done
		if result.advanced {
			t.Fatal("verification advanced after the first proof download failed")
		}
		if !errors.Is(result.err, firstErr) {
			t.Fatalf("verification error = %v, want first proof error", result.err)
		}
		if errors.Is(result.err, secondErr) {
			t.Fatalf("verification returned later proof error: %v", result.err)
		}
	})
}
