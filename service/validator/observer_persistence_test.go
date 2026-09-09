package validator

import (
	"errors"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestObserverPersistenceFailureWithoutLogger(t *testing.T) {
	t.Run("storage callback", func(t *testing.T) {
		storage := newRuntimeTestStorage()
		var complete func(error)
		storage.saveHook = func(_ SessionStorageID, _ CandidateRecord, done func(error)) {
			complete = done
		}
		resolver := newResolverForTest(storage, nil, 1, simplex.DefaultParams())
		defer resolver.cancel()
		id := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x33}}
		artifact := &CandidateArtifact{Candidate: simplex.Candidate{ID: id}}
		if err := resolver.stage(artifact, []byte{1}); err != nil {
			t.Fatal(err)
		}
		resolver.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
		runtime := &sessionRuntime{candidates: resolver}

		runtime.persistNotarizedCandidate(id)
		if complete == nil {
			t.Fatal("observer did not submit the candidate without a logger")
		}
		complete(errors.New("disk write failed"))

		// A failed background save must leave finalization able to retry it.
		complete = nil
		runtime.persistNotarizedCandidate(id)
		if complete == nil || storage.saveCount() != 2 {
			t.Fatal("failed candidate persistence could not be retried")
		}
		complete(nil)
		resolver.close()
	})

	t.Run("submission error", func(t *testing.T) {
		storage := newRuntimeTestStorage()
		resolver := newResolverForTest(storage, nil, 1, simplex.DefaultParams())
		defer resolver.close()
		id := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x33}}
		resolver.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
		runtime := &sessionRuntime{candidates: resolver}

		// The certificate is known, but its candidate payload is unavailable.
		runtime.persistNotarizedCandidate(id)
		if storage.saveCount() != 0 {
			t.Fatal("observer submitted an unavailable candidate")
		}
	})
}
