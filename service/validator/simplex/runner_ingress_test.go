package simplex

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
)

// These queue tests use certificate envelopes without valid contents: queue
// admission checks the envelope, while the engine still owns authentication.
func ingressTestWire(size int) []byte {
	wire := make([]byte, size)
	binary.LittleEndian.PutUint32(wire, idCertificate)
	return wire
}

func TestRunnerRejectsInvalidEnvelopesBeforeAllocation(t *testing.T) {
	env := newTestEnv(t)
	runner := NewRunner(env.eng)
	vote := make([]byte, signedVoteWireMax)
	binary.LittleEndian.PutUint32(vote, idSignedVote)
	oversizedVote := make([]byte, signedVoteWireMax+1)
	binary.LittleEndian.PutUint32(oversizedVote, idSignedVote)
	for _, tc := range []struct {
		name      string
		validator int
		wire      []byte
	}{
		{"truncated", 0, []byte{1, 2, 3}},
		{"unknown", 0, make([]byte, 16)},
		{"oversized vote", 0, oversizedVote},
		{"oversized certificate", 0, ingressTestWire(env.eng.maxCertWire + 1)},
		{"observer vote", ObserverIndex, vote},
		{"unknown validator", len(env.eng.validators()), vote},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if allocs := testing.AllocsPerRun(100, func() {
				runner.HandleMessage(peer(0), tc.validator, tc.wire)
			}); allocs != 0 {
				t.Fatalf("rejected envelope allocated %g times", allocs)
			}
		})
	}
	if runner.messageCount != 0 || len(runner.messages) != 0 || len(runner.messagePeers) != 0 {
		t.Fatal("invalid envelopes occupied the ingress queue")
	}
	runner.publishStats()
	if runner.StatsSnapshot().MessagesDropped == 0 {
		t.Fatal("ingress drops are missing from published stats")
	}
}

func TestRunnerPeerFloodLeavesRoomForOtherPeersAndCompletions(t *testing.T) {
	env := newTestEnv(t)
	runner := NewRunner(env.eng)
	wire := ingressTestWire(128)
	for range runnerMaxPeerMessages * 4 {
		runner.HandleMessage(peer(0), 0, wire)
	}
	runner.HandleMessage(peer(1), 1, wire)
	if runner.messageCount != runnerMaxPeerMessages+1 {
		t.Fatalf("one peer's flood retained %d messages", runner.messageCount)
	}

	var order []string
	env.eng.post(func() { order = append(order, "journal") })
	runner.postPriority(func() { order = append(order, "candidate") })
	callbacks, messages, stopped, more := runner.takeBatch()
	if stopped || !more || len(callbacks) != 2 || len(messages) != runnerBatchSize-2 {
		t.Fatalf("batch callbacks=%d messages=%d stopped=%t more=%t", len(callbacks), len(messages), stopped, more)
	}
	for _, callback := range callbacks {
		callback()
	}
	if len(order) != 2 || order[0] != "candidate" || order[1] != "journal" {
		t.Fatalf("internal completions delayed by ingress: %v", order)
	}

	// Taking a batch must not free its budget while signature verification
	// can still retain those buffers.
	runner.HandleMessage(peer(0), 0, wire)
	if runner.messageCount != runnerMaxPeerMessages+1 {
		t.Fatal("detached messages no longer count against the peer budget")
	}
	runner.releaseMessages(messages)
	runner.HandleMessage(peer(0), 0, wire)
	if runner.messageCount != 4 {
		t.Fatalf("consumed peer budget was not reusable: %d messages", runner.messageCount)
	}
	for _, message := range messages {
		if message.data != nil {
			t.Fatal("consumed batch retained message payloads")
		}
	}
}

func TestRunnerGlobalIngressCountAndShutdown(t *testing.T) {
	runner := NewRunner(&Engine{maxCertWire: 128})
	wire := ingressTestWire(128)
	for i := range runnerMaxQueuedMessages + 32 {
		var source PeerID
		binary.LittleEndian.PutUint32(source[:], uint32(i))
		runner.HandleMessage(source, ObserverIndex, wire)
	}
	if runner.messageCount != runnerMaxQueuedMessages {
		t.Fatalf("global queue retained %d messages", runner.messageCount)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		runner.HandleMessage(peer(0), ObserverIndex, wire)
	}); allocs != 0 {
		t.Fatalf("full queue allocated %g times", allocs)
	}
	runner.stopped = true
	runner.HandleMessage(peer(0), ObserverIndex, wire)
	_, messages, stopped, more := runner.takeBatch()
	if !stopped || more || len(messages) != 0 || runner.messageCount != 0 || runner.messageBytes != 0 || len(runner.messagePeers) != 0 {
		t.Fatal("shutdown retained ingress messages or budgets")
	}
}

func TestRunnerIngressByteBudgetsIncludeDetachedBatches(t *testing.T) {
	const messageSize = 64 << 10
	runner := NewRunner(&Engine{maxCertWire: messageSize})
	wire := ingressTestWire(messageSize)
	perPeer := runnerMaxPeerMessageBytes / len(wire)
	peerCount := runnerMaxQueuedMessageBytes / runnerMaxPeerMessageBytes
	for i := range peerCount {
		for range perPeer + 1 {
			runner.HandleMessage(peer(i), ObserverIndex, wire)
		}
	}
	if runner.messageBytes != runnerMaxQueuedMessageBytes || runner.messageCount != peerCount*perPeer {
		t.Fatalf("retained %d bytes in %d messages", runner.messageBytes, runner.messageCount)
	}
	_, messages, _, more := runner.takeBatch()
	if more {
		t.Fatal("byte-limited queue did not fit in one batch")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		runner.HandleMessage(peer(peerCount), ObserverIndex, wire)
	}); allocs != 0 {
		t.Fatalf("detached full byte budget allocated %g times", allocs)
	}
	runner.releaseMessages(messages)
	if runner.messageCount != 0 || runner.messageBytes != 0 || len(runner.messagePeers) != 0 {
		t.Fatal("consumed batch retained byte budgets")
	}
	runner.HandleMessage(peer(peerCount), ObserverIndex, wire)
	if runner.messageCount != 1 {
		t.Fatal("released byte budget was not reusable")
	}
}

func TestRunnerOwnsAdmittedMessageBuffers(t *testing.T) {
	runner := NewRunner(&Engine{maxCertWire: 128})
	wire := ingressTestWire(128)
	want := bytes.Clone(wire)
	runner.HandleMessage(peer(0), ObserverIndex, wire)
	clear(wire)
	_, messages, stopped, more := runner.takeBatch()
	if stopped || more || len(messages) != 1 || !bytes.Equal(messages[0].data, want) {
		t.Fatal("caller changed an admitted message's contents")
	}
	runner.releaseMessages(messages)
}

func TestRunnerIngressAllowsOneRosterSizedCertificate(t *testing.T) {
	const certificateSize = runnerMaxQueuedMessageBytes + 1
	runner := NewRunner(&Engine{maxCertWire: certificateSize})
	wire := ingressTestWire(certificateSize)
	runner.HandleMessage(peer(0), ObserverIndex, wire)
	if runner.messageCount != 1 || runner.messageBytes != certificateSize {
		t.Fatal("byte budgets exclude a roster-sized certificate")
	}
	runner.HandleMessage(peer(1), ObserverIndex, wire)
	if runner.messageCount != 1 {
		t.Fatal("large-roster byte budget admitted another certificate")
	}
	_, messages, _, _ := runner.takeBatch()
	runner.releaseMessages(messages)
}

func TestRunnerConcurrentIngressRespectsBudgets(t *testing.T) {
	runner := NewRunner(&Engine{maxCertWire: 128})
	wire := ingressTestWire(128)
	var workers sync.WaitGroup
	for i := range 32 {
		workers.Go(func() {
			for range runnerMaxPeerMessages * 2 {
				runner.HandleMessage(peer(i), ObserverIndex, wire)
			}
		})
	}
	workers.Wait()
	if runner.messageCount != runnerMaxQueuedMessages || runner.messageBytes != len(wire)*runnerMaxQueuedMessages {
		t.Fatalf("concurrent ingress retained %d messages / %d bytes", runner.messageCount, runner.messageBytes)
	}
	for source, budget := range runner.messagePeers {
		if budget.count > runnerMaxPeerMessages || budget.bytes > runnerMaxPeerMessageBytes {
			t.Fatalf("peer %x exceeded its budget: %+v", source, budget)
		}
	}
}

func TestRunnerStoppedStatsRemainZeroAfterIngressDrops(t *testing.T) {
	env := newTestEnv(t)
	runner := NewRunner(env.eng)
	runner.HandleMessage(peer(0), 0, nil)
	runner.stopped = true
	close(runner.done)
	if got := runner.Stats(); got.MessagesDropped != 0 {
		t.Fatalf("stopped runner returned nonzero stats: %+v", got)
	}
}
