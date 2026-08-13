package pebblestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	summaryVersion = byte(1)
	summarySize    = 137
)

var errSessionSummaryMissing = errors.New("validator pebblestore: session summary is missing")

type sessionSummary struct {
	candidates              uint64
	finalized               uint64
	votes                   uint64
	certificates            uint64
	leaderWindows           uint64
	firstNonAnnouncedWindow uint32
	lastFinalized           *simplex.CandidateID
	lastLeaderWindow        *validator.LeaderWindowRecord
}

func encodeSessionSummary(summary sessionSummary) []byte {
	value := make([]byte, 0, summarySize)
	value = append(value, summaryVersion)
	value = binary.LittleEndian.AppendUint64(value, summary.candidates)
	value = binary.LittleEndian.AppendUint64(value, summary.finalized)
	value = binary.LittleEndian.AppendUint64(value, summary.votes)
	value = binary.LittleEndian.AppendUint64(value, summary.certificates)
	value = binary.LittleEndian.AppendUint64(value, summary.leaderWindows)
	value = binary.LittleEndian.AppendUint32(value, summary.firstNonAnnouncedWindow)
	if summary.lastFinalized != nil {
		value = append(value, 1)
		value = appendCandidateID(value, *summary.lastFinalized)
	} else {
		value = append(value, 0)
		value = append(value, make([]byte, 36)...)
	}
	if summary.lastLeaderWindow != nil {
		value = append(value, 1)
		value = append(value, encodeLeaderWindow(*summary.lastLeaderWindow)...)
	} else {
		value = append(value, 0)
		value = append(value, make([]byte, leaderRecordSize)...)
	}

	return value
}

func decodeSessionSummary(value []byte) (sessionSummary, error) {
	if len(value) != summarySize {
		return sessionSummary{}, fmt.Errorf("validator pebblestore: session summary length %d", len(value))
	}
	if value[0] != summaryVersion {
		return sessionSummary{}, fmt.Errorf("validator pebblestore: session summary version %d", value[0])
	}

	summary := sessionSummary{
		candidates:              binary.LittleEndian.Uint64(value[1:9]),
		finalized:               binary.LittleEndian.Uint64(value[9:17]),
		votes:                   binary.LittleEndian.Uint64(value[17:25]),
		certificates:            binary.LittleEndian.Uint64(value[25:33]),
		leaderWindows:           binary.LittleEndian.Uint64(value[33:41]),
		firstNonAnnouncedWindow: binary.LittleEndian.Uint32(value[41:45]),
	}
	switch value[45] {
	case 0:
		if !isZero(value[46:82]) {
			return sessionSummary{}, errors.New("validator pebblestore: absent finalized summary has an ID")
		}
	case 1:
		id := simplex.CandidateID{Slot: binary.BigEndian.Uint32(value[46:50])}
		copy(id.Hash[:], value[50:82])
		summary.lastFinalized = &id
	default:
		return sessionSummary{}, fmt.Errorf("validator pebblestore: finalized summary marker %d", value[45])
	}
	switch value[82] {
	case 0:
		if !isZero(value[83:137]) {
			return sessionSummary{}, errors.New("validator pebblestore: absent leader summary has a record")
		}
	case 1:
		window, err := decodeLeaderWindow(value[83:137])
		if err != nil {
			return sessionSummary{}, fmt.Errorf("validator pebblestore: decode last leader window: %w", err)
		}
		summary.lastLeaderWindow = &window
	default:
		return sessionSummary{}, fmt.Errorf("validator pebblestore: leader summary marker %d", value[82])
	}
	if (summary.finalized == 0) != (summary.lastFinalized == nil) {
		return sessionSummary{}, errors.New("validator pebblestore: finalized summary count and last ID disagree")
	}
	if (summary.leaderWindows == 0) != (summary.lastLeaderWindow == nil) {
		return sessionSummary{}, errors.New("validator pebblestore: leader summary count and last record disagree")
	}

	return summary, nil
}

func loadSessionSummary(batch *pebble.Batch, namespace storageNamespace) (sessionSummary, error) {
	value, err := getBatchCopy(batch, summaryKey(namespace))
	if err != nil {
		return sessionSummary{}, err
	}

	return decodeSessionSummary(value)
}

func saveSessionSummary(batch *pebble.Batch, namespace storageNamespace, summary sessionSummary) error {
	if err := batch.Set(summaryKey(namespace), encodeSessionSummary(summary), nil); err != nil {
		return fmt.Errorf("validator pebblestore: save session summary: %w", err)
	}

	return nil
}

func incrementSummaryCount(name string, count *uint64) error {
	if *count == math.MaxUint64 {
		return fmt.Errorf("validator pebblestore: %s summary count overflow", name)
	}
	(*count)++

	return nil
}

func candidateIDLess(a, b simplex.CandidateID) bool {
	if a.Slot != b.Slot {
		return a.Slot < b.Slot
	}

	return bytes.Compare(a.Hash[:], b.Hash[:]) < 0
}

func isZero(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}

	return true
}

func (summary sessionSummary) storedSession(id validator.SessionStorageID) validator.StoredSession {
	return validator.StoredSession{
		ID:                      id,
		Candidates:              summary.candidates,
		Finalized:               summary.finalized,
		Votes:                   summary.votes,
		Certificates:            summary.certificates,
		LeaderWindows:           summary.leaderWindows,
		FirstNonAnnouncedWindow: summary.firstNonAnnouncedWindow,
		LastFinalized:           summary.lastFinalized,
		LastLeaderWindow:        summary.lastLeaderWindow,
	}
}
