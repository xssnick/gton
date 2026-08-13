package collator

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestRemoteHandlersSeparateAuthenticatedQueryFromWirePayload(t *testing.T) {
	if fields := reflect.TypeOf(RemoteHandlers{}).NumField(); fields != 2 {
		t.Fatalf("RemoteHandlers has %d fields, want only the two Delegated-v3 query handlers", fields)
	}

	auth := AuthenticatedQuery{SessionID: [32]byte{1}, SourceADNL: [32]byte{2}}
	prepare := simplex.ConsensusPleaseCollatePrepare{WindowStartSlot: 12}
	commit := simplex.ConsensusPleaseCollate{WindowStartSlot: 12, Signature: []byte{3, 4}}

	var preparedAuth AuthenticatedQuery
	var preparedWire simplex.ConsensusPleaseCollatePrepare
	var committedAuth AuthenticatedQuery
	var committedWire simplex.ConsensusPleaseCollate
	handlers := RemoteHandlers{
		Probe: func(
			_ context.Context,
			query AuthenticatedQuery,
			wire simplex.ConsensusPleaseCollatePrepare,
		) error {
			preparedAuth = query
			preparedWire = wire
			return nil
		},
		Commit: func(
			_ context.Context,
			query AuthenticatedQuery,
			wire simplex.ConsensusPleaseCollate,
		) error {
			committedAuth = query
			committedWire = wire
			return nil
		},
	}

	if err := handlers.Probe(context.Background(), auth, prepare); err != nil {
		t.Fatal(err)
	}
	if err := handlers.Commit(context.Background(), auth, commit); err != nil {
		t.Fatal(err)
	}
	if preparedAuth != auth || preparedWire != prepare {
		t.Fatalf("prepare handler received auth=%+v wire=%+v", preparedAuth, preparedWire)
	}
	if committedAuth != auth || committedWire.WindowStartSlot != commit.WindowStartSlot ||
		!bytes.Equal(committedWire.Signature, commit.Signature) {
		t.Fatalf("commit handler received auth=%+v wire=%+v", committedAuth, committedWire)
	}
}

func TestOverlaySessionCarriesReferenceMembershipAndLimits(t *testing.T) {
	session := OverlaySession{
		Session: Session{ID: [32]byte{1}},
		CollatorsByValidator: []groups.CollatorRegistryEntry{{
			ValidatorKeyID:  [32]byte{2},
			CollatorADNLIDs: [][32]byte{{3}, {4}},
		}},
		AllOverlayNodes:           [][32]byte{{5}, {6}},
		MaxBlockSize:              1 << 20,
		MaxCollatedDataSize:       2 << 20,
		BroadcastMode:             CandidateBroadcastBlockSyncOverlay,
		ObserversInPrivateOverlay: true,
	}

	if session.Session.ID[0] != 1 || session.CollatorsByValidator[0].ValidatorKeyID[0] != 2 ||
		len(session.CollatorsByValidator[0].CollatorADNLIDs) != 2 || len(session.AllOverlayNodes) != 2 ||
		session.MaxBlockSize == 0 || session.MaxCollatedDataSize == 0 ||
		session.BroadcastMode != CandidateBroadcastBlockSyncOverlay || !session.ObserversInPrivateOverlay {
		t.Fatalf("overlay session lost a required network input: %+v", session)
	}
}
