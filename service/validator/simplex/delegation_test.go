package simplex

import (
	"crypto/ed25519"
	"strings"
	"testing"
)

// delegate re-signs a candidate with the collator key and attaches the
// leader-signed window delegation, the way a delegated collator does.
func (env *testEnv) delegate(c *Candidate, collatorPub ed25519.PublicKey, collatorPriv ed25519.PrivateKey) {
	env.t.Helper()
	windowStart := c.ID.Slot - c.ID.Slot%env.spw
	collatorID := KeyNodeIDShort(collatorPub)
	delegationSignature, err := SignDelegation(
		newTestSigner(env.keys[c.Leader]),
		env.session,
		windowStart,
		collatorID,
	)
	if err != nil {
		env.t.Fatal(err)
	}
	c.Delegation = &Delegation{
		CollatorKey: collatorPub,
		Signature:   delegationSignature,
	}
	sig, err := SignCandidate(newTestSigner(collatorPriv), env.session, c.ID)
	if err != nil {
		env.t.Fatal(err)
	}
	c.Signature = sig
}

func TestDelegationSignature(t *testing.T) {
	leader := testKey(4)
	sessionID := [32]byte{0x51}
	collatorID := [32]byte{0xa7}

	signature, err := SignDelegation(newTestSigner(leader), sessionID, 12, collatorID)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyDelegationSignature(leader.Public().(ed25519.PublicKey), sessionID, 12, collatorID, signature) {
		t.Fatal("valid collation-window signature was rejected")
	}
	if VerifyDelegationSignature(leader.Public().(ed25519.PublicKey), sessionID, 16, collatorID, signature) {
		t.Fatal("signature for another window was accepted")
	}

	otherCollator := collatorID
	otherCollator[0] ^= 0xff
	if VerifyDelegationSignature(leader.Public().(ed25519.PublicKey), sessionID, 12, otherCollator, signature) {
		t.Fatal("signature for another collator was accepted")
	}
}

func TestDelegatedCandidateAccepted(t *testing.T) {
	collatorPub, collatorPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	env := newTestEnv(t, withLocal(1))
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x51)
	env.delegate(cand, collatorPub, collatorPriv)

	if err = env.eng.SubmitCandidate(cand); err != nil {
		t.Fatalf("delegated candidate rejected: %v", err)
	}
	if len(env.hooks.validations) != 1 || env.hooks.validations[0].ID != cand.ID {
		t.Fatal("delegated candidate must reach validation")
	}
	if env.hooks.validations[0].Delegation == nil {
		t.Fatal("delegation must survive ingestion")
	}
}

func TestDelegatedCandidateRejections(t *testing.T) {
	collatorPub, collatorPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	otherPub, otherPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	type rejectionCase struct {
		name   string
		mutate func(env *testEnv, c *Candidate)
		want   string
	}
	tests := []rejectionCase{
		{
			name: "delegation signed for another collator",
			mutate: func(env *testEnv, c *Candidate) {
				env.delegate(c, collatorPub, collatorPriv)
				// A different collator key cannot validate this delegation.
				c.Delegation.CollatorKey = otherPub
			},
			want: "delegation signature",
		},
		{
			name: "candidate signed by the leader instead of the collator",
			mutate: func(env *testEnv, c *Candidate) {
				leaderSig := c.Signature
				env.delegate(c, collatorPub, collatorPriv)
				c.Signature = leaderSig
			},
			want: "delegated candidate signature",
		},
		{
			name: "candidate signed by an unauthorized key",
			mutate: func(env *testEnv, c *Candidate) {
				env.delegate(c, collatorPub, collatorPriv)
				sig, err := SignCandidate(newTestSigner(otherPriv), env.session, c.ID)
				if err != nil {
					env.t.Fatal(err)
				}
				c.Signature = sig
			},
			want: "delegated candidate signature",
		},
		{
			name: "invalid collator key",
			mutate: func(env *testEnv, c *Candidate) {
				env.delegate(c, collatorPub, collatorPriv)
				c.Delegation.CollatorKey = collatorPub[:16]
			},
			want: "collator key is invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newTestEnv(t, withLocal(1))
			env.start()

			cand := env.makeCandidate(0, Genesis(), 0x52)
			test.mutate(env, cand)
			err := env.eng.SubmitCandidate(cand)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if len(env.hooks.validations) != 0 {
				t.Fatal("rejected candidate must not reach validation")
			}
		})
	}
}
