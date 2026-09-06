package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func (a *BlockAccepter) acceptForTest(
	ctx context.Context,
	acceptance BlockAcceptance,
	resolveView BlockAcceptanceViewResolver,
) error {
	prepared, err := a.Prepare(ctx, acceptance, resolveView)
	if err != nil {
		return err
	}
	if err = prepared.Submit(ctx); err != nil {
		return err
	}

	return prepared.Describe(ctx)
}

type acceptanceTestNode struct {
	blocks    []p2p.DownloadedBlock
	proofs    map[string][]byte
	history   map[storage.BlockSeqRef]ton.BlockIDExt
	published []storage.LiveBlockArtifacts
}

func (n *acceptanceTestNode) PublishAcceptedBlockState(artifacts storage.LiveBlockArtifacts) error {
	n.published = append(n.published, artifacts)

	return nil
}

func (n *acceptanceTestNode) BlockArtifactsSignal() <-chan struct{} {
	return make(chan struct{})
}

func (n *acceptanceTestNode) BlockData(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (n *acceptanceTestNode) BlockProof(
	_ context.Context,
	_ storage.ServedProofKind,
	block ton.BlockIDExt,
) ([]byte, error) {
	proof, exists := n.proofs[storage.FormatBlockRef(block)]
	if !exists {
		return nil, storage.ErrNotFound
	}

	return proof, nil
}

func (n *acceptanceTestNode) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	return nil, storage.ErrNotFound
}

func (n *acceptanceTestNode) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (n *acceptanceTestNode) LookupBlockBySeqNo(
	_ context.Context,
	ref storage.BlockSeqRef,
) (ton.BlockIDExt, error) {
	block, exists := n.history[ref]
	if !exists {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}

	return block, nil
}

func (n *acceptanceTestNode) SubmitBlockLocally(block p2p.DownloadedBlock) {
	n.blocks = append(n.blocks, block)
}

type acceptanceTestFixture struct {
	config     SessionConfig
	privateKey ed25519.PrivateKey
	candidate  *CandidateArtifact
	validators *blockproof.PreparedValidatorSet
}

func newAcceptanceTestAccepter(
	fixture acceptanceTestFixture,
	node *acceptanceTestNode,
) (*BlockAccepter, error) {
	return NewBlockAccepter(BlockAccepterOptions{
		Config: fixture.config,
		Node:   node,
	})
}

// acceptanceTestViewResolver serves one already known view, mirroring a chain
// view that is ready by the time acceptance asks for it.
func acceptanceTestViewResolver(view BlockAcceptanceView) BlockAcceptanceViewResolver {
	return func() (BlockAcceptanceView, error) {
		return view, nil
	}
}

func (f acceptanceTestFixture) view(t *testing.T) BlockAcceptanceView {
	t.Helper()

	_, parsed, err := parseAcceptedBlock(f.candidate.Candidate.Block, f.candidate.BlockBOC, nil)
	if err != nil {
		t.Fatalf("parse acceptance view block: %v", err)
	}
	meta, err := storage.BuildBlockMetaFromParsedBlock(f.candidate.Candidate.Block, parsed)
	if err != nil {
		t.Fatalf("build acceptance view block meta: %v", err)
	}
	if len(meta.PrevRefs) != 1 || parsed.BlockInfo.MasterRef == nil {
		t.Fatalf("acceptance view fixture has %d predecessors or no master ref", len(meta.PrevRefs))
	}

	return BlockAcceptanceView{
		MasterchainBlock: masterchainBlockFromReference(parsed.BlockInfo.MasterRef),
		Registered: []groups.ShardDescription{{
			Shard: f.config.Shard,
			Block: meta.PrevRefs[0],
		}},
	}
}

func (f acceptanceTestFixture) masterchainBlock(t *testing.T) ton.BlockIDExt {
	t.Helper()

	_, parsed, err := parseAcceptedBlock(f.candidate.Candidate.Block, f.candidate.BlockBOC, nil)
	if err != nil {
		t.Fatalf("parse acceptance fixture block: %v", err)
	}
	if parsed.BlockInfo.MasterRef == nil {
		t.Fatal("acceptance fixture block has no masterchain reference")
	}

	return masterchainBlockFromReference(parsed.BlockInfo.MasterRef)
}

func TestBlockAccepterAcceptsShardCertificates(t *testing.T) {
	tests := []struct {
		name         string
		kind         simplex.VoteKind
		throughEmpty bool
	}{
		{
			name: "notarize ordinary candidate",
			kind: simplex.VoteNotarize,
		},
		{
			name:         "finalize through empty candidate",
			kind:         simplex.VoteFinalize,
			throughEmpty: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceTestFixture(t, groups.ShardID{
				Workchain: 0,
				Shard:     math.MinInt64,
			})
			acceptance := fixture.acceptance(test.kind, test.throughEmpty)
			node := &acceptanceTestNode{}
			accepter, err := newAcceptanceTestAccepter(fixture, node)
			if err != nil {
				t.Fatalf("construct block accepter: %v", err)
			}

			if err = accepter.acceptForTest(context.Background(), acceptance, acceptanceTestViewResolver(fixture.view(t))); err != nil {
				t.Fatalf("accept block: %v", err)
			}
			block := requireAcceptanceTestBlock(t, node, fixture)
			if !block.IsLink {
				t.Fatal("shard block was not submitted with a proof link")
			}
			if err = blockproof.CheckProofShape(block.ID, block.Proof, true); err != nil {
				t.Fatalf("check shard proof link: %v", err)
			}
			parsed, err := blockproof.ParseCell(block.ID, block.Proof)
			if err != nil {
				t.Fatalf("parse shard proof link: %v", err)
			}
			if parsed.Proof.Signatures != nil {
				t.Fatal("shard proof link contains validator signatures")
			}

			signatures := fixture.signatureSet(t, acceptance)
			if err = blockproof.CheckPreparedSignatures(block.ID, signatures, fixture.validators); err != nil {
				t.Fatalf("verify expected shard signatures: %v", err)
			}
			if !bytes.Equal(block.SignaturesVerifiedKey, signatures.ContentKey(block.ID)) {
				t.Fatal("shard signature verification key does not commit to logical simplex signatures")
			}
		})
	}
}

func TestBlockAccepterAcceptsMasterchainFinalCertificates(t *testing.T) {
	tests := []struct {
		name         string
		throughEmpty bool
	}{
		{name: "ordinary candidate"},
		{name: "through empty candidate", throughEmpty: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceTestFixture(t, groups.ShardID{
				Workchain: -1,
				Shard:     math.MinInt64,
			})
			acceptance := fixture.acceptance(simplex.VoteFinalize, test.throughEmpty)
			node := &acceptanceTestNode{}
			accepter, err := newAcceptanceTestAccepter(fixture, node)
			if err != nil {
				t.Fatalf("construct block accepter: %v", err)
			}

			if err = accepter.acceptForTest(
				context.Background(),
				acceptance,
				acceptanceTestViewResolver(BlockAcceptanceView{}),
			); err != nil {
				t.Fatalf("accept block: %v", err)
			}
			block := requireAcceptanceTestBlock(t, node, fixture)
			if block.IsLink {
				t.Fatal("masterchain block was submitted with a proof link")
			}
			if err = blockproof.CheckProofShape(block.ID, block.Proof, false); err != nil {
				t.Fatalf("check masterchain proof: %v", err)
			}
			parsed, err := blockproof.ParseCell(block.ID, block.Proof)
			if err != nil {
				t.Fatalf("parse masterchain proof: %v", err)
			}
			if parsed.Proof.Signatures == nil {
				t.Fatal("masterchain proof does not contain final signatures")
			}
			blockSignatures, err := blockproof.ParseBlockSignatureSetCell(parsed.Proof.Signatures)
			if err != nil {
				t.Fatalf("parse masterchain signatures: %v", err)
			}
			if !blockSignatures.ValidatorSignatures.IsSimplex() || !blockSignatures.ValidatorSignatures.Final() {
				t.Fatal("masterchain proof does not contain final simplex signatures")
			}
			if err = blockproof.CheckPreparedBlockSignatures(block.ID, blockSignatures, fixture.validators); err != nil {
				t.Fatalf("verify masterchain block signatures: %v", err)
			}
			if !bytes.Equal(block.SignaturesVerifiedKey, blockSignatures.ContentKey(block.ID)) {
				t.Fatal("masterchain signature verification key does not commit to serialized signature weight")
			}
		})
	}
}

func TestBlockAccepterRejectsMasterchainNotarization(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{
		Workchain: -1,
		Shard:     math.MinInt64,
	})
	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatalf("construct block accepter: %v", err)
	}

	err = accepter.acceptForTest(
		context.Background(),
		fixture.acceptance(simplex.VoteNotarize, false),
		acceptanceTestViewResolver(BlockAcceptanceView{}),
	)
	if err == nil || !strings.Contains(err.Error(), "masterchain block requires final certificate") {
		t.Fatalf("accept masterchain notarization error = %v", err)
	}
	if len(node.blocks) != 0 {
		t.Fatalf("submitted %d blocks after rejected notarization", len(node.blocks))
	}
}

func TestBlockAccepterRejectsInvalidBlockHeadersBeforeSignatures(t *testing.T) {
	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	masterchain := groups.ShardID{Workchain: -1, Shard: math.MinInt64}
	tests := []struct {
		name   string
		shard  groups.ShardID
		mutate func(*tlb.BlockHeader)
		want   string
	}{
		{
			name:  "nonzero version",
			shard: shard,
			mutate: func(header *tlb.BlockHeader) {
				header.Version = 1
			},
			want: "unsupported block header version",
		},
		{
			name:  "vertical sequence increment",
			shard: shard,
			mutate: func(header *tlb.BlockHeader) {
				header.VertSeqnoIncr = true
				header.PrevVertRef = &tlb.BlkPrevInfo{
					Prev1: acceptanceTestExtBlockRef(header.SeqNo-1, 0x71),
				}
			},
			want: "vert_seqno_incr",
		},
		{
			name:  "shard marked as masterchain",
			shard: shard,
			mutate: func(header *tlb.BlockHeader) {
				header.NotMaster = false
			},
			want: "invalid not_master flag",
		},
		{
			name:  "masterchain marked as shard",
			shard: masterchain,
			mutate: func(header *tlb.BlockHeader) {
				header.NotMaster = true
				masterRef := acceptanceTestExtBlockRef(header.SeqNo-1, 0x72)
				header.MasterRef = &masterRef
			},
			want: "invalid not_master flag",
		},
		{
			name:  "masterchain after merge",
			shard: masterchain,
			mutate: func(header *tlb.BlockHeader) {
				header.AfterMerge = true
				second := acceptanceTestExtBlockRef(header.SeqNo-1, 0x73)
				header.PrevRef.Prev2 = &second
			},
			want: "masterchain block announces a split or merge",
		},
		{
			name:  "masterchain after split",
			shard: masterchain,
			mutate: func(header *tlb.BlockHeader) {
				header.AfterSplit = true
			},
			want: "masterchain block announces a split or merge",
		},
		{
			name:  "masterchain before split",
			shard: masterchain,
			mutate: func(header *tlb.BlockHeader) {
				header.BeforeSplit = true
			},
			want: "masterchain block announces a split or merge",
		},
		{
			name:  "shard key block",
			shard: shard,
			mutate: func(header *tlb.BlockHeader) {
				header.KeyBlock = true
			},
			want: "non-masterchain block is marked as a key block",
		},
		{
			name:  "validator set hash binding",
			shard: shard,
			mutate: func(header *tlb.BlockHeader) {
				header.GenValidatorListHashShort++
			},
			want: "validator set hash mismatch",
		},
		{
			name:  "catchain sequence binding",
			shard: shard,
			mutate: func(header *tlb.BlockHeader) {
				header.GenCatchainSeqno++
			},
			want: "catchain seqno mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceTestFixtureWithHeader(t, test.shard, test.mutate)
			kind := simplex.VoteNotarize
			if test.shard.IsMasterchain() {
				kind = simplex.VoteFinalize
			}
			acceptance := fixture.acceptance(kind, false)
			node := &acceptanceTestNode{}
			accepter, err := newAcceptanceTestAccepter(fixture, node)
			if err != nil {
				t.Fatalf("construct block accepter: %v", err)
			}

			for _, resident := range []bool{false, true} {
				name := "decode wire"
				if resident {
					name = "resident root"
					acceptance.state = acceptanceResidentState(t, acceptance.Candidate)
				}
				t.Run(name, func(t *testing.T) {
					err := accepter.acceptForTest(t.Context(), acceptance, acceptanceTestViewResolver(BlockAcceptanceView{}))
					if err == nil || !strings.Contains(err.Error(), test.want) {
						t.Fatalf("accept invalid block error = %v, want %q", err, test.want)
					}
					if len(node.blocks) != 0 {
						t.Fatalf("submitted %d invalid blocks", len(node.blocks))
					}
				})
			}
		})
	}
}

func TestValidateAcceptedBlockHeaderRejectsMalformedShape(t *testing.T) {
	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	fixture := newAcceptanceTestFixture(t, shard)
	_, parsed, err := parseAcceptedBlock(fixture.candidate.Candidate.Block, fixture.candidate.BlockBOC, nil)
	if err != nil {
		t.Fatalf("parse fixture block: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*tlb.Block)
		want   string
	}{
		{
			name: "after merge without second predecessor",
			mutate: func(block *tlb.Block) {
				block.BlockInfo.AfterMerge = true
			},
			want: "after_merge announces 2 predecessors, header contains 1",
		},
		{
			name: "second predecessor without after merge",
			mutate: func(block *tlb.Block) {
				second := acceptanceTestExtBlockRef(block.BlockInfo.SeqNo-1, 0x74)
				block.BlockInfo.PrevRef.Prev2 = &second
			},
			want: "after_merge announces 1 predecessors, header contains 2",
		},
		{
			name: "noncanonical shard prefix length",
			mutate: func(block *tlb.Block) {
				block.BlockInfo.Shard.PrefixBits = 61
			},
			want: "prefix length 61 is outside 0..60",
		},
		{
			name: "noncanonical shard prefix bits",
			mutate: func(block *tlb.Block) {
				block.BlockInfo.Shard.PrefixBits = 1
				block.BlockInfo.Shard.ShardPrefix = 1
			},
			want: "shard prefix has non-zero bits outside its declared length",
		},
		{
			name: "invalid workchain",
			mutate: func(block *tlb.Block) {
				block.BlockInfo.Shard.WorkchainID = math.MinInt32
			},
			want: "workchain is invalid",
		},
		{
			name: "pruned predecessors",
			mutate: func(block *tlb.Block) {
				block.BlockInfo.PrevRef.Pruned = true
			},
			want: "predecessor references are pruned",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block := *parsed
			test.mutate(&block)

			err := validateAcceptedBlockHeader(fixture.candidate.Candidate.Block, &block)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate malformed header error = %v, want %q", err, test.want)
			}
		})
	}
}

func newAcceptanceTestFixture(t *testing.T, shard groups.ShardID) acceptanceTestFixture {
	return newAcceptanceTestFixtureWithHeader(t, shard, nil)
}

func newAcceptanceTestFixtureWithHeader(
	t *testing.T,
	shard groups.ShardID,
	mutate func(*tlb.BlockHeader),
) acceptanceTestFixture {
	t.Helper()

	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x21}, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var publicKeyArray [32]byte
	copy(publicKeyArray[:], publicKey)
	publicKeyHash, err := groups.PublicKeyHash(publicKeyArray)
	if err != nil {
		t.Fatalf("hash validator public key: %v", err)
	}
	validator := groups.Validator{
		PublicKey:     publicKeyArray,
		PublicKeyHash: publicKeyHash,
		ADNL:          [32]byte{0x44},
		Weight:        1,
	}
	const catchainSeqno = uint32(7)
	validatorSetHash, err := groups.ValidatorSetHash(groups.ValidatorSetHashInput{
		CatchainSeqno: catchainSeqno,
		Validators:    []groups.Validator{validator},
	})
	if err != nil {
		t.Fatalf("calculate validator set hash: %v", err)
	}
	config := SessionConfig{
		SessionID:        [32]byte{0x55},
		Shard:            shard,
		CatchainSeqno:    catchainSeqno,
		ValidatorSetHash: validatorSetHash,
		Validators:       []groups.Validator{validator},
	}

	root := acceptanceTestBlockRoot(t, shard, catchainSeqno, validatorSetHash, mutate)
	blockBOC := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockBOC)
	candidate := simplex.Candidate{
		Parent: simplex.Genesis(),
		Leader: 0,
		Block: ton.BlockIDExt{
			Workchain: shard.Workchain,
			Shard:     shard.Shard,
			SeqNo:     2,
			RootHash:  rootHash[:],
			FileHash:  fileHash[:],
		},
		CollatedFileHash: sha256.Sum256([]byte("collated data")),
	}
	candidate.ID = candidate.ComputeID(3)

	validatorAddr := &tlb.ValidatorAddr{
		PublicKey: tlb.SigPubKeyED25519{Key: publicKey},
		Weight:    validator.Weight,
		ADNLAddr:  validator.ADNL[:],
	}
	validators, err := blockproof.PrepareValidatorSet(catchainSeqno, []*tlb.ValidatorAddr{validatorAddr})
	if err != nil {
		t.Fatalf("prepare validators: %v", err)
	}

	return acceptanceTestFixture{
		config:     config,
		privateKey: privateKey,
		candidate: &CandidateArtifact{
			Candidate: candidate,
			BlockBOC:  blockBOC,
		},
		validators: validators,
	}
}

func (f acceptanceTestFixture) acceptance(kind simplex.VoteKind, throughEmpty bool) BlockAcceptance {
	certified := f.candidate
	if throughEmpty {
		empty := simplex.Candidate{
			Parent: simplex.Parent(f.candidate.Candidate.ID),
			Leader: 0,
			Empty:  true,
			Block:  f.candidate.Candidate.Block,
		}
		empty.ID = empty.ComputeID(f.candidate.Candidate.ID.Slot + 1)
		certified = &CandidateArtifact{Candidate: empty}
	}

	vote := simplex.NotarizeVote(certified.Candidate.ID)
	if kind == simplex.VoteFinalize {
		vote = simplex.FinalizeVote(certified.Candidate.ID)
	}
	certificate := &simplex.Certificate{
		Vote: vote,
		Signatures: []simplex.VoteSignature{{
			ValidatorIndex: 0,
			Signature: ed25519.Sign(
				f.privateKey,
				simplex.DataToSign(f.config.SessionID, simplex.VoteBytes(vote)),
			),
		}},
	}
	// The accepter only ever sees sealed certificates, so the fixture goes
	// through the real verification rather than fabricating a seal.
	verified, err := simplex.VerifyCertificate(
		f.config.SessionID,
		runtimeValidators(f.config.Validators),
		certificate,
	)
	if err != nil {
		panic(fmt.Sprintf("acceptance fixture: verify certificate: %v", err))
	}

	return BlockAcceptance{
		Candidate:          f.candidate,
		Certificate:        verified,
		CertifiedCandidate: certified,
	}
}

func (f acceptanceTestFixture) signatureSet(
	t *testing.T,
	acceptance BlockAcceptance,
) *blockproof.ValidatorSignatureSet {
	t.Helper()

	validatorID, err := groups.PublicKeyHash(f.config.Validators[0].PublicKey)
	if err != nil {
		t.Fatalf("hash validator public key: %v", err)
	}
	certificate := acceptance.Certificate.Certificate()

	return blockproof.NewSimplexValidatorSignatureSet(
		f.config.CatchainSeqno,
		f.config.ValidatorSetHash,
		[]ton.Signature{{
			NodeIDShort: validatorID[:],
			Signature:   certificate.Signatures[0].Signature,
		}},
		certificate.Vote.Kind == simplex.VoteFinalize,
		f.config.SessionID[:],
		int32(certificate.Vote.ID.Slot),
		acceptance.CertifiedCandidate.Candidate.HashDataBytes(),
	)
}

func requireAcceptanceTestBlock(
	t *testing.T,
	node *acceptanceTestNode,
	fixture acceptanceTestFixture,
) p2p.DownloadedBlock {
	t.Helper()

	if len(node.blocks) != 1 {
		t.Fatalf("submitted blocks = %d, want 1", len(node.blocks))
	}
	block := node.blocks[0]
	if !block.ID.Equals(&fixture.candidate.Candidate.Block) {
		t.Fatalf("submitted block = %s, want %s", block.BlockRef(), storage.FormatBlockRef(fixture.candidate.Candidate.Block))
	}
	if block.Kind != acceptedBlockKind {
		t.Fatalf("submitted block kind = %q, want %q", block.Kind, acceptedBlockKind)
	}
	if block.Block == nil || block.Proof == nil || block.Meta == nil || block.StateUpdate == nil {
		t.Fatal("submitted block is incomplete")
	}
	if !bytes.Equal(block.BlockBOC, fixture.candidate.BlockBOC) || len(block.ProofBOC) == 0 {
		t.Fatal("submitted block is missing block or proof boc")
	}
	if !block.VerifiedRootHash {
		t.Fatal("submitted block root hash is not marked verified")
	}
	if len(block.SignaturesVerifiedKey) == 0 {
		t.Fatal("submitted block has no signature verification key")
	}

	return block
}

func acceptanceTestBlockRoot(
	t *testing.T,
	shard groups.ShardID,
	catchainSeqno uint32,
	validatorSetHash uint32,
	mutations ...func(*tlb.BlockHeader),
) *cell.Cell {
	t.Helper()

	const seqno = uint32(2)
	prefixBits, err := shard.PrefixBits()
	if err != nil {
		t.Fatalf("derive shard prefix length: %v", err)
	}
	header := tlb.BlockHeader{}
	header.Version = 0
	header.NotMaster = !shard.IsMasterchain()
	header.Shard = tlb.ShardIdent{
		PrefixBits:  int8(prefixBits),
		WorkchainID: shard.Workchain,
		ShardPrefix: uint64(shard.Shard) & (uint64(shard.Shard) - 1),
	}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.GenValidatorListHashShort = validatorSetHash
	header.GenCatchainSeqno = catchainSeqno
	header.MinRefMcSeqno = seqno - 1
	header.PrevRef = tlb.BlkPrevInfo{Prev1: acceptanceTestExtBlockRef(seqno-1, 0x31)}
	if header.NotMaster {
		masterRef := acceptanceTestExtBlockRef(seqno-1, 0x41)
		header.MasterRef = &masterRef
	}
	for _, mutate := range mutations {
		if mutate != nil {
			mutate(&header)
		}
	}

	oldState := cell.BeginCell().MustStoreUInt(1, 32).EndCell()
	newState := cell.BeginCell().MustStoreUInt(2, 32).EndCell()
	info, err := header.ToCell()
	if err != nil {
		t.Fatalf("build block header: %v", err)
	}
	valueFlow, err := (tlb.ValueFlow{}).ToCell()
	if err != nil {
		t.Fatalf("build block value flow: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreInt(-239, 32).
		MustStoreRef(info).
		MustStoreRef(valueFlow).
		MustStoreRef(acceptanceTestMerkleUpdate(t, oldState, newState)).
		MustStoreRef(acceptanceTestBlockExtra(shard.IsMasterchain())).
		EndCell()
}

func acceptanceTestExtBlockRef(seqno uint32, marker byte) tlb.ExtBlkRef {
	return tlb.ExtBlkRef{
		EndLt:    1,
		SeqNo:    seqno,
		RootHash: bytes.Repeat([]byte{marker}, 32),
		FileHash: bytes.Repeat([]byte{marker + 1}, 32),
	}
}

func acceptanceTestMerkleUpdate(t *testing.T, oldRoot, newRoot *cell.Cell) *cell.Cell {
	t.Helper()

	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(oldRoot.Hash(0), 256).
		MustStoreSlice(newRoot.Hash(0), 256).
		MustStoreUInt(uint64(oldRoot.Depth(0)), 16).
		MustStoreUInt(uint64(newRoot.Depth(0)), 16).
		MustStoreRef(oldRoot).
		MustStoreRef(newRoot).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("build merkle update: %v", err)
	}

	return update
}

func acceptanceTestBlockExtra(masterchain bool) *cell.Cell {
	inMessages := cell.BeginCell().EndCell()
	outMessages := cell.BeginCell().EndCell()
	accountBlocks := cell.BeginCell().EndCell()
	builder := cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(inMessages).
		MustStoreRef(outMessages).
		MustStoreRef(accountBlocks).
		MustStoreSlice(bytes.Repeat([]byte{0x51}, 32), 256).
		MustStoreSlice(bytes.Repeat([]byte{0x61}, 32), 256)
	if !masterchain {
		return builder.MustStoreBoolBit(false).EndCell()
	}

	details := cell.BeginCell().
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		EndCell()
	masterchainExtra := cell.BeginCell().
		MustStoreUInt(0xcca5, 16).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 4).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 4).
		MustStoreBoolBit(false).
		MustStoreRef(details).
		EndCell()

	return builder.MustStoreBoolBit(true).MustStoreRef(masterchainExtra).EndCell()
}

// TestAcceptancePublishesTheShardStateAheadOfTheStore pins the write half of the
// read rule at the point where it happens.
//
// Local submission is asynchronous by design, so a block this node has finalized
// is invisible to every local reader until the shard client applies it — and for a
// shard block that needs a masterchain block carrying this shard top. The
// acceptance path already holds everything a reader needs, so it publishes:
//
//   - the state, BY REFERENCE. The pointer is the assertion, not the hash: the
//     live view returns a resident state as itself and chain_state.go compares tip
//     states by pointer, so a copy anywhere on this path costs a silent full
//     re-apply per candidate.
//   - the block BOC and root, because a chain tip carries both and the store's own
//     readiness check does not even look at the BOC.
//   - the proof link, which is what the NEXT block's shard top description reads
//     and the other half of what the acceptance retry loop used to wait for.
func TestAcceptancePublishesTheShardStateAheadOfTheStore(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: math.MinInt64})
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	computed := cell.BeginCell().MustStoreUInt(0xc0ffee, 32).EndCell()
	acceptance.state = &ChainState{
		shard: fixture.config.Shard,
		tips: []ChainTip{{
			ID:       *acceptance.Candidate.Candidate.Block.Copy(),
			BlockBOC: acceptance.Candidate.BlockBOC,
			State:    computed,
		}},
		root: computed,
	}

	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatalf("construct block accepter: %v", err)
	}
	if err = accepter.acceptForTest(context.Background(), acceptance, acceptanceTestViewResolver(fixture.view(t))); err != nil {
		t.Fatalf("accept block: %v", err)
	}

	if len(node.published) != 1 {
		t.Fatalf("published %d states, want the one just accepted", len(node.published))
	}
	published := node.published[0]
	if !published.Block.Equals(&acceptance.Candidate.Candidate.Block) {
		t.Fatal("the publication names another block")
	}
	if published.State == nil || published.State.Cell != computed {
		t.Error("the published state is not the very cell this node computed")
	}
	if !bytes.Equal(published.BlockData, acceptance.Candidate.BlockBOC) {
		t.Error("the publication carries no block data, so a chain tip read still cannot be served")
	}
	if published.Root == nil || !bytes.Equal(published.Root.Hash(), published.Block.RootHash) {
		t.Error("the publication carries no parsed block root")
	}
	var link bool
	for _, proof := range published.Proofs {
		if proof.Kind == storage.ServedProofBlockLink && len(proof.Data) > 0 {
			link = true
		}
	}
	if !link {
		t.Error("the publication carries no proof link, so the next shard top description still waits for the store")
	}
	// The publication is submitted before it is published, so a reader that finds
	// it in the live view is never ahead of the pipeline that will commit it.
	if len(node.blocks) != 1 {
		t.Fatalf("submitted %d blocks, want one", len(node.blocks))
	}
}

// Nothing is published without a state to publish. A replayed finalization after
// a restart has none, and a block on its own would sit in the live view with no
// bound of its own until its flush.
func TestAcceptanceWithoutAResolvedStatePublishesNothing(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: math.MinInt64})
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	acceptance.Replay = true

	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatalf("construct block accepter: %v", err)
	}
	if err = accepter.acceptForTest(context.Background(), acceptance, acceptanceTestViewResolver(fixture.view(t))); err != nil {
		t.Fatalf("accept block: %v", err)
	}
	if len(node.published) != 0 {
		t.Fatalf("published %d states without one to publish", len(node.published))
	}
}

// The masterchain is deliberately out of scope. Its applied state follows within a
// block or two, it never stalled in the measured incident, and its replay ordering
// is a deliberate parity with the reference — while an uncommitted masterchain
// state would additionally reach the masterchain state caches.
func TestAcceptanceDoesNotPublishMasterchainStates(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: -1, Shard: math.MinInt64})
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	computed := cell.BeginCell().MustStoreUInt(0xdecaf, 32).EndCell()
	acceptance.state = &ChainState{
		shard: fixture.config.Shard,
		tips: []ChainTip{{
			ID:       *acceptance.Candidate.Candidate.Block.Copy(),
			BlockBOC: acceptance.Candidate.BlockBOC,
			State:    computed,
		}},
		root: computed,
	}

	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatalf("construct block accepter: %v", err)
	}
	if err = accepter.acceptForTest(
		context.Background(),
		acceptance,
		acceptanceTestViewResolver(BlockAcceptanceView{}),
	); err != nil {
		t.Fatalf("accept block: %v", err)
	}
	if len(node.published) != 0 {
		t.Fatalf("published %d masterchain states, want none", len(node.published))
	}
}
