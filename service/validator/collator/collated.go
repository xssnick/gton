package collator

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
)

const accountStorageDictProofTag = uint64(0x37c1e3fc)
const blockTag = uint64(0x11ef55aa)

type verifiedCollatedData struct {
	roots          []*cell.Cell
	virtualRoots   map[cell.Hash]*cell.Cell
	accountStorage map[cell.Hash]*cell.Cell
	genUtimeMS     uint64
	full           bool
}

type collatedProof struct {
	hash cell.Hash
	root *cell.Cell
}

func (d *verifiedCollatedData) Full() bool {
	return d.full
}

func (d *verifiedCollatedData) StateRoot(id ton.BlockIDExt) (*cell.Cell, error) {
	return collatedStateRoot(d, id)
}

func (d *verifiedCollatedData) AccountStorageRoot(hash cell.Hash) (*cell.Cell, error) {
	root := d.accountStorage[hash]
	if root == nil {
		return nil, fmt.Errorf("%w: account storage root %x", ErrCollatedRootNotFound, hash)
	}

	return root, nil
}

func canonicalCollatedProofs(roots []*cell.Cell) ([]*cell.Cell, error) {
	proofs := make([]collatedProof, 0, len(roots))
	seen := make(map[cell.Hash]struct{}, len(roots))
	for i, root := range roots {
		virtualRoot, err := unwrapCollatedProof(root)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid supplied collated proof %d: %v", ErrInvalidInput, i, err)
		}
		hash := virtualRoot.HashKey()
		if _, duplicate := seen[hash]; duplicate {
			return nil, fmt.Errorf("%w: duplicate supplied collated proof root %x", ErrInvalidInput, hash)
		}
		seen[hash] = struct{}{}
		proofs = append(proofs, collatedProof{hash: hash, root: root})
	}
	sort.Slice(proofs, func(i, j int) bool {
		return bytes.Compare(proofs[i].hash[:], proofs[j].hash[:]) < 0
	})
	canonical := make([]*cell.Cell, len(proofs))
	for i := range proofs {
		canonical[i] = proofs[i].root
	}

	return canonical, nil
}

func verifyCollatedRoots(roots []*cell.Cell, genUtime uint32) (verifiedCollatedData, error) {
	verified := verifiedCollatedData{
		roots:          roots,
		virtualRoots:   make(map[cell.Hash]*cell.Cell),
		accountStorage: make(map[cell.Hash]*cell.Cell),
	}
	consensusFound := false
	topBlockDescrFound := false

	for i, root := range roots {
		if root == nil {
			return verifiedCollatedData{}, fmt.Errorf("%w: collated root %d is nil", ErrInvalidInput, i)
		}
		if root.IsSpecial() {
			virtualRoot, err := unwrapCollatedProof(root)
			if err != nil {
				return verifiedCollatedData{}, fmt.Errorf("%w: invalid collated proof root %d: %v", ErrInvalidInput, i, err)
			}
			virtualHash := virtualRoot.HashKey()
			if _, duplicate := verified.virtualRoots[virtualHash]; duplicate {
				return verifiedCollatedData{}, fmt.Errorf(
					"%w: duplicate collated proof virtual root %x",
					ErrInvalidInput,
					virtualHash,
				)
			}
			verified.virtualRoots[virtualHash] = virtualRoot
			verified.full = true
			continue
		}

		loader, err := root.BeginParse()
		if err != nil || loader.BitsLeft() < 32 {
			continue
		}
		tag, err := loader.LoadUInt(32)
		if err != nil {
			continue
		}
		switch tag {
		case masterTopBlockDescrSetTag:
			if topBlockDescrFound {
				return verifiedCollatedData{}, fmt.Errorf("%w: duplicate top block descriptor set", ErrInvalidInput)
			}
			if err = verifyTopBlockDescrSet(root); err != nil {
				return verifiedCollatedData{}, fmt.Errorf("%w: invalid top block descriptor set: %v", ErrInvalidInput, err)
			}
			topBlockDescrFound = true

		case accountStorageDictProofTag:
			if loader.BitsLeft() != 0 || loader.RefsNum() != 1 {
				return verifiedCollatedData{}, fmt.Errorf("%w: malformed account storage proof root %d", ErrInvalidInput, i)
			}
			proof, proofErr := loader.LoadRefCell()
			if proofErr != nil {
				return verifiedCollatedData{}, fmt.Errorf("%w: load account storage proof root %d: %v", ErrInvalidInput, i, proofErr)
			}
			virtualRoot, proofErr := unwrapCollatedProof(proof)
			if proofErr != nil {
				return verifiedCollatedData{}, fmt.Errorf("%w: invalid account storage proof root %d: %v", ErrInvalidInput, i, proofErr)
			}
			virtualHash := virtualRoot.HashKey()
			if _, duplicate := verified.accountStorage[virtualHash]; duplicate {
				return verifiedCollatedData{}, fmt.Errorf(
					"%w: duplicate account storage proof virtual root %x",
					ErrInvalidInput,
					virtualHash,
				)
			}
			verified.accountStorage[virtualHash] = virtualRoot
			verified.full = true

		case consensusExtraDataTag:
			if consensusFound {
				return verifiedCollatedData{}, fmt.Errorf("%w: duplicate consensus collated data", ErrInvalidInput)
			}
			_, flagsErr := loader.LoadUInt(32)
			genUtimeMS, timeErr := loader.LoadUInt(64)
			if flagsErr != nil || timeErr != nil || genUtimeMS/1000 != uint64(genUtime) ||
				loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
				return verifiedCollatedData{}, fmt.Errorf(
					"%w: consensus collated data time or shape differs from block",
					ErrInvalidInput,
				)
			}
			verified.genUtimeMS = genUtimeMS
			consensusFound = true
		}
	}
	if !consensusFound {
		return verifiedCollatedData{}, fmt.Errorf("%w: candidate collated data has no consensus root", ErrInvalidInput)
	}

	return verified, nil
}

func unwrapCollatedProof(proof *cell.Cell) (*cell.Cell, error) {
	if proof == nil {
		return nil, fmt.Errorf("proof is nil")
	}
	if !proof.IsSpecial() || proof.GetType() != cell.MerkleProofCellType {
		return nil, fmt.Errorf("expected Merkle proof")
	}
	body, err := proof.PeekRef(0)
	if err != nil {
		return nil, err
	}
	return cell.UnwrapProofVirtualized(proof, body.Hash(0))
}

func verifyCollatedPredecessors(
	data *verifiedCollatedData,
	first PreviousBlock,
	second *PreviousBlock,
) error {
	if err := verifyCollatedBlockState(data, &first, "first predecessor"); err != nil {
		return err
	}
	if second != nil {
		if err := verifyCollatedBlockState(data, second, "second predecessor"); err != nil {
			return err
		}
	}

	return nil
}

func verifyCollatedBlockState(data *verifiedCollatedData, block *PreviousBlock, label string) error {
	rootHash, err := blockIDRootHash(block.ID)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrInvalidInput, label, err)
	}
	root := data.virtualRoots[rootHash]
	if root == nil {
		return fmt.Errorf("%w: %s block proof is absent from full collated data", ErrInvalidInput, label)
	}

	if block.ID.SeqNo == 0 {
		if root.HashKeyAt(0) != block.State.HashKeyAt(0) {
			return fmt.Errorf("%w: %s zerostate proof differs from supplied state", ErrInvalidInput, label)
		}
		return nil
	}

	stateUpdate, err := collatedBlockStateUpdate(root)
	if err != nil {
		return fmt.Errorf("%w: decode %s collated block proof: %v", ErrInvalidInput, label, err)
	}
	stateProof, err := stateUpdate.PeekRef(1)
	if err != nil {
		return fmt.Errorf("%w: load %s collated state hash: %v", ErrInvalidInput, label, err)
	}
	stateRoot := data.virtualRoots[stateProof.HashKeyAt(0)]
	if stateRoot == nil {
		return fmt.Errorf("%w: %s state proof is absent from full collated data", ErrInvalidInput, label)
	}
	if stateRoot.HashKeyAt(0) != block.State.HashKeyAt(0) {
		return fmt.Errorf("%w: %s collated state differs from supplied state", ErrInvalidInput, label)
	}

	return nil
}

func verifyCollatedNeighborStates(data *verifiedCollatedData, neighbors []Neighbor) error {
	for i := range neighbors {
		neighbor := &neighbors[i]
		if neighbor.Block.Workchain == address.MasterchainID {
			continue
		}
		stateRoot, err := collatedStateRoot(data, neighbor.Block)
		if err != nil {
			return fmt.Errorf("%w: neighbor %d: %v", ErrInvalidInput, i, err)
		}
		var state tlb.ShardStateUnsplit
		if err = parseProofExact(&state, stateRoot); err != nil {
			return fmt.Errorf("%w: decode neighbor %d state proof: %v", ErrInvalidInput, i, err)
		}
		if state.ShardIdent.WorkchainID != neighbor.Block.Workchain ||
			int64(state.ShardIdent.GetShardID()) != neighbor.Block.Shard ||
			state.Seqno != neighbor.Block.SeqNo {
			return fmt.Errorf("%w: neighbor %d state proof differs from block id", ErrInvalidInput, i)
		}
		if state.GenLT != neighbor.EndLT {
			return fmt.Errorf("%w: neighbor %d end lt differs from its proven state", ErrInvalidInput, i)
		}
		var queue tlb.OutMsgQueueInfo
		if err = parseProofExact(&queue, state.OutMsgQueueInfo); err != nil {
			return fmt.Errorf("%w: decode neighbor %d queue proof: %v", ErrInvalidInput, i, err)
		}
		records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, neighbor.Shard.Shard)
		if err != nil {
			return fmt.Errorf("%w: decode neighbor %d processed frontier: %v", ErrInvalidInput, i, err)
		}
		if !equalProcessedRecords(records, neighbor.Processed) {
			return fmt.Errorf("%w: neighbor %d processed frontier differs from its proven state", ErrInvalidInput, i)
		}
	}

	return nil
}

type neighborShardKey struct {
	workchain int32
	shard     int64
}

type expectedNeighbor struct {
	block ton.BlockIDExt
	endLT uint64
}

func verifyFullCollatedNeighborSet(
	masterchain MasterchainContext,
	target tlb.ShardIdent,
	neighbors []Neighbor,
) error {
	expected, err := expectedShardNeighbors(masterchain, groups.ShardID{
		Workchain: target.WorkchainID,
		Shard:     int64(target.GetShardID()),
	})
	if err != nil {
		return err
	}

	for i := range neighbors {
		neighbor := &neighbors[i]
		key := neighborShardKey{workchain: neighbor.Block.Workchain, shard: neighbor.Block.Shard}
		block, exists := expected[key]
		if !exists || !block.Equals(&neighbor.Block) {
			return fmt.Errorf("%w: neighbor %d is not the registered shard block", ErrInvalidInput, i)
		}
		if neighbor.Block.Workchain == address.MasterchainID {
			if err := verifyMasterchainNeighbor(masterchain, neighbor); err != nil {
				return err
			}
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		return fmt.Errorf("%w: full collated data is missing %d registered neighbors", ErrInvalidInput, len(expected))
	}

	return nil
}

func verifyMasterchainNeighbor(masterchain MasterchainContext, neighbor *Neighbor) error {
	if neighbor.EndLT != masterchain.EndLT {
		return fmt.Errorf("%w: masterchain neighbor end lt differs from the verified context", ErrInvalidInput)
	}
	if masterchain.OutMsgQueueInfo == nil {
		return fmt.Errorf("%w: masterchain context has no authenticated output queue", ErrInvalidInput)
	}

	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, masterchain.OutMsgQueueInfo); err != nil {
		return fmt.Errorf("%w: decode masterchain output queue: %v", ErrInvalidInput, err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, neighbor.Shard.Shard)
	if err != nil {
		return fmt.Errorf("%w: decode masterchain processed frontier: %v", ErrInvalidInput, err)
	}
	if !equalProcessedRecords(records, neighbor.Processed) {
		return fmt.Errorf("%w: masterchain processed frontier differs from the verified context", ErrInvalidInput)
	}

	return nil
}

func verifyFullMasterNeighborSet(
	previous PreviousBlock,
	previousState *tlb.ShardStateUnsplit,
	registry *ShardRegistry,
	neighbors []Neighbor,
) error {
	expected := make(map[neighborShardKey]expectedNeighbor)
	masterKey := neighborShardKey{workchain: address.MasterchainID, shard: sharddomain.Root}
	expected[masterKey] = expectedNeighbor{block: previous.ID, endLT: previousState.GenLT}
	for _, top := range registry.Tops() {
		if top.Block.SeqNo == 0 {
			continue
		}
		fields, err := parseShardDescriptorFields(top.Descriptor)
		if err != nil {
			return fmt.Errorf("%w: decode registered neighbor descriptor: %v", ErrInvalidInput, err)
		}
		key := neighborShardKey{workchain: top.Block.Workchain, shard: top.Block.Shard}
		expected[key] = expectedNeighbor{block: top.Block, endLT: fields.endLT}
	}

	masterchain := MasterchainContext{
		ID:              previous.ID,
		EndLT:           previousState.GenLT,
		OutMsgQueueInfo: previousState.OutMsgQueueInfo,
	}
	for i := range neighbors {
		neighbor := &neighbors[i]
		key := neighborShardKey{workchain: neighbor.Block.Workchain, shard: neighbor.Block.Shard}
		want, exists := expected[key]
		if !exists || !want.block.Equals(&neighbor.Block) {
			return fmt.Errorf("%w: neighbor %d is not a registered post-transition block", ErrInvalidInput, i)
		}
		if neighbor.EndLT != want.endLT {
			return fmt.Errorf("%w: neighbor %d end lt differs from its registered descriptor", ErrInvalidInput, i)
		}
		if key == masterKey {
			if err := verifyMasterchainNeighbor(masterchain, neighbor); err != nil {
				return err
			}
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		return fmt.Errorf("%w: full collated data is missing %d post-transition neighbors", ErrInvalidInput, len(expected))
	}

	return nil
}

func equalProcessedRecords(left, right []tlb.ProcessedUptoRecord) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

func collatedStateRoot(data *verifiedCollatedData, id ton.BlockIDExt) (*cell.Cell, error) {
	rootHash, err := blockIDRootHash(id)
	if err != nil {
		return nil, err
	}
	root := data.virtualRoots[rootHash]
	if root == nil {
		return nil, fmt.Errorf("%w: block proof is absent from full collated data", ErrCollatedRootNotFound)
	}
	if id.SeqNo == 0 {
		return root, nil
	}

	stateUpdate, err := collatedBlockStateUpdate(root)
	if err != nil {
		return nil, fmt.Errorf("decode collated block proof: %w", err)
	}
	stateProof, err := stateUpdate.PeekRef(1)
	if err != nil {
		return nil, fmt.Errorf("load collated state hash: %w", err)
	}
	stateRoot := data.virtualRoots[stateProof.HashKeyAt(0)]
	if stateRoot == nil {
		return nil, fmt.Errorf("%w: state proof is absent from full collated data", ErrCollatedRootNotFound)
	}

	return stateRoot, nil
}

func collatedBlockStateUpdate(root *cell.Cell) (*cell.Cell, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	tag, err := loader.LoadUInt(32)
	if err != nil || tag != blockTag {
		return nil, fmt.Errorf("invalid block tag")
	}
	if _, err = loader.LoadInt(32); err != nil {
		return nil, fmt.Errorf("load block global id: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 4 {
		return nil, fmt.Errorf("invalid block proof shape")
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, fmt.Errorf("load block info reference: %w", err)
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, fmt.Errorf("load block value flow reference: %w", err)
	}
	stateUpdate, err := loader.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load block state update reference: %w", err)
	}
	// Full collated block proofs prune unrelated subtrees, so only the shape is
	// checked here; a deep Merkle-update validation would incorrectly reject
	// those virtualized pruned branches.
	if stateUpdate.GetType() != cell.MerkleUpdateCellType {
		return nil, fmt.Errorf("invalid block state update")
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, fmt.Errorf("load block extra reference: %w", err)
	}

	return stateUpdate, nil
}

func blockIDRootHash(id ton.BlockIDExt) (cell.Hash, error) {
	var hash cell.Hash
	if len(id.RootHash) != len(hash) {
		return hash, fmt.Errorf("block root hash must be 256 bits")
	}
	copy(hash[:], id.RootHash)
	return hash, nil
}
