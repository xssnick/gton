package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	defaultContractProfile = "legacy-jetton-v1"

	legacyMinterCodeHash = "13f5d7a316c6d76e1053e88ac59b5de65a072a388451371dc5c5becbba13f50e"
	legacyWalletCodeHash = "beb0683ebeb8927fe9fc8ec0a18bc7dd17899689825a121eab46c5a3a860d0ce"

	// These are the legacy reference jetton contracts expected by the Go
	// deploy/mint flow. Keeping the BOCs with the load generator prevents a
	// mutable cppnode checkout from silently changing the workload protocol.
	legacyMinterCodeBOC = "te6ccgECCwEAAe0AART/APSkE/S88sgLAQIBYgIDAgLMBAUCA3pgCQoD79mRDjgEit8GhpgYC42Eit8H0gGADpj+mf9qJofQB9IGpqGEAKqThdRxgamqiq44L5cCSA/SB9AGoYEGhAMGuQ/QAYEogaKCF4BNAqkGQoAn0BLGeLZmZk9qpwQQg97svvKThdcYEakuAB8YEYAmACcYEvgsIH+XhAYHCACTs/BQiAbgqEAmqCgHkKAJ9ASxniwDni2ZkkWRlgIl6AHoAZYBkkHyAODpkZYFlA+X/5Og7wAxkZYKsZ4soAn0BCeW1iWZmZLj9gEA/jYD+gD6QPgoVBIIcFQgE1QUA8hQBPoCWM8WAc8WzMkiyMsBEvQA9ADLAMn5AHB0yMsCygfL/8nQUAjHBfLgShKhA1AkyFAE+gJYzxbMzMntVAH6QDAg1wsBwwCOH4IQ1TJ223CAEMjLBVADzxYi+gISy2rLH8s/yYBC+wCRW+IAMDUVxwXy4En6QDBZyFAE+gJYzxbMzMntVAAuUUPHBfLgSdQwAchQBPoCWM8WzMzJ7VQAfa289qJofQB9IGpqGDYY/BQAuCoQCaoKAeQoAn0BLGeLAOeLZmSRZGWAiXoAegBlgGT8gDg6ZGWBZQPl/+ToQAAfrxb2omh9AH0gamoYP6qQQA=="
	legacyWalletCodeBOC = "te6ccgECEQEAAyMAART/APSkE/S88sgLAQIBYgIDAgLMBAUAG6D2BdqJofQB9IH0gahhAgHUBgcCASAICQDDCDHAJJfBOAB0NMDAXGwlRNfA/AM4PpA+kAx+gAxcdch+gAx+gAwc6m0AALTH4IQD4p+pVIgupUxNFnwCeCCEBeNRRlSILqWMUREA/AK4DWCEFlfB7y6k1nwC+BfBIQP8vCAAET6RDBwuvLhTYAIBIAoLAIPUAQa5D2omh9AH0gfSBqGAJpj8EIC8aijKkQXUEIPe7L7wndCVj5cWLpn5j9ABgJ0CgR5CgCfQEsZ4sA54tmZPaqQB8VA9M/+gD6QCHwAe1E0PoA+kD6QNQwUTahUirHBfLiwSjC//LiwlQ0QnBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJIPkAcHTIywLKB8v/ydAE+kD0BDH6ACDXScIA8uLEd4AYyMsFUAjPFnD6AhfLaxPMgMAgEgDQ4AnoIQF41FGcjLHxnLP1AH+gIizxZQBs8WJfoCUAPPFslQBcwjkXKRceJQCKgToIIJycOAoBS88uLFBMmAQPsAECPIUAT6AljPFgHPFszJ7VQC9ztRND6APpA+kDUMAjTP/oAUVGgBfpA+kBTW8cFVHNtcFQgE1QUA8hQBPoCWM8WAc8WzMkiyMsBEvQA9ADLAMn5AHB0yMsCygfL/8nQUA3HBRyx8uLDCvoAUaihggiYloBmtgihggiYloCgGKEnlxBJEDg3XwTjDSXXCwGAPEADXO1E0PoA+kD6QNQwB9M/+gD6QDBRUaFSSccF8uLBJ8L/8uLCBYIJMS0AoBa88uLDghB73ZfeyMsfFcs/UAP6AiLPFgHPFslxgBjIywUkzxZw+gLLaszJgED7AEATyFAE+gJYzxYBzxbMye1UgAHBSeaAYoYIQc2LQnMjLH1Iwyz9Y+gJQB88WUAfPFslxgBDIywUkzxZQBvoCFctqFMzJcfsAECQQIwB8wwAjwgCwjiGCENUydttwgBDIywVQCM8WUAT6AhbLahLLHxLLP8ly+wCTNWwh4gPIUAT6AljPFgHPFszJ7VQ="
)

type resolvedContractProfile struct {
	name           string
	minterCode     *cell.Cell
	walletCode     *cell.Cell
	minterCodeHash string
	walletCodeHash string
}

func resolveContractProfile(name, minterPath, walletPath string) (resolvedContractProfile, error) {
	if name != defaultContractProfile {
		return resolvedContractProfile{}, fmt.Errorf("unknown contract profile %q", name)
	}

	minterCode, err := loadProfileCell(minterPath, legacyMinterCodeBOC, legacyMinterCodeHash)
	if err != nil {
		return resolvedContractProfile{}, fmt.Errorf("load %s minter code: %w", name, err)
	}
	walletCode, err := loadProfileCell(walletPath, legacyWalletCodeBOC, legacyWalletCodeHash)
	if err != nil {
		return resolvedContractProfile{}, fmt.Errorf("load %s wallet code: %w", name, err)
	}

	return resolvedContractProfile{
		name:           name,
		minterCode:     minterCode,
		walletCode:     walletCode,
		minterCodeHash: legacyMinterCodeHash,
		walletCodeHash: legacyWalletCodeHash,
	}, nil
}

func loadProfileCell(path, embeddedBOC, expectedHash string) (*cell.Cell, error) {
	var data []byte
	var err error
	if path == "" {
		data, err = base64.StdEncoding.DecodeString(embeddedBOC)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}

	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("parse BOC: %w", err)
	}
	actualHash := cellHashHex(root)
	if actualHash != expectedHash {
		return nil, fmt.Errorf("root cell hash %s, expected %s", actualHash, expectedHash)
	}

	return root, nil
}

func cellHashHex(value *cell.Cell) string {
	return hex.EncodeToString(value.Hash())
}
