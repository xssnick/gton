package liteserver

import (
	"bytes"
	"fmt"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func parseTrustedBlockBOC(block ton.BlockIDExt, data []byte) (*cell.Cell, error) {
	roots, _, err := cell.FromBOCMultiRootReader(bytes.NewReader(data), cell.BOCParseOptions{TrustedHashes: true})
	if err != nil {
		return nil, fmt.Errorf("parse block boc %s: %w", storage.FormatBlockRef(block), err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("block boc %s should contain exactly one root, got %d", storage.FormatBlockRef(block), len(roots))
	}

	root, err := normalizeLiveBlockRoot(block, roots[0])
	if err != nil {
		return nil, fmt.Errorf("normalize block boc %s: %w", storage.FormatBlockRef(block), err)
	}
	return root, nil
}
