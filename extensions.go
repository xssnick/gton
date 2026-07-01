package gton

import (
	"fmt"

	"github.com/xssnick/gton/service/hooks"
)

func extensionFromFactory(factory hooks.ExtensionFactory, node hooks.Node) (hooks.Extension, error) {
	if factory == nil {
		return nil, nil
	}

	extension, err := factory(node)
	if err != nil {
		return nil, fmt.Errorf("initialize static extension: %w", err)
	}
	if extension == nil {
		return nil, fmt.Errorf("static extension returned nil extension")
	}
	return extension, nil
}
