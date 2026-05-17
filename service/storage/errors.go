package storage

import "errors"

var (
	ErrNotFound             = errors.New("not found")
	ErrCurrentStateAdvanced = errors.New("current state advanced")
)
