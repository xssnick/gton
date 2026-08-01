package blocksync

import (
	"github.com/rs/zerolog"
)

func discardLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}
