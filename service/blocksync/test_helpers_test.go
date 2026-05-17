package blocksync

import (
	"github.com/xssnick/gton/internal/logutil"

	"github.com/rs/zerolog"
)

func discardLogger() *zerolog.Logger {
	logger := logutil.Discard()
	return &logger
}
