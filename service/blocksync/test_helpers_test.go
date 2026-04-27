package blocksync

import (
	"flexserver/internal/logutil"

	"github.com/rs/zerolog"
)

func discardLogger() *zerolog.Logger {
	logger := logutil.Discard()
	return &logger
}
