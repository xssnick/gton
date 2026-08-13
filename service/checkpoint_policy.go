package service

const (
	DefaultNextBlockCheckpointBlocks = 400
	DefaultCheckpointBytes           = uint64(512 << 20)
	DefaultSyncBackpressureWindows   = 4
)

func checkpointBackpressureBlocks(target uint32, windows uint32) uint32 {
	if target == 0 || windows == 0 {
		return 0
	}
	if target > ^uint32(0)/windows {
		return ^uint32(0)
	}
	return target * windows
}

func checkpointBackpressureBytes(target uint64, windows uint32) uint64 {
	if target == 0 || windows == 0 {
		return 0
	}
	windowCount := uint64(windows)
	if target > ^uint64(0)/windowCount {
		return ^uint64(0)
	}
	return target * windowCount
}
