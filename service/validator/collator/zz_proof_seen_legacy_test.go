package collator

import "github.com/xssnick/tonutils-go/tvm/cell"

// observeLegacy is the two-valued form of observe the set carried before
// selection and charging became separate bits: an entry is either loaded or a
// bare referenced boundary, and the caller learns whether it existed and which
// of the two it was. It exists so the tests written against that shape keep
// asserting exactly what they asserted, over the table that now holds three
// flags.
func (s *proofSeenHashSet) observeLegacy(hash cell.Hash, loaded bool) (bool, bool) {
	set := proofSeenBoundary
	if loaded {
		set = proofSeenLoaded
	}
	prior := s.observe(hash, set, 0)

	return prior != 0, prior&proofSeenLoaded != 0
}
