// Package network implements the TON consensus wire boundary and the
// session-scoped transport over node-owned dynamic private overlays. Manager
// multiplexes voting-validator and standalone-collator consumers which share a
// consensus overlay while preserving their independent lifecycles and signers.
package network
