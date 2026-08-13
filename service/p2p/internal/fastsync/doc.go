// Package fastsync owns the synchronous membership and peer-descriptor state
// used by FastSync overlays. It starts no goroutines and performs no network
// I/O; the parent p2p package owns scheduling and transport.
package fastsync
