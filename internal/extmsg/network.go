package extmsg

import "errors"

// ErrNetworkOffline reports that the external-message network cannot accept
// a broadcast. Higher layers can classify wrapped sender errors with
// errors.Is without depending on a concrete transport package.
var ErrNetworkOffline = errors.New("external message network is offline")
