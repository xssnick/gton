# Extension implementation guide

This directory contains optional implementations of the released
`service/hooks` SDK. Extensions may import core APIs and service contracts; core
packages must never import this directory or `service/hooks`.

Keep lifecycle and hook adaptation here. If an extension needs a new core
capability, define a narrow implementation-agnostic contract at the owning core
package and wire it in the composition root. Do not add extension-specific
branches, reflection, aliases, or forwarding methods to core code.

Event payloads on block and message paths are borrowed immutable data. Pass
them through without copying. An extension that retains mutable input beyond a
callback must make its own ownership copy.
