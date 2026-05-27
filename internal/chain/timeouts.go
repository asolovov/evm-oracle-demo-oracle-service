package chain

import "time"

// defaultDialTimeout is the upper bound on the chain-id probe issued during
// Dial. Kept short — if the RPC can't answer ChainID in a couple of seconds
// the service should crash-loop rather than start with a stale connection.
const defaultDialTimeout = 5 * time.Second
