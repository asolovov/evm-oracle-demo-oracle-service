package submitter

import (
	"sync/atomic"
	"time"
)

// breakerState is the circuit-breaker lifecycle. Mutated ONLY by the single
// sender goroutine (so the state field needs no lock); the `open` atomic mirror
// below is what other goroutines read.
type breakerState int

const (
	breakerClosed   breakerState = iota // normal: sends proceed
	breakerOpen                         // every wallet drained: sender holds, probes on backoff
	breakerHalfOpen                     // a probe found a fundable wallet: next send is the trial
)

// breaker guards the sender against hammering the RPC once EVERY broadcaster
// wallet is drained. It trips open only after a send has tried (and been
// refused by) all wallets — so the "funds exhausted" event fires once, not per
// attempt, and not until the whole pool is confirmed broke.
//
// The state machine is driven exclusively by the sender goroutine. `open` is an
// atomic mirror so the heartbeat scheduler can pause and the metrics gauge can
// read the state without a data race.
type breaker struct {
	state      breakerState
	minBackoff time.Duration
	maxBackoff time.Duration
	curBackoff time.Duration

	open atomic.Bool

	// Transition hooks (set once before Start; nil-tolerant). onOpen fires
	// exactly once per closed→open transition; onClose once per recovery.
	onOpen  func()
	onClose func()
}

func newBreaker(minBackoff, maxBackoff time.Duration) *breaker {
	if minBackoff <= 0 {
		minBackoff = time.Minute
	}
	if maxBackoff < minBackoff {
		maxBackoff = minBackoff
	}
	return &breaker{
		state:      breakerClosed,
		minBackoff: minBackoff,
		maxBackoff: maxBackoff,
		curBackoff: minBackoff,
	}
}

// isOpen reports whether broadcasting is currently suspended (open OR a pending
// half-open trial). Safe to call from any goroutine.
func (b *breaker) isOpen() bool { return b.open.Load() }

// tripOpen is called by the sender when a send exhausted every wallet on funds.
//   - from closed: the first trip — fire onOpen (the single failure event),
//     reset backoff to the floor.
//   - from half-open: the recovery trial failed — grow the backoff so probes
//     space out, but do NOT re-fire onOpen.
func (b *breaker) tripOpen() {
	switch b.state {
	case breakerClosed:
		b.curBackoff = b.minBackoff
		b.state = breakerOpen
		b.open.Store(true)
		if b.onOpen != nil {
			b.onOpen()
		}
	case breakerHalfOpen:
		b.growBackoff()
		b.state = breakerOpen
	case breakerOpen:
		// Already open; nothing to do (sends don't run while open).
	}
}

// recordSuccess closes the breaker after any successful broadcast.
func (b *breaker) recordSuccess() {
	if b.state == breakerClosed {
		return
	}
	b.state = breakerClosed
	b.curBackoff = b.minBackoff
	b.open.Store(false)
	if b.onClose != nil {
		b.onClose()
	}
}

// toHalfOpen marks that a probe found a fundable wallet; the next send is the
// trial. The open mirror stays true (heartbeat stays paused) until the trial
// actually succeeds.
func (b *breaker) toHalfOpen() { b.state = breakerHalfOpen }

func (b *breaker) growBackoff() {
	b.curBackoff *= 2
	if b.curBackoff > b.maxBackoff {
		b.curBackoff = b.maxBackoff
	}
}
