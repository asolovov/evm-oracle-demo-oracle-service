package submitter

import (
	"sync/atomic"
	"time"
)

// breakerState is the circuit-breaker lifecycle. Mutated ONLY by the single
// sender goroutine (so the state field needs no lock); the `suspended` atomic
// mirror below is what other goroutines read.
type breakerState int

const (
	breakerClosed   breakerState = iota // normal: sends proceed
	breakerOpen                         // every wallet drained: sender holds, probes on backoff
	breakerHalfOpen                     // a probe found a fundable wallet: trial traffic allowed
)

// breaker guards the sender against hammering the RPC once EVERY broadcaster
// wallet is drained. It trips open only after a send has tried (and been
// refused by) all wallets — so the "funds exhausted" event fires once, not per
// attempt, and not until the whole pool is confirmed broke.
//
// The state machine is driven exclusively by the sender goroutine. `suspended`
// is an atomic mirror so the heartbeat scheduler can pause and the metrics
// gauge can read the state without a data race. Crucially, half-open is NOT
// suspended: a probe that finds a fundable wallet un-pauses so heartbeat /
// queued traffic can drive the recovery trial. Half-open is never terminal — a
// trial that fails on funds re-opens; one that succeeds closes; a generic
// transient leaves it half-open (un-paused) so traffic keeps probing recovery.
type breaker struct {
	state      breakerState
	minBackoff time.Duration
	maxBackoff time.Duration
	curBackoff time.Duration

	suspended atomic.Bool

	// Hooks (set once before Start; nil-tolerant).
	//   onOpen          — episode start: funds-blocked counter + error log. Once.
	//   onClose         — episode end (recovery): info log. Once.
	//   onSuspendChange — every suspend/resume transition: drives the gauge.
	onOpen          func()
	onClose         func()
	onSuspendChange func(suspended bool)
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

// isSuspended reports whether broadcasting is currently suspended (breaker fully
// open). Half-open is NOT suspended. Safe to call from any goroutine.
func (b *breaker) isSuspended() bool { return b.suspended.Load() }

// setSuspended flips the atomic mirror and notifies the gauge hook. Sender
// goroutine only.
func (b *breaker) setSuspended(v bool) {
	b.suspended.Store(v)
	if b.onSuspendChange != nil {
		b.onSuspendChange(v)
	}
}

// tripOpen is called by the sender when a send exhausted every wallet on funds.
//   - from closed: the first trip — suspend, fire onOpen (the single failure
//     event), reset backoff to the floor.
//   - from half-open: the recovery trial failed on funds — re-suspend and grow
//     the backoff so probes space out, but do NOT re-fire onOpen.
func (b *breaker) tripOpen() {
	switch b.state {
	case breakerClosed:
		b.curBackoff = b.minBackoff
		b.state = breakerOpen
		b.setSuspended(true)
		if b.onOpen != nil {
			b.onOpen()
		}
	case breakerHalfOpen:
		b.growBackoff()
		b.state = breakerOpen
		b.setSuspended(true)
	case breakerOpen:
		// Already open; nothing to do (sends don't run while open).
	}
}

// recordSuccess closes the breaker after any successful broadcast, ending the
// episode.
func (b *breaker) recordSuccess() {
	if b.state == breakerClosed {
		return
	}
	b.state = breakerClosed
	b.curBackoff = b.minBackoff
	b.setSuspended(false)
	if b.onClose != nil {
		b.onClose()
	}
}

// toHalfOpen marks that a probe found a fundable wallet. It UN-suspends so
// heartbeat / queued traffic can drive the recovery trial; the trial's outcome
// then closes (success) or re-opens (funds) the breaker. Leaving half-open
// un-suspended is what prevents a permanently-paused heartbeat when a trial
// hits a non-funds transient.
func (b *breaker) toHalfOpen() {
	b.state = breakerHalfOpen
	b.setSuspended(false)
}

func (b *breaker) growBackoff() {
	b.curBackoff *= 2
	if b.curBackoff > b.maxBackoff {
		b.curBackoff = b.maxBackoff
	}
}
