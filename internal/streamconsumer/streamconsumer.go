// Package streamconsumer is the long-lived gRPC client of
// indexer.StreamEvents(kinds=[EVENT_KIND_PRICE_REQUESTED]).
//
// Plain Go package (architecture rule 5). The consumer replaces the old
// TriggerUpdate RPC per spec OQ-10: indexer is the single chain-observer and
// emits events past the confirmation gate; oracle reacts to that stream.
//
// Lifecycle:
//
//   - Start() launches Run() in a goroutine; Run blocks until Stop() / ctx
//     cancellation.
//   - Run loops over: open stream from cursor -> receive events -> dispatch
//     to the Dispatcher (the submitter) -> advance cursor on accept. On any
//     stream error or EOF, sleep + reconnect with exponential backoff.
//
// Idempotency:
//
//   - Before dispatch, the consumer asks the repository whether the req_id
//     has already been recorded. The indexer stream guarantees at-least-once
//     delivery; this check makes oracle's reaction at-most-once.
package streamconsumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	indexerv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/indexer/v1"
)

// Dispatcher is the submitter-facing seam. Real impl is *submitter.Submitter;
// tests supply a stub. HandleEvent runs inline on the receive loop; the
// submitter is expected to persist the submission row + kick off broadcast
// asynchronously so this returns quickly.
type Dispatcher interface {
	HandleEvent(ctx context.Context, ev *indexerv1.Event) error
}

// CursorStore is the repository surface the consumer needs. Defined here as
// an interface so tests can sub a fake without pulling pgx in.
type CursorStore interface {
	GetStreamCursor(ctx context.Context) (uint64, error)
	AdvanceStreamCursor(ctx context.Context, block uint64) error
	ExistsByReqID(ctx context.Context, reqID string) (bool, error)
}

// StreamClient is the indexer.v1 client surface we use. Defined here so tests
// can sub a fake without standing up a real gRPC server.
type StreamClient interface {
	StreamEvents(ctx context.Context, in *indexerv1.StreamEventsRequest, opts ...grpc.CallOption) (
		grpc.ServerStreamingClient[indexerv1.Event], error,
	)
}

// Consumer owns the indexer-stream lifecycle.
type Consumer struct {
	client     StreamClient
	store      CursorStore
	dispatcher Dispatcher
	cfg        *config.StreamConfig
	log        *logrus.Entry

	// Lifecycle.
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}

	// Metrics hooks; nil-tolerant so application.go can defer wiring.
	onEventReceived  func(kind string)
	onReconnect      func()
	onLagSeconds     func(float64)
}

// Option tunes the Consumer at construction time.
type Option func(*Consumer)

// WithLogger sets the structured-log handle (optional).
func WithLogger(log *logrus.Entry) Option {
	return func(c *Consumer) { c.log = log }
}

// WithMetricsHooks wires Prometheus counter/gauge callbacks. Any nil hook is
// ignored, so application.go can pass a partial set.
func WithMetricsHooks(onEvent func(string), onReconnect func(), onLag func(float64)) Option {
	return func(c *Consumer) {
		c.onEventReceived = onEvent
		c.onReconnect = onReconnect
		c.onLagSeconds = onLag
	}
}

// New constructs a Consumer. Caller wires Start/Stop from application.go.
func New(client StreamClient, store CursorStore, dispatcher Dispatcher, cfg *config.StreamConfig, opts ...Option) *Consumer {
	c := &Consumer{
		client:     client,
		store:      store,
		dispatcher: dispatcher,
		cfg:        cfg,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		log:        logrus.NewEntry(logrus.StandardLogger()).WithField("component", "streamconsumer"),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start launches the receive loop in a goroutine. Returns immediately.
func (c *Consumer) Start(ctx context.Context) {
	go c.run(ctx)
}

// Stop signals the loop to exit and waits for it to drain. Idempotent.
func (c *Consumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stop) })
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run is the supervisor loop: open stream from cursor, receive, on error
// sleep + reconnect with exponential backoff.
func (c *Consumer) run(parent context.Context) {
	defer close(c.done)

	backoff := time.Duration(c.cfg.ReconnectBackoffSec) * time.Second
	maxBackoff := time.Duration(c.cfg.ReconnectMaxBackoffSec) * time.Second
	if maxBackoff < backoff {
		maxBackoff = backoff
	}
	current := backoff

	for {
		select {
		case <-c.stop:
			return
		case <-parent.Done():
			return
		default:
		}

		// Run one stream session. Returns nil when the parent ctx cancels
		// cleanly; returns an error on any transport / dispatch failure.
		err := c.runOnce(parent)
		if err == nil {
			return // clean shutdown (parent ctx done)
		}

		if c.onReconnect != nil {
			c.onReconnect()
		}
		c.log.WithError(err).Warnf("stream disconnected; reconnecting after %s", current)

		select {
		case <-c.stop:
			return
		case <-parent.Done():
			return
		case <-time.After(current):
		}

		// Exponential bump, capped. If the initial backoff is 0, bump to 1s
		// so reconnect-storms don't spin the CPU under a persistent failure.
		if current == 0 {
			current = time.Second
		} else {
			current *= 2
		}
		if maxBackoff > 0 && current > maxBackoff {
			current = maxBackoff
		}
	}
}

// runOnce opens one StreamEvents session and processes events until the
// stream ends or returns an error. The returned error signals the caller
// to reconnect; (nil) signals normal parent-ctx-cancelled shutdown.
func (c *Consumer) runOnce(ctx context.Context) error {
	fromBlock, err := c.store.GetStreamCursor(ctx)
	if err != nil {
		return fmt.Errorf("get cursor: %w", err)
	}
	if c.cfg.BackfillFromBlock > fromBlock {
		fromBlock = c.cfg.BackfillFromBlock
	}

	stream, err := c.client.StreamEvents(ctx, &indexerv1.StreamEventsRequest{
		Kinds:     []indexerv1.EventKind{indexerv1.EventKind_EVENT_KIND_PRICE_REQUESTED},
		FromBlock: fromBlock,
	})
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	c.log.WithField("from_block", fromBlock).Info("indexer stream opened")

	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("stream EOF")
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("recv: %w", err)
		}

		if err := c.handleEvent(ctx, ev); err != nil {
			// A handler error is treated as a transient stream failure: we
			// log + reconnect from the cursor. The submitter is expected
			// to never error on duplicate work, so a real error here
			// represents either a DB/repo problem or a misconfigured
			// dispatcher.
			return fmt.Errorf("handle event: %w", err)
		}
	}
}

// handleEvent is the per-event critical section: idempotency check ->
// dispatch -> advance cursor.
func (c *Consumer) handleEvent(ctx context.Context, ev *indexerv1.Event) error {
	if ev == nil || ev.GetMeta() == nil {
		return errors.New("malformed event (nil or missing meta)")
	}

	if c.onEventReceived != nil {
		c.onEventReceived(ev.GetKind().String())
	}
	if c.onLagSeconds != nil {
		if obs := ev.GetMeta().GetObservedAt(); obs != nil {
			lag := time.Since(obs.AsTime()).Seconds()
			c.onLagSeconds(lag)
		}
	}

	pr := ev.GetPriceRequested()
	if pr == nil {
		// We're filtered to PRICE_REQUESTED so any other shape is a server
		// bug; record + advance cursor so we don't re-receive it forever.
		c.log.WithField("kind", ev.GetKind().String()).
			Warn("unexpected event payload type; skipping")
		return c.advance(ctx, ev.GetMeta().GetBlockNumber())
	}

	exists, err := c.store.ExistsByReqID(ctx, pr.GetReqId())
	if err != nil {
		return fmt.Errorf("idempotency check: %w", err)
	}
	if exists {
		c.log.WithField("req_id", pr.GetReqId()).
			Debug("submission already recorded; skipping")
		return c.advance(ctx, ev.GetMeta().GetBlockNumber())
	}

	if err := c.dispatcher.HandleEvent(ctx, ev); err != nil {
		return fmt.Errorf("dispatch req_id %s: %w", pr.GetReqId(), err)
	}

	return c.advance(ctx, ev.GetMeta().GetBlockNumber())
}

// advance is a small wrapper so the metric hook reads cleanly.
func (c *Consumer) advance(ctx context.Context, block uint64) error {
	if err := c.store.AdvanceStreamCursor(ctx, block); err != nil {
		return fmt.Errorf("advance cursor to %d: %w", block, err)
	}
	return nil
}
