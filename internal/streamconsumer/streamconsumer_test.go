package streamconsumer

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	indexerv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/indexer/v1"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type fakeStore struct {
	mu       sync.Mutex
	cursor   uint64
	// existing is keyed by (aggregator|reqID) so we can simulate the real
	// per-aggregator idempotency scope.
	existing map[string]bool
	advances []uint64
}

func newFakeStore(initial uint64) *fakeStore {
	return &fakeStore{cursor: initial, existing: map[string]bool{}}
}

func (f *fakeStore) GetStreamCursor(_ context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursor, nil
}

func (f *fakeStore) AdvanceStreamCursor(_ context.Context, block uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if block >= f.cursor {
		f.cursor = block
	}
	f.advances = append(f.advances, block)
	return nil
}

func existsKey(addr common.Address, reqID string) string {
	return addr.Hex() + "|" + reqID
}

func (f *fakeStore) ExistsForAggregatorReqID(_ context.Context, addr common.Address, reqID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.existing[existsKey(addr, reqID)], nil
}

func (f *fakeStore) markSeen(addr common.Address, reqID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.existing[existsKey(addr, reqID)] = true
}

type recordingDispatcher struct {
	mu   sync.Mutex
	seen []string
	err  error
}

func (d *recordingDispatcher) HandleEvent(_ context.Context, ev *indexerv1.Event) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	d.seen = append(d.seen, ev.GetPriceRequested().GetReqId())
	return nil
}

func (d *recordingDispatcher) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.seen))
	copy(out, d.seen)
	return out
}

// fakeStreamClient lets us script the stream responses. Each call to
// StreamEvents returns the next scripted "session" — a slice of events
// terminated by either io.EOF or a transport error.
type fakeStreamClient struct {
	mu        sync.Mutex
	sessions  []func() (*indexerv1.Event, error)
	openCount int32
}

func (f *fakeStreamClient) StreamEvents(ctx context.Context, _ *indexerv1.StreamEventsRequest, _ ...grpc.CallOption) (
	grpc.ServerStreamingClient[indexerv1.Event], error,
) {
	atomic.AddInt32(&f.openCount, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sessions) == 0 {
		return &scriptedStream{ctx: ctx, next: func() (*indexerv1.Event, error) { return nil, io.EOF }}, nil
	}
	next := f.sessions[0]
	f.sessions = f.sessions[1:]
	return &scriptedStream{ctx: ctx, next: next}, nil
}

func (f *fakeStreamClient) opens() int32 { return atomic.LoadInt32(&f.openCount) }

// scriptedStream implements grpc.ServerStreamingClient[indexerv1.Event] by
// delegating Recv to the next() closure.
type scriptedStream struct {
	grpc.ClientStream
	ctx  context.Context
	next func() (*indexerv1.Event, error)
}

func (s *scriptedStream) Recv() (*indexerv1.Event, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	return s.next()
}

func (s *scriptedStream) Context() context.Context { return s.ctx }

const defaultAgg = "0x000000000000000000000000000000000000aaaa"

func priceRequestedEvent(block uint64, reqID string) *indexerv1.Event {
	return priceRequestedEventFor(block, reqID, defaultAgg)
}

func priceRequestedEventFor(block uint64, reqID, aggregatorHex string) *indexerv1.Event {
	return &indexerv1.Event{
		Meta: &indexerv1.EventMeta{
			ContractAddress: aggregatorHex,
			BlockNumber:     block,
			ObservedAt:      timestamppb.Now(),
		},
		Kind: indexerv1.EventKind_EVENT_KIND_PRICE_REQUESTED,
		Payload: &indexerv1.Event_PriceRequested{
			PriceRequested: &indexerv1.PriceRequestedEvent{
				ReqId:   reqID,
				AssetId: "0xweth",
			},
		},
	}
}

// scriptEvents builds a session function that walks through `events` then
// returns `terminal` (e.g. io.EOF).
func scriptEvents(events []*indexerv1.Event, terminal error) func() (*indexerv1.Event, error) {
	idx := 0
	return func() (*indexerv1.Event, error) {
		if idx >= len(events) {
			return nil, terminal
		}
		ev := events[idx]
		idx++
		return ev, nil
	}
}

func newConsumer(t *testing.T, store *fakeStore, disp *recordingDispatcher, client *fakeStreamClient) *Consumer {
	t.Helper()
	return New(client, store, disp, &config.StreamConfig{
		ReconnectBackoffSec:    1,
		ReconnectMaxBackoffSec: 1,
	})
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestRunOnce_DispatchesAndAdvances(t *testing.T) {
	store := newFakeStore(100)
	disp := &recordingDispatcher{}
	client := &fakeStreamClient{sessions: []func() (*indexerv1.Event, error){
		scriptEvents([]*indexerv1.Event{
			priceRequestedEvent(101, "1"),
			priceRequestedEvent(102, "2"),
		}, io.EOF),
	}}
	c := newConsumer(t, store, disp, client)

	err := c.runOnce(context.Background())
	if err == nil || err.Error() != "stream EOF" {
		t.Fatalf("expected stream EOF, got %v", err)
	}
	got := disp.snapshot()
	if len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Fatalf("dispatcher saw %v", got)
	}
	if store.cursor != 102 {
		t.Fatalf("cursor advanced to %d, want 102", store.cursor)
	}
}

func TestRunOnce_SkipsAlreadyAcked(t *testing.T) {
	store := newFakeStore(100)
	store.markSeen(common.HexToAddress(defaultAgg), "1") // pretend we already dispatched this one
	disp := &recordingDispatcher{}
	client := &fakeStreamClient{sessions: []func() (*indexerv1.Event, error){
		scriptEvents([]*indexerv1.Event{
			priceRequestedEvent(101, "1"),
			priceRequestedEvent(102, "2"),
		}, io.EOF),
	}}
	c := newConsumer(t, store, disp, client)
	_ = c.runOnce(context.Background())

	got := disp.snapshot()
	if len(got) != 1 || got[0] != "2" {
		t.Fatalf("dispatcher should have skipped reqId=1, got %v", got)
	}
	if store.cursor != 102 {
		t.Fatalf("cursor advanced to %d, want 102 (advance still happens on skip)", store.cursor)
	}
}

// TestRunOnce_PerAggregatorIdempotency covers the live-debug bug where the
// streamconsumer's idempotency check skipped every aggregator's req_id=N
// after the FIRST aggregator's req_id=N got persisted, because the check
// was scoped by req_id alone instead of (aggregator, req_id).
//
// req_id is per-aggregator on chain — every PriceAggregator has its own
// counter starting at 1. A multi-asset stress test (8 simultaneous
// requestPrice calls during a live run) revealed that 7 of the 8 events
// were silently dropped after WETH's req_id=3 confirmed first.
func TestRunOnce_PerAggregatorIdempotency(t *testing.T) {
	store := newFakeStore(100)
	disp := &recordingDispatcher{}

	wethAgg := "0x000000000000000000000000000000000000aaaa"
	wbtcAgg := "0x000000000000000000000000000000000000bbbb"

	// Pre-seed: WETH's req_id=3 already recorded as a previous successful
	// submission. The consumer should still dispatch WBTC's req_id=3 since
	// it's a different aggregator's counter.
	store.markSeen(common.HexToAddress(wethAgg), "3")

	client := &fakeStreamClient{sessions: []func() (*indexerv1.Event, error){
		scriptEvents([]*indexerv1.Event{
			priceRequestedEventFor(101, "3", wethAgg), // should SKIP — already seen
			priceRequestedEventFor(102, "3", wbtcAgg), // should DISPATCH — different aggregator
			priceRequestedEventFor(103, "3", wethAgg), // should SKIP — duplicate of seeded one
		}, io.EOF),
	}}
	c := newConsumer(t, store, disp, client)

	_ = c.runOnce(context.Background())

	got := disp.snapshot()
	if len(got) != 1 || got[0] != "3" {
		t.Fatalf("expected exactly one dispatch (WBTC req_id=3), got %v", got)
	}
	if store.cursor != 103 {
		t.Fatalf("cursor must advance past every event (skips advance too); got %d", store.cursor)
	}
}

func TestRunOnce_PropagatesDispatchError(t *testing.T) {
	store := newFakeStore(100)
	disp := &recordingDispatcher{err: errors.New("submitter ded")}
	client := &fakeStreamClient{sessions: []func() (*indexerv1.Event, error){
		scriptEvents([]*indexerv1.Event{priceRequestedEvent(101, "1")}, io.EOF),
	}}
	c := newConsumer(t, store, disp, client)

	err := c.runOnce(context.Background())
	if err == nil {
		t.Fatal("expected dispatch error to propagate")
	}
	if store.cursor != 100 {
		t.Fatalf("cursor should not advance on error; got %d", store.cursor)
	}
}

func TestRun_ReconnectsAfterEOF(t *testing.T) {
	store := newFakeStore(100)
	disp := &recordingDispatcher{}
	client := &fakeStreamClient{sessions: []func() (*indexerv1.Event, error){
		scriptEvents([]*indexerv1.Event{priceRequestedEvent(101, "1")}, io.EOF),
		scriptEvents([]*indexerv1.Event{priceRequestedEvent(102, "2")}, io.EOF),
		scriptEvents([]*indexerv1.Event{priceRequestedEvent(103, "3")}, io.EOF),
	}}

	c := New(client, store, disp, &config.StreamConfig{
		ReconnectBackoffSec:    0, // immediate reconnect for the test
		ReconnectMaxBackoffSec: 0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.Start(ctx)

	// First reconnect is immediate (cfg backoff=0); subsequent reconnects bump
	// to a 1s safety floor so a failing stream doesn't spin the CPU. 3 events
	// therefore need up to ~2s.
	waitFor(t, func() bool {
		return len(disp.snapshot()) == 3
	}, 3*time.Second)

	cancel()
	_ = c.Stop(context.Background())

	if got := client.opens(); got < 3 {
		t.Fatalf("expected at least 3 stream opens (reconnects), got %d", got)
	}
}

func TestRun_StopHonoured(t *testing.T) {
	store := newFakeStore(100)
	disp := &recordingDispatcher{}
	client := &fakeStreamClient{sessions: []func() (*indexerv1.Event, error){
		// Session that blocks forever until ctx cancels.
		func() (*indexerv1.Event, error) {
			select {} // unreachable in practice since scriptedStream.Recv guards on ctx
		},
	}}
	c := newConsumer(t, store, disp, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	cancel() // parent ctx cancels first; Recv exits
	if err := c.Stop(stopCtx); err != nil {
		t.Fatalf("Stop blocked: %v", err)
	}
}

func TestRunOnce_HonoursBackfillFromBlockFloor(t *testing.T) {
	store := newFakeStore(50)
	disp := &recordingDispatcher{}
	// Capture which fromBlock the client received.
	var got uint64
	captureClient := &captureFromBlockClient{
		out: &got,
		next: scriptEvents(nil, io.EOF),
	}
	c := New(captureClient, store, disp, &config.StreamConfig{
		BackfillFromBlock:      100, // higher than cursor
		ReconnectBackoffSec:    0,
		ReconnectMaxBackoffSec: 0,
	})
	_ = c.runOnce(context.Background())
	if got != 100 {
		t.Fatalf("from_block should be max(cursor, backfill); got %d", got)
	}
}

// captureFromBlockClient is a stream client that records the from_block it was
// asked for before returning a one-shot scripted stream.
type captureFromBlockClient struct {
	out  *uint64
	next func() (*indexerv1.Event, error)
}

func (c *captureFromBlockClient) StreamEvents(ctx context.Context, in *indexerv1.StreamEventsRequest, _ ...grpc.CallOption) (
	grpc.ServerStreamingClient[indexerv1.Event], error,
) {
	*c.out = in.GetFromBlock()
	return &scriptedStream{ctx: ctx, next: c.next}, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
