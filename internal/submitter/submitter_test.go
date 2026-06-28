package submitter

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	chainpkg "github.com/asolovov/evm-oracle-demo-oracle-service/internal/chain"
	indexerv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/indexer/v1"
	pricev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/price/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

// ---------------------------------------------------------------------------
// Asset fixtures: 3 distinct aggregators so per-asset isolation is testable.
// ---------------------------------------------------------------------------

const (
	aggWETH = "0x075be31662c2548c4e940d7e769c328a34dcb281"
	aggWBTC = "0xf8ad3a2505eece7ad276db038c7c56930bd436e4"
	aggLINK = "0xecc43e6ec38ce135b81ae8042df96eef55915d14"
)

func aggregators() map[string]string {
	return map[string]string{"weth": aggWETH, "wbtc": aggWBTC, "link": aggLINK}
}

func assetIDs() map[common.Address]common.Hash {
	out := map[common.Address]common.Hash{}
	for _, a := range aggregators() {
		out[common.HexToAddress(a)] = common.HexToHash("0x" + a[2:]) // any stable bytes32
	}
	return out
}

// ---------------------------------------------------------------------------
// fakePrice: per-symbol behavior. A symbol can be configured to error N times
// then succeed, or always error (e.g. NotFound).
// ---------------------------------------------------------------------------

type priceBehavior struct {
	median     float64
	err        error // returned while calls <= failTimes (or always if failForever)
	failTimes  int
	failForever bool
}

type fakePrice struct {
	mu     sync.Mutex
	byAsset map[string]*priceBehavior
	calls  map[string]int
}

func newFakePrice() *fakePrice {
	return &fakePrice{byAsset: map[string]*priceBehavior{}, calls: map[string]int{}}
}

func (f *fakePrice) set(asset string, b *priceBehavior) { f.byAsset[asset] = b }

func (f *fakePrice) GetPrice(_ context.Context, asset string) (*pricev1.AggregatedPrice, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[asset]++
	b, ok := f.byAsset[asset]
	if !ok {
		b = &priceBehavior{median: 100.0}
	}
	if b.failForever || (b.err != nil && f.calls[asset] <= b.failTimes) {
		if b.err != nil {
			return nil, b.err
		}
	}
	return &pricev1.AggregatedPrice{AssetId: asset, MedianPrice: b.median, AggregatedAt: timestamppb.Now()}, nil
}

// ---------------------------------------------------------------------------
// fakeChain: records nonce-assignment order; configurable send/receipt result.
// ---------------------------------------------------------------------------

type fakeChain struct {
	mu          sync.Mutex
	seedNonce   uint64
	noncesUsed  []uint64
	sentFrom    []common.Address // broadcaster address per successful send (in order)
	submitErr   error            // if set, SubmitFulfillment returns it for ALL sends
	receiptOK   bool             // true => mined success
	txN         int
	latestStart *big.Int
	// balances overrides the default balance per address (for the gate). A nil
	// map (or missing key) => defaultBalance (effectively unlimited).
	balances       map[common.Address]*big.Int
	defaultBalance *big.Int
	// onSubmit, if set, runs at the top of SubmitFulfillment and lets a test
	// inject a per-wallet send error (e.g. funds error from the node itself,
	// independent of the balance gate). Returning nil falls through to success.
	onSubmit func(from common.Address) error
}

func newFakeChain() *fakeChain {
	return &fakeChain{
		seedNonce: 7, receiptOK: true, latestStart: big.NewInt(0),
		balances:       map[common.Address]*big.Int{},
		defaultBalance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil), // 1 ETH
	}
}

func (c *fakeChain) SuggestGas(_ context.Context, _ int) (chainpkg.GasStrategy, error) {
	return chainpkg.GasStrategy{GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2)}, nil
}
func (c *fakeChain) NonceAt(_ context.Context, _ common.Address) (uint64, error) {
	return c.seedNonce, nil
}
func (c *fakeChain) SubmitFulfillment(_ context.Context, auth *bind.TransactOpts,
	_ common.Address, _, _, _ *big.Int, _ [][]byte, _ chainpkg.GasStrategy) (common.Hash, error) {
	// onSubmit is set once before Start (no concurrent write) and may inject a
	// per-wallet error; called outside the lock so the closure can use its own
	// synchronization without contending on c.mu.
	if c.onSubmit != nil {
		if err := c.onSubmit(auth.From); err != nil {
			return common.Hash{}, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.submitErr != nil {
		return common.Hash{}, c.submitErr
	}
	c.noncesUsed = append(c.noncesUsed, auth.Nonce.Uint64())
	c.sentFrom = append(c.sentFrom, auth.From)
	c.txN++
	return common.BigToHash(big.NewInt(int64(c.txN))), nil
}
func (c *fakeChain) ReplaceFulfillment(ctx context.Context, auth *bind.TransactOpts,
	agg common.Address, reqID, price, ts *big.Int, sigs [][]byte, gas chainpkg.GasStrategy) (common.Hash, error) {
	return c.SubmitFulfillment(ctx, auth, agg, reqID, price, ts, sigs, gas)
}
func (c *fakeChain) TxReceipt(_ context.Context, _ common.Hash) (*types.Receipt, error) {
	if c.receiptOK {
		return &types.Receipt{Status: types.ReceiptStatusSuccessful, GasUsed: 100000}, nil
	}
	return nil, chainpkg.ErrTxNotMined
}
func (c *fakeChain) LatestStartedAt(_ context.Context, _ common.Address) (*big.Int, error) {
	return new(big.Int).Set(c.latestStart), nil
}
func (c *fakeChain) BalanceAt(_ context.Context, addr common.Address) (*big.Int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.balances[addr]; ok {
		return new(big.Int).Set(b), nil
	}
	return new(big.Int).Set(c.defaultBalance), nil
}
func (c *fakeChain) setBalance(addr common.Address, wei *big.Int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.balances[addr] = wei
}
func (c *fakeChain) recordedNonces() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]uint64(nil), c.noncesUsed...)
	return out
}
func (c *fakeChain) recordedFrom() []common.Address {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]common.Address(nil), c.sentFrom...)
	return out
}

// ---------------------------------------------------------------------------
// fakeSigner
// ---------------------------------------------------------------------------

// fakeSigner exposes a broadcaster pool. A single-address pool reproduces the
// pre-06.3 behavior; multi-address pools exercise rotation + failover.
type fakeSigner struct{ addrs []common.Address }

func newFakeSigner(addrs ...common.Address) *fakeSigner {
	if len(addrs) == 0 {
		addrs = []common.Address{common.HexToAddress("0xb0b")}
	}
	return &fakeSigner{addrs: addrs}
}

func (f *fakeSigner) BuildDigest(_ *big.Int, _ common.Hash, _, _ *big.Int, _ common.Address) ([]byte, error) {
	return make([]byte, 32), nil
}
func (f *fakeSigner) Sign(_ []byte) ([][]byte, error) { return [][]byte{{0x01}, {0x02}, {0x03}}, nil }
func (f *fakeSigner) Broadcasters() []common.Address {
	return append([]common.Address(nil), f.addrs...)
}
func (f *fakeSigner) NewBroadcasterFor(addr common.Address) (*bind.TransactOpts, error) {
	for _, a := range f.addrs {
		if a == addr {
			return &bind.TransactOpts{
				From:    addr,
				Signer:  func(_ common.Address, t *types.Transaction) (*types.Transaction, error) { return t, nil },
				Context: context.Background(),
			}, nil
		}
	}
	return nil, errors.New("unknown broadcaster")
}

// ---------------------------------------------------------------------------
// fakeRepo: thread-safe in-memory store (workers + sender + watchers all write).
// ---------------------------------------------------------------------------

type fakeRepo struct {
	mu        sync.Mutex
	rows      map[int64]*models.Submission
	nextID    int64
	resumable []*models.Submission
}

func newFakeRepo() *fakeRepo { return &fakeRepo{rows: map[int64]*models.Submission{}} }

func (r *fakeRepo) EnqueueRequest(_ context.Context, s *models.Submission) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	cp := *s
	cp.ID = r.nextID
	r.rows[cp.ID] = &cp
	s.ID = cp.ID
	return cp.ID, nil
}
func (r *fakeRepo) InsertSubmission(_ context.Context, s *models.Submission) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	cp := *s
	cp.ID = r.nextID
	r.rows[cp.ID] = &cp
	s.ID = cp.ID
	return cp.ID, nil
}
func (r *fakeRepo) UpdateSubmission(_ context.Context, s *models.Submission) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rows[s.ID]; !ok {
		return errors.New("not found")
	}
	cp := *s
	r.rows[s.ID] = &cp
	return nil
}
func (r *fakeRepo) MarkExpired(_ context.Context, id int64, lastErr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if row, ok := r.rows[id]; ok {
		row.Status = models.SubmissionStatusExpired
		row.LastError = lastErr
	}
	return nil
}
func (r *fakeRepo) LoadResumable(_ context.Context) ([]*models.Submission, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.resumable
	r.resumable = nil
	return out, nil
}
func (r *fakeRepo) ExpireOverdue(_ context.Context) (int, error) { return 0, nil }
func (r *fakeRepo) InsertPendingTx(_ context.Context, _ int64, _ string, _ uint64, _ string, _ []byte) error {
	return nil
}
func (r *fakeRepo) DeletePendingTx(_ context.Context, _ string) error { return nil }

func (r *fakeRepo) byAssetStatus(asset string, st models.SubmissionStatus) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, row := range r.rows {
		if row.AssetID == asset && row.Status == st {
			n++
		}
	}
	return n
}
func (r *fakeRepo) countStatus(st models.SubmissionStatus) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, row := range r.rows {
		if row.Status == st {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

func newSubmitter(t *testing.T, fc *fakeChain, fp *fakePrice, fr *fakeRepo, ttl time.Duration, extra ...Option) *Submitter {
	t.Helper()
	return newSubmitterWithSigner(t, fc, fp, fr, ttl, newFakeSigner(), extra...)
}

func newSubmitterWithSigner(t *testing.T, fc *fakeChain, fp *fakePrice, fr *fakeRepo, ttl time.Duration, sgn *fakeSigner, extra ...Option) *Submitter {
	t.Helper()
	cfg := &config.SubmissionConfig{
		MaxRetries: 2, ReplaceAfterSec: 1, GasMultiplier: 1.1, ConfirmTimeoutSec: 30,
		Workers: 3, RequestTTLSec: int(ttl.Seconds()),
		GasLimitEstimate: 300_000, BreakerBackoffMinSec: 60, BreakerBackoffMaxSec: 900,
	}
	opts := append([]Option{
		WithPollInterval(20 * time.Millisecond),
		WithRetryBackoff(30 * time.Millisecond),
		WithBreakerBackoff(40*time.Millisecond, 200*time.Millisecond),
	}, extra...)
	return New(fc, fp, sgn, fr,
		cfg, &config.ConversionConfig{OnChainDecimals: 8},
		aggregators(), assetIDs(), opts...)
}

func priceRequested(aggHex, reqID string) *indexerv1.Event {
	return &indexerv1.Event{
		Meta: &indexerv1.EventMeta{ContractAddress: aggHex, BlockNumber: 100},
		Kind: indexerv1.EventKind_EVENT_KIND_PRICE_REQUESTED,
		Payload: &indexerv1.Event_PriceRequested{
			PriceRequested: &indexerv1.PriceRequestedEvent{ReqId: reqID, AssetId: "0x"},
		},
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
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

func startSubmitter(t *testing.T, s *Submitter) {
	t.Helper()
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandleEvent_HappyPath_Confirmed(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450.20})
	s := newSubmitter(t, fc, fp, fr, time.Minute)
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })
}

// TestHeadOfLineIsolation is the core fix: a middle un-priceable asset must NOT
// block the others. weth + link confirm; wbtc (price always NotFound) expires;
// neither weth nor link waits on wbtc.
func TestHeadOfLineIsolation(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450})
	fp.set("link", &priceBehavior{median: 14})
	fp.set("wbtc", &priceBehavior{err: errors.New("rpc error: code = NotFound desc = no price for asset \"wbtc\" yet"), failForever: true})

	s := newSubmitter(t, fc, fp, fr, 1*time.Second) // short TTL so wbtc expires fast
	startSubmitter(t, s)

	// Enqueue wbtc FIRST so, under the old serial design, it would block the rest.
	for _, ev := range []*indexerv1.Event{
		priceRequested(aggWBTC, "1"),
		priceRequested(aggWETH, "1"),
		priceRequested(aggLINK, "1"),
	} {
		if err := s.HandleEvent(context.Background(), ev); err != nil {
			t.Fatalf("HandleEvent: %v", err)
		}
	}

	waitFor(t, 3*time.Second, func() bool {
		return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 &&
			fr.byAssetStatus("link", models.SubmissionStatusConfirmed) == 1
	})
	waitFor(t, 4*time.Second, func() bool {
		return fr.byAssetStatus("wbtc", models.SubmissionStatusExpired) == 1
	})
}

// TestNonceSerialization: many concurrent-ready sends must produce strictly
// sequential, gap-free, unique nonces from the single sender goroutine.
func TestNonceSerialization(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fc.seedNonce = 100
	for _, sym := range []string{"weth", "wbtc", "link"} {
		fp.set(sym, &priceBehavior{median: 1})
	}
	s := newSubmitter(t, fc, fp, fr, time.Minute)
	startSubmitter(t, s)

	const n = 9
	aggs := []string{aggWETH, aggWBTC, aggLINK}
	for i := 0; i < n; i++ {
		if err := s.HandleEvent(context.Background(), priceRequested(aggs[i%3], big.NewInt(int64(i+1)).String())); err != nil {
			t.Fatalf("HandleEvent: %v", err)
		}
	}
	waitFor(t, 3*time.Second, func() bool { return len(fc.recordedNonces()) == n })

	got := fc.recordedNonces()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i := 0; i < n; i++ {
		want := uint64(100 + i)
		if got[i] != want {
			t.Fatalf("nonce[%d] = %d, want %d (gap/reuse?); full: %v", i, got[i], want, got)
		}
	}
}

// TestTTLExpiry: a request whose price never becomes available is marked
// expired within the TTL and consumes no nonce.
func TestTTLExpiry(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{err: errors.New("NotFound"), failForever: true})
	// TTL is second-granular (config is RequestTTLSec int); 1s is the floor.
	s := newSubmitter(t, fc, fp, fr, 1*time.Second)
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusExpired) == 1 })
	if got := len(fc.recordedNonces()); got != 0 {
		t.Fatalf("expired request must consume no nonce; got %d", got)
	}
}

// TestTransientThenSuccess: price fails twice then succeeds; the request
// retries within TTL and eventually confirms.
func TestTransientThenSuccess(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450, err: errors.New("temporarily down"), failTimes: 2})
	s := newSubmitter(t, fc, fp, fr, 5*time.Second)
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 4*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })
}

// TestPermanentRevert: a broadcast revert marks the request failed and does not
// loop forever (no requeue).
func TestPermanentRevert(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fc.submitErr = errors.New("fulfillPrice: execution reverted")
	fp.set("weth", &priceBehavior{median: 3450})
	s := newSubmitter(t, fc, fp, fr, time.Minute)
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusFailed) == 1 })
	// Give it a moment; ensure it didn't also enqueue endless retries.
	time.Sleep(200 * time.Millisecond)
	if got := len(fc.recordedNonces()); got != 0 {
		t.Fatalf("reverted send must consume no nonce; got %d", got)
	}
}

// TestHandleHeartbeat_BypassesQueue: heartbeats use reqId 0, never create a
// `queued` row (EnqueueRequest not called), and confirm via a fresh row.
func TestHandleHeartbeat_BypassesQueue(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450})
	s := newSubmitter(t, fc, fp, fr, time.Minute)
	startSubmitter(t, s)

	if err := s.HandleHeartbeat(context.Background(), "weth"); err != nil {
		t.Fatalf("HandleHeartbeat: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })
	if fr.countStatus(models.SubmissionStatusQueued) != 0 {
		t.Fatal("heartbeat must not create a queued row")
	}
	// The single row should carry the heartbeat reqId.
	fr.mu.Lock()
	defer fr.mu.Unlock()
	for _, row := range fr.rows {
		if row.ReqID != models.HeartbeatReqID {
			t.Fatalf("heartbeat row reqID = %q, want %q", row.ReqID, models.HeartbeatReqID)
		}
	}
}

func TestHandleHeartbeat_UnknownSymbol(t *testing.T) {
	s := newSubmitter(t, newFakeChain(), newFakePrice(), newFakeRepo(), time.Minute)
	startSubmitter(t, s)
	if err := s.HandleHeartbeat(context.Background(), "doge"); err == nil {
		t.Fatal("expected unknown-symbol error")
	}
}

func TestHandleEvent_UnknownAggregator_SkipNotError(t *testing.T) {
	fr := newFakeRepo()
	s := newSubmitter(t, newFakeChain(), newFakePrice(), fr, time.Minute)
	startSubmitter(t, s)
	ev := priceRequested("0x0000000000000000000000000000000000000000", "1")
	if err := s.HandleEvent(context.Background(), ev); err != nil {
		t.Fatalf("expected nil (skip), got %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if len(fr.rows) != 0 {
		t.Fatalf("unknown aggregator must not enqueue; got %d rows", len(fr.rows))
	}
}

// TestRecovery: a durable `queued` row present at startup is processed to a
// terminal state without any stream event (crash-recovery path).
func TestRecovery(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450})
	fr.resumable = []*models.Submission{{
		ID: 42, ReqID: "5", AssetID: "weth", Aggregator: common.HexToAddress(aggWETH),
		Status: models.SubmissionStatusQueued, ExpiresAt: time.Now().Add(time.Minute),
	}}
	fr.rows[42] = fr.resumable[0]

	s := newSubmitter(t, fc, fp, fr, time.Minute)
	startSubmitter(t, s) // Start() runs recovery
	waitFor(t, 2*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })
}

// ---------------------------------------------------------------------------
// Multi-wallet broadcaster: rotation, failover, and the breaker (06.2/06.3).
// ---------------------------------------------------------------------------

var (
	walletA = common.HexToAddress("0x00000000000000000000000000000000000000Aa")
	walletB = common.HexToAddress("0x00000000000000000000000000000000000000Bb")
	walletC = common.HexToAddress("0x00000000000000000000000000000000000000Cc")
)

// gateFloor is gasFeeCap(2) * gasLimitEstimate(300000); a balance below this
// fails the pre-flight gate.
func gateFloor() *big.Int { return big.NewInt(2 * 300_000) }

// TestFailoverWhenWalletDrained: wallet A is drained (fails the balance gate)
// but B is funded — the send must fail over to B, advance B's nonce from its
// seed, leave A's nonce untouched, and confirm. (Fixes "using only one wallet".)
func TestFailoverWhenWalletDrained(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fc.seedNonce = 50
	fp.set("weth", &priceBehavior{median: 3450})
	fc.setBalance(walletA, big.NewInt(0)) // drained → gate skips it
	// walletB uses the default (1 ETH) balance.

	s := newSubmitterWithSigner(t, fc, fp, fr, time.Minute, newFakeSigner(walletA, walletB))
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })

	from := fc.recordedFrom()
	if len(from) != 1 || from[0] != walletB {
		t.Fatalf("expected the single broadcast to come from walletB, got %v", from)
	}
	if got := fc.recordedNonces(); len(got) != 1 || got[0] != 50 {
		t.Fatalf("walletB nonce should start at its seed 50, got %v", got)
	}
}

// TestNoFailureUntilAllWalletsTried is the core requirement: with EVERY wallet
// drained, the funds-failure event fires exactly once (only after the whole
// pool was tried), the request is NOT marked terminally failed, and no nonce is
// consumed. Refunding one wallet then recovers via the breaker and confirms.
func TestNoFailureUntilAllWalletsTried(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450})
	for _, w := range []common.Address{walletA, walletB, walletC} {
		fc.setBalance(w, big.NewInt(0)) // all drained
	}

	var fundsBlocked atomic.Int64
	var breakerOpen atomic.Bool
	s := newSubmitterWithSigner(t, fc, fp, fr, time.Minute, newFakeSigner(walletA, walletB, walletC),
		WithFundsMetrics(
			func() { fundsBlocked.Add(1) },
			func(open bool) { breakerOpen.Store(open) },
		),
	)
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	// The breaker opens after the pool is exhausted, firing the event once.
	waitFor(t, 2*time.Second, func() bool { return breakerOpen.Load() && s.BroadcastSuspended() })
	if got := fundsBlocked.Load(); got != 1 {
		t.Fatalf("funds-blocked event should fire exactly once, got %d", got)
	}
	if n := fr.countStatus(models.SubmissionStatusFailed); n != 0 {
		t.Fatalf("a funds shortage must NOT mark the request failed; got %d failed rows", n)
	}
	if n := len(fc.recordedNonces()); n != 0 {
		t.Fatalf("no wallet could pay; no nonce must be consumed, got %d", n)
	}

	// Refund walletB; the breaker probes, recovers, and the request confirms.
	fc.setBalance(walletB, gateFloor())
	waitFor(t, 3*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })
	waitFor(t, time.Second, func() bool { return !breakerOpen.Load() && !s.BroadcastSuspended() })

	if got := fundsBlocked.Load(); got != 1 {
		t.Fatalf("funds-blocked event must not re-fire on recovery; got %d", got)
	}
	from := fc.recordedFrom()
	if len(from) != 1 || from[0] != walletB {
		t.Fatalf("recovery broadcast should come from the refunded walletB, got %v", from)
	}
}

// TestPerWalletNoncesSequential: across a rotating pool, each wallet's nonces
// stay strictly sequential + gap-free from its own seed (no cross-wallet bleed).
func TestPerWalletNoncesSequential(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fc.seedNonce = 200
	for _, sym := range []string{"weth", "wbtc", "link"} {
		fp.set(sym, &priceBehavior{median: 1})
	}
	s := newSubmitterWithSigner(t, fc, fp, fr, time.Minute, newFakeSigner(walletA, walletB))
	startSubmitter(t, s)

	const n = 8
	aggs := []string{aggWETH, aggWBTC, aggLINK}
	for i := 0; i < n; i++ {
		if err := s.HandleEvent(context.Background(), priceRequested(aggs[i%3], big.NewInt(int64(i+1)).String())); err != nil {
			t.Fatalf("HandleEvent: %v", err)
		}
	}
	waitFor(t, 3*time.Second, func() bool { return len(fc.recordedFrom()) == n })

	// Bucket the recorded nonces per wallet; each bucket must be 200,201,202...
	perWallet := map[common.Address][]uint64{}
	from, nonces := fc.recordedFrom(), fc.recordedNonces()
	for i := range from {
		perWallet[from[i]] = append(perWallet[from[i]], nonces[i])
	}
	if len(perWallet) != 2 {
		t.Fatalf("expected both wallets used (round-robin), got %d", len(perWallet))
	}
	for w, ns := range perWallet {
		sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
		for i, got := range ns {
			if want := uint64(200 + i); got != want {
				t.Fatalf("wallet %s nonce[%d]=%d, want %d (gap/bleed?); full %v", w.Hex(), i, got, want, ns)
			}
		}
	}
}

// TestRevertDoesNotFailover: a deterministic revert is permanent — the same
// calldata reverts from any wallet, so the sender must mark FAILED immediately
// without trying other wallets.
func TestRevertDoesNotFailover(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fc.submitErr = errors.New("fulfillPrice: execution reverted")
	fp.set("weth", &priceBehavior{median: 3450})
	s := newSubmitterWithSigner(t, fc, fp, fr, time.Minute, newFakeSigner(walletA, walletB, walletC))
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusFailed) == 1 })
	time.Sleep(150 * time.Millisecond)
	if s.BroadcastSuspended() {
		t.Fatal("a revert must not trip the funds breaker")
	}
}

// TestFundsErrorFromSubmitFailsOver: wallet A PASSES the balance gate but its
// broadcast is rejected by the node with an authoritative `insufficient funds`
// — the send must fail over to B (no balance corroboration needed for the
// node's own funds rejection).
func TestFundsErrorFromSubmitFailsOver(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fc.seedNonce = 70
	fp.set("weth", &priceBehavior{median: 3450})
	fc.onSubmit = func(from common.Address) error {
		if from == walletA {
			return errors.New("insufficient funds for transfer")
		}
		return nil
	}
	s := newSubmitterWithSigner(t, fc, fp, fr, time.Minute, newFakeSigner(walletA, walletB))
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })

	from := fc.recordedFrom()
	if len(from) != 1 || from[0] != walletB {
		t.Fatalf("expected failover to walletB, got %v", from)
	}
	if s.BroadcastSuspended() {
		t.Fatal("a single-wallet funds failover must not suspend broadcasting")
	}
}

// TestGasAllowanceOnFundedWalletRetries: a wallet that PASSES the balance gate
// but returns the ambiguous `gas required exceeds allowance` must be treated as
// a transient (retry the item), NOT a drain — no failover, no breaker trip.
func TestGasAllowanceOnFundedWalletRetries(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450})
	var calls atomic.Int64
	fc.onSubmit = func(_ common.Address) error {
		if calls.Add(1) <= 2 { // fail the first two attempts, then succeed
			return errors.New("gas required exceeds allowance (7800)")
		}
		return nil
	}
	s := newSubmitterWithSigner(t, fc, fp, fr, 5*time.Second, newFakeSigner(walletA),
		WithFundsMetrics(func() { t.Error("funds-blocked must NOT fire for a funded wallet's allowance error") }, nil),
	)
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 4*time.Second, func() bool { return fr.byAssetStatus("weth", models.SubmissionStatusConfirmed) == 1 })
	if s.BroadcastSuspended() {
		t.Fatal("allowance error on a funded wallet must not suspend broadcasting")
	}
}

// TestBreakerUnsuspendsOnProbe is the B1 regression. Once the breaker is open,
// recovery must come from the balance PROBE un-suspending (moving to half-open)
// — NOT from a successful send. Here every broadcast keeps failing with a
// NON-funds transient even after the wallet is refunded, so the only way the
// breaker can un-suspend is the probe. The buggy version left half-open
// suspended, pinning the gauge at 1 and pausing heartbeats forever.
func TestBreakerUnsuspendsOnProbe(t *testing.T) {
	fc, fp, fr := newFakeChain(), newFakePrice(), newFakeRepo()
	fp.set("weth", &priceBehavior{median: 3450})
	fc.setBalance(walletA, big.NewInt(0)) // drained → gate fails → breaker opens
	// Sends always fail with a non-funds transient, so recovery can ONLY come
	// from the probe un-suspending half-open, never from a successful send.
	fc.onSubmit = func(_ common.Address) error { return errors.New("connection refused") }

	s := newSubmitterWithSigner(t, fc, fp, fr, 30*time.Second, newFakeSigner(walletA))
	startSubmitter(t, s)

	if err := s.HandleEvent(context.Background(), priceRequested(aggWETH, "1")); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return s.BroadcastSuspended() })

	// Refund: the probe must un-suspend even though sends keep failing.
	fc.setBalance(walletA, gateFloor())
	waitFor(t, 2*time.Second, func() bool { return !s.BroadcastSuspended() })

	// And it must STAY un-suspended — a non-funds transient on a funded wallet
	// must not re-trip the funds breaker.
	time.Sleep(300 * time.Millisecond)
	if s.BroadcastSuspended() {
		t.Fatal("funded wallet with a non-funds transient must not re-suspend the funds breaker")
	}
}

func TestNew_BuildsAddressLookups(t *testing.T) {
	s := newSubmitter(t, newFakeChain(), newFakePrice(), newFakeRepo(), time.Minute)
	if _, ok := s.AggregatorBySymbol("WETH"); !ok {
		t.Fatal("case-insensitive WETH lookup failed")
	}
	if len(s.SymbolsManaged()) != 3 {
		t.Fatalf("expected 3 symbols, got %v", s.SymbolsManaged())
	}
}
