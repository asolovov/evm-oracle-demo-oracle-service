// Package submitter is the oracle-service's price-submission business logic.
//
// Async pipeline (task 06.1) — designed so one un-priceable asset can never
// block any other:
//
//	stream consumer ──HandleEvent──▶ durably enqueue (queued row) + push to
//	    requests channel, return fast (cursor advances immediately)
//	requests channel ──▶ worker pool (N goroutines): fetch price + convert +
//	    clamp timestamp + sign — INDEPENDENT per request; a slow/failing asset
//	    occupies at most one slot. Transient failure → requeue with backoff
//	    while within TTL; past TTL → `expired` (terminal, pre-broadcast only).
//	signed payloads ──▶ sender (ONE goroutine): owns the broadcaster nonce
//	    counter, the only serialized stage. Broadcasts fulfillPrice; permanent
//	    revert → `failed`; transient → requeue (nonce NOT advanced); success →
//	    `pending` + spawn confirmation watcher.
//
// Heartbeat submissions (reqId == 0) BYPASS the queue/TTL/recovery — they are
// scheduler-driven and internally rate-limited — but still serialize through
// the sender for nonce safety.
//
// Single-instance design: dispatch is an in-memory channel (each item reaches
// exactly one worker), with the DB row purely for durability + observability +
// crash recovery. A multi-instance deployment would instead need a
// `FOR UPDATE SKIP LOCKED` claim; out of scope for this single-VPS service.
//
// Architecture rule 4 / 5: internal business logic, not a template module.
package submitter

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	chainpkg "github.com/asolovov/evm-oracle-demo-oracle-service/internal/chain"
	indexerv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/indexer/v1"
	pricev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/price/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

// ChainClient is the chain-package surface the submitter depends on.
type ChainClient interface {
	SuggestGas(ctx context.Context, attempt int) (chainpkg.GasStrategy, error)
	NonceAt(ctx context.Context, addr common.Address) (uint64, error)
	SubmitFulfillment(ctx context.Context, auth *bind.TransactOpts,
		aggregator common.Address, reqID, price, timestamp *big.Int,
		signatures [][]byte, gas chainpkg.GasStrategy) (common.Hash, error)
	ReplaceFulfillment(ctx context.Context, auth *bind.TransactOpts,
		aggregator common.Address, reqID, price, timestamp *big.Int,
		signatures [][]byte, gas chainpkg.GasStrategy) (common.Hash, error)
	TxReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	LatestStartedAt(ctx context.Context, aggregator common.Address) (*big.Int, error)
	// BalanceAt backs the pre-flight balance gate (task 06.2): the sender skips
	// a wallet that demonstrably can't pay before burning an estimateGas call.
	BalanceAt(ctx context.Context, addr common.Address) (*big.Int, error)
}

// PriceClient is the price-service gRPC surface.
type PriceClient interface {
	GetPrice(ctx context.Context, assetSymbol string) (*pricev1.AggregatedPrice, error)
}

// SignerClient is the signer.Signer surface.
type SignerClient interface {
	BuildDigest(reqID *big.Int, assetID common.Hash, price, timestamp *big.Int, aggregator common.Address) ([]byte, error)
	Sign(digest []byte) ([][]byte, error)
	// Broadcasters is the pool of EOAs the sender rotates across + fails over
	// between (task 06.3). NewBroadcasterFor builds a transactor for a chosen
	// pool wallet.
	Broadcasters() []common.Address
	NewBroadcasterFor(addr common.Address) (*bind.TransactOpts, error)
}

// Repo is the repository surface the submitter needs.
type Repo interface {
	EnqueueRequest(ctx context.Context, s *models.Submission) (int64, error)
	UpdateSubmission(ctx context.Context, s *models.Submission) error
	InsertSubmission(ctx context.Context, s *models.Submission) (int64, error)
	MarkExpired(ctx context.Context, id int64, lastErr string) error
	LoadResumable(ctx context.Context) ([]*models.Submission, error)
	ExpireOverdue(ctx context.Context) (int, error)
	InsertPendingTx(ctx context.Context, submissionID int64, txHash string, nonce uint64, broadcaster string, gasStrategyJSON []byte) error
	DeletePendingTx(ctx context.Context, txHash string) error
}

// workItem is a request flowing through the worker pool. submissionID is the
// durable `oracle_submissions` row id (0 for heartbeats, which have no row).
type workItem struct {
	submissionID int64
	symbol       string
	aggregator   common.Address
	reqID        *big.Int
	expiresAt    time.Time // zero => no TTL (heartbeat)
	heartbeat    bool
	attempts     int
}

// signedTx is a fully-priced, signed payload ready for the sender to broadcast.
type signedTx struct {
	item  *workItem
	price *big.Int
	ts    *big.Int
	sigs  [][]byte
}

// Submitter is the wiring point. One per service.
type Submitter struct {
	chain  ChainClient
	price  PriceClient
	signer SignerClient
	repo   Repo
	cfg    *config.SubmissionConfig
	conv   *config.ConversionConfig

	bySymbol            map[string]common.Address
	byAddress           map[common.Address]string
	assetIDByAggregator map[common.Address]common.Hash

	log *logrus.Entry

	workers          int
	ttl              time.Duration
	pollEvery        time.Duration
	retryBackoff     time.Duration // base unit; backoff = retryBackoff*attempts, capped
	gasLimitEstimate uint64        // assumed fulfillPrice gas limit for the balance gate

	requests chan *workItem
	sendCh   chan *signedTx

	// Broadcaster pool (task 06.3). All owned exclusively by the single sender
	// goroutine — no lock. nonces tracks a per-wallet counter; nextBroadcaster
	// is the round-robin cursor; breaker trips only when EVERY wallet drains.
	broadcasters    []common.Address
	nonces          map[common.Address]uint64
	nextBroadcaster int
	breaker         *breaker

	wg       sync.WaitGroup
	stop     chan struct{}
	stopOnce sync.Once

	// Metrics hooks (nil-tolerant).
	onSubmission   func(asset, status string)
	onGasUsed      func(uint64)
	onQueued       func()
	onExpired      func(asset string)
	onProcessing   func(seconds float64)
	onFundsBlocked func()
	onBreaker      func(open bool)
}

// Option tunes the Submitter at construction time.
type Option func(*Submitter)

// WithLogger sets the structured-log handle.
func WithLogger(log *logrus.Entry) Option {
	return func(s *Submitter) { s.log = log }
}

// WithMetricsHooks wires the submission + gas Prometheus counters.
func WithMetricsHooks(onSubmission func(asset, status string), onGasUsed func(uint64)) Option {
	return func(s *Submitter) {
		s.onSubmission = onSubmission
		s.onGasUsed = onGasUsed
	}
}

// WithQueueMetrics wires the async-pipeline counters (task 06.1). Nil hooks ignored.
func WithQueueMetrics(onQueued func(), onExpired func(asset string), onProcessing func(float64)) Option {
	return func(s *Submitter) {
		s.onQueued = onQueued
		s.onExpired = onExpired
		s.onProcessing = onProcessing
	}
}

// WithFundsMetrics wires the funds-awareness/circuit-breaker metrics (task 06.2):
// onFundsBlocked increments when a send exhausts every wallet, onBreaker sets
// the open/closed gauge. Nil hooks ignored.
func WithFundsMetrics(onFundsBlocked func(), onBreaker func(open bool)) Option {
	return func(s *Submitter) {
		s.onFundsBlocked = onFundsBlocked
		s.onBreaker = onBreaker
	}
}

// WithPollInterval overrides the receipt-poll cadence. Used by tests.
func WithPollInterval(d time.Duration) Option {
	return func(s *Submitter) { s.pollEvery = d }
}

// WithRetryBackoff overrides the base retry backoff (default 2s). Used by tests
// to keep TTL/retry assertions fast.
func WithRetryBackoff(d time.Duration) Option {
	return func(s *Submitter) { s.retryBackoff = d }
}

// WithBreakerBackoff overrides the circuit-breaker probe backoff bounds. Used
// by tests to drive sub-second breaker recovery (config is seconds-granular).
func WithBreakerBackoff(minBackoff, maxBackoff time.Duration) Option {
	return func(s *Submitter) { s.breaker = newBreaker(minBackoff, maxBackoff) }
}

// New constructs a Submitter. Call Start to launch the worker pool + sender.
func New(c ChainClient, p PriceClient, sgn SignerClient, repo Repo,
	cfg *config.SubmissionConfig, conv *config.ConversionConfig,
	aggregatorAddresses map[string]string,
	assetIDByAggregator map[common.Address]common.Hash,
	opts ...Option,
) *Submitter {
	bySym := make(map[string]common.Address, len(aggregatorAddresses))
	byAddr := make(map[common.Address]string, len(aggregatorAddresses))
	for sym, addr := range aggregatorAddresses {
		s := strings.ToLower(sym)
		a := common.HexToAddress(addr)
		bySym[s] = a
		byAddr[a] = s
	}

	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}
	ttl := time.Duration(cfg.RequestTTLSec) * time.Second
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	gasLimitEstimate := cfg.GasLimitEstimate
	if gasLimitEstimate == 0 {
		gasLimitEstimate = 300_000
	}

	sub := &Submitter{
		chain: c, price: p, signer: sgn, repo: repo,
		cfg: cfg, conv: conv,
		bySymbol: bySym, byAddress: byAddr,
		assetIDByAggregator: assetIDByAggregator,
		workers:             workers,
		ttl:                 ttl,
		pollEvery:           5 * time.Second,
		retryBackoff:        2 * time.Second,
		gasLimitEstimate:    gasLimitEstimate,
		broadcasters:        sgn.Broadcasters(),
		nonces:              make(map[common.Address]uint64),
		breaker: newBreaker(
			time.Duration(cfg.BreakerBackoffMinSec)*time.Second,
			time.Duration(cfg.BreakerBackoffMaxSec)*time.Second,
		),
		requests: make(chan *workItem, workers*8),
		sendCh:   make(chan *signedTx, workers*2),
		stop:     make(chan struct{}),
		log:      logrus.NewEntry(logrus.StandardLogger()).WithField("component", "submitter"),
	}
	for _, opt := range opts {
		opt(sub)
	}
	return sub
}

// Start seeds a per-wallet nonce for every broadcaster, wires the breaker's
// transition hooks, launches the sender + worker pool, and runs startup
// recovery (re-enqueue durable non-terminal rows; expire the overdue).
func (s *Submitter) Start(ctx context.Context) error {
	if len(s.broadcasters) == 0 {
		return fmt.Errorf("no broadcaster wallets configured")
	}
	seeds := make(map[string]uint64, len(s.broadcasters))
	for _, addr := range s.broadcasters {
		n, err := s.chain.NonceAt(ctx, addr)
		if err != nil {
			return fmt.Errorf("seed nonce for broadcaster %s: %w", addr.Hex(), err)
		}
		s.nonces[addr] = n
		seeds[addr.Hex()] = n
	}

	// Breaker hooks. onOpen/onClose fire ONCE per funds episode (event + log);
	// onSuspendChange drives the gauge on every suspend/resume transition.
	s.breaker.onOpen = func() {
		if s.onFundsBlocked != nil {
			s.onFundsBlocked()
		}
		s.log.Error("ALL broadcaster wallets drained — broadcasting suspended; will probe for a refunded wallet on backoff")
	}
	s.breaker.onClose = func() {
		s.log.Info("broadcaster wallet funded again — broadcasting resumed")
	}
	s.breaker.onSuspendChange = func(suspended bool) {
		if s.onBreaker != nil {
			s.onBreaker(suspended)
		}
	}

	s.wg.Add(1)
	// The sender owns its lifecycle via s.stop, NOT the Start ctx — Background
	// is intentional so an in-flight broadcast/probe isn't hard-canceled on
	// shutdown; draining is signaled through s.stop instead.
	go s.runSender() //nolint:gosec // G118: deliberate — lifecycle is s.stop, see above

	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.runWorker()
	}

	s.recover(ctx)
	s.log.WithFields(logrus.Fields{
		"workers":      s.workers,
		"ttl":          s.ttl.String(),
		"broadcasters": len(s.broadcasters),
		"seed_nonces":  seeds, // snapshot — never the live s.nonces map (sender mutates it)
	}).Info("submitter pipeline started")
	return nil
}

// BroadcastSuspended reports whether the circuit breaker is open (every
// broadcaster wallet is drained). The heartbeat scheduler consults this to
// pause firing instead of enqueuing doomed work every tick (task 06.2).
func (s *Submitter) BroadcastSuspended() bool {
	return s.breaker != nil && s.breaker.isSuspended()
}

// Stop signals the pipeline to drain and waits (bounded by ctx).
func (s *Submitter) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stop) })
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait blocks until the pipeline fully drains. Retained for callers that only
// need the join (e.g. tests).
func (s *Submitter) Wait() { s.wg.Wait() }

// AggregatorBySymbol returns the configured aggregator for a lowercase symbol.
func (s *Submitter) AggregatorBySymbol(symbol string) (common.Address, bool) {
	a, ok := s.bySymbol[strings.ToLower(symbol)]
	return a, ok
}

// SymbolsManaged returns the lowercase asset symbols this service manages.
func (s *Submitter) SymbolsManaged() []string {
	out := make([]string, 0, len(s.bySymbol))
	for sym := range s.bySymbol {
		out = append(out, sym)
	}
	return out
}

// HandleEvent is the streamconsumer.Dispatcher seam. It durably enqueues the
// request and returns immediately so the consumer can advance its cursor —
// the actual price-fetch/sign/broadcast happens asynchronously. Only a DB
// failure (genuinely transient) returns an error and blocks the cursor.
func (s *Submitter) HandleEvent(ctx context.Context, ev *indexerv1.Event) error {
	pr := ev.GetPriceRequested()
	if pr == nil {
		return errors.New("event is not PriceRequested")
	}
	aggregator := common.HexToAddress(ev.GetMeta().GetContractAddress())

	symbol, ok := s.byAddress[aggregator]
	if !ok {
		// Asset we don't manage — skip + let the consumer advance.
		s.log.WithField("aggregator", aggregator.Hex()).Warn("skipping event from unknown aggregator")
		return nil
	}

	reqID, ok := models.ReqIDToBigInt(pr.GetReqId())
	if !ok {
		// Malformed req_id is a permanent property of this event — skip +
		// advance rather than wedge the stream on it forever.
		s.log.WithField("req_id", pr.GetReqId()).Warn("malformed req_id; skipping")
		return nil
	}

	sub := &models.Submission{
		ReqID:      pr.GetReqId(),
		AssetID:    symbol,
		Aggregator: aggregator,
		Status:     models.SubmissionStatusQueued,
		ExpiresAt:  time.Now().Add(s.ttl),
	}
	id, err := s.repo.EnqueueRequest(ctx, sub)
	if err != nil {
		// DB unreachable — genuinely transient; propagate so the consumer
		// blocks the cursor and redelivers (the only legitimate block case).
		return fmt.Errorf("enqueue request: %w", err)
	}
	if s.onQueued != nil {
		s.onQueued()
	}
	_ = reqID // parsed for validation; the worker re-parses from the row on dispatch
	s.enqueueItem(&workItem{
		submissionID: id,
		symbol:       symbol,
		aggregator:   aggregator,
		reqID:        reqID,
		expiresAt:    sub.ExpiresAt,
	})
	return nil
}

// HandleHeartbeat is the heartbeat-scheduler seam. Heartbeats bypass the
// durable queue + TTL (they are periodic and re-fire on the next tick) but
// still go through the worker → sender path for nonce serialization.
func (s *Submitter) HandleHeartbeat(_ context.Context, symbol string) error {
	aggregator, ok := s.AggregatorBySymbol(symbol)
	if !ok {
		return fmt.Errorf("unknown symbol %q", symbol)
	}
	s.enqueueItem(&workItem{
		symbol:     symbol,
		aggregator: aggregator,
		reqID:      big.NewInt(0),
		heartbeat:  true,
	})
	return nil
}

// enqueueItem pushes onto the worker channel, honoring shutdown. Mild
// backpressure on a full channel is acceptable (it rate-limits intake; it does
// NOT wedge on a single failing asset, since failures requeue via delayed
// timers rather than hot-looping the channel).
func (s *Submitter) enqueueItem(item *workItem) {
	select {
	case s.requests <- item:
	case <-s.stop:
	}
}

func (s *Submitter) markSubmissionMetric(asset string, st models.SubmissionStatus) {
	if s.onSubmission != nil {
		s.onSubmission(asset, st.String())
	}
}

func mustBigInt(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return big.NewInt(0)
	}
	return v
}
