// Package heartbeat is the per-asset heartbeat scheduler.
//
// On each tick the scheduler reads the on-chain latestRoundData for the
// asset's aggregator (via chain client), fetches the off-chain aggregated
// price (via price-service), and dispatches a heartbeat submission when
// either:
//
//   - more than IntervalSec has elapsed since the last on-chain updatedAt, OR
//   - the off-chain price has moved by more than DeviationThreshold from the
//     last on-chain price.
//
// The default config schedule is layered behind the per-asset override
// persisted via OracleService.SetHeartbeat (read here on every tick so admin
// changes take effect on the next interval).
//
// Spec FR-05 + spec §4.2 — heartbeat is INTERNAL: no inter-service RPC.
package heartbeat

import (
	"context"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	pricev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/price/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

// ChainReader is the chain.Client surface we need.
type ChainReader interface {
	LatestRoundData(ctx context.Context, aggregator common.Address) (*big.Int, *big.Int, error)
}

// PriceClient is the price-service gRPC surface.
type PriceClient interface {
	GetPrice(ctx context.Context, assetSymbol string) (*pricev1.AggregatedPrice, error)
}

// Dispatcher is the submitter-facing seam; production wiring uses
// submitter.HandleHeartbeat.
type Dispatcher interface {
	HandleHeartbeat(ctx context.Context, symbol string) error
}

// ScheduleStore is the repository read surface for the persisted schedule.
type ScheduleStore interface {
	ListHeartbeats(ctx context.Context) ([]models.HeartbeatSchedule, error)
}

// AssetIndex exposes the configured aggregator address per symbol — the
// submitter provides this view.
type AssetIndex interface {
	AggregatorBySymbol(symbol string) (common.Address, bool)
	SymbolsManaged() []string
}

// Scheduler is the wiring point.
type Scheduler struct {
	cfg       *config.HeartbeatConfig
	conv      *config.ConversionConfig
	price     PriceClient
	chain     ChainReader
	store     ScheduleStore
	assets    AssetIndex
	dispatch  Dispatcher
	log       *logrus.Entry

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}

	onSkipped func(symbol string)
}

// Option tunes the Scheduler at construction time.
type Option func(*Scheduler)

// WithLogger sets the structured-log handle.
func WithLogger(log *logrus.Entry) Option {
	return func(s *Scheduler) { s.log = log }
}

// WithSkippedCounter wires the oracle_heartbeat_skipped_total metric.
func WithSkippedCounter(cb func(symbol string)) Option {
	return func(s *Scheduler) { s.onSkipped = cb }
}

// New constructs a Scheduler. application.go wires Start/Stop.
func New(
	cfg *config.HeartbeatConfig,
	conv *config.ConversionConfig,
	price PriceClient,
	chain ChainReader,
	store ScheduleStore,
	assets AssetIndex,
	dispatch Dispatcher,
	opts ...Option,
) *Scheduler {
	s := &Scheduler{
		cfg: cfg, conv: conv,
		price: price, chain: chain,
		store: store, assets: assets, dispatch: dispatch,
		stop: make(chan struct{}),
		done: make(chan struct{}),
		log:  logrus.NewEntry(logrus.StandardLogger()).WithField("component", "heartbeat"),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start launches one goroutine per managed asset. Returns immediately.
func (s *Scheduler) Start(ctx context.Context) {
	if !s.cfg.Enabled {
		s.log.Info("heartbeat disabled in config")
		close(s.done)
		return
	}

	go s.run(ctx)
}

// Stop signals the loop to drain. Idempotent.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stop) })
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) run(parent context.Context) {
	defer close(s.done)

	// Single ticker — at each tick we walk every managed asset and decide
	// whether it warrants a submission. The cadence is the *minimum* check
	// interval; the per-asset interval is what gates an actual fire.
	checkEvery := time.Duration(s.cfg.IntervalSec) * time.Second
	if checkEvery <= 0 {
		checkEvery = time.Hour
	}
	// Tighter check cadence than the heartbeat interval so deviation-based
	// fires don't lag a full interval. 30s is a good demo default.
	if checkEvery > 30*time.Second {
		checkEvery = 30 * time.Second
	}

	ticker := time.NewTicker(checkEvery)
	defer ticker.Stop()

	s.tick(parent) // first tick fires immediately

	for {
		select {
		case <-s.stop:
			return
		case <-parent.Done():
			return
		case <-ticker.C:
			s.tick(parent)
		}
	}
}

// tick walks every managed symbol and dispatches the ones whose policy says
// "fire now".
func (s *Scheduler) tick(ctx context.Context) {
	// Reload the per-asset schedule from the repo so SetHeartbeat changes
	// take effect on the next tick.
	persisted, err := s.store.ListHeartbeats(ctx)
	if err != nil {
		s.log.WithError(err).Warn("list heartbeats (using config defaults)")
	}
	override := make(map[string]models.HeartbeatSchedule, len(persisted))
	for _, h := range persisted {
		override[strings.ToLower(h.AssetID)] = h
	}

	for _, sym := range s.assets.SymbolsManaged() {
		policy := s.policyFor(sym, override)
		if policy.Interval == 0 && policy.DeviationBps == 0 {
			continue // disabled for this asset
		}
		if err := s.maybeFire(ctx, sym, policy); err != nil {
			s.log.WithError(err).WithField("symbol", sym).Warn("heartbeat evaluation failed")
		}
	}
}

func (s *Scheduler) policyFor(sym string, override map[string]models.HeartbeatSchedule) models.HeartbeatSchedule {
	if h, ok := override[strings.ToLower(sym)]; ok {
		return h
	}
	// Convert config defaults (float threshold) into bps so the rest of the
	// pipeline runs on one numeric type.
	return models.HeartbeatSchedule{
		AssetID:      sym,
		Interval:     time.Duration(s.cfg.IntervalSec) * time.Second,
		DeviationBps: uint32(math.Round(s.cfg.DeviationThreshold * 10000)),
	}
}

func (s *Scheduler) maybeFire(ctx context.Context, sym string, policy models.HeartbeatSchedule) error {
	aggregator, ok := s.assets.AggregatorBySymbol(sym)
	if !ok {
		return nil // configured but no aggregator — silently skip
	}

	onchainPrice, onchainUpdatedAt, err := s.chain.LatestRoundData(ctx, aggregator)
	if err != nil {
		return err
	}

	priceMsg, err := s.price.GetPrice(ctx, sym)
	if err != nil {
		return err
	}

	// Time-based fire: now - updatedAt > interval.
	if policy.Interval > 0 && onchainUpdatedAt != nil {
		since := time.Since(time.Unix(onchainUpdatedAt.Int64(), 0))
		if since >= policy.Interval {
			return s.fire(ctx, sym, "interval")
		}
	}

	// Deviation-based fire: |new - last|/last > threshold.
	if policy.DeviationBps > 0 && onchainPrice != nil && onchainPrice.Sign() != 0 {
		last, err := models.Int256ToFloat(onchainPrice, s.conv.OnChainDecimals)
		if err != nil {
			return err
		}
		if last != 0 {
			dev := math.Abs((priceMsg.GetMedianPrice() - last) / last)
			if dev > policy.DeviationRatio() {
				return s.fire(ctx, sym, "deviation")
			}
		}
	}

	if s.onSkipped != nil {
		s.onSkipped(sym)
	}
	return nil
}

func (s *Scheduler) fire(ctx context.Context, sym, reason string) error {
	s.log.WithFields(logrus.Fields{"symbol": sym, "reason": reason}).Info("heartbeat firing")
	return s.dispatch.HandleHeartbeat(ctx, sym)
}
