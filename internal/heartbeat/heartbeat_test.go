package heartbeat

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	pricev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/price/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

type fakeChain struct {
	mu        sync.Mutex
	price     *big.Int
	updatedAt *big.Int
	err       error
}

func (f *fakeChain) LatestRoundData(_ context.Context, _ common.Address) (*big.Int, *big.Int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.price, f.updatedAt, f.err
}

type fakePrice struct {
	median float64
	err    error
}

func (f *fakePrice) GetPrice(_ context.Context, _ string) (*pricev1.AggregatedPrice, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &pricev1.AggregatedPrice{
		MedianPrice:  f.median,
		AggregatedAt: timestamppb.Now(),
	}, nil
}

type fakeStore struct {
	hs []models.HeartbeatSchedule
}

func (f *fakeStore) ListHeartbeats(_ context.Context) ([]models.HeartbeatSchedule, error) {
	return f.hs, nil
}

type fakeAssets struct {
	syms []string
	addr common.Address
}

func (f *fakeAssets) AggregatorBySymbol(_ string) (common.Address, bool) { return f.addr, true }
func (f *fakeAssets) SymbolsManaged() []string                            { return f.syms }

type recDispatcher struct {
	mu      sync.Mutex
	called  int32
	reasons []string
}

func (r *recDispatcher) HandleHeartbeat(_ context.Context, _ string) error {
	atomic.AddInt32(&r.called, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reasons = append(r.reasons, "fired")
	return nil
}

func newScheduler(t *testing.T, fc *fakeChain, fp *fakePrice, fs *fakeStore, fa *fakeAssets, fd *recDispatcher, cfg *config.HeartbeatConfig) *Scheduler {
	t.Helper()
	return New(cfg,
		&config.ConversionConfig{OnChainDecimals: 8},
		fp, fc, fs, fa, fd,
	)
}

func TestTick_FiresOnInterval(t *testing.T) {
	// On-chain price updated 2 hours ago; interval is 1 hour.
	fc := &fakeChain{
		price:     big.NewInt(345020000000),
		updatedAt: big.NewInt(time.Now().Add(-2 * time.Hour).Unix()),
	}
	fp := &fakePrice{median: 3450.20}
	fa := &fakeAssets{syms: []string{"weth"}, addr: common.HexToAddress("0xaa")}
	fd := &recDispatcher{}
	sch := newScheduler(t, fc, fp, &fakeStore{}, fa, fd, &config.HeartbeatConfig{
		Enabled: true, IntervalSec: 3600, DeviationThreshold: 0.5,
	})

	sch.tick(context.Background())
	if atomic.LoadInt32(&fd.called) != 1 {
		t.Fatalf("expected 1 fire, got %d", fd.called)
	}
}

func TestTick_FiresOnDeviation(t *testing.T) {
	// On-chain $3450, off-chain $3500 -> 1.4% > 1% threshold.
	fc := &fakeChain{
		price:     big.NewInt(345000000000),
		updatedAt: big.NewInt(time.Now().Unix()), // fresh — interval can't fire
	}
	fp := &fakePrice{median: 3500.00}
	fa := &fakeAssets{syms: []string{"weth"}, addr: common.HexToAddress("0xaa")}
	fd := &recDispatcher{}
	sch := newScheduler(t, fc, fp, &fakeStore{}, fa, fd, &config.HeartbeatConfig{
		Enabled: true, IntervalSec: 3600, DeviationThreshold: 0.01, // 100 bps
	})

	sch.tick(context.Background())
	if atomic.LoadInt32(&fd.called) != 1 {
		t.Fatalf("expected 1 fire on deviation, got %d", fd.called)
	}
}

func TestTick_SkipsWhenWithinTolerance(t *testing.T) {
	// On-chain $3450, off-chain $3451 -> 0.029% < 1% threshold; fresh.
	fc := &fakeChain{
		price:     big.NewInt(345000000000),
		updatedAt: big.NewInt(time.Now().Unix()),
	}
	fp := &fakePrice{median: 3451.00}
	fa := &fakeAssets{syms: []string{"weth"}, addr: common.HexToAddress("0xaa")}
	fd := &recDispatcher{}

	skipped := int32(0)
	sch := New(&config.HeartbeatConfig{Enabled: true, IntervalSec: 3600, DeviationThreshold: 0.01},
		&config.ConversionConfig{OnChainDecimals: 8},
		fp, fc, &fakeStore{}, fa, fd,
		WithSkippedCounter(func(_ string) { atomic.AddInt32(&skipped, 1) }),
	)

	sch.tick(context.Background())
	if atomic.LoadInt32(&fd.called) != 0 {
		t.Fatalf("expected 0 fires within tolerance, got %d", fd.called)
	}
	if atomic.LoadInt32(&skipped) != 1 {
		t.Fatalf("expected skipped counter to fire once, got %d", skipped)
	}
}

func TestTick_OverrideFromPersistedSchedule(t *testing.T) {
	// Config default would skip (within tolerance + fresh) but the override
	// disables both paths -> still skip, but verify the policy is taken.
	fc := &fakeChain{
		price:     big.NewInt(345000000000),
		updatedAt: big.NewInt(time.Now().Unix()),
	}
	fp := &fakePrice{median: 3500.00}
	fa := &fakeAssets{syms: []string{"weth"}, addr: common.HexToAddress("0xaa")}
	fd := &recDispatcher{}
	fs := &fakeStore{hs: []models.HeartbeatSchedule{
		{AssetID: "weth", Interval: 0, DeviationBps: 0}, // disabled
	}}
	sch := newScheduler(t, fc, fp, fs, fa, fd, &config.HeartbeatConfig{
		Enabled: true, IntervalSec: 3600, DeviationThreshold: 0.01, // would otherwise fire
	})

	sch.tick(context.Background())
	if atomic.LoadInt32(&fd.called) != 0 {
		t.Fatalf("expected 0 fires when override disables the asset, got %d", fd.called)
	}
}

func TestStart_DisabledShortCircuits(t *testing.T) {
	fc := &fakeChain{price: big.NewInt(1), updatedAt: big.NewInt(0)}
	fp := &fakePrice{}
	fa := &fakeAssets{syms: []string{"weth"}}
	fd := &recDispatcher{}
	sch := newScheduler(t, fc, fp, &fakeStore{}, fa, fd, &config.HeartbeatConfig{Enabled: false})
	sch.Start(context.Background())
	if err := sch.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
