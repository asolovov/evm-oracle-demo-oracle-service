// Package internal contains the core application wiring for the oracle-service.
package internal

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/chain"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/grpcclient"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/grpcsrv"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/healthz"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/heartbeat"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/metrics"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/module"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/repository"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/signer"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/streamconsumer"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/submitter"
	"github.com/asolovov/evm-oracle-demo-oracle-service/pkg/logger"
	"github.com/asolovov/evm-oracle-demo-oracle-service/pkg/version"
)

// App is the oracle-service application instance.
//
// Per architecture rule 2, this file is the sole construction site for
// dependencies. Construction order tracks the dependency graph documented
// in spec §3.2 / task 06 step 7:
//
//	DB pool -> repository
//	         -> reporter signer (loaded from disk)
//	         -> chain client (dials RPC + verifies chain id)
//	         -> price-service gRPC client
//	         -> indexer-service gRPC client
//	         -> submitter (event/heartbeat handler)
//	         -> stream consumer (only after submitter so events have a destination)
//	         -> gRPC server (admin + read only)
//	         -> heartbeat scheduler
//	         -> healthz + metrics listener
type App struct {
	cfg     *config.Scheme
	version *version.Version
	modules *module.Manager
	log     *logrus.Entry

	// Constructed components — held so Stop() can drain in reverse order.
	repoModule      *repository.Module
	repo            *repository.PgxRepository
	priceClient     *grpcclient.PriceClient
	indexerClient   *grpcclient.IndexerClient
	chainClient     *chain.Client
	signer          *signer.Signer
	submitter       *submitter.Submitter
	streamConsumer  *streamconsumer.Consumer
	grpcServer      *grpcsrv.Server
	heartbeatSched  *heartbeat.Scheduler
	healthz         *healthz.Server
	metrics         *metrics.Metrics

	// Coordination.
	grpcDone    chan error
	healthzDone chan error
	stoppedOnce sync.Once
}

// NewApplication creates an App with a zero config (filled by cmd/root via viper).
func NewApplication() (*App, error) {
	ver, err := version.NewVersion()
	if err != nil {
		return nil, fmt.Errorf("init app version: %w", err)
	}

	return &App{
		cfg:         &config.Scheme{},
		version:     ver,
		modules:     module.NewManager(),
		log:         logger.Log().WithField("component", "application"),
		grpcDone:    make(chan error, 1),
		healthzDone: make(chan error, 1),
	}, nil
}

// Init validates configuration and constructs every component.
func (app *App) Init() error {
	if err := app.cfg.Validate(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Metrics registry.
	app.metrics = metrics.New()

	// 2. Repository — registered with the module manager so health-check
	// aggregation and reverse-order Stop fall out for free (matches the
	// indexer service's shape).
	repoModule := repository.NewModule(&app.cfg.Database)
	if err := repoModule.Init(ctx); err != nil {
		return fmt.Errorf("repository: %w", err)
	}
	app.modules.Register(repoModule)
	app.repoModule = repoModule
	repo := repoModule.Repo()
	app.repo = repo

	// 3. Signer (reads keys from disk + enforces perms).
	sgn, err := signer.LoadFromConfig(&app.cfg.Signer, app.cfg.Chain.ChainID)
	if err != nil {
		return fmt.Errorf("signer: %w", err)
	}
	app.signer = sgn

	// 4. Chain client (dials RPC; verifies chain id).
	cc, err := chain.Dial(ctx, app.cfg.Chain.RPCURL, app.cfg.Chain.ChainID, app.cfg.Submission.GasMultiplier)
	if err != nil {
		return fmt.Errorf("chain client: %w", err)
	}
	app.chainClient = cc

	// 5. Outbound gRPC clients.
	pc, err := grpcclient.DialPrice(ctx, &app.cfg.Price)
	if err != nil {
		return fmt.Errorf("price client: %w", err)
	}
	app.priceClient = pc

	ic, err := grpcclient.DialIndexer(ctx, &app.cfg.Indexer)
	if err != nil {
		return fmt.Errorf("indexer client: %w", err)
	}
	app.indexerClient = ic

	// 6. On-chain assetId lookup. The deployed PriceAggregator stores the
	// canonical bytes32 we must use in the EIP-712 digest; re-deriving via
	// keccak256(symbol) off-chain is fragile (the original deploy used
	// uppercase symbols, the price-service wire uses lowercase, and the two
	// don't match). Fail-fast if any configured aggregator can't be reached
	// or if the on-chain assetId doesn't correspond to ANY case-variant of
	// the operator-supplied symbol — that almost certainly means the wrong
	// aggregator was wired to the wrong symbol.
	assetIDs, err := resolveAssetIDs(ctx, cc, app.cfg.Chain.AggregatorAddresses, app.log)
	if err != nil {
		return fmt.Errorf("resolve on-chain assetIds: %w", err)
	}

	// 7. Submitter (depends on signer + chain + price + repo + assetIDs).
	sub := submitter.New(
		cc, pc, sgn, repo,
		&app.cfg.Submission, &app.cfg.Conversion,
		app.cfg.Chain.AggregatorAddresses, assetIDs,
		submitter.WithLogger(app.log.WithField("component", "submitter")),
		submitter.WithMetricsHooks(
			func(asset, st string) { app.metrics.SubmissionsTotal.WithLabelValues(asset, st).Inc() },
			func(gas uint64) { app.metrics.GasUsed.Observe(float64(gas)) },
		),
	)
	app.submitter = sub

	// 7. Stream consumer (started AFTER submitter wiring).
	sc := streamconsumer.New(
		ic.StreamClient(), repo, sub, &app.cfg.Stream,
		streamconsumer.WithLogger(app.log.WithField("component", "streamconsumer")),
		streamconsumer.WithMetricsHooks(
			func(kind string) { app.metrics.StreamEventsReceived.WithLabelValues(kind).Inc() },
			func() { app.metrics.StreamReconnectTotal.Inc() },
			func(lag float64) { app.metrics.StreamLagSeconds.Set(lag) },
		),
	)
	app.streamConsumer = sc

	// 8. Heartbeat scheduler.
	hb := heartbeat.New(
		&app.cfg.Heartbeat, &app.cfg.Conversion,
		pc, cc, repo, sub, sub,
		heartbeat.WithLogger(app.log.WithField("component", "heartbeat")),
		heartbeat.WithSkippedCounter(func(sym string) {
			app.metrics.HeartbeatSkippedTotal.WithLabelValues(sym).Inc()
		}),
	)
	app.heartbeatSched = hb

	// 9. gRPC server (admin + read only).
	gs := grpcsrv.New(&app.cfg.GRPC, repo, repo, app.log.WithField("component", "grpcsrv"))
	app.grpcServer = gs

	// 10. Healthz / metrics listener.
	app.healthz = healthz.New(
		&app.cfg.Healthz,
		app.metrics.Registry,
		app.readiness,
		app.log.WithField("component", "healthz"),
	)

	// Reporter address gauge (best-effort balance read at startup).
	weiPerEth := new(big.Float).SetPrec(256).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	for _, addr := range sgn.Reporters() {
		bal, err := cc.BalanceAt(ctx, addr)
		if err == nil && bal != nil {
			// Express in ETH for human-readable scraping. 1e18 wei == 1 ETH.
			eth := new(big.Float).SetInt(bal)
			eth.Quo(eth, weiPerEth)
			fl, _ := eth.Float64()
			app.metrics.ReporterBalance.WithLabelValues(addr.Hex()).Set(fl)
		}
	}

	app.log.Info("application initialized")
	return nil
}

// Serve starts every long-running component and blocks until a shutdown
// signal is received.
func (app *App) Serve() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start gRPC server.
	go func() { app.grpcDone <- app.grpcServer.Serve() }()

	// Start healthz listener.
	go func() { app.healthzDone <- app.healthz.Serve() }()

	// Start stream consumer + heartbeat.
	app.streamConsumer.Start(ctx)
	app.heartbeatSched.Start(ctx)

	app.log.Info("application running; press Ctrl+C to stop")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-quit:
		app.log.Info("shutdown signal received")
	case err := <-app.grpcDone:
		if err != nil {
			app.log.WithError(err).Error("grpc server exited")
		}
	case err := <-app.healthzDone:
		if err != nil {
			app.log.WithError(err).Error("healthz server exited")
		}
	}
	return nil
}

// Stop drains components in reverse construction order.
func (app *App) Stop() error {
	var stopErr error
	app.stoppedOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		stopErr = errors.Join(
			app.stopHeartbeat(ctx),
			app.stopStream(ctx),
			app.stopGRPC(ctx),
			app.stopHealthz(ctx),
			app.stopSubmitter(),
			app.closeIndexer(),
			app.closePrice(),
			app.closeChain(),
			app.closeRepo(),
		)
	})
	return stopErr
}

func (app *App) stopHeartbeat(ctx context.Context) error {
	if app.heartbeatSched == nil {
		return nil
	}
	return app.heartbeatSched.Stop(ctx)
}

func (app *App) stopStream(ctx context.Context) error {
	if app.streamConsumer == nil {
		return nil
	}
	return app.streamConsumer.Stop(ctx)
}

func (app *App) stopGRPC(ctx context.Context) error {
	if app.grpcServer == nil {
		return nil
	}
	return app.grpcServer.Stop(ctx)
}

func (app *App) stopHealthz(ctx context.Context) error {
	if app.healthz == nil {
		return nil
	}
	return app.healthz.Stop(ctx)
}

func (app *App) stopSubmitter() error {
	if app.submitter == nil {
		return nil
	}
	app.submitter.Wait()
	return nil
}

func (app *App) closeIndexer() error {
	if app.indexerClient == nil {
		return nil
	}
	return app.indexerClient.Close()
}

func (app *App) closePrice() error {
	if app.priceClient == nil {
		return nil
	}
	return app.priceClient.Close()
}

func (app *App) closeChain() error {
	if app.chainClient != nil {
		app.chainClient.Close()
	}
	return nil
}

func (app *App) closeRepo() error {
	if app.repoModule == nil {
		// Fallback for fail-fast paths where the module wrapper never landed.
		if app.repo != nil {
			app.repo.Close()
		}
		return nil
	}
	return app.repoModule.Stop(context.Background())
}

// Config exposes the loaded configuration.
func (app *App) Config() *config.Scheme { return app.cfg }

// Version returns the formatted version string.
func (app *App) Version() string { return app.version.String() }

// Modules returns the module manager. The repository is the one registered
// module; other components are wired directly in application.go since they
// lifecycle as Init+goroutine rather than the Init/Start/Stop module shape.
func (app *App) Modules() *module.Manager { return app.modules }

// resolveAssetIDs reads the on-chain `assetId()` from every configured
// PriceAggregator and verifies it matches `keccak256(symbol)` for either
// case-variant of the operator-supplied symbol (uppercase or lowercase).
//
// Why this exists: the EIP-712 digest the contract verifies uses the on-chain
// assetId as its `bytes32 assetId` field. Re-deriving the bytes32 off-chain
// from a symbol string is fragile — the original deploy used uppercase
// (keccak256("WETH")) while the price-service wire format uses lowercase
// ("weth"). A live test caught the case mismatch as
// `fulfillPrice: execution reverted` (InsufficientSignatures, since the
// digest the contract recomputes from its own assetId never matches what
// the off-chain signer produced).
//
// Returning the on-chain values + verifying the symbol mapping catches typos
// (wrong aggregator wired to a symbol) at startup instead of at first event.
func resolveAssetIDs(
	ctx context.Context,
	cc *chain.Client,
	aggregatorAddresses map[string]string,
	log *logrus.Entry,
) (map[common.Address]common.Hash, error) {
	out := make(map[common.Address]common.Hash, len(aggregatorAddresses))

	for sym, addrHex := range aggregatorAddresses {
		addr := common.HexToAddress(addrHex)
		onchain, err := cc.AssetID(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("aggregator %s (%s): assetId(): %w", sym, addr.Hex(), err)
		}

		upper := crypto.Keccak256Hash([]byte(strings.ToUpper(sym)))
		lower := crypto.Keccak256Hash([]byte(strings.ToLower(sym)))

		var matched string
		switch onchain {
		case upper:
			matched = "uppercase"
		case lower:
			matched = "lowercase"
		default:
			return nil, fmt.Errorf(
				"aggregator %s (%s): on-chain assetId %s matches neither keccak256(%q)=%s nor keccak256(%q)=%s — wrong aggregator for symbol?",
				sym, addr.Hex(), onchain.Hex(),
				strings.ToUpper(sym), upper.Hex(),
				strings.ToLower(sym), lower.Hex(),
			)
		}
		out[addr] = onchain
		log.WithFields(logrus.Fields{
			"symbol":     sym,
			"aggregator": addr.Hex(),
			"asset_id":   onchain.Hex(),
			"derived":    matched,
		}).Info("resolved on-chain assetId")
	}
	return out, nil
}

// readiness is the closure healthz exposes on /readyz. Returns nil iff every
// load-bearing dependency reports healthy. Walks every registered module via
// the manager and aggregates errors so a future module addition lights up
// /readyz without touching this site.
func (app *App) readiness(ctx context.Context) error {
	results := app.modules.HealthCheckAll(ctx)
	var errs []error
	for name, err := range results {
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
