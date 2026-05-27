// Package internal contains the core application wiring for the oracle-service.
package internal

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/module"
	"github.com/asolovov/evm-oracle-demo-oracle-service/pkg/logger"
	"github.com/asolovov/evm-oracle-demo-oracle-service/pkg/version"
)

// App is the oracle-service application instance.
//
// Per architecture rule 2, this file is the sole point that constructs
// dependencies and wires them together. Wiring order:
//
//	DB pool -> submission repo -> signer -> chain client ->
//	    price/indexer gRPC clients -> submitter ->
//	    stream consumer -> gRPC server (admin+read) ->
//	    heartbeat scheduler -> healthz/metrics.
//
// Concrete wiring lands incrementally as each package is introduced.
type App struct {
	config  *config.Scheme
	version *version.Version
	modules *module.Manager
}

// NewApplication creates an App with a zero config (filled by cmd/root via viper).
func NewApplication() (*App, error) {
	ver, err := version.NewVersion()
	if err != nil {
		return nil, fmt.Errorf("init app version: %w", err)
	}

	return &App{
		config:  &config.Scheme{},
		version: ver,
		modules: module.NewManager(),
	}, nil
}

// Init validates configuration and wires modules.
func (app *App) Init() error {
	if err := app.config.Validate(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	logger.Log().Info("application initialised (no modules wired yet)")
	return nil
}

// Serve starts modules and blocks until a shutdown signal is received.
func (app *App) Serve() error {
	ctx := context.Background()

	if err := app.modules.StartAll(ctx); err != nil {
		return fmt.Errorf("start modules: %w", err)
	}

	logger.Log().Info("application running; press Ctrl+C to stop")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit
	logger.Log().Info("shutdown signal received")
	return nil
}

// Stop drains modules in reverse registration order.
func (app *App) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return app.modules.StopAll(ctx)
}

// Config exposes the loaded configuration.
func (app *App) Config() *config.Scheme { return app.config }

// Version returns the formatted version string.
func (app *App) Version() string { return app.version.String() }

// Modules returns the module manager (used by healthz).
func (app *App) Modules() *module.Manager { return app.modules }
