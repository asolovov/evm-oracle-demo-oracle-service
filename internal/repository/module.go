package repository

import (
	"context"
	"fmt"
	"sync"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
)

// Module wraps *PgxRepository in the module.Module lifecycle so it can be
// registered with the application's module.Manager alongside future
// template-style components. Matches the shape used by the indexer service.
//
// Init opens the pgxpool and pings; HealthCheck re-pings; Stop closes the
// pool. Start is a no-op (the repo is a passive dependency, not a long-
// running worker).
type Module struct {
	cfg *config.DatabaseConfig

	mu   sync.Mutex
	repo *PgxRepository
}

// NewModule constructs the module from config.
func NewModule(cfg *config.DatabaseConfig) *Module {
	return &Module{cfg: cfg}
}

// Name implements module.Module.
func (m *Module) Name() string { return "repository" }

// Init dials Postgres and pings.
func (m *Module) Init(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.repo != nil {
		return nil // idempotent
	}
	repo, err := NewPgxRepository(ctx, m.cfg)
	if err != nil {
		return fmt.Errorf("repository init: %w", err)
	}
	m.repo = repo
	return nil
}

// Start is a no-op — the repo has no background workers.
func (m *Module) Start(_ context.Context) error { return nil }

// Stop closes the pool.
func (m *Module) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.repo != nil {
		m.repo.Close()
		m.repo = nil
	}
	return nil
}

// HealthCheck pings the pool.
func (m *Module) HealthCheck(ctx context.Context) error {
	m.mu.Lock()
	repo := m.repo
	m.mu.Unlock()

	if repo == nil {
		return fmt.Errorf("repository not initialized")
	}
	return repo.Ping(ctx)
}

// Repo returns the underlying *PgxRepository. Returns nil if Init hasn't run.
func (m *Module) Repo() *PgxRepository {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.repo
}
