//go:build integration

// Integration tests for PgxRepository. Boots a real Postgres via
// testcontainers, applies migrations/0001_init.up.sql, and exercises every
// repo method end-to-end. Run with:
//
//	make test-integration   # or: go test -tags=integration ./internal/repository/...
//
// Requires Docker to be running locally.
package repository

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

const testDBName = "evm_oracle_test"

func startPostgres(t *testing.T) (*PgxRepository, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "oracle_user",
			"POSTGRES_PASSWORD": "oracle_pass",
			"POSTGRES_DB":       testDBName,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("container port: %v", err)
	}

	portInt, err := strconv.Atoi(port.Port())
	if err != nil {
		t.Fatalf("container port to int: %v", err)
	}
	cfg := &config.DatabaseConfig{
		Host: host, Port: portInt,
		User: "oracle_user", Password: "oracle_pass",
		Name: testDBName, SSLMode: "disable",
		MaxOpenConns: 4, MaxIdleConns: 1,
	}
	repo, err := NewPgxRepository(ctx, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Apply migration directly.
	migPath := filepath.Join("..", "..", "db", "migrations", "0001_init.up.sql")
	sql, err := os.ReadFile(migPath)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	pool, err := pgxpool.New(ctx, "postgres://oracle_user:oracle_pass@"+host+":"+port.Port()+"/"+testDBName+"?sslmode=disable")
	if err != nil {
		t.Fatalf("connect for migration: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	cleanup := func() {
		repo.Close()
		_ = container.Terminate(ctx)
	}
	return repo, cleanup
}

func TestIntegration_InsertAndQuerySubmission(t *testing.T) {
	repo, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	sub := &models.Submission{
		ReqID:          "42",
		AssetID:        "weth",
		Aggregator:     common.HexToAddress("0x075be31662c2548c4e940d7e769c328a34dcb281"),
		TxHash:         common.HexToHash("0xaabbccdd"),
		SubmittedPrice: "345020000000",
		SubmittedAt:    time.Now().UTC(),
		Status:         models.SubmissionStatusPending,
	}
	id, err := repo.InsertSubmission(ctx, sub)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if id == 0 {
		t.Fatal("expected id > 0")
	}

	got, err := repo.GetSubmissionByReqID(ctx, "42")
	if err != nil {
		t.Fatalf("get by req_id: %v", err)
	}
	if got.AssetID != "weth" || got.SubmittedPrice != "345020000000" || got.Status != models.SubmissionStatusPending {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	gotTx, err := repo.GetSubmissionByTxHash(ctx, common.HexToHash("0xaabbccdd").Hex())
	if err != nil {
		t.Fatalf("get by tx_hash: %v", err)
	}
	if gotTx.ReqID != "42" {
		t.Fatalf("by tx_hash returned %v", gotTx)
	}

	weth := common.HexToAddress("0x075be31662c2548c4e940d7e769c328a34dcb281")
	wbtc := common.HexToAddress("0xf8ad3a2505eece7ad276db038c7c56930bd436e4")

	// Inserted aggregator (WETH) — should report exists.
	exists, err := repo.ExistsForAggregatorReqID(ctx, weth, "42")
	if err != nil || !exists {
		t.Fatalf("expected exists=true for (WETH, 42); %v %v", exists, err)
	}
	// Same req_id but DIFFERENT aggregator — must NOT collide.
	// This is the regression-guard for the per-aggregator scope bug
	// (live run revealed the original ExistsByReqID returned true here,
	// causing all sibling assets' events to be silently dropped).
	wrongAgg, err := repo.ExistsForAggregatorReqID(ctx, wbtc, "42")
	if err != nil || wrongAgg {
		t.Fatalf("expected per-aggregator scope: WBTC/42 must be exists=false; got %v %v", wrongAgg, err)
	}
	// Unknown req_id on the known aggregator — also false.
	missing, err := repo.ExistsForAggregatorReqID(ctx, weth, "9999")
	if err != nil || missing {
		t.Fatalf("expected exists=false for (WETH, 9999); %v %v", missing, err)
	}
}

func TestIntegration_ListSubmissionsPagination(t *testing.T) {
	repo, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		sub := &models.Submission{
			ReqID:          formatReqID(i + 1),
			AssetID:        "weth",
			Aggregator:     common.HexToAddress("0x01"),
			SubmittedPrice: "1",
			SubmittedAt:    time.Now().UTC().Add(time.Duration(i) * time.Second),
			Status:         models.SubmissionStatusConfirmed,
		}
		if _, err := repo.InsertSubmission(ctx, sub); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	page, total, err := repo.ListSubmissions(ctx, "weth", 2, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 5 {
		t.Fatalf("total %d, want 5", total)
	}
	if len(page) != 2 {
		t.Fatalf("page len %d, want 2", len(page))
	}

	// Other-asset filter -> 0.
	_, total, err = repo.ListSubmissions(ctx, "wbtc", 10, 0)
	if err != nil {
		t.Fatalf("list filtered: %v", err)
	}
	if total != 0 {
		t.Fatalf("expected 0 for wbtc, got %d", total)
	}
}

func TestIntegration_StreamCursorMonotonic(t *testing.T) {
	repo, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	if got, err := repo.GetStreamCursor(ctx); err != nil || got != 0 {
		t.Fatalf("initial cursor: %d / %v", got, err)
	}
	if err := repo.AdvanceStreamCursor(ctx, 100); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if got, _ := repo.GetStreamCursor(ctx); got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
	// Older block: must NOT rewind.
	if err := repo.AdvanceStreamCursor(ctx, 50); err != nil {
		t.Fatalf("advance backward should be a no-op: %v", err)
	}
	if got, _ := repo.GetStreamCursor(ctx); got != 100 {
		t.Fatalf("monotonic violated: %d", got)
	}
}

func TestIntegration_PendingTxRoundTrip(t *testing.T) {
	repo, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	sub := &models.Submission{
		ReqID: "1", AssetID: "weth",
		Aggregator: common.HexToAddress("0x01"), TxHash: common.HexToHash("0xab"),
		SubmittedPrice: "1", SubmittedAt: time.Now().UTC(),
		Status: models.SubmissionStatusPending,
	}
	id, err := repo.InsertSubmission(ctx, sub)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := repo.InsertPendingTx(ctx, id, "0xab", 5, "0x00000000000000000000000000000000000000Bb", []byte(`{"k":"v"}`)); err != nil {
		t.Fatalf("insert pending: %v", err)
	}

	all, err := repo.ListAllPendingTx(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 1 || all[0].Nonce != 5 {
		t.Fatalf("unexpected: %+v", all)
	}

	if err := repo.DeletePendingTx(ctx, "0xab"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	all, _ = repo.ListAllPendingTx(ctx)
	if len(all) != 0 {
		t.Fatalf("expected 0 after delete, got %d", len(all))
	}
}

func TestIntegration_HeartbeatUpsert(t *testing.T) {
	repo, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	hs := models.HeartbeatSchedule{AssetID: "weth", Interval: time.Hour, DeviationBps: 150}
	if err := repo.UpsertHeartbeat(ctx, hs); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// Second upsert with new values should overwrite, not duplicate.
	hs.DeviationBps = 300
	if err := repo.UpsertHeartbeat(ctx, hs); err != nil {
		t.Fatalf("upsert 2: %v", err)
	}
	list, err := repo.ListHeartbeats(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].DeviationBps != 300 {
		t.Fatalf("unexpected: %+v", list)
	}
}

func TestIntegration_UpdateSubmissionNotFound(t *testing.T) {
	repo, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()

	// The ID 9999 doesn't exist; UpdateSubmission must surface ErrNotFound.
	err := repo.UpdateSubmission(ctx, &models.Submission{
		ID:             9999,
		ReqID:          "x",
		AssetID:        "weth",
		Aggregator:     common.HexToAddress("0x01"),
		SubmittedPrice: "1",
		Status:         models.SubmissionStatusFailed,
	})
	if err == nil {
		t.Fatal("expected error for non-existent row")
	}
	// pgx.ErrNoRows is not wrapped by UpdateSubmission; ErrNotFound is.
	_ = pgx.ErrNoRows // keep import
}

func formatReqID(i int) string {
	return time.Now().Format("150405") + "_" + intToStr(i)
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	out := ""
	for i > 0 {
		out = string(rune('0'+i%10)) + out
		i /= 10
	}
	return out
}
