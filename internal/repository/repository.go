// Package repository is the pgx/v5 persistence layer for oracle-service.
//
// All persistence sits behind the Repository interface so tests can sub a
// mock (the streamconsumer and submitter packages do exactly that). Real
// callers construct PgxRepository via NewPgxRepository in application.go.
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
)

// ErrNotFound surfaces from getters when no row matches.
var ErrNotFound = errors.New("not found")

// Repository is the minimal surface oracle-service uses against the DB.
// Defined here as an interface so callers in submitter / streamconsumer /
// grpc can take it as a dependency and tests can sub a fake.
type Repository interface {
	// Submissions
	InsertSubmission(ctx context.Context, s *models.Submission) (int64, error)
	UpdateSubmission(ctx context.Context, s *models.Submission) error
	GetSubmissionByReqID(ctx context.Context, reqID string) (*models.Submission, error)
	GetSubmissionByTxHash(ctx context.Context, txHash string) (*models.Submission, error)
	ListSubmissions(ctx context.Context, assetID string, limit, offset int) ([]*models.Submission, int, error)
	// ExistsForAggregatorReqID is the streamconsumer's idempotency check.
	// MUST be scoped by (aggregator, req_id) — req_id is per-aggregator on
	// chain, so two aggregators legitimately share the same req_id (every
	// asset has its own counter starting at 1).
	ExistsForAggregatorReqID(ctx context.Context, aggregator common.Address, reqID string) (bool, error)

	// Async pipeline (task 06.1).
	// EnqueueRequest persists a price-less `queued` row with a TTL deadline and
	// returns its id. The price + tx are filled in later by the worker/sender.
	EnqueueRequest(ctx context.Context, s *models.Submission) (int64, error)
	// MarkExpired terminally abandons a pre-broadcast request that blew its TTL.
	MarkExpired(ctx context.Context, id int64, lastErr string) error
	// LoadResumable returns non-terminal pre-broadcast rows
	// (queued/processing/sending) for startup recovery, with ExpiresAt set.
	LoadResumable(ctx context.Context) ([]*models.Submission, error)
	// ExpireOverdue bulk-marks `expired` any non-terminal row whose TTL has
	// already passed. Returns the count expired.
	ExpireOverdue(ctx context.Context) (int, error)

	// Pending tx tracking (restart resilience).
	InsertPendingTx(ctx context.Context, submissionID int64, txHash string, nonce uint64, broadcaster string, gasStrategyJSON []byte) error
	ListPendingByTxHash(ctx context.Context, txHash string) ([]PendingTx, error)
	DeletePendingTx(ctx context.Context, txHash string) error
	ListAllPendingTx(ctx context.Context) ([]PendingTx, error)

	// Indexer-stream resume cursor.
	GetStreamCursor(ctx context.Context) (uint64, error)
	AdvanceStreamCursor(ctx context.Context, block uint64) error

	// Heartbeat schedule.
	UpsertHeartbeat(ctx context.Context, h models.HeartbeatSchedule) error
	ListHeartbeats(ctx context.Context) ([]models.HeartbeatSchedule, error)

	// Lifecycle.
	Ping(ctx context.Context) error
	Close()
}

// PendingTx is the in-flight tx record. Light-weight DTO (not in models/
// because it never leaves the repo + submitter pair).
type PendingTx struct {
	ID              int64
	SubmissionID    int64
	TxHash          string
	Nonce           uint64
	GasStrategyJSON []byte
	FirstSeenAt     time.Time
}

// PgxRepository implements Repository on top of a pgxpool.Pool.
type PgxRepository struct {
	pool *pgxpool.Pool
}

// NewPgxRepository dials Postgres and returns a Repository-shaped wrapper.
// Caller owns the lifecycle via Close().
func NewPgxRepository(ctx context.Context, cfg *config.DatabaseConfig) (*PgxRepository, error) {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, cfg.SSLMode,
	)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		//nolint:gosec // config-bounded conversion
		poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		//nolint:gosec // config-bounded conversion
		poolCfg.MinConns = int32(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		poolCfg.MaxConnLifetime = time.Duration(cfg.ConnMaxLifetime) * time.Second
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("dial postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PgxRepository{pool: pool}, nil
}

// Close releases the underlying pool.
func (r *PgxRepository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

// Ping is the readiness probe.
func (r *PgxRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// ---------------------------------------------------------------------------
// Submissions
// ---------------------------------------------------------------------------

const insertSubmissionSQL = `
INSERT INTO oracle_submissions (req_id, asset_id, aggregator, tx_hash,
                                submitted_price, submitted_at, status,
                                retry_count, last_error, broadcaster)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING id`

// InsertSubmission persists a new submission row and returns its DB id.
func (r *PgxRepository) InsertSubmission(ctx context.Context, s *models.Submission) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, insertSubmissionSQL,
		s.ReqID,
		s.AssetID,
		s.Aggregator.Hex(),
		txHashStr(s.TxHash),
		nullableStr(s.SubmittedPrice),
		nowOrPassed(s.SubmittedAt),
		s.Status.String(),
		s.RetryCount,
		s.LastError,
		addrStr(s.Broadcaster),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert submission: %w", err)
	}
	s.ID = id
	return id, nil
}

const updateSubmissionSQL = `
UPDATE oracle_submissions
SET    tx_hash         = $2,
       submitted_price = $3,
       submitted_at    = $4,
       status          = $5,
       retry_count     = $6,
       last_error      = $7,
       -- Preserve a previously-recorded broadcaster when this update doesn't
       -- carry one (status-only transitions like queued->sending, finalize).
       broadcaster     = COALESCE(NULLIF($8, ''), broadcaster),
       updated_at      = now()
WHERE  id = $1`

// UpdateSubmission persists a status / retry / tx-hash update.
func (r *PgxRepository) UpdateSubmission(ctx context.Context, s *models.Submission) error {
	tag, err := r.pool.Exec(ctx, updateSubmissionSQL,
		s.ID,
		txHashStr(s.TxHash),
		nullableStr(s.SubmittedPrice),
		nowOrPassed(s.SubmittedAt),
		s.Status.String(),
		s.RetryCount,
		s.LastError,
		addrStr(s.Broadcaster),
	)
	if err != nil {
		return fmt.Errorf("update submission: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

const selectSubmissionByReqIDSQL = `
SELECT id, req_id, asset_id, aggregator, tx_hash, submitted_price,
       submitted_at, status, retry_count, last_error
FROM   oracle_submissions
WHERE  req_id = $1 AND req_id <> '0'
ORDER  BY submitted_at DESC
LIMIT  1`

// GetSubmissionByReqID returns the most recent submission for a non-heartbeat
// req_id. Heartbeat submissions (req_id == "0") are not addressable by req_id;
// callers must use tx_hash instead.
func (r *PgxRepository) GetSubmissionByReqID(ctx context.Context, reqID string) (*models.Submission, error) {
	return r.querySubmission(ctx, selectSubmissionByReqIDSQL, reqID)
}

const selectSubmissionByTxHashSQL = `
SELECT id, req_id, asset_id, aggregator, tx_hash, submitted_price,
       submitted_at, status, retry_count, last_error
FROM   oracle_submissions
WHERE  tx_hash = $1`

// GetSubmissionByTxHash looks up a submission by its broadcast tx hash.
func (r *PgxRepository) GetSubmissionByTxHash(ctx context.Context, txHash string) (*models.Submission, error) {
	return r.querySubmission(ctx, selectSubmissionByTxHashSQL, txHash)
}

const listSubmissionsCountSQL = `
SELECT COUNT(*)
FROM   oracle_submissions
WHERE  ($1 = '' OR asset_id = $1)`

const listSubmissionsSQL = `
SELECT id, req_id, asset_id, aggregator, tx_hash, submitted_price,
       submitted_at, status, retry_count, last_error
FROM   oracle_submissions
WHERE  ($1 = '' OR asset_id = $1)
ORDER  BY submitted_at DESC
LIMIT  $2 OFFSET $3`

// ListSubmissions returns a page of submissions ordered by submitted_at DESC.
// `assetID == ""` means no asset filter. Returns the page slice + total count.
func (r *PgxRepository) ListSubmissions(ctx context.Context, assetID string, limit, offset int) ([]*models.Submission, int, error) {
	var total int
	if err := r.pool.QueryRow(ctx, listSubmissionsCountSQL, assetID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count submissions: %w", err)
	}

	rows, err := r.pool.Query(ctx, listSubmissionsSQL, assetID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list submissions: %w", err)
	}
	defer rows.Close()

	out := make([]*models.Submission, 0, limit)
	for rows.Next() {
		sub, err := scanSubmission(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate submissions: %w", err)
	}
	return out, total, nil
}

const existsForAggregatorReqIDSQL = `
SELECT EXISTS (
    SELECT 1 FROM oracle_submissions
    WHERE  req_id = $1
      AND  aggregator = $2
      AND  req_id <> '0'
)`

// ExistsForAggregatorReqID is the idempotency check the stream consumer
// hits before dispatching a delivered event. Scoped by (aggregator, req_id)
// because req_id is per-aggregator on chain — every asset's PriceAggregator
// has its own counter, so the same req_id value coexists across all 10
// assets. Scoping by req_id alone (as the original implementation did)
// caused the second-and-subsequent asset's events to be silently skipped
// after the first asset's row was persisted. See bugfix note #4 in
// docs/SECURITY.md for the live-debug forensics.
//
// Heartbeat submissions (req_id == "0") are excluded by definition.
func (r *PgxRepository) ExistsForAggregatorReqID(ctx context.Context, aggregator common.Address, reqID string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, existsForAggregatorReqIDSQL, reqID, aggregator.Hex()).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("exists by aggregator+req_id: %w", err)
	}
	return exists, nil
}

// ---------------------------------------------------------------------------
// Async request queue (task 06.1)
// ---------------------------------------------------------------------------

const enqueueRequestSQL = `
INSERT INTO oracle_submissions (req_id, asset_id, aggregator, tx_hash,
                                submitted_price, submitted_at, status,
                                retry_count, last_error, queued_at, expires_at)
VALUES ($1, $2, $3, '', NULL, now(), 'queued', 0, '', now(), $4)
RETURNING id`

// EnqueueRequest persists a price-less `queued` row with its TTL deadline.
// The streamconsumer's ExistsForAggregatorReqID call upstream provides the
// (aggregator, req_id) idempotency, so this is a plain insert.
func (r *PgxRepository) EnqueueRequest(ctx context.Context, s *models.Submission) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, enqueueRequestSQL,
		s.ReqID,
		s.AssetID,
		s.Aggregator.Hex(),
		s.ExpiresAt,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("enqueue request: %w", err)
	}
	s.ID = id
	return id, nil
}

const markExpiredSQL = `
UPDATE oracle_submissions
SET    status = 'expired', last_error = $2, updated_at = now()
WHERE  id = $1
  AND  status IN ('queued', 'processing', 'sending')`

// MarkExpired terminally abandons a pre-broadcast request. The status guard
// ensures we never expire a row that has already consumed a nonce (pending+).
func (r *PgxRepository) MarkExpired(ctx context.Context, id int64, lastErr string) error {
	_, err := r.pool.Exec(ctx, markExpiredSQL, id, lastErr)
	if err != nil {
		return fmt.Errorf("mark expired: %w", err)
	}
	return nil
}

const loadResumableSQL = `
SELECT id, req_id, asset_id, aggregator, tx_hash, submitted_price,
       submitted_at, status, retry_count, last_error, expires_at
FROM   oracle_submissions
WHERE  status IN ('queued', 'processing', 'sending')
ORDER  BY id`

// LoadResumable returns pre-broadcast non-terminal rows for startup recovery.
func (r *PgxRepository) LoadResumable(ctx context.Context) ([]*models.Submission, error) {
	rows, err := r.pool.Query(ctx, loadResumableSQL)
	if err != nil {
		return nil, fmt.Errorf("load resumable: %w", err)
	}
	defer rows.Close()

	out := make([]*models.Submission, 0)
	for rows.Next() {
		var (
			s             models.Submission
			aggregatorHex string
			txHashHex     string
			price         sql.NullString
			statusStr     string
			expiresAt     sql.NullTime
		)
		if err := rows.Scan(&s.ID, &s.ReqID, &s.AssetID, &aggregatorHex, &txHashHex,
			&price, &s.SubmittedAt, &statusStr, &s.RetryCount, &s.LastError, &expiresAt); err != nil {
			return nil, fmt.Errorf("scan resumable: %w", err)
		}
		s.Aggregator = common.HexToAddress(aggregatorHex)
		if txHashHex != "" {
			s.TxHash = common.HexToHash(txHashHex)
		}
		s.SubmittedPrice = price.String
		if expiresAt.Valid {
			s.ExpiresAt = expiresAt.Time
		}
		status, err := models.ParseSubmissionStatus(statusStr)
		if err != nil {
			return nil, fmt.Errorf("decode resumable status: %w", err)
		}
		s.Status = status
		out = append(out, &s)
	}
	return out, rows.Err()
}

const expireOverdueSQL = `
UPDATE oracle_submissions
SET    status = 'expired', last_error = 'ttl exceeded (overdue sweep)', updated_at = now()
WHERE  status IN ('queued', 'processing', 'sending')
  AND  expires_at IS NOT NULL
  AND  expires_at < now()`

// ExpireOverdue bulk-marks expired any pre-broadcast row whose TTL has passed.
func (r *PgxRepository) ExpireOverdue(ctx context.Context) (int, error) {
	tag, err := r.pool.Exec(ctx, expireOverdueSQL)
	if err != nil {
		return 0, fmt.Errorf("expire overdue: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ---------------------------------------------------------------------------
// Pending txs
// ---------------------------------------------------------------------------

const insertPendingTxSQL = `
INSERT INTO pending_txs (submission_id, tx_hash, nonce, gas_strategy_json, broadcaster)
VALUES ($1, $2, $3, COALESCE($4::jsonb, '{}'::jsonb), $5)`

// InsertPendingTx records an in-flight tx so a restart can resume
// reconciliation. broadcaster is the wallet whose nonce the tx consumed
// (task 06.3) — recorded so a future recovery can replace-by-fee from the
// same wallet+nonce.
func (r *PgxRepository) InsertPendingTx(ctx context.Context, submissionID int64, txHash string, nonce uint64, broadcaster string, gasStrategyJSON []byte) error {
	//nolint:gosec // nonce comes from chain client and fits int64 in practice
	_, err := r.pool.Exec(ctx, insertPendingTxSQL, submissionID, txHash, int64(nonce), gasStrategyJSON, broadcaster)
	if err != nil {
		return fmt.Errorf("insert pending tx: %w", err)
	}
	return nil
}

const selectPendingByTxHashSQL = `
SELECT id, submission_id, tx_hash, nonce, gas_strategy_json, first_seen_at
FROM   pending_txs
WHERE  tx_hash = $1`

// ListPendingByTxHash returns all pending records for a given tx hash (usually
// 0 or 1 rows, but multiple if the same tx hash has been recorded twice).
func (r *PgxRepository) ListPendingByTxHash(ctx context.Context, txHash string) ([]PendingTx, error) {
	return r.queryPending(ctx, selectPendingByTxHashSQL, txHash)
}

const listAllPendingSQL = `
SELECT id, submission_id, tx_hash, nonce, gas_strategy_json, first_seen_at
FROM   pending_txs
ORDER  BY first_seen_at ASC`

// ListAllPendingTx is used by application startup to reconcile in-flight txs.
func (r *PgxRepository) ListAllPendingTx(ctx context.Context) ([]PendingTx, error) {
	return r.queryPending(ctx, listAllPendingSQL)
}

const deletePendingTxSQL = `DELETE FROM pending_txs WHERE tx_hash = $1`

// DeletePendingTx clears a pending tx row once the submission is terminal.
func (r *PgxRepository) DeletePendingTx(ctx context.Context, txHash string) error {
	_, err := r.pool.Exec(ctx, deletePendingTxSQL, txHash)
	if err != nil {
		return fmt.Errorf("delete pending tx: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Stream cursor
// ---------------------------------------------------------------------------

const getStreamCursorSQL = `SELECT last_acked_block FROM stream_cursor WHERE id = 1`

// GetStreamCursor returns the last block the stream consumer successfully
// acked. The row is initialized to (1, 0) by the init migration so this
// never returns ErrNotFound.
func (r *PgxRepository) GetStreamCursor(ctx context.Context) (uint64, error) {
	var b int64
	if err := r.pool.QueryRow(ctx, getStreamCursorSQL).Scan(&b); err != nil {
		return 0, fmt.Errorf("get stream cursor: %w", err)
	}
	if b < 0 {
		b = 0
	}
	return uint64(b), nil
}

const advanceStreamCursorSQL = `
UPDATE stream_cursor
SET    last_acked_block = $1,
       updated_at       = now()
WHERE  id = 1
  AND  $1 >= last_acked_block`

// AdvanceStreamCursor is monotonic — a no-op if `block` is less than the
// recorded cursor. Prevents accidental rewinds.
func (r *PgxRepository) AdvanceStreamCursor(ctx context.Context, block uint64) error {
	//nolint:gosec // block height fits int64 in practice
	_, err := r.pool.Exec(ctx, advanceStreamCursorSQL, int64(block))
	if err != nil {
		return fmt.Errorf("advance stream cursor: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Heartbeat schedules
// ---------------------------------------------------------------------------

const upsertHeartbeatSQL = `
INSERT INTO heartbeat_schedules (asset_id, interval_sec, deviation_bps, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (asset_id) DO UPDATE
   SET interval_sec  = EXCLUDED.interval_sec,
       deviation_bps = EXCLUDED.deviation_bps,
       updated_at    = now()`

// UpsertHeartbeat persists a single asset's heartbeat schedule.
func (r *PgxRepository) UpsertHeartbeat(ctx context.Context, h models.HeartbeatSchedule) error {
	_, err := r.pool.Exec(ctx, upsertHeartbeatSQL,
		h.AssetID,
		int(h.Interval.Seconds()),
		int(h.DeviationBps),
	)
	if err != nil {
		return fmt.Errorf("upsert heartbeat: %w", err)
	}
	return nil
}

const listHeartbeatsSQL = `
SELECT asset_id, interval_sec, deviation_bps, updated_at
FROM   heartbeat_schedules`

// ListHeartbeats returns every persisted heartbeat schedule.
func (r *PgxRepository) ListHeartbeats(ctx context.Context) ([]models.HeartbeatSchedule, error) {
	rows, err := r.pool.Query(ctx, listHeartbeatsSQL)
	if err != nil {
		return nil, fmt.Errorf("list heartbeats: %w", err)
	}
	defer rows.Close()

	var out []models.HeartbeatSchedule
	for rows.Next() {
		var (
			h            models.HeartbeatSchedule
			intervalSec  int
			deviationBps int
			updatedAt    time.Time
		)
		if err := rows.Scan(&h.AssetID, &intervalSec, &deviationBps, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan heartbeat: %w", err)
		}
		h.Interval = time.Duration(intervalSec) * time.Second
		//nolint:gosec // bps is bounded to uint32 range at insert time
		h.DeviationBps = uint32(deviationBps)
		h.UpdatedAt = updatedAt
		out = append(out, h)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (r *PgxRepository) querySubmission(ctx context.Context, sql string, args ...any) (*models.Submission, error) {
	row := r.pool.QueryRow(ctx, sql, args...)
	sub, err := scanSubmission(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return sub, nil
}

func (r *PgxRepository) queryPending(ctx context.Context, sql string, args ...any) ([]PendingTx, error) {
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query pending tx: %w", err)
	}
	defer rows.Close()

	out := make([]PendingTx, 0)
	for rows.Next() {
		var (
			p     PendingTx
			nonce int64
		)
		if err := rows.Scan(&p.ID, &p.SubmissionID, &p.TxHash, &nonce, &p.GasStrategyJSON, &p.FirstSeenAt); err != nil {
			return nil, fmt.Errorf("scan pending tx: %w", err)
		}
		//nolint:gosec // nonce fits uint64 by definition
		p.Nonce = uint64(nonce)
		out = append(out, p)
	}
	return out, rows.Err()
}

// rowScanner abstracts QueryRow and Rows so scanSubmission can serve both
// the single-row and multi-row code paths without duplication.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanSubmission(r rowScanner) (*models.Submission, error) {
	var (
		s             models.Submission
		aggregatorHex string
		txHashHex     string
		price         sql.NullString
		statusStr     string
	)
	err := r.Scan(
		&s.ID,
		&s.ReqID,
		&s.AssetID,
		&aggregatorHex,
		&txHashHex,
		&price,
		&s.SubmittedAt,
		&statusStr,
		&s.RetryCount,
		&s.LastError,
	)
	if err != nil {
		return nil, err
	}
	s.Aggregator = common.HexToAddress(aggregatorHex)
	if txHashHex != "" {
		s.TxHash = common.HexToHash(txHashHex)
	}
	s.SubmittedPrice = price.String // empty when NULL (queued/expired rows)
	status, err := models.ParseSubmissionStatus(statusStr)
	if err != nil {
		return nil, fmt.Errorf("decode submission status: %w", err)
	}
	s.Status = status
	return &s, nil
}

func txHashStr(h common.Hash) string {
	if (h == common.Hash{}) {
		return ""
	}
	return h.Hex()
}

// addrStr maps the zero address to an empty string so an un-broadcast row
// stores "" rather than the all-zero address in broadcaster.
func addrStr(a common.Address) string {
	if (a == common.Address{}) {
		return ""
	}
	return a.Hex()
}

// nullableStr maps an empty string to a SQL NULL so a price-less `queued` row
// stores NULL rather than a misleading "" in submitted_price.
func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nowOrPassed(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t
}
