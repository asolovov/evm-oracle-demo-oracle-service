// Package grpcsrv hosts the OracleService gRPC server.
//
// Surface (per spec OQ-10): admin + read only — no TriggerUpdate. The trigger
// comes from the indexer.StreamEvents subscription (see streamconsumer).
//
//   - SetHeartbeat: admin RPC; persists a per-asset heartbeat schedule.
//   - GetSubmissionStatus: read RPC; by req_id or tx_hash (exactly one).
//   - ListSubmissions: read RPC; paginated by submitted_at DESC.
package grpcsrv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	commonv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/common/v1"
	oraclev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/oracle/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/repository"
)

// HeartbeatStore is the persistence + reload surface the server needs for
// SetHeartbeat. Production wiring uses *repository.PgxRepository; tests sub
// a fake.
type HeartbeatStore interface {
	UpsertHeartbeat(ctx context.Context, h models.HeartbeatSchedule) error
}

// SubmissionReader is the read-side repo surface used by GetSubmissionStatus
// and ListSubmissions.
type SubmissionReader interface {
	GetSubmissionByReqID(ctx context.Context, reqID string) (*models.Submission, error)
	GetSubmissionByTxHash(ctx context.Context, txHash string) (*models.Submission, error)
	ListSubmissions(ctx context.Context, assetID string, limit, offset int) ([]*models.Submission, int, error)
}

// Server is the wiring point. Holds the configured *grpc.Server and the
// listener so application.go can close it cleanly.
type Server struct {
	oraclev1.UnimplementedOracleServiceServer

	cfg      *config.GRPCConfig
	subs     SubmissionReader
	hb       HeartbeatStore
	log      *logrus.Entry
	listener net.Listener
	gs       *grpc.Server
}

// New constructs the Server. Caller invokes Serve() to start listening.
func New(cfg *config.GRPCConfig, subs SubmissionReader, hb HeartbeatStore, log *logrus.Entry) *Server {
	if log == nil {
		log = logrus.NewEntry(logrus.StandardLogger()).WithField("component", "grpcsrv")
	}
	return &Server{cfg: cfg, subs: subs, hb: hb, log: log}
}

// Serve binds the listener and starts the server. Returns once Stop() is
// called or the listener errors.
func (s *Server) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	ln, err := net.Listen("tcp", addr) //nolint:noctx // gRPC servers don't have a context surface here
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.listener = ln

	gs := grpc.NewServer()
	oraclev1.RegisterOracleServiceServer(gs, s)

	hs := health.NewServer()
	hs.SetServingStatus("oracle.v1.OracleService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)

	if s.cfg.ReflectionEnabled {
		reflection.Register(gs)
	}
	s.gs = gs

	s.log.WithField("addr", addr).Info("oracle gRPC server listening")
	return gs.Serve(ln)
}

// Stop drains in-flight RPCs and closes the listener.
func (s *Server) Stop(ctx context.Context) error {
	if s.gs == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		s.gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.gs.Stop()
		return ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// SetHeartbeat — admin
// ---------------------------------------------------------------------------

// SetHeartbeat persists a per-asset heartbeat configuration. asset_id is the
// canonical lowercase symbol ("weth"); interval_sec == 0 disables time-based
// fires; deviation_bps == 0 disables the deviation-based path.
func (s *Server) SetHeartbeat(ctx context.Context, req *oraclev1.SetHeartbeatRequest) (*oraclev1.SetHeartbeatResponse, error) {
	if req.GetAssetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "asset_id is required")
	}
	hs := models.HeartbeatSchedule{
		AssetID:      req.GetAssetId(),
		Interval:     time.Duration(req.GetIntervalSec()) * time.Second,
		DeviationBps: req.GetDeviationBps(),
	}
	if err := s.hb.UpsertHeartbeat(ctx, hs); err != nil {
		s.log.WithError(err).Error("upsert heartbeat")
		return nil, status.Error(codes.Internal, "persist heartbeat")
	}
	return &oraclev1.SetHeartbeatResponse{}, nil
}

// ---------------------------------------------------------------------------
// GetSubmissionStatus — read
// ---------------------------------------------------------------------------

// GetSubmissionStatus selects by exactly one of req_id or tx_hash. Sending
// both, or neither, returns INVALID_ARGUMENT.
func (s *Server) GetSubmissionStatus(ctx context.Context, req *oraclev1.GetSubmissionStatusRequest) (*oraclev1.SubmissionStatus, error) {
	hasReq := req.GetReqId() != ""
	hasTx := req.GetTxHash() != ""
	if hasReq == hasTx {
		return nil, status.Error(codes.InvalidArgument, "exactly one of req_id or tx_hash is required")
	}
	if hasReq && req.GetReqId() == models.HeartbeatReqID {
		return nil, status.Error(codes.InvalidArgument, "heartbeat submissions are not addressable by req_id; use tx_hash")
	}

	var (
		sub *models.Submission
		err error
	)
	if hasReq {
		sub, err = s.subs.GetSubmissionByReqID(ctx, req.GetReqId())
	} else {
		sub, err = s.subs.GetSubmissionByTxHash(ctx, req.GetTxHash())
	}
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "submission not found")
		}
		s.log.WithError(err).Error("read submission")
		return nil, status.Error(codes.Internal, "read submission")
	}
	return sub.ToProto(), nil
}

// ---------------------------------------------------------------------------
// ListSubmissions — read
// ---------------------------------------------------------------------------

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

// ListSubmissions paginates submitted_at DESC, optionally filtered by
// asset_id. Page tokens are simple offsets (page_number) — the spec doesn't
// require opaque cursors here.
func (s *Server) ListSubmissions(ctx context.Context, req *oraclev1.ListSubmissionsRequest) (*oraclev1.ListSubmissionsResponse, error) {
	limit, offset := pageBounds(req.GetPage())

	subs, total, err := s.subs.ListSubmissions(ctx, req.GetAssetId(), limit, offset)
	if err != nil {
		s.log.WithError(err).Error("list submissions")
		return nil, status.Error(codes.Internal, "list submissions")
	}

	out := make([]*oraclev1.SubmissionStatus, len(subs))
	for i, sub := range subs {
		out[i] = sub.ToProto()
	}

	page := req.GetPage()
	pageNumber := int32(1)
	pageSize := int32(limit) //nolint:gosec // limit is clamped to maxPageSize (<= int32) by pageBounds
	if page != nil {
		if page.GetPage() > 0 {
			pageNumber = page.GetPage()
		}
		if page.GetPageSize() > 0 {
			pageSize = page.GetPageSize()
		}
	}

	totalPages := int32(1)
	if pageSize > 0 {
		//nolint:gosec // bounded by DB count + clamped page size
		totalPages = int32((total + int(pageSize) - 1) / int(pageSize))
		if totalPages < 1 {
			totalPages = 1
		}
	}

	return &oraclev1.ListSubmissionsResponse{
		Submissions: out,
		Page: &commonv1.PageResponse{
			//nolint:gosec // total bounded by DB count; fits int32 in practice
			TotalCount: int32(total),
			Page:       pageNumber,
			PageSize:   pageSize,
			TotalPages: totalPages,
		},
	}, nil
}

func pageBounds(page *commonv1.PageRequest) (limit, offset int) {
	if page == nil {
		return defaultPageSize, 0
	}
	limit = int(page.GetPageSize())
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	num := int(page.GetPage())
	if num < 1 {
		num = 1
	}
	offset = (num - 1) * limit
	return limit, offset
}
