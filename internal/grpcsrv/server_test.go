package grpcsrv

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
	commonv1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/common/v1"
	oraclev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/oracle/v1"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/models"
	"github.com/asolovov/evm-oracle-demo-oracle-service/internal/repository"
)

type fakeReader struct {
	byReq    map[string]*models.Submission
	byTx     map[string]*models.Submission
	list     []*models.Submission
	listTotal int
	listErr  error
}

func (f *fakeReader) GetSubmissionByReqID(_ context.Context, reqID string) (*models.Submission, error) {
	if s, ok := f.byReq[reqID]; ok {
		return s, nil
	}
	return nil, repository.ErrNotFound
}
func (f *fakeReader) GetSubmissionByTxHash(_ context.Context, txHash string) (*models.Submission, error) {
	if s, ok := f.byTx[txHash]; ok {
		return s, nil
	}
	return nil, repository.ErrNotFound
}
func (f *fakeReader) ListSubmissions(_ context.Context, _ string, _, _ int) ([]*models.Submission, int, error) {
	return f.list, f.listTotal, f.listErr
}

type fakeHB struct {
	upserted *models.HeartbeatSchedule
	err      error
}

func (f *fakeHB) UpsertHeartbeat(_ context.Context, h models.HeartbeatSchedule) error {
	if f.err != nil {
		return f.err
	}
	f.upserted = &h
	return nil
}

func newServer(reader SubmissionReader, hb HeartbeatStore) *Server {
	return New(&config.GRPCConfig{Host: "127.0.0.1", Port: 0}, reader, hb, nil)
}

func TestSetHeartbeat_Persists(t *testing.T) {
	hb := &fakeHB{}
	srv := newServer(&fakeReader{}, hb)
	_, err := srv.SetHeartbeat(context.Background(), &oraclev1.SetHeartbeatRequest{
		AssetId: "weth", IntervalSec: 3600, DeviationBps: 150,
	})
	if err != nil {
		t.Fatalf("SetHeartbeat: %v", err)
	}
	if hb.upserted == nil {
		t.Fatal("expected upsert")
	}
	if hb.upserted.Interval != time.Hour {
		t.Fatalf("interval %v, want 1h", hb.upserted.Interval)
	}
	if hb.upserted.DeviationBps != 150 {
		t.Fatalf("bps %d, want 150", hb.upserted.DeviationBps)
	}
}

func TestSetHeartbeat_RejectsEmptyAsset(t *testing.T) {
	srv := newServer(&fakeReader{}, &fakeHB{})
	_, err := srv.SetHeartbeat(context.Background(), &oraclev1.SetHeartbeatRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestSetHeartbeat_PersistFailureMaps500(t *testing.T) {
	srv := newServer(&fakeReader{}, &fakeHB{err: errors.New("boom")})
	_, err := srv.SetHeartbeat(context.Background(), &oraclev1.SetHeartbeatRequest{AssetId: "weth"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestGetSubmissionStatus_RejectsBothFields(t *testing.T) {
	srv := newServer(&fakeReader{}, &fakeHB{})
	_, err := srv.GetSubmissionStatus(context.Background(), &oraclev1.GetSubmissionStatusRequest{
		ReqId: "1", TxHash: "0x01",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestGetSubmissionStatus_RejectsBothEmpty(t *testing.T) {
	srv := newServer(&fakeReader{}, &fakeHB{})
	_, err := srv.GetSubmissionStatus(context.Background(), &oraclev1.GetSubmissionStatusRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestGetSubmissionStatus_RejectsHeartbeatReqID(t *testing.T) {
	srv := newServer(&fakeReader{}, &fakeHB{})
	_, err := srv.GetSubmissionStatus(context.Background(), &oraclev1.GetSubmissionStatusRequest{
		ReqId: models.HeartbeatReqID,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("heartbeat req_id should be rejected, got %v", err)
	}
}

func TestGetSubmissionStatus_ByReqID_Found(t *testing.T) {
	want := &models.Submission{
		ReqID:          "42",
		AssetID:        "weth",
		SubmittedPrice: "345020000000",
		Status:         models.SubmissionStatusConfirmed,
	}
	reader := &fakeReader{byReq: map[string]*models.Submission{"42": want}}
	srv := newServer(reader, &fakeHB{})

	got, err := srv.GetSubmissionStatus(context.Background(), &oraclev1.GetSubmissionStatusRequest{ReqId: "42"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got.GetReqId() != "42" {
		t.Fatalf("got reqId %q, want 42", got.GetReqId())
	}
}

func TestGetSubmissionStatus_NotFound(t *testing.T) {
	srv := newServer(&fakeReader{byReq: map[string]*models.Submission{}}, &fakeHB{})
	_, err := srv.GetSubmissionStatus(context.Background(), &oraclev1.GetSubmissionStatusRequest{ReqId: "999"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestListSubmissions_Pagination(t *testing.T) {
	subs := []*models.Submission{
		{ReqID: "1", AssetID: "weth", Status: models.SubmissionStatusConfirmed},
		{ReqID: "2", AssetID: "weth", Status: models.SubmissionStatusPending},
	}
	reader := &fakeReader{list: subs, listTotal: 5}
	srv := newServer(reader, &fakeHB{})

	resp, err := srv.ListSubmissions(context.Background(), &oraclev1.ListSubmissionsRequest{
		AssetId: "weth",
		Page:    &commonv1.PageRequest{Page: 1, PageSize: 2},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.GetSubmissions()) != 2 {
		t.Fatalf("expected 2 submissions, got %d", len(resp.GetSubmissions()))
	}
	if resp.GetPage().GetTotalCount() != 5 {
		t.Fatalf("total %d, want 5", resp.GetPage().GetTotalCount())
	}
	if resp.GetPage().GetTotalPages() != 3 {
		t.Fatalf("total pages %d, want 3", resp.GetPage().GetTotalPages())
	}
}

func TestListSubmissions_DefaultPageSize(t *testing.T) {
	reader := &fakeReader{list: nil, listTotal: 0}
	srv := newServer(reader, &fakeHB{})
	resp, err := srv.ListSubmissions(context.Background(), &oraclev1.ListSubmissionsRequest{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp.GetPage().GetPageSize() != defaultPageSize {
		t.Fatalf("expected default page size %d, got %d", defaultPageSize, resp.GetPage().GetPageSize())
	}
}

func TestListSubmissions_ClampsPageSize(t *testing.T) {
	reader := &fakeReader{list: nil, listTotal: 0}
	srv := newServer(reader, &fakeHB{})
	resp, err := srv.ListSubmissions(context.Background(), &oraclev1.ListSubmissionsRequest{
		Page: &commonv1.PageRequest{Page: 1, PageSize: 9999},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// pageBounds clamps to maxPageSize internally; the response echoes the
	// caller's page size (we don't lie about what we accepted) but the SQL
	// would have used maxPageSize. Verify clamp via pageBounds directly.
	if resp.GetPage().GetPageSize() != 9999 {
		t.Fatalf("response should echo caller's page_size; got %d", resp.GetPage().GetPageSize())
	}
	limit, _ := pageBounds(&commonv1.PageRequest{Page: 1, PageSize: 9999})
	if limit != maxPageSize {
		t.Fatalf("expected clamp to %d, got %d", maxPageSize, limit)
	}
}

func TestPageBounds(t *testing.T) {
	cases := []struct {
		in     *commonv1.PageRequest
		limit  int
		offset int
	}{
		{nil, defaultPageSize, 0},
		{&commonv1.PageRequest{Page: 1, PageSize: 10}, 10, 0},
		{&commonv1.PageRequest{Page: 3, PageSize: 10}, 10, 20},
		{&commonv1.PageRequest{Page: 0, PageSize: 10}, 10, 0},
		{&commonv1.PageRequest{Page: 1, PageSize: 99999}, maxPageSize, 0},
	}
	for _, c := range cases {
		l, o := pageBounds(c.in)
		if l != c.limit || o != c.offset {
			t.Fatalf("pageBounds(%v) = (%d,%d), want (%d,%d)", c.in, l, o, c.limit, c.offset)
		}
	}
}
