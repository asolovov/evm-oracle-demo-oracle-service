package models

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	oraclev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/oracle/v1"
)

func TestSubmission_ToProto(t *testing.T) {
	now := time.Now().UTC()
	sub := &Submission{
		ID:             1,
		ReqID:          "42",
		AssetID:        "weth",
		Aggregator:     common.HexToAddress("0x075be31662c2548c4e940d7e769c328a34dcb281"),
		TxHash:         common.HexToHash("0xabc"),
		SubmittedPrice: "345020000000",
		SubmittedAt:    now,
		Status:         SubmissionStatusConfirmed,
		RetryCount:     2,
		LastError:      "",
	}
	p := sub.ToProto()
	if p.GetReqId() != "42" || p.GetAssetId() != "weth" || p.GetSubmittedPrice() != "345020000000" {
		t.Fatalf("scalar fields wrong: %+v", p)
	}
	if p.GetStatus() != oraclev1.SubmissionStatus_STATUS_CONFIRMED {
		t.Fatalf("status %v, want CONFIRMED", p.GetStatus())
	}
	if p.GetRetryCount() != 2 {
		t.Fatalf("retry %d, want 2", p.GetRetryCount())
	}
	if p.GetTxHash() == "" {
		t.Fatal("tx hash should be populated")
	}
	if !p.GetSubmittedAt().AsTime().Equal(now) {
		t.Fatalf("submittedAt round-trip failed")
	}
}

func TestSubmission_ToProto_ZeroTxHash_OmitsHex(t *testing.T) {
	sub := &Submission{
		ReqID:          "1",
		AssetID:        "weth",
		SubmittedPrice: "0",
		Status:         SubmissionStatusPending,
	}
	p := sub.ToProto()
	if p.GetTxHash() != "" {
		t.Fatalf("zero hash should serialize to empty string, got %q", p.GetTxHash())
	}
	if p.GetSubmittedAt() != nil {
		t.Fatalf("zero submittedAt should serialize to nil, got %v", p.GetSubmittedAt())
	}
}

func TestSubmissionFromProto_RoundTrip(t *testing.T) {
	orig := &Submission{
		ID:             7,
		ReqID:          "99",
		AssetID:        "xau",
		Aggregator:     common.HexToAddress("0xdead"),
		TxHash:         common.HexToHash("0xbeef"),
		SubmittedPrice: "200000000000",
		SubmittedAt:    time.Unix(1700000000, 0).UTC(),
		Status:         SubmissionStatusFailed,
		RetryCount:     1,
		LastError:      "tx reverted",
	}
	round := SubmissionFromProto(orig.ToProto())
	if round.ReqID != orig.ReqID || round.AssetID != orig.AssetID ||
		round.SubmittedPrice != orig.SubmittedPrice || round.Status != orig.Status ||
		round.RetryCount != orig.RetryCount || round.LastError != orig.LastError {
		t.Fatalf("round-trip mismatch: %+v vs %+v", round, orig)
	}
	if round.TxHash != orig.TxHash {
		t.Fatalf("tx hash round-trip: %s vs %s", round.TxHash, orig.TxHash)
	}
}

func TestSubmissionFromProto_Nil(t *testing.T) {
	if SubmissionFromProto(nil) != nil {
		t.Fatal("nil input should produce nil output")
	}
}
