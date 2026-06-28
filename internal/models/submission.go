package models

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/types/known/timestamppb"

	oraclev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/oracle/v1"
)

// HeartbeatReqID is the sentinel req_id used for heartbeat submissions where
// no on-chain PriceRequested triggered the update. Stored as the string "0"
// to match the on-chain `uint256` convention on the wire.
const HeartbeatReqID = "0"

// Submission is the persisted record of one on-chain fulfilPrice attempt
// (consumer-request driven OR heartbeat).
//
// req_id and submitted_price are decimal strings — uint256/int256 do not fit
// any Go primitive cleanly, and we never need to do arithmetic on them after
// signing.
type Submission struct {
	ID             int64
	ReqID          string // "0" for heartbeat per spec
	AssetID        string // canonical lowercase symbol ("weth", "xau", ...) or 0x-hex bytes32
	Aggregator     common.Address
	TxHash         common.Hash      // zero until first broadcast
	SubmittedPrice string           // int256 in Chainlink 8-decimal scale, decimal string; empty until a worker prices it
	SubmittedAt    time.Time
	Status         SubmissionStatus
	RetryCount     int
	LastError      string

	// ExpiresAt is the TTL deadline for a queued request (task 06.1). Zero for
	// heartbeat submissions (which bypass the queue) and for terminal rows.
	ExpiresAt time.Time

	// Broadcaster is the EOA that broadcast this submission's tx (task 06.3).
	// Zero until the sender picks a wallet from the pool. Recorded so
	// replace-by-fee + ops can attribute a tx to its wallet+nonce.
	Broadcaster common.Address
}

// ToProto produces the wire form for ListSubmissions / GetSubmissionStatus.
func (s *Submission) ToProto() *oraclev1.SubmissionStatus {
	var txHashStr string
	if (s.TxHash != common.Hash{}) {
		txHashStr = s.TxHash.Hex()
	}
	var ts *timestamppb.Timestamp
	if !s.SubmittedAt.IsZero() {
		ts = timestamppb.New(s.SubmittedAt)
	}
	return &oraclev1.SubmissionStatus{
		ReqId:          s.ReqID,
		AssetId:        s.AssetID,
		TxHash:         txHashStr,
		SubmittedPrice: s.SubmittedPrice,
		SubmittedAt:    ts,
		Status:         s.Status.ToProto(),
		RetryCount:     uint32(s.RetryCount), //nolint:gosec // bounded by SubmissionConfig.MaxRetries (≤ small int)
		LastError:      s.LastError,
	}
}

// SubmissionFromProto is the inverse of ToProto. Used in tests; the server
// itself usually builds Submission from domain inputs rather than proto.
func SubmissionFromProto(p *oraclev1.SubmissionStatus) *Submission {
	if p == nil {
		return nil
	}
	out := &Submission{
		ReqID:          p.GetReqId(),
		AssetID:        p.GetAssetId(),
		SubmittedPrice: p.GetSubmittedPrice(),
		Status:         SubmissionStatusFromProto(p.GetStatus()),
		RetryCount:     int(p.GetRetryCount()),
		LastError:      p.GetLastError(),
	}
	if h := p.GetTxHash(); h != "" {
		out.TxHash = common.HexToHash(h)
	}
	if ts := p.GetSubmittedAt(); ts != nil {
		out.SubmittedAt = ts.AsTime()
	}
	return out
}

// ReqIDToBigInt parses the decimal-string req_id into *big.Int. Returns an
// error on garbage input so callers can NOT_FOUND rather than panic.
func ReqIDToBigInt(reqID string) (*big.Int, bool) {
	v, ok := new(big.Int).SetString(reqID, 10)
	return v, ok
}
