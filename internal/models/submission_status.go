// Package models holds the oracle-service domain types and all proto/DB
// conversions, per architecture rule 3.
package models

import (
	"fmt"

	oraclev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/oracle/v1"
)

// SubmissionStatus is the int-backed enum mirror of the on-chain submission
// lifecycle. Mirror shape follows the template's user_status.go example.
type SubmissionStatus int

const (
	// SubmissionStatusUnknown is the zero value; surfaces as STATUS_UNSPECIFIED on the wire.
	SubmissionStatusUnknown SubmissionStatus = iota
	// SubmissionStatusPending — built/signed (and possibly broadcast) but not yet mined.
	SubmissionStatusPending
	// SubmissionStatusConfirmed — mined with status=1.
	SubmissionStatusConfirmed
	// SubmissionStatusFailed — mined-and-reverted, or rejected pre-broadcast (nonce/sig).
	SubmissionStatusFailed
	// SubmissionStatusDropped — fell out of the mempool after MaxRetries replacements.
	SubmissionStatusDropped
	// SubmissionStatusQueued — durably enqueued by the stream consumer, not yet
	// claimed by a worker. Internal-only, no wire counterpart (task 06.1).
	SubmissionStatusQueued
	// SubmissionStatusProcessing — a worker is fetching the price + signing.
	// Internal-only.
	SubmissionStatusProcessing
	// SubmissionStatusSending — signed and handed to the sender for broadcast.
	// Internal-only.
	SubmissionStatusSending
	// SubmissionStatusExpired — abandoned before broadcast because it could not
	// be fulfilled within its TTL (e.g. price never became available). Terminal.
	SubmissionStatusExpired
)

// submissionStatusUnknownName is the canonical string for an unrecognized
// status. Used both as the lookup-table value for SubmissionStatusUnknown
// and as the String() fallback for out-of-range receivers.
const submissionStatusUnknownName = "unknown"

// submissionStatusNames is the symmetric String/FromString lookup table.
var submissionStatusNames = map[SubmissionStatus]string{
	SubmissionStatusUnknown:    submissionStatusUnknownName,
	SubmissionStatusPending:    "pending",
	SubmissionStatusConfirmed:  "confirmed",
	SubmissionStatusFailed:     "failed",
	SubmissionStatusDropped:    "dropped",
	SubmissionStatusQueued:     "queued",
	SubmissionStatusProcessing: "processing",
	SubmissionStatusSending:    "sending",
	SubmissionStatusExpired:    "expired",
}

// submissionStatusValues is the reverse lookup.
var submissionStatusValues = func() map[string]SubmissionStatus {
	m := make(map[string]SubmissionStatus, len(submissionStatusNames))
	for k, v := range submissionStatusNames {
		m[v] = k
	}
	return m
}()

// String returns the canonical lowercase name. Round-trip with ParseSubmissionStatus.
func (s SubmissionStatus) String() string {
	if name, ok := submissionStatusNames[s]; ok {
		return name
	}
	return submissionStatusUnknownName
}

// IsValid reports whether s is a defined non-unknown variant.
func (s SubmissionStatus) IsValid() bool {
	_, ok := submissionStatusNames[s]
	return ok && s != SubmissionStatusUnknown
}

// ParseSubmissionStatus is the symmetric inverse of String. Unknown names map
// to SubmissionStatusUnknown with an error so callers can fail-fast on DB
// corruption rather than silently drift.
func ParseSubmissionStatus(s string) (SubmissionStatus, error) {
	if v, ok := submissionStatusValues[s]; ok {
		return v, nil
	}
	return SubmissionStatusUnknown, fmt.Errorf("unknown submission status: %q", s)
}

// ToProto converts to the oracle.v1.SubmissionStatus_Status enum.
//
// The async-pipeline internal statuses (queued/processing/sending) have no
// distinct wire value; external consumers see them as STATUS_PENDING ("in
// flight"). `expired` maps to its own STATUS_EXPIRED (added in protocols for
// task 06.1) — distinct from STATUS_FAILED: nothing was broadcast on-chain.
func (s SubmissionStatus) ToProto() oraclev1.SubmissionStatus_Status {
	switch s {
	case SubmissionStatusPending, SubmissionStatusQueued, SubmissionStatusProcessing, SubmissionStatusSending:
		return oraclev1.SubmissionStatus_STATUS_PENDING
	case SubmissionStatusConfirmed:
		return oraclev1.SubmissionStatus_STATUS_CONFIRMED
	case SubmissionStatusFailed:
		return oraclev1.SubmissionStatus_STATUS_FAILED
	case SubmissionStatusExpired:
		return oraclev1.SubmissionStatus_STATUS_EXPIRED
	case SubmissionStatusDropped:
		return oraclev1.SubmissionStatus_STATUS_DROPPED
	case SubmissionStatusUnknown:
		return oraclev1.SubmissionStatus_STATUS_UNSPECIFIED
	default:
		return oraclev1.SubmissionStatus_STATUS_UNSPECIFIED
	}
}

// SubmissionStatusFromProto is the inverse of ToProto.
func SubmissionStatusFromProto(p oraclev1.SubmissionStatus_Status) SubmissionStatus {
	switch p {
	case oraclev1.SubmissionStatus_STATUS_PENDING:
		return SubmissionStatusPending
	case oraclev1.SubmissionStatus_STATUS_CONFIRMED:
		return SubmissionStatusConfirmed
	case oraclev1.SubmissionStatus_STATUS_FAILED:
		return SubmissionStatusFailed
	case oraclev1.SubmissionStatus_STATUS_DROPPED:
		return SubmissionStatusDropped
	case oraclev1.SubmissionStatus_STATUS_EXPIRED:
		return SubmissionStatusExpired
	case oraclev1.SubmissionStatus_STATUS_UNSPECIFIED:
		return SubmissionStatusUnknown
	default:
		return SubmissionStatusUnknown
	}
}
