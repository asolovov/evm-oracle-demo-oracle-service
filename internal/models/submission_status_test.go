package models

import (
	"testing"

	oraclev1 "github.com/asolovov/evm-oracle-demo-oracle-service/internal/genproto/oracle/v1"
)

func TestSubmissionStatus_String_Unknown(t *testing.T) {
	got := SubmissionStatus(99).String()
	if got != "unknown" {
		t.Fatalf("out-of-range status should stringify to %q, got %q", "unknown", got)
	}
}

func TestSubmissionStatus_IsValid(t *testing.T) {
	cases := []struct {
		s    SubmissionStatus
		want bool
	}{
		{SubmissionStatusUnknown, false},
		{SubmissionStatusPending, true},
		{SubmissionStatusConfirmed, true},
		{SubmissionStatusFailed, true},
		{SubmissionStatusDropped, true},
		{SubmissionStatus(99), false},
	}
	for _, c := range cases {
		if c.s.IsValid() != c.want {
			t.Fatalf("IsValid(%v) = %v, want %v", c.s, c.s.IsValid(), c.want)
		}
	}
}

func TestSubmissionStatus_ToProto(t *testing.T) {
	cases := []struct {
		in   SubmissionStatus
		want oraclev1.SubmissionStatus_Status
	}{
		{SubmissionStatusPending, oraclev1.SubmissionStatus_STATUS_PENDING},
		{SubmissionStatusConfirmed, oraclev1.SubmissionStatus_STATUS_CONFIRMED},
		{SubmissionStatusFailed, oraclev1.SubmissionStatus_STATUS_FAILED},
		{SubmissionStatusDropped, oraclev1.SubmissionStatus_STATUS_DROPPED},
		{SubmissionStatusExpired, oraclev1.SubmissionStatus_STATUS_EXPIRED},
		// Internal-only pipeline statuses surface as PENDING on the wire.
		{SubmissionStatusQueued, oraclev1.SubmissionStatus_STATUS_PENDING},
		{SubmissionStatusProcessing, oraclev1.SubmissionStatus_STATUS_PENDING},
		{SubmissionStatusSending, oraclev1.SubmissionStatus_STATUS_PENDING},
		{SubmissionStatusUnknown, oraclev1.SubmissionStatus_STATUS_UNSPECIFIED},
		{SubmissionStatus(99), oraclev1.SubmissionStatus_STATUS_UNSPECIFIED},
	}
	for _, c := range cases {
		if got := c.in.ToProto(); got != c.want {
			t.Fatalf("ToProto(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestSubmissionStatusFromProto(t *testing.T) {
	cases := []struct {
		in   oraclev1.SubmissionStatus_Status
		want SubmissionStatus
	}{
		{oraclev1.SubmissionStatus_STATUS_PENDING, SubmissionStatusPending},
		{oraclev1.SubmissionStatus_STATUS_CONFIRMED, SubmissionStatusConfirmed},
		{oraclev1.SubmissionStatus_STATUS_FAILED, SubmissionStatusFailed},
		{oraclev1.SubmissionStatus_STATUS_DROPPED, SubmissionStatusDropped},
		{oraclev1.SubmissionStatus_STATUS_EXPIRED, SubmissionStatusExpired},
		{oraclev1.SubmissionStatus_STATUS_UNSPECIFIED, SubmissionStatusUnknown},
	}
	for _, c := range cases {
		if got := SubmissionStatusFromProto(c.in); got != c.want {
			t.Fatalf("FromProto(%v) = %v, want %v", c.in, got, c.want)
		}
	}
	// Unknown proto values map to Unknown.
	if got := SubmissionStatusFromProto(oraclev1.SubmissionStatus_Status(99)); got != SubmissionStatusUnknown {
		t.Fatalf("unknown proto value should map to Unknown, got %v", got)
	}
}
