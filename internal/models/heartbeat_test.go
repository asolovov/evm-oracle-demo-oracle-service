package models

import (
	"testing"
	"time"
)

func TestHeartbeatSchedule_DeviationRatio(t *testing.T) {
	cases := []struct {
		bps  uint32
		want float64
	}{
		{0, 0.0},
		{1, 0.0001},
		{150, 0.015},
		{10000, 1.0},
	}
	for _, c := range cases {
		hs := HeartbeatSchedule{DeviationBps: c.bps}
		got := hs.DeviationRatio()
		if got != c.want {
			t.Fatalf("DeviationRatio(%d bps) = %f, want %f", c.bps, got, c.want)
		}
	}
}

func TestHeartbeatSchedule_FieldsRoundTripVia(t *testing.T) {
	// Smoke test that the aggregate struct holds the values we expect.
	now := time.Now().UTC()
	hs := HeartbeatSchedule{
		AssetID:      "weth",
		Interval:     time.Hour,
		DeviationBps: 150,
		UpdatedAt:    now,
	}
	if hs.Interval != time.Hour || hs.DeviationBps != 150 || hs.UpdatedAt != now {
		t.Fatalf("unexpected: %+v", hs)
	}
}
