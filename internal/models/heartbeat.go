package models

import "time"

// HeartbeatSchedule is the persisted per-asset heartbeat configuration set
// via OracleService.SetHeartbeat. Internal scheduler reads this on tick.
//
// Interval == 0 disables the time-based path; DeviationBps == 0 disables the
// deviation-based path; both 0 disables the heartbeat entirely for the asset.
type HeartbeatSchedule struct {
	AssetID       string
	Interval      time.Duration
	DeviationBps  uint32 // 1 bp = 0.01%
	UpdatedAt     time.Time
}

// DeviationRatio returns DeviationBps expressed as a float ratio (e.g. 150 bp
// -> 0.015), matching the float-based comparison used in the submitter's
// deviation check.
func (h HeartbeatSchedule) DeviationRatio() float64 {
	return float64(h.DeviationBps) / 10000.0
}
