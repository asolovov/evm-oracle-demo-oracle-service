package models

import (
	"errors"
	"fmt"
	"math"
	"math/big"
)

// MaxInt256 mirrors Solidity's `type(int256).max` for overflow checks.
var MaxInt256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(1))

// MinInt256 mirrors Solidity's `type(int256).min`.
var MinInt256 = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 255))

// ErrConversionOverflow is returned when a float would not fit into int256
// after scaling — guards against a corrupted upstream feed pushing the value
// over 2^255-1.
var ErrConversionOverflow = errors.New("price conversion overflow: value does not fit in int256")

// ErrConversionNotFinite is returned when the input is NaN or +/-Inf.
var ErrConversionNotFinite = errors.New("price conversion input is not finite")

// FloatToInt256 converts an IEEE-754 double USD price into the Chainlink-style
// int256 representation at `decimals` precision, rounding to nearest even.
//
// The off-chain pipeline uses doubles end-to-end (spec OQ-11); this is the
// SINGLE place double->int256 conversion happens. Any other service that
// wants this should call here.
//
// Round-half-even is chosen to match IEEE-754 default rounding mode and
// avoid systematic upward bias under repeated rounding. Implementation:
// scale the float, then math.RoundToEven, then promote into big.Int.
// Precision is float64's 53-bit mantissa; for $90M-and-under prices the
// representation is exact at 8 decimals (well above the asset universe).
func FloatToInt256(price float64, decimals int) (*big.Int, error) {
	if math.IsNaN(price) || math.IsInf(price, 0) {
		return nil, ErrConversionNotFinite
	}
	if decimals < 0 || decimals > 18 {
		return nil, fmt.Errorf("decimals out of range (0, 18]: %d", decimals)
	}

	scaled := price * math.Pow10(decimals)
	if math.IsInf(scaled, 0) {
		return nil, ErrConversionOverflow
	}
	rounded := math.RoundToEven(scaled)

	// Promote via big.Float so we never silently clamp to int64 bounds.
	bf := new(big.Float).SetPrec(256).SetFloat64(rounded)
	result, _ := bf.Int(nil)

	if result.Cmp(MaxInt256) > 0 || result.Cmp(MinInt256) < 0 {
		return nil, ErrConversionOverflow
	}
	return result, nil
}

// Int256ToFloat is the reverse direction, used by the heartbeat deviation
// check after reading the last on-chain price via latestRoundData.
//
// Precision is lossy for very large int256 values (> 2^53 distinct integers),
// but on Chainlink's 8-decimal scale a USD price of ~$1e23 would still fit
// in float64's range and the relative error is bounded by float64 eps.
func Int256ToFloat(value *big.Int, decimals int) (float64, error) {
	if value == nil {
		return 0, errors.New("nil value")
	}
	if decimals < 0 || decimals > 18 {
		return 0, fmt.Errorf("decimals out of range (0, 18]: %d", decimals)
	}
	bf := new(big.Float).SetPrec(256).SetInt(value)
	scale := new(big.Float).SetPrec(256).SetInt(pow10(decimals))
	out := new(big.Float).SetPrec(256).Quo(bf, scale)
	f, _ := out.Float64()
	return f, nil
}

// pow10 returns 10^n as a *big.Int. Memoised at package scope to keep the hot
// path allocation-free.
func pow10(n int) *big.Int {
	if n < len(pow10Table) {
		return new(big.Int).Set(pow10Table[n])
	}
	out := new(big.Int).Set(pow10Table[len(pow10Table)-1])
	for i := len(pow10Table) - 1; i < n; i++ {
		out.Mul(out, big.NewInt(10))
	}
	return out
}

var pow10Table = func() []*big.Int {
	t := make([]*big.Int, 19)
	t[0] = big.NewInt(1)
	for i := 1; i < len(t); i++ {
		t[i] = new(big.Int).Mul(t[i-1], big.NewInt(10))
	}
	return t
}()
