package models

import (
	"math"
	"math/big"
	"testing"
)

func TestFloatToInt256_KnownFixtures(t *testing.T) {
	tests := []struct {
		name     string
		price    float64
		decimals int
		want     string
	}{
		{"WETH 3450.20 @ 8 dec", 3450.20, 8, "345020000000"},
		{"WBTC 65000 @ 8 dec", 65000.0, 8, "6500000000000"},
		{"sub-dollar (LINK 14.5) @ 8 dec", 14.5, 8, "1450000000"},
		{"sub-cent precision (1.23456789) @ 8 dec", 1.23456789, 8, "123456789"},
		{"zero", 0.0, 8, "0"},
		{"round-half-even (0.5 ULP @ 0 decimals -> 0)", 0.5, 0, "0"},
		{"round-half-even (1.5 ULP @ 0 decimals -> 2)", 1.5, 0, "2"},
		{"round-half-even (2.5 ULP @ 0 decimals -> 2)", 2.5, 0, "2"},
		{"negative price (WTI -5.0)", -5.0, 8, "-500000000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FloatToInt256(tt.price, tt.decimals)
			if err != nil {
				t.Fatalf("FloatToInt256(%v, %d) error: %v", tt.price, tt.decimals, err)
			}
			if got.String() != tt.want {
				t.Fatalf("FloatToInt256(%v, %d) = %s, want %s", tt.price, tt.decimals, got.String(), tt.want)
			}
		})
	}
}

func TestFloatToInt256_RejectsNonFinite(t *testing.T) {
	cases := []float64{math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, p := range cases {
		if _, err := FloatToInt256(p, 8); err == nil {
			t.Fatalf("expected error for non-finite input %v", p)
		}
	}
}

func TestFloatToInt256_RejectsBadDecimals(t *testing.T) {
	if _, err := FloatToInt256(1.0, -1); err == nil {
		t.Fatal("expected error for negative decimals")
	}
	if _, err := FloatToInt256(1.0, 19); err == nil {
		t.Fatal("expected error for >18 decimals")
	}
}

func TestInt256ToFloat_RoundTripTolerance(t *testing.T) {
	const decimals = 8
	cases := []float64{3450.20, 65000.0, 14.5, 1.23456789, 0.0, -5.0}
	for _, p := range cases {
		i, err := FloatToInt256(p, decimals)
		if err != nil {
			t.Fatalf("forward: %v", err)
		}
		back, err := Int256ToFloat(i, decimals)
		if err != nil {
			t.Fatalf("reverse: %v", err)
		}
		// Allowed error is one ULP at the target precision.
		eps := 1.0 / math.Pow10(decimals)
		if math.Abs(back-p) > eps {
			t.Fatalf("round-trip %v -> %s -> %v exceeds %v", p, i.String(), back, eps)
		}
	}
}

func TestSubmissionStatus_RoundTrip(t *testing.T) {
	for _, s := range []SubmissionStatus{
		SubmissionStatusPending, SubmissionStatusConfirmed,
		SubmissionStatusFailed, SubmissionStatusDropped,
	} {
		got, err := ParseSubmissionStatus(s.String())
		if err != nil {
			t.Fatalf("ParseSubmissionStatus(%q): %v", s.String(), err)
		}
		if got != s {
			t.Fatalf("round-trip mismatch: %v -> %q -> %v", s, s.String(), got)
		}
	}
}

func TestSubmissionStatus_UnknownNameErrors(t *testing.T) {
	if _, err := ParseSubmissionStatus("nope"); err == nil {
		t.Fatal("expected error on unknown status name")
	}
}

func TestReqIDToBigInt(t *testing.T) {
	v, ok := ReqIDToBigInt("12345")
	if !ok || v.Cmp(big.NewInt(12345)) != 0 {
		t.Fatalf("unexpected: %v %v", v, ok)
	}
	if _, ok := ReqIDToBigInt("not-a-number"); ok {
		t.Fatal("expected parse failure on garbage")
	}
}

func TestNewAssetIDFromSymbol(t *testing.T) {
	a, err := NewAssetIDFromSymbol("WETH")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if a.Symbol != "weth" {
		t.Fatalf("symbol normalized to %q, want %q", a.Symbol, "weth")
	}
	if a.OnChain.Hex() == "" {
		t.Fatal("on-chain hash empty")
	}
}

func TestNewAssetIDFromSymbol_Empty(t *testing.T) {
	if _, err := NewAssetIDFromSymbol("  "); err == nil {
		t.Fatal("expected ErrAssetIDEmpty")
	}
}
