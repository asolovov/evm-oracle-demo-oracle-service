package chain

import (
	"math/big"
	"testing"
)

func TestScaleBigInt(t *testing.T) {
	cases := []struct {
		name string
		in   int64
		mult float64
		want int64
	}{
		{"unit", 100, 1.0, 100},
		{"+10%", 100, 1.1, 110},
		{"+50%", 200, 1.5, 300},
		{"big number +10%", 1_000_000_000, 1.1, 1_100_000_000},
		// 1.21 = 1.1^2; we just verify the per-call behaviour here.
		{"1.21x via callsite math", 100, 1.21, 121},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := scaleBigInt(big.NewInt(tt.in), tt.mult)
			if got.Int64() != tt.want {
				t.Fatalf("scaleBigInt(%d, %v) = %d, want %d", tt.in, tt.mult, got.Int64(), tt.want)
			}
		})
	}
}

func TestScaleBigInt_PreservesInput(t *testing.T) {
	in := big.NewInt(42)
	_ = scaleBigInt(in, 1.5)
	if in.Int64() != 42 {
		t.Fatalf("scaleBigInt mutated input to %d", in.Int64())
	}
}
