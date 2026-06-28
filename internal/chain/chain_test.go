package chain

import (
	"errors"
	"fmt"
	"math/big"
	"testing"
)

func TestIsRevertError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// Permanent — return TRUE so submitter persists FAILED + advances cursor.
		{"go-ethereum sim revert", errors.New("execution reverted"), true},
		{"wrapped sim revert", fmt.Errorf("fulfillPrice: %w", errors.New("execution reverted")), true},
		{"reverted with reason", errors.New("execution reverted: NoRoundData"), true},
		{"reverted past tense", errors.New("the transaction reverted"), true},
		{"mixed case", errors.New("Execution REVERTED"), true},
		{"invalid opcode (legacy revert proxy)", errors.New("invalid opcode: STOP"), true},

		// Transient — return FALSE so submitter retries.
		{"insufficient funds", errors.New("insufficient funds for transfer"), false},
		{"connection refused", errors.New("dial tcp: connection refused"), false},
		{"context deadline", errors.New("context deadline exceeded"), false},
		{"nonce too low", errors.New("nonce too low"), false},

		// Edge.
		{"nil", nil, false},
		{"empty string", errors.New(""), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRevertError(c.err); got != c.want {
				t.Fatalf("IsRevertError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestIsInsufficientFundsError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// Funds — broadcast-time (node rejects signed tx).
		{"core ErrInsufficientFunds", errors.New("insufficient funds for gas * price + value"), true},
		{"insufficient funds for transfer", errors.New("insufficient funds for transfer"), true},
		{"wrapped funds", fmt.Errorf("broadcast fulfillPrice: %w", errors.New("insufficient funds for gas * price + value")), true},
		// Funds — client-side estimateGas ceiling collapse on a drained wallet.
		{"gas required exceeds allowance", errors.New("gas required exceeds allowance (7800)"), true},
		{"mixed case", errors.New("Insufficient Funds for gas"), true},

		// NOT funds — must stay out of the funds class.
		{"plain revert", errors.New("execution reverted"), false},
		{"contract InsufficientFee custom error", errors.New("execution reverted: InsufficientFee"), false},
		{"contract InsufficientSignatures", errors.New("execution reverted: InsufficientSignatures"), false},
		{"connection refused", errors.New("dial tcp: connection refused"), false},
		{"nonce too low", errors.New("nonce too low"), false},

		// Edge.
		{"nil", nil, false},
		{"empty", errors.New(""), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsInsufficientFundsError(c.err); got != c.want {
				t.Fatalf("IsInsufficientFundsError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestIsGasAllowanceError(t *testing.T) {
	if !IsGasAllowanceError(errors.New("gas required exceeds allowance (7800)")) {
		t.Fatal("should match the estimator-ceiling string")
	}
	// The authoritative node funds rejection is NOT the allowance string.
	if IsGasAllowanceError(errors.New("insufficient funds for transfer")) {
		t.Fatal("node insufficient-funds is not the allowance error")
	}
	if IsGasAllowanceError(errors.New("execution reverted")) || IsGasAllowanceError(nil) {
		t.Fatal("revert/nil must not match")
	}
}

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
		// 1.21 = 1.1^2; we just verify the per-call behavior here.
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
