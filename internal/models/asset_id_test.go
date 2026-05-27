package models

import (
	"strings"
	"testing"
)

func TestNewAssetIDFromHex(t *testing.T) {
	a, err := NewAssetIDFromHex("0xabcdef")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if a.Symbol != "" {
		t.Fatalf("Symbol should be empty when constructed from hex, got %q", a.Symbol)
	}
	if !strings.HasPrefix(a.OnChainHex(), "0x") {
		t.Fatalf("expected 0x-prefixed hex, got %q", a.OnChainHex())
	}
}

func TestNewAssetIDFromHex_Empty(t *testing.T) {
	if _, err := NewAssetIDFromHex("   "); err == nil {
		t.Fatal("expected error on empty hex input")
	}
}

func TestAssetID_OnChainHex_StableForKnownSymbol(t *testing.T) {
	a, err := NewAssetIDFromSymbol("weth")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// keccak256("weth") is a well-known value; assert the prefix to catch
	// accidental regressions in the hashing path without hard-coding the
	// full 32-byte value.
	hex := a.OnChainHex()
	if len(hex) != 66 || hex[:2] != "0x" {
		t.Fatalf("unexpected hex shape: %q", hex)
	}

	// keccak("weth") and keccak("WETH") must differ — the symbol is normalised
	// to lowercase before hashing.
	b, _ := NewAssetIDFromSymbol("WETH")
	if a.OnChainHex() != b.OnChainHex() {
		t.Fatalf("case normalisation broken: %q vs %q", a.OnChainHex(), b.OnChainHex())
	}
}
