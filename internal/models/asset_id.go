package models

import (
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// AssetID is the canonical asset identifier in two forms used across the
// project:
//
//   - Off-chain form: lowercase symbol ("weth", "xau", "spx", ...) — what
//     price-service uses and what the dashboard displays.
//   - On-chain form: bytes32 = keccak256(symbol) — what the contracts and
//     the chain events use.
//
// These helpers convert between the two without scattering the choice across
// services (rule 3).
type AssetID struct {
	Symbol string      // canonical lowercase ("weth")
	OnChain common.Hash // keccak256(symbol)
}

// ErrAssetIDEmpty signals a nil/empty symbol passed in by mistake.
var ErrAssetIDEmpty = errors.New("asset id is empty")

// NewAssetIDFromSymbol builds an AssetID from a human-typed symbol of any case.
func NewAssetIDFromSymbol(symbol string) (AssetID, error) {
	sym := strings.ToLower(strings.TrimSpace(symbol))
	if sym == "" {
		return AssetID{}, ErrAssetIDEmpty
	}
	hash := crypto.Keccak256Hash([]byte(sym))
	return AssetID{Symbol: sym, OnChain: hash}, nil
}

// NewAssetIDFromHex builds an AssetID when only the on-chain bytes32 is known
// (e.g. parsing a chain event). The Symbol field is left empty — callers that
// need the symbol must look it up against their aggregator registry.
func NewAssetIDFromHex(hex string) (AssetID, error) {
	if strings.TrimSpace(hex) == "" {
		return AssetID{}, ErrAssetIDEmpty
	}
	return AssetID{OnChain: common.HexToHash(hex)}, nil
}

// OnChainHex returns the 0x-prefixed lowercase hex of the bytes32 id.
func (a AssetID) OnChainHex() string { return a.OnChain.Hex() }
