// Package signer loads reporter EOA keys and produces the EIP-712 signatures
// the on-chain PriceAggregator verifies in fulfillPrice.
//
// This is a plain Go package (architecture rule 5 — chain client + signer are
// external-system handlers, not template modules with Init/Start/Stop).
//
// Digest format mirrors src/libs/PriceLib.sol::buildDigest exactly:
//
//	DOMAIN_NAME    = "LIGHTHOUSE_V1"
//	DOMAIN_VERSION = "1"
//	DOMAIN_TYPEHASH = keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
//	PRICE_TYPEHASH  = keccak256("Price(uint256 reqId,bytes32 assetId,int256 price,uint256 timestamp)")
//	domainSeparator = keccak256(abi.encode(DOMAIN_TYPEHASH, keccak256(DOMAIN_NAME), keccak256(DOMAIN_VERSION), chainId, aggregator))
//	structHash      = keccak256(abi.encode(PRICE_TYPEHASH, reqId, assetId, price, timestamp))
//	digest          = keccak256(0x1901 || domainSeparator || structHash)
//
// Reporter keys are loaded from disk at startup. Permissions are enforced
// fail-fast (0644+ rejected) unless config.signer.allow_insecure_perms is
// true — the dev-only escape hatch that production must keep false.
package signer

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
)

// ErrInsecureKeyPerms is returned when a reporter key file has perms that
// would allow other users to read it. Production must never tolerate this;
// see config.SignerConfig.AllowInsecurePerms for the dev override.
var ErrInsecureKeyPerms = errors.New("reporter key file has insecure perms")

// ErrAddressMismatch is returned when a loaded key derives to a different
// address than the operator-supplied checklist (config.signer.reporter_addresses).
var ErrAddressMismatch = errors.New("reporter key derives to unexpected address")

// Reporter is one reporter EOA loaded from disk.
type Reporter struct {
	Address    common.Address
	privateKey *ecdsa.PrivateKey
}

// Signer holds all loaded reporter keys + the configured M-of-N threshold.
type Signer struct {
	reporters []Reporter
	threshold int
	chainID   *big.Int
}

// LoadFromConfig reads every key file listed in config.Signer.ReporterKeyPaths,
// optionally validates each against config.Signer.ReporterAddresses (when set),
// and returns a Signer ready to produce digest signatures.
//
// Caller is expected to have already run config.Validate(); we still
// defensively check the threshold and path slice here so misconfiguration
// surfaces close to the offending dependency.
func LoadFromConfig(cfg *config.SignerConfig, chainID uint64) (*Signer, error) {
	if cfg == nil {
		return nil, errors.New("nil signer config")
	}
	if len(cfg.ReporterKeyPaths) == 0 {
		return nil, errors.New("no reporter key paths configured")
	}
	if cfg.Threshold <= 0 || cfg.Threshold > len(cfg.ReporterKeyPaths) {
		return nil, fmt.Errorf("threshold %d invalid for %d keys", cfg.Threshold, len(cfg.ReporterKeyPaths))
	}

	reps := make([]Reporter, 0, len(cfg.ReporterKeyPaths))
	for i, p := range cfg.ReporterKeyPaths {
		r, err := loadReporter(p, cfg.AllowInsecurePerms)
		if err != nil {
			return nil, fmt.Errorf("load reporter %d (%s): %w", i+1, p, err)
		}
		if len(cfg.ReporterAddresses) > 0 {
			want := common.HexToAddress(cfg.ReporterAddresses[i])
			if r.Address != want {
				return nil, fmt.Errorf("%w: index %d, got %s, want %s",
					ErrAddressMismatch, i+1, r.Address.Hex(), want.Hex())
			}
		}
		reps = append(reps, r)
	}

	return &Signer{
		reporters: reps,
		threshold: cfg.Threshold,
		//nolint:gosec // chain id from config; uint64 -> *big.Int is safe
		chainID: new(big.Int).SetUint64(chainID),
	}, nil
}

// Reporters returns a snapshot of the loaded reporter addresses. The
// underlying ECDSA keys never leave this package.
func (s *Signer) Reporters() []common.Address {
	out := make([]common.Address, len(s.reporters))
	for i, r := range s.reporters {
		out[i] = r.Address
	}
	return out
}

// Threshold returns the configured M-of-N threshold.
func (s *Signer) Threshold() int { return s.threshold }

// BroadcasterAddress is the EOA the chain client uses to broadcast
// fulfillPrice transactions. The contracts do not check msg.sender (the
// digest signature set is the sole authorisation), so we reuse the first
// reporter key for broadcasting. This keeps the demo at three funded
// wallets instead of four.
func (s *Signer) BroadcasterAddress() common.Address {
	if len(s.reporters) == 0 {
		return common.Address{}
	}
	return s.reporters[0].Address
}

// NewBroadcaster returns a *bind.TransactOpts configured for the first
// reporter key. Callers are expected to set GasTipCap / GasFeeCap / Nonce
// before broadcasting; this helper only seals the from-address + signer.
//
// (Imported above lazily because TransactOpts lives in accounts/abi/bind.)
func (s *Signer) NewBroadcaster() (*bind.TransactOpts, error) {
	if len(s.reporters) == 0 {
		return nil, errors.New("no reporter keys loaded; cannot build broadcaster")
	}
	opts, err := bind.NewKeyedTransactorWithChainID(s.reporters[0].privateKey, s.chainID)
	if err != nil {
		return nil, fmt.Errorf("build keyed transactor: %w", err)
	}
	return opts, nil
}

// BuildDigest mirrors PriceLib.buildDigest. Exposed so the submitter can sign
// the same digest that the on-chain verifier will recompute.
//
// assetID must be the on-chain bytes32 form (keccak256(symbol)) — not the
// off-chain lowercase string. Use models.AssetID to convert.
func (s *Signer) BuildDigest(
	reqID *big.Int,
	assetID common.Hash,
	price *big.Int,
	timestamp *big.Int,
	aggregator common.Address,
) ([]byte, error) {
	domainSep, err := domainSeparator(s.chainID, aggregator)
	if err != nil {
		return nil, fmt.Errorf("domain separator: %w", err)
	}
	structHash, err := priceStructHash(reqID, assetID, price, timestamp)
	if err != nil {
		return nil, fmt.Errorf("struct hash: %w", err)
	}

	// EIP-712 toTypedDataHash: keccak256(0x1901 || domainSep || structHash).
	out := make([]byte, 2, 2+32+32)
	out[0] = 0x19
	out[1] = 0x01
	out = append(out, domainSep...)
	out = append(out, structHash...)
	return crypto.Keccak256(out), nil
}

// Sign produces one signature per loaded reporter over the supplied digest.
// Signatures are 65 bytes (r || s || v) with v normalised to {27, 28} to
// match OpenZeppelin's ECDSA.recover expectation.
//
// Caller decides quorum: pass all signatures to fulfillPrice; the on-chain
// verifier counts distinct authorised signers and requires >= threshold.
func (s *Signer) Sign(digest []byte) ([][]byte, error) {
	if len(digest) != 32 {
		return nil, fmt.Errorf("digest must be 32 bytes, got %d", len(digest))
	}
	sigs := make([][]byte, 0, len(s.reporters))
	for _, r := range s.reporters {
		sig, err := crypto.Sign(digest, r.privateKey)
		if err != nil {
			return nil, fmt.Errorf("sign with %s: %w", r.Address.Hex(), err)
		}
		// crypto.Sign emits v in {0,1}; OZ ECDSA.recover expects {27,28}.
		sig[64] += 27
		sigs = append(sigs, sig)
	}
	return sigs, nil
}

// loadReporter reads a key file from disk, enforces perms, and decodes the
// ECDSA private key. Two file formats are accepted:
//
//   - Raw hex (legacy abigen format) — first non-empty line is 64 hex chars
//     (optionally 0x-prefixed).
//   - JSON keystore — { "privateKey": "0x..." }. Compatible with the
//     `.reporters/reporter{1,2,3}.json` shape generated by the contracts
//     repo's generateReporters.ts.
func loadReporter(path string, allowInsecurePerms bool) (Reporter, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Reporter{}, fmt.Errorf("stat: %w", err)
	}
	if !allowInsecurePerms {
		// Reject if group or world has any permission. 0600 is the only safe mode.
		mode := info.Mode().Perm()
		if mode&0o077 != 0 {
			return Reporter{}, fmt.Errorf("%w: %s has mode %#o (require 0600)",
				ErrInsecureKeyPerms, path, mode)
		}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return Reporter{}, fmt.Errorf("read: %w", err)
	}

	keyHex, err := extractPrivateKeyHex(raw)
	if err != nil {
		return Reporter{}, err
	}
	keyHex = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(keyHex)), "0x")

	pk, err := crypto.HexToECDSA(keyHex)
	if err != nil {
		return Reporter{}, fmt.Errorf("decode key: %w", err)
	}

	return Reporter{
		Address:    crypto.PubkeyToAddress(pk.PublicKey),
		privateKey: pk,
	}, nil
}

// extractPrivateKeyHex pulls the private key hex out of either format.
// Kept tiny on purpose; we don't try to support every keystore variant.
func extractPrivateKeyHex(raw []byte) (string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "", errors.New("empty key file")
	}

	// JSON keystore variant.
	if strings.HasPrefix(trimmed, "{") {
		// Hand-roll the parse to avoid pulling in encoding/json for this one shape.
		// Look for "privateKey": "...".
		const marker = "\"privateKey\""
		idx := strings.Index(trimmed, marker)
		if idx < 0 {
			return "", errors.New("keystore JSON missing privateKey field")
		}
		rest := trimmed[idx+len(marker):]
		// Skip whitespace + colon.
		rest = strings.TrimLeft(rest, " \t\r\n:")
		if !strings.HasPrefix(rest, "\"") {
			return "", errors.New("keystore JSON: malformed privateKey value")
		}
		rest = rest[1:]
		end := strings.IndexByte(rest, '"')
		if end < 0 {
			return "", errors.New("keystore JSON: unterminated privateKey value")
		}
		return rest[:end], nil
	}

	// Raw-hex variant: use the first non-empty line.
	for _, line := range strings.Split(trimmed, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		return line, nil
	}
	return "", errors.New("no key content found")
}

// ---------------------------------------------------------------------------
// EIP-712 helpers
// ---------------------------------------------------------------------------

var (
	domainTypeHash = crypto.Keccak256Hash([]byte(
		"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
	))
	priceTypeHash = crypto.Keccak256Hash([]byte(
		"Price(uint256 reqId,bytes32 assetId,int256 price,uint256 timestamp)",
	))
	nameHash    = crypto.Keccak256Hash([]byte("LIGHTHOUSE_V1"))
	versionHash = crypto.Keccak256Hash([]byte("1"))
)

func domainSeparator(chainID *big.Int, verifyingContract common.Address) ([]byte, error) {
	args := abi.Arguments{
		{Type: mustABIType("bytes32")},
		{Type: mustABIType("bytes32")},
		{Type: mustABIType("bytes32")},
		{Type: mustABIType("uint256")},
		{Type: mustABIType("address")},
	}
	enc, err := args.Pack(
		[32]byte(domainTypeHash),
		[32]byte(nameHash),
		[32]byte(versionHash),
		chainID,
		verifyingContract,
	)
	if err != nil {
		return nil, err
	}
	return crypto.Keccak256(enc), nil
}

func priceStructHash(reqID *big.Int, assetID common.Hash, price, timestamp *big.Int) ([]byte, error) {
	args := abi.Arguments{
		{Type: mustABIType("bytes32")},
		{Type: mustABIType("uint256")},
		{Type: mustABIType("bytes32")},
		{Type: mustABIType("int256")},
		{Type: mustABIType("uint256")},
	}
	enc, err := args.Pack(
		[32]byte(priceTypeHash),
		reqID,
		[32]byte(assetID),
		price,
		timestamp,
	)
	if err != nil {
		return nil, err
	}
	return crypto.Keccak256(enc), nil
}

func mustABIType(name string) abi.Type {
	t, err := abi.NewType(name, "", nil)
	if err != nil {
		panic(fmt.Sprintf("abi.NewType(%q): %v", name, err))
	}
	return t
}
