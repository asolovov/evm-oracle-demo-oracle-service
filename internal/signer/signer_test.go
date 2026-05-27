package signer

import (
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/asolovov/evm-oracle-demo-oracle-service/config"
)

// writeKeyFile writes a hex-format key with 0600 perms (the default safe form).
func writeKeyFile(t *testing.T, dir string, name string, hex string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(hex), mode); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return path
}

func TestLoadFromConfig_HappyPath(t *testing.T) {
	pk1, _ := crypto.GenerateKey()
	pk2, _ := crypto.GenerateKey()
	pk3, _ := crypto.GenerateKey()

	dir := t.TempDir()
	p1 := writeKeyFile(t, dir, "r1.hex", "0x"+toHex(pk1), 0o600)
	p2 := writeKeyFile(t, dir, "r2.json", `{"privateKey":"0x`+toHex(pk2)+`","address":"0x0000"}`, 0o600)
	p3 := writeKeyFile(t, dir, "r3.hex", toHex(pk3), 0o600) // no 0x prefix

	cfg := &config.SignerConfig{
		ReporterKeyPaths: []string{p1, p2, p3},
		Threshold:        2,
	}
	s, err := LoadFromConfig(cfg, 11155111)
	if err != nil {
		t.Fatalf("LoadFromConfig: %v", err)
	}
	if got := len(s.Reporters()); got != 3 {
		t.Fatalf("expected 3 reporters, got %d", got)
	}
	if s.Threshold() != 2 {
		t.Fatalf("expected threshold 2, got %d", s.Threshold())
	}

	// Address checklist enforcement.
	want := []common.Address{
		crypto.PubkeyToAddress(pk1.PublicKey),
		crypto.PubkeyToAddress(pk2.PublicKey),
		crypto.PubkeyToAddress(pk3.PublicKey),
	}
	for i, got := range s.Reporters() {
		if got != want[i] {
			t.Fatalf("address %d mismatch: got %s, want %s", i, got, want[i])
		}
	}
}

func TestLoadFromConfig_AddressChecklistMismatch(t *testing.T) {
	pk1, _ := crypto.GenerateKey()
	dir := t.TempDir()
	p1 := writeKeyFile(t, dir, "r1.hex", "0x"+toHex(pk1), 0o600)

	cfg := &config.SignerConfig{
		ReporterKeyPaths:  []string{p1},
		ReporterAddresses: []string{"0x000000000000000000000000000000000000dead"},
		Threshold:         1,
	}
	_, err := LoadFromConfig(cfg, 1)
	if !errors.Is(err, ErrAddressMismatch) {
		t.Fatalf("expected ErrAddressMismatch, got %v", err)
	}
}

func TestLoadFromConfig_InsecurePerms(t *testing.T) {
	pk, _ := crypto.GenerateKey()
	dir := t.TempDir()
	p := writeKeyFile(t, dir, "r1.hex", "0x"+toHex(pk), 0o644)

	cfg := &config.SignerConfig{
		ReporterKeyPaths: []string{p},
		Threshold:        1,
	}
	_, err := LoadFromConfig(cfg, 1)
	if !errors.Is(err, ErrInsecureKeyPerms) {
		t.Fatalf("expected ErrInsecureKeyPerms on 0644, got %v", err)
	}

	cfg.AllowInsecurePerms = true
	if _, err := LoadFromConfig(cfg, 1); err != nil {
		t.Fatalf("override should allow load: %v", err)
	}
}

func TestSign_RecoverableByDigestVerifier(t *testing.T) {
	pk, _ := crypto.GenerateKey()
	dir := t.TempDir()
	p := writeKeyFile(t, dir, "r.hex", "0x"+toHex(pk), 0o600)

	cfg := &config.SignerConfig{
		ReporterKeyPaths: []string{p},
		Threshold:        1,
	}
	s, err := LoadFromConfig(cfg, 11155111)
	if err != nil {
		t.Fatalf("LoadFromConfig: %v", err)
	}

	reqID := big.NewInt(42)
	assetID := common.HexToHash("0xabcdef")
	price := big.NewInt(345020000000)
	ts := big.NewInt(1700000000)
	aggregator := common.HexToAddress("0x000000000000000000000000000000000000beef")

	digest, err := s.BuildDigest(reqID, assetID, price, ts, aggregator)
	if err != nil {
		t.Fatalf("BuildDigest: %v", err)
	}
	if len(digest) != 32 {
		t.Fatalf("digest len %d", len(digest))
	}

	sigs, err := s.Sign(digest)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("expected 1 sig, got %d", len(sigs))
	}

	// Recover the signer from the EIP-712 digest. We need to de-normalise
	// the OZ v (27/28) back to crypto's v (0/1) before calling Ecrecover.
	sig := append([]byte(nil), sigs[0]...)
	if sig[64] < 27 {
		t.Fatalf("v should be 27/28 after OZ normalisation, got %d", sig[64])
	}
	sig[64] -= 27

	pubKey, err := crypto.SigToPub(digest, sig)
	if err != nil {
		t.Fatalf("SigToPub: %v", err)
	}
	got := crypto.PubkeyToAddress(*pubKey)
	want := crypto.PubkeyToAddress(pk.PublicKey)
	if got != want {
		t.Fatalf("recovered %s, want %s", got, want)
	}
}

func TestBuildDigest_StableAcrossRuns(t *testing.T) {
	pk, _ := crypto.GenerateKey()
	dir := t.TempDir()
	p := writeKeyFile(t, dir, "r.hex", "0x"+toHex(pk), 0o600)
	cfg := &config.SignerConfig{ReporterKeyPaths: []string{p}, Threshold: 1}
	s, _ := LoadFromConfig(cfg, 11155111)

	d1, _ := s.BuildDigest(
		big.NewInt(1),
		common.HexToHash("0x01"),
		big.NewInt(100),
		big.NewInt(1700000000),
		common.HexToAddress("0xbeef"),
	)
	d2, _ := s.BuildDigest(
		big.NewInt(1),
		common.HexToHash("0x01"),
		big.NewInt(100),
		big.NewInt(1700000000),
		common.HexToAddress("0xbeef"),
	)
	if string(d1) != string(d2) {
		t.Fatal("digest non-deterministic")
	}
}

func TestSign_RejectsBadDigestLen(t *testing.T) {
	pk, _ := crypto.GenerateKey()
	dir := t.TempDir()
	p := writeKeyFile(t, dir, "r.hex", "0x"+toHex(pk), 0o600)
	s, _ := LoadFromConfig(&config.SignerConfig{ReporterKeyPaths: []string{p}, Threshold: 1}, 1)
	if _, err := s.Sign([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error on short digest")
	}
}

func TestExtractPrivateKeyHex_VariantsAndErrors(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"raw hex", "0xabc123", "0xabc123", false},
		{"with leading whitespace", "   0xabc123\n", "0xabc123", false},
		{"json keystore", `{"address":"0x00","privateKey":"0xdeadbeef"}`, "0xdeadbeef", false},
		{"json missing field", `{"address":"0x00"}`, "", true},
		{"empty", "", "", true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractPrivateKeyHex([]byte(tt.in))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.EqualFold(got, tt.want) {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

// toHex returns the 64-char lowercase hex representation of an ECDSA private key.
func toHex(pk *ecdsa.PrivateKey) string {
	return hex.EncodeToString(crypto.FromECDSA(pk))
}
