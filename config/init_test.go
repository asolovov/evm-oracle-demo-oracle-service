package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestDefaults_SmokeKeys(t *testing.T) {
	t.Cleanup(func() { viper.Reset() })
	viper.Reset()
	setDefaults()

	tests := []struct {
		key  string
		want any
	}{
		{"database.port", 5432},
		{"database.name", "evm_oracle"},
		{"grpc.port", 9090},
		{"healthz.port", 8080},
		{"chain.chain_id", 11155111},
		{"signer.threshold", 2},
		{"submission.max_retries", 3},
		{"submission.gas_multiplier", 1.1},
		{"heartbeat.interval_sec", 3600},
		{"conversion.on_chain_decimals", 8},
		{"telemetry.log_format", "json"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := viper.Get(tt.key)
			switch want := tt.want.(type) {
			case int:
				if viper.GetInt(tt.key) != want {
					t.Fatalf("%s: want %d got %v", tt.key, want, got)
				}
			case float64:
				if viper.GetFloat64(tt.key) != want {
					t.Fatalf("%s: want %f got %v", tt.key, want, got)
				}
			case string:
				if viper.GetString(tt.key) != want {
					t.Fatalf("%s: want %q got %v", tt.key, want, got)
				}
			}
		})
	}
}

func TestValidate_FailsOnMissingRequired(t *testing.T) {
	cfg := &Scheme{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on empty Scheme")
	}
}

func TestValidate_OKOnMinimalValidConfig(t *testing.T) {
	cfg := &Scheme{
		Database: DatabaseConfig{Name: "evm_oracle", Password: "x"},
		Chain: ChainConfig{
			ChainID:             11155111,
			RPCURL:              "https://rpc.example",
			RegistryAddress:     "0x0000000000000000000000000000000000000001",
			AggregatorAddresses: map[string]string{"WETH": "0x0000000000000000000000000000000000000002"},
		},
		Price:   PriceClientConfig{Address: "price:9090"},
		Indexer: IndexerClientConfig{Address: "indexer:9090"},
		Signer: SignerConfig{
			ReporterKeyPaths: []string{"/secrets/r1.key", "/secrets/r2.key", "/secrets/r3.key"},
			Threshold:        2,
		},
		Submission: SubmissionConfig{
			MaxRetries:           3,
			ReplaceAfterSec:      60,
			GasMultiplier:        1.1,
			Workers:              4,
			RequestTTLSec:        600,
			GasLimitEstimate:     300000,
			BreakerBackoffMinSec: 60,
			BreakerBackoffMaxSec: 900,
		},
		Conversion: ConversionConfig{OnChainDecimals: 8},
		Heartbeat:  HeartbeatConfig{Enabled: true, IntervalSec: 3600, DeviationThreshold: 0.015},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}
