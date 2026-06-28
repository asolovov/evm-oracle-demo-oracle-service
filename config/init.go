package config

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

// init registers viper defaults at package load.
//
// Per architecture rule 6, viper.SetDefault is mandatory for every nested key
// the service reads — AutomaticEnv alone does NOT populate nested keys on
// Unmarshal. SetDefault is also machine-discoverable documentation:
// `grep "SetDefault" config/` enumerates the entire env-var surface.
//
//nolint:gochecknoinits // config defaults registered at package load
func init() {
	setDefaults()
}

// setDefaults registers viper defaults for every Scheme key.
// Required-but-empty keys default to the zero value and are caught by Validate.
func setDefaults() {
	// Database.
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.user", "oracle_user")
	viper.SetDefault("database.password", "")
	viper.SetDefault("database.name", "evm_oracle")
	viper.SetDefault("database.ssl_mode", "disable")
	viper.SetDefault("database.max_open_conns", 10)
	viper.SetDefault("database.max_idle_conns", 2)
	viper.SetDefault("database.conn_max_lifetime", 300)

	// gRPC server.
	viper.SetDefault("grpc.host", "0.0.0.0")
	viper.SetDefault("grpc.port", 9090)
	viper.SetDefault("grpc.reflection_enabled", true)

	// Healthz / metrics listener.
	viper.SetDefault("healthz.host", "0.0.0.0")
	viper.SetDefault("healthz.port", 8080)

	// Chain.
	viper.SetDefault("chain.name", "sepolia")
	viper.SetDefault("chain.chain_id", 11155111)
	viper.SetDefault("chain.rpc_url", "")
	viper.SetDefault("chain.registry_address", "")
	viper.SetDefault("chain.aggregator_addresses", map[string]string{})

	// Price-service client.
	viper.SetDefault("price.address", "price-service:9090")
	viper.SetDefault("price.timeout_sec", 5)

	// Indexer-service client.
	viper.SetDefault("indexer.address", "indexer-service:9090")
	viper.SetDefault("indexer.timeout_sec", 30)

	// Stream consumer.
	viper.SetDefault("stream.backfill_from_block", 0)
	viper.SetDefault("stream.reconnect_backoff_sec", 1)
	viper.SetDefault("stream.reconnect_max_backoff_sec", 30)

	// Signer.
	viper.SetDefault("signer.reporter_key_paths", []string{})
	viper.SetDefault("signer.reporter_addresses", []string{})
	viper.SetDefault("signer.threshold", 2)
	viper.SetDefault("signer.allow_insecure_perms", false)

	// Submission.
	viper.SetDefault("submission.max_retries", 3)
	viper.SetDefault("submission.replace_after_sec", 60)
	viper.SetDefault("submission.gas_multiplier", 1.1)
	viper.SetDefault("submission.confirm_timeout_sec", 300)
	viper.SetDefault("submission.workers", 4)
	viper.SetDefault("submission.request_ttl_sec", 600)
	viper.SetDefault("submission.gas_limit_estimate", 300000)
	viper.SetDefault("submission.breaker_backoff_min_sec", 60)
	viper.SetDefault("submission.breaker_backoff_max_sec", 900)

	// Heartbeat.
	viper.SetDefault("heartbeat.enabled", true)
	viper.SetDefault("heartbeat.interval_sec", 3600)
	viper.SetDefault("heartbeat.deviation_threshold", 0.015)

	// Conversion (Chainlink 8 decimals).
	viper.SetDefault("conversion.on_chain_decimals", 8)

	// Telemetry.
	viper.SetDefault("telemetry.log_level", "info")
	viper.SetDefault("telemetry.log_format", "json")
}

// validate range-checks the submission keys. Split out of Scheme.Validate to
// keep that function's cyclomatic complexity in check.
func (c *SubmissionConfig) validate() []error {
	var errs []error
	if c.MaxRetries < 0 {
		errs = append(errs, errors.New("submission.max_retries must be >= 0"))
	}
	if c.ReplaceAfterSec <= 0 {
		errs = append(errs, errors.New("submission.replace_after_sec must be > 0"))
	}
	if c.GasMultiplier < 1.0 {
		errs = append(errs, errors.New("submission.gas_multiplier must be >= 1.0"))
	}
	if c.Workers <= 0 {
		errs = append(errs, errors.New("submission.workers must be > 0"))
	}
	if c.RequestTTLSec <= 0 {
		errs = append(errs, errors.New("submission.request_ttl_sec must be > 0"))
	}
	if c.GasLimitEstimate == 0 {
		errs = append(errs, errors.New("submission.gas_limit_estimate must be > 0"))
	}
	if c.BreakerBackoffMinSec <= 0 {
		errs = append(errs, errors.New("submission.breaker_backoff_min_sec must be > 0"))
	}
	if c.BreakerBackoffMaxSec < c.BreakerBackoffMinSec {
		errs = append(errs, errors.New("submission.breaker_backoff_max_sec must be >= submission.breaker_backoff_min_sec"))
	}
	return errs
}

// Validate fails fast on missing or out-of-range required keys.
// Called from App.Init() — see internal/application.go.
func (s *Scheme) Validate() error {
	var errs []error

	if s.Database.Password == "" {
		errs = append(errs, errors.New("database.password is required"))
	}
	if s.Database.Name == "" {
		errs = append(errs, errors.New("database.name is required"))
	}

	if s.Chain.RPCURL == "" {
		errs = append(errs, errors.New("chain.rpc_url is required"))
	}
	if s.Chain.ChainID == 0 {
		errs = append(errs, errors.New("chain.chain_id is required"))
	}
	if s.Chain.RegistryAddress == "" {
		errs = append(errs, errors.New("chain.registry_address is required"))
	}
	if len(s.Chain.AggregatorAddresses) == 0 {
		errs = append(errs, errors.New("chain.aggregator_addresses must not be empty"))
	}

	if s.Price.Address == "" {
		errs = append(errs, errors.New("price.address is required"))
	}
	if s.Indexer.Address == "" {
		errs = append(errs, errors.New("indexer.address is required"))
	}

	if len(s.Signer.ReporterKeyPaths) == 0 {
		errs = append(errs, errors.New("signer.reporter_key_paths is required"))
	}
	if s.Signer.Threshold <= 0 {
		errs = append(errs, errors.New("signer.threshold must be > 0"))
	}
	if s.Signer.Threshold > len(s.Signer.ReporterKeyPaths) {
		errs = append(errs, fmt.Errorf("signer.threshold (%d) must be <= len(signer.reporter_key_paths) (%d)",
			s.Signer.Threshold, len(s.Signer.ReporterKeyPaths)))
	}
	if len(s.Signer.ReporterAddresses) > 0 && len(s.Signer.ReporterAddresses) != len(s.Signer.ReporterKeyPaths) {
		errs = append(errs, errors.New("signer.reporter_addresses, when set, must match signer.reporter_key_paths length"))
	}

	errs = append(errs, s.Submission.validate()...)

	if s.Conversion.OnChainDecimals <= 0 || s.Conversion.OnChainDecimals > 18 {
		errs = append(errs, errors.New("conversion.on_chain_decimals must be in (0, 18]"))
	}

	if s.Heartbeat.Enabled {
		if s.Heartbeat.IntervalSec <= 0 {
			errs = append(errs, errors.New("heartbeat.interval_sec must be > 0 when heartbeat.enabled"))
		}
		if s.Heartbeat.DeviationThreshold < 0 {
			errs = append(errs, errors.New("heartbeat.deviation_threshold must be >= 0"))
		}
	}

	return errors.Join(errs...)
}
