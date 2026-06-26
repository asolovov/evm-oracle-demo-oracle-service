// Package config defines the oracle-service configuration schema, defaults, and validation.
//
// All configuration lives here per architecture rule 6. Every nested key is
// registered with viper.SetDefault in init.go (rule 6 — viper services); a
// separate Validate pass fails fast on missing required keys post-Unmarshal.
package config

// Scheme is the top-level configuration aggregate.
type Scheme struct {
	Database   DatabaseConfig   `mapstructure:"database"`
	GRPC       GRPCConfig       `mapstructure:"grpc"`
	Healthz    HealthzConfig    `mapstructure:"healthz"`
	Chain      ChainConfig      `mapstructure:"chain"`
	Price      PriceClientConfig `mapstructure:"price"`
	Indexer    IndexerClientConfig `mapstructure:"indexer"`
	Stream     StreamConfig     `mapstructure:"stream"`
	Signer     SignerConfig     `mapstructure:"signer"`
	Submission SubmissionConfig `mapstructure:"submission"`
	Heartbeat  HeartbeatConfig  `mapstructure:"heartbeat"`
	Conversion ConversionConfig `mapstructure:"conversion"`
	Telemetry  TelemetryConfig  `mapstructure:"telemetry"`
}

// DatabaseConfig holds Postgres connection settings (rule 7 — dedicated DB).
type DatabaseConfig struct {
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	User            string `mapstructure:"user"`
	Password        string `mapstructure:"password"`
	Name            string `mapstructure:"name"`
	SSLMode         string `mapstructure:"ssl_mode"`
	MaxOpenConns    int    `mapstructure:"max_open_conns"`
	MaxIdleConns    int    `mapstructure:"max_idle_conns"`
	ConnMaxLifetime int    `mapstructure:"conn_max_lifetime"` // seconds
}

// GRPCConfig holds the admin/read gRPC server settings (no TriggerUpdate).
type GRPCConfig struct {
	Host              string `mapstructure:"host"`
	Port              int    `mapstructure:"port"`
	ReflectionEnabled bool   `mapstructure:"reflection_enabled"`
}

// HealthzConfig holds the /healthz, /readyz, /metrics listener settings.
type HealthzConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

// ChainConfig holds settings for the single target chain (Ethereum Sepolia per
// task 04 deployment; spec says Base Sepolia but the contracts repo deployed to
// Ethereum Sepolia — same flag the indexer task records).
type ChainConfig struct {
	Name                string            `mapstructure:"name"`
	ChainID             uint64            `mapstructure:"chain_id"`
	RPCURL              string            `mapstructure:"rpc_url"`
	RegistryAddress     string            `mapstructure:"registry_address"`
	AggregatorAddresses map[string]string `mapstructure:"aggregator_addresses"` // assetID (e.g. "WETH") -> hex address
}

// PriceClientConfig holds the price-service gRPC client settings.
type PriceClientConfig struct {
	Address       string `mapstructure:"address"`
	TimeoutSec    int    `mapstructure:"timeout_sec"`
}

// IndexerClientConfig holds the indexer-service gRPC client settings.
type IndexerClientConfig struct {
	Address    string `mapstructure:"address"`
	TimeoutSec int    `mapstructure:"timeout_sec"`
}

// StreamConfig holds the indexer.StreamEvents consumer settings.
type StreamConfig struct {
	BackfillFromBlock   uint64 `mapstructure:"backfill_from_block"`
	ReconnectBackoffSec int    `mapstructure:"reconnect_backoff_sec"`
	ReconnectMaxBackoffSec int `mapstructure:"reconnect_max_backoff_sec"`
}

// SignerConfig holds reporter-key loading settings.
type SignerConfig struct {
	ReporterKeyPaths     []string `mapstructure:"reporter_key_paths"`
	ReporterAddresses    []string `mapstructure:"reporter_addresses"` // expected EOA addresses (checklist)
	Threshold            int      `mapstructure:"threshold"`
	AllowInsecurePerms   bool     `mapstructure:"allow_insecure_perms"` // dev-only escape hatch; production must keep this false
}

// SubmissionConfig holds on-chain submission retry + gas settings, plus the
// async-pipeline knobs (task 06.1).
type SubmissionConfig struct {
	MaxRetries        int     `mapstructure:"max_retries"`
	ReplaceAfterSec   int     `mapstructure:"replace_after_sec"`
	GasMultiplier     float64 `mapstructure:"gas_multiplier"`
	ConfirmTimeoutSec int     `mapstructure:"confirm_timeout_sec"`

	// Workers is the size of the async processing pool (price-fetch + sign).
	// One stuck/un-priceable asset occupies at most one slot; the rest keep
	// flowing — this is what prevents head-of-line blocking. (task 06.1)
	Workers int `mapstructure:"workers"`
	// RequestTTLSec bounds how long a queued request is retried before it is
	// marked `expired` and abandoned. TTL applies only PRE-broadcast — once a
	// request consumes a nonce it runs to a terminal tx state. (task 06.1)
	RequestTTLSec int `mapstructure:"request_ttl_sec"`
}

// HeartbeatConfig holds per-asset heartbeat scheduler defaults.
type HeartbeatConfig struct {
	IntervalSec        int     `mapstructure:"interval_sec"`
	DeviationThreshold float64 `mapstructure:"deviation_threshold"`
	Enabled            bool    `mapstructure:"enabled"`
}

// ConversionConfig holds float-to-int256 settings.
type ConversionConfig struct {
	OnChainDecimals int `mapstructure:"on_chain_decimals"`
}

// TelemetryConfig holds logging + metrics flags.
type TelemetryConfig struct {
	LogLevel  string `mapstructure:"log_level"`
	LogFormat string `mapstructure:"log_format"` // "json" or "text"
}
