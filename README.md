# evm-oracle-demo-oracle-service

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?logo=go)](https://go.dev/)

Reporter signing + on-chain price submission for the [EVM Oracle Demo](https://github.com/asolovov?tab=repositories&q=evm-oracle-demo). One of four Go microservices that together form a pull-based, multi-source price oracle covering 5 crypto and 5 RWA assets on Ethereum Sepolia.

This is a **portfolio piece**, not a production oracle. See [`docs/SECURITY.md`](./docs/SECURITY.md) for the demo-mode caveats and what production would require.

## Role in the system

```
indexer-service ──(gRPC StreamEvents)──►  oracle-service  ──(JSON-RPC fulfillPrice)──► PriceAggregator on chain
price-service   ──(gRPC GetPrice)─────►        │
                                               └── M-of-N EIP-712 signatures (LIGHTHOUSE_V1)
```

Per [spec OQ-10](https://github.com/asolovov/evm-oracle-demo-protocols), the oracle is **reactive**:

- Subscribes to `indexer.StreamEvents(kinds=[EVENT_KIND_PRICE_REQUESTED])` as a long-lived gRPC client. The indexer is the single chain-observer; events flow through this stream only after they cross the confirmation threshold.
- On each delivered event: fetch aggregated price (double) from `price-service`, convert to int256 at Chainlink's 8-decimal scale, sign the EIP-712 digest with each reporter key, submit `fulfillPrice` to the asset's aggregator.
- Internal heartbeat scheduler emits time- and deviation-driven submissions (`reqId == 0`) without any inter-service RPC.

The gRPC server surface is **admin + read only**: `SetHeartbeat`, `GetSubmissionStatus`, `ListSubmissions`. There is **no `TriggerUpdate`** — that was removed when the indexer became the single observer.

## Quickstart

```bash
# 1. Install codegen toolchain (pinned versions; rule 9).
make proto-install

# 2. Generate proto stubs into internal/genproto/ (gitignored).
make proto-gen

# 3. Build (proto-gen runs as a prerequisite).
make build

# 4. Run unit tests.
make test

# 5. Run with the full docker-compose stack (Postgres + migrate + oracle).
#    Requires CHAIN_RPC_URL and CHAIN_AGGREGATOR_ADDRESSES set in .env.
make compose-up
```

The binary lands at `./bin/evm-oracle-demo-oracle-service`.

> ⚠️  **Fund reporter\[0\] before going live.** The chain client reuses the first
> reporter key as the broadcaster EOA for every `fulfillPrice` tx. If reporter\[0\]
> doesn't hold native gas balance, every event will loop in the stream consumer
> with `insufficient funds for transfer`. See [`docs/SECURITY.md`](./docs/SECURITY.md#operational-pre-requisite-fund-the-broadcaster-wallet)
> for the rationale + recommended floor.

## Configuration

Every env var the service reads is registered with `viper.SetDefault` in `config/init.go` (rule 6), so a single `grep SetDefault config/` enumerates the full surface.

Required (no useful default):

| Key | Purpose |
|-----|---------|
| `DATABASE_PASSWORD` | Postgres password for `oracle_user`. |
| `CHAIN_RPC_URL` | JSON-RPC endpoint for Ethereum Sepolia (Alchemy / Infura / public). |
| `CHAIN_AGGREGATOR_ADDRESSES` | JSON map: `{"WETH":"0x...","WBTC":"0x..."}`. |
| `SIGNER_REPORTER_KEY_PATHS` | JSON array of absolute paths to reporter key files. |

Sensible defaults:

| Key | Default |
|-----|---------|
| `DATABASE_HOST` / `PORT` / `USER` / `NAME` / `SSL_MODE` | `localhost` / `5432` / `oracle_user` / `evm_oracle` / `disable` |
| `GRPC_HOST` / `PORT` | `0.0.0.0` / `9090` |
| `HEALTHZ_HOST` / `PORT` | `0.0.0.0` / `8080` |
| `CHAIN_NAME` / `CHAIN_ID` / `REGISTRY_ADDRESS` | `sepolia` / `11155111` / `0x89a6c12a403733c6a817472cec46a530581cb7ef` |
| `PRICE_ADDRESS` / `INDEXER_ADDRESS` | `price-service:9090` / `indexer-service:9090` |
| `SIGNER_THRESHOLD` / `SIGNER_ALLOW_INSECURE_PERMS` | `2` / `false` |
| `SUBMISSION_MAX_RETRIES` / `REPLACE_AFTER_SEC` / `GAS_MULTIPLIER` / `CONFIRM_TIMEOUT_SEC` | `3` / `60` / `1.1` / `300` |
| `SUBMISSION_WORKERS` / `SUBMISSION_REQUEST_TTL_SEC` | `4` / `600` (async pool size / pre-broadcast request TTL) |
| `HEARTBEAT_ENABLED` / `INTERVAL_SEC` / `DEVIATION_THRESHOLD` | `true` / `3600` / `0.015` |
| `CONVERSION_ON_CHAIN_DECIMALS` | `8` (Chainlink scale) |

## gRPC surface

Defined in [`protocols/oracle/v1/oracle.proto`](./protocols/oracle/v1/oracle.proto):

```protobuf
service OracleService {
  rpc SetHeartbeat(SetHeartbeatRequest) returns (SetHeartbeatResponse);
  rpc GetSubmissionStatus(GetSubmissionStatusRequest) returns (SubmissionStatus);
  rpc ListSubmissions(ListSubmissionsRequest) returns (ListSubmissionsResponse);
}
```

Reflection is on by default for `grpcurl`:

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext -d '{"req_id":"42"}' localhost:9090 oracle.v1.OracleService/GetSubmissionStatus
```

## HTTP surface (healthz / metrics)

| Endpoint | Description |
|----------|-------------|
| `GET /healthz` | Liveness probe (returns 200 once the listener is up). |
| `GET /readyz` | Readiness probe — pings the DB; returns 503 with a JSON `reason` on failure. |
| `GET /metrics` | Prometheus exposition format. Service-owned registry (not the global one). |

Metrics emitted:

- `oracle_submissions_total{asset, status}`
- `oracle_submission_duration_seconds{asset}`
- `oracle_signature_set_total{reporter_address}`
- `oracle_gas_used`
- `oracle_reporter_balance_eth{address}`
- `oracle_stream_events_received_total{kind}`
- `oracle_stream_reconnect_total`
- `oracle_stream_lag_seconds`
- `oracle_heartbeat_skipped_total{symbol}`

## Project layout

```
.
├── cmd/                          # Cobra entrypoint + serve subcommand (CLI + config init only — rule 1).
├── config/                       # /config is the sole config home (rule 6). SetDefault per nested key.
├── db/migrations/                # 0001_init.up.sql / .down.sql — oracle_submissions, pending_txs,
│                                 #   stream_cursor, heartbeat_schedules. Migrated via golang-migrate.
├── internal/
│   ├── application.go            # The single wiring point (rule 2).
│   ├── chain/                    # go-ethereum client wrapping abigen bindings (rule 5).
│   ├── grpcclient/               # Thin gRPC wrappers (price + indexer) — rule 5 packages.
│   ├── grpcsrv/                  # OracleService server (admin + read only — no TriggerUpdate).
│   ├── healthz/                  # /healthz, /readyz, /metrics listener on its own port.
│   ├── heartbeat/                # Per-asset scheduler driven by chain + price-service.
│   ├── metrics/                  # Service-owned Prometheus registry.
│   ├── models/                   # Domain types + ALL proto conversions (rule 3).
│   ├── module/                   # Generic Init/Start/Stop manager (template-derived; unused by us).
│   ├── repository/               # pgx/v5 Postgres repo + integration test (-tags=integration).
│   ├── signer/                   # Reporter key loading + EIP-712 signing (rule 5).
│   ├── streamconsumer/           # Long-lived indexer.StreamEvents client + idempotency.
│   └── submitter/                # Event/heartbeat -> sign + broadcast + watch (terminal state).
├── pkg/contracts/                # abigen bindings (rule 5 exception — committed).
│   ├── oracleregistry/
│   ├── priceaggregator/
│   └── reporterset/
├── protocols/                    # git subtree from evm-oracle-demo-protocols (proto + lint only).
├── buf.gen.yaml                  # Service-owned codegen config; retargets go_package via managed.
├── Makefile
├── Dockerfile                    # Multi-stage distroless build.
└── docker-compose.yml            # Local stack: Postgres + migrate + oracle.
```

## Architecture rules honoured

1. `cmd/` does CLI + config init only.
2. `internal/application.go` is the sole wiring point.
3. Domain types + every proto/DB conversion live in `internal/models/`.
4. Modules limited to data storage, services, servers, in-project handlers.
5. External-system clients (`internal/chain`, `internal/signer`, `internal/grpcclient`, `internal/streamconsumer`) are plain packages, NOT template modules. Abigen bindings under `pkg/contracts/` are the documented exception.
6. All configuration lives in `/config`; `viper.SetDefault` registered for every nested key.
7. Owns DB `evm_oracle`; no other service touches it.
8. Single binary, env-var-driven; bootstrap (migrations) runs as a sidecar in compose.
9. Generated `.pb.go` is gitignored; service owns its `buf.gen.yaml` with pinned codegen versions. Abigen output is the only generated Go that ships in git.

## Known v1 gaps (carry-over)

- `docker compose up --build` smoke test (cold start → healthy → first stream event → confirmed `PriceFulfilled`) not executed in this session. Compose file is in place; needs a human verification pass against a real RPC + indexer.
- The `internal/streamconsumer` and `internal/heartbeat` coverage is in the 60–70% range — the metric/watcher branches need live wiring to exercise. Recommend covering during task 13 shakedown rather than synthesising with mocks.
- Reporter wallet balances are read once at startup. Continuous refresh would belong in the heartbeat scheduler (every N ticks) — added if/when alerting goes live.

## Deviations to flag

- **Ethereum Sepolia, not Base Sepolia.** The contracts repo deployed to Ethereum Sepolia per task 04's status report; the project entry-point still says Base. Aligning here with the indexer service.
- **Reporter[0] doubles as the broadcaster.** The contract doesn't gate `msg.sender` (the M-of-N digest verification is the sole authorisation), so reusing reporter[0]'s EOA for tx broadcast keeps the demo at three funded wallets instead of four.
- **logrus retained.** The task spec called for zerolog; the template ships logrus and `TELEMETRY_LOG_FORMAT=json` produces structured output anyway. Switching would touch every log site for marginal benefit.

---

Built by **Andrei Solovov** — [GitHub](https://github.com/asolovov) · [LinkedIn](https://www.linkedin.com/in/asolovov/)
