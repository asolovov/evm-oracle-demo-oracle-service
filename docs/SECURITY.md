# Security Notes — `evm-oracle-demo-oracle-service`

> **This is a portfolio demo. It is not a production oracle.** Read this whole
> file before deploying anywhere that touches real funds.

## Operational pre-requisite: fund the broadcaster wallets

The submitter broadcasts `fulfillPrice` transactions from a **pool** of EOAs — by default the full reporter set (`SIGNER_REPORTER_KEY_PATHS`). The on-chain contract does not gate `msg.sender` — the M-of-N digest signatures are the sole authorisation — so any funded EOA can broadcast, and reusing the reporter keys keeps the demo at three funded wallets instead of six.

**Multi-wallet rotation + failover (task 06.3).** The single sender goroutine rotates across the pool round-robin, each wallet carrying its own nonce counter. If a wallet can't pay, the send **fails over to the next wallet** — draining one wallet no longer stalls the oracle. A submission is only ever considered funds-blocked **after every wallet in the pool has been tried and refused**.

**Funds-awareness + circuit breaker (task 06.2).** Before each broadcast a pre-flight balance gate skips a wallet that demonstrably can't cover `maxFeePerGas × SUBMISSION_GAS_LIMIT_ESTIMATE`, so a drained wallet doesn't even burn an `eth_estimateGas` call. When **all** wallets are drained the breaker trips **open**: broadcasting suspends, the heartbeat scheduler pauses, and a balance probe runs on exponential backoff (`SUBMISSION_BREAKER_BACKOFF_MIN_SEC` → `…_MAX_SEC`). Refunding any wallet closes the breaker and resumes automatically. This replaces the previous failure mode where a drained reporter\[0\] caused an infinite `gas required exceeds allowance` / `insufficient funds` retry storm against the RPC.

Recommended floor on Sepolia: **≥ 0.1 ETH per wallet** (each `fulfillPrice` is ~150k gas; 0.1 ETH covers thousands of submissions at Sepolia base-fee). The `oracle_reporter_balance_eth{address}` Prometheus gauge is refreshed continuously (every 30s, not just at startup) for **every** pool wallet — alert on any falling below the floor, and on `oracle_circuit_breaker_open == 1` / `oracle_broadcaster_funds_blocked_total` increasing.

If you decouple broadcasting from signing (a future `SIGNER_BROADCASTER_KEY_PATHS`), only the broadcaster wallets need balance; pure signers do not.

## Threat model in scope

The oracle-service custodies three EOA private keys whose signatures collectively authorise on-chain price submissions. The on-chain contract verifies M-of-N signatures over an EIP-712 digest; whoever holds M of the N keys can post any price they want, signed.

Concretely: if an attacker reads two of `reporter{1,2,3}.json` off disk, they can submit arbitrary `fulfillPrice(price, ...)` calls to the deployed `PriceAggregator` contracts. Any downstream consumer (the demo dashboard, plus anyone integrating against the Chainlink-shaped feed) sees the attacker's price as canonical.

## Demo-mode simplifications (what production would change)

| Surface | Demo | Production |
|---------|------|------------|
| Reporter key custody | Plain JSON files on disk at `/etc/lighthouse/secrets/reporter{1,2,3}.json` with `0400` perms, owned by the service user. | KMS / HSM / hardware wallet. The service should call out to a signing oracle (e.g. `eth_signTypedData_v4` against a remote signer) rather than holding raw private keys. |
| Reporter quorum | 2-of-3 across the same VPS. | M-of-N across N independent operators on distinct infrastructure. |
| Key file permissions | Enforced fail-fast: `signer.LoadFromConfig` rejects anything more permissive than `0600` unless `SIGNER_ALLOW_INSECURE_PERMS=true`. That flag exists for local development only. | `SIGNER_ALLOW_INSECURE_PERMS` is never set in production. |
| Image build | Distroless `nonroot`. Keys are NEVER baked into the image — they are mounted as a read-only volume at runtime. | Same shape, with the secret source pointing at the cluster's secret manager (Vault, AWS Secrets Manager, GCP Secret Manager). |
| Network | Service binds gRPC on `0.0.0.0:9090`; expected to sit behind Caddy / nginx on the VPS, not exposed to the open internet. | mTLS + workload identity. The admin RPC (`SetHeartbeat`) needs a real auth layer, not just network isolation. |
| Submission idempotency | Stream consumer checks `oracle_submissions.req_id` before dispatch. The indexer stream is at-least-once; the DB check makes oracle's reaction at-most-once. **Transient** broadcast errors (insufficient funds, RPC unreachable) do NOT persist a FAILED row — they propagate up so the consumer reconnects and retries the same event after the operator fixes the underlying issue. **Permanent** broadcast errors (`execution reverted` from `eth_estimateGas` / `eth_call` simulation) DO persist FAILED with the revert reason in `last_error` and return nil so the consumer advances past the dead event. Same for on-chain reverts surfaced by the receipt watcher. | Same shape. The integrity guarantee scales with how much you trust the indexer's confirmation gate. |
| Asset id resolution | `application.go::resolveAssetIDs()` reads `aggregator.assetId()` from chain at startup and verifies it matches `keccak256` of either case-variant of the operator-supplied symbol. The submitter signs digests with this on-chain bytes32, NOT a re-derived hash. Catches symbol-case drift between the deploy script and the price-service wire format (the original deploy used uppercase, price-service wire is lowercase). | Same shape; consider periodic re-resolution if asset rotation is expected during the service's lifetime. |
| Submitted timestamp (monotonic guard) | The contract reverts with `StaleTimestamp(submittedAt, latestStartedAt)` unless `submittedAt > latestStartedAt`. `submitter.submit()` reads `latestStartedAt` via `chain.LatestStartedAt()` before signing and clamps `submittedAt = max(price.aggregated_at, latestStartedAt + 1)`. Required because price-service's `aggregated_at` can legitimately repeat across requests (cached aggregations when nothing has changed) — without the clamp the second-and-subsequent submission for the same asset bricks on chain. | Same shape; consider extending the floor to `max(price.aggregated_at, latestStartedAt + 1, block.timestamp - skew)` if clock skew between off-chain wall-clock and chain block timestamps becomes a problem. |
| Idempotency scope | `repository.ExistsForAggregatorReqID(aggregator, req_id)` is the streamconsumer's per-event check. **Scope is `(aggregator, req_id)`, NOT `req_id` alone** — each PriceAggregator owns its own req_id counter starting at 1, so the same numeric req_id legitimately exists across all 10 aggregators. The original scope (req_id alone) silently dropped 7 of 8 sibling-asset events when one had already been recorded. | Same shape. |
| Replace-by-fee | Same nonce + gas bump on `SUBMISSION_REPLACE_AFTER_SEC` elapsed; max 3 retries then `STATUS_DROPPED`. | Tighter operator monitoring; alert on `oracle_submissions_total{status=\"dropped\"}` > 0. |
| Async pipeline + request TTL (task 06.1) | Requests are processed asynchronously by a worker pool (`SUBMISSION_WORKERS`); one un-priceable asset can't block others. The single sender goroutine serializes the broadcaster nonce (the only ordered stage). A request not fulfilled within `SUBMISSION_REQUEST_TTL_SEC` is marked terminal `STATUS_EXPIRED` — **pre-broadcast only**, so a nonce is never consumed-then-abandoned (no nonce-gap stall). Durable `queued`/`processing`/`sending` rows are re-enqueued on restart. | Same shape. Alert on `oracle_requests_expired_total` (assets that repeatedly can't be priced) and watch `oracle_request_processing_duration_seconds`. Multi-instance would need a `FOR UPDATE SKIP LOCKED` claim instead of in-memory dispatch. |
| Multi-wallet broadcaster + funds breaker (task 06.2 / 06.3) | The sender rotates across a pool of broadcaster EOAs (the reporter set) with **per-wallet nonce counters** and round-robin selection. A pre-flight balance gate + `insufficient funds` / `gas required exceeds allowance` classification route a drained wallet to **failover**, not retry. The funds-blocked event + circuit breaker fire **only after every wallet is exhausted**; the breaker then suspends broadcasting + pauses heartbeats and auto-recovers via a balance probe on backoff. Funds shortage is recoverable, so the request is NOT marked `STATUS_FAILED` — it stays queued within its TTL. The `broadcaster` column records which wallet+nonce sent each tx (for replace-by-fee + ops; restart reconciliation of in-flight `pending` rows remains the documented v1 gap). | Same shape. Decouple broadcaster keys from signer keys; per-wallet alerting; consider highest-balance-first selection and KMS-backed transactors. |

## What the on-chain contract checks (independent of this service)

- `keccak256(0x1901 ‖ domainSeparator ‖ keccak256(abi.encode(PRICE_TYPEHASH, reqId, assetId, price, timestamp)))` — the EIP-712 digest.
- M distinct authorised reporters signed. Duplicate signers count once.
- `reqId` has not been fulfilled before (replay protection at the contract level).
- `timestamp >= latestStartedAt` (monotonic; M-01 audit fix).
- `safeCast(price)` into int256 with overflow protection (M-02 audit fix).

If you re-deploy contracts you MUST also re-roll the reporter set so old leaked keys can't post to the new chain id.

## Operational controls

- `/healthz` returns 200 once the listener is up. `/readyz` pings the DB.
- `/metrics` exposes the Prometheus registry. Alert sources:
  - `oracle_submissions_total{status=\"failed\"}` rate > 0 over 5m.
  - `oracle_submissions_total{status=\"dropped\"}` > 0 (replace-by-fee exhausted).
  - `oracle_stream_lag_seconds` > 30s sustained (indexer falling behind).
  - `oracle_stream_reconnect_total` rate spike (connectivity issues).
  - `oracle_reporter_balance_eth{address}` < threshold (refill needed; refreshed every 30s).
  - `oracle_circuit_breaker_open == 1` (ALL broadcaster wallets drained — broadcasting suspended).
  - `oracle_broadcaster_funds_blocked_total` increasing (a submission exhausted the whole pool on funds).
- Logs are JSON when `TELEMETRY_LOG_FORMAT=json`. Reporter addresses are logged; reporter private keys are NEVER logged.

## Disclosure

This is a personal demo project. If you find a real security issue (vs. a documented demo simplification), please open an issue or email the address in this repo's profile.

---

Author: **Andrei Solovov** — [GitHub](https://github.com/asolovov) · [LinkedIn](https://www.linkedin.com/in/asolovov/)
