# Security Notes — `evm-oracle-demo-oracle-service`

> **This is a portfolio demo. It is not a production oracle.** Read this whole
> file before deploying anywhere that touches real funds.

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
| Submission idempotency | Stream consumer checks `oracle_submissions.req_id` before dispatch. The indexer stream is at-least-once; the DB check makes oracle's reaction at-most-once. | Same shape. The integrity guarantee scales with how much you trust the indexer's confirmation gate. |
| Replace-by-fee | Same nonce + gas bump on `SUBMISSION_REPLACE_AFTER_SEC` elapsed; max 3 retries then `STATUS_DROPPED`. | Tighter operator monitoring; alert on `oracle_submissions_total{status=\"dropped\"}` > 0. |

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
  - `oracle_reporter_balance_eth{address}` < threshold (refill needed).
- Logs are JSON when `TELEMETRY_LOG_FORMAT=json`. Reporter addresses are logged; reporter private keys are NEVER logged.

## Disclosure

This is a personal demo project. If you find a real security issue (vs. a documented demo simplification), please open an issue or email the address in this repo's profile.

---

Author: **Andrei Solovov** — [GitHub](https://github.com/asolovov) · [LinkedIn](https://www.linkedin.com/in/asolovov/)
