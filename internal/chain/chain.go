// Package chain is the oracle-service's JSON-RPC client wrapper around the
// abigen contract bindings (priceaggregator, oracleregistry).
//
// Plain Go package (architecture rule 5 — chain client is an external-system
// handler, not a template module). The Client struct holds one *ethclient.Client
// dialed at startup; callers come from the submitter (tx broadcast) and the
// heartbeat scheduler (latestRoundData read).
package chain

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/asolovov/evm-oracle-demo-oracle-service/pkg/contracts/oracleregistry"
	"github.com/asolovov/evm-oracle-demo-oracle-service/pkg/contracts/priceaggregator"
)

// ErrTxNotMined is the well-known sentinel for WaitMined polling.
var ErrTxNotMined = errors.New("tx not yet mined")

// IsRevertError reports whether err originated from an on-chain revert —
// typically surfaced via eth_estimateGas or eth_call simulation before the
// tx is ever broadcast, but the watcher's receipt-status=0 path also routes
// reverts here for symmetry.
//
// These errors are PERMANENT. Re-broadcasting the same calldata cannot change
// a deterministic contract revert, so callers (the submitter) treat them as
// terminal failures: persist STATUS_FAILED and let the streamconsumer advance
// its cursor past the event. The alternative — retrying — burns RPC quota
// and fills logs without any chance of progress.
//
// We use a small substring match because go-ethereum returns the RPC error
// verbatim (`execution reverted`, `execution reverted: reason ...`) without a
// stable typed error wrapper, and decoded custom-error data requires
// per-contract ABI plumbing that we don't need just to classify.
func IsRevertError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "revert") ||
		strings.Contains(msg, "invalid opcode")
}

// IsInsufficientFundsError reports whether err means the broadcasting wallet
// cannot pay for the transaction — a THIRD outcome class distinct from a
// permanent revert (IsRevertError) and a generic transient error.
//
// Two go-ethereum code paths surface a drained wallet, and we match both:
//
//   - "insufficient funds …" — the node rejects the signed tx at broadcast
//     time. Covers core ErrInsufficientFunds
//     ("insufficient funds for gas * price + value", incl. the EIP-1559
//     maxFeePerGas balance check) and "insufficient funds for transfer".
//   - "gas required exceeds allowance" — surfaces at CLIENT-SIDE
//     eth_estimateGas (we leave GasLimit unset, so bind estimates). The
//     estimator caps its gas ceiling at balance/feeCap, which collapses to
//     near-zero for a near-empty wallet, so estimation fails here long before
//     the tx is signed.
//
// CAUTION: "gas required exceeds allowance" is NOT categorically a funds
// problem — it is the generic estimator-ceiling error and a funded wallet can
// hit it transiently. Callers MUST corroborate it with an explicit balance
// check (see Submitter.canAfford) before treating it as drained; the string
// alone must not trip the circuit breaker.
//
// We deliberately match the EXACT substrings below and never bare
// "insufficient": the PriceAggregator contract defines InsufficientFee /
// InsufficientSignatures custom errors whose revert strings would otherwise
// be misclassified as funds problems.
func IsInsufficientFundsError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "insufficient funds") ||
		strings.Contains(msg, "gas required exceeds allowance")
}

// IsGasAllowanceError reports whether err is specifically the client-side
// estimator-ceiling error ("gas required exceeds allowance"). Unlike a node
// "insufficient funds" rejection (authoritative — that wallet truly can't pay),
// this string is AMBIGUOUS: it also fires for an ordinary out-of-gas on a
// funded wallet. Callers should corroborate it with a balance check before
// treating the wallet as drained; on its own it must not drive failover or trip
// the breaker.
func IsGasAllowanceError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "gas required exceeds allowance")
}

// Client is a thin go-ethereum wrapper centered on the surface oracle-service
// needs: fulfillPrice broadcasts, EIP-1559 gas pricing, latestRoundData reads.
type Client struct {
	eth     *ethclient.Client
	chainID *big.Int
	// gasMultiplier scales the EIP-1559 caps before broadcast. 1.1 is the
	// usual demo default; tunes the replace-by-fee bump per attempt.
	gasMultiplier float64
}

// GasStrategy is the post-suggest gas envelope used for one broadcast.
type GasStrategy struct {
	GasTipCap *big.Int // maxPriorityFeePerGas
	GasFeeCap *big.Int // maxFeePerGas
	GasLimit  uint64
}

// Dial opens the RPC connection.
//
// gasMultiplier is the per-attempt bump applied to both maxFeePerGas and
// maxPriorityFeePerGas. 1.1 is a sensible demo default.
func Dial(ctx context.Context, rpcURL string, chainID uint64, gasMultiplier float64) (*Client, error) {
	eth, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", rpcURL, err)
	}
	// Sanity check the chain id so we surface misconfiguration immediately.
	cidCtx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer cancel()
	got, err := eth.ChainID(cidCtx)
	if err != nil {
		eth.Close()
		return nil, fmt.Errorf("get chain id: %w", err)
	}
	if got.Uint64() != chainID {
		eth.Close()
		return nil, fmt.Errorf("rpc chain id %s != configured %d", got.String(), chainID)
	}
	if gasMultiplier < 1.0 {
		gasMultiplier = 1.0
	}
	return &Client{eth: eth, chainID: got, gasMultiplier: gasMultiplier}, nil
}

// Close releases the RPC connection.
func (c *Client) Close() {
	if c.eth != nil {
		c.eth.Close()
	}
}

// ChainID returns the chain id this client is bound to (re-fetched at Dial).
func (c *Client) ChainID() *big.Int {
	return new(big.Int).Set(c.chainID)
}

// SuggestGas pulls the latest base fee + tip suggestion from the node and
// builds an EIP-1559 envelope scaled by gasMultiplier^attempt. attempt is
// 0 for the first broadcast, 1+ for replace-by-fee bumps.
func (c *Client) SuggestGas(ctx context.Context, attempt int) (GasStrategy, error) {
	tip, err := c.eth.SuggestGasTipCap(ctx)
	if err != nil {
		return GasStrategy{}, fmt.Errorf("suggest tip cap: %w", err)
	}
	head, err := c.eth.HeaderByNumber(ctx, nil)
	if err != nil {
		return GasStrategy{}, fmt.Errorf("get head: %w", err)
	}
	baseFee := big.NewInt(0)
	if head.BaseFee != nil {
		baseFee = new(big.Int).Set(head.BaseFee)
	}
	// feeCap = baseFee*2 + tip. Standard heuristic that survives a single
	// base-fee doubling between block N and inclusion.
	feeCap := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tip)

	// Bump by gasMultiplier^(attempt+1) so attempt 0 is already +10%.
	mult := c.gasMultiplier
	for i := 0; i <= attempt; i++ {
		tip = scaleBigInt(tip, mult)
		feeCap = scaleBigInt(feeCap, mult)
	}

	return GasStrategy{GasTipCap: tip, GasFeeCap: feeCap, GasLimit: 0}, nil
}

// SubmitFulfillment builds, signs and broadcasts a fulfillPrice tx against
// the asset's aggregator. It does NOT wait for inclusion — callers persist
// the returned tx hash and poll via WaitMined.
//
// The transactor is built from the supplied transactOpts. Caller is expected
// to wire the deployer/owner key here (NOT a reporter key — the reporter
// keys never sign txs, only the digest). For demo-mode any funded EOA works.
func (c *Client) SubmitFulfillment(
	ctx context.Context,
	auth *bind.TransactOpts,
	aggregator common.Address,
	reqID, price, timestamp *big.Int,
	signatures [][]byte,
	gas GasStrategy,
) (common.Hash, error) {
	bound, err := priceaggregator.NewPriceAggregator(aggregator, c.eth)
	if err != nil {
		return common.Hash{}, fmt.Errorf("bind aggregator: %w", err)
	}

	// Clone auth so callers can reuse it across submissions without our
	// gas mutations leaking back.
	opts := *auth
	opts.Context = ctx
	opts.GasTipCap = gas.GasTipCap
	opts.GasFeeCap = gas.GasFeeCap
	opts.GasLimit = gas.GasLimit
	opts.NoSend = false

	tx, err := bound.FulfillPrice(&opts, reqID, price, timestamp, signatures)
	if err != nil {
		return common.Hash{}, fmt.Errorf("fulfillPrice: %w", err)
	}
	return tx.Hash(), nil
}

// ReplaceFulfillment re-submits with the same nonce + bumped gas. Used by
// the submitter when ReplaceAfterSec elapses without inclusion.
//
// The replacement nonce is read off the auth opts caller — submitter pins
// the original Nonce before the first broadcast and reuses it.
func (c *Client) ReplaceFulfillment(
	ctx context.Context,
	auth *bind.TransactOpts,
	aggregator common.Address,
	reqID, price, timestamp *big.Int,
	signatures [][]byte,
	gas GasStrategy,
) (common.Hash, error) {
	// Same wire shape as SubmitFulfillment; the difference is *behavioral*
	// (caller pre-pins auth.Nonce + bumps the gas strategy). Kept as a
	// separate method so the submitter's intent reads cleanly.
	return c.SubmitFulfillment(ctx, auth, aggregator, reqID, price, timestamp, signatures, gas)
}

// LatestRoundData reads (roundId, answer, startedAt, updatedAt, answeredInRound)
// from the aggregator. Returns the answer (int256, Chainlink scale) plus the
// updatedAt timestamp the heartbeat scheduler needs.
func (c *Client) LatestRoundData(ctx context.Context, aggregator common.Address) (price *big.Int, updatedAt *big.Int, err error) {
	bound, err := priceaggregator.NewPriceAggregator(aggregator, c.eth)
	if err != nil {
		return nil, nil, fmt.Errorf("bind aggregator: %w", err)
	}
	r, err := bound.LatestRoundData(&bind.CallOpts{Context: ctx})
	if err != nil {
		return nil, nil, fmt.Errorf("latest round data: %w", err)
	}
	return r.Answer, r.UpdatedAt, nil
}

// LatestStartedAt returns `latestRoundData().startedAt` from the aggregator,
// or 0 if the aggregator has no rounds yet (NoRoundData revert).
//
// The contract enforces a monotonic-startedAt invariant: every fulfillPrice
// submission must carry `submittedAt > latestStartedAt`, else it reverts with
// `StaleTimestamp(submittedAt, latestStartedAt)`. The submitter reads this
// before signing so it can clamp its on-chain timestamp to satisfy the guard
// — without that floor, two close-in-time submissions for the same asset can
// both end up with the same `aggregated_at` from price-service (if the
// upstream aggregation hasn't refreshed), and the second one bricks on chain.
func (c *Client) LatestStartedAt(ctx context.Context, aggregator common.Address) (*big.Int, error) {
	bound, err := priceaggregator.NewPriceAggregator(aggregator, c.eth)
	if err != nil {
		return nil, fmt.Errorf("bind aggregator: %w", err)
	}
	r, err := bound.LatestRoundData(&bind.CallOpts{Context: ctx})
	if err != nil {
		// `NoRoundData()` reverts when there's no previous round to read.
		// Treat as zero so the caller's floor falls back to "anything > 0".
		if IsRevertError(err) {
			return new(big.Int), nil
		}
		return nil, fmt.Errorf("latest round data: %w", err)
	}
	return r.StartedAt, nil
}

// AssetID reads the bytes32 asset id off the aggregator. Used at startup so
// the chain client can verify it has the right address for an asset and
// reject typos at config time.
func (c *Client) AssetID(ctx context.Context, aggregator common.Address) (common.Hash, error) {
	bound, err := priceaggregator.NewPriceAggregator(aggregator, c.eth)
	if err != nil {
		return common.Hash{}, fmt.Errorf("bind aggregator: %w", err)
	}
	id, err := bound.AssetId(&bind.CallOpts{Context: ctx})
	if err != nil {
		return common.Hash{}, fmt.Errorf("read assetId: %w", err)
	}
	return common.Hash(id), nil
}

// RegistryListAssets is used by the heartbeat scheduler to enumerate the
// supported asset universe without re-reading config — keeps the source of
// truth on-chain.
func (c *Client) RegistryListAssets(ctx context.Context, registry common.Address) ([]common.Hash, error) {
	bound, err := oracleregistry.NewOracleRegistry(registry, c.eth)
	if err != nil {
		return nil, fmt.Errorf("bind registry: %w", err)
	}
	ids, err := bound.ListAssets(&bind.CallOpts{Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("list assets: %w", err)
	}
	out := make([]common.Hash, len(ids))
	for i, id := range ids {
		out[i] = common.Hash(id)
	}
	return out, nil
}

// NonceAt is exposed so the submitter can pin the nonce before the first
// broadcast (replace-by-fee re-uses the same nonce on every attempt).
func (c *Client) NonceAt(ctx context.Context, addr common.Address) (uint64, error) {
	return c.eth.PendingNonceAt(ctx, addr)
}

// TxReceipt polls for a receipt. Returns ErrTxNotMined if not yet mined so
// callers can choose to wait or proceed to replace.
func (c *Client) TxReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	r, err := c.eth.TransactionReceipt(ctx, hash)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return nil, ErrTxNotMined
		}
		return nil, fmt.Errorf("tx receipt: %w", err)
	}
	return r, nil
}

// BalanceAt returns an account's native-token balance — used by the metric
// that pages on a reporter wallet running low.
func (c *Client) BalanceAt(ctx context.Context, addr common.Address) (*big.Int, error) {
	return c.eth.BalanceAt(ctx, addr, nil)
}

// scaleBigInt multiplies a *big.Int by a float multiplier using a fixed-point
// trick. mult is expected to be a small bump (1.0 - 2.0); we keep precision
// by scaling by 1e6 first.
func scaleBigInt(v *big.Int, mult float64) *big.Int {
	if mult == 1.0 {
		return new(big.Int).Set(v)
	}
	const prec = 1_000_000
	num := big.NewInt(int64(mult * prec))
	out := new(big.Int).Mul(v, num)
	out.Div(out, big.NewInt(prec))
	return out
}
